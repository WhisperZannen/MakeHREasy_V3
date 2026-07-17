"""薪酬月度办理核心引擎。

这里负责把人员有效快照、薪酬规则版本、社保个人扣款和月度评分组装成
可解释的工资草稿。页面只负责收集输入和展示结果，不再自行拼工资公式。
"""

import calendar
import json
import os
import sqlite3
from datetime import datetime

from modules.core_arrangements import get_effective_arrangement


def _get_db_connection():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.environ.get(
        "MAKE_HR_DB_PATH", os.path.join(project_root, "database", "hr_core.db")
    )
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


def _normalize_month(value):
    text = str(value or "").strip()
    try:
        parsed = datetime.strptime(text, "%Y-%m")
    except ValueError as exc:
        raise ValueError("月份必须使用 YYYY-MM 格式") from exc
    return parsed.strftime("%Y-%m")


def previous_month(value):
    month = datetime.strptime(_normalize_month(value), "%Y-%m")
    year = month.year if month.month > 1 else month.year - 1
    number = month.month - 1 if month.month > 1 else 12
    return f"{year:04d}-{number:02d}"


def _payroll_rank(value):
    if value is None or str(value).strip() in {"", "None", "nan"}:
        return None
    return int(float(value))


def get_effective_payroll_snapshot(target_month, conn=None):
    """按15日口径返回指定月份人员算薪所需的完整档案快照。"""
    month = _normalize_month(target_month)
    own_conn = conn is None
    conn = conn or _get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT e.emp_id, e.name, e.status, e.dept_id, e.post_rank,
                   e.post_grade, ep.pos_id, ep.tech_grade
            FROM employees e
            LEFT JOIN employee_profiles ep ON ep.emp_id = e.emp_id
            """
        ).fetchall()
        snapshots = {str(row["emp_id"]): dict(row) for row in rows}
        deadline = f"{month}-15 23:59:59"
        changes = conn.execute(
            """
            SELECT * FROM personnel_changes
            WHERE change_date > ?
            ORDER BY change_date DESC, change_id DESC
            """,
            (deadline,),
        ).fetchall()
        field_pairs = (
            ("dept_id", "old_dept_id", "new_dept_id"),
            ("pos_id", "old_pos_id", "new_pos_id"),
            ("tech_grade", "old_tech_grade", "new_tech_grade"),
            ("post_rank", "old_post_rank", "new_post_rank"),
            ("post_grade", "old_post_grade", "new_post_grade"),
        )
        for change in changes:
            snapshot = snapshots.get(str(change["emp_id"]))
            if not snapshot:
                continue
            for target, old_field, new_field in field_pairs:
                if change[old_field] is not None and change[new_field] is not None:
                    snapshot[target] = change[old_field]

        departments = {
            int(row["dept_id"]): row["dept_name"]
            for row in conn.execute("SELECT dept_id, dept_name FROM departments")
        }
        positions = {
            int(row["pos_id"]): row["pos_name"]
            for row in conn.execute("SELECT pos_id, pos_name FROM positions")
        }
        for snapshot in snapshots.values():
            dept_id = snapshot.get("dept_id")
            pos_id = snapshot.get("pos_id")
            snapshot["dept_name"] = departments.get(
                int(dept_id), "未分配部门"
            ) if dept_id is not None else "未分配部门"
            snapshot["pos_name"] = positions.get(
                int(pos_id), "未设置岗位"
            ) if pos_id is not None else "未设置岗位"
        return snapshots
    finally:
        if own_conn:
            conn.close()


def get_rule_version_for_month(target_month, conn=None, allow_draft=True):
    """选择指定月份适用的规则；没有正式版本时允许使用草稿试运行。"""
    month = _normalize_month(target_month)
    own_conn = conn is None
    conn = conn or _get_db_connection()
    try:
        statuses = ("active", "draft") if allow_draft else ("active",)
        placeholders = ",".join("?" for _ in statuses)
        row = conn.execute(
            f"""
            SELECT * FROM payroll_rule_versions
            WHERE effective_from_month <= ?
              AND (effective_to_month IS NULL OR effective_to_month >= ?)
              AND status IN ({placeholders})
            ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END,
                     effective_from_month DESC, rule_version_id DESC
            LIMIT 1
            """,
            (month, month, *statuses),
        ).fetchone()
        return dict(row) if row else None
    finally:
        if own_conn:
            conn.close()


def _salary_amount(conn, version_id, snapshot):
    rank = _payroll_rank(snapshot.get("post_rank"))
    grade = str(snapshot.get("post_grade") or "").strip().upper()
    if rank is None or not grade:
        return 0.0, "岗位工资缺少岗级或档次"
    row = conn.execute(
        """
        SELECT amount FROM payroll_salary_matrix_rules
        WHERE rule_version_id=? AND post_rank=? AND post_grade=?
        """,
        (version_id, rank, grade),
    ).fetchone()
    if not row:
        return 0.0, f"岗位工资表没有{rank}{grade}"
    return float(row[0]), None


def _performance_components(conn, version, snapshot):
    """按照总阀门计算普通身份下的原绩效与激励包。"""
    version_id = int(version["rule_version_id"])
    mapping = conn.execute(
        """
        SELECT * FROM payroll_position_rule_mappings
        WHERE rule_version_id=? AND pos_id=? AND enabled=1
        """,
        (version_id, snapshot.get("pos_id")),
    ).fetchone()
    result = {
        "category": "unclassified",
        "management_role": None,
        "official_position": None,
        "original_performance": 0.0,
        "incentive_pack": 0.0,
        "original_coefficient": None,
        "incentive_coefficient": None,
        "warnings": [],
    }
    if not mapping:
        result["warnings"].append(f"岗位“{snapshot.get('pos_name')}”尚未归类")
        return result
    result["category"] = mapping["payroll_category"]
    result["management_role"] = mapping["management_role"]
    result["official_position"] = mapping["official_position_name"]
    rank = _payroll_rank(snapshot.get("post_rank"))
    if rank is None:
        result["warnings"].append("缺少绩效计算岗级")
        return result

    category = mapping["payroll_category"]
    if category == "company_leader":
        row = conn.execute(
            """
            SELECT standard_amount FROM payroll_company_leader_rules
            WHERE rule_version_id=?
              AND (pos_id=? OR (pos_id IS NULL AND leader_position_name=?))
            ORDER BY CASE WHEN pos_id=? THEN 0 ELSE 1 END
            LIMIT 1
            """,
            (
                version_id, snapshot.get("pos_id"), snapshot.get("pos_name"),
                snapshot.get("pos_id"),
            ),
        ).fetchone()
        if row and row[0] is not None:
            result["original_performance"] = float(row[0])
        else:
            result["warnings"].append("公司领导绩效标准尚未维护")
        return result

    if category == "management":
        role = mapping["management_role"]
        multiplier = 1.0
        base_role = role
        if role == "senior_advisor":
            derived = conn.execute(
                """
                SELECT base_management_role, multiplier
                FROM payroll_derived_management_rules
                WHERE rule_version_id=? AND special_role=?
                """,
                (version_id, role),
            ).fetchone()
            if derived:
                base_role = derived["base_management_role"]
                multiplier = float(derived["multiplier"])
            else:
                result["warnings"].append("高级顾问80%派生规则缺失")
        perf = conn.execute(
            """
            SELECT coefficient FROM payroll_original_perf_rules
            WHERE rule_version_id=? AND employee_category=? AND post_rank=?
            """,
            (version_id, base_role, rank),
        ).fetchone()
        incentive = conn.execute(
            """
            SELECT coefficient FROM payroll_management_incentive_rules
            WHERE rule_version_id=? AND management_role=?
            """,
            (version_id, base_role),
        ).fetchone()
        if perf and perf[0] is not None:
            result["original_coefficient"] = float(perf[0]) * multiplier
            result["original_performance"] = round(
                float(version["original_perf_base"])
                * result["original_coefficient"], 2
            )
        else:
            result["warnings"].append("管理岗位原绩效系数缺失")
        if incentive and incentive[0] is not None:
            result["incentive_coefficient"] = float(incentive[0]) * multiplier
            result["incentive_pack"] = round(
                float(version["incentive_base"])
                * result["incentive_coefficient"], 2
            )
        else:
            result["warnings"].append("管理岗位激励包系数缺失")
        return result

    if category == "professional":
        perf = conn.execute(
            """
            SELECT coefficient FROM payroll_original_perf_rules
            WHERE rule_version_id=? AND employee_category='professional'
              AND post_rank=?
            """,
            (version_id, rank),
        ).fetchone()
        if perf and perf[0] is not None:
            result["original_coefficient"] = float(perf[0])
            result["original_performance"] = round(
                float(version["original_perf_base"])
                * result["original_coefficient"], 2
            )
        else:
            result["warnings"].append("专业岗位原绩效系数缺失")
        tech = str(snapshot.get("tech_grade") or "").strip().lower()
        column = f"{tech}_coefficient"
        allowed = {f"t{i}_coefficient" for i in range(1, 6)}
        if column not in allowed:
            result["warnings"].append("专业岗位缺少有效T级")
            return result
        incentive = conn.execute(
            f"""
            SELECT {column} FROM payroll_position_value_rules
            WHERE rule_version_id=? AND official_position_name=?
            """,
            (version_id, mapping["official_position_name"]),
        ).fetchone()
        if incentive and incentive[0] is not None:
            result["incentive_coefficient"] = float(incentive[0])
            result["incentive_pack"] = round(
                float(version["incentive_base"])
                * result["incentive_coefficient"], 2
            )
        else:
            result["warnings"].append("该岗位与T级组合没有激励包系数")
        return result

    result["warnings"].append(f"岗位“{snapshot.get('pos_name')}”尚未归类")
    return result


def _identity_effects(conn, version_id, emp_id, target_month):
    last_day = calendar.monthrange(
        int(target_month[:4]), int(target_month[5:7])
    )[1]
    target_date = f"{target_month}-{last_day:02d}"
    rows = conn.execute(
        """
        SELECT i.*, r.calculation_mode, r.performance_multiplier,
               r.annual_allowance, r.monthly_share, r.annual_share,
               r.parameters_json, r.remarks AS rule_remarks
        FROM employee_payroll_identities i
        LEFT JOIN payroll_identity_rules r
          ON r.rule_version_id=? AND r.identity_type=i.identity_type
         AND r.identity_level=i.identity_level AND r.enabled=1
        WHERE i.emp_id=? AND i.status IN ('active', 'ended')
          AND i.start_date <= ?
          AND (i.end_date IS NULL OR i.end_date >= ?)
        ORDER BY i.identity_id
        """,
        (version_id, emp_id, target_date, target_date),
    ).fetchall()
    performance_multiplier = 1.0
    monthly_allowance = 0.0
    labels = []
    warnings = []
    for row in rows:
        labels.append(f"{row['identity_type']}:{row['identity_level']}")
        if row["calculation_mode"] is None:
            warnings.append("存在有效薪酬身份，但总阀门没有对应待遇规则")
            continue
        if row["calculation_mode"] == "performance_multiplier":
            performance_multiplier = max(
                performance_multiplier, float(row["performance_multiplier"] or 1)
            )
        elif row["calculation_mode"] == "annual_allowance":
            monthly_allowance += round(
                float(row["annual_allowance"] or 0)
                * float(row["monthly_share"] or 0) / 12, 2
            )
        elif row["calculation_mode"] == "historical_adjustment":
            baseline = json.loads(row["baseline_snapshot"] or "{}")
            if not baseline:
                warnings.append("专家身份缺少聘任前待遇基线，暂不自动计算专家调整")
            else:
                warnings.append("专家历史差额规则已留底，需复核基线后再启用自动金额")
    return {
        "performance_multiplier": performance_multiplier,
        "monthly_allowance": monthly_allowance,
        "identities": labels,
        "warnings": warnings,
    }


def _score_maps(conn, run_id):
    person_scores = {}
    department_scores = {}
    for row in conn.execute(
        "SELECT * FROM payroll_score_inputs WHERE payroll_run_id=?",
        (run_id,),
    ):
        if row["score_scope"] == "person" and row["emp_id"]:
            person_scores[str(row["emp_id"])] = float(row["score"])
        elif row["score_scope"] == "department" and row["dept_id"] is not None:
            department_scores[int(row["dept_id"])] = float(row["score"])
    return person_scores, department_scores


def save_person_scores(pay_month, scores, source_file=None):
    """保存姓名/人员匹配后的个人评分，scores为{emp_id: score}。"""
    month = _normalize_month(pay_month)
    run_id = f"PAY-{month}"
    conn = _get_db_connection()
    try:
        if not conn.execute(
            "SELECT 1 FROM payroll_runs WHERE payroll_run_id=?", (run_id,)
        ).fetchone():
            raise ValueError("请先生成本月工资底稿")
        for emp_id, score in scores.items():
            conn.execute(
                """
                DELETE FROM payroll_score_inputs
                WHERE payroll_run_id=? AND score_scope='person' AND emp_id=?
                """,
                (run_id, str(emp_id)),
            )
            conn.execute(
                """
                INSERT INTO payroll_score_inputs(
                    payroll_run_id, score_scope, emp_id, score, source_file
                ) VALUES (?, 'person', ?, ?, ?)
                """,
                (run_id, str(emp_id), float(score), source_file),
            )
        conn.commit()
    finally:
        conn.close()


def _carry_values(conn, emp_id, pay_month):
    columns = (
        "base_salary, seniority_pay, comp_subsidy, telecom_subsidy, "
        "perf_float_subsidy, intern_subsidy, graduate_allowance, "
        "perf_standard, perf_base, perf_pack_coef, perf_leader_coef"
    )
    row = conn.execute(
        f"""
        SELECT {columns} FROM payroll_monthly_records
        WHERE emp_id=? AND cost_month < ?
        ORDER BY cost_month DESC LIMIT 1
        """,
        (emp_id, pay_month),
    ).fetchone()
    return dict(row) if row else {}


def generate_payroll_draft(pay_month, performance_month=None):
    """一键生成或刷新工资草稿，返回人数和待处理问题。"""
    pay_month = _normalize_month(pay_month)
    performance_month = _normalize_month(
        performance_month or previous_month(pay_month)
    )
    conn = _get_db_connection()
    try:
        version = get_rule_version_for_month(pay_month, conn, allow_draft=True)
        if not version:
            raise ValueError("没有找到适用于该月份的薪酬规则版本")
        version_id = int(version["rule_version_id"])
        run_id = f"PAY-{pay_month}"
        conn.execute(
            """
            INSERT INTO payroll_runs(
                payroll_run_id, pay_month, performance_month,
                rule_version_id, rule_status_snapshot, generated_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(pay_month) DO UPDATE SET
                performance_month=excluded.performance_month,
                rule_version_id=excluded.rule_version_id,
                rule_status_snapshot=excluded.rule_status_snapshot,
                generated_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP
            """,
            (run_id, pay_month, performance_month, version_id, version["status"]),
        )
        pay_snapshot = get_effective_payroll_snapshot(pay_month, conn)
        performance_snapshot = get_effective_payroll_snapshot(
            performance_month, conn
        )
        person_scores, department_scores = _score_maps(conn, run_id)
        social_rows = {
            str(row["emp_id"]): dict(row)
            for row in conn.execute(
                "SELECT * FROM ss_monthly_records WHERE cost_month=?",
                (pay_month,),
            )
        }
        current_records = {
            str(row["emp_id"]): dict(row)
            for row in conn.execute(
                "SELECT * FROM payroll_monthly_records WHERE cost_month=?",
                (pay_month,),
            )
        }
        generated = 0
        excluded = 0
        warning_people = []
        for emp_id, pay_person in pay_snapshot.items():
            if pay_person.get("status") != "在职":
                continue
            arrangement = get_effective_arrangement(emp_id, pay_month, conn)
            if not int(arrangement.get("payroll_included", 1)):
                excluded += 1
                continue
            perf_person = performance_snapshot.get(emp_id, pay_person)
            carry = _carry_values(conn, emp_id, pay_month)
            current = current_records.get(emp_id, {})
            warnings = []

            override = conn.execute(
                """
                SELECT * FROM payroll_person_calculation_overrides
                WHERE rule_version_id=? AND emp_id=? AND enabled=1
                """,
                (version_id, emp_id),
            ).fetchone()
            calculation_mode = override["calculation_mode"] if override else "automatic"
            if calculation_mode == "external_notice":
                base_salary = float(current.get("base_salary") or carry.get("base_salary") or 0)
                components = {
                    "category": "external_notice",
                    "management_role": None,
                    "official_position": None,
                    "original_performance": float(current.get("perf_standard") or carry.get("perf_standard") or 0),
                    "incentive_pack": float(current.get("perf_base") or carry.get("perf_base") or 0),
                    "original_coefficient": None,
                    "incentive_coefficient": None,
                    "warnings": [f"工资由{override['counterparty_name'] or '外部单位'}来函核定"],
                }
            else:
                base_salary, salary_warning = _salary_amount(
                    conn, version_id, pay_person
                )
                if salary_warning:
                    warnings.append(salary_warning)
                components = _performance_components(conn, version, perf_person)
            warnings.extend(components["warnings"])
            identity = _identity_effects(
                conn, version_id, emp_id, performance_month
            )
            warnings.extend(identity["warnings"])

            existing_score = current.get("perf_kpi_score")
            if emp_id in person_scores:
                score = person_scores[emp_id]
                score_source = "个人评分导入"
            elif (
                components.get("management_role") == "management_director"
                and pay_person.get("dept_id") in department_scores
            ):
                score = department_scores[pay_person["dept_id"]]
                score_source = "部门分数（主任分数）"
            elif existing_score not in (None, 0):
                score = float(existing_score)
                score_source = "本月已保存评分"
            else:
                score = 100.0
                score_source = "暂按100分，待导入"

            leader_coef = float(current.get("perf_leader_coef") or 1)
            pack_coef = float(current.get("perf_pack_coef") or 1)
            identity_coef = float(identity["performance_multiplier"] or 1)
            performance = round(
                (
                    float(components["original_performance"])
                    + float(components["incentive_pack"]) * pack_coef
                ) * score / 100 * leader_coef * identity_coef,
                2,
            )
            expert_allowance = float(identity["monthly_allowance"] or 0)
            social = social_rows.get(emp_id, {})
            explanation = {
                "rule_version": version["rule_name"],
                "rule_status": version["status"],
                "pay_month": pay_month,
                "performance_month": performance_month,
                "pay_position": pay_person.get("pos_name"),
                "performance_position": perf_person.get("pos_name"),
                "category": components["category"],
                "original_coefficient": components["original_coefficient"],
                "incentive_coefficient": components["incentive_coefficient"],
                "score_source": score_source,
                "identities": identity["identities"],
            }
            record_id = f"{pay_month}_{emp_id}"
            conn.execute(
                """
                INSERT INTO payroll_monthly_records(
                    record_id, cost_month, performance_month, payroll_run_id,
                    rule_version_id, emp_id, dept_name, dept_id_snapshot,
                    pos_id_snapshot, pos_name_snapshot, post_rank_snapshot,
                    post_grade_snapshot, tech_grade_snapshot, calculation_mode,
                    base_salary, seniority_pay, comp_subsidy, telecom_subsidy,
                    perf_float_subsidy, intern_subsidy, graduate_allowance,
                    expert_allowance, perf_standard, perf_base,
                    perf_kpi_score, perf_pack_coef, perf_leader_coef,
                    perf_excel_coef, perf_salary_calc,
                    ss_pension_pers, ss_medical_mix, ss_unemp_pers,
                    ss_fund_pers, ss_annuity_pers, arrangement_id,
                    business_type_snapshot, payroll_entity_code,
                    actual_work_unit_code, ultimate_cost_bearer_code,
                    salary_source, rule_explanation, calculation_warnings
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(record_id) DO UPDATE SET
                    performance_month=excluded.performance_month,
                    payroll_run_id=excluded.payroll_run_id,
                    rule_version_id=excluded.rule_version_id,
                    dept_name=excluded.dept_name,
                    dept_id_snapshot=excluded.dept_id_snapshot,
                    pos_id_snapshot=excluded.pos_id_snapshot,
                    pos_name_snapshot=excluded.pos_name_snapshot,
                    post_rank_snapshot=excluded.post_rank_snapshot,
                    post_grade_snapshot=excluded.post_grade_snapshot,
                    tech_grade_snapshot=excluded.tech_grade_snapshot,
                    calculation_mode=excluded.calculation_mode,
                    base_salary=excluded.base_salary,
                    expert_allowance=excluded.expert_allowance,
                    perf_standard=excluded.perf_standard,
                    perf_base=excluded.perf_base,
                    perf_kpi_score=excluded.perf_kpi_score,
                    perf_pack_coef=excluded.perf_pack_coef,
                    perf_leader_coef=excluded.perf_leader_coef,
                    perf_excel_coef=excluded.perf_excel_coef,
                    perf_salary_calc=excluded.perf_salary_calc,
                    ss_pension_pers=excluded.ss_pension_pers,
                    ss_medical_mix=excluded.ss_medical_mix,
                    ss_unemp_pers=excluded.ss_unemp_pers,
                    ss_fund_pers=excluded.ss_fund_pers,
                    ss_annuity_pers=excluded.ss_annuity_pers,
                    arrangement_id=excluded.arrangement_id,
                    business_type_snapshot=excluded.business_type_snapshot,
                    payroll_entity_code=excluded.payroll_entity_code,
                    actual_work_unit_code=excluded.actual_work_unit_code,
                    ultimate_cost_bearer_code=excluded.ultimate_cost_bearer_code,
                    salary_source=excluded.salary_source,
                    rule_explanation=excluded.rule_explanation,
                    calculation_warnings=excluded.calculation_warnings,
                    update_time=CURRENT_TIMESTAMP
                """,
                (
                    record_id, pay_month, performance_month, run_id, version_id,
                    emp_id, pay_person["dept_name"], pay_person.get("dept_id"),
                    pay_person.get("pos_id"), pay_person.get("pos_name"),
                    pay_person.get("post_rank"), pay_person.get("post_grade"),
                    pay_person.get("tech_grade"), calculation_mode,
                    base_salary,
                    float(current.get("seniority_pay") or carry.get("seniority_pay") or 0),
                    float(current.get("comp_subsidy") or carry.get("comp_subsidy") or 0),
                    float(current.get("telecom_subsidy") or carry.get("telecom_subsidy") or 0),
                    float(current.get("perf_float_subsidy") or carry.get("perf_float_subsidy") or 0),
                    float(current.get("intern_subsidy") or carry.get("intern_subsidy") or 0),
                    float(current.get("graduate_allowance") or carry.get("graduate_allowance") or 0),
                    expert_allowance, components["original_performance"],
                    components["incentive_pack"], score, pack_coef,
                    leader_coef, identity_coef, performance,
                    float(social.get("pension_pers") or 0),
                    float(social.get("medical_pers") or 0)
                    + float(social.get("medical_serious_pers") or 0),
                    float(social.get("unemp_pers") or 0),
                    float(social.get("fund_pers") or 0),
                    float(social.get("annuity_pers") or 0),
                    arrangement.get("arrangement_id"),
                    arrangement.get("arrangement_type", "normal"),
                    arrangement.get("payroll_entity_code"),
                    arrangement.get("actual_work_unit_code"),
                    arrangement.get("ultimate_cost_bearer_code"),
                    "外部来函核定" if calculation_mode == "external_notice" else "本单位发放",
                    json.dumps(explanation, ensure_ascii=False),
                    json.dumps(list(dict.fromkeys(warnings)), ensure_ascii=False),
                ),
            )
            generated += 1
            if warnings:
                warning_people.append({
                    "emp_id": emp_id,
                    "name": pay_person["name"],
                    "warnings": list(dict.fromkeys(warnings)),
                })
        conn.commit()
        recalculate_payroll_totals(pay_month, conn=conn)
        return {
            "payroll_run_id": run_id,
            "pay_month": pay_month,
            "performance_month": performance_month,
            "rule_version_id": version_id,
            "rule_name": version["rule_name"],
            "rule_status": version["status"],
            "generated": generated,
            "excluded": excluded,
            "warning_people": warning_people,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def recalculate_payroll_performance(pay_month, score_updates):
    """保存页面编辑的评分/系数并重新计算绩效，保留总阀门和身份系数。"""
    month = _normalize_month(pay_month)
    conn = _get_db_connection()
    try:
        run_id = f"PAY-{month}"
        for row in score_updates:
            score = float(row.get("score", 100) or 100)
            pack_coef = float(row.get("pack_coef", 1) or 1)
            leader_coef = float(row.get("leader_coef", 1) or 1)
            record = conn.execute(
                """
                SELECT perf_standard, perf_base, perf_excel_coef
                FROM payroll_monthly_records
                WHERE cost_month=? AND emp_id=?
                """,
                (month, str(row["emp_id"])),
            ).fetchone()
            if not record:
                continue
            calculated = round(
                (float(record["perf_standard"] or 0)
                 + float(record["perf_base"] or 0) * pack_coef)
                * score / 100 * leader_coef
                * float(record["perf_excel_coef"] or 1),
                2,
            )
            conn.execute(
                """
                UPDATE payroll_monthly_records
                SET perf_kpi_score=?, perf_pack_coef=?, perf_leader_coef=?,
                    perf_salary_calc=?, update_time=CURRENT_TIMESTAMP
                WHERE cost_month=? AND emp_id=?
                """,
                (score, pack_coef, leader_coef, calculated, month, str(row["emp_id"])),
            )
            conn.execute(
                """
                DELETE FROM payroll_score_inputs
                WHERE payroll_run_id=? AND score_scope='person' AND emp_id=?
                """,
                (run_id, str(row["emp_id"])),
            )
            conn.execute(
                """
                INSERT INTO payroll_score_inputs(
                    payroll_run_id, score_scope, emp_id, score,
                    source_name, remarks
                ) VALUES (?, 'person', ?, ?, '页面调整', '月度工资办理页面保存')
                """,
                (run_id, str(row["emp_id"]), score),
            )
        conn.commit()
        recalculate_payroll_totals(month, conn=conn)
    finally:
        conn.close()


def recalculate_payroll_totals(pay_month, conn=None):
    """重算应发、现金发放和实发；女工劳保费不进入人工成本应发。"""
    month = _normalize_month(pay_month)
    own_conn = conn is None
    conn = conn or _get_db_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM payroll_monthly_records WHERE cost_month=?", (month,)
        ).fetchall()
        for row in rows:
            gross = round(sum(float(row[name] or 0) for name in (
                "base_salary", "seniority_pay", "comp_subsidy",
                "telecom_subsidy", "perf_float_subsidy", "position_adj",
                "intern_subsidy", "graduate_allowance", "expert_allowance",
                "perf_salary_calc", "perf_adj", "commission_pay",
                "special_bonus_total", "history_clearance", "promotion_backpay",
            )), 2)
            cash_payable = round(gross + float(row["female_labor_subsidy"] or 0), 2)
            deductions = sum(float(row[name] or 0) for name in (
                "ss_pension_pers", "ss_medical_mix", "ss_unemp_pers",
                "ss_fund_pers", "ss_annuity_pers", "tax_deduction",
            ))
            conn.execute(
                """
                UPDATE payroll_monthly_records
                SET gross_salary_total=?, cash_payable_total=?, net_salary=?,
                    update_time=CURRENT_TIMESTAMP
                WHERE record_id=?
                """,
                (gross, cash_payable, round(cash_payable - deductions, 2), row["record_id"]),
            )
        conn.commit()
    finally:
        if own_conn:
            conn.close()


def get_payroll_identities(emp_id=None):
    """返回人员薪酬身份及聘期，供人员模块维护。"""
    conn = _get_db_connection()
    try:
        where = "WHERE i.emp_id=?" if emp_id else ""
        params = (str(emp_id),) if emp_id else ()
        return [dict(row) for row in conn.execute(
            f"""
            SELECT i.*, COALESCE(e.employee_no, '待分配') AS employee_no,
                   e.name AS employee_name
            FROM employee_payroll_identities i
            JOIN employees e ON e.emp_id=i.emp_id
            {where}
            ORDER BY i.start_date DESC, i.identity_id DESC
            """,
            params,
        ).fetchall()]
    finally:
        conn.close()


def save_payroll_identity(emp_id, identity_type, identity_level, start_date,
                          end_date=None, source_document="", remarks=""):
    """新增一段有时效的薪酬身份；不覆盖同一人员的其他身份。"""
    start = datetime.strptime(str(start_date), "%Y-%m-%d").strftime("%Y-%m-%d")
    end = None
    if end_date:
        end = datetime.strptime(str(end_date), "%Y-%m-%d").strftime("%Y-%m-%d")
        if end < start:
            return False, "结束日期不能早于开始日期"
    conn = _get_db_connection()
    try:
        conn.execute(
            """
            INSERT INTO employee_payroll_identities(
                emp_id, identity_type, identity_level, start_date, end_date,
                source_document, status, remarks
            ) VALUES (?, ?, ?, ?, ?, ?, 'active', ?)
            ON CONFLICT(emp_id, identity_type, identity_level, start_date)
            DO UPDATE SET end_date=excluded.end_date,
                          source_document=excluded.source_document,
                          status='active', remarks=excluded.remarks,
                          updated_at=CURRENT_TIMESTAMP
            """,
            (
                str(emp_id), str(identity_type), str(identity_level), start, end,
                str(source_document or "").strip(), str(remarks or "").strip(),
            ),
        )
        conn.commit()
        return True, "薪酬身份及有效期已保存"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def end_payroll_identity(identity_id, end_date):
    end = datetime.strptime(str(end_date), "%Y-%m-%d").strftime("%Y-%m-%d")
    conn = _get_db_connection()
    try:
        row = conn.execute(
            "SELECT start_date FROM employee_payroll_identities WHERE identity_id=?",
            (int(identity_id),),
        ).fetchone()
        if not row:
            return False, "薪酬身份不存在"
        if end < row["start_date"]:
            return False, "结束日期不能早于开始日期"
        conn.execute(
            """
            UPDATE employee_payroll_identities
            SET end_date=?, status='ended', updated_at=CURRENT_TIMESTAMP
            WHERE identity_id=?
            """,
            (end, int(identity_id)),
        )
        conn.commit()
        return True, "薪酬身份已结束，历史月份不会被改写"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()
