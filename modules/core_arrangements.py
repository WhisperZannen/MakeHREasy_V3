"""多形态用工关系、社保险种路由和结算归属解析。"""

from __future__ import annotations

import calendar
import hashlib
import os
import re
import sqlite3
from datetime import date
from typing import Any, Dict, Optional, Tuple

import pandas as pd


ARRANGEMENT_LABELS = {
    "normal": "普通在职",
    "proxy_social": "挂靠代缴",
    "city_transfer": "地市工作转入",
    "down_secondment": "下沉人员",
}

INSURANCE_LABELS = {
    "pension": "养老",
    "medical": "基本医疗",
    "medical_serious": "大病医疗",
    "unemp": "失业",
    "injury": "工伤",
    "maternity": "生育",
    "fund": "住房公积金",
    "annuity": "企业年金",
    "supplemental_medical": "补充医疗",
}


def _get_db_connection() -> sqlite3.Connection:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.environ.get(
        "MAKE_HR_DB_PATH",
        os.path.join(project_root, "database", "hr_core.db"),
    )
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn


def _month_range(target_month: str) -> Tuple[str, str]:
    year, month = [int(v) for v in target_month[:7].split("-")]
    last_day = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"


def entity_code_from_name(conn: sqlite3.Connection, entity_name: Optional[str]) -> Optional[str]:
    if entity_name is None or str(entity_name).strip() in {"", "本级", "None", "nan"}:
        return None
    value = str(entity_name).strip()
    row = conn.execute(
        "SELECT entity_code FROM business_entities WHERE entity_code = ? OR entity_name = ? LIMIT 1",
        (value, value),
    ).fetchone()
    return row["entity_code"] if row else value


def entity_name_from_code(conn: sqlite3.Connection, entity_code: Optional[str]) -> str:
    if not entity_code:
        return ""
    row = conn.execute(
        "SELECT entity_name FROM business_entities WHERE entity_code = ? LIMIT 1",
        (entity_code,),
    ).fetchone()
    return row["entity_name"] if row else str(entity_code)


def _implicit_arrangement(conn: sqlite3.Connection, emp_id: str) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT e.emp_id, e.status, e.dept_id, e.join_company_date,
               COALESCE(m.cost_center, '本级') AS cost_center
        FROM employees e
        LEFT JOIN ss_emp_matrix m ON e.emp_id = m.emp_id
        WHERE e.emp_id = ?
        """,
        (emp_id,),
    ).fetchone()
    if not row:
        return {
            "arrangement_id": None,
            "arrangement_type": "normal",
            "payroll_included": 1,
            "settlement_mode": "none",
            "settlement_cycle": "none",
        }

    is_proxy = row["status"] == "挂靠人员"
    branch_code = entity_code_from_name(conn, row["cost_center"])
    if row["cost_center"] == "本级":
        branch_code = None
    return {
        "arrangement_id": None,
        "emp_id": emp_id,
        "arrangement_type": "proxy_social" if is_proxy else "normal",
        "contract_entity_code": None,
        "payroll_entity_code": None,
        "home_dept_id": row["dept_id"],
        "actual_work_unit_code": branch_code,
        "related_branch_code": branch_code,
        "accounting_entity_code": None,
        "ultimate_cost_bearer_code": branch_code,
        "start_date": row["join_company_date"] or "1900-01-01",
        "planned_end_date": None,
        "actual_end_date": None,
        "payroll_included": 0 if is_proxy else 1,
        "settlement_mode": "proxy_social" if is_proxy else "none",
        "settlement_cycle": "quarterly" if is_proxy else "none",
        "status": "implicit",
        "remarks": "由现有人事状态和社保财务归属兼容推导",
    }


def get_effective_arrangement(
    emp_id: str,
    target_month: str,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Any]:
    own_conn = conn is None
    conn = conn or _get_db_connection()
    try:
        month_start, month_end = _month_range(target_month)
        row = conn.execute(
            """
            SELECT *
            FROM employee_arrangements
            WHERE emp_id = ?
              AND status NOT IN ('cancelled', '已取消')
              AND date(start_date) <= date(?)
              AND (actual_end_date IS NULL OR date(actual_end_date) >= date(?))
            ORDER BY date(start_date) DESC, arrangement_id DESC
            LIMIT 1
            """,
            (str(emp_id), month_end, month_start),
        ).fetchone()
        return dict(row) if row else _implicit_arrangement(conn, str(emp_id))
    finally:
        if own_conn:
            conn.close()


def _resolve_rule_entity(
    conn: sqlite3.Connection,
    rule: str,
    fixed_code: Optional[str],
    arrangement: Dict[str, Any],
    legacy_code: Optional[str],
) -> Optional[str]:
    if rule == "fixed":
        return fixed_code
    if rule == "contract_entity":
        return arrangement.get("contract_entity_code") or legacy_code
    if rule == "payroll_entity":
        return arrangement.get("payroll_entity_code") or legacy_code
    if rule == "actual_work_unit":
        return arrangement.get("actual_work_unit_code") or legacy_code
    if rule == "related_branch":
        return arrangement.get("related_branch_code") or legacy_code
    if rule == "accounting_entity":
        return arrangement.get("accounting_entity_code") or legacy_code
    if rule == "ultimate_cost_bearer":
        return arrangement.get("ultimate_cost_bearer_code") or legacy_code
    return legacy_code


def _default_payment_channel(payer_name: str, insurance_item: str) -> str:
    if payer_name == "中电数智":
        return "ct_digital:all_social"
    if payer_name == "省公司":
        return (
            "province_company:annuity"
            if insurance_item == "annuity"
            else "province_company:medical_group"
        )
    if payer_name == "省公众":
        if insurance_item == "fund":
            return "province_public:fund"
        if insurance_item in {"pension", "unemp", "injury"}:
            return "province_public:social_group"
        # 医疗、生育以后转到省公众时自动形成新通道，不会落进旧表遗漏。
        return "province_public:medical_group"
    return f"{payer_name or 'unknown'}:{insurance_item}"


def resolve_social_route(
    emp_id: str,
    insurance_item: str,
    target_month: str,
    legacy_enabled: int = 1,
    legacy_payer_name: str = "省公众",
    legacy_cost_center: str = "本级",
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Any]:
    """按“个人例外 > 关系政策 > 旧配置”解析单个险种。"""
    own_conn = conn is None
    conn = conn or _get_db_connection()
    try:
        arrangement = get_effective_arrangement(emp_id, target_month, conn)
        legacy_payer_code = entity_code_from_name(conn, legacy_payer_name)
        legacy_cost_code = entity_code_from_name(conn, legacy_cost_center)

        override = conn.execute(
            """
            SELECT * FROM employee_social_overrides
            WHERE emp_id = ? AND insurance_item = ? AND active = 1
              AND effective_from_month <= ?
              AND (effective_to_month IS NULL OR effective_to_month >= ?)
            ORDER BY effective_from_month DESC, override_id DESC
            LIMIT 1
            """,
            (str(emp_id), insurance_item, target_month, target_month),
        ).fetchone()

        policy = conn.execute(
            """
            SELECT * FROM social_route_policies
            WHERE arrangement_type = ? AND insurance_item = ? AND active = 1
              AND effective_from_month <= ?
              AND (effective_to_month IS NULL OR effective_to_month >= ?)
              AND (contract_entity_code IS NULL OR contract_entity_code = ?)
            ORDER BY priority DESC, effective_from_month DESC, route_policy_id DESC
            LIMIT 1
            """,
            (
                arrangement.get("arrangement_type", "normal"),
                insurance_item,
                target_month,
                target_month,
                arrangement.get("contract_entity_code"),
            ),
        ).fetchone()

        context: Dict[str, Any] = {
            "arrangement_id": arrangement.get("arrangement_id"),
            "arrangement_type": arrangement.get("arrangement_type", "normal"),
            "enabled": int(legacy_enabled or 0),
            "calculation_policy_entity": legacy_payer_name,
            "payer_entity_code": legacy_payer_code,
            "cost_bearer_code": (
                arrangement.get("ultimate_cost_bearer_code") or legacy_cost_code
            ),
            "settlement_counterparty_code": None,
            "settlement_mode": arrangement.get("settlement_mode") or "none",
            "settlement_cycle": arrangement.get("settlement_cycle") or "none",
            "amount_source": "system_calculated",
            "payment_channel_code": None,
            "route_policy_id": None,
            "override_id": None,
        }

        if policy:
            p = dict(policy)
            context["route_policy_id"] = p["route_policy_id"]
            if p.get("enabled_default") is not None:
                context["enabled"] = int(p["enabled_default"])
            context["payer_entity_code"] = _resolve_rule_entity(
                conn,
                p.get("payer_entity_rule", "legacy"),
                p.get("payer_entity_code"),
                arrangement,
                legacy_payer_code,
            )
            context["cost_bearer_code"] = _resolve_rule_entity(
                conn,
                p.get("cost_bearer_rule", "legacy"),
                p.get("cost_bearer_code"),
                arrangement,
                context["cost_bearer_code"],
            )
            for field in [
                "calculation_policy_entity",
                "settlement_counterparty_code",
                "settlement_mode",
                "settlement_cycle",
                "amount_source",
                "payment_channel_code",
            ]:
                if p.get(field) not in {None, ""}:
                    context[field] = p[field]

        if override:
            o = dict(override)
            context["override_id"] = o["override_id"]
            if o.get("enabled") is not None:
                context["enabled"] = int(o["enabled"])
            for field in [
                "calculation_policy_entity",
                "payer_entity_code",
                "cost_bearer_code",
                "settlement_counterparty_code",
                "settlement_mode",
                "settlement_cycle",
                "amount_source",
                "payment_channel_code",
            ]:
                if o.get(field) not in {None, ""}:
                    context[field] = o[field]

        payer_name = entity_name_from_code(conn, context.get("payer_entity_code"))
        cost_bearer_name = entity_name_from_code(conn, context.get("cost_bearer_code"))
        policy_value = context.get("calculation_policy_entity") or payer_name
        context["calculation_policy_entity"] = entity_name_from_code(conn, policy_value)
        context["payer_entity_name"] = payer_name or legacy_payer_name
        context["cost_bearer_name"] = cost_bearer_name or legacy_cost_center
        if not context.get("payment_channel_code"):
            context["payment_channel_code"] = _default_payment_channel(
                context["payer_entity_name"], insurance_item
            )
        return context
    finally:
        if own_conn:
            conn.close()


def is_payroll_included(emp_id: str, target_month: str, conn=None) -> bool:
    arrangement = get_effective_arrangement(emp_id, target_month, conn)
    return bool(int(arrangement.get("payroll_included", 1)))


def get_arrangements_dataframe(include_closed: bool = True) -> pd.DataFrame:
    conn = _get_db_connection()
    try:
        where = "" if include_closed else "WHERE a.status = 'active'"
        return pd.read_sql_query(
            f"""
            SELECT a.*, e.name AS emp_name, d.dept_name AS home_dept_name,
                   be_contract.entity_name AS contract_entity_name,
                   be_payroll.entity_name AS payroll_entity_name,
                   be_work.entity_name AS actual_work_unit_name,
                   be_branch.entity_name AS related_branch_name,
                   be_cost.entity_name AS ultimate_cost_bearer_name
            FROM employee_arrangements a
            JOIN employees e ON a.emp_id = e.emp_id
            LEFT JOIN departments d ON a.home_dept_id = d.dept_id
            LEFT JOIN business_entities be_contract ON a.contract_entity_code = be_contract.entity_code
            LEFT JOIN business_entities be_payroll ON a.payroll_entity_code = be_payroll.entity_code
            LEFT JOIN business_entities be_work ON a.actual_work_unit_code = be_work.entity_code
            LEFT JOIN business_entities be_branch ON a.related_branch_code = be_branch.entity_code
            LEFT JOIN business_entities be_cost ON a.ultimate_cost_bearer_code = be_cost.entity_code
            {where}
            ORDER BY a.start_date DESC, a.arrangement_id DESC
            """,
            conn,
        )
    finally:
        conn.close()


def get_entities_dataframe(active_only: bool = True) -> pd.DataFrame:
    conn = _get_db_connection()
    try:
        where = "WHERE active = 1" if active_only else ""
        return pd.read_sql_query(
            f"""
            SELECT entity_code, entity_name, entity_type, active
            FROM business_entities
            {where}
            ORDER BY CASE entity_type
                         WHEN '法人' THEN 1
                         WHEN '上级单位' THEN 2
                         WHEN '地市分公司' THEN 3
                         ELSE 9
                     END,
                     entity_name
            """,
            conn,
        )
    finally:
        conn.close()


def get_route_policies_dataframe(active_only: bool = True) -> pd.DataFrame:
    conn = _get_db_connection()
    try:
        where = "WHERE p.active = 1" if active_only else ""
        return pd.read_sql_query(
            f"""
            SELECT p.*,
                   ec.entity_name AS contract_entity_name,
                   ep.entity_name AS payer_entity_name,
                   eb.entity_name AS cost_bearer_name,
                   es.entity_name AS settlement_counterparty_name
            FROM social_route_policies p
            LEFT JOIN business_entities ec ON p.contract_entity_code = ec.entity_code
            LEFT JOIN business_entities ep ON p.payer_entity_code = ep.entity_code
            LEFT JOIN business_entities eb ON p.cost_bearer_code = eb.entity_code
            LEFT JOIN business_entities es ON p.settlement_counterparty_code = es.entity_code
            {where}
            ORDER BY p.effective_from_month DESC, p.priority DESC, p.route_policy_id DESC
            """,
            conn,
        )
    finally:
        conn.close()


def create_route_policy(data: Dict[str, Any]) -> Tuple[bool, str]:
    if not str(data.get("policy_name") or "").strip():
        return False, "政策名称必填"
    if data.get("insurance_item") not in INSURANCE_LABELS:
        return False, "险种编码无效"
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", str(data.get("effective_from_month") or "")):
        return False, "生效月份必须为 YYYY-MM"
    if data.get("effective_to_month") and not re.fullmatch(
        r"\d{4}-(0[1-9]|1[0-2])", str(data["effective_to_month"])
    ):
        return False, "失效月份必须为 YYYY-MM"
    if data.get("payer_entity_rule") == "fixed" and not data.get("payer_entity_code"):
        return False, "缴费主体规则为 fixed 时必须选择固定主体"
    if data.get("cost_bearer_rule") == "fixed" and not data.get("cost_bearer_code"):
        return False, "成本承担规则为 fixed 时必须选择固定单位"
    conn = _get_db_connection()
    try:
        columns = [
            "policy_name", "arrangement_type", "contract_entity_code",
            "insurance_item", "effective_from_month", "effective_to_month",
            "enabled_default", "calculation_policy_entity", "payer_entity_rule",
            "payer_entity_code", "cost_bearer_rule", "cost_bearer_code",
            "settlement_counterparty_code", "settlement_mode", "settlement_cycle",
            "amount_source", "payment_channel_code", "priority", "active", "remarks",
        ]
        conn.execute(
            f"INSERT INTO social_route_policies ({','.join(columns)}) "
            f"VALUES ({','.join(['?'] * len(columns))})",
            tuple(data.get(column) for column in columns),
        )
        conn.commit()
        return True, "险种路由政策已新增；旧政策和历史账单未被覆盖"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def get_social_overrides_dataframe(active_only: bool = True) -> pd.DataFrame:
    conn = _get_db_connection()
    try:
        where = "WHERE o.active = 1" if active_only else ""
        return pd.read_sql_query(
            f"""
            SELECT o.*, e.name AS emp_name,
                   ep.entity_name AS payer_entity_name,
                   eb.entity_name AS cost_bearer_name,
                   es.entity_name AS settlement_counterparty_name
            FROM employee_social_overrides o
            JOIN employees e ON o.emp_id = e.emp_id
            LEFT JOIN business_entities ep ON o.payer_entity_code = ep.entity_code
            LEFT JOIN business_entities eb ON o.cost_bearer_code = eb.entity_code
            LEFT JOIN business_entities es ON o.settlement_counterparty_code = es.entity_code
            {where}
            ORDER BY o.effective_from_month DESC, o.override_id DESC
            """,
            conn,
        )
    finally:
        conn.close()


def create_social_override(data: Dict[str, Any]) -> Tuple[bool, str]:
    if data.get("insurance_item") not in INSURANCE_LABELS:
        return False, "险种编码无效"
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", str(data.get("effective_from_month") or "")):
        return False, "生效月份必须为 YYYY-MM"
    if data.get("effective_to_month") and not re.fullmatch(
        r"\d{4}-(0[1-9]|1[0-2])", str(data["effective_to_month"])
    ):
        return False, "失效月份必须为 YYYY-MM"
    if not str(data.get("special_reason") or "").strip():
        return False, "特殊原因必填"
    conn = _get_db_connection()
    try:
        columns = [
            "emp_id", "insurance_item", "effective_from_month", "effective_to_month",
            "enabled", "calculation_policy_entity", "payer_entity_code",
            "cost_bearer_code", "settlement_counterparty_code", "settlement_mode",
            "settlement_cycle", "amount_source", "payment_channel_code",
            "special_reason", "source_document_no", "active",
        ]
        conn.execute(
            f"INSERT INTO employee_social_overrides ({','.join(columns)}) "
            f"VALUES ({','.join(['?'] * len(columns))})",
            tuple(data.get(column) for column in columns),
        )
        conn.commit()
        return True, "个人险种例外已保存"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def create_arrangement(data: Dict[str, Any]) -> Tuple[bool, str]:
    conn = _get_db_connection()
    try:
        start_date = str(data["start_date"])
        end_date = data.get("actual_end_date") or "9999-12-31"
        overlap = conn.execute(
            """
            SELECT arrangement_id FROM employee_arrangements
            WHERE emp_id = ? AND status NOT IN ('cancelled', '已取消')
              AND date(start_date) <= date(?)
              AND date(COALESCE(actual_end_date, '9999-12-31')) >= date(?)
            LIMIT 1
            """,
            (str(data["emp_id"]), str(end_date), start_date),
        ).fetchone()
        if overlap:
            return False, f"该员工与关系记录 #{overlap['arrangement_id']} 的有效期重叠"

        columns = [
            "emp_id", "arrangement_type", "contract_entity_code",
            "payroll_entity_code", "home_dept_id", "actual_work_unit_code",
            "related_branch_code", "accounting_entity_code",
            "ultimate_cost_bearer_code", "start_date", "planned_end_date",
            "actual_end_date", "payroll_included", "settlement_mode",
            "settlement_cycle", "status", "source_document_no", "remarks",
        ]
        conn.execute(
            f"INSERT INTO employee_arrangements ({','.join(columns)}) "
            f"VALUES ({','.join(['?'] * len(columns))})",
            tuple(data.get(column) for column in columns),
        )
        conn.commit()
        return True, "用工与结算关系已保存"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def close_arrangement(
    arrangement_id: int,
    actual_end_date: date,
    status: str,
    remarks: str = "",
) -> Tuple[bool, str]:
    conn = _get_db_connection()
    try:
        conn.execute(
            """
            UPDATE employee_arrangements
            SET actual_end_date = ?, status = ?,
                remarks = CASE WHEN ? = '' THEN remarks ELSE ? END,
                updated_at = CURRENT_TIMESTAMP
            WHERE arrangement_id = ?
            """,
            (str(actual_end_date), status, remarks, remarks, int(arrangement_id)),
        )
        conn.commit()
        return True, "关系已结束，历史月份保持不变"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def seed_proxy_arrangements() -> Tuple[int, str]:
    """把现有挂靠状态转成有历史起点的关系记录，不改变旧参保配置。"""
    conn = _get_db_connection()
    inserted = 0
    try:
        rows = conn.execute(
            """
            SELECT e.emp_id, e.dept_id, e.join_company_date,
                   COALESCE(m.cost_center, '本级') AS cost_center,
                   MIN(s.cost_month) AS first_ss_month
            FROM employees e
            LEFT JOIN ss_emp_matrix m ON e.emp_id = m.emp_id
            LEFT JOIN ss_monthly_records s ON e.emp_id = s.emp_id
            WHERE e.status = '挂靠人员'
              AND NOT EXISTS (
                  SELECT 1 FROM employee_arrangements a WHERE a.emp_id = e.emp_id
              )
            GROUP BY e.emp_id, e.dept_id, e.join_company_date, m.cost_center
            """
        ).fetchall()
        for row in rows:
            branch_code = entity_code_from_name(conn, row["cost_center"])
            start_date = (
                f"{row['first_ss_month']}-01"
                if row["first_ss_month"]
                else (row["join_company_date"] or "1900-01-01")
            )
            conn.execute(
                """
                INSERT INTO employee_arrangements(
                    emp_id, arrangement_type, home_dept_id,
                    actual_work_unit_code, related_branch_code,
                    ultimate_cost_bearer_code, start_date,
                    payroll_included, settlement_mode, settlement_cycle,
                    status, remarks
                ) VALUES (?, 'proxy_social', ?, ?, ?, ?, ?, 0,
                          'proxy_social', 'quarterly', 'active', ?)
                """,
                (
                    row["emp_id"], row["dept_id"], branch_code, branch_code,
                    branch_code, start_date, "由现有挂靠人员与社保矩阵兼容迁移",
                ),
            )
            inserted += 1
        conn.commit()
        return inserted, f"已迁移 {inserted} 名挂靠代缴人员"
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def backfill_relationship_snapshots() -> Dict[str, int]:
    """为旧月度账补关系快照和险种明细；金额完全取旧账，不重新计算。"""
    conn = _get_db_connection()
    counts = {"social_records": 0, "social_items": 0, "payroll_records": 0, "ledger_records": 0}
    try:
        matrix_rows = conn.execute("SELECT * FROM ss_emp_matrix").fetchall()
        matrix_by_emp = {str(row["emp_id"]): dict(row) for row in matrix_rows}
        social_rows = conn.execute(
            "SELECT * FROM ss_monthly_records ORDER BY cost_month, emp_id"
        ).fetchall()
        item_columns = {
            "pension": ("pension_comp", "pension_pers", "pension_route", "pension_enabled", "pension_account"),
            "medical": ("medical_comp", "medical_pers", "medical_route", "medical_enabled", "medical_account"),
            "medical_serious": (None, "medical_serious_pers", "medical_route", "medical_enabled", "medical_account"),
            "unemp": ("unemp_comp", "unemp_pers", "unemp_route", "unemp_enabled", "unemp_account"),
            "injury": ("injury_comp", None, "injury_route", "injury_enabled", "injury_account"),
            "maternity": ("maternity_comp", None, "maternity_route", "maternity_enabled", "maternity_account"),
            "fund": ("fund_comp", "fund_pers", "fund_route", "fund_enabled", "fund_account"),
            "annuity": ("annuity_comp", "annuity_pers", "annuity_route", "annuity_enabled", "annuity_account"),
        }

        for source in social_rows:
            row = dict(source)
            emp_id = str(row["emp_id"])
            month = str(row["cost_month"])
            arrangement = get_effective_arrangement(emp_id, month, conn)
            conn.execute(
                """
                UPDATE ss_monthly_records
                SET arrangement_id = ?, business_type_snapshot = ?
                WHERE record_id = ?
                """,
                (
                    arrangement.get("arrangement_id"),
                    arrangement.get("arrangement_type", "normal"),
                    row["record_id"],
                ),
            )
            counts["social_records"] += 1
            matrix = matrix_by_emp.get(emp_id, {})

            for item, (company_col, personal_col, route_col, enabled_col, account_col) in item_columns.items():
                source_item = "medical" if item == "medical_serious" else item
                legacy_payer = row.get(route_col) or matrix.get(account_col) or "省公众"
                context = resolve_social_route(
                    emp_id,
                    source_item,
                    month,
                    legacy_enabled=int(matrix.get(enabled_col, 1) or 0),
                    legacy_payer_name=str(legacy_payer),
                    legacy_cost_center=str(row.get("cost_center") or matrix.get("cost_center") or "本级"),
                    conn=conn,
                )
                raw_base = (
                    matrix.get("fund_base_avg") or matrix.get("base_salary_avg") or 0.0
                    if item == "fund"
                    else matrix.get("base_salary_avg") or 0.0
                )
                company_amount = float(row.get(company_col) or 0.0) if company_col else 0.0
                personal_amount = float(row.get(personal_col) or 0.0) if personal_col else 0.0
                cursor = conn.execute(
                    """
                    INSERT INTO social_monthly_items(
                        item_record_id, monthly_record_id, cost_month, emp_id,
                        arrangement_id, business_type_snapshot, insurance_item,
                        base_amount, company_amount, personal_amount,
                        calculation_policy_entity, payer_entity_code, cost_bearer_code,
                        settlement_counterparty_code, settlement_mode, settlement_cycle,
                        amount_source, payment_channel_code, route_policy_id, override_id,
                        close_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(cost_month, emp_id, insurance_item) DO UPDATE SET
                        monthly_record_id=excluded.monthly_record_id,
                        arrangement_id=excluded.arrangement_id,
                        business_type_snapshot=excluded.business_type_snapshot,
                        base_amount=excluded.base_amount,
                        company_amount=excluded.company_amount,
                        personal_amount=excluded.personal_amount,
                        calculation_policy_entity=excluded.calculation_policy_entity,
                        payer_entity_code=excluded.payer_entity_code,
                        cost_bearer_code=excluded.cost_bearer_code,
                        settlement_counterparty_code=excluded.settlement_counterparty_code,
                        settlement_mode=excluded.settlement_mode,
                        settlement_cycle=excluded.settlement_cycle,
                        amount_source=excluded.amount_source,
                        payment_channel_code=excluded.payment_channel_code,
                        route_policy_id=excluded.route_policy_id,
                        override_id=excluded.override_id
                    """,
                    (
                        f"{row['record_id']}_{item}", row["record_id"], month, emp_id,
                        context.get("arrangement_id"), context.get("arrangement_type", "normal"),
                        item, float(raw_base or 0.0), company_amount, personal_amount,
                        context.get("calculation_policy_entity"), context.get("payer_entity_code"),
                        context.get("cost_bearer_code"), context.get("settlement_counterparty_code"),
                        context.get("settlement_mode") or "none", context.get("settlement_cycle") or "none",
                        "historical_snapshot", context.get("payment_channel_code"),
                        context.get("route_policy_id"), context.get("override_id"),
                        row.get("close_status") or "draft",
                    ),
                )
                counts["social_items"] += max(cursor.rowcount, 0)

        payroll_rows = conn.execute(
            "SELECT record_id, cost_month, emp_id FROM payroll_monthly_records"
        ).fetchall()
        for row in payroll_rows:
            arrangement = get_effective_arrangement(str(row["emp_id"]), str(row["cost_month"]), conn)
            conn.execute(
                """
                UPDATE payroll_monthly_records
                SET arrangement_id = ?, business_type_snapshot = ?, payroll_entity_code = ?,
                    actual_work_unit_code = ?, ultimate_cost_bearer_code = ?
                WHERE record_id = ?
                """,
                (
                    arrangement.get("arrangement_id"), arrangement.get("arrangement_type", "normal"),
                    arrangement.get("payroll_entity_code"), arrangement.get("actual_work_unit_code"),
                    arrangement.get("ultimate_cost_bearer_code"), row["record_id"],
                ),
            )
            counts["payroll_records"] += 1

        ledger_rows = conn.execute(
            "SELECT record_id, cost_month, emp_id FROM labor_cost_ledger"
        ).fetchall()
        for row in ledger_rows:
            arrangement = get_effective_arrangement(str(row["emp_id"]), str(row["cost_month"]), conn)
            relation_type = arrangement.get("arrangement_type", "normal")
            if relation_type == "city_transfer":
                mode, status = "annual_labor_cost_reallocation", "pending"
            elif relation_type == "down_secondment":
                mode, status = "mixed_by_item", "pending"
            elif relation_type == "proxy_social":
                mode, status = "quarterly_social_settlement", "pending"
            else:
                mode, status = "none", "not_required"
            conn.execute(
                """
                UPDATE labor_cost_ledger
                SET arrangement_id = ?, business_type_snapshot = ?, actual_work_unit_code = ?,
                    accounting_entity_code = ?, ultimate_cost_bearer_code = ?,
                    reallocation_mode = ?, reallocation_status = ?
                WHERE record_id = ?
                """,
                (
                    arrangement.get("arrangement_id"), relation_type,
                    arrangement.get("actual_work_unit_code"), arrangement.get("accounting_entity_code"),
                    arrangement.get("ultimate_cost_bearer_code"), mode, status, row["record_id"],
                ),
            )
            counts["ledger_records"] += 1

        conn.commit()
        return counts
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def register_social_settlement_batch(
    period_start: str,
    period_end: str,
    branch_name: str,
    payee_name: str,
    total_amount: float,
) -> str:
    """登记/刷新同一期间、同一地市、同一收款主体的结算批次，避免重复出账。"""
    conn = _get_db_connection()
    try:
        branch_code = entity_code_from_name(conn, branch_name)
        payee_code = entity_code_from_name(conn, payee_name)
        raw_key = f"{period_start}|{period_end}|{branch_code}|{payee_code}"
        digest = hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:12]
        batch_id = f"SS-{period_start}-{period_end}-{digest}"

        existing = conn.execute(
            "SELECT status FROM settlement_batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if existing and existing["status"] in {"settled", "paid", "已收款"}:
            return batch_id

        conn.execute(
            """
            INSERT INTO settlement_batches(
                batch_id, business_type, settlement_cycle,
                period_start, period_end, payer_entity_code, payee_entity_code,
                related_branch_code, amount_scope, total_amount, status, generated_at
            ) VALUES (?, 'social_settlement', 'period', ?, ?, ?, ?, ?,
                      'company_and_personal', ?, 'generated', CURRENT_TIMESTAMP)
            ON CONFLICT(batch_id) DO UPDATE SET
                total_amount=excluded.total_amount,
                status='generated',
                generated_at=CURRENT_TIMESTAMP
            """,
            (
                batch_id, period_start, period_end, branch_code, payee_code,
                branch_code, float(total_amount or 0.0),
            ),
        )

        item_rows = conn.execute(
            """
            SELECT item_record_id, emp_id, insurance_item,
                   company_amount + personal_amount AS amount
            FROM social_monthly_items
            WHERE cost_month BETWEEN ? AND ?
              AND cost_bearer_code = ?
              AND payer_entity_code = ?
              AND settlement_mode IN ('proxy_social', 'annual_reimbursement', 'central_chargeback')
            """,
            (period_start, period_end, branch_code, payee_code),
        ).fetchall()
        for item in item_rows:
            conn.execute(
                """
                INSERT INTO settlement_batch_items(
                    batch_id, source_type, source_record_id, emp_id, item_name, amount
                ) VALUES (?, 'social_monthly_item', ?, ?, ?, ?)
                ON CONFLICT(batch_id, source_type, source_record_id) DO UPDATE SET
                    amount=excluded.amount,
                    item_name=excluded.item_name
                """,
                (
                    batch_id, item["item_record_id"], item["emp_id"],
                    item["insurance_item"], float(item["amount"] or 0.0),
                ),
            )
        conn.commit()
        return batch_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_settlement_batches_dataframe() -> pd.DataFrame:
    conn = _get_db_connection()
    try:
        return pd.read_sql_query(
            """
            SELECT b.*, payer.entity_name AS payer_name,
                   payee.entity_name AS payee_name,
                   branch.entity_name AS branch_name
            FROM settlement_batches b
            LEFT JOIN business_entities payer ON b.payer_entity_code = payer.entity_code
            LEFT JOIN business_entities payee ON b.payee_entity_code = payee.entity_code
            LEFT JOIN business_entities branch ON b.related_branch_code = branch.entity_code
            ORDER BY b.period_end DESC, b.generated_at DESC
            """,
            conn,
        )
    finally:
        conn.close()


def update_settlement_batch_status(
    batch_id: str,
    status: str,
    settled_amount: float = 0.0,
    voucher_no: str = "",
) -> Tuple[bool, str]:
    conn = _get_db_connection()
    try:
        conn.execute(
            """
            UPDATE settlement_batches
            SET status = ?, settled_amount = ?, voucher_no = ?,
                settled_at = CASE WHEN ? IN ('settled', 'paid', '已收款')
                                  THEN CURRENT_TIMESTAMP ELSE settled_at END
            WHERE batch_id = ?
            """,
            (status, float(settled_amount or 0.0), voucher_no, status, batch_id),
        )
        conn.commit()
        return True, "结算批次状态已更新"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()
