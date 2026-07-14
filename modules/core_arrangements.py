"""多形态用工关系、社保险种路由和结算归属解析。"""

from __future__ import annotations

import calendar
import hashlib
import os
import re
import sqlite3
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import pandas as pd


ARRANGEMENT_LABELS = {
    "normal": "普通在职",
    "proxy_social": "挂靠代缴",
    "city_transfer": "地市正式转入",
    "down_secondment": "下沉人员",
}

SPECIAL_ARRANGEMENT_TYPES = {
    key: value for key, value in ARRANGEMENT_LABELS.items() if key != "normal"
}

PAYER_RULE_LABELS = {
    "legacy": "沿用原参保配置",
    "fixed": "固定单位",
    "contract_entity": "劳动合同主体",
    "payroll_entity": "工资发放主体",
    "actual_work_unit": "实际工作单位",
    "related_branch": "关联地市",
}

COST_BEARER_RULE_LABELS = {
    "legacy": "沿用原成本归属",
    "fixed": "固定单位",
    "accounting_entity": "当前记账单位",
    "ultimate_cost_bearer": "最终成本承担单位",
    "related_branch": "关联地市",
}

SETTLEMENT_MODE_LABELS = {
    "none": "无需结算",
    "proxy_social": "挂靠代缴结算",
    "central_chargeback": "集中缴费后内部结算",
    "annual_reimbursement": "年度费用结算",
    "local_direct": "属地直接缴纳",
    "record_only": "只记录、不收付款",
    "annual_labor_cost_reallocation": "年度全口径人工成本划转",
    "mixed_by_item": "按项目分别结算",
}

SETTLEMENT_CYCLE_LABELS = {
    "none": "不结算",
    "monthly": "每月",
    "quarterly": "每季度",
    "annual": "每年",
    "mixed": "按不同项目分别处理",
}

AMOUNT_SOURCE_LABELS = {
    "system_calculated": "系统按政策计算",
    "external_actual": "外部实缴金额",
    "manual_confirmed": "人工确认金额",
}

ACTIVE_LABELS = {1: "启用", 0: "停用"}
ENABLED_LABELS = {1: "参保", 0: "不参保", None: "沿用上级配置"}

ARRANGEMENT_STATUS_LABELS = {
    "active": "执行中",
    "returned": "已返回",
    "transferred": "已正式转入",
    "extended_replaced": "已延期并建立新关系",
    "closed": "已结束",
    "cancelled": "已取消",
}

ARRANGEMENT_CLOSE_RESULT_LABELS = {
    "returned": "返回原单位",
    "transferred": "正式转入当前单位",
    "extended_replaced": "延期并另建新关系",
    "closed": "其他原因结束",
}

SETTLEMENT_BATCH_STATUS_LABELS = {
    "draft": "草稿",
    "generated": "已生成",
    "sent": "已发送",
    "confirmed": "对方已确认",
    "paid": "已到账",
    "settled": "已结清",
}

REALLOCATION_MODE_LABELS = {
    "none": "无需划转",
    "annual_labor_cost_reallocation": "年度全口径人工成本划转",
    "mixed_by_item": "按项目分别结算",
    "quarterly_social_settlement": "季度社保代缴结算",
}

REALLOCATION_STATUS_LABELS = {
    "not_required": "无需划转",
    "pending": "待划转",
    "generated": "已生成",
    "submitted": "已报送",
    "confirmed": "已确认",
    "settled": "已结算",
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

# 人员页面只展示实际需要逐人判断的七项。大病医疗跟随基本医疗，
# 补充医疗暂由人工成本/福利模块承接，避免给用户制造一个尚未落地的假开关。
PERSON_TREATMENT_ITEMS = [
    "pension", "medical", "unemp", "injury", "maternity", "fund", "annuity"
]


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
        branch_code = None if is_proxy else "province_public"
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
        "labor_cost_included": 0 if is_proxy else 1,
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
        legacy_cost_code = (
            "province_public"
            if str(legacy_cost_center or "").strip() in {"", "本级", "省公众"}
            else entity_code_from_name(conn, legacy_cost_center)
        )

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

        # 简化规则允许把“关联地市/原单位”作为动态办理方或成本方。
        # 当办理方和成本方不一致而规则未写死结算对象时，自动取真正需要
        # 往来的单位，避免生成只有金额、没有对方单位的结算记录。
        if (
            not context.get("settlement_counterparty_code")
            and context.get("payer_entity_code")
            and context.get("cost_bearer_code")
            and context["payer_entity_code"] != context["cost_bearer_code"]
        ):
            if context["payer_entity_code"] in {"province_public", "province_company"}:
                context["settlement_counterparty_code"] = context["cost_bearer_code"]
            else:
                context["settlement_counterparty_code"] = context["payer_entity_code"]

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


def is_labor_cost_included(emp_id: str, target_month: str, conn=None) -> bool:
    """判断某人某月是否进入本单位人工成本主账。"""
    arrangement = get_effective_arrangement(emp_id, target_month, conn)
    default_value = 0 if arrangement.get("arrangement_type") in {
        "proxy_social", "down_secondment"
    } else 1
    return bool(int(arrangement.get("labor_cost_included", default_value)))


def _previous_month(month: str) -> str:
    parsed = datetime.strptime(f"{month}-01", "%Y-%m-%d").date()
    return (parsed - timedelta(days=1)).strftime("%Y-%m")


def _derive_settlement(
    payer_code: Optional[str],
    cost_bearer_code: Optional[str],
    arrangement_type: str,
) -> Tuple[Optional[str], str, str]:
    """把业务人员能理解的办理/成本选择翻译成系统结算字段。"""
    if not payer_code or not cost_bearer_code or payer_code == cost_bearer_code:
        return None, "none", "none"
    if payer_code == "province_public":
        cycle = "quarterly" if arrangement_type == "proxy_social" else "annual"
        return cost_bearer_code, "proxy_social", cycle
    if cost_bearer_code == "province_public":
        return payer_code, "central_chargeback", "monthly"
    return payer_code, "mixed_by_item", "mixed"


def get_person_treatment_dataframe(emp_id: str, target_month: str) -> pd.DataFrame:
    """返回人员页面使用的中文待遇办理结果，不暴露底层规则编码。"""
    conn = _get_db_connection()
    try:
        matrix_row = conn.execute(
            "SELECT * FROM ss_emp_matrix WHERE emp_id = ?", (str(emp_id),)
        ).fetchone()
        matrix = dict(matrix_row) if matrix_row else {}
        arrangement = get_effective_arrangement(str(emp_id), target_month, conn)
        matrix_columns = {
            "pension": ("pension_enabled", "pension_account", "省公众"),
            "medical": ("medical_enabled", "medical_account", "省公司"),
            "unemp": ("unemp_enabled", "unemp_account", "省公众"),
            "injury": ("injury_enabled", "injury_account", "省公司"),
            "maternity": ("maternity_enabled", "maternity_account", "省公司"),
            "fund": ("fund_enabled", "fund_account", "省公众"),
            "annuity": ("annuity_enabled", "annuity_account", "省公司"),
        }
        rows = []
        for item in PERSON_TREATMENT_ITEMS:
            enabled_col, account_col, fallback_payer = matrix_columns[item]
            enabled = int(matrix.get(enabled_col, 1 if item != "annuity" else 0) or 0)
            payer_name = str(matrix.get(account_col) or fallback_payer)
            context = resolve_social_route(
                str(emp_id), item, target_month,
                legacy_enabled=enabled,
                legacy_payer_name=payer_name,
                legacy_cost_center=str(matrix.get("cost_center") or "本级"),
                conn=conn,
            )
            source = "个人特殊设置" if context.get("override_id") else (
                "同类人员规则" if context.get("route_policy_id") else "原参保设置"
            )
            payer = context.get("payer_entity_name") or payer_name
            cost = context.get("cost_bearer_name") or (
                "省公众" if arrangement.get("labor_cost_included", 1) else "其他单位"
            )
            if not int(context.get("enabled", 0)):
                result = "不缴纳"
            elif payer == "省公众" and cost == "省公众":
                result = "省公众自行缴纳，计入本单位人工成本"
            elif payer == "省公司" and cost == "省公众":
                result = "省公司集中代缴，计入本单位人工成本并向省公司结算"
            elif payer == cost:
                result = f"由{payer}直接缴纳和承担，不进入本单位人工成本"
            else:
                result = f"由{payer}缴纳，费用由{cost}承担，系统生成结算记录"
            rows.append({
                "项目": INSURANCE_LABELS[item],
                "是否缴纳": "是" if int(context.get("enabled", 0)) else "否",
                "办理单位": payer,
                "成本归属": cost,
                "系统处理结果": result,
                "规则来源": source,
                "insurance_item": item,
                "override_id": context.get("override_id"),
            })
        return pd.DataFrame(rows)
    finally:
        conn.close()


def get_people_management_dataframe(target_month: str) -> pd.DataFrame:
    """生成简洁的人员情形名单，供人员页面搜索和筛选。"""
    conn = _get_db_connection()
    try:
        people = pd.read_sql_query(
            """
            SELECT e.emp_id, e.name, e.status, e.dept_id, d.dept_name
            FROM employees e
            LEFT JOIN departments d ON e.dept_id = d.dept_id
            WHERE e.status IN ('在职', '挂靠人员')
            ORDER BY d.sort_order, e.name
            """,
            conn,
        )
        override_counts = dict(conn.execute(
            """
            SELECT emp_id, COUNT(*)
            FROM employee_social_overrides
            WHERE active = 1 AND effective_from_month <= ?
              AND (effective_to_month IS NULL OR effective_to_month >= ?)
            GROUP BY emp_id
            """,
            (target_month, target_month),
        ).fetchall())
        rows = []
        for _, person in people.iterrows():
            arrangement = get_effective_arrangement(str(person["emp_id"]), target_month, conn)
            relation_type = arrangement.get("arrangement_type", "normal")
            exception_count = int(override_counts.get(str(person["emp_id"]), 0))
            rows.append({
                **person.to_dict(),
                "arrangement_id": arrangement.get("arrangement_id"),
                "arrangement_type": relation_type,
                "人员情形": ARRANGEMENT_LABELS.get(relation_type, relation_type),
                "工资处理": "本系统发放" if int(arrangement.get("payroll_included", 1)) else "其他单位发放",
                "人工成本处理": "计入本单位" if int(arrangement.get("labor_cost_included", 1)) else "不计入本单位",
                "个人例外数": exception_count,
                "特殊标记": "有单项例外" if exception_count else "",
            })
        return pd.DataFrame(rows)
    finally:
        conn.close()


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
                   be_accounting.entity_name AS accounting_entity_name,
                   be_cost.entity_name AS ultimate_cost_bearer_name
            FROM employee_arrangements a
            JOIN employees e ON a.emp_id = e.emp_id
            LEFT JOIN departments d ON a.home_dept_id = d.dept_id
            LEFT JOIN business_entities be_contract ON a.contract_entity_code = be_contract.entity_code
            LEFT JOIN business_entities be_payroll ON a.payroll_entity_code = be_payroll.entity_code
            LEFT JOIN business_entities be_work ON a.actual_work_unit_code = be_work.entity_code
            LEFT JOIN business_entities be_branch ON a.related_branch_code = be_branch.entity_code
            LEFT JOIN business_entities be_accounting ON a.accounting_entity_code = be_accounting.entity_code
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
        where = "WHERE e.active = 1" if active_only else ""
        return pd.read_sql_query(
            f"""
            SELECT e.entity_code, e.entity_name, e.entity_type,
                   e.parent_entity_code, parent.entity_name AS parent_entity_name,
                   e.active
            FROM business_entities e
            LEFT JOIN business_entities parent
              ON e.parent_entity_code = parent.entity_code
            {where}
            ORDER BY CASE e.entity_type
                         WHEN '法人' THEN 1
                         WHEN '上级单位' THEN 2
                         WHEN '地市分公司' THEN 3
                         ELSE 9
                     END,
                     e.entity_name
            """,
            conn,
        )
    finally:
        conn.close()


def create_business_entity(
    entity_name: str,
    entity_type: str,
    parent_entity_code: Optional[str] = None,
) -> Tuple[bool, str]:
    """新增可参与缴费、工作归属和结算的业务单位。"""
    name = str(entity_name or "").strip()
    allowed_types = {"法人", "上级单位", "地市分公司", "其他承接单位"}
    if not name:
        return False, "单位名称必填"
    if entity_type not in allowed_types:
        return False, "单位类型无效"

    conn = _get_db_connection()
    try:
        existing = conn.execute(
            "SELECT entity_code, active FROM business_entities WHERE entity_name = ?",
            (name,),
        ).fetchone()
        if existing:
            if int(existing["active"] or 0) == 0:
                conn.execute(
                    """
                    UPDATE business_entities
                    SET active = 1, entity_type = ?, parent_entity_code = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE entity_code = ?
                    """,
                    (entity_type, parent_entity_code, existing["entity_code"]),
                )
                conn.commit()
                return True, f"{name} 已重新启用"
            return False, f"{name} 已存在，无需重复新增"

        prefix = {
            "地市分公司": "branch:",
            "法人": "legal:",
            "上级单位": "parent:",
            "其他承接单位": "partner:",
        }[entity_type]
        entity_code = f"{prefix}{name}"
        code_owner = conn.execute(
            "SELECT entity_name FROM business_entities WHERE entity_code = ?",
            (entity_code,),
        ).fetchone()
        if code_owner:
            suffix = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
            entity_code = f"{prefix}{suffix}"

        conn.execute(
            """
            INSERT INTO business_entities(
                entity_code, entity_name, entity_type, parent_entity_code, active
            ) VALUES (?, ?, ?, ?, 1)
            """,
            (entity_code, name, entity_type, parent_entity_code),
        )
        conn.commit()
        return True, f"业务单位“{name}”已新增"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def set_business_entity_active(entity_code: str, active: bool) -> Tuple[bool, str]:
    """启用或停用单位；历史关系仍保留名称和关联。"""
    conn = _get_db_connection()
    try:
        row = conn.execute(
            "SELECT entity_name FROM business_entities WHERE entity_code = ?",
            (entity_code,),
        ).fetchone()
        if not row:
            return False, "未找到该业务单位"
        conn.execute(
            """
            UPDATE business_entities
            SET active = ?, updated_at = CURRENT_TIMESTAMP
            WHERE entity_code = ?
            """,
            (1 if active else 0, entity_code),
        )
        conn.commit()
        action = "启用" if active else "停用"
        return True, f"{row['entity_name']} 已{action}；历史记录未删除"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
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
                   ecalc.entity_name AS calculation_policy_entity_name,
                   ep.entity_name AS payer_entity_name,
                   eb.entity_name AS cost_bearer_name,
                   es.entity_name AS settlement_counterparty_name
            FROM social_route_policies p
            LEFT JOIN business_entities ec ON p.contract_entity_code = ec.entity_code
            LEFT JOIN business_entities ecalc ON p.calculation_policy_entity = ecalc.entity_code
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
        return False, "缴费主体规则选择“固定单位”时，必须指定固定缴费主体"
    if data.get("cost_bearer_rule") == "fixed" and not data.get("cost_bearer_code"):
        return False, "成本承担规则选择“固定单位”时，必须指定固定成本单位"
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


def get_normal_route_defaults(target_month: str) -> pd.DataFrame:
    """给社保页面展示普通人员在指定月份实际采用的默认办理方式。"""
    conn = _get_db_connection()
    try:
        matrix_columns = {
            "pension": "pension_account", "medical": "medical_account",
            "unemp": "unemp_account", "injury": "injury_account",
            "maternity": "maternity_account", "fund": "fund_account",
            "annuity": "annuity_account",
        }
        rows = []
        for item in PERSON_TREATMENT_ITEMS:
            policy = conn.execute(
                """
                SELECT p.*, payer.entity_name AS payer_name
                FROM social_route_policies p
                LEFT JOIN business_entities payer ON p.payer_entity_code = payer.entity_code
                WHERE p.arrangement_type = 'normal' AND p.insurance_item = ?
                  AND p.active = 1 AND p.effective_from_month <= ?
                  AND (p.effective_to_month IS NULL OR p.effective_to_month >= ?)
                ORDER BY p.priority DESC, p.effective_from_month DESC, p.route_policy_id DESC
                LIMIT 1
                """,
                (item, target_month, target_month),
            ).fetchone()
            if policy:
                payer_code = policy["payer_entity_code"]
                payer_name = policy["payer_name"] or entity_name_from_code(conn, payer_code)
                source = f"统一规则（{policy['effective_from_month']}起）"
            else:
                account_column = matrix_columns[item]
                legacy = conn.execute(
                    f"""
                    SELECT m.{account_column} AS payer_name, COUNT(*) AS people_count
                    FROM ss_emp_matrix m
                    JOIN employees e ON e.emp_id = m.emp_id
                    WHERE e.status = '在职' AND trim(COALESCE(m.{account_column}, '')) <> ''
                    GROUP BY m.{account_column}
                    ORDER BY people_count DESC
                    LIMIT 1
                    """
                ).fetchone()
                payer_name = legacy["payer_name"] if legacy else (
                    "省公司" if item in {"medical", "injury", "maternity", "annuity"}
                    else "省公众"
                )
                payer_code = entity_code_from_name(conn, payer_name)
                source = "现有人员参保设置"
            result = (
                "省公众自行缴纳"
                if payer_code == "province_public"
                else f"{payer_name}集中代缴，省公众结算"
            )
            rows.append({
                "insurance_item": item,
                "项目": INSURANCE_LABELS[item],
                "办理单位编码": payer_code,
                "办理方式": payer_name,
                "成本归属": "省公众",
                "系统处理": result,
                "规则来源": source,
            })
        return pd.DataFrame(rows)
    finally:
        conn.close()


def save_normal_route_default(
    insurance_item: str,
    payer_entity_code: str,
    effective_from_month: str,
    remarks: str = "",
) -> Tuple[bool, str]:
    """保存普通人员统一办理方式；个人例外仍保持最高优先级。"""
    if insurance_item not in PERSON_TREATMENT_ITEMS:
        return False, "待遇项目无效"
    if not payer_entity_code:
        return False, "请选择办理单位"
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", str(effective_from_month or "")):
        return False, "生效月份必须为 YYYY-MM"
    counterparty, settlement_mode, settlement_cycle = _derive_settlement(
        payer_entity_code, "province_public", "normal"
    )
    conn = _get_db_connection()
    try:
        conn.execute(
            """
            UPDATE social_route_policies
            SET active = 0, updated_at = CURRENT_TIMESTAMP
            WHERE arrangement_type = 'normal' AND insurance_item = ?
              AND active = 1 AND effective_from_month = ?
            """,
            (insurance_item, effective_from_month),
        )
        conn.execute(
            """
            UPDATE social_route_policies
            SET effective_to_month = ?, updated_at = CURRENT_TIMESTAMP
            WHERE arrangement_type = 'normal' AND insurance_item = ?
              AND active = 1 AND effective_from_month < ?
              AND (effective_to_month IS NULL OR effective_to_month >= ?)
            """,
            (
                _previous_month(effective_from_month), insurance_item,
                effective_from_month, effective_from_month,
            ),
        )
        payer_name = entity_name_from_code(conn, payer_entity_code)
        conn.execute(
            """
            INSERT INTO social_route_policies(
                policy_name, arrangement_type, insurance_item,
                effective_from_month, enabled_default,
                calculation_policy_entity, payer_entity_rule, payer_entity_code,
                cost_bearer_rule, cost_bearer_code, settlement_counterparty_code,
                settlement_mode, settlement_cycle, amount_source,
                priority, active, remarks
            ) VALUES (?, 'normal', ?, ?, NULL, ?, 'fixed', ?,
                      'fixed', 'province_public', ?, ?, ?,
                      'system_calculated', 100, 1, ?)
            """,
            (
                f"普通人员{INSURANCE_LABELS[insurance_item]}办理方式",
                insurance_item, effective_from_month, payer_entity_code,
                payer_entity_code, counterparty, settlement_mode,
                settlement_cycle, str(remarks or "").strip(),
            ),
        )
        conn.commit()
        return True, f"普通人员{INSURANCE_LABELS[insurance_item]}办理方式已保存"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


SPECIAL_DEFAULT_ARRANGEMENT_TYPES = {
    "down_secondment": "下沉人员",
    "city_transfer": "地市正式转入",
}

SPECIAL_ROUTE_UNIT_LABELS = {
    "province_public": "省公众",
    "province_company": "省公司",
    "related_branch": "关联地市/原单位",
}


def _policy_rule_display(
    conn: sqlite3.Connection,
    rule: Optional[str],
    entity_code: Optional[str],
    role: str,
) -> str:
    if rule == "related_branch":
        return "关联地市/原单位"
    if rule == "actual_work_unit":
        return "实际工作单位"
    if rule in {"fixed", None, ""} and entity_code:
        return entity_name_from_code(conn, entity_code)
    if role == "payer":
        return "沿用人员原办理方式"
    return "沿用人员原成本归属"


def get_arrangement_route_defaults(
    arrangement_type: str,
    target_month: str,
) -> pd.DataFrame:
    """展示下沉/地市转入人员在指定月份采用的统一待遇规则。"""
    if arrangement_type not in SPECIAL_DEFAULT_ARRANGEMENT_TYPES:
        return pd.DataFrame()
    conn = _get_db_connection()
    try:
        rows = []
        for item in PERSON_TREATMENT_ITEMS:
            policy = conn.execute(
                """
                SELECT * FROM social_route_policies
                WHERE arrangement_type = ? AND insurance_item = ? AND active = 1
                  AND effective_from_month <= ?
                  AND (effective_to_month IS NULL OR effective_to_month >= ?)
                ORDER BY priority DESC, effective_from_month DESC, route_policy_id DESC
                LIMIT 1
                """,
                (arrangement_type, item, target_month, target_month),
            ).fetchone()
            if not policy:
                continue
            data = dict(policy)
            payer = _policy_rule_display(
                conn, data.get("payer_entity_rule"), data.get("payer_entity_code"), "payer"
            )
            cost = _policy_rule_display(
                conn, data.get("cost_bearer_rule"), data.get("cost_bearer_code"), "cost"
            )
            if not int(data.get("enabled_default") if data.get("enabled_default") is not None else 1):
                result = "默认不办理"
            elif payer == cost:
                result = f"由{payer}办理并承担"
            elif cost == "省公众":
                result = f"由{payer}办理，计入本单位人工成本并生成结算记录"
            else:
                result = f"由{payer}办理，费用由{cost}承担，不进入本单位人工成本"
            payer_choice = (
                data.get("payer_entity_code")
                if data.get("payer_entity_rule") == "fixed"
                else data.get("payer_entity_rule")
            )
            rows.append({
                "项目": INSURANCE_LABELS[item],
                "默认办理": "是" if int(data.get("enabled_default") if data.get("enabled_default") is not None else 1) else "否",
                "办理单位": payer,
                "成本归属": cost,
                "系统处理": result,
                "生效月份": data.get("effective_from_month"),
                "说明": data.get("remarks") or "",
                "insurance_item": item,
                "payer_choice": payer_choice,
                "include_company_cost": cost == "省公众",
            })
        return pd.DataFrame(rows)
    finally:
        conn.close()


def save_arrangement_route_default(
    arrangement_type: str,
    insurance_item: str,
    enabled: bool,
    payer_choice: str,
    include_company_cost: bool,
    effective_from_month: str,
    remarks: str = "",
) -> Tuple[bool, str]:
    """保存一类特殊人员的一项默认规则，个人例外仍保持最高优先级。"""
    if arrangement_type not in SPECIAL_DEFAULT_ARRANGEMENT_TYPES:
        return False, "请选择下沉人员或地市正式转入"
    if insurance_item not in PERSON_TREATMENT_ITEMS:
        return False, "待遇项目无效"
    if payer_choice not in SPECIAL_ROUTE_UNIT_LABELS:
        return False, "办理单位无效"
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", str(effective_from_month or "")):
        return False, "生效月份必须为 YYYY-MM"

    payer_rule = "related_branch" if payer_choice == "related_branch" else "fixed"
    payer_code = None if payer_rule == "related_branch" else payer_choice
    cost_rule = "fixed" if include_company_cost else "related_branch"
    cost_code = "province_public" if include_company_cost else None
    calculation_entity = (
        "province_company" if payer_choice == "province_company" else "province_public"
    )
    if payer_rule == cost_rule == "related_branch":
        settlement_mode, settlement_cycle = "none", "none"
    elif include_company_cost and payer_rule == "related_branch":
        settlement_mode, settlement_cycle = "annual_labor_cost_reallocation", "annual"
    elif not include_company_cost and payer_choice == "province_public":
        settlement_mode, settlement_cycle = "annual_reimbursement", "annual"
    elif not include_company_cost and payer_choice == "province_company":
        settlement_mode, settlement_cycle = "mixed_by_item", "annual"
    else:
        settlement_mode, settlement_cycle = "none", "none"

    conn = _get_db_connection()
    try:
        conn.execute(
            """
            UPDATE social_route_policies
            SET active = 0, updated_at = CURRENT_TIMESTAMP
            WHERE arrangement_type = ? AND insurance_item = ?
              AND active = 1 AND effective_from_month = ?
            """,
            (arrangement_type, insurance_item, effective_from_month),
        )
        conn.execute(
            """
            UPDATE social_route_policies
            SET effective_to_month = ?, updated_at = CURRENT_TIMESTAMP
            WHERE arrangement_type = ? AND insurance_item = ? AND active = 1
              AND effective_from_month < ?
              AND (effective_to_month IS NULL OR effective_to_month >= ?)
            """,
            (
                _previous_month(effective_from_month), arrangement_type,
                insurance_item, effective_from_month, effective_from_month,
            ),
        )
        next_policy = conn.execute(
            """
            SELECT effective_from_month FROM social_route_policies
            WHERE arrangement_type = ? AND insurance_item = ? AND active = 1
              AND effective_from_month > ?
            ORDER BY effective_from_month ASC LIMIT 1
            """,
            (arrangement_type, insurance_item, effective_from_month),
        ).fetchone()
        effective_to = (
            _previous_month(next_policy["effective_from_month"]) if next_policy else None
        )
        conn.execute(
            """
            INSERT INTO social_route_policies(
                policy_name, arrangement_type, insurance_item,
                effective_from_month, effective_to_month, enabled_default,
                calculation_policy_entity, payer_entity_rule, payer_entity_code,
                cost_bearer_rule, cost_bearer_code,
                settlement_mode, settlement_cycle, amount_source,
                priority, active, remarks
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      'system_calculated', 200, 1, ?)
            """,
            (
                f"{SPECIAL_DEFAULT_ARRANGEMENT_TYPES[arrangement_type]}{INSURANCE_LABELS[insurance_item]}默认规则",
                arrangement_type, insurance_item, effective_from_month, effective_to,
                1 if enabled else 0, calculation_entity, payer_rule, payer_code,
                cost_rule, cost_code, settlement_mode, settlement_cycle,
                str(remarks or "").strip(),
            ),
        )
        conn.commit()
        return True, (
            f"{SPECIAL_DEFAULT_ARRANGEMENT_TYPES[arrangement_type]}的"
            f"{INSURANCE_LABELS[insurance_item]}默认规则已保存"
        )
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
                   ecalc.entity_name AS calculation_policy_entity_name,
                   ep.entity_name AS payer_entity_name,
                   eb.entity_name AS cost_bearer_name,
                   es.entity_name AS settlement_counterparty_name
            FROM employee_social_overrides o
            JOIN employees e ON o.emp_id = e.emp_id
            LEFT JOIN business_entities ecalc ON o.calculation_policy_entity = ecalc.entity_code
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


def save_person_social_override(
    emp_id: str,
    insurance_item: str,
    effective_from_month: str,
    enabled: bool,
    payer_entity_code: str,
    include_in_company_cost: bool,
    external_cost_bearer_code: Optional[str],
    special_reason: str,
    effective_to_month: Optional[str] = None,
    source_document_no: str = "",
) -> Tuple[bool, str]:
    """人员页面的简化保存接口：自动推导成本、结算对象和结算周期。"""
    if insurance_item not in PERSON_TREATMENT_ITEMS:
        return False, "请选择有效的待遇项目"
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", str(effective_from_month or "")):
        return False, "生效月份必须为 YYYY-MM"
    if effective_to_month and not re.fullmatch(
        r"\d{4}-(0[1-9]|1[0-2])", str(effective_to_month)
    ):
        return False, "失效月份必须为 YYYY-MM"
    if effective_to_month and effective_to_month < effective_from_month:
        return False, "失效月份不能早于生效月份"
    if not payer_entity_code:
        return False, "请选择办理单位"
    if not str(special_reason or "").strip():
        return False, "请填写特殊原因"

    cost_bearer_code = (
        "province_public" if include_in_company_cost else external_cost_bearer_code
    )
    if not cost_bearer_code:
        return False, "不计入本单位人工成本时，必须选择实际承担费用的单位"

    conn = _get_db_connection()
    try:
        arrangement = get_effective_arrangement(str(emp_id), effective_from_month, conn)
        arrangement_type = arrangement.get("arrangement_type", "normal")
        counterparty, settlement_mode, settlement_cycle = _derive_settlement(
            payer_entity_code, cost_bearer_code, arrangement_type
        )

        # 同一个人同一项目的新版本自动结束旧的开放版本，历史月份仍保留。
        conn.execute(
            """
            UPDATE employee_social_overrides
            SET active = 0, updated_at = CURRENT_TIMESTAMP
            WHERE emp_id = ? AND insurance_item = ? AND active = 1
              AND effective_from_month = ?
            """,
            (str(emp_id), insurance_item, effective_from_month),
        )
        conn.execute(
            """
            UPDATE employee_social_overrides
            SET effective_to_month = ?, updated_at = CURRENT_TIMESTAMP
            WHERE emp_id = ? AND insurance_item = ? AND active = 1
              AND effective_from_month < ?
              AND (effective_to_month IS NULL OR effective_to_month >= ?)
            """,
            (
                _previous_month(effective_from_month), str(emp_id), insurance_item,
                effective_from_month, effective_from_month,
            ),
        )
        columns = [
            "emp_id", "insurance_item", "effective_from_month", "effective_to_month",
            "enabled", "calculation_policy_entity", "payer_entity_code",
            "cost_bearer_code", "settlement_counterparty_code", "settlement_mode",
            "settlement_cycle", "amount_source", "payment_channel_code",
            "special_reason", "source_document_no", "active",
        ]
        values = {
            "emp_id": str(emp_id),
            "insurance_item": insurance_item,
            "effective_from_month": effective_from_month,
            "effective_to_month": effective_to_month,
            "enabled": 1 if enabled else 0,
            "calculation_policy_entity": payer_entity_code,
            "payer_entity_code": payer_entity_code,
            "cost_bearer_code": cost_bearer_code,
            "settlement_counterparty_code": counterparty,
            "settlement_mode": settlement_mode,
            "settlement_cycle": settlement_cycle,
            "amount_source": "system_calculated",
            "payment_channel_code": None,
            "special_reason": str(special_reason).strip(),
            "source_document_no": str(source_document_no or "").strip(),
            "active": 1,
        }
        conn.execute(
            f"INSERT INTO employee_social_overrides ({','.join(columns)}) "
            f"VALUES ({','.join(['?'] * len(columns))})",
            tuple(values[column] for column in columns),
        )
        conn.commit()
        return True, "个人待遇例外已保存；旧月份规则保持不变"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def end_person_social_override(override_id: int, effective_to_month: str) -> Tuple[bool, str]:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", str(effective_to_month or "")):
        return False, "结束月份必须为 YYYY-MM"
    conn = _get_db_connection()
    try:
        row = conn.execute(
            "SELECT effective_from_month FROM employee_social_overrides WHERE override_id = ?",
            (int(override_id),),
        ).fetchone()
        if not row:
            return False, "未找到这条个人例外"
        if effective_to_month < row["effective_from_month"]:
            return False, "结束月份不能早于生效月份"
        conn.execute(
            """
            UPDATE employee_social_overrides
            SET effective_to_month = ?, updated_at = CURRENT_TIMESTAMP
            WHERE override_id = ?
            """,
            (effective_to_month, int(override_id)),
        )
        conn.commit()
        return True, "个人例外结束月份已保存"
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
            "actual_end_date", "payroll_included", "labor_cost_included", "settlement_mode",
            "settlement_cycle", "status", "source_document_no", "remarks",
        ]
        conn.execute(
            f"INSERT INTO employee_arrangements ({','.join(columns)}) "
            f"VALUES ({','.join(['?'] * len(columns))})",
            tuple(
                (
                    data.get(column)
                    if data.get(column) is not None
                    else (0 if data.get("arrangement_type") in {"proxy_social", "down_secondment"} else 1)
                )
                if column == "labor_cost_included" else data.get(column)
                for column in columns
            ),
        )
        conn.commit()
        return True, "用工与结算关系已保存"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def save_simple_arrangement(data: Dict[str, Any]) -> Tuple[bool, str]:
    """保存人员情形，页面无需理解合同主体、记账主体和结算模式等技术字段。"""
    emp_id = str(data.get("emp_id") or "").strip()
    relation_type = data.get("arrangement_type") or "normal"
    start_date = str(data.get("start_date") or "").strip()
    if relation_type not in ARRANGEMENT_LABELS:
        return False, "人员情形无效"
    try:
        start_day = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        return False, "生效日期必须为 YYYY-MM-DD"

    conn = _get_db_connection()
    try:
        employee = conn.execute(
            "SELECT emp_id, dept_id FROM employees WHERE emp_id = ?", (emp_id,)
        ).fetchone()
        if not employee:
            return False, "未找到该人员"

        current = conn.execute(
            """
            SELECT * FROM employee_arrangements
            WHERE emp_id = ? AND status = 'active'
              AND (actual_end_date IS NULL OR date(actual_end_date) >= date(?))
            ORDER BY date(start_date) DESC, arrangement_id DESC
            LIMIT 1
            """,
            (emp_id, start_date),
        ).fetchone()

        if relation_type == "normal":
            if not current:
                return True, "该人员已经按普通人员管理，无需重复设置"
            if start_day <= datetime.strptime(current["start_date"], "%Y-%m-%d").date():
                return False, "恢复普通人员的日期必须晚于当前特殊关系开始日期"
            conn.execute(
                """
                UPDATE employee_arrangements
                SET actual_end_date = ?, status = 'closed', updated_at = CURRENT_TIMESTAMP
                WHERE arrangement_id = ?
                """,
                ((start_day - timedelta(days=1)).isoformat(), current["arrangement_id"]),
            )
            conn.commit()
            return True, "特殊人员关系已结束，之后恢复普通人员规则"

        related_code = data.get("related_branch_code")
        actual_work_code = data.get("actual_work_unit_code") or related_code
        payroll_included = 1 if data.get("payroll_included") else 0
        labor_included = 1 if data.get("labor_cost_included") else 0
        if relation_type in {"proxy_social", "down_secondment", "city_transfer"} and not related_code:
            return False, "请选择关联地市或单位"
        if not labor_included and not related_code:
            return False, "不计入本单位人工成本时必须选择费用承担单位"

        defaults = {
            "proxy_social": ("proxy_social", "quarterly"),
            "down_secondment": ("mixed_by_item", "mixed"),
            "city_transfer": ("annual_labor_cost_reallocation", "annual"),
        }
        settlement_mode, settlement_cycle = defaults[relation_type]
        record = {
            "emp_id": emp_id,
            "arrangement_type": relation_type,
            "contract_entity_code": "province_public" if relation_type != "proxy_social" else None,
            "payroll_entity_code": "province_public" if payroll_included else None,
            "home_dept_id": int(employee["dept_id"]),
            "actual_work_unit_code": actual_work_code,
            "related_branch_code": related_code,
            "accounting_entity_code": "province_public",
            "ultimate_cost_bearer_code": "province_public" if labor_included else related_code,
            "start_date": start_date,
            "planned_end_date": data.get("planned_end_date"),
            "actual_end_date": None,
            "payroll_included": payroll_included,
            "labor_cost_included": labor_included,
            "settlement_mode": settlement_mode,
            "settlement_cycle": settlement_cycle,
            "status": "active",
            "source_document_no": str(data.get("source_document_no") or "").strip(),
            "remarks": str(data.get("remarks") or "").strip(),
        }

        columns = [
            "emp_id", "arrangement_type", "contract_entity_code", "payroll_entity_code",
            "home_dept_id", "actual_work_unit_code", "related_branch_code",
            "accounting_entity_code", "ultimate_cost_bearer_code", "start_date",
            "planned_end_date", "actual_end_date", "payroll_included",
            "labor_cost_included", "settlement_mode", "settlement_cycle", "status",
            "source_document_no", "remarks",
        ]
        if current and str(current["start_date"]) == start_date:
            assignments = ",".join(f"{column} = ?" for column in columns[1:])
            conn.execute(
                f"UPDATE employee_arrangements SET {assignments}, updated_at = CURRENT_TIMESTAMP "
                "WHERE arrangement_id = ?",
                tuple(record[column] for column in columns[1:]) + (current["arrangement_id"],),
            )
        else:
            if current:
                current_start = datetime.strptime(current["start_date"], "%Y-%m-%d").date()
                if start_day <= current_start:
                    return False, "新情形的生效日期必须晚于当前关系开始日期"
                conn.execute(
                    """
                    UPDATE employee_arrangements
                    SET actual_end_date = ?, status = 'closed', updated_at = CURRENT_TIMESTAMP
                    WHERE arrangement_id = ?
                    """,
                    ((start_day - timedelta(days=1)).isoformat(), current["arrangement_id"]),
                )
            conn.execute(
                f"INSERT INTO employee_arrangements ({','.join(columns)}) "
                f"VALUES ({','.join(['?'] * len(columns))})",
                tuple(record[column] for column in columns),
            )
        conn.commit()
        return True, "人员情形已保存，历史期间保持不变"
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
                    payroll_included, labor_cost_included, settlement_mode, settlement_cycle,
                    status, remarks
                ) VALUES (?, 'proxy_social', ?, ?, ?, ?, ?, 0, 0,
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
                    labor_cost_included_snapshot = ?,
                    reallocation_mode = ?, reallocation_status = ?
                WHERE record_id = ?
                """,
                (
                    arrangement.get("arrangement_id"), relation_type,
                    arrangement.get("actual_work_unit_code"), arrangement.get("accounting_entity_code"),
                    arrangement.get("ultimate_cost_bearer_code"),
                    int(arrangement.get("labor_cost_included", 1)),
                    mode, status, row["record_id"],
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
