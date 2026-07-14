# ==============================================================================
# 文件路径: modules/core_social_security.py
# 功能描述: 社保与福利结算模块底层中枢 (V4.4 彻底绞杀大病双轨制Bug)
# 核心修正说明:
#   1. 彻底斩断大病医疗的双重计费，199 与 7 绝对物理隔离。
#   2. 斩断入库时的 7.0 强行硬编码，严格根据前置开关动态入库。
# ==============================================================================

import sqlite3
import os
import pandas as pd
import math
import re
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from modules.core_arrangements import resolve_social_route

# ------------------------------------------------------------------------------
# 核心防御机制：空值清洗器 (必须在底层也配备一把，防止入库时遇到脏数据崩溃)
# ------------------------------------------------------------------------------
def safe_float(val, default=0.0):
    try:
        if pd.notna(val) and val is not None and str(val).strip() != '':
            return float(val)
        return default
    except Exception:
        return default

# ------------------------------------------------------------------------------
# 数据库连接池初始化
# ------------------------------------------------------------------------------
def _get_db_connection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    db_path = os.environ.get('MAKE_HR_DB_PATH', os.path.join(project_root, 'database', 'hr_core.db'))
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn

# ------------------------------------------------------------------------------
# 数据库表结构静默升级程序
# ------------------------------------------------------------------------------
def _ensure_multi_entity_schema():
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA table_info(ss_policy_rules)")
        columns = [col['name'] for col in cursor.fetchall()]
        if 'manage_entity' not in columns:
            # 旧版只有 rule_year 主键。先改名并完整迁移，禁止直接 DROP
            # 导致已经维护的费率参数丢失。
            cursor.execute("ALTER TABLE ss_policy_rules RENAME TO ss_policy_rules_legacy")
            cursor.execute('''
            CREATE TABLE ss_policy_rules (
                rule_year TEXT, manage_entity TEXT,
                pension_upper REAL, pension_lower REAL, pension_comp_rate REAL, pension_pers_rate REAL,
                medical_upper REAL, medical_lower REAL, medical_comp_rate REAL, medical_pers_rate REAL,
                medical_serious_fix REAL DEFAULT 7.0,
                unemp_upper REAL, unemp_lower REAL, unemp_comp_rate REAL, unemp_pers_rate REAL,
                injury_upper REAL, injury_lower REAL, injury_comp_rate REAL,
                maternity_upper REAL, maternity_lower REAL, maternity_comp_rate REAL,
                fund_upper REAL, fund_lower REAL, fund_comp_rate REAL, fund_pers_rate REAL,
                annuity_comp_rate REAL, annuity_pers_rate REAL,
                rounding_mode TEXT DEFAULT 'round_to_yuan',
                fund_calc_method TEXT DEFAULT 'reverse_from_ss',
                fund_soe_upper REAL DEFAULT 0.0,
                fund_soe_lower REAL DEFAULT 0.0,
                PRIMARY KEY (rule_year, manage_entity) 
            )
            ''')
            cursor.execute("PRAGMA table_info(ss_policy_rules_legacy)")
            legacy_columns = [col['name'] for col in cursor.fetchall()]
            copy_columns = [
                c for c in legacy_columns
                if c not in {'manage_entity'}
            ]
            select_columns = ", ".join(copy_columns)
            insert_columns = ", ".join(['manage_entity'] + copy_columns)
            for entity in ['省公众', '中电数智', '省公司']:
                cursor.execute(
                    f"INSERT INTO ss_policy_rules ({insert_columns}) "
                    f"SELECT ?, {select_columns} FROM ss_policy_rules_legacy",
                    (entity,)
                )
            cursor.execute("DROP TABLE ss_policy_rules_legacy")
        else:
            if 'fund_soe_upper' not in columns:
                cursor.execute("ALTER TABLE ss_policy_rules ADD COLUMN fund_soe_upper REAL DEFAULT 0.0")
            if 'fund_soe_lower' not in columns:
                cursor.execute("ALTER TABLE ss_policy_rules ADD COLUMN fund_soe_lower REAL DEFAULT 0.0")
        conn.commit()
    except Exception as e:
        print(f"底层表升级异常: {e}")
    finally:
        conn.close()

_ensure_multi_entity_schema()

# ------------------------------------------------------------------------------
# 业务接口 1: 规则读取引擎
# ------------------------------------------------------------------------------
def _normalize_effective_month(value: str) -> str:
    text = str(value or '').strip()
    if re.fullmatch(r'\d{4}', text):
        return f'{text}-12'
    if re.fullmatch(r'\d{4}-(0[1-9]|1[0-2])', text):
        return text
    return date.today().strftime('%Y-%m')


def get_policy_rules(period: str, entity: str) -> dict:
    """读取目标月份之前最近一次生效的规则；兼容旧代码传入年度。"""
    target_month = _normalize_effective_month(period)
    conn = _get_db_connection()
    try:
        query = """
            SELECT * FROM ss_policy_versions
            WHERE effective_from_month <= ? AND manage_entity = ?
            ORDER BY effective_from_month DESC LIMIT 1
        """
        df = pd.read_sql_query(query, conn, params=[target_month, entity])
        if not df.empty:
            return df.iloc[0].to_dict()
        # 老数据库或尚未迁移的测试库仍可使用原年度表。
        year = target_month[:4]
        legacy = pd.read_sql_query(
            "SELECT * FROM ss_policy_rules WHERE rule_year <= ? AND manage_entity = ? "
            "ORDER BY rule_year DESC LIMIT 1",
            conn, params=[year, entity],
        )
        return legacy.iloc[0].to_dict() if not legacy.empty else {}
    except Exception as e:
        print(f"读取规则失败: {e}")
        return {}
    finally:
        conn.close()

# ------------------------------------------------------------------------------
# 业务接口 2: 规则写入引擎
# ------------------------------------------------------------------------------
def upsert_policy_rules(params_tuple: tuple, is_all_entities: bool = False) -> tuple:
    """按生效月份保存新版本；旧版本和已封账月份不被覆盖。"""
    entities = ["省公众", "中电数智", "省公司"] if is_all_entities else [params_tuple[1]]
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        sql = """
            INSERT INTO ss_policy_versions (
                effective_from_month, manage_entity, rounding_mode, fund_calc_method, medical_serious_fix,
                pension_upper, pension_lower, pension_comp_rate, pension_pers_rate,
                medical_upper, medical_lower, medical_comp_rate, medical_pers_rate,
                unemp_upper, unemp_lower, unemp_comp_rate, unemp_pers_rate,
                injury_upper, injury_lower, injury_comp_rate,
                maternity_upper, maternity_lower, maternity_comp_rate,
                fund_upper, fund_lower, fund_comp_rate, fund_pers_rate,
                annuity_comp_rate, annuity_pers_rate, fund_soe_upper, fund_soe_lower,
                new_hire_fund_delay_months, annuity_requires_regularization,
                base_generation_rounding_mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(effective_from_month, manage_entity) DO UPDATE SET
                rounding_mode=excluded.rounding_mode, fund_calc_method=excluded.fund_calc_method, medical_serious_fix=excluded.medical_serious_fix,
                pension_upper=excluded.pension_upper, pension_lower=excluded.pension_lower, pension_comp_rate=excluded.pension_comp_rate, pension_pers_rate=excluded.pension_pers_rate,
                medical_upper=excluded.medical_upper, medical_lower=excluded.medical_lower, medical_comp_rate=excluded.medical_comp_rate, medical_pers_rate=excluded.medical_pers_rate,
                unemp_upper=excluded.unemp_upper, unemp_lower=excluded.unemp_lower, unemp_comp_rate=excluded.unemp_comp_rate, unemp_pers_rate=excluded.unemp_pers_rate,
                injury_upper=excluded.injury_upper, injury_lower=excluded.injury_lower, injury_comp_rate=excluded.injury_comp_rate,
                maternity_upper=excluded.maternity_upper, maternity_lower=excluded.maternity_lower, maternity_comp_rate=excluded.maternity_comp_rate,
                fund_upper=excluded.fund_upper, fund_lower=excluded.fund_lower, fund_comp_rate=excluded.fund_comp_rate, fund_pers_rate=excluded.fund_pers_rate,
                annuity_comp_rate=excluded.annuity_comp_rate, annuity_pers_rate=excluded.annuity_pers_rate,
                fund_soe_upper=excluded.fund_soe_upper, fund_soe_lower=excluded.fund_soe_lower,
                new_hire_fund_delay_months=excluded.new_hire_fund_delay_months,
                annuity_requires_regularization=excluded.annuity_requires_regularization,
                base_generation_rounding_mode=excluded.base_generation_rounding_mode,
                updated_at=CURRENT_TIMESTAMP
        """
        for ent in entities:
            current_params = list(params_tuple)
            current_params[1] = ent
            cursor.execute(sql, tuple(current_params))
        conn.commit()
        return True, f"✅ 已成功同步 {len(entities)} 个主体的全局算力规则！"
    except Exception as e:
        conn.rollback()
        return False, f"❌ 数据库写入失败: {e}"
    finally:
        conn.close()

# ------------------------------------------------------------------------------
# 业务接口 3: 人员参保状态与基数批量灌库引擎
# ------------------------------------------------------------------------------
def batch_update_emp_matrix(df: pd.DataFrame) -> tuple:
    if '工号' not in df.columns or '已录入原始基数' not in df.columns:
        return False, "❌ Excel 模板错误：必须包含【工号】和【已录入原始基数】两列！"

    df_clean = df.dropna(subset=['工号', '已录入原始基数']).copy()

    def safe_get(col_name, default_val, is_num=False):
        if col_name in df_clean.columns:
            return pd.to_numeric(df_clean[col_name], errors='coerce').fillna(default_val) if is_num else df_clean[col_name].fillna(default_val)
        return [default_val] * len(df_clean)

    emp_ids = df_clean['工号'].tolist()
    c_center = safe_get('财务归属', '本级').tolist()
    base_avg = safe_get('已录入原始基数', 0.0, True).tolist()
    fund_avg = safe_get('独立公积金基数(选填)', 0.0, True).tolist()

    p_en = safe_get('养老参保(1是0否)', 1, True).tolist()
    p_acc = safe_get('养老缴纳主体', '省公众').tolist()
    m_en = safe_get('医疗参保(1是0否)', 1, True).tolist()
    m_acc = safe_get('医疗缴纳主体', '省公司').tolist()
    u_en = safe_get('失业参保(1是0否)', 1, True).tolist()
    u_acc = safe_get('失业缴纳主体', '省公众').tolist()
    i_en = safe_get('工伤参保(1是0否)', 1, True).tolist()
    i_acc = safe_get('工伤缴纳主体', '省公众').tolist()
    mat_en = safe_get('生育参保(1是0否)', 1, True).tolist()
    mat_acc = safe_get('生育缴纳主体', '省公司').tolist()
    f_en = safe_get('公积金参保(1是0否)', 1, True).tolist()
    f_acc = safe_get('公积金缴纳主体', '省公众').tolist()
    a_en = safe_get('年金参保(1是0否)', 0, True).tolist()
    a_acc = safe_get('年金缴纳主体', '省公司').tolist()

    conn = _get_db_connection()
    cursor = conn.cursor()

    try:
        upsert_sql = """
            INSERT INTO ss_emp_matrix (
                emp_id, cost_center, base_salary_avg, fund_base_avg,
                pension_enabled, pension_account,
                medical_enabled, medical_account,
                unemp_enabled, unemp_account,
                injury_enabled, injury_account,
                maternity_enabled, maternity_account,
                fund_enabled, fund_account,
                annuity_enabled, annuity_account
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(emp_id) DO UPDATE SET
                cost_center=excluded.cost_center,
                base_salary_avg=excluded.base_salary_avg,
                fund_base_avg=excluded.fund_base_avg,
                pension_enabled=excluded.pension_enabled,
                pension_account=excluded.pension_account,
                medical_enabled=excluded.medical_enabled,
                medical_account=excluded.medical_account,
                unemp_enabled=excluded.unemp_enabled,
                unemp_account=excluded.unemp_account,
                injury_enabled=excluded.injury_enabled,
                injury_account=excluded.injury_account,
                maternity_enabled=excluded.maternity_enabled,
                maternity_account=excluded.maternity_account,
                fund_enabled=excluded.fund_enabled,
                fund_account=excluded.fund_account,
                annuity_enabled=excluded.annuity_enabled,
                annuity_account=excluded.annuity_account
        """
        data_to_update = list(zip(
            emp_ids, c_center, base_avg, fund_avg,
            p_en, p_acc, m_en, m_acc, u_en, u_acc, i_en, i_acc, mat_en, mat_acc, f_en, f_acc, a_en, a_acc
        ))

        cursor.executemany(upsert_sql, data_to_update)
        conn.commit()
        return True, f"✅ 成功将 {len(data_to_update)} 名员工的矩阵配置硬写入库！"
    except Exception as e:
        conn.rollback()
        return False, f"❌ 底层写入崩溃: {e}"
    finally:
        conn.close()


def batch_update_social_bases(df: pd.DataFrame) -> tuple:
    """简化入口只更新基数；参保项目和办理单位继续由人员规则管理。"""
    if '工号' not in df.columns or '已录入原始基数' not in df.columns:
        return False, "❌ 文件必须包含【工号】和【已录入原始基数】两列"
    clean = df.dropna(subset=['工号', '已录入原始基数']).copy()
    conn = _get_db_connection()
    try:
        count = 0
        for _, row in clean.iterrows():
            emp_id = str(row['工号']).replace('.0', '').strip()
            if not emp_id or emp_id == 'nan':
                continue
            base_value = safe_float(row.get('已录入原始基数'))
            fund_value = safe_float(row.get('独立公积金基数(选填)'))
            cost_center = str(row.get('财务归属') or '本级').strip()
            conn.execute(
                """
                INSERT INTO ss_emp_matrix(emp_id, cost_center, base_salary_avg, fund_base_avg)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(emp_id) DO UPDATE SET
                    cost_center = CASE
                        WHEN ? = '' THEN ss_emp_matrix.cost_center ELSE excluded.cost_center
                    END,
                    base_salary_avg = excluded.base_salary_avg,
                    fund_base_avg = excluded.fund_base_avg
                """,
                (emp_id, cost_center, base_value, fund_value, cost_center),
            )
            count += 1
        conn.commit()
        return True, f"✅ 已更新 {count} 人的社保及公积金基数，人员待遇规则未被改动"
    except Exception as exc:
        conn.rollback()
        return False, f"❌ 基数更新失败：{exc}"
    finally:
        conn.close()

def _round_half_up(value: float, quantum: str) -> float:
    return float(Decimal(str(value)).quantize(Decimal(quantum), rounding=ROUND_HALF_UP))


def apply_rounding(value: float, mode: str) -> float:
    if mode == 'exact':
        return _round_half_up(value, '0.01')
    elif mode == 'round_to_yuan':
        return _round_half_up(value, '1')
    elif mode == 'round_to_ten':
        return _round_half_up(Decimal(str(value)) / Decimal('10'), '1') * 10
    elif mode == 'floor_to_ten':
        return float(math.floor(value / 10.0) * 10)
    return _round_half_up(value, '0.01')

# ------------------------------------------------------------------------------
# 核心算子 2: 单个险种金额计算 (彻底废除公积金强制抹零机制)
# ------------------------------------------------------------------------------
def calc_insurance_item(item_type: str, raw_base: float, upper: float, lower: float,
                        comp_rate: float, pers_rate: float, round_mode: str,
                        fund_method: str = 'independent', soe_upper: float = 0.0, soe_lower: float = 0.0):
    if raw_base <= 0:
        return 0.0, 0.0, 0.0

    actual_base = float(raw_base)
    is_capped = False

    # 独立公积金基数是人工/外部系统核定值，不套社保基数整十规则。
    if item_type != 'fund' or fund_method != 'independent':
        actual_base = apply_rounding(actual_base, round_mode)

    # 如果是公积金，且有内部执行线，优先用内部线判定封顶保底
    if item_type == 'fund' and (soe_upper > 0 or soe_lower > 0):
        if soe_upper > 0 and actual_base >= soe_upper:
            actual_base = soe_upper
            is_capped = True
        elif soe_lower > 0 and actual_base <= soe_lower:
            actual_base = soe_lower
            is_capped = True
    else:
        if upper > 0 and actual_base >= upper:
            actual_base = upper
            is_capped = True
        elif lower > 0 and actual_base <= lower:
            actual_base = lower
            is_capped = True

    if item_type != 'fund':
        comp_amount = _round_half_up(actual_base * comp_rate, '0.01')
        pers_amount = _round_half_up(actual_base * pers_rate, '0.01')
    else:
        if fund_method == 'reverse_from_ss' and not is_capped:
            # 正常人员：先把单边缴交额按十元四舍五入，再用最终缴交额反推执行基数。
            comp_amount = apply_rounding(actual_base * comp_rate, 'round_to_ten') if comp_rate else 0.0
            pers_amount = apply_rounding(actual_base * pers_rate, 'round_to_ten') if pers_rate else 0.0
            reference_amount = comp_amount if comp_rate else pers_amount
            reference_rate = comp_rate if comp_rate else pers_rate
            actual_base = _round_half_up(reference_amount / reference_rate, '0.01') if reference_rate else 0.0
        else:
            # 独立基数及封顶/保底值严格按比例计算到分。
            comp_amount = _round_half_up(actual_base * comp_rate, '0.01')
            pers_amount = _round_half_up(actual_base * pers_rate, '0.01')

    return actual_base, comp_amount, pers_amount


def _add_months(month_text: str, months: int) -> str:
    year, month = map(int, month_text.split('-'))
    value = year * 12 + month - 1 + int(months or 0)
    return f'{value // 12:04d}-{value % 12 + 1:02d}'


def get_lifecycle_participation(emp_id: str, item: str, target_month: str, rules: dict) -> tuple:
    """返回人员生命周期是否允许参保及业务说明；个人例外在调用方优先。"""
    if item not in {'fund', 'annuity'}:
        return True, ''
    conn = _get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT e.join_company_date, p.first_employment, p.employment_stage,
                   p.actual_regularization_date
            FROM employees e
            LEFT JOIN employee_profiles p ON p.emp_id = e.emp_id
            WHERE e.emp_id = ?
            """,
            (str(emp_id),),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return True, ''
    row = dict(row)
    if item == 'fund' and int(row.get('first_employment') or 0) == 1:
        join_month = str(row.get('join_company_date') or '')[:7]
        delay = int(rules.get('new_hire_fund_delay_months', 1) or 0)
        if re.fullmatch(r'\d{4}-(0[1-9]|1[0-2])', join_month):
            start_month = _add_months(join_month, delay)
            if target_month < start_month:
                return False, f'首次就业人员公积金从{start_month}开始缴纳'
    if item == 'annuity' and int(rules.get('annuity_requires_regularization', 1) or 0) == 1:
        if str(row.get('employment_stage') or 'regular') == 'intern':
            return False, '实习期不缴企业年金，确认转正后自动启用'
        regularized = str(row.get('actual_regularization_date') or '')[:7]
        if regularized and target_month < regularized:
            return False, f'企业年金从实际转正月{regularized}开始缴纳'
    return True, ''


# ------------------------------------------------------------------------------
# 核心算子 3: 五险两金全量计算引擎 (终极修复：公积金独立基数绝对豁免版)
# ------------------------------------------------------------------------------
def calculate_complete_bill(emp_row: dict, target_year: str, target_month: str = None) -> dict:
    target_month = target_month or f"{target_year}-01"
    res = {'工号': emp_row['工号'], '姓名': emp_row['姓名'], '财务归属': emp_row['财务归属']}

    items_config = {
        'pension': ('已录入原始基数', '养老参保(1是0否)', '养老缴纳主体'),
        'medical': ('已录入原始基数', '医疗参保(1是0否)', '医疗缴纳主体'),
        'unemp': ('已录入原始基数', '失业参保(1是0否)', '失业缴纳主体'),
        'injury': ('已录入原始基数', '工伤参保(1是0否)', '工伤缴纳主体'),
        'maternity': ('已录入原始基数', '生育参保(1是0否)', '生育缴纳主体'),
        'fund': ('独立公积金基数(选填)', '公积金参保(1是0否)', '公积金缴纳主体'),
        'annuity': ('已录入原始基数', '年金参保(1是0否)', '年金缴纳主体')
    }

    total_comp, total_pers = 0.0, 0.0
    social_actual_base = 0.0
    fund_actual_base = 0.0
    participation_notes = []

    # [核心防护] 提前初始化大病医疗字段，防止后端报 KeyError
    res['medical_serious_个'] = 0.0

    for item, (base_col, en_col, acc_col) in items_config.items():
        context = resolve_social_route(
            str(emp_row['工号']),
            item,
            target_month,
            legacy_enabled=int(emp_row.get(en_col, 0) or 0),
            legacy_payer_name=str(emp_row.get(acc_col, "省公众")),
            legacy_cost_center=str(emp_row.get('财务归属', '本级')),
        )

        # 隐藏快照列只供入库使用，页面导出时会过滤掉。
        for key in [
            'arrangement_id', 'arrangement_type', 'calculation_policy_entity',
            'payer_entity_code', 'cost_bearer_code',
            'settlement_counterparty_code', 'settlement_mode',
            'settlement_cycle', 'amount_source', 'payment_channel_code',
            'route_policy_id', 'override_id'
        ]:
            res[f'__{item}_{key}'] = context.get(key)

        if context.get('cost_bearer_name'):
            res['财务归属'] = context['cost_bearer_name']

        route_entity = context.get('payer_entity_name') or emp_row.get(acc_col, "省公众")
        res[f'{item}_route'] = route_entity
        policy_entity = context.get('calculation_policy_entity') or route_entity
        rules = get_policy_rules(target_month, policy_entity)

        # 个人例外优先；没有个人例外时才应用新入职/实习期自动规则。
        lifecycle_enabled, lifecycle_reason = True, ''
        if not context.get('override_id') and rules:
            lifecycle_enabled, lifecycle_reason = get_lifecycle_participation(
                str(emp_row['工号']), item, target_month, rules
            )

        # 如果个人未开启参保，或尚未到自动启用月份，强制全部归零。
        if int(context.get('enabled', 0)) == 0 or not lifecycle_enabled:
            res[f'{item}_企'] = res[f'{item}_个'] = 0.0
            res[f'{item}_route'] = '不参保'
            res[f'__{item}_base_amount'] = 0.0
            if lifecycle_reason:
                res[f'__{item}_participation_note'] = lifecycle_reason
                participation_notes.append(lifecycle_reason)
            continue

        # 地市属地直缴等项目需要回传实缴金额，不允许套省内规则伪算。
        if context.get('amount_source') != 'system_calculated':
            res[f'{item}_企'] = res[f'{item}_个'] = 0.0
            res[f'__{item}_base_amount'] = 0.0
            continue

        if not rules:
            res[f'{item}_企'] = res[f'{item}_个'] = 0.0
            res[f'__{item}_base_amount'] = 0.0
            res[f'__{item}_calculation_error'] = f'{target_year}-{policy_entity}未配置计算规则'
            continue

        # ----------------------------------------------------------------------
        # [核心解毒] 公积金基数寻址与全局算法豁免机制
        # ----------------------------------------------------------------------
        raw_base = emp_row.get(base_col, 0.0)
        current_fund_method = rules.get('fund_calc_method', 'independent')

        if item == 'fund':
            if raw_base == 0:
                # 场景 1：没有填独立基数，借用社保原始基数，并接受全局算法的支配(如：反推逢元进十)
                raw_base = emp_row.get('已录入原始基数', 0.0)
            else:
                # 场景 2：[终极修复] 填了独立基数！立刻激活免死金牌，强行阻断全局“反推法/逢元进十”！
                # 强制降级为最原始纯粹的 'independent' 算法（严格按比例计算，只四舍五入到 1 元）
                current_fund_method = 'independent'

        actual_base, c_amt, p_amt = calc_insurance_item(
            item, raw_base,
            rules.get(f'{item}_upper', 0), rules.get(f'{item}_lower', 0),
            rules.get(f'{item}_comp_rate', 0), rules.get(f'{item}_pers_rate', 0),
            rules.get('rounding_mode', 'round_to_yuan'),
            current_fund_method,  # 传入刚才经过拦截器判定的算法模式
            rules.get('fund_soe_upper', 0),
            rules.get('fund_soe_lower', 0)
        )
        res[f'__{item}_base_amount'] = actual_base
        if item == 'fund':
            fund_actual_base = actual_base
        elif item in {'pension', 'medical', 'unemp', 'injury', 'maternity'} and social_actual_base == 0:
            social_actual_base = actual_base

        # [核心解毒] 大病医疗 7 块钱绝对独立出来，绝不再塞进基本医疗里！
        if item == 'medical':
            serious_fix = rules.get('medical_serious_fix', 7.0)
            res['medical_serious_个'] = serious_fix
            total_pers += serious_fix  # 单独加到合计中，保证底账平齐
            for key in [
                'arrangement_id', 'arrangement_type', 'calculation_policy_entity',
                'payer_entity_code', 'cost_bearer_code',
                'settlement_counterparty_code', 'settlement_mode',
                'settlement_cycle', 'amount_source', 'payment_channel_code',
                'route_policy_id', 'override_id'
            ]:
                res[f'__medical_serious_{key}'] = context.get(key)
            res['__medical_serious_base_amount'] = actual_base

        res[f'{item}_企'] = c_amt
        res[f'{item}_个'] = p_amt
        total_comp += c_amt
        total_pers += p_amt

    res['合计企业缴纳'] = total_comp
    res['合计个人扣款'] = total_pers
    res['社保执行基数'] = social_actual_base
    res['公积金执行基数'] = fund_actual_base
    res['待遇生效说明'] = '；'.join(dict.fromkeys(participation_notes))
    return res

# ------------------------------------------------------------------------------
# 业务接口 4: 月度核算账单持久化引擎 (斩断硬编码毒瘤)
# ------------------------------------------------------------------------------
def save_monthly_ss_records(df: pd.DataFrame, month: str) -> tuple:
    if df.empty:
        return False, "❌ 没有可保存的数据！"

    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        locked_count = cursor.execute(
            "SELECT COUNT(*) FROM ss_monthly_records WHERE cost_month = ? AND close_status = 'closed'",
            (month,)
        ).fetchone()[0]
        if locked_count:
            return False, f"❌ {month} 已封账，禁止覆盖。请先执行有记录的解封流程。"

        cursor.execute("DELETE FROM social_monthly_items WHERE cost_month = ?", (month,))
        cursor.execute("DELETE FROM ss_monthly_records WHERE cost_month = ?", (month,))

        sql = """
            INSERT INTO ss_monthly_records (
                record_id, cost_month, emp_id, cost_center,
                pension_pers, medical_pers, medical_serious_pers, unemp_pers, fund_pers, annuity_pers,
                pension_comp, medical_comp, unemp_comp, injury_comp, maternity_comp, fund_comp, annuity_comp,
                pension_route, medical_route, unemp_route, injury_route, maternity_route, fund_route, annuity_route,
                arrangement_id, business_type_snapshot, calculation_status, close_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        insert_data = []
        item_insert_data = []
        item_codes = ['pension', 'medical', 'medical_serious', 'unemp', 'injury', 'maternity', 'fund', 'annuity']
        for _, row in df.iterrows():
            eid = str(row['工号'])

            # [核心解毒] 动态抓取前置计算出来的大病金额，不参保的人这里就是 0.0，彻底告别强行写死 7.0
            serious_pers_val = safe_float(row.get('medical_serious_个', 0.0))

            record_id = f"{month}_{eid}"
            arrangement_id = row.get('__pension_arrangement_id')
            business_type = row.get('__pension_arrangement_type', 'normal') or 'normal'
            calculation_status = (
                'external_pending'
                if any(row.get(f'__{item}_amount_source') not in {None, '', 'system_calculated'} for item in item_codes)
                else 'calculated'
            )

            insert_data.append((
                record_id, month, eid, row.get('财务归属', '本级'),
                safe_float(row.get('pension_个')), safe_float(row.get('medical_个')), serious_pers_val,
                safe_float(row.get('unemp_个')), safe_float(row.get('fund_个')), safe_float(row.get('annuity_个')),

                safe_float(row.get('pension_企')), safe_float(row.get('medical_企')), safe_float(row.get('unemp_企')),
                safe_float(row.get('injury_企')), safe_float(row.get('maternity_企')), safe_float(row.get('fund_企')), safe_float(row.get('annuity_企')),

                row.get('pension_route', ''), row.get('medical_route', ''), row.get('unemp_route', ''),
                row.get('injury_route', ''), row.get('maternity_route', ''), row.get('fund_route', ''), row.get('annuity_route', ''),
                arrangement_id, business_type, calculation_status, 'draft'
            ))

            for item in item_codes:
                source_item = 'medical' if item == 'medical_serious' else item
                company_amount = 0.0 if item == 'medical_serious' else safe_float(row.get(f'{item}_企'))
                personal_amount = (
                    serious_pers_val if item == 'medical_serious'
                    else safe_float(row.get(f'{item}_个'))
                )
                item_insert_data.append((
                    f"{record_id}_{item}", record_id, month, eid,
                    row.get(f'__{source_item}_arrangement_id'),
                    row.get(f'__{source_item}_arrangement_type', business_type) or business_type,
                    item, safe_float(row.get(f'__{item}_base_amount')),
                    company_amount, personal_amount,
                    row.get(f'__{source_item}_calculation_policy_entity'),
                    row.get(f'__{source_item}_payer_entity_code'),
                    row.get(f'__{source_item}_cost_bearer_code'),
                    row.get(f'__{source_item}_settlement_counterparty_code'),
                    row.get(f'__{source_item}_settlement_mode', 'none') or 'none',
                    row.get(f'__{source_item}_settlement_cycle', 'none') or 'none',
                    row.get(f'__{source_item}_amount_source', 'system_calculated') or 'system_calculated',
                    row.get(f'__{source_item}_payment_channel_code'),
                    row.get(f'__{source_item}_route_policy_id'),
                    row.get(f'__{source_item}_override_id'),
                    'draft'
                ))

        cursor.executemany(sql, insert_data)
        cursor.executemany("""
            INSERT INTO social_monthly_items (
                item_record_id, monthly_record_id, cost_month, emp_id,
                arrangement_id, business_type_snapshot, insurance_item,
                base_amount, company_amount, personal_amount,
                calculation_policy_entity, payer_entity_code, cost_bearer_code,
                settlement_counterparty_code, settlement_mode, settlement_cycle,
                amount_source, payment_channel_code, route_policy_id, override_id,
                close_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, item_insert_data)
        cursor.executemany(
            """
            UPDATE ss_emp_matrix
            SET ss_base_actual = ?, fund_base_actual = ?
            WHERE emp_id = ?
            """,
            [
                (
                    safe_float(row.get('社保执行基数')),
                    safe_float(row.get('公积金执行基数')),
                    str(row['工号']),
                )
                for _, row in df.iterrows()
            ],
        )
        conn.commit()
        return True, (
            f"✅ {month} 共固化 {len(insert_data)} 人、{len(item_insert_data)} 条险种明细；"
            "旧版汇总账同步保留。"
        )
    except Exception as e:
        conn.rollback()
        return False, f"❌ 保存失败: {e}"
    finally:
        conn.close()
