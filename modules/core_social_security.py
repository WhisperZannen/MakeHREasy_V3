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
    db_path = os.path.join(project_root, 'database', 'hr_core.db')
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
            cursor.execute("DROP TABLE IF EXISTS ss_policy_rules")
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
def get_policy_rules(year: str, entity: str) -> dict:
    conn = _get_db_connection()
    try:
        query = "SELECT * FROM ss_policy_rules WHERE rule_year = ? AND manage_entity = ?"
        df = pd.read_sql_query(query, conn, params=[year, entity])
        if not df.empty:
            return df.iloc[0].to_dict()
        else:
            return {}
    except Exception as e:
        print(f"读取规则失败: {e}")
        return {}
    finally:
        conn.close()

# ------------------------------------------------------------------------------
# 业务接口 2: 规则写入引擎
# ------------------------------------------------------------------------------
def upsert_policy_rules(params_tuple: tuple, is_all_entities: bool = False) -> tuple:
    entities = ["省公众", "中电数智", "省公司"] if is_all_entities else [params_tuple[1]]
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        sql = """
            INSERT INTO ss_policy_rules (
                rule_year, manage_entity, rounding_mode, fund_calc_method, medical_serious_fix,
                pension_upper, pension_lower, pension_comp_rate, pension_pers_rate,
                medical_upper, medical_lower, medical_comp_rate, medical_pers_rate,
                unemp_upper, unemp_lower, unemp_comp_rate, unemp_pers_rate,
                injury_upper, injury_lower, injury_comp_rate,
                maternity_upper, maternity_lower, maternity_comp_rate,
                fund_upper, fund_lower, fund_comp_rate, fund_pers_rate,
                annuity_comp_rate, annuity_pers_rate, fund_soe_upper, fund_soe_lower
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(rule_year, manage_entity) DO UPDATE SET
                rounding_mode=excluded.rounding_mode, fund_calc_method=excluded.fund_calc_method, medical_serious_fix=excluded.medical_serious_fix,
                pension_upper=excluded.pension_upper, pension_lower=excluded.pension_lower, pension_comp_rate=excluded.pension_comp_rate, pension_pers_rate=excluded.pension_pers_rate,
                medical_upper=excluded.medical_upper, medical_lower=excluded.medical_lower, medical_comp_rate=excluded.medical_comp_rate, medical_pers_rate=excluded.medical_pers_rate,
                unemp_upper=excluded.unemp_upper, unemp_lower=excluded.unemp_lower, unemp_comp_rate=excluded.unemp_comp_rate, unemp_pers_rate=excluded.unemp_pers_rate,
                injury_upper=excluded.injury_upper, injury_lower=excluded.injury_lower, injury_comp_rate=excluded.injury_comp_rate,
                maternity_upper=excluded.maternity_upper, maternity_lower=excluded.maternity_lower, maternity_comp_rate=excluded.maternity_comp_rate,
                fund_upper=excluded.fund_upper, fund_lower=excluded.fund_lower, fund_comp_rate=excluded.fund_comp_rate, fund_pers_rate=excluded.fund_pers_rate,
                annuity_comp_rate=excluded.annuity_comp_rate, annuity_pers_rate=excluded.annuity_pers_rate,
                fund_soe_upper=excluded.fund_soe_upper, fund_soe_lower=excluded.fund_soe_lower
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

def apply_rounding(value: float, mode: str) -> float:
    if mode == 'exact':
        return round(value, 2)
    elif mode == 'round_to_yuan':
        return float(round(value))
    elif mode == 'round_to_ten':
        return float(round(value / 10.0) * 10)
    elif mode == 'floor_to_ten':
        return float(math.floor(value / 10.0) * 10)
    return round(value, 2)

def calc_insurance_item(item_type: str, raw_base: float, upper: float, lower: float,
                        comp_rate: float, pers_rate: float, round_mode: str,
                        fund_method: str = 'independent', soe_upper: float = 0.0, soe_lower: float = 0.0):

    effective_upper = soe_upper if (item_type == 'fund' and soe_upper > 0) else upper
    effective_lower = soe_lower if (item_type == 'fund' and soe_lower > 0) else lower

    actual_base = raw_base
    is_capped = False

    if effective_upper > 0 and raw_base >= effective_upper:
        actual_base = effective_upper
        is_capped = True
    elif effective_lower > 0 and raw_base <= effective_lower:
        actual_base = effective_lower

    if item_type != 'fund':
        actual_base = apply_rounding(actual_base, round_mode)
        comp_amount = round(actual_base * comp_rate, 2)
        pers_amount = round(actual_base * pers_rate, 2)
    else:
        if fund_method == 'reverse_from_ss' and not is_capped:
            raw_comp = actual_base * comp_rate
            raw_pers = actual_base * pers_rate
            comp_amount = float(round(raw_comp / 10.0) * 10)
            pers_amount = float(round(raw_pers / 10.0) * 10)
        else:
            comp_amount = float(round(actual_base * comp_rate))
            pers_amount = float(round(actual_base * pers_rate))

    return actual_base, comp_amount, pers_amount

# ------------------------------------------------------------------------------
# 核心算子 3: 五险两金全量计算引擎 (终极拨乱反正版)
# ------------------------------------------------------------------------------
def calculate_complete_bill(emp_row: dict, target_year: str) -> dict:
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

    # [核心防护] 提前初始化大病医疗字段，防止后端报 KeyError
    res['medical_serious_个'] = 0.0

    for item, (base_col, en_col, acc_col) in items_config.items():
        # 如果个人未开启参保开关，强制全部归零
        if int(emp_row.get(en_col, 0)) == 0:
            res[f'{item}_企'] = res[f'{item}_个'] = 0.0
            res[f'{item}_route'] = '不参保'
            continue

        route_entity = emp_row.get(acc_col, "省公众")
        res[f'{item}_route'] = route_entity
        rules = get_policy_rules(target_year, route_entity)
        if not rules:
            res[f'{item}_企'] = res[f'{item}_个'] = 0.0
            continue

        raw_base = emp_row[base_col]
        if item == 'fund' and raw_base == 0:
            raw_base = emp_row['已录入原始基数']

        _, c_amt, p_amt = calc_insurance_item(
            item, raw_base,
            rules.get(f'{item}_upper', 0), rules.get(f'{item}_lower', 0),
            rules.get(f'{item}_comp_rate', 0), rules.get(f'{item}_pers_rate', 0),
            rules.get('rounding_mode', 'round_to_yuan'),
            rules.get('fund_calc_method', 'independent'),
            rules.get('fund_soe_upper', 0),
            rules.get('fund_soe_lower', 0)
        )

        # [核心解毒] 大病医疗 7 块钱绝对独立出来，绝不再塞进基本医疗里！
        if item == 'medical':
            serious_fix = rules.get('medical_serious_fix', 7.0)
            res['medical_serious_个'] = serious_fix
            total_pers += serious_fix  # 单独加到合计中，保证底账平齐

        res[f'{item}_企'] = c_amt
        res[f'{item}_个'] = p_amt
        total_comp += c_amt
        total_pers += p_amt

    res['合计企业缴纳'] = total_comp
    res['合计个人扣款'] = total_pers
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
        cursor.execute("DELETE FROM ss_monthly_records WHERE cost_month = ?", (month,))

        sql = """
            INSERT INTO ss_monthly_records (
                record_id, cost_month, emp_id, cost_center,
                pension_pers, medical_pers, medical_serious_pers, unemp_pers, fund_pers, annuity_pers,
                pension_comp, medical_comp, unemp_comp, injury_comp, maternity_comp, fund_comp, annuity_comp,
                pension_route, medical_route, unemp_route, injury_route, maternity_route, fund_route, annuity_route
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        insert_data = []
        for _, row in df.iterrows():
            eid = row['工号']

            # [核心解毒] 动态抓取前置计算出来的大病金额，不参保的人这里就是 0.0，彻底告别强行写死 7.0
            serious_pers_val = safe_float(row.get('medical_serious_个', 0.0))

            insert_data.append((
                f"{month}_{eid}", month, eid, row.get('财务归属', '本级'),
                safe_float(row.get('pension_个')), safe_float(row.get('medical_个')), serious_pers_val,
                safe_float(row.get('unemp_个')), safe_float(row.get('fund_个')), safe_float(row.get('annuity_个')),

                safe_float(row.get('pension_企')), safe_float(row.get('medical_企')), safe_float(row.get('unemp_企')),
                safe_float(row.get('injury_企')), safe_float(row.get('maternity_企')), safe_float(row.get('fund_企')), safe_float(row.get('annuity_企')),

                row.get('pension_route', ''), row.get('medical_route', ''), row.get('unemp_route', ''),
                row.get('injury_route', ''), row.get('maternity_route', ''), row.get('fund_route', ''), row.get('annuity_route', '')
            ))

        cursor.executemany(sql, insert_data)
        conn.commit()
        return True, f"✅ {month} 月份共 {len(insert_data)} 条核算记录（彻底斩断双重计费）已成功固化入库！"
    except Exception as e:
        conn.rollback()
        return False, f"❌ 保存失败: {e}"
    finally:
        conn.close()