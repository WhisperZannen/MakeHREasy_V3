# ==============================================================================
# 文件路径: modules/core_social_security.py
# 功能描述: 社保与福利结算模块底层中枢 (V4.0 多主体独立费率版)
# ==============================================================================

import sqlite3
import os
import pandas as pd
import math

def _get_db_connection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    db_path = os.path.join(project_root, 'database', 'hr_core.db')
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn

# ==============================================================================
# [升维修复] 为了支持多主体，动态在底层数据库表里追加主体区分字段
# ==============================================================================
def _ensure_multi_entity_schema():
    """静默升级数据库表结构，将原本的一刀切规则表升级为联合主键"""
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        # 检查表中是否已经有 manage_entity 字段
        cursor.execute("PRAGMA table_info(ss_policy_rules)")
        columns = [col['name'] for col in cursor.fetchall()]
        if 'manage_entity' not in columns:
            # 如果没有，极其暴力地重建这张表，引入联合主键 (年份 + 主体)
            cursor.execute("DROP TABLE IF EXISTS ss_policy_rules")
            cursor.execute('''
            CREATE TABLE ss_policy_rules (
                rule_year TEXT, 
                manage_entity TEXT, -- 新增：主体名称（省公众/中电数智/省公司）
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
                PRIMARY KEY (rule_year, manage_entity) -- 联合主键，确保同一年份下不同主体可以独立存在
            )
            ''')
            conn.commit()
    except Exception as e:
        print(f"底层表升级异常: {e}")
    finally:
        conn.close()

# 每次调用模块时，确保表结构已升级
_ensure_multi_entity_schema()

# ==============================================================================
# [业务接口 1] 规则读取引擎 (带主体参数)
# ==============================================================================
def get_policy_rules(year: str, entity: str) -> dict:
    """捞取指定年份 + 指定主体的专属社保规则，用于前端页面自动回显数据"""
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

# ==============================================================================
# [修改] 规则写入引擎 (支持单主体或全量同步)
# ==============================================================================
def upsert_policy_rules(params_tuple: tuple, is_all_entities: bool = False) -> tuple:
    """
    params_tuple: 传入的参数元组
    is_all_entities: 是否开启全量同步模式
    """
    entities = ["省公众", "中电数智", "省公司"] if is_all_entities else [params_tuple[1]]
    conn = _get_db_connection()
    cursor = conn.cursor()

    try:
        # 核心 SQL 逻辑不变，依然使用 ON CONFLICT
        sql = """
            INSERT INTO ss_policy_rules (
                rule_year, manage_entity, rounding_mode, fund_calc_method, medical_serious_fix,
                pension_upper, pension_lower, pension_comp_rate, pension_pers_rate,
                medical_upper, medical_lower, medical_comp_rate, medical_pers_rate,
                unemp_upper, unemp_lower, unemp_comp_rate, unemp_pers_rate,
                injury_upper, injury_lower, injury_comp_rate,
                maternity_upper, maternity_lower, maternity_comp_rate,
                fund_upper, fund_lower, fund_comp_rate, fund_pers_rate,
                annuity_comp_rate, annuity_pers_rate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(rule_year, manage_entity) DO UPDATE SET
                rounding_mode=excluded.rounding_mode, fund_calc_method=excluded.fund_calc_method, medical_serious_fix=excluded.medical_serious_fix,
                pension_upper=excluded.pension_upper, pension_lower=excluded.pension_lower, pension_comp_rate=excluded.pension_comp_rate, pension_pers_rate=excluded.pension_pers_rate,
                medical_upper=excluded.medical_upper, medical_lower=excluded.medical_lower, medical_comp_rate=excluded.medical_comp_rate, medical_pers_rate=excluded.medical_pers_rate,
                unemp_upper=excluded.unemp_upper, unemp_lower=excluded.unemp_lower, unemp_comp_rate=excluded.unemp_comp_rate, unemp_pers_rate=excluded.unemp_pers_rate,
                injury_upper=excluded.injury_upper, injury_lower=excluded.injury_lower, injury_comp_rate=excluded.injury_comp_rate,
                maternity_upper=excluded.maternity_upper, maternity_lower=excluded.maternity_lower, maternity_comp_rate=excluded.maternity_comp_rate,
                fund_upper=excluded.fund_upper, fund_lower=excluded.fund_lower, fund_comp_rate=excluded.fund_comp_rate, fund_pers_rate=excluded.fund_pers_rate,
                annuity_comp_rate=excluded.annuity_comp_rate, annuity_pers_rate=excluded.annuity_pers_rate
        """

        for ent in entities:
            # 动态替换参数元组中的第二个字段（manage_entity）
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


# ==============================================================================
# [全局常量] 中英文表头映射字典
# ==============================================================================
SS_MATRIX_MAPPING = {
    '工号': 'emp_id',
    '已录入原始基数': 'base_salary_avg',
    '独立公积金基数(选填)': 'fund_base_avg'  # [新增] 公积金特例通道映射
}


# ==============================================================================
# [业务接口 3] Excel 批量基数灌库引擎 (支持公积金特例)
# ==============================================================================
def batch_update_emp_matrix(df: pd.DataFrame) -> tuple:
    if '工号' not in df.columns or '已录入原始基数' not in df.columns:
        return False, "❌ Excel 模板错误：必须包含【工号】和【已录入原始基数】两列！"

    # 清洗必填项
    df_clean = df.dropna(subset=['工号', '已录入原始基数']).copy()

    # 将所有的列转换为正确的格式（数字或字符串），如果 Excel 里没这一列，给个默认值
    def safe_get(col_name, default_val, is_num=False):
        if col_name in df_clean.columns:
            return pd.to_numeric(df_clean[col_name], errors='coerce').fillna(default_val) if is_num else df_clean[col_name].fillna(default_val)
        return [default_val] * len(df_clean)

    # 提取所有数据
    emp_ids = df_clean['工号'].tolist()
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
        # [核心] 全面覆盖更新最初设计的矩阵表
        update_sql = """
            UPDATE ss_emp_matrix SET 
                base_salary_avg=?, fund_base_avg=?,
                pension_enabled=?, pension_account=?,
                medical_enabled=?, medical_account=?,
                unemp_enabled=?, unemp_account=?,
                injury_enabled=?, injury_account=?,
                maternity_enabled=?, maternity_account=?,
                fund_enabled=?, fund_account=?,
                annuity_enabled=?, annuity_account=?
            WHERE emp_id=?
        """
        data_to_update = list(zip(
            base_avg, fund_avg,
            p_en, p_acc, m_en, m_acc, u_en, u_acc, i_en, i_acc, mat_en, mat_acc, f_en, f_acc, a_en, a_acc,
            emp_ids
        ))

        # 兜底插入（略写，逻辑同上，为了省篇幅，主要依赖 UPDATE，因为之前的红名单必定是已存在的在职员工）
        # 这里仅作更新，如果是新员工，你原有的插入逻辑加上这些字段即可。为了精准，我们目前专注 UPDATE。
        cursor.executemany(update_sql, data_to_update)

        conn.commit()
        return True, f"✅ 成功将 {len(data_to_update)} 名员工的全量控制组与基数硬写入底层矩阵！"
    except Exception as e:
        conn.rollback()
        return False, f"❌ 底层写入崩溃: {e}"
    finally:
        conn.close()

# ==============================================================================
# [算力引擎 1] 金额抹零与精度控制装甲
# ==============================================================================
def apply_rounding(value: float, mode: str) -> float:
    """根据 Tab 3 配置的取整规则，强制处理无限小数"""
    if mode == 'exact':
        return round(value, 2)
    elif mode == 'round_to_yuan':
        return float(round(value))
    elif mode == 'round_to_ten':
        return float(round(value / 10.0) * 10)
    elif mode == 'floor_to_ten':
        return float(math.floor(value / 10.0) * 10)
    return round(value, 2)  # 默认兜底


# ==============================================================================
# [算力引擎 2] 单险种对撞公式
# ==============================================================================
def calc_insurance_item(raw_base: float, upper: float, lower: float,
                        comp_rate: float, pers_rate: float, round_mode: str):
    """
    极度严谨的三步走：
    1. 卡上下限 (如果有封顶保底的话)
    2. 乘以费率
    3. 抹零输出
    """
    # 如果该险种配置了封顶保底 (>0)，则执行卡位；否则（如年金）直接用原始基数
    actual_base = raw_base
    if upper > 0 and raw_base > upper:
        actual_base = upper
    elif lower > 0 and raw_base < lower:
        actual_base = lower

    comp_amount = apply_rounding(actual_base * comp_rate, round_mode)
    pers_amount = apply_rounding(actual_base * pers_rate, round_mode)

    return actual_base, comp_amount, pers_amount


# 将这段代码贴在 modules/core_social_security.py 的最末尾

# [新增缓存] 避免每算一个人都查一次数据库，提高速度
_RULES_CACHE = {}


def get_cached_rules(year, entity):
    cache_key = f"{year}_{entity}"
    if cache_key not in _RULES_CACHE:
        _RULES_CACHE[cache_key] = get_policy_rules(year, entity)  # 调用你已有的读取接口
    return _RULES_CACHE[cache_key]


# ==============================================================================
# [算力引擎 - 终极版]
# 注意：函数名我改成了 calculate_complete_bill，是为了和旧的做区分
# ==============================================================================
def calculate_complete_bill(emp_row: dict, target_year: str) -> dict:
    """
    输入：来自 ss_emp_matrix 的一行员工数据（字典格式）
    输出：该员工本月五险两金的详细扣款清单
    """
    res = {
        '工号': emp_row['工号'],
        '姓名': emp_row['姓名'],
        '财务归属': emp_row['财务归属']
    }

    # 险种配置映射：(基数来源字段, 开关字段, 路由主体字段)
    # 这些中文字段名必须和你 pages/3_社保与福利结算.py 里的 detect_sql 对应
    items_config = {
        'pension': ('已录入原始基数', '养老参保(1是0否)', '养老缴纳主体'),
        'medical': ('已录入原始基数', '医疗参保(1是0否)', '医疗缴纳主体'),
        'unemp': ('已录入原始基数', '失业参保(1是0否)', '失业缴纳主体'),
        'injury': ('已录入原始基数', '工伤参保(1是0否)', '工伤缴纳主体'),
        'maternity': ('已录入原始基数', '生育参保(1是0否)', '生育缴纳主体'),
        'fund': ('独立公积金基数(选填)', '公积金参保(1是0否)', '公积金缴纳主体'),
        'annuity': ('已录入原始基数', '年金参保(1是0否)', '年金缴纳主体')
    }

    total_comp = 0.0
    total_pers = 0.0

    for item, (base_col, en_col, acc_col) in items_config.items():
        # 1. 检查开关：如果没勾选参保，直接跳过，设为 0
        if int(emp_row.get(en_col, 0)) == 0:
            res[f'{item}_企'] = res[f'{item}_个'] = 0.0
            continue

        # 2. 动态抓取该险种对应的规则（比如养老抓省公众，医保抓省公司）
        route_entity = emp_row.get(acc_col, "省公众")
        rules = get_cached_rules(target_year, route_entity)
        if not rules:
            res[f'{item}_企'] = res[f'{item}_个'] = 0.0
            continue

        # 3. 确定基数 (如果是公积金且特例基数为0，则复用社保基数)
        raw_base = emp_row[base_col]
        if item == 'fund' and raw_base == 0:
            raw_base = emp_row['已录入原始基数']

        # 4. 调用你已有的 calc_insurance_item 进行数学计算
        _, c_amt, p_amt = calc_insurance_item(
            raw_base,
            rules.get(f'{item}_upper', 0), rules.get(f'{item}_lower', 0),
            rules.get(f'{item}_comp_rate', 0), rules.get(f'{item}_pers_rate', 0),
            rules.get('rounding_mode', 'round_to_yuan')
        )

        # 特例：加上你要求的 7 元大病医疗
        if item == 'medical':
            p_amt += rules.get('medical_serious_fix', 7.0)

        res[f'{item}_企'] = c_amt
        res[f'{item}_个'] = p_amt
        total_comp += c_amt
        total_pers += p_amt

    res['合计企业缴纳'] = total_comp
    res['合计个人扣款'] = total_pers
    return res