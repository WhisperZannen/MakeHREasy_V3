# ==============================================================================
# 文件路径: modules/core_social_security.py
# 功能描述: 社保与福利结算模块底层中枢 (V4.3 终极实战交付版)
# 核心修正说明:
#   1. 彻底消灭了内存缓存 (_RULES_CACHE)，强制实时查库，修改即刻生效。
#   2. 修正了抹零逻辑，强制作用于“基数”而非“结果”，彻底符合中国财务准则。
#   3. 入库逻辑 (save_monthly_ss_records) 已满血扩容，完整保存 7 大险种的路由主体。
# ==============================================================================

import sqlite3
import os
import pandas as pd
import math

# ------------------------------------------------------------------------------
# 数据库连接池初始化
# ------------------------------------------------------------------------------
def _get_db_connection():
    # 获取当前文件的绝对路径，向上推一层找到 database 目录，防止相对路径报错
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    db_path = os.path.join(project_root, 'database', 'hr_core.db')
    # 连接 SQLite 数据库
    conn = sqlite3.connect(db_path)
    # 强制开启外键约束验证，防止产生孤立数据
    conn.execute("PRAGMA foreign_keys = ON;")
    # 将查询结果配置为字典模式，方便按列名取值
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
            # 如果是老古董表，直接重建
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
            # [热更新] 如果表已经在运行了，强行追加两个国企特色字段，不影响旧数据
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
# 业务接口 1: 规则读取引擎 (已彻底废除缓存，每次调用直接读底层库)
# ------------------------------------------------------------------------------
def get_policy_rules(year: str, entity: str) -> dict:
    """根据年份和主体名称，从数据库中提取社保费率与上下限规则"""
    conn = _get_db_connection()
    try:
        # 严格执行 SQL 参数化查询，防止注入
        query = "SELECT * FROM ss_policy_rules WHERE rule_year = ? AND manage_entity = ?"
        df = pd.read_sql_query(query, conn, params=[year, entity])
        # 如果查到数据，将其转换为字典格式返回；否则返回空字典
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
# 业务接口 2: 规则写入引擎 (支持全量同步与单主体特例配置)
# ------------------------------------------------------------------------------
def upsert_policy_rules(params_tuple: tuple, is_all_entities: bool = False) -> tuple:
    entities = ["省公众", "中电数智", "省公司"] if is_all_entities else [params_tuple[1]]
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        # SQL 扩容为 31 个参数
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
# 业务接口 3: 人员参保状态与基数批量灌库引擎 (极度精简版)
# ------------------------------------------------------------------------------
def batch_update_emp_matrix(df: pd.DataFrame) -> tuple:
    """接收 Excel 数据，解析并 UPSERT 写入社保人员基因矩阵表"""
    # 前置安全校验，缺失这两列直接驳回
    if '工号' not in df.columns or '已录入原始基数' not in df.columns:
        return False, "❌ Excel 模板错误：必须包含【工号】和【已录入原始基数】两列！"

    # 清除没有工号或没有基数的空行脏数据
    df_clean = df.dropna(subset=['工号', '已录入原始基数']).copy()

    # 内部辅助函数：安全提取列数据，如果缺失则填入默认值
    def safe_get(col_name, default_val, is_num=False):
        if col_name in df_clean.columns:
            return pd.to_numeric(df_clean[col_name], errors='coerce').fillna(default_val) if is_num else df_clean[col_name].fillna(default_val)
        return [default_val] * len(df_clean)

    # 批量提取各类字段并转换为列表
    emp_ids = df_clean['工号'].tolist()
    c_center = safe_get('财务归属', '本级').tolist()
    base_avg = safe_get('已录入原始基数', 0.0, True).tolist()
    fund_avg = safe_get('独立公积金基数(选填)', 0.0, True).tolist()

    # 提取 5 险 2 金的参保开关与路由账户设定
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
        # 使用 SQLite 特有的 UPSERT 进行强力写入，完美兼容新员工和老员工
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
        # 利用 zip 打包生成可供批量执行的数据矩阵
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

# ------------------------------------------------------------------------------
# 核心算子 1: 绝对精度控制（抹零辅助函数）
# ------------------------------------------------------------------------------
def apply_rounding(value: float, mode: str) -> float:
    """根据财务规则，对小数进行处理"""
    if mode == 'exact':
        return round(value, 2)
    elif mode == 'round_to_yuan':
        return float(round(value))
    elif mode == 'round_to_ten':
        return float(round(value / 10.0) * 10)
    elif mode == 'floor_to_ten':
        return float(math.floor(value / 10.0) * 10)
    return round(value, 2)

# ------------------------------------------------------------------------------
# 核心算子 2: 单险种推演公式 (引入公积金双轨制与“封顶豁免”特例)
# ------------------------------------------------------------------------------
def calc_insurance_item(item_type: str, raw_base: float, upper: float, lower: float,
                        comp_rate: float, pers_rate: float, round_mode: str,
                        fund_method: str = 'independent', soe_upper: float = 0.0, soe_lower: float = 0.0):

    # [核心隔离区]：如果当前算是公积金，且配置了国企特色执行线，优先截断使用企业线！
    effective_upper = soe_upper if (item_type == 'fund' and soe_upper > 0) else upper
    effective_lower = soe_lower if (item_type == 'fund' and soe_lower > 0) else lower

    actual_base = raw_base
    is_capped = False  # [新增探针] 记录这个人是否触碰了封顶线

    if effective_upper > 0 and raw_base >= effective_upper:
        actual_base = effective_upper
        is_capped = True  # 打上触顶标记！
    elif effective_lower > 0 and raw_base <= effective_lower:
        actual_base = effective_lower

    if item_type != 'fund':
        actual_base = apply_rounding(actual_base, round_mode)
        comp_amount = round(actual_base * comp_rate, 2)
        pers_amount = round(actual_base * pers_rate, 2)
    else:
        # 【神级业务逻辑】如果这个人触碰了封顶线，强制豁免“倒推法”，直接精准直算防违规！
        if fund_method == 'reverse_from_ss' and not is_capped:
            # 没触顶的普通员工，继续倒推（取十位整数）
            raw_comp = actual_base * comp_rate
            raw_pers = actual_base * pers_rate
            comp_amount = float(round(raw_comp / 10.0) * 10)
            pers_amount = float(round(raw_pers / 10.0) * 10)
        else:
            # 触顶人员（或选了独立核算的人）：直接 34550 * 费率，四舍五入到元（完美避开 4150）
            comp_amount = float(round(actual_base * comp_rate))
            pers_amount = float(round(actual_base * pers_rate))

    return actual_base, comp_amount, pers_amount

# ------------------------------------------------------------------------------
# 核心算子 3: 五险两金全量计算引擎
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

    for item, (base_col, en_col, acc_col) in items_config.items():
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

        # 将企业内部执行线传给底层核心算子
        _, c_amt, p_amt = calc_insurance_item(
            item, raw_base,
            rules.get(f'{item}_upper', 0), rules.get(f'{item}_lower', 0),
            rules.get(f'{item}_comp_rate', 0), rules.get(f'{item}_pers_rate', 0),
            rules.get('rounding_mode', 'round_to_yuan'),
            rules.get('fund_calc_method', 'independent'),
            rules.get('fund_soe_upper', 0), # 传递国企执行封顶
            rules.get('fund_soe_lower', 0)  # 传递国企执行保底
        )

        if item == 'medical':
            p_amt += rules.get('medical_serious_fix', 7.0)

        res[f'{item}_企'] = c_amt
        res[f'{item}_个'] = p_amt
        total_comp += c_amt
        total_pers += p_amt

    res['合计企业缴纳'] = total_comp
    res['合计个人扣款'] = total_pers
    return res

# ------------------------------------------------------------------------------
# 业务接口 4: 月度核算账单持久化引擎 (包含 24 项维度的极度严谨防爆入库)
# ------------------------------------------------------------------------------
def save_monthly_ss_records(df: pd.DataFrame, month: str) -> tuple:
    """
    负责将内存中经过前端清洗展示的 temp_bills 数据，以 SQL 级别物理持久化。
    """
    if df.empty:
        return False, "❌ 没有可保存的数据！"

    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        # 第一道防线：清空该月该系统的所有历史旧账，保障可无限次重复计算核对
        cursor.execute("DELETE FROM ss_monthly_records WHERE cost_month = ?", (month,))

        # 核心持久化 SQL。此处不仅存金额，更是加入了 route（多主体去向路由）的全面落盘！
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
            # 将 DataFrame 的中文属性提取并映射到底层 SQL 字段
            insert_data.append((
                f"{month}_{eid}", month, eid, row['财务归属'],
                row['pension_个'], row['medical_个'], 7.0, row['unemp_个'], row['fund_个'], row['annuity_个'],
                row['pension_企'], row['medical_企'], row['unemp_企'], row['injury_企'], row['maternity_企'], row['fund_企'], row['annuity_企'],
                row.get('pension_route', ''), row.get('medical_route', ''), row.get('unemp_route', ''),
                row.get('injury_route', ''), row.get('maternity_route', ''), row.get('fund_route', ''), row.get('annuity_route', '')
            ))

        cursor.executemany(sql, insert_data)
        conn.commit()
        return True, f"✅ {month} 月份共 {len(insert_data)} 条核算记录（含多主体拆分路由）已成功固化入库！"
    except Exception as e:
        conn.rollback()
        return False, f"❌ 保存失败: {e}"
    finally:
        conn.close()