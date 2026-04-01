# ==============================================================================
# 文件路径: modules/core_labor_cost.py
# 功能描述: 人工成本台账的底层算力与数据库交互核心 (MVC架构 - Model层)
# ==============================================================================

import sqlite3
import os
import pandas as pd

# ------------------------------------------------------------------------------
# 核心字典：负责前端中文表头与底层英文列名的双向翻译
# ------------------------------------------------------------------------------
LEDGER_MAP = {
    '核算月份': 'cost_month', '工号': 'emp_id', '姓名': 'emp_name', '归属部门': 'dept_name', '人员状态': 'emp_status',
    '岗位工资': 'base_salary', '工龄工资': 'seniority_pay', '综合补贴': 'comp_subsidy', '岗位绩效浮动补贴': 'perf_float_subsidy',
    '通讯费': 'telecom_subsidy', '其他岗位工资': 'other_base_pay', '实习补贴': 'intern_subsidy', '高校毕业生/专家津贴': 'grad_allowance',
    '绩效工资标准(参考)': 'perf_standard', 'KPI得分(参考)': 'kpi_score', '考核绩效': 'eval_perf_pay', '提成绩效': 'commission_pay',
    '其他月度绩效': 'other_month_perf', '专项奖(含考勤扣罚)': 'special_award', '年终绩效奖': 'year_end_bonus', '其他专项奖': 'other_special_award',
    '工资应发合计': 'gross_salary_total',

    # [核心修正] 明确备注合并口径，底层依然只占用 medical_personal 一个坑位
    '养老保险-个人': 'pension_personal', '医疗保险-个人(含大病)': 'medical_personal',
    '失业保险-个人': 'unemployment_personal', '住房公积金-个人': 'provident_fund_personal', '企业年金-个人': 'annuity_personal',

    '个税-日常': 'tax_personal_month', '个税-年终奖': 'tax_personal_bonus', '个人实发': 'net_salary',
    '养老保险-企业': 'pension_company', '医疗保险-企业': 'medical_company', '失业保险-企业': 'unemployment_company',
    '工伤保险-企业': 'work_injury_company', '生育保险-企业': 'maternity_company', '住房公积金-企业': 'provident_fund_company',
    '企业年金-企业': 'annuity_company', '日常用餐': 'meal_daily', '加班用餐': 'meal_ot', '员工慰问费': 'welfare_condolence',
    '独生子女补贴': 'welfare_single_child', '员工体检费': 'welfare_health_check', '入职体检': 'welfare_entry_check',
    '其他福利': 'welfare_other', '防暑降温费': 'allowance_heat', '女工劳保费': 'allowance_women', '补充医保费': 'medical_supplement',
    '工会经费': 'union_funds', '职工教育经费': 'edu_funds', '经费尾差微调': 'cost_adjustment',
    '其他人工成本合计': 'other_cost_total', '人工成本合计': 'total_labor_cost'
}

DB_TO_CN_MAP = {v: k for k, v in LEDGER_MAP.items()}
NUMERIC_COLS = list(LEDGER_MAP.keys())[5:]

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
# 脏数据静默清洗引擎
# ------------------------------------------------------------------------------
def cleanse_db_timestamps():
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE labor_cost_ledger SET cost_month = substr(cost_month, 1, 7) WHERE length(cost_month) > 7")
        conn.commit()
    except Exception: pass
    finally: conn.close()

# ------------------------------------------------------------------------------
# 基础提取引擎
# ------------------------------------------------------------------------------
def get_ledger_data(month_filter=None, dept_filter=None):
    conn = _get_db_connection()
    try:
        query = "SELECT * FROM labor_cost_ledger WHERE 1=1"
        params = []
        if month_filter:
            query += " AND cost_month = ?"
            params.append(month_filter)
        if dept_filter:
            placeholders = ",".join(['?'] * len(dept_filter))
            query += f" AND dept_name IN ({placeholders})"
            params.extend(dept_filter)
        query += " ORDER BY cost_month DESC, dept_name ASC"
        df = pd.read_sql_query(query, conn, params=params)
        return df
    finally:
        conn.close()

# ------------------------------------------------------------------------------
# 全局基础排序引擎
# ------------------------------------------------------------------------------
def sort_flat_ledger_df(df):
    if df.empty: return df
    conn = _get_db_connection()
    try:
        dept_df = pd.read_sql_query("SELECT dept_name, sort_order FROM departments", conn)
        dept_weights = dict(zip(dept_df['dept_name'], dept_df['sort_order']))

        pos_df = pd.read_sql_query("SELECT pos_id, sort_order FROM positions", conn)
        pos_weights = dict(zip(pos_df['pos_id'], pos_df['sort_order']))

        emp_df = pd.read_sql_query("SELECT emp_id, post_rank FROM employees", conn)
        ep_df = pd.read_sql_query("SELECT emp_id, pos_id FROM employee_profiles", conn)

        personnel_meta = {}
        emp_merged = pd.merge(emp_df, ep_df, on='emp_id', how='left')
        for _, r in emp_merged.iterrows():
            eid = str(r['emp_id'])
            p_rank = float(r['post_rank']) if pd.notna(r['post_rank']) else 9999.0
            p_weight = pos_weights.get(r['pos_id'], 9999)
            personnel_meta[eid] = {'pos_weight': p_weight, 'rank_order': p_rank}
    except Exception:
        dept_weights, personnel_meta = {}, {}
    finally:
        conn.close()

    def get_combined_sorter(row):
        emp_id = str(row.get('工号', row.get('emp_id', '')))
        emp_status = str(row.get('人员状态', row.get('emp_status', '')))
        dept_name = str(row.get('归属部门', row.get('dept_name', '')))

        if emp_status == '退休' or '离退休' in dept_name: dept_block_weight = 9999
        elif emp_status == '公共账目' or '统筹' in dept_name or '公共' in dept_name: dept_block_weight = 9998
        else: dept_block_weight = dept_weights.get(dept_name, 999)

        meta = personnel_meta.get(emp_id, {})
        return (dept_block_weight, meta.get('pos_weight', 9999), meta.get('rank_order', 9999.0), emp_id)

    df['__sort_tuple__'] = df.apply(get_combined_sorter, axis=1)
    df[['__dw__', '__pw__', '__rw__', '__id__']] = pd.DataFrame(df['__sort_tuple__'].tolist(), index=df.index)
    df = df.sort_values(by=['__dw__', '__pw__', '__rw__', '__id__'], ascending=[True, True, True, True])
    return df.drop(columns=['__sort_tuple__', '__dw__', '__pw__', '__rw__', '__id__'])

# ------------------------------------------------------------------------------
# 财务级报表计算引擎
# ------------------------------------------------------------------------------
def add_subtotals_and_totals(df, numeric_cols):
    if df.empty: return df

    conn = _get_db_connection()
    try:
        dept_df = pd.read_sql_query("SELECT dept_name, sort_order FROM departments", conn)
        dept_weights = dict(zip(dept_df['dept_name'], dept_df['sort_order']))

        pos_df = pd.read_sql_query("SELECT pos_id, sort_order FROM positions", conn)
        pos_weights = dict(zip(pos_df['pos_id'], pos_df['sort_order']))

        emp_df = pd.read_sql_query("SELECT emp_id, post_rank FROM employees", conn)
        ep_df = pd.read_sql_query("SELECT emp_id, pos_id FROM employee_profiles", conn)

        personnel_meta = {}
        emp_merged = pd.merge(emp_df, ep_df, on='emp_id', how='left')
        for _, r in emp_merged.iterrows():
            eid = str(r['emp_id'])
            p_rank = float(r['post_rank']) if pd.notna(r['post_rank']) else 9999.0
            p_weight = pos_weights.get(r['pos_id'], 9999)
            personnel_meta[eid] = {'pos_weight': p_weight, 'rank_order': p_rank}
    except Exception:
        dept_weights, personnel_meta = {}, {}
    finally:
        conn.close()

    def get_combined_sorter(row):
        emp_id = str(row.get('工号', ''))
        emp_status = str(row.get('人员状态', ''))
        dept_name = str(row.get('归属部门', ''))

        if emp_status == '退休' or '离退休' in dept_name: dept_block_weight = 9999
        elif emp_status == '公共账目' or '统筹' in dept_name or '公共' in dept_name: dept_block_weight = 9998
        else: dept_block_weight = dept_weights.get(dept_name, 999)

        meta = personnel_meta.get(emp_id, {})
        return (dept_block_weight, meta.get('pos_weight', 9999), meta.get('rank_order', 9999.0), emp_id)

    df['__sort_tuple__'] = df.apply(get_combined_sorter, axis=1)
    df[['__dept_block__', '__pos_base__', '__pers_rank__', '__fallback_id__']] = pd.DataFrame(df['__sort_tuple__'].tolist(), index=df.index)
    df = df.sort_values(by=['__dept_block__', '__pos_base__', '__pers_rank__', '__fallback_id__'], ascending=[True, True, True, True])

    final_rows = []
    active_mask = df['__dept_block__'] < 9999
    active_df = df[active_mask]
    retired_df = df[~active_mask]
    temp_cols = ['__sort_tuple__', '__dept_block__', '__pos_base__', '__pers_rank__', '__fallback_id__']

    if not active_df.empty:
        for dept_name, group in active_df.groupby('归属部门', sort=False):
            final_rows.append(group.drop(columns=temp_cols))
            subtotal = pd.Series(index=df.columns, dtype='object')
            subtotal['归属部门'] = dept_name
            subtotal['姓名'] = '【小计】'
            for col in numeric_cols:
                if col in group.columns: subtotal[col] = group[col].sum()
            final_rows.append(pd.DataFrame([subtotal.drop(labels=temp_cols)]))

        grand_total = pd.Series(index=df.columns, dtype='object')
        grand_total['归属部门'] = '【在职及统筹部分】'
        grand_total['姓名'] = '【实际成本总计】'
        for col in numeric_cols:
            if col in active_df.columns: grand_total[col] = active_df[col].sum()
        final_rows.append(pd.DataFrame([grand_total.drop(labels=temp_cols)]))

    if not retired_df.empty:
        empty_row = pd.Series(index=df.columns.drop(temp_cols), dtype='object')
        final_rows.extend([pd.DataFrame([empty_row])] * 3)

        for dept_name, group in retired_df.groupby('归属部门', sort=False):
            final_rows.append(group.drop(columns=temp_cols))
            retire_subtotal = pd.Series(index=df.columns, dtype='object')
            retire_subtotal['归属部门'] = dept_name
            retire_subtotal['姓名'] = '【小计】'
            for col in numeric_cols:
                if col in group.columns: retire_subtotal[col] = group[col].sum()
            final_rows.append(pd.DataFrame([retire_subtotal.drop(labels=temp_cols)]))

    final_df = pd.concat(final_rows, ignore_index=True)

    seq_list = []
    current_seq = 1
    for _, row in final_df.iterrows():
        name_val = str(row.get('姓名', ''))
        if pd.isna(row.get('姓名')) or '【' in name_val: seq_list.append("")
        else:
            seq_list.append(current_seq)
            current_seq += 1

    final_df.insert(0, '序号', seq_list)
    return final_df