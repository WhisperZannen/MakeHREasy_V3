# ==============================================================================
# 文件路径: modules/core_labor_cost.py
# 功能描述: 人工成本台账的底层算力与数据库交互核心 (MVC架构 - Model层)
# ==============================================================================

import sqlite3
import os
import io
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd

from modules.core_arrangements import (
    ARRANGEMENT_LABELS,
    REALLOCATION_MODE_LABELS,
    REALLOCATION_STATUS_LABELS,
    get_effective_arrangement,
    is_labor_cost_included,
)

# ------------------------------------------------------------------------------
# 核心字典：负责前端中文表头与底层英文列名的双向翻译
# ------------------------------------------------------------------------------
LEDGER_MAP = {
    '核算月份': 'cost_month', '工号': 'emp_id', '姓名': 'emp_name', '归属部门': 'dept_name', '人员状态': 'emp_status',
    '用工关系ID': 'arrangement_id', '业务关系类型': 'business_type_snapshot',
    '实际工作单位编码': 'actual_work_unit_code', '当前记账单位编码': 'accounting_entity_code',
    '最终成本承担单位编码': 'ultimate_cost_bearer_code', '成本划转方式': 'reallocation_mode',
    '成本划转状态': 'reallocation_status',
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
TEXT_DB_COLUMNS = {
    'cost_month', 'emp_id', 'emp_name', 'dept_name', 'emp_status',
    'arrangement_id', 'business_type_snapshot', 'actual_work_unit_code',
    'accounting_entity_code', 'ultimate_cost_bearer_code',
    'reallocation_mode', 'reallocation_status',
}
NUMERIC_COLS = [cn for cn, db in LEDGER_MAP.items() if db not in TEXT_DB_COLUMNS]

GROSS_COMPONENT_COLUMNS_CN = [
    '岗位工资', '工龄工资', '综合补贴', '岗位绩效浮动补贴', '通讯费',
    '其他岗位工资', '实习补贴', '高校毕业生/专家津贴', '考核绩效',
    '提成绩效', '其他月度绩效', '专项奖(含考勤扣罚)', '年终绩效奖',
    '其他专项奖',
]

PERSONAL_DEDUCTION_COLUMNS_CN = [
    '养老保险-个人', '医疗保险-个人(含大病)', '失业保险-个人',
    '住房公积金-个人', '企业年金-个人', '个税-日常', '个税-年终奖',
]

OTHER_COST_COMPONENT_COLUMNS_CN = [
    '养老保险-企业', '医疗保险-企业', '失业保险-企业', '工伤保险-企业',
    '生育保险-企业', '住房公积金-企业', '企业年金-企业', '日常用餐',
    '加班用餐', '员工慰问费', '独生子女补贴', '员工体检费',
    '入职体检', '其他福利', '防暑降温费', '补充医保费', '工会经费',
    '职工教育经费', '经费尾差微调',
]


# 财务总账科目与人工成本台账字段的稳定映射。
# “monthly_processing”只决定是否能自动补到人员台账，不影响核对。
FINANCE_LABOR_CONTROL_RULES = [
    {
        'key': 'gross_salary', 'label': '工资应发合计',
        'account_codes': ('6400010100',), 'ledger_columns': ('工资应发合计',),
        'monthly_processing': '核对',
        'remarks': '按费用科目借方发生额核对；研发划转属于后续会计重分类，不重复增加人工成本。',
    },
    {
        'key': 'net_salary', 'label': '个人实发（含女工劳保费）',
        'account_codes': ('2211010398',), 'ledger_columns': ('个人实发',),
        'monthly_processing': '核对',
        'remarks': '女工劳保费随工资实发，但不进入工资应发和人工成本。',
    },
    {
        'key': 'meal_daily', 'label': '工作用餐',
        'account_codes': ('6400030200',), 'ledger_columns': ('日常用餐',),
        'monthly_processing': '核对',
        'remarks': '财务表只有总额，人员分摊仍以业务明细为准。',
    },
    {
        'key': 'welfare_recuperation', 'label': '职工疗养费',
        'account_codes': ('6400030400',), 'ledger_columns': ('其他福利',),
        'ledger_scope': 'non_retired',
        'monthly_processing': '核对',
        'remarks': '财务科目在系统内映射到非退休人员的“其他福利”，不扩展人工成本主表字段。',
    },
    {
        'key': 'welfare_health_check', 'label': '员工体检费',
        'account_codes': ('6400031200',), 'ledger_columns': ('员工体检费',),
        'ledger_scope': 'non_retired',
        'monthly_processing': '核对',
        'remarks': '退休人员医药费使用单独科目，不再挤入本字段。',
    },
    {
        'key': 'medical_maternity_company', 'label': '基本医疗及生育保险（企业）',
        'account_codes': ('6400040100',),
        'ledger_columns': ('医疗保险-企业', '生育保险-企业'),
        'monthly_processing': '核对',
        'remarks': '当前财务口径把企业医疗和生育合并在基本医疗保险科目。',
    },
    {
        'key': 'medical_supplement', 'label': '补充医疗保险',
        'account_codes': ('6400040200',), 'ledger_columns': ('补充医保费',),
        'ledger_scope': 'non_retired',
        'monthly_processing': '核对',
        'remarks': '系统只汇总非退休人员和公共统筹的补充医保；退休人员金额按下一条规则核对。',
    },
    {
        'key': 'pension_company', 'label': '基本养老保险（企业）',
        'account_codes': ('6400040300',), 'ledger_columns': ('养老保险-企业',),
        'monthly_processing': '核对', 'remarks': '',
    },
    {
        'key': 'annuity_company', 'label': '企业年金（企业）',
        'account_codes': ('6400040400',), 'ledger_columns': ('企业年金-企业',),
        'monthly_processing': '核对', 'remarks': '',
    },
    {
        'key': 'work_injury_company', 'label': '工伤保险（企业）',
        'account_codes': ('6400040500',), 'ledger_columns': ('工伤保险-企业',),
        'monthly_processing': '核对', 'remarks': '',
    },
    {
        'key': 'unemployment_company', 'label': '失业保险（企业）',
        'account_codes': ('6400040700',), 'ledger_columns': ('失业保险-企业',),
        'monthly_processing': '核对', 'remarks': '',
    },
    {
        'key': 'provident_fund_company', 'label': '住房公积金（企业）',
        'account_codes': ('6400050000',), 'ledger_columns': ('住房公积金-企业',),
        'monthly_processing': '核对', 'remarks': '',
    },
    {
        'key': 'union_funds', 'label': '工会经费',
        'account_codes': ('6400070000',), 'ledger_columns': ('工会经费',),
        'monthly_processing': '按工资应发自动分摊', 'rate': Decimal('0.02'),
        'remarks': '逐人按两位小数计算，分摊尾差固定由人力资源部主任承接；上传当月财务表时以财务金额为控制数。',
    },
    {
        'key': 'edu_funds', 'label': '职工教育经费',
        'account_codes': ('6400080000',), 'ledger_columns': ('职工教育经费',),
        'monthly_processing': '按工资应发自动分摊', 'rate': Decimal('0.015'),
        'remarks': '逐人按两位小数计算，分摊尾差固定由人力资源部主任承接；上传当月财务表时以财务金额为控制数。',
    },
    {
        'key': 'retiree_pension_subsidy', 'label': '退休人员养老金补贴',
        'account_codes': ('6602690200',),
        'ledger_columns': ('补充医保费', '员工慰问费'),
        'ledger_scope': 'retired',
        'monthly_processing': '系统内映射核对',
        'remarks': '系统按既有历史口径汇总退休人员“补充医保费/员工慰问费”，不修改人工成本主表字段。',
    },
    {
        'key': 'retiree_medical_expense', 'label': '退休人员医药费',
        'account_codes': ('6602690201',), 'ledger_columns': ('员工体检费',),
        'ledger_scope': 'retired',
        'monthly_processing': '系统内映射核对',
        'remarks': '系统将退休人员“员工体检费”解释为退休医药费，不修改人工成本主表字段。',
    },
    {
        'key': 'retiree_other_expense', 'label': '退休人员积分兑换',
        'account_codes': ('6602690299',), 'ledger_columns': ('其他福利',),
        'ledger_scope': 'retired',
        'monthly_processing': '系统内映射核对',
        'remarks': '工会确认科目性质为退休人员积分兑换，系统映射到退休人员“其他福利”。',
    },
    {
        'key': 'pension_personal', 'label': '基本养老保险（个人）',
        'account_codes': ('2241030100',), 'ledger_columns': ('养老保险-个人',),
        'monthly_processing': '核对', 'remarks': '',
    },
    {
        'key': 'medical_personal', 'label': '基本医疗保险（个人）',
        'account_codes': ('2241030200',),
        'ledger_columns': ('医疗保险-个人(含大病)',),
        'monthly_processing': '核对', 'remarks': '',
    },
    {
        'key': 'unemployment_personal', 'label': '失业保险（个人）',
        'account_codes': ('2241030300',), 'ledger_columns': ('失业保险-个人',),
        'monthly_processing': '核对', 'remarks': '',
    },
    {
        'key': 'provident_fund_personal', 'label': '住房公积金（个人）',
        'account_codes': ('2241030400',), 'ledger_columns': ('住房公积金-个人',),
        'monthly_processing': '核对', 'remarks': '',
    },
    {
        'key': 'annuity_personal', 'label': '企业年金（个人）',
        'account_codes': ('2241030500',), 'ledger_columns': ('企业年金-个人',),
        'monthly_processing': '核对', 'remarks': '',
    },
]


def localize_labor_cost_codes(df):
    """将台账中的稳定英文代码转换为面向业务人员的中文名称。"""
    localized = df.copy()
    mappings = {
        'business_type_snapshot': ARRANGEMENT_LABELS,
        '业务关系类型': ARRANGEMENT_LABELS,
        'reallocation_mode': REALLOCATION_MODE_LABELS,
        '成本划转方式': REALLOCATION_MODE_LABELS,
        '划转方式': REALLOCATION_MODE_LABELS,
        'reallocation_status': REALLOCATION_STATUS_LABELS,
        '成本划转状态': REALLOCATION_STATUS_LABELS,
        '划转状态': REALLOCATION_STATUS_LABELS,
    }
    for column, mapping in mappings.items():
        if column in localized.columns:
            localized[column] = localized[column].map(
                lambda value: mapping.get(value, value) if pd.notna(value) else value
            )
    return localized

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


def get_company_social_snapshot(target_month, conn=None):
    """提取真正属于省公众人工成本的社保明细，排除下沉和挂靠代垫。"""
    own_conn = conn is None
    conn = conn or _get_db_connection()
    try:
        item_df = pd.read_sql_query(
            """
            SELECT emp_id, insurance_item, company_amount, personal_amount,
                   cost_bearer_code, business_type_snapshot
            FROM social_monthly_items
            WHERE cost_month = ?
            """,
            conn,
            params=[target_month],
        )
        result_by_emp = {}
        employees_with_item_detail = set()
        if not item_df.empty:
            for _, row in item_df.iterrows():
                emp_id = str(row['emp_id']).replace('.0', '').strip()
                employees_with_item_detail.add(emp_id)
                relation_type = str(row.get('business_type_snapshot') or 'normal')
                cost_code = row.get('cost_bearer_code')
                belongs_to_company = (
                    cost_code == 'province_public'
                    or (
                        (cost_code is None or pd.isna(cost_code) or str(cost_code).strip() == '')
                        and relation_type in {'normal', 'city_transfer'}
                    )
                )
                if not belongs_to_company:
                    continue
                target = result_by_emp.setdefault(emp_id, {'emp_id': emp_id})
                item = str(row['insurance_item'])
                company_amount = float(row.get('company_amount') or 0.0)
                personal_amount = float(row.get('personal_amount') or 0.0)
                if item == 'medical_serious':
                    target['medical_pers'] = target.get('medical_pers', 0.0) + personal_amount
                else:
                    target[f'{item}_comp'] = target.get(f'{item}_comp', 0.0) + company_amount
                    target[f'{item}_pers'] = target.get(f'{item}_pers', 0.0) + personal_amount

        # 兼容尚未生成险种明细的旧月份。只对真正进入本单位人工成本的人回退到旧汇总账。
        legacy_df = pd.read_sql_query(
            "SELECT * FROM ss_monthly_records WHERE cost_month = ?",
            conn,
            params=[target_month],
        )
        legacy_columns = [
            'pension_comp', 'medical_comp', 'unemp_comp', 'injury_comp',
            'maternity_comp', 'fund_comp', 'annuity_comp', 'pension_pers',
            'medical_pers', 'medical_serious_pers', 'unemp_pers', 'fund_pers',
            'annuity_pers',
        ]
        for _, row in legacy_df.iterrows():
            emp_id = str(row['emp_id']).replace('.0', '').strip()
            if emp_id in employees_with_item_detail:
                continue
            if not is_labor_cost_included(emp_id, target_month, conn):
                continue
            target = result_by_emp.setdefault(emp_id, {'emp_id': emp_id})
            for column in legacy_columns:
                value = float(row.get(column) or 0.0)
                if column == 'medical_serious_pers':
                    target['medical_pers'] = target.get('medical_pers', 0.0) + value
                else:
                    target[column] = value

        result = pd.DataFrame(result_by_emp.values())
        expected_columns = ['emp_id'] + [
            'pension_comp', 'medical_comp', 'unemp_comp', 'injury_comp',
            'maternity_comp', 'fund_comp', 'annuity_comp', 'pension_pers',
            'medical_pers', 'unemp_pers', 'fund_pers', 'annuity_pers',
        ]
        for column in expected_columns:
            if column not in result.columns:
                result[column] = '' if column == 'emp_id' else 0.0
        return result[expected_columns]
    finally:
        if own_conn:
            conn.close()

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


# ------------------------------------------------------------------------------
# 财务人工成本表：读取、自动补数与双口径核对
# ------------------------------------------------------------------------------
def _source_as_bytes(source):
    """把路径、bytes 或 Streamlit UploadedFile 统一成可重复读取的输入。"""
    if isinstance(source, (str, os.PathLike)):
        with open(source, 'rb') as file_obj:
            return file_obj.read()
    if isinstance(source, bytes):
        return source
    if hasattr(source, 'getvalue'):
        return source.getvalue()
    if hasattr(source, 'read'):
        current_pos = source.tell() if hasattr(source, 'tell') else None
        data = source.read()
        if current_pos is not None and hasattr(source, 'seek'):
            source.seek(current_pos)
        return data
    raise TypeError('不支持的文件输入类型。')


def _find_excel_header(raw_df, required_columns):
    required = set(required_columns)
    for row_index in range(min(len(raw_df), 30)):
        values = {
            str(value).strip()
            for value in raw_df.iloc[row_index].tolist()
            if pd.notna(value)
        }
        if required.issubset(values):
            return row_index
    return None


def read_labor_ledger_workbook(source, file_name=None):
    """读取人工成本台账，只接收真正包含人员明细表头的 Sheet。"""
    effective_name = file_name or getattr(source, 'name', '')
    if str(effective_name).lower().endswith('.csv'):
        return pd.read_csv(io.BytesIO(_source_as_bytes(source)))

    workbook_bytes = _source_as_bytes(source)
    excel_file = pd.ExcelFile(io.BytesIO(workbook_bytes))
    valid_sheets = []
    required_columns = ('核算月份', '工号', '姓名')
    for sheet_name in excel_file.sheet_names:
        raw = pd.read_excel(
            io.BytesIO(workbook_bytes), sheet_name=sheet_name, header=None
        )
        header_index = _find_excel_header(raw, required_columns)
        if header_index is None:
            continue
        valid_sheets.append(pd.read_excel(
            io.BytesIO(workbook_bytes), sheet_name=sheet_name, header=header_index
        ))

    if not valid_sheets:
        raise ValueError('没有找到同时包含“核算月份、工号、姓名”的人员台账 Sheet。')
    return pd.concat(valid_sheets, ignore_index=True)


def _normalize_account_code(value):
    if pd.isna(value):
        return ''
    text_value = str(value).strip()
    return text_value[:-2] if text_value.endswith('.0') else text_value


def read_finance_account_workbook(source):
    """读取财务科目余额表，兼容表头前存在标题行的工作簿。"""
    workbook_bytes = _source_as_bytes(source)
    excel_file = pd.ExcelFile(io.BytesIO(workbook_bytes))
    required_columns = ('科目编号', '科目名称', '本期借方发生额')
    valid_sheets = []

    for sheet_name in excel_file.sheet_names:
        raw = pd.read_excel(
            io.BytesIO(workbook_bytes), sheet_name=sheet_name, header=None
        )
        header_index = _find_excel_header(raw, required_columns)
        if header_index is None:
            continue
        sheet_df = pd.read_excel(
            io.BytesIO(workbook_bytes), sheet_name=sheet_name, header=header_index
        )
        sheet_df['来源Sheet'] = sheet_name
        valid_sheets.append(sheet_df)

    if not valid_sheets:
        raise ValueError('没有找到包含“科目编号、科目名称、本期借方发生额”的财务科目表。')

    finance_df = pd.concat(valid_sheets, ignore_index=True)
    finance_df['科目编号'] = finance_df['科目编号'].map(_normalize_account_code)
    for column in ('期初余额', '本期借方发生额', '本期贷方发生额', '期末余额'):
        if column not in finance_df.columns:
            finance_df[column] = 0.0
        finance_df[column] = pd.to_numeric(finance_df[column], errors='coerce').fillna(0.0)
    finance_df['科目名称'] = finance_df['科目名称'].fillna('').astype(str).str.strip()
    return finance_df


def _money(value):
    return Decimal(str(value or 0)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def _ensure_numeric_columns(df, columns):
    for column in columns:
        if column not in df.columns:
            df[column] = 0.0
        df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0.0)
    return df


def _finance_amount(finance_df, account_codes):
    selected = finance_df[finance_df['科目编号'].isin(account_codes)]
    return float(_money(selected['本期借方发生额'].sum()))


def _ledger_amount(ledger_df, columns, ledger_scope='all'):
    scoped_df = ledger_df
    if ledger_scope == 'retired':
        scoped_df = ledger_df[_retired_mask(ledger_df)]
    elif ledger_scope == 'non_retired':
        scoped_df = ledger_df[~_retired_mask(ledger_df)]
    existing_columns = [column for column in columns if column in ledger_df.columns]
    if not existing_columns:
        return 0.0
    amount = scoped_df[existing_columns].apply(
        pd.to_numeric, errors='coerce'
    ).fillna(0.0).sum().sum()
    return float(_money(amount))


def _retired_mask(ledger_df):
    status = ledger_df.get(
        '人员状态', pd.Series('', index=ledger_df.index, dtype='object')
    ).fillna('').astype(str)
    department = ledger_df.get(
        '归属部门', pd.Series('', index=ledger_df.index, dtype='object')
    ).fillna('').astype(str)
    return status.eq('退休') | department.str.contains('离退休', na=False)


def _allocate_by_gross(
    ledger_df,
    target_column,
    control_total,
    rate,
    tail_carrier_emp_id=None,
):
    """逐人按 Excel 两位小数分摊，并把尾差固定放到指定承接人员。"""
    ledger_df = _ensure_numeric_columns(
        ledger_df, ['工资应发合计', target_column]
    )
    eligible = ledger_df['工资应发合计'] > 0
    ledger_df.loc[:, target_column] = 0.0
    if not eligible.any():
        return ledger_df, {
            '自动处理项目': target_column,
            '计提控制数': float(_money(control_total)),
            '逐人计算合计': 0.0,
            '分摊尾差': float(_money(control_total)),
            '尾差承接人员': '无可分摊人员',
        }

    allocations = {}
    for index in ledger_df.index[eligible]:
        gross = Decimal(str(ledger_df.at[index, '工资应发合计']))
        allocations[index] = (gross * rate).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )

    calculated_total = sum(allocations.values(), Decimal('0.00'))
    tail_difference = _money(control_total) - calculated_total
    if tail_carrier_emp_id:
        if '工号' not in ledger_df.columns:
            raise ValueError('底表缺少工号，无法定位人力资源部主任承接经费尾差。')
        normalized_ids = (
            ledger_df['工号'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        )
        carrier_matches = ledger_df.index[
            normalized_ids.eq(str(tail_carrier_emp_id).strip()) & eligible
        ].tolist()
        if not carrier_matches:
            raise ValueError(
                f'底表中没有找到可承接尾差的人力资源部主任（工号{tail_carrier_emp_id}）。'
            )
        carrier_index = carrier_matches[0]
    else:
        human_resource_rows = eligible
        if '归属部门' in ledger_df.columns:
            human_resource_rows = (
                eligible
                & ledger_df['归属部门'].fillna('').astype(str).str.contains('人力资源部', na=False)
            )
        if not human_resource_rows.any():
            raise ValueError('无法识别人力资源部主任，不能自动处理工会和教育经费尾差。')
        carrier_index = ledger_df.loc[human_resource_rows, '工资应发合计'].idxmax()
    allocations[carrier_index] += tail_difference

    for index, amount in allocations.items():
        ledger_df.at[index, target_column] = float(amount)

    carrier_name = str(ledger_df.at[carrier_index, '姓名']) if '姓名' in ledger_df.columns else ''
    carrier_id = str(ledger_df.at[carrier_index, '工号']) if '工号' in ledger_df.columns else ''
    carrier_display = carrier_name
    if carrier_id:
        carrier_display = f'{carrier_display}（{carrier_id}）' if carrier_display else carrier_id
    return ledger_df, {
        '自动处理项目': target_column,
        '计提控制数': float(_money(control_total)),
        '逐人计算合计': float(calculated_total),
        '分摊尾差': float(tail_difference),
        '尾差承接人员': carrier_display,
    }


def recalculate_labor_cost_columns(ledger_df):
    """按正式业务口径重算工资应发、实发、其他成本与人工成本合计。"""
    result = ledger_df.copy()
    all_numeric = (
        GROSS_COMPONENT_COLUMNS_CN
        + PERSONAL_DEDUCTION_COLUMNS_CN
        + OTHER_COST_COMPONENT_COLUMNS_CN
        + ['女工劳保费', '个人实发', '工资应发合计', '其他人工成本合计', '人工成本合计']
    )
    result = _ensure_numeric_columns(result, list(dict.fromkeys(all_numeric)))

    original_net = result['个人实发'].copy()
    result['工资应发合计'] = result[GROSS_COMPONENT_COLUMNS_CN].sum(axis=1).round(2)
    calculated_net = (
        result['工资应发合计']
        + result['女工劳保费']
        - result[PERSONAL_DEDUCTION_COLUMNS_CN].sum(axis=1)
    ).round(2)
    keep_original_net = (
        result['工资应发合计'].eq(0)
        & result['女工劳保费'].eq(0)
        & original_net.ne(0)
    )
    result['个人实发'] = calculated_net
    result.loc[keep_original_net, '个人实发'] = original_net[keep_original_net]
    result['其他人工成本合计'] = result[OTHER_COST_COMPONENT_COLUMNS_CN].sum(axis=1).round(2)
    result['人工成本合计'] = (
        result['工资应发合计'] + result['其他人工成本合计']
    ).round(2)
    return result


def _build_reconciliation(finance_df, ledger_df, scope_label):
    rows = []
    for rule in FINANCE_LABOR_CONTROL_RULES:
        finance_amount = _finance_amount(finance_df, rule['account_codes'])
        ledger_amount = _ledger_amount(
            ledger_df,
            rule['ledger_columns'],
            rule.get('ledger_scope', 'all'),
        )
        difference = float(_money(ledger_amount - finance_amount))
        if rule['monthly_processing'] == '待业务确认' and abs(finance_amount) >= 0.005:
            status = '待人工确认' if abs(difference) <= 0.01 else '待确认'
        elif abs(difference) <= 0.01:
            status = '一致'
        else:
            status = '有差异'
        rows.append({
            '核对范围': scope_label,
            '核对项目': rule['label'],
            '财务科目': '、'.join(rule['account_codes']),
            '财务金额': finance_amount,
            '台账金额': ledger_amount,
            '差额（台账-财务）': difference,
            '处理方式': rule['monthly_processing'],
            '核对状态': status,
            '说明': rule['remarks'],
        })
    return pd.DataFrame(rows)


def _find_pending_expense_accounts(finance_df):
    mapped_codes = {
        code
        for rule in FINANCE_LABOR_CONTROL_RULES
        for code in rule['account_codes']
    }
    expense_mask = finance_df['科目编号'].str.startswith(('640', '660'), na=False)
    research_reclass_mask = finance_df['科目编号'].str.startswith('660402', na=False)
    nonzero_mask = finance_df['本期借方发生额'].abs() >= 0.005
    pending = finance_df[
        expense_mask & ~research_reclass_mask & nonzero_mask
        & ~finance_df['科目编号'].isin(mapped_codes)
    ].copy()
    pending['处理状态'] = '未建立人工成本映射'

    confirmation_codes = {
        code
        for rule in FINANCE_LABOR_CONTROL_RULES
        if rule['monthly_processing'] == '待业务确认'
        for code in rule['account_codes']
    }
    confirmation_pending = finance_df[
        finance_df['科目编号'].isin(confirmation_codes) & nonzero_mask
    ].copy()
    confirmation_pending['处理状态'] = '已有暂存字段，但费用性质和人员归属待确认'

    combined = pd.concat([pending, confirmation_pending], ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=['科目编号', '科目名称', '本期借方发生额', '处理状态'])
    return combined[
        ['科目编号', '科目名称', '本期借方发生额', '处理状态']
    ].drop_duplicates(subset=['科目编号'], keep='last')


def prepare_finance_labor_precheck(
    ledger_df,
    monthly_finance_df=None,
    ytd_finance_df=None,
    historical_ledger_df=None,
    tail_carrier_emp_id=None,
):
    """
    自动补充分摊项并生成当月/累计核对结果。

    只传底表时按工资应发总额和计提比例自动计算；财务当月表存在时用它
    控制当月分录，累计表只负责发现历史漂移。系统不会为了让累计数看起来
    一致而篡改当月正确金额。
    """
    processed = recalculate_labor_cost_columns(ledger_df)
    auto_actions = []

    monthly_controls = {}
    if monthly_finance_df is not None:
        monthly_controls = {
            rule['key']: _finance_amount(monthly_finance_df, rule['account_codes'])
            for rule in FINANCE_LABOR_CONTROL_RULES
        }

    gross_control_total = _money(processed['工资应发合计'].sum())

    # 工会、教育经费以财务当月发生额为控制数，解决逐人四舍五入尾差。
    for rule in FINANCE_LABOR_CONTROL_RULES:
        if rule['monthly_processing'] != '按工资应发自动分摊':
            continue
        formula_control_total = (gross_control_total * rule['rate']).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
        control_total = monthly_controls.get(rule['key'], float(formula_control_total))
        processed, action = _allocate_by_gross(
            processed,
            rule['ledger_columns'][0],
            control_total,
            rule['rate'],
            tail_carrier_emp_id=tail_carrier_emp_id,
        )
        action['控制数来源'] = (
            '财务当月表' if monthly_finance_df is not None else '工资应发合计×计提比例'
        )
        auto_actions.append(action)

    processed = recalculate_labor_cost_columns(processed)
    ordered_columns = [
        column for column in LEDGER_MAP.keys() if column in processed.columns
    ]
    extra_columns = [
        column for column in processed.columns if column not in ordered_columns
    ]
    processed = processed[ordered_columns + extra_columns]
    monthly_reconciliation = pd.DataFrame()
    if monthly_finance_df is not None:
        monthly_reconciliation = _build_reconciliation(
            monthly_finance_df, processed, '当月'
        )

    ytd_reconciliation = pd.DataFrame()
    if ytd_finance_df is not None:
        ytd_ledger_parts = []
        if historical_ledger_df is not None and not historical_ledger_df.empty:
            history = historical_ledger_df.copy()
            history = history.rename(columns=DB_TO_CN_MAP)
            ytd_ledger_parts.append(history)
        ytd_ledger_parts.append(processed)
        ytd_ledger = pd.concat(ytd_ledger_parts, ignore_index=True, sort=False)
        ytd_reconciliation = _build_reconciliation(
            ytd_finance_df, ytd_ledger, '本年累计'
        )

    female_fee_total = _ledger_amount(processed, ('女工劳保费',))
    gross_total = _ledger_amount(processed, ('工资应发合计',))
    deductions_total = _ledger_amount(processed, PERSONAL_DEDUCTION_COLUMNS_CN)
    net_total = _ledger_amount(processed, ('个人实发',))
    formula_difference = float(_money(
        net_total - (gross_total + female_fee_total - deductions_total)
    ))
    business_checks = pd.DataFrame([{
        '业务公式': '个人实发 = 工资应发 + 女工劳保费 - 个人社保公积金年金 - 个税',
        '工资应发': gross_total,
        '女工劳保费': female_fee_total,
        '个人扣款及个税': deductions_total,
        '个人实发': net_total,
        '公式差额': formula_difference,
        '核对状态': '一致' if abs(formula_difference) <= 0.01 else '有差异',
    }])

    pending_accounts = (
        _find_pending_expense_accounts(monthly_finance_df)
        if monthly_finance_df is not None
        else pd.DataFrame(
            columns=['科目编号', '科目名称', '本期借方发生额', '处理状态']
        )
    )
    return {
        'processed_ledger': processed,
        'monthly_reconciliation': monthly_reconciliation,
        'ytd_reconciliation': ytd_reconciliation,
        'auto_actions': pd.DataFrame(auto_actions),
        'business_checks': business_checks,
        'pending_accounts': pending_accounts,
    }
