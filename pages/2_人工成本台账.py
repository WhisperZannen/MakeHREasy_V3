# ==============================================================================
# 文件路径: pages/2_人工成本台账.py
# 功能描述: 人工成本台账管理中心 (财务合规与领导审阅终极版)
# 实现了什么具体逻辑:
#   1. [排版引擎] 注入 3D 高亮视觉格式，核心汇总行与关键金额列极度醒目。
#   2. [看板口径] 严格剔除离退休人员，确保前端指标卡片与报表“实际成本总计”分毫不差。
#   3. [底层清洗] 加入静默 DB 脏数据清洗与导入时的时间戳截断器，永久告别时分秒。
#   4. [精准拦截] 修复智能清洗引擎的“过度防卫”，只拦截标准汇总行，放行带特殊符号的正常数据。
# ==============================================================================

import streamlit as st
import pandas as pd
import sqlite3
import os
import io

# 用于 Excel 报表精装修
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

st.set_page_config(page_title="人工成本台账", layout="wide")

# ==============================================================================
# 数据库与字段映射中枢
# ==============================================================================
def _get_db_connection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    db_path = os.path.join(project_root, 'database', 'hr_core.db')
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn

# 每次加载页面时，静默清洗数据库中历史残留的时间戳脏数据
def _cleanse_db_timestamps():
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE labor_cost_ledger SET cost_month = substr(cost_month, 1, 7) WHERE length(cost_month) > 7")
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()

_cleanse_db_timestamps()

LEDGER_MAP = {
    '核算月份': 'cost_month', '工号': 'emp_id', '姓名': 'emp_name', '归属部门': 'dept_name', '人员状态': 'emp_status',
    '岗位工资': 'base_salary', '工龄工资': 'seniority_pay', '综合补贴': 'comp_subsidy', '岗位绩效浮动补贴': 'perf_float_subsidy',
    '通讯费': 'telecom_subsidy', '其他岗位工资': 'other_base_pay', '实习补贴': 'intern_subsidy', '高校毕业生/专家津贴': 'grad_allowance',
    '绩效工资标准(参考)': 'perf_standard', 'KPI得分(参考)': 'kpi_score', '考核绩效': 'eval_perf_pay', '提成绩效': 'commission_pay',
    '其他月度绩效': 'other_month_perf', '专项奖(含考勤扣罚)': 'special_award', '年终绩效奖': 'year_end_bonus', '其他专项奖': 'other_special_award',
    '工资应发合计': 'gross_salary_total', '养老保险-个人': 'pension_personal', '医疗保险-个人': 'medical_personal',
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

# ==============================================================================
# 全局基础排序引擎 (专用于前端看板与纯净底表，不产生汇总行)
# ==============================================================================
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
        # 兼容中文表头和数据库原英文字段
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

# ==============================================================================
# 财务级报表引擎：绝对排序、隔离与汇总
# ==============================================================================
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
    except Exception as e:
        print(f"🚨 排序引擎数据库拉取异常: {e}")
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

# ==============================================================================
# UI 消息锁与数据提取逻辑
# ==============================================================================
if 'ledger_msg' in st.session_state:
    if st.session_state.ledger_msg_type == 'success': st.success(st.session_state.ledger_msg)
    else: st.error(st.session_state.ledger_msg)
    del st.session_state.ledger_msg, st.session_state.ledger_msg_type

def set_msg(msg, type='success'):
    st.session_state.ledger_msg = msg
    st.session_state.ledger_msg_type = type
    st.rerun()

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

# ==============================================================================
# Excel 财务级排版渲染引擎 (3D 高亮视觉版)
# ==============================================================================
def format_excel_sheet(worksheet, df_columns):
    worksheet.freeze_panes = 'A2'
    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    header_fill = PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid")
    subtotal_fill = PatternFill(start_color="E6F2FF", end_color="E6F2FF", fill_type="solid")
    total_fill = PatternFill(start_color="FFE6CC", end_color="FFE6CC", fill_type="solid")
    key_col_fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
    key_columns = ['工资应发合计', '其他人工成本合计', '人工成本合计']

    df_cols_list = list(df_columns)
    name_col_idx = df_cols_list.index('姓名') + 1 if '姓名' in df_cols_list else -1

    for row_idx in range(1, worksheet.max_row + 1):
        is_subtotal = False
        is_total = False
        if name_col_idx != -1 and row_idx > 1:
            cell_val = str(worksheet.cell(row=row_idx, column=name_col_idx).value or "")
            if "【小计】" == cell_val: is_subtotal = True
            elif "【实际成本总计】" == cell_val: is_total = True

        for col_idx, col_name in enumerate(df_columns, 1):
            col_letter = get_column_letter(col_idx)
            cell = worksheet[f"{col_letter}{row_idx}"]
            cell.border = thin_border

            if row_idx == 1:
                worksheet.column_dimensions[col_letter].width = 8 if col_name == '序号' else (15 if col_name in NUMERIC_COLS else 12)
                cell.font = Font(bold=True)
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal='center', vertical='center')
            else:
                if col_name in NUMERIC_COLS:
                    cell.number_format = '#,##0.00'
                    cell.alignment = Alignment(horizontal='right', vertical='center')
                else:
                    cell.alignment = Alignment(horizontal='center', vertical='center')

                if is_total:
                    cell.fill = total_fill
                    cell.font = Font(bold=True, color="000000")
                elif is_subtotal:
                    cell.fill = subtotal_fill
                    cell.font = Font(bold=True, color="000000")
                elif col_name in key_columns:
                    cell.fill = key_col_fill
                    cell.font = Font(bold=True)

# ==============================================================================
# 页面框架
# ==============================================================================
st.title("💰 人工成本台账管理中心")
st.caption("🔒 财务数据合规要求：台账一旦生成不可在系统内手动篡改。如需修正，请在 Excel 中修改后重新导入，系统将自动覆盖重置原账目。")

tab1, tab2, tab3 = st.tabs(["📊 台账多维看板", "📤 领导审阅导出 (含汇总)", "📥 财务底表导入"])

# ------------------------------------------------------------------------------
# Tab 1: 台账多维看板
# ------------------------------------------------------------------------------
with tab1:
    conn = _get_db_connection()
    available_months = pd.read_sql_query("SELECT DISTINCT cost_month FROM labor_cost_ledger ORDER BY cost_month DESC", conn)['cost_month'].tolist()
    available_depts = pd.read_sql_query("SELECT DISTINCT dept_name FROM labor_cost_ledger", conn)['dept_name'].tolist()
    conn.close()

    # [核心升级] 注入姓名/工号多维检索
    sc1, sc2, sc3 = st.columns([1, 1, 1])
    with sc1: f_month = st.selectbox("📅 核算月份筛选", ["全部月份"] + available_months)
    with sc2: f_dept = st.multiselect("🏢 归属部门筛选", options=available_depts)
    with sc3: q_search = st.text_input("🔍 搜姓名 / 工号")

    raw_df = get_ledger_data(month_filter=None if f_month == "全部月份" else f_month, dept_filter=f_dept if f_dept else None)

    if not raw_df.empty:
        # 执行文本检索过滤
        if q_search:
            raw_df = raw_df[raw_df['emp_name'].str.contains(q_search, na=False) | raw_df['emp_id'].str.contains(q_search, na=False)]

    if not raw_df.empty:
        active_metric_df = raw_df[~( (raw_df['emp_status'] == '退休') | (raw_df['dept_name'].str.contains('离退休', na=False)) )]

        total_cost = active_metric_df['total_labor_cost'].sum()
        total_gross = active_metric_df['gross_salary_total'].sum()
        total_other = active_metric_df['other_cost_total'].sum()
        total_headcount = len(active_metric_df)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("在职及统筹总成本 (元)", f"{total_cost:,.2f}")
        m2.metric("工资应发合计 (元)", f"{total_gross:,.2f}")
        m3.metric("其他人工成本合计 (元)", f"{total_other:,.2f}")
        m4.metric("在职及统筹核算人次", f"{total_headcount} 人次")

        disp_df = raw_df.rename(columns=DB_TO_CN_MAP)
        disp_cols = [col for col in LEDGER_MAP.keys() if col in disp_df.columns]

        # [核心修复] 对前端展示的表格执行绝对排序！
        disp_final = sort_flat_ledger_df(disp_df[disp_cols].copy())

        disp_final.insert(0, '序号', range(1, len(disp_final) + 1))
        st.dataframe(disp_final, use_container_width=True, hide_index=True)
    else:
        st.info("💡 当前筛选条件下暂无台账数据。")

# ------------------------------------------------------------------------------
# Tab 2: 领导审阅导出 (高自由度灵性定制版 + 下月繁衍引擎)
# ------------------------------------------------------------------------------
with tab2:
    st.subheader("📤 生成向领导汇报的标准台账")

    st.info("💡 操作提示：下方的月份选择框支持**多选**。你可以挑选任意几个月（例如选 1月到10月），系统会自动为你打包提取。")
    selected_months = st.multiselect("📅 选择要导出的核算月份", options=available_months, default=available_months[:1] if available_months else [])

    need_summary = st.checkbox("📊 同时生成【选中月份的累计汇总】Sheet (勾选后，系统会自动把您选中的这几个月加起来算个总账)", value=True)

    if st.button("🚀 一键生成并下载 Excel 报表", type="primary"):
        if not selected_months:
            st.warning("⚠️ 请至少在上方选择一个核算月份！")
        else:
            conn = _get_db_connection()
            placeholders = ",".join(["?"] * len(selected_months))
            query = f"SELECT * FROM labor_cost_ledger WHERE cost_month IN ({placeholders}) ORDER BY dept_name ASC"
            raw_export_df = pd.read_sql_query(query, conn, params=selected_months)
            conn.close()

            if not raw_export_df.empty:
                ob = io.BytesIO()
                with pd.ExcelWriter(ob, engine='openpyxl') as writer:

                    if need_summary:
                        db_num_cols = [LEDGER_MAP[c] for c in NUMERIC_COLS if c in LEDGER_MAP]
                        agg_dict = {col: 'sum' for col in db_num_cols}
                        agg_dict.update({'emp_name': 'first', 'dept_name': 'first', 'emp_status': 'last'})

                        summary_df = raw_export_df.groupby('emp_id').agg(agg_dict).reset_index()
                        summary_cn = summary_df.rename(columns=DB_TO_CN_MAP)
                        report_cols = [c for c in LEDGER_MAP.keys() if c in summary_cn.columns and c != '核算月份']
                        summary_cn = summary_cn[report_cols]

                        summary_final = add_subtotals_and_totals(summary_cn, NUMERIC_COLS)

                        sum_sheet_name = f"{len(selected_months)}个月累计汇总"
                        summary_final.to_excel(writer, index=False, sheet_name=sum_sheet_name)
                        format_excel_sheet(writer.sheets[sum_sheet_name], summary_final.columns)

                    for month in sorted(selected_months):
                        month_df = raw_export_df[raw_export_df['cost_month'] == month].copy()
                        if not month_df.empty:
                            month_cn = month_df.rename(columns=DB_TO_CN_MAP)
                            month_cols = [c for c in LEDGER_MAP.keys() if c in month_cn.columns]
                            month_cn = month_cn[month_cols]

                            month_final = add_subtotals_and_totals(month_cn, NUMERIC_COLS)
                            safe_month = str(month).replace(':', '-').replace('/', '-').replace('\\', '-').replace('?', '').replace('*', '')
                            safe_sheet_name = f"{safe_month[:28]}明细"

                            month_final.to_excel(writer, index=False, sheet_name=safe_sheet_name)
                            format_excel_sheet(writer.sheets[safe_sheet_name], month_final.columns)

                file_name = f"人工成本台账汇报_{len(selected_months)}个月数据.xlsx"
                st.download_button("📥 点击下载财务报表", data=ob.getvalue(), file_name=file_name, type="secondary")
            else:
                st.warning("所选范围内无数据。")

    # ==========================================================================
    # [核心补充] 下月数据初始化引擎 (纯净底表繁衍)
    # ==========================================================================
    st.divider()
    st.subheader("🆕 生成新月份初始化底表 (纯净录入版)")
    st.info("💡 痛点解决：系统将提取【基准月】人员（含已离职但有账目的人），自动追加【档案里新入职】但在基准月没发过钱的新人。生成的 Excel 只有 1 个纯净 Sheet，无小计/总计。")

    col_t1, col_t2, col_t3 = st.columns(3)
    with col_t1: base_month = st.selectbox("参照基准月 (提取其人员框架)", options=available_months)
    with col_t2: target_month = st.text_input("目标生成月 (格式 YYYY-MM)", value="2026-02" if base_month == "2026-01" else "")
    with col_t3: st.write(""); clear_nums = st.checkbox("清空所有变动金额 (仅保留人员与上月固薪)", value=False)

    if st.button("🚀 生成新月度录入底表", type="primary"):
        if not target_month:
            st.warning("请填写目标月份！")
        else:
            conn = _get_db_connection()
            base_df = pd.read_sql_query("SELECT * FROM labor_cost_ledger WHERE cost_month = ?", conn, params=[base_month])
            active_emps = pd.read_sql_query("SELECT emp_id, name, dept_id, status FROM employees WHERE status = '在职'", conn)
            # [核心修复] 拆解危险的 zip 单行代码，使用原生 Pandas 序列映射，绝不引发长短键值对报错
            _tmp_dept_df = pd.read_sql_query("SELECT dept_id, dept_name FROM departments", conn)
            dept_dict = dict(zip(_tmp_dept_df['dept_id'], _tmp_dept_df['dept_name']))
            conn.close()

            if not base_df.empty:
                base_df['cost_month'] = target_month
                base_emp_ids = set(base_df['emp_id'].tolist())
                new_emps = active_emps[~active_emps['emp_id'].isin(base_emp_ids)]

                new_rows = []
                for _, r in new_emps.iterrows():
                    new_row = {col: 0.0 if col in NUMERIC_COLS else None for col in base_df.columns}
                    new_row['cost_month'] = target_month
                    new_row['emp_id'] = r['emp_id']
                    new_row['emp_name'] = r['name']
                    new_row['dept_name'] = dept_dict.get(r['dept_id'], '未分配部门')
                    new_row['emp_status'] = r['status']
                    new_rows.append(new_row)

                if new_rows:
                    base_df = pd.concat([base_df, pd.DataFrame(new_rows)], ignore_index=True)

                export_cn = base_df.rename(columns=DB_TO_CN_MAP)

                if clear_nums:
                    for cn_col in NUMERIC_COLS:
                        if cn_col in export_cn.columns: export_cn[cn_col] = 0.0

                ordered_cols = [c for c in LEDGER_MAP.keys() if c in export_cn.columns]
                export_cn = export_cn[ordered_cols]
                export_cn = sort_flat_ledger_df(export_cn)

                ob_clean = io.BytesIO()
                with pd.ExcelWriter(ob_clean, engine='openpyxl') as w:
                    export_cn.to_excel(w, index=False, sheet_name=f"{target_month}明细_纯净版")
                    ws = w.sheets[f"{target_month}明细_纯净版"]
                    ws.freeze_panes = 'A2'
                    for col_idx in range(1, ws.max_column + 1):
                        ws.column_dimensions[get_column_letter(col_idx)].width = 13

                st.download_button(f"📥 下载 {target_month} 纯净底表", data=ob_clean.getvalue(), file_name=f"台账初始化_{target_month}.xlsx", type="secondary")
            else:
                st.error("基准月没有数据，无法繁衍！")

# ------------------------------------------------------------------------------
# Tab 3: 财务底表导入 (修复了缩进致命错误的终极版)
# ------------------------------------------------------------------------------
with tab3:
    st.subheader("📥 历史财务数据导入引擎")

    tc1, tc2 = st.columns(2)
    with tc1:
        st.write("请先下载标准模板。系统依靠 **`核算月份`** 和 **`工号`** 确认数据。如果发现错漏，直接在 Excel 修改后重新上传，系统将自动覆盖。")
        template_df = pd.DataFrame(columns=list(LEDGER_MAP.keys()))
        tout = io.BytesIO()
        with pd.ExcelWriter(tout, engine='openpyxl') as w: template_df.to_excel(w, index=False)
        st.download_button("下载标准导入模板", data=tout.getvalue(), file_name="人工成本台账导入模板.xlsx")

    with tc2:
        up_file = st.file_uploader("上传已填写的台账 Excel", type=["xlsx", "csv"])
        if up_file and st.button("🚀 执行导入与数据库覆盖"):
            try:
                if up_file.name.endswith('.csv'):
                    in_df = pd.read_csv(up_file)
                else:
                    xls_dict = pd.read_excel(up_file, sheet_name=None)
                    in_df = pd.concat(xls_dict.values(), ignore_index=True)

                conn = _get_db_connection()
                cursor = conn.cursor()
                success_count = 0

                for idx, row in in_df.iterrows():
                    e_name = str(row.get('姓名', '')).strip()
                    d_name = str(row.get('归属部门', '')).strip()

                    if e_name in ['【小计】', '【实际成本总计】'] or d_name in ['【在职及统筹部分】']:
                        continue

                    raw_month = str(row.get('核算月份', '')).strip()
                    # [核心加固] 自动处理 Excel 科学计数法和浮点数产生的 .0 尾缀
                    raw_id = row.get('工号', '')
                    if pd.isna(raw_id):
                        e_id = ""
                    elif isinstance(raw_id, float):
                        # 如果是浮点数，先转 int 去掉 .0，再转 str
                        e_id = str(int(raw_id))
                    else:
                        # 如果已经是字符串，去掉可能的 .0 后缀并修剪空格
                        e_id = str(raw_id).replace('.0', '').strip()

                    if not raw_month or not e_id or raw_month == 'nan' or e_id == 'nan': continue

                    c_month = raw_month[:7].replace('/', '-') if len(raw_month) >= 7 else raw_month

                    db_data = {}
                    # ==========================================
                    # 步骤一：提取 Excel 里的基础明细项
                    # ==========================================
                    for cn_col, db_col in LEDGER_MAP.items():
                        if db_col == 'cost_month':
                            db_data[db_col] = c_month
                            continue

                        val = row.get(cn_col, None)
                        if cn_col in NUMERIC_COLS:
                            try:
                                clean_val = str(val).replace(',', '').strip()
                                db_data[db_col] = float(clean_val) if pd.notna(val) and clean_val != '' else 0.0
                            except:
                                db_data[db_col] = 0.0
                        else:
                            db_data[db_col] = str(val).strip() if pd.notna(val) else ""

                    # ==========================================
                    # 步骤二：后端强制自动重新核算 (必须在上面的 for 循环结束后执行！)
                    # ==========================================
                    gross_cols = ['base_salary', 'seniority_pay', 'comp_subsidy', 'perf_float_subsidy', 'telecom_subsidy', 'other_base_pay', 'intern_subsidy', 'grad_allowance', 'eval_perf_pay', 'commission_pay', 'other_month_perf', 'special_award', 'year_end_bonus', 'other_special_award']
                    db_data['gross_salary_total'] = sum(db_data.get(col, 0.0) for col in gross_cols)

                    deduct_cols = ['pension_personal', 'medical_personal', 'unemployment_personal', 'provident_fund_personal', 'annuity_personal', 'tax_personal_month', 'tax_personal_bonus']
                    db_data['net_salary'] = db_data['gross_salary_total'] - sum(db_data.get(col, 0.0) for col in deduct_cols)

                    company_cost_cols = ['pension_company', 'medical_company', 'unemployment_company', 'work_injury_company', 'maternity_company', 'provident_fund_company', 'annuity_company', 'meal_daily', 'meal_ot', 'welfare_condolence', 'welfare_single_child', 'welfare_health_check', 'welfare_entry_check', 'welfare_other', 'allowance_heat', 'allowance_women', 'medical_supplement', 'union_funds', 'edu_funds', 'cost_adjustment']
                    db_data['other_cost_total'] = sum(db_data.get(col, 0.0) for col in company_cost_cols)

                    db_data['total_labor_cost'] = db_data['gross_salary_total'] + db_data['other_cost_total']

                    # ==========================================
                    # 步骤三：执行覆写入库
                    # ==========================================
                    columns = list(db_data.keys())
                    placeholders = ",".join(["?"] * len(columns))
                    updates = ",".join([f"{col}=excluded.{col}" for col in columns if col not in ['cost_month', 'emp_id']])

                    sql = f"""
                        INSERT INTO labor_cost_ledger ({",".join(columns)})
                        VALUES ({placeholders})
                        ON CONFLICT(cost_month, emp_id) DO UPDATE SET
                        {updates}
                    """
                    cursor.execute(sql, tuple(db_data.values()))
                    success_count += 1

                conn.commit()
                set_msg(f"台账导入/覆盖完成！成功处理 {success_count} 条记录。")
            except Exception as e:
                conn.rollback()
                st.error(f"导入底层崩溃: {e}")
            finally:
                if 'conn' in locals(): conn.close()