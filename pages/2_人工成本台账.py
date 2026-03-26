# ==============================================================================
# 文件路径: pages/2_人工成本台账.py
# 功能描述: 人工成本台账管理中心 (财务合规与领导审阅版)
# 实现了什么具体逻辑:
#   1. [合规底线] 彻底移除 UI 层的“增删改”功能，唯一数据入口为 Excel 导入 (UPSERT 覆盖机制)。
#   2. [领导报表] 导出功能自动注入“部门小计”与“总计”行，表头极度纯净，即下即用。
#   3. [高容错导入] 数值列空白自动填充 0.0，杜绝 NaN 导致底层数学运算崩溃。
# ==============================================================================

import streamlit as st
import pandas as pd
import sqlite3
import os
import io
from datetime import datetime

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


# 严格映射：中文表头 <-> 数据库字段 (全颗粒度终极版)
# 定义前端中文表头与底层数据库字段名称的严格映射字典
LEDGER_MAP = {
    # 时空锚点区字段映射
    '核算月份': 'cost_month',
    # 员工工号字段映射
    '工号': 'emp_id',
    # 员工姓名字段映射
    '姓名': 'emp_name',
    # 归属部门字段映射
    '归属部门': 'dept_name',
    # 人员状态字段映射
    '人员状态': 'emp_status',

    # 岗位工资区字段映射
    '岗位工资': 'base_salary',
    # 工龄工资金额映射
    '工龄工资': 'seniority_pay',
    # 综合补贴金额映射
    '综合补贴': 'comp_subsidy',
    # 岗位绩效浮动补贴金额映射
    '岗位绩效浮动补贴': 'perf_float_subsidy',
    # 通讯费报销金额映射
    '通讯费': 'telecom_subsidy',
    # 其他岗位工资金额映射
    '其他岗位工资': 'other_base_pay',
    # 实习补贴金额映射
    '实习补贴': 'intern_subsidy',
    # 高校毕业生及专家津贴金额映射
    '高校毕业生/专家津贴': 'grad_allowance',

    # 绩效工资标准金额映射，作为参考项
    '绩效工资标准(参考)': 'perf_standard',
    # KPI得分数值映射，作为参考项
    'KPI得分(参考)': 'kpi_score',
    # 实际核算出的考核绩效金额映射
    '考核绩效': 'eval_perf_pay',
    # 提成绩效金额映射
    '提成绩效': 'commission_pay',
    # 其他各项月度绩效金额映射
    '其他月度绩效': 'other_month_perf',

    # 专项奖包含考勤扣罚金额映射
    '专项奖(含考勤扣罚)': 'special_award',
    # 年终一次性绩效奖金额映射
    '年终绩效奖': 'year_end_bonus',
    # 其他各项专项奖金额映射
    '其他专项奖': 'other_special_award',
    # 整个工资应发部分的最终合计数映射
    '工资应发合计': 'gross_salary_total',

    # 个人承担的养老保险扣减项映射
    '养老保险-个人': 'pension_personal',
    # 个人承担的医疗保险扣减项映射
    '医疗保险-个人': 'medical_personal',
    # 个人承担的失业保险扣减项映射
    '失业保险-个人': 'unemployment_personal',
    # 个人承担的住房公积金扣减项映射
    '住房公积金-个人': 'provident_fund_personal',
    # 个人承担的企业年金扣减项映射
    '企业年金-个人': 'annuity_personal',
    # 日常综合所得相关的个人所得税代扣金额映射
    '个税-日常': 'tax_personal_month',
    # 年终奖单独计税相关的个人所得税代扣金额映射
    '个税-年终奖': 'tax_personal_bonus',
    # 扣除各项后员工实际收到的最终薪资金额映射
    '个人实发': 'net_salary',

    # 企业承担的养老保险统筹成本映射
    '养老保险-企业': 'pension_company',
    # 企业承担的医疗保险统筹成本映射
    '医疗保险-企业': 'medical_company',
    # 企业承担的失业保险统筹成本映射
    '失业保险-企业': 'unemployment_company',
    # 企业承担的工伤保险统筹成本映射
    '工伤保险-企业': 'work_injury_company',
    # 企业承担的生育保险统筹成本映射
    '生育保险-企业': 'maternity_company',
    # 企业承担的住房公积金成本映射
    '住房公积金-企业': 'provident_fund_company',
    # 企业承担的企业年金成本映射
    '企业年金-企业': 'annuity_company',

    # 日常用餐分摊成本映射
    '日常用餐': 'meal_daily',
    # 加班用餐分摊成本映射
    '加班用餐': 'meal_ot',
    # 员工慰问费用成本映射
    '员工慰问费': 'welfare_condolence',
    # 独生子女专项补贴成本映射
    '独生子女补贴': 'welfare_single_child',
    # 员工常规体检费用成本映射
    '员工体检费': 'welfare_health_check',
    # 员工入职体检费用报销成本映射
    '入职体检': 'welfare_entry_check',
    # 其他未明确分类的临时福利成本映射
    '其他福利': 'welfare_other',
    # 专项防暑降温补贴成本映射
    '防暑降温费': 'allowance_heat',
    # 女工专属劳保补贴成本映射
    '女工劳保费': 'allowance_women',
    # 补充医疗保险统筹成本映射
    '补充医保费': 'medical_supplement',
    # 划拨的专项工会经费映射
    '工会经费': 'union_funds',
    # 提取的职工教育经费映射
    '职工教育经费': 'edu_funds',

    # 用于人工干预抹平几分钱算力误差的金额映射
    '经费尾差微调': 'cost_adjustment',

    # [新增核心字段] 记录所有非发给个人的其他人工成本总计映射
    '其他人工成本合计': 'other_cost_total',

    # 最终的单人当月总人工成本合计映射
    '人工成本合计': 'total_labor_cost'
}
DB_TO_CN_MAP = {v: k for k, v in LEDGER_MAP.items()}

# 金额类字段列表 (导入时用于强制转 0.0)
NUMERIC_COLS = list(LEDGER_MAP.keys())[5:]


# ==============================================================================
# 小计与总计运算引擎 (复用模块)
# ==============================================================================
def add_subtotals_and_totals(df, numeric_cols):
    if df.empty: return df
    final_rows = []

    # 按照部门进行分组，保持原有排序
    dept_groups = df.groupby('归属部门', sort=False)

    for dept_name, group in dept_groups:
        final_rows.append(group)  # 塞入该部门的所有员工明细

        # 计算该部门的小计
        subtotal = pd.Series(index=group.columns, dtype='object')
        subtotal['归属部门'] = dept_name
        subtotal['姓名'] = '【部门小计】'
        for col in numeric_cols:
            if col in group.columns: subtotal[col] = group[col].sum()
        final_rows.append(pd.DataFrame([subtotal]))

    # 计算全公司总计
    grand_total = pd.Series(index=df.columns, dtype='object')
    grand_total['归属部门'] = '【全公司】'
    grand_total['姓名'] = '【总计】'
    for col in numeric_cols:
        if col in df.columns: grand_total[col] = df[col].sum()
    final_rows.append(pd.DataFrame([grand_total]))

    return pd.concat(final_rows, ignore_index=True)

# ==============================================================================
# UI 消息锁
# ==============================================================================
if 'ledger_msg' in st.session_state:
    if st.session_state.ledger_msg_type == 'success':
        st.success(st.session_state.ledger_msg)
    else:
        st.error(st.session_state.ledger_msg)
    del st.session_state.ledger_msg, st.session_state.ledger_msg_type


def set_msg(msg, type='success'):
    st.session_state.ledger_msg = msg
    st.session_state.ledger_msg_type = type
    st.rerun()


# ==============================================================================
# 数据提取逻辑
# ==============================================================================
def get_ledger_data(month_filter=None, dept_filter=None):
    conn = _get_db_connection()
    try:
        # 1. 初始化基础查询语句与参数列表
        query = "SELECT * FROM labor_cost_ledger WHERE 1=1"
        params = []

        # 2. 动态拼装核算月份的预编译条件
        if month_filter:
            query += " AND cost_month = ?"
            params.append(month_filter)

        # 3. 动态拼装归属部门的预编译条件 (支持多选)
        if dept_filter:
            # 根据选择的部门数量，动态生成对应数量的占位符 '?'
            placeholders = ",".join(['?'] * len(dept_filter))
            query += f" AND dept_name IN ({placeholders})"
            params.extend(dept_filter)

        # 4. 追加排序规则：月份倒序，部门正序
        query += " ORDER BY cost_month DESC, dept_name ASC"

        # 5. [核心修复点] 将动态拼装好的 params 列表传递给 Pandas 引擎，完成安全绑定
        df = pd.read_sql_query(query, conn, params=params)

        return df
    finally:
        conn.close()


# ==============================================================================
# 页面框架
# ==============================================================================
st.title("💰 人工成本台账管理中心")
st.caption(
    "🔒 财务数据合规要求：台账一旦生成不可在系统内手动篡改。如需修正，请在 Excel 中修改后重新导入，系统将自动覆盖重置原账目。")

tab1, tab2, tab3 = st.tabs(["📊 台账多维看板", "📤 领导审阅导出 (含汇总)", "📥 财务底表导入"])

# ------------------------------------------------------------------------------
# Tab 1: 台账多维看板
# ------------------------------------------------------------------------------
with tab1:
    conn = _get_db_connection()
    available_months = \
    pd.read_sql_query("SELECT DISTINCT cost_month FROM labor_cost_ledger ORDER BY cost_month DESC", conn)[
        'cost_month'].tolist()
    available_depts = pd.read_sql_query("SELECT DISTINCT dept_name FROM labor_cost_ledger", conn)['dept_name'].tolist()
    conn.close()

    sc1, sc2 = st.columns(2)
    with sc1:
        f_month = st.selectbox("📅 核算月份筛选", ["全部月份"] + available_months)
    with sc2:
        f_dept = st.multiselect("🏢 归属部门筛选", options=available_depts)

    raw_df = get_ledger_data(month_filter=None if f_month == "全部月份" else f_month,
                             dept_filter=f_dept if f_dept else None)

    if not raw_df.empty:
        # 顶层指标卡片
        total_cost = raw_df['total_labor_cost'].sum()
        total_gross = raw_df['gross_salary_total'].sum()
        total_headcount = len(raw_df)

        m1, m2, m3 = st.columns(3)
        m1.metric("当期总人工成本 (元)", f"{total_cost:,.2f}")
        m2.metric("当期总工资应发 (元)", f"{total_gross:,.2f}")
        m3.metric("发薪/核算总人次", f"{total_headcount} 人次")

        # 核心数据流展示 (替换为纯净中文表头)
        disp_df = raw_df.rename(columns=DB_TO_CN_MAP)
        st.dataframe(disp_df[[col for col in LEDGER_MAP.keys() if col in disp_df.columns]], use_container_width=True,
                     hide_index=True)
    else:
        st.info("💡 当前筛选条件下暂无台账数据。")

# ------------------------------------------------------------------------------
# Tab 2: 领导审阅导出 (多 Sheet 动态汇总版)
# ------------------------------------------------------------------------------
with tab2:
    st.subheader("📤 生成向领导汇报的标准台账")
    st.write("系统将导出包含多个 Sheet 的 Excel 文件：首页为选中月份的【累计汇总】，后续为各单月的明细表。且均已插入部门小计与总计。")

    # [核心升级] 支持任意月份的自由组合筛选
    selected_months = st.multiselect("📅 选择要导出的核算月份 (可多选、可跨年)", options=available_months, default=available_months[:1] if available_months else [])

    if st.button("🚀 一键生成并下载多 Sheet 报表", type="primary"):
        if not selected_months:
            st.warning("⚠️ 请至少在上方选择一个核算月份！")
        else:
            # 1. 一次性把选中月份的所有数据捞出来
            conn = _get_db_connection()
            placeholders = ",".join(["?"] * len(selected_months))
            query = f"SELECT * FROM labor_cost_ledger WHERE cost_month IN ({placeholders}) ORDER BY dept_name ASC"
            raw_export_df = pd.read_sql_query(query, conn, params=selected_months)
            conn.close()

            if not raw_export_df.empty:
                ob = io.BytesIO()
                # 启用 ExcelWriter，支持写入多个 Sheet
                with pd.ExcelWriter(ob, engine='openpyxl') as writer:

                    # ======= Sheet 1: 生成指定时段的【累计汇总】 =======
                    # 确定哪些数据库字段需要求和
                    db_num_cols = [LEDGER_MAP[c] for c in NUMERIC_COLS if c in LEDGER_MAP]
                    agg_dict = {col: 'sum' for col in db_num_cols}
                    # 姓名和部门取第一个，状态取最后一次快照的状态
                    agg_dict.update({'emp_name': 'first', 'dept_name': 'first', 'emp_status': 'last'})

                    # 按照工号聚合
                    summary_df = raw_export_df.groupby('emp_id').agg(agg_dict).reset_index()
                    summary_cn = summary_df.rename(columns=DB_TO_CN_MAP)
                    report_cols = [c for c in LEDGER_MAP.keys() if c in summary_cn.columns and c != '核算月份']
                    summary_cn = summary_cn[report_cols]

                    # 挂载小计和总计，并写入 Sheet 1
                    summary_final = add_subtotals_and_totals(summary_cn, NUMERIC_COLS)
                    summary_final.to_excel(writer, index=False, sheet_name='累计汇总')

                    # ======= Sheet 2~N: 生成各单月的【明细表】 =======
                    # 按照选中的月份顺序列出
                    for month in sorted(selected_months):
                        month_df = raw_export_df[raw_export_df['cost_month'] == month].copy()
                        if not month_df.empty:
                            month_cn = month_df.rename(columns=DB_TO_CN_MAP)
                            # 单月表需要显示核算月份
                            month_cols = [c for c in LEDGER_MAP.keys() if c in month_cn.columns]
                            month_cn = month_cn[month_cols]

                            # 挂载小计和总计，准备写入后续 Sheet
                            month_final = add_subtotals_and_totals(month_cn, NUMERIC_COLS)

                            # [核心防御] 清洗 Excel Sheet 命名中的非法字符，并限制总长度不超过微软的 31 字符上限
                            safe_month = str(month).replace(':', '-').replace('/', '-').replace('\\', '-').replace('?',
                                                                                                                   '').replace(
                                '*', '')
                            # '明细' 占 2 个字符，为防止名字太长导致 Excel 崩溃，截取前 28 个字符
                            safe_sheet_name = f"{safe_month[:28]}明细"

                            month_final.to_excel(writer, index=False, sheet_name=safe_sheet_name)

                # 4. 推送下载
                file_name = f"人工成本台账汇报_{len(selected_months)}个月汇总.xlsx"
                st.download_button("📥 点击下载多 Sheet 财务报表", data=ob.getvalue(), file_name=file_name, type="secondary")
            else:
                st.warning("所选范围内无数据。")

# ------------------------------------------------------------------------------
# Tab 3: 财务底表导入 (核心逻辑：高容错与 UPSERT 覆盖)
# ------------------------------------------------------------------------------
with tab3:
    st.subheader("📥 历史财务数据导入引擎")

    tc1, tc2 = st.columns(2)
    with tc1:
        st.write(
            "请先下载标准模板。系统依靠 **`核算月份`** 和 **`工号`** 确认数据。如果发现错漏，直接在 Excel 修改后重新上传，系统将自动覆盖。")
        template_df = pd.DataFrame(columns=list(LEDGER_MAP.keys()))
        tout = io.BytesIO()
        with pd.ExcelWriter(tout, engine='openpyxl') as w: template_df.to_excel(w, index=False)
        st.download_button("下载标准导入模板", data=tout.getvalue(), file_name="人工成本台账导入模板.xlsx")

    with tc2:
        up_file = st.file_uploader("上传已填写的台账 Excel", type=["xlsx"])
        if up_file and st.button("🚀 执行导入与数据库覆盖"):
            in_df = pd.read_excel(up_file)

            conn = _get_db_connection()
            cursor = conn.cursor()

            success_count = 0
            err_logs = []

            try:
                for idx, row in in_df.iterrows():
                    # 校验核心双主键
                    c_month = str(row.get('核算月份', '')).strip()
                    e_id = str(row.get('工号', '')).strip()

                    if not c_month or not e_id or c_month == 'nan' or e_id == 'nan':
                        err_logs.append(f"行 {idx + 2}: 核算月份或工号缺失，跳过。")
                        continue

                    # 构建写入字典，缺失的数值列一律填充为 0.0
                    db_data = {}
                    for cn_col, db_col in LEDGER_MAP.items():
                        val = row.get(cn_col)
                        if db_col in ['cost_month', 'emp_id', 'emp_name', 'dept_name', 'emp_status']:
                            db_data[db_col] = str(val).strip() if pd.notna(val) else ""
                        else:
                            try:
                                db_data[db_col] = float(val) if pd.notna(val) else 0.0
                            except:
                                db_data[db_col] = 0.0

                    # 动态构建 SQLite 的 UPSERT 语句 (ON CONFLICT DO UPDATE)
                    cols = list(db_data.keys())
                    placeholders = ",".join(["?"] * len(cols))
                    updates = ",".join([f"{c}=excluded.{c}" for c in cols if c not in ['cost_month', 'emp_id']])

                    sql = f"""
                        INSERT INTO labor_cost_ledger ({','.join(cols)})
                        VALUES ({placeholders})
                        ON CONFLICT(cost_month, emp_id) 
                        DO UPDATE SET {updates}
                    """
                    cursor.execute(sql, tuple(db_data.values()))
                    success_count += 1

                conn.commit()
                if err_logs: st.warning("部分记录未成功:\n" + "\n".join(err_logs))
                set_msg(f"台账导入/覆盖完成！成功处理 {success_count} 条记录。")
            except Exception as e:
                conn.rollback()
                st.error(f"导入底层崩溃: {e}")
            finally:
                conn.close()