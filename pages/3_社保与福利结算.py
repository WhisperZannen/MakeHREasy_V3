# ==============================================================================
# 文件路径: pages/3_社保与福利结算.py
# 功能描述: 多主体社保与福利结算中枢 (MVC 架构前端 UI 层 - 终极业务对齐版)
# 核心改造:
#   1. Tab 1: 业务流重塑 (基数核对 -> 当期核算 -> 历史补缴与滞纳金对账入库)。
#   2. Tab 2: 财务输出中心 (对内单月 5 通道流水账 + 对外地市公对公跨期结算函)。
#   3. 彻底废弃伪需求分支表，全量依赖 cost_center (财务归属) 作为公对公结算锚点。
# ==============================================================================

import streamlit as st
import pandas as pd
import io
import uuid

# 导入底层接口
from modules.core_social_security import (
    get_policy_rules,
    upsert_policy_rules,
    _get_db_connection,
    batch_update_emp_matrix
)

st.set_page_config(page_title="社保与福利结算", layout="wide")

# ==============================================================================
# [核心防御机制] 空值清洗器
# ==============================================================================
def safe_float(val, default=0.0):
    try:
        if pd.notna(val) and val is not None and str(val).strip() != '':
            return float(val)
        return default
    except Exception:
        return default

# ==============================================================================
# [状态机初始化]
# ==============================================================================
if 'show_confirm' not in st.session_state: st.session_state['show_confirm'] = False
if 'pending_params' not in st.session_state: st.session_state['pending_params'] = None
if 'scan_locked' not in st.session_state: st.session_state['scan_locked'] = False

# ==============================================================================
# 页面主框架与导航
# ==============================================================================
st.title("🛡️ 社保与福利结算中心")
st.caption("核心业务流向：当月基数备料 ➡️ 理论核算与补缴对账 ➡️ 跨主体结算与公对公要款 ➡️ 引擎底座配置")

tab1, tab2, tab3 = st.tabs(["🧮 当月社保沙盘 (含补缴)", "📤 财务提款与公对公结算", "⚙️ 全局规则与参数配置"])

# ------------------------------------------------------------------------------
# Tab 1: 当月社保沙盘与对账池
# ------------------------------------------------------------------------------
with tab1:
    st.info("💡 业务铁律：先在【第一步】确保所有人基数就绪，再在【第二步】跑出当期理论账单，最后在【第三步】补录官方滞纳金与历史补缴差额。")

    # 核心输入：确定要算哪个月的账
    calc_month = st.text_input("📅 输入当前核算工作月份 (格式: YYYY-MM，如 2026-03)", value="2026-03", max_chars=7)

    # ==========================================
    # 第一步：基数极速抢救与特例通道 (移至顶部)
    # ==========================================
    st.subheader("🛠️ 第一步：基数初始化与特例抢救")
    conn = _get_db_connection()
    try:
        detect_sql = """
            SELECT 
                e.emp_id AS '工号', e.name AS '姓名', d.dept_name AS '部门', e.status AS '人事状态',
                IFNULL(m.cost_center, '本级') AS '财务归属', IFNULL(m.base_salary_avg, 0.0) AS '已录入原始基数',
                IFNULL(m.fund_base_avg, 0.0) AS '独立公积金基数(选填)',
                IFNULL(m.pension_enabled, 1) AS '养老参保(1是0否)', IFNULL(m.pension_account, '省公众') AS '养老缴纳主体',
                IFNULL(m.medical_enabled, 1) AS '医疗参保(1是0否)', IFNULL(m.medical_account, '省公司') AS '医疗缴纳主体',
                IFNULL(m.unemp_enabled, 1) AS '失业参保(1是0否)', IFNULL(m.unemp_account, '省公众') AS '失业缴纳主体',
                IFNULL(m.injury_enabled, 1) AS '工伤参保(1是0否)', IFNULL(m.injury_account, '省公众') AS '工伤缴纳主体',
                IFNULL(m.maternity_enabled, 1) AS '生育参保(1是0否)', IFNULL(m.maternity_account, '省公司') AS '生育缴纳主体',
                IFNULL(m.fund_enabled, 1) AS '公积金参保(1是0否)', IFNULL(m.fund_account, '省公众') AS '公积金缴纳主体',
                IFNULL(m.annuity_enabled, 0) AS '年金参保(1是0否)', IFNULL(m.annuity_account, '省公司') AS '年金缴纳主体'
            FROM employees e
            LEFT JOIN departments d ON e.dept_id = d.dept_id
            LEFT JOIN ss_emp_matrix m ON e.emp_id = m.emp_id
            WHERE e.status IN ('在职', '挂靠人员')
        """
        roster_df = pd.read_sql_query(detect_sql, conn)
    finally:
        conn.close()

    c_down, c_up = st.columns(2)
    with c_down:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            roster_df.to_excel(writer, index=False, sheet_name='基数名单')
        st.download_button("📥 1. 下载当期参保人员基数名单", data=buffer.getvalue(), file_name=f"全员基数表_{calc_month}.xlsx", mime="application/vnd.ms-excel")
    with c_up:
        uploaded_file = st.file_uploader("📤 2. 上传填好基数/修改过开关的 Excel", type=["xlsx", "csv"], label_visibility="collapsed")
        if uploaded_file is not None:
            if st.session_state.get('last_processed_file_id') != uploaded_file.file_id:
                try:
                    upload_df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
                    success, msg = batch_update_emp_matrix(upload_df)
                    if success:
                        st.session_state['last_processed_file_id'] = uploaded_file.file_id
                        st.rerun()
                    else: st.error(msg)
                except Exception as e: st.error(f"❌ 读取文件失败: {e}")
            else: st.success("✅ 基数已成功灌入引擎，可以开始核算！")

    st.divider()

    # ==========================================
    # 第二步：正常算力引擎与当期对账
    # ==========================================
    st.subheader("🧮 第二步：本月正常参保核算")
    rule_year_to_use = st.selectbox("⚙️ 选择本次套用的【规则年度】(如次年6月前沿用上年规则)", ["2024", "2025", "2026", "2027", "2028"], index=1)

    if st.button("🚀 启动引擎，生成当期理论账单", type="primary"):
        missing_count = len(roster_df[roster_df['已录入原始基数'] == 0.0])
        if missing_count > 0:
            st.error(f"🚨 警告：发现 {missing_count} 名员工基数为 0，请在第一步补充完整！")
        else:
            all_bills = []
            from modules.core_social_security import calculate_complete_bill
            for _, row in roster_df.iterrows():
                all_bills.append(calculate_complete_bill(row.to_dict(), rule_year_to_use))
            st.session_state['temp_bills'] = pd.DataFrame(all_bills)

    if 'temp_bills' in st.session_state:
        # 这是底层引擎吐出的生肉数据（带半英文，供固化入库使用）
        raw_df_preview = st.session_state['temp_bills']

        # [核心修复] 创建一个专门用于前端展示和导出的副本进行大清洗
        export_df = raw_df_preview.copy()

        # 1. 物理铲除违背常识的“个人工伤”和“个人生育”幻影列
        cols_to_drop = ['injury_个', 'maternity_个']
        export_df = export_df.drop(columns=[c for c in cols_to_drop if c in export_df.columns])

        # 2. 表头全面汉化字典
        audit_rename_map = {
            'pension_企': '养老(企业)', 'pension_个': '养老(个人)', 'pension_route': '养老缴纳主体',
            'medical_企': '医疗(企业)', 'medical_个': '医疗(个人)', 'medical_route': '医疗缴纳主体',
            'unemp_企': '失业(企业)', 'unemp_个': '失业(个人)', 'unemp_route': '失业缴纳主体',
            'injury_企': '工伤(企业)', 'injury_route': '工伤缴纳主体',
            'maternity_企': '生育(企业)', 'maternity_route': '生育缴纳主体',
            'fund_企': '公积金(企业)', 'fund_个': '公积金(个人)', 'fund_route': '公积金缴纳主体',
            'annuity_企': '年金(企业)', 'annuity_个': '年金(个人)', 'annuity_route': '年金缴纳主体'
        }
        export_df = export_df.rename(columns=audit_rename_map)

        # 调整一下列的前后顺序，把总计放前面，明细放后面
        ordered_front_cols = ['工号', '姓名', '财务归属', '合计企业缴纳', '合计个人扣款']
        detail_cols = [c for c in export_df.columns if c not in ordered_front_cols]
        export_df = export_df[ordered_front_cols + detail_cols]

        # ==========================================
        # 1. 找回审计探针：精准搜索过滤
        # ==========================================
        search_query = st.text_input("🔍 抽查指定员工 (输入姓名或工号进行过滤审核)", "")
        display_df = export_df
        if search_query:
            display_df = display_df[
                display_df['姓名'].str.contains(search_query, na=False) |
                display_df['工号'].str.contains(search_query, na=False)
            ]

        st.dataframe(display_df[['工号', '姓名', '财务归属', '合计企业缴纳', '合计个人扣款']], use_container_width=True, hide_index=True)

        st.write("---")
        c_audit, c_save = st.columns(2)

        with c_audit:
            # ==========================================
            # 2. 找回全量下载：全中文表头，供线下复核
            # ==========================================
            buffer_audit = io.BytesIO()
            with pd.ExcelWriter(buffer_audit, engine='xlsxwriter') as writer:
                export_df.to_excel(writer, index=False, sheet_name='当月核算全量底稿')

            st.download_button(
                label="📥 1. 下载全量明细底稿 (全中文，供线下复核)",
                data=buffer_audit.getvalue(),
                file_name=f"当期核算明细底稿_{calc_month}.xlsx",
                type="secondary"
            )

        with c_save:
            # ==========================================
            # 3. 固化入库操作
            # ==========================================
            if st.button("💾 2. 线下复核无误，将当期明细固化入库", type="primary"):
                from modules.core_social_security import save_monthly_ss_records
                # 【防线保证】这里传给底层的依然是带着英文主键的 raw_df_preview，绝对不会导致入库报错！
                success, msg = save_monthly_ss_records(raw_df_preview, calc_month)
                if success:
                    st.success(msg)
                    del st.session_state['temp_bills']
                    st.rerun()
                else:
                    st.error(msg)

    # ==========================================
    # 第三步：官方补缴与滞纳金手工入库通道
    # ==========================================
    st.subheader("📥 第三步：补缴与滞纳金手工入账 (对齐官方核定单)")
    st.write("🔧 遇到历史跨月补缴、滞纳金等系统无法自动推演的账目，请在此按社保局单据直接填报写入。")

    retro_cols = ['处理月份(即本月)', '工号', '补缴起始月', '补缴结束月', '补缴类型(如:跨年补差)', '企业本金合计', '个人本金合计', '企业承担滞纳金', '备注']
    rc1, rc2 = st.columns(2)
    with rc1:
        retro_template = pd.DataFrame(columns=retro_cols)
        retro_buffer = io.BytesIO()
        with pd.ExcelWriter(retro_buffer, engine='xlsxwriter') as writer: retro_template.to_excel(writer, index=False)
        st.download_button("📥 下载补缴与滞纳金导入模板", data=retro_buffer.getvalue(), file_name=f"补缴导入模板_{calc_month}.xlsx")

    with rc2:
        retro_file = st.file_uploader("📤 上传已填好的补缴核定单", type=["xlsx", "csv"], label_visibility="collapsed")
        if retro_file and st.button("💾 将补缴数据强行入库"):
            try:
                r_df = pd.read_csv(retro_file) if retro_file.name.endswith('.csv') else pd.read_excel(retro_file)
                conn = _get_db_connection()
                cursor = conn.cursor()
                # 写入底层的 ss_retroactive_records
                sql = """
                    INSERT INTO ss_retroactive_records (
                        retro_id, process_month, emp_id, target_start_month, target_end_month, retro_type,
                        total_comp_retro, total_pers_retro, late_fee, remarks
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                count = 0
                for _, row in r_df.iterrows():
                    eid = str(row.get('工号', '')).replace('.0', '').strip()
                    if not eid or eid == 'nan': continue
                    cursor.execute(sql, (
                        str(uuid.uuid4())[:12], str(row.get('处理月份(即本月)', calc_month)).strip(), eid,
                        str(row.get('补缴起始月', '')), str(row.get('补缴结束月', '')), str(row.get('补缴类型(如:跨年补差)', '手工补缴')),
                        safe_float(row.get('企业本金合计', 0.0)), safe_float(row.get('个人本金合计', 0.0)),
                        safe_float(row.get('企业承担滞纳金', 0.0)), str(row.get('备注', ''))
                    ))
                    count += 1
                conn.commit()
                st.success(f"✅ 成功将 {count} 笔特殊补缴与滞纳金记录封印入库！后续台账与工资计算将自动识别叠加。")
            except Exception as e:
                st.error(f"❌ 写入补缴表失败: {e}")
            finally:
                if 'conn' in locals(): conn.close()

# ------------------------------------------------------------------------------
# Tab 2: 财务输出中心 (对内账单与对外要款)
# ------------------------------------------------------------------------------
with tab2:
    st.subheader("📤 第一部分：对内审批提款单 (单月精准切割)")
    st.info("💡 财务内部走账专用。系统直接从数据库拉取【已固化入库】的数据，并严格按照缴费通道劈成 5 张干净明细表。")

    conn = _get_db_connection()
    available_months = pd.read_sql_query("SELECT DISTINCT cost_month FROM ss_monthly_records ORDER BY cost_month DESC", conn)['cost_month'].tolist()
    conn.close()

    internal_month = st.selectbox("📅 选择要出具对内账单的月份", options=available_months if available_months else ["无数据"])

    if st.button("🚀 生成对内 6 大请款与审计明细表", type="primary") and internal_month != "无数据":
        conn = _get_db_connection()
        # 1. 抓取正常当期账单
        query = """
            SELECT r.*, e.name AS '姓名' 
            FROM ss_monthly_records r 
            LEFT JOIN employees e ON r.emp_id = e.emp_id 
            WHERE r.cost_month = ?
        """
        raw_df = pd.read_sql_query(query, conn, params=[internal_month])

        # [核心新增] 2. 抓取该月的异常补缴与滞纳金账单
        retro_query = """
            SELECT r.*, e.name AS '姓名', IFNULL(m.cost_center, '本级') AS '财务归属'
            FROM ss_retroactive_records r
            LEFT JOIN employees e ON r.emp_id = e.emp_id
            LEFT JOIN ss_emp_matrix m ON r.emp_id = m.emp_id
            WHERE r.process_month = ?
        """
        retro_df = pd.read_sql_query(retro_query, conn, params=[internal_month])
        conn.close()

        if not raw_df.empty or not retro_df.empty:
            buffer_internal = io.BytesIO()
            with pd.ExcelWriter(buffer_internal, engine='xlsxwriter') as writer:
                rename_map = {
                    'emp_id': '工号', 'cost_center': '财务归属',
                    'pension_comp': '养老(企业)', 'pension_pers': '养老(个人)',
                    'medical_comp': '医疗(企业)', 'medical_pers': '医疗(个人)',
                    'unemp_comp': '失业(企业)', 'unemp_pers': '失业(个人)',
                    'injury_comp': '工伤(企业)', 'maternity_comp': '生育(企业)',
                    'fund_comp': '公积金(企业)', 'fund_pers': '公积金(个人)',
                    'annuity_comp': '年金(企业)', 'annuity_pers': '年金(个人)'
                }

                def export_channel_sheet(sheet_name, channel, items):
                    df_sub = raw_df.copy()
                    if df_sub.empty: return
                    has_amt = pd.Series([False] * len(df_sub))
                    cols = ['emp_id', '姓名', 'cost_center']
                    for it in items:
                        mask = df_sub[f'{it}_route'] == channel
                        if f'{it}_comp' in df_sub.columns: df_sub.loc[~mask, f'{it}_comp'] = 0.0
                        if f'{it}_pers' in df_sub.columns: df_sub.loc[~mask, f'{it}_pers'] = 0.0

                        has_amt = has_amt | (df_sub[f'{it}_comp'] > 0)
                        if f'{it}_pers' in df_sub.columns: has_amt = has_amt | (df_sub[f'{it}_pers'] > 0)

                        cols.append(f'{it}_comp')
                        if f'{it}_pers' in df_sub.columns: cols.append(f'{it}_pers')

                    df_sub = df_sub[has_amt]
                    if not df_sub.empty:
                        df_sub[cols].rename(columns=rename_map).to_excel(writer, index=False, sheet_name=sheet_name)

                # 生成前 5 张正常的内部走交流程单
                export_channel_sheet('1.中电数智(养_失_工)', '中电数智', ['pension', 'unemp', 'injury'])
                export_channel_sheet('2.省公司(年金专表)', '省公司', ['annuity'])
                export_channel_sheet('3.省公司(医_生_工)', '省公司', ['medical', 'maternity', 'injury'])
                export_channel_sheet('4.省公众(养_失_工)', '省公众', ['pension', 'unemp', 'injury'])

                df_gjj = raw_df.copy()
                if not df_gjj.empty:
                    df_gjj = df_gjj[(df_gjj['fund_comp'] > 0) | (df_gjj['fund_pers'] > 0)]
                    if not df_gjj.empty:
                        cols_gjj = ['emp_id', '姓名', 'cost_center', 'fund_comp', 'fund_pers', 'fund_route']
                        df_gjj[cols_gjj].rename(columns=rename_map).rename(columns={'fund_route': '公积金缴纳主体'}).to_excel(writer, index=False, sheet_name='5.公积金(专表)')

                # =================================================================
                # [核心新增] 第 6 张表：异常补缴与滞纳金专项审批单
                # =================================================================
                if not retro_df.empty:
                    retro_map = {
                        'emp_id': '工号', 'retro_type': '补缴险种', 'target_start_month': '补缴起始月',
                        'target_end_month': '补缴结束月', 'total_comp_retro': '企业本金',
                        'total_pers_retro': '个人本金', 'late_fee': '滞纳金(异常支出)', 'remarks': '产生原因(备注)'
                    }
                    retro_cols = ['工号', '姓名', '财务归属', '补缴险种', '补缴起始月', '补缴结束月', '企业本金', '个人本金', '滞纳金(异常支出)', '产生原因(备注)']
                    df_retro_export = retro_df.rename(columns=retro_map)[retro_cols]
                    df_retro_export.to_excel(writer, index=False, sheet_name='6.异常款项专项审批')

                    # 强力审计渲染：自动将滞纳金列标红，加宽备注列
                    workbook = writer.book
                    worksheet = writer.sheets['6.异常款项专项审批']
                    alert_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'bold': True})
                    worksheet.set_column('I:I', 16, alert_format) # 滞纳金列高亮标红
                    worksheet.set_column('J:J', 35) # 产生原因列拉宽，方便领导审阅

            st.download_button(f"📥 下载 {internal_month} 内部提款与审计对账单", data=buffer_internal.getvalue(), file_name=f"对内提款明细_{internal_month}.xlsx", type="secondary")

    # ==========================================
    # 第二部分：对外公对公结算 (跨期，按地市切割)
    # ==========================================
    st.subheader("📜 第二部分：对外公对公结算函 (向地市分公司索款)")
    st.write("🔧 系统自动抽取指定时间段内，`财务归属` 为各地市分公司的人员，并**严格按缴费通道切分**独立请款账单。")

    ec1, ec2 = st.columns(2)
    with ec1: start_month = st.selectbox("⏳ 结算起始月", options=available_months if available_months else ["无数据"], key='s_month')
    with ec2: end_month = st.selectbox("⏳ 结算结束月", options=available_months if available_months else ["无数据"], key='e_month')

    if st.button("🚀 生成各地市分公司专属结算函大包", type="primary") and start_month != "无数据":
        conn = _get_db_connection()
        # 抓取时间段内，所有非本级的代缴数据
        ext_query = """
            SELECT r.*, e.name AS '姓名' 
            FROM ss_monthly_records r 
            LEFT JOIN employees e ON r.emp_id = e.emp_id 
            WHERE r.cost_month >= ? AND r.cost_month <= ? AND r.cost_center != '本级'
        """
        ext_df = pd.read_sql_query(ext_query, conn, params=[start_month, end_month])
        conn.close()

        if ext_df.empty:
            st.warning("该时间段内没有非本级（代缴）的核算数据。")
        else:
            buffer_ext = io.BytesIO()
            with pd.ExcelWriter(buffer_ext, engine='xlsxwriter') as writer:
                # 按财务归属（例如：孝感分公司）进行循环打包
                for cc, group in ext_df.groupby('cost_center'):

                    # 侦测该分公司的人，到底分布在哪些资金通道里（省公众、中电数智等）
                    routes_used = set()
                    for r_col in ['pension_route', 'medical_route', 'unemp_route', 'injury_route', 'maternity_route', 'fund_route', 'annuity_route']:
                        routes_used.update(group[r_col].dropna().unique())
                    routes_used.discard('')
                    routes_used.discard('不参保')
                    routes_used.discard('None')

                    # 针对该分公司的每个实体通道，生成独立结算单
                    for route_name in routes_used:
                        df_cc_route = group.copy()
                        has_amt = pd.Series([False] * len(df_cc_route))

                        export_cols = ['cost_month', 'emp_id', '姓名']
                        total_comp_sum, total_pers_sum = 0.0, 0.0

                        # 清洗并筛选只属于该通道的金额
                        for it in ['pension', 'medical', 'unemp', 'injury', 'maternity', 'fund', 'annuity']:
                            mask = df_cc_route[f'{it}_route'] == route_name

                            c_col, p_col = f'{it}_comp', f'{it}_pers'
                            if c_col in df_cc_route.columns:
                                df_cc_route.loc[~mask, c_col] = 0.0
                                has_amt = has_amt | (df_cc_route[c_col] > 0)
                                export_cols.append(c_col)
                                total_comp_sum += df_cc_route[c_col].sum()

                            if p_col in df_cc_route.columns:
                                df_cc_route.loc[~mask, p_col] = 0.0
                                has_amt = has_amt | (df_cc_route[p_col] > 0)
                                export_cols.append(p_col)
                                total_pers_sum += df_cc_route[p_col].sum()

                        df_cc_route = df_cc_route[has_amt]

                        if not df_cc_route.empty:
                            sheet_name = f"{cc[:5]}_{route_name[:5]}" # 缩短 Sheet 名防止超限
                            rename_dict = {'cost_month': '代缴月份', 'emp_id': '工号', 'pension_comp': '养老(企)', 'pension_pers': '养老(个)', 'medical_comp': '医疗(企)', 'medical_pers': '医疗(个)', 'unemp_comp': '失业(企)', 'unemp_pers': '失业(个)', 'injury_comp': '工伤(企)', 'maternity_comp': '生育(企)', 'fund_comp': '公积金(企)', 'fund_pers': '公积金(个)', 'annuity_comp': '年金(企)', 'annuity_pers': '年金(个)'}
                            df_cc_route[export_cols].rename(columns=rename_dict).to_excel(writer, index=False, sheet_name=sheet_name)

                            # 在界面打印请款制式说明
                            st.success(f"✅ 生成结算档：**{cc} - {route_name}** 通道")
                            st.code(f"致 {cc}：\n兹附上 {start_month} 至 {end_month} 期间，贵司挂靠我司【{route_name}】通道代缴社保公积金明细。\n本次结算企业统筹部分合计：{total_comp_sum:.2f} 元；个人代扣部分合计：{total_pers_sum:.2f} 元。\n总计请款额：{(total_comp_sum + total_pers_sum):.2f} 元。请按明细核对并安排打款。", language="text")

            st.download_button(f"📥 下载公对公结算明细大包 (Excel)", data=buffer_ext.getvalue(), file_name=f"公对公跨期结算单_{start_month}至{end_month}.xlsx", type="primary")

# ------------------------------------------------------------------------------
# Tab 3: 全局规则与参数配置 (未变动，保持 34550 双轨制)
# ------------------------------------------------------------------------------
with tab3:
    st.subheader("🛠️ 全局算力引擎参数设置")
    st.info("💡 此处的设置将直接决定全公司数千条社保数据的最终核算金额。")

    c_y, c_e = st.columns(2)
    with c_y: target_year = st.selectbox("📅 规则生效年度", ["2024", "2025", "2026", "2027", "2028"], index=1)
    with c_e: target_entity = st.selectbox("🏢 适用法人主体", ["全量设置", "省公众", "中电数智", "省公司"])

    fetch_entity = "省公众" if "全量" in target_entity else target_entity
    curr = get_policy_rules(target_year, fetch_entity)

    if curr: st.success(f"已加载 {target_year} 年度 【{fetch_entity}】 的历史配置，可直接修改。")
    else: st.info(f"【{fetch_entity}】 在 {target_year} 年度尚无配置记录，请录入参数后保存。")

    if st.session_state.get('show_confirm', False):
        st.warning("⚠️ 警告：您即将修改底层财务参数，请核对变更情况！")
        if st.button("🔥 确认无误，强行写入数据库", type="primary", key="confirm_upsert_btn"):
            params = st.session_state.get('pending_params')
            if params:
                success, msg = upsert_policy_rules(params, is_all_entities=("全量" in params[1]))
                if success:
                    st.success(msg)
                    st.session_state['show_confirm'] = False
                    st.rerun()
                else: st.error(f"🚨 数据库写入被底层拦截！真实原因: {msg}")
        if st.button("❌ 撤销修改，返回编辑", key="cancel_upsert_btn"):
            st.session_state['show_confirm'] = False
            st.rerun()
        st.divider()

    with st.form("policy_rules_form"):
        st.write(f"**【{target_year} 年度】算法控制总开关**")
        c_mode, c_fund, c_med = st.columns(3)
        with c_mode:
            r_keys = ['exact', 'round_to_yuan', 'round_to_ten', 'floor_to_ten']
            cur_round = curr.get('rounding_mode', 'round_to_yuan')
            sel_round = st.selectbox("社保取整规则", options=r_keys, index=r_keys.index(cur_round) if cur_round in r_keys else 1)
        with c_fund:
            f_keys = ['independent', 'reverse_from_ss']
            cur_fund = curr.get('fund_calc_method', 'reverse_from_ss')
            sel_fund = st.selectbox("公积金特殊算法", options=f_keys, index=f_keys.index(cur_fund) if cur_fund in f_keys else 1)
        with c_med:
            med_serious = st.number_input("大病医疗个人固定扣款", value=safe_float(curr.get('medical_serious_fix', 7.0)), step=1.0)

        st.divider()
        def render_ins_row(label, prefix, has_pers=True, has_limit=True):
            cols = st.columns([1.5, 2, 2, 2, 2])
            cols[0].markdown(f"**{label}**")
            up = cols[1].number_input(f"{label}封顶", value=safe_float(curr.get(f'{prefix}_upper')), step=100.0, label_visibility="collapsed") if has_limit else 0.0
            lw = cols[2].number_input(f"{label}保底", value=safe_float(curr.get(f'{prefix}_lower')), step=100.0, label_visibility="collapsed") if has_limit else 0.0
            cr = cols[3].number_input(f"{label}企%", value=safe_float(curr.get(f'{prefix}_comp_rate')), step=0.01, format="%.4f", label_visibility="collapsed")
            pr = cols[4].number_input(f"{label}个%", value=safe_float(curr.get(f'{prefix}_pers_rate')), step=0.01, format="%.4f", label_visibility="collapsed") if has_pers else 0.0
            return up, lw, cr, pr

        p_up, p_lw, p_cr, p_pr = render_ins_row("养老保险", "pension")
        m_up, m_lw, m_cr, m_pr = render_ins_row("医疗保险", "medical")
        u_up, u_lw, u_cr, u_pr = render_ins_row("失业保险", "unemp")
        i_up, i_lw, i_cr, _ = render_ins_row("工伤保险", "injury", has_pers=False)
        mat_up, mat_lw, mat_cr, _ = render_ins_row("生育保险", "maternity", has_pers=False)
        f_up, f_lw, f_cr, f_pr = render_ins_row("住房公积金(官方线)", "fund")

        cols_soe = st.columns([1.5, 2, 2, 2, 2])
        cols_soe[0].markdown("**↳ 内部实际执行线**")
        f_soe_up = cols_soe[1].number_input("内部封顶", value=safe_float(curr.get('fund_soe_upper')), step=100.0, label_visibility="collapsed")
        f_soe_lw = cols_soe[2].number_input("内部保底", value=safe_float(curr.get('fund_soe_lower')), step=100.0, label_visibility="collapsed")

        _, _, a_cr, a_pr = render_ins_row("企业年金", "annuity", has_limit=False)

        if st.form_submit_button("🔍 对比并预览修改", type="primary"):
            st.session_state['pending_params'] = (
                target_year, target_entity, sel_round, sel_fund, med_serious,
                p_up, p_lw, p_cr, p_pr, m_up, m_lw, m_cr, m_pr,
                u_up, u_lw, u_cr, u_pr, i_up, i_lw, i_cr,
                mat_up, mat_lw, mat_cr, f_up, f_lw, f_cr, f_pr,
                a_cr, a_pr, f_soe_up, f_soe_lw
            )
            st.session_state['show_confirm'] = True
            st.rerun()