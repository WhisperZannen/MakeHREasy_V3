# ==============================================================================
# 文件路径: pages/3_社保与福利结算.py
# 功能描述: 多主体社保与福利结算中枢 (MVC 架构前端 UI 层 - 终极满血防御版)
# 核心逻辑:
#   1. 彻底不写 SQL，所有数据交互强依赖 modules.core_social_security 接口。
#   2. 引入 safe_float 绝对防御机制，拦截数据库空值导致的界面渲染崩溃。
#   3. 构建高密度参数矩阵，实现 28 项基数与费率参数的一键保存。
#   4. 完美集成“多主体隔离”、“一键全量同步”与“修改确认防呆弹窗”。
#   5. [终极重构] 解绑自然年规则，引入全中文审计表头，彻底打通入库逻辑。
# ==============================================================================

import streamlit as st
import pandas as pd
import io  # 用于在内存中生成 Excel 模板，避免落地产生垃圾文件

# 导入底层接口
from modules.core_social_security import (
    get_policy_rules,
    upsert_policy_rules,
    _get_db_connection,
    batch_update_emp_matrix
)

# 页面基础配置：开启宽屏模式以容纳庞大的参数矩阵和预览大宽表
st.set_page_config(page_title="社保与福利结算", layout="wide")

# ==============================================================================
# [核心防御机制] 空值清洗器
# ==============================================================================
def safe_float(val, default=0.0):
    """
    强悍的类型转换装甲。
    业务场景：当底层数据库某个基数忘记填（NULL），传到前端会变成 None。
    如果直接用 float(None) 会导致系统当场死机。此函数可将其强制清洗为 0.0。
    """
    try:
        if pd.notna(val) and val is not None and str(val).strip() != '':
            return float(val)
        return default
    except Exception:
        return default

# ==============================================================================
# [状态机初始化] 用于拦截表单保存动作与维持 UI 状态
# ==============================================================================
# 控制 Tab 3 修改费率时的防呆警告弹窗
if 'show_confirm' not in st.session_state:
    st.session_state['show_confirm'] = False
# 暂存 Tab 3 准备提交的 28 个参数
if 'pending_params' not in st.session_state:
    st.session_state['pending_params'] = None
# 控制 Tab 1 点击“扫描”后，界面下方元素永久展开，不随组件交互而折叠
if 'scan_locked' not in st.session_state:
    st.session_state['scan_locked'] = False

# ==============================================================================
# 页面主框架与导航
# ==============================================================================
st.title("🛡️ 社保与福利结算中心")
st.caption("核心业务流向：当月人员名单核算 (含状态自动推演) ➡️ 跨主体对账单导出 ➡️ 全局参数配置底座")

# 按照日常业务使用频率，从高到低排列标签页
tab1, tab2, tab3 = st.tabs(["🧮 当月社保核算 (含补缴)", "📤 跨主体结算与对账", "⚙️ 全局规则与参数配置"])

# ------------------------------------------------------------------------------
# Tab 1: 当月社保核算 (高频核心业务)
# ------------------------------------------------------------------------------
with tab1:
    st.subheader("🧮 本月社保参保人员与费用核算")
    st.info("💡 引擎将自动读取人事档案中的【转正状态】与【离职状态】。由于系统冷启动缺少上年台账，本年度社保原始基数需手动初始化。")

    # 核心输入：确定要算哪个月的账
    calc_month = st.text_input("📅 输入核算月份 (格式: YYYY-MM，如 2026-03)", value="2026-03", max_chars=7)

    # 点击雷达扫描按钮，锁定状态并重载页面
    if st.button("📡 第一步：扫描本月参保名单与基数状态", type="primary"):
        st.session_state['scan_locked'] = True
        st.rerun()

    # 只要雷达锁开启，此区块永久显示
    if st.session_state.get('scan_locked', False):
        conn = _get_db_connection()
        try:
            # [核心 SQL] 从人事主表关联社保矩阵，全量拉取状态与开关
            detect_sql = """
                SELECT 
                    e.emp_id AS '工号',
                    e.name AS '姓名',
                    d.dept_name AS '部门',
                    e.status AS '人事状态',
                    IFNULL(m.cost_center, '本级') AS '财务归属',
                    IFNULL(m.base_salary_avg, 0.0) AS '已录入原始基数',
                    IFNULL(m.fund_base_avg, 0.0) AS '独立公积金基数(选填)',
                    
                    IFNULL(m.pension_enabled, 1) AS '养老参保(1是0否)',
                    IFNULL(m.pension_account, '省公众') AS '养老缴纳主体',
                    
                    IFNULL(m.medical_enabled, 1) AS '医疗参保(1是0否)',
                    IFNULL(m.medical_account, '省公司') AS '医疗缴纳主体',
                    
                    IFNULL(m.unemp_enabled, 1) AS '失业参保(1是0否)',
                    IFNULL(m.unemp_account, '省公众') AS '失业缴纳主体',
                    
                    IFNULL(m.injury_enabled, 1) AS '工伤参保(1是0否)',
                    IFNULL(m.injury_account, '省公众') AS '工伤缴纳主体',
                    
                    IFNULL(m.maternity_enabled, 1) AS '生育参保(1是0否)',
                    IFNULL(m.maternity_account, '省公司') AS '生育缴纳主体',
                    
                    IFNULL(m.fund_enabled, 1) AS '公积金参保(1是0否)',
                    IFNULL(m.fund_account, '省公众') AS '公积金缴纳主体',
                    
                    IFNULL(m.annuity_enabled, 0) AS '年金参保(1是0否)',
                    IFNULL(m.annuity_account, '省公司') AS '年金缴纳主体'
                FROM employees e
                LEFT JOIN departments d ON e.dept_id = d.dept_id
                LEFT JOIN ss_emp_matrix m ON e.emp_id = m.emp_id
                WHERE e.status IN ('在职', '挂靠人员')
            """
            roster_df = pd.read_sql_query(detect_sql, conn)

            if roster_df.empty:
                st.warning("人事档案库中未检索到任何在职人员！")
            else:
                # 校验基数是否为 0，防止核算出空账
                missing_base_df = roster_df[roster_df['已录入原始基数'] == 0.0]
                missing_count = len(missing_base_df)
                total_count = len(roster_df)

                st.success(f"✅ 扫描完毕！本月共有 {total_count} 名在职员工需要核算社保。")

                if missing_count > 0:
                    # 发现基数为 0 的异常人员，强制拦截
                    st.error(f"🚨 警告：发现 {missing_count} 名员工的【原始社保基数】为 0，引擎将无法进行算账！")
                    st.dataframe(missing_base_df, use_container_width=True, hide_index=True)
                else:
                    # 人员数据正常，放行算力引擎
                    st.success("🎉 所有人员的社保基数均已就绪，可以安全启动核算引擎！")

                    st.write("---")
                    # [核心修复 1] 规则年度强制选择器，打破自然年绑定
                    rule_year_to_use = st.selectbox(
                        "⚙️ 请选择本次核算套用的【规则年度】(注：按国内社保惯例，次年 6 月前通常沿用上年规则)",
                        ["2024", "2025", "2026", "2027", "2028"],
                        index=1  # 默认选中 2025
                    )

                    # 执行内存全量推演
                    if st.button("🚀 开始全员核算预览", type="primary", use_container_width=True):
                        all_bills = []
                        # 延迟导入，防止循环引用
                        from modules.core_social_security import calculate_complete_bill
                        for _, row in roster_df.iterrows():
                            # 传入你选定的规则年份
                            bill = calculate_complete_bill(row.to_dict(), rule_year_to_use)
                            all_bills.append(bill)

                        # 将算好的账单挂载到全局 Session，防止刷新丢失
                        st.session_state['temp_bills'] = pd.DataFrame(all_bills)

                    # ==================================================================
                    # [审计工具箱] 如果内存中存在账单，则渲染前端审计大表与入库控制台
                    # ==================================================================
                    if 'temp_bills' in st.session_state:
                        st.divider()
                        st.subheader(f"📊 {calc_month} 月度核算明细与审计")
                        st.info("💡 建议下载 Excel 进行全景核对，或使用下方搜索框精准抽查特例人员的计算结果。")

                        # 取出原始英文键名的账单
                        raw_df = st.session_state['temp_bills']

                        # [核心修复 2] 全中文表头强力汉化矩阵
                        bill_col_map = {
                            'pension_企': '养老(企业)', 'pension_个': '养老(个人)', 'pension_route': '养老缴纳主体',
                            'medical_企': '医疗(企业)', 'medical_个': '医疗(个人)', 'medical_route': '医疗缴纳主体',
                            'unemp_企': '失业(企业)', 'unemp_个': '失业(个人)', 'unemp_route': '失业缴纳主体',
                            'injury_企': '工伤(企业)', 'injury_个': '工伤(个人)', 'injury_route': '工伤缴纳主体',
                            'maternity_企': '生育(企业)', 'maternity_个': '生育(个人)', 'maternity_route': '生育缴纳主体',
                            'fund_企': '公积金(企业)', 'fund_个': '公积金(个人)', 'fund_route': '公积金缴纳主体',
                            'annuity_企': '年金(企业)', 'annuity_个': '年金(个人)', 'annuity_route': '年金缴纳主体'
                        }

                        # 执行重命名以供展示
                        df_preview = raw_df.rename(columns=bill_col_map)

                        # 调整列顺序，将核心汇总放前面
                        ordered_cols = ['工号', '姓名', '财务归属', '合计企业缴纳', '合计个人扣款']
                        detail_cols = [c for c in df_preview.columns if c not in ordered_cols]
                        df_preview = df_preview[ordered_cols + detail_cols]

                        # 精准狙击搜索框
                        search_query = st.text_input("🔍 抽查指定员工 (输入姓名或工号进行过滤)", "")

                        display_df = df_preview
                        if search_query:
                            # 加上 na=False，防止空值引发 ValueError 崩溃
                            display_df = display_df[
                                display_df['姓名'].str.contains(search_query, na=False) |
                                display_df['工号'].str.contains(search_query, na=False)
                            ]

                        # 渲染到前端大屏
                        st.dataframe(display_df, use_container_width=True, hide_index=True)

                        # [UI 升级 2] 在内存中生成全中文 Excel 文件供财务级审计
                        # ==========================================================
                        # [财务级工作流拆分引擎 - 终极防串表版]
                        # ==========================================================
                        buffer_preview = io.BytesIO()
                        with pd.ExcelWriter(buffer_preview, engine='xlsxwriter') as writer:

                            # 0. 全景总表 (原样输出，供全局对账)
                            df_preview.to_excel(writer, index=False, sheet_name='0.全景汇总大表')

                            base_cols = ['工号', '姓名', '财务归属']

                            # [核心利器：智能染色与金额剥离函数]
                            def export_route_sheet(sheet_name, route_name, target_items):
                                # 复制一份原数据，避免污染其他表
                                df_sub = raw_df.copy()
                                has_any_amount = pd.Series([False] * len(df_sub))
                                cols_to_export = base_cols.copy()

                                for item in target_items:
                                    # 【致命修复】精准判断：如果这个人的该险种路由不是当前目标主体，强制把金额抹零！
                                    # 这样就能防止“省公众”的金额串门跑到“中电数智”的单子里！
                                    mask = df_sub[f'{item}_route'] == route_name
                                    df_sub.loc[~mask, f'{item}_企'] = 0.0
                                    df_sub.loc[~mask, f'{item}_个'] = 0.0

                                    # 记录是否产生了有效金额 (过滤掉全为 0 的无效行)
                                    has_any_amount = has_any_amount | (df_sub[f'{item}_企'] > 0) | (df_sub[f'{item}_个'] > 0)
                                    cols_to_export.extend([f'{item}_企', f'{item}_个'])

                                # 只保留有金额的行
                                df_sub = df_sub[has_any_amount]

                                if not df_sub.empty:
                                    df_sub[cols_to_export].rename(columns=bill_col_map).to_excel(writer, index=False, sheet_name=sheet_name)

                            # 1. 流程一：中电数智 (养老、失业、工伤)
                            export_route_sheet('1.中电数智(养_失_工)', '中电数智', ['pension', 'unemp', 'injury'])

                            # 2. 流程二：省公司年金
                            export_route_sheet('2.省公司(年金专表)', '省公司', ['annuity'])

                            # 3. 流程三：省公司医疗、生育、工伤
                            export_route_sheet('3.省公司(医_生_工)', '省公司', ['medical', 'maternity', 'injury'])

                            # 4. 流程四：省公众养老、失业、工伤
                            export_route_sheet('4.省公众(养_失_工)', '省公众', ['pension', 'unemp', 'injury'])

                            # 5. 流程五：公积金 (专表)
                            # 公积金比较特殊，直接按有金额的输出即可，带上它的路由标签
                            df_gjj = raw_df.copy()
                            df_gjj = df_gjj[(df_gjj['fund_企'] > 0) | (df_gjj['fund_个'] > 0)]
                            if not df_gjj.empty:
                                cols_gjj = base_cols + ['fund_企', 'fund_个', 'fund_route']
                                df_gjj[cols_gjj].rename(columns=bill_col_map).to_excel(writer, index=False, sheet_name='5.公积金(专表)')

                        st.write("---")
                        c_dl, c_save = st.columns(2)

                        with c_dl:
                            st.download_button(
                                label="📥 1. 下载全中文预览账单 (Excel格式)",
                                data=buffer_preview.getvalue(),
                                file_name=f"社保核算明细_{calc_month}.xlsx",
                                mime="application/vnd.ms-excel"
                            )

                        with c_save:
                            if st.button("💾 2. 核对无误，保存正式账单并固化入库", type="primary"):
                                # 核心：把带有英文 route 字段的原版 raw_df 传给数据库引擎！
                                from modules.core_social_security import save_monthly_ss_records
                                success, msg = save_monthly_ss_records(raw_df, calc_month)

                                if success:
                                    st.success(msg)
                                    # 销毁内存账单，防止按错导致重复执行 SQL
                                    del st.session_state['temp_bills']
                                    st.balloons()
                                else:
                                    st.error(msg)

                # ======================================================================
                # 底部基数配置模块：无论是补充 0 基数，还是调整公积金基数，都在此操作
                # ======================================================================
                st.divider()
                st.write("#### 🛠️ 基数极速抢救与特例通道：导出名单 -> 修改基数 -> 重新灌入")
                st.info("💡 无论是补充缺失基数，还是修改某人的【独立公积金基数】，都在此表内完成。")

                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    roster_df.to_excel(writer, index=False, sheet_name='基数初始化名单')

                c_down, c_up = st.columns(2)
                with c_down:
                    st.download_button(
                        label="📥 1. 下载全员参保基数配置表",
                        data=buffer.getvalue(),
                        file_name=f"全员参保基数配置表_{calc_month}.xlsx",
                        mime="application/vnd.ms-excel",
                        type="primary"
                    )

                with c_up:
                    uploaded_file = st.file_uploader("📤 2. 上传填好基数的 Excel/CSV 模板", type=["xlsx", "xls", "csv"])
                    if uploaded_file is not None:
                        # 基于文件 ID 的极客刷新法，根除死循环
                        current_file_id = uploaded_file.file_id
                        if st.session_state.get('last_processed_file_id') != current_file_id:
                            try:
                                if uploaded_file.name.endswith('.csv'):
                                    upload_df = pd.read_csv(uploaded_file)
                                else:
                                    upload_df = pd.read_excel(uploaded_file)

                                success, msg = batch_update_emp_matrix(upload_df)
                                if success:
                                    st.session_state['last_processed_file_id'] = current_file_id
                                    st.rerun()  # 强行刷新页面，让顶部的雷达重新检测
                                else:
                                    st.error(msg)
                            except Exception as e:
                                st.error(f"❌ 读取文件失败: {e}")
                        else:
                            st.success("✅ 数据已成功灌入引擎，上方雷达已自动更新！(若需修改请更新文件后重传)")

        except Exception as e:
            st.error(f"❌ 扫描数据库引擎崩溃: {e}")
        finally:
            conn.close()

# ------------------------------------------------------------------------------
# Tab 2: 跨主体结算与对账 (中频业务)
# ------------------------------------------------------------------------------
with tab2:
    st.subheader("📤 生成结算单与请款函")
    st.write("🔧 [工程进度]: 等待接入 Excel 对账单导出与 Word 请款函生成引擎...")

# ------------------------------------------------------------------------------
# Tab 3: 全局规则与参数配置 (低频业务，一年配一次，但极其致命)
# ------------------------------------------------------------------------------
with tab3:
    st.subheader("🛠️ 全局算力引擎参数设置")
    st.info("💡 此处的设置将直接决定全公司数千条社保数据的最终核算金额。每年 7 月份社保局出新基数时，请切换到新年份进行配置。比例请填入小数（例如 16% 填 0.16）。")

    c_y, c_e = st.columns(2)
    with c_y:
        target_year = st.selectbox("📅 规则生效年度", ["2024", "2025", "2026", "2027", "2028"], index=1)
    with c_e:
        entity_opts = ["全量设置", "省公众", "中电数智", "省公司"]
        target_entity = st.selectbox("🏢 适用法人主体", entity_opts)

    # 回显逻辑
    fetch_entity = "省公众" if "全量" in target_entity else target_entity
    curr = get_policy_rules(target_year, fetch_entity)

    if curr:
        st.success(f"已加载 {target_year} 年度 【{fetch_entity}】 的历史配置，可直接修改。")
    else:
        st.info(f"【{fetch_entity}】 在 {target_year} 年度尚无配置记录，请录入参数后保存。")

    # 拦截确认弹窗逻辑
    if st.session_state.get('show_confirm', False):
        st.warning("⚠️ 警告：您即将修改底层财务参数，请核对变更情况！")

        # [核心修复：绝不让报错一闪而过！]
        if st.button("🔥 确认无误，强行写入数据库", type="primary", key="confirm_upsert_btn"):
            params = st.session_state.get('pending_params')
            if params:
                is_all = "全量" in params[1]
                success, msg = upsert_policy_rules(params, is_all_entities=is_all)

                if success:
                    st.success(msg)
                    # 只有真正写入成功，才允许消除弹窗并刷新
                    st.session_state['show_confirm'] = False
                    st.rerun()
                else:
                    # 如果底层 SQL 写入失败，将死死钉在屏幕上，绝不刷新！
                    st.error(f"🚨 数据库写入被底层拦截！真实原因: {msg}")

        if st.button("❌ 撤销修改，返回编辑", key="cancel_upsert_btn"):
            st.session_state['show_confirm'] = False
            st.rerun()
        st.divider()

    # 表单配置区
    with st.form("policy_rules_form"):
        st.write(f"**【{target_year} 年度】算法控制总开关**")
        c_mode, c_fund, c_med = st.columns(3)

        with c_mode:
            round_opts = {
                'exact': '精确到分 (保留两位小数)',
                'round_to_yuan': '四舍五入到元 (例如 84.5 -> 85)',
                'round_to_ten': '四舍五入到十元 (例如 85 -> 90)',
                'floor_to_ten': '向下抹零到十元 (例如 89 -> 80)'
            }
            r_keys = list(round_opts.keys())
            cur_round = curr.get('rounding_mode', 'round_to_yuan')
            sel_round = st.selectbox("社保取整规则引擎", options=r_keys, format_func=lambda x: round_opts[x], index=r_keys.index(cur_round) if cur_round in r_keys else 1)

        with c_fund:
            fund_opts = {
                'independent': '独立核算 (基于公积金原始基数直接测算)',
                'reverse_from_ss': '逆向倒推法 (按社保金额取十位整数后，倒推公积金真实基数)'
            }
            f_keys = list(fund_opts.keys())
            cur_fund = curr.get('fund_calc_method', 'reverse_from_ss')
            sel_fund = st.selectbox("公积金特殊算法引擎", options=f_keys, format_func=lambda x: fund_opts[x], index=f_keys.index(cur_fund) if cur_fund in f_keys else 1)

        with c_med:
            med_serious = st.number_input("大病医疗个人绝对值固定扣款 (元)", value=safe_float(curr.get('medical_serious_fix', 7.0)), step=1.0)

        st.divider()
        st.write(f"**【{target_year} 年度】社保与福利费率矩阵**")

        def render_ins_row(label, prefix, has_pers=True, has_limit=True):
            cols = st.columns([1.5, 2, 2, 2, 2])
            cols[0].markdown(f"**{label}**")
            up = cols[1].number_input(f"{label}封顶", value=safe_float(curr.get(f'{prefix}_upper')), step=100.0, label_visibility="collapsed") if has_limit else 0.0
            lw = cols[2].number_input(f"{label}保底", value=safe_float(curr.get(f'{prefix}_lower')), step=100.0, label_visibility="collapsed") if has_limit else 0.0
            cr = cols[3].number_input(f"{label}企%", value=safe_float(curr.get(f'{prefix}_comp_rate')), step=0.01, format="%.4f", label_visibility="collapsed")
            pr = cols[4].number_input(f"{label}个%", value=safe_float(curr.get(f'{prefix}_pers_rate')), step=0.01, format="%.4f", label_visibility="collapsed") if has_pers else 0.0
            return up, lw, cr, pr

        hc = st.columns([1.5, 2, 2, 2, 2])
        for col, text in zip(hc, ["险种", "封顶基数 (元)", "保底基数 (元)", "企业承担比例 (小数)", "个人承担比例 (小数)"]):
            col.caption(text)

        p_up, p_lw, p_cr, p_pr = render_ins_row("养老保险", "pension")
        m_up, m_lw, m_cr, m_pr = render_ins_row("医疗保险", "medical")
        u_up, u_lw, u_cr, u_pr = render_ins_row("失业保险", "unemp")
        i_up, i_lw, i_cr, _ = render_ins_row("工伤保险", "injury", has_pers=False)
        mat_up, mat_lw, mat_cr, _ = render_ins_row("生育保险", "maternity", has_pers=False)

        # [核心UI隔离] 将官方线和企业实际执行线完全拆开
        f_up, f_lw, f_cr, f_pr = render_ins_row("住房公积金(官方线)", "fund")

        cols_soe = st.columns([1.5, 2, 2, 2, 2])
        cols_soe[0].markdown("**↳ 内部实际执行线**")
        f_soe_up = cols_soe[1].number_input("内部封顶", value=safe_float(curr.get('fund_soe_upper')), step=100.0, label_visibility="collapsed")
        f_soe_lw = cols_soe[2].number_input("内部保底", value=safe_float(curr.get('fund_soe_lower')), step=100.0, label_visibility="collapsed")
        cols_soe[3].write("")
        cols_soe[4].write("")

        _, _, a_cr, a_pr = render_ins_row("企业年金", "annuity", has_limit=False)

        # 打包 31 个参数 (追加了 f_soe_up, f_soe_lw)
        submitted = st.form_submit_button("🔍 对比并预览修改", type="primary")
        if submitted:
            st.session_state['pending_params'] = (
                target_year, target_entity, sel_round, sel_fund, med_serious,
                p_up, p_lw, p_cr, p_pr, m_up, m_lw, m_cr, m_pr,
                u_up, u_lw, u_cr, u_pr, i_up, i_lw, i_cr,
                mat_up, mat_lw, mat_cr, f_up, f_lw, f_cr, f_pr,
                a_cr, a_pr, f_soe_up, f_soe_lw
            )
            st.session_state['show_confirm'] = True
            st.rerun()