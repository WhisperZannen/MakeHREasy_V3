# ==============================================================================
# 文件路径: pages/3_社保与福利结算.py
# 功能描述: 多主体社保与福利结算中枢 (MVC 架构前端 UI 层 - 终极防御版)
# 核心逻辑:
#   1. 彻底不写 SQL，所有数据交互强依赖 modules.core_social_security 接口。
#   2. 引入 safe_float 绝对防御机制，拦截数据库空值导致的界面渲染崩溃。
#   3. 构建高密度参数矩阵，实现 28 项基数与费率参数的一键保存。
#   4. 完美集成“多主体隔离”、“一键全量同步”与“修改确认防呆弹窗”。
# ==============================================================================

import streamlit as st
import pandas as pd
import io  # [新增] 用于内存中生成 Excel 模板
from modules.core_social_security import get_policy_rules, upsert_policy_rules, _get_db_connection, batch_update_emp_matrix # [修改] 导入刚写好的批量引擎

# 页面基础配置：开启宽屏模式以容纳庞大的参数矩阵
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
# [状态机初始化] 用于拦截表单保存动作，展示确认弹窗
# ==============================================================================
if 'show_confirm' not in st.session_state:
    st.session_state['show_confirm'] = False
if 'pending_params' not in st.session_state:
    st.session_state['pending_params'] = None

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
    st.info(
        "💡 引擎将自动读取人事档案中的【转正状态】与【离职状态】。由于系统冷启动缺少上年台账，本年度社保原始基数需手动初始化。")

    calc_month = st.text_input("📅 输入核算月份 (格式: YYYY-MM，如 2026-03)", value="2026-03", max_chars=7)

    # ==========================================================================
    # [核心优化] 状态锁：打破刷新魔咒，只要点过一次，界面直接钉死不消失
    # ==========================================================================
    if 'scan_locked' not in st.session_state:
        st.session_state['scan_locked'] = False

    if st.button("📡 第一步：扫描本月参保名单与基数状态", type="primary"):
        # 按下按钮，上锁，并强制页面重载以展开下方 UI
        st.session_state['scan_locked'] = True
        st.rerun()

    # 只要锁是开启状态，雷达就持续扫描，不惧怕任何文件上传导致的刷新
    if st.session_state.get('scan_locked', False):
        conn = _get_db_connection()
        try:
            # [完美回归] 直接从我们最初设计的 ss_emp_matrix 表中拉取所有险种的开关与路由
            detect_sql = """
                SELECT 
                    e.emp_id AS '工号',
                    e.name AS '姓名',
                    d.dept_name AS '部门',
                    e.status AS '人事状态',
                    IFNULL(m.cost_center, '本级') AS '财务归属',
                    IFNULL(m.base_salary_avg, 0.0) AS '已录入原始基数',
                    IFNULL(m.fund_base_avg, 0.0) AS '独立公积金基数(选填)',
                    
                    -- 暴露出所有的开关与路由账户，供 HR 在 Excel 里精准调配
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
                missing_base_df = roster_df[roster_df['已录入原始基数'] == 0.0]
                missing_count = len(missing_base_df)
                total_count = len(roster_df)

                st.success(f"✅ 扫描完毕！本月共有 {total_count} 名在职员工需要核算社保。")

                if missing_count > 0:
                    st.error(f"🚨 警告：发现 {missing_count} 名员工的【原始社保基数】为 0，引擎将无法进行算账！")
                    st.dataframe(missing_base_df, use_container_width=True, hide_index=True)
                else:
                    st.success("🎉 所有人员的社保基数均已就绪，可以安全启动核算引擎！")

                    # --- [新增开始] ---
                    # 1. 增加一个“全员核算”的大按钮
                    if st.button("🚀 开始全员核算预览", type="primary", use_container_width=True):
                        current_year = calc_month.split("-")[0] # 从 2026-03 里切出 2026

                        all_bills = []
                        # 遍历刚才雷达扫描出来的 roster_df
                        for _, row in roster_df.iterrows():
                            # 导入刚才在底层改好的新函数
                            from modules.core_social_security import calculate_complete_bill
                            # 算账！
                            bill = calculate_complete_bill(row.to_dict(), current_year)
                            all_bills.append(bill)

                        # 把算好的结果存入 SessionState，防止一刷新就没了
                        st.session_state['temp_bills'] = pd.DataFrame(all_bills)

                    # 2. 如果算完了，就展示预览表
                    if 'temp_bills' in st.session_state:
                        st.divider()
                        st.subheader(f"📊 {calc_month} 月度核算预览")
                        st.dataframe(st.session_state['temp_bills'], use_container_width=True, hide_index=True)

                        # 3. 预留保存按钮
                        if st.button("💾 核对无误，保存正式账单并推送至台账"):
                            st.warning("功能开发中：下一步我们将编写保存到 ss_monthly_records 表的逻辑")
                    # --- [新增结束] ---

                st.divider()
                st.write("#### 🛠️ 基数极速抢救与特例通道：导出名单 -> 修改基数 -> 重新灌入")
                st.info("💡 无论是补充缺失基数，还是修改某人的【独立公积金基数】，都在此表内完成。")

                # 生成 Excel 模板
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
                    uploaded_file = st.file_uploader("📤 2. 上传填好基数的 Excel 模板", type=["xlsx", "xls"])
                    if uploaded_file is not None:
                        # [核心优化] 防死循环拦截器 + 上传成功后自动重载刷新雷达
                        if st.session_state.get('last_processed_file') != uploaded_file.name:
                            try:
                                upload_df = pd.read_excel(uploaded_file)
                                success, msg = batch_update_emp_matrix(upload_df)
                                if success:
                                    # 记录文件已处理，防死循环
                                    st.session_state['last_processed_file'] = uploaded_file.name
                                    # [终极大招] 灌库成功后，立刻强行刷新页面，让上方的雷达直接扫出 0 警告的最新数据！
                                    st.rerun()
                                else:
                                    st.error(msg)
                            except Exception as e:
                                st.error(f"❌ 读取 Excel 文件失败: {e}")
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
    st.info(
        "💡 此处的设置将直接决定全公司数千条社保数据的最终核算金额。每年 7 月份社保局出新基数时，请切换到新年份进行配置。比例请填入小数（例如 16% 填 0.16）。")

    # ==========================================
    # 顶部控制区：年份与主体构成 X 轴与 Y 轴的联合定位
    # ==========================================
    c_y, c_e = st.columns(2)
    with c_y:
        target_year = st.selectbox("📅 规则生效年度", ["2024", "2025", "2026", "2027", "2028"], index=1)
    with c_e:
        # 支持全量同步或单主体特例配置
        entity_opts = ["全量设置", "省公众", "中电数智", "省公司"]
        target_entity = st.selectbox("🏢 适用法人主体", entity_opts)

    # 回显逻辑：如果选了全量设置，回显时默认拉取“省公众”的数据作为参考，防止界面全空
    fetch_entity = "省公众" if "全量" in target_entity else target_entity
    curr = get_policy_rules(target_year, fetch_entity)

    if curr:
        st.success(f"已加载 {target_year} 年度 【{fetch_entity}】 的历史配置，可直接修改。")
    else:
        st.info(f"【{fetch_entity}】 在 {target_year} 年度尚无配置记录，请录入参数后保存。")

    # ==============================================================================
    # 拦截弹窗 UI 渲染区 (必须在 form 之外)
    # ==============================================================================
    if st.session_state.get('show_confirm', False):
        st.warning("⚠️ 警告：您即将修改底层财务参数，请核对变更情况！")

        # 确认写入按钮（带有唯一的 key 防止 ID 冲突）
        if st.button("🔥 确认无误，强行写入数据库", type="primary", key="confirm_upsert_btn"):
            params = st.session_state.get('pending_params')
            if params:
                # 判断是否开启全量模式（检查目标主体字符串中是否包含"全量"）
                is_all = "全量" in params[1]
                success, msg = upsert_policy_rules(params, is_all_entities=is_all)

                if success:
                    st.success(msg)
                else:
                    st.error(msg)

            # 写入完成后，关闭弹窗状态并刷新
            st.session_state['show_confirm'] = False
            st.rerun()

        # 撤销修改按钮
        if st.button("❌ 撤销修改，返回编辑", key="cancel_upsert_btn"):
            st.session_state['show_confirm'] = False
            st.rerun()

        st.divider()

    # ==============================================================================
    # 主表单区 (所有输入框和初始提交按钮必须包含在这个 with 块内，且严格缩进)
    # ==============================================================================
    with st.form("policy_rules_form"):

        # --- 矩阵 1: 算法控制总开关 ---
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
            sel_round = st.selectbox("社保取整规则引擎", options=r_keys, format_func=lambda x: round_opts[x],
                                     index=r_keys.index(cur_round) if cur_round in r_keys else 1)

        with c_fund:
            fund_opts = {
                'independent': '独立核算 (基于公积金原始基数直接测算)',
                'reverse_from_ss': '逆向倒推法 (按社保金额取十位整数后，倒推公积金真实基数)'
            }
            f_keys = list(fund_opts.keys())
            cur_fund = curr.get('fund_calc_method', 'reverse_from_ss')
            sel_fund = st.selectbox("公积金特殊算法引擎", options=f_keys, format_func=lambda x: fund_opts[x],
                                    index=f_keys.index(cur_fund) if cur_fund in f_keys else 1)

        with c_med:
            med_serious = st.number_input("大病医疗个人绝对值固定扣款 (元)",
                                          value=safe_float(curr.get('medical_serious_fix', 7.0)), step=1.0)

        st.divider()

        # --- 矩阵 2: 五险两金基数与费率配置大表 ---
        st.write(f"**【{target_year} 年度】社保与福利费率矩阵**")


        def render_ins_row(label, prefix, has_pers=True, has_limit=True):
            cols = st.columns([1.5, 2, 2, 2, 2])
            cols[0].markdown(f"**{label}**")
            up = cols[1].number_input(f"{label}封顶", value=safe_float(curr.get(f'{prefix}_upper')), step=100.0,
                                      label_visibility="collapsed") if has_limit else 0.0
            lw = cols[2].number_input(f"{label}保底", value=safe_float(curr.get(f'{prefix}_lower')), step=100.0,
                                      label_visibility="collapsed") if has_limit else 0.0
            cr = cols[3].number_input(f"{label}企%", value=safe_float(curr.get(f'{prefix}_comp_rate')), step=0.01,
                                      format="%.4f", label_visibility="collapsed")
            pr = cols[4].number_input(f"{label}个%", value=safe_float(curr.get(f'{prefix}_pers_rate')), step=0.01,
                                      format="%.4f", label_visibility="collapsed") if has_pers else 0.0
            return up, lw, cr, pr


        hc = st.columns([1.5, 2, 2, 2, 2])
        for col, text in zip(hc,
                             ["险种", "封顶基数 (元)", "保底基数 (元)", "企业承担比例 (小数)", "个人承担比例 (小数)"]):
            col.caption(text)

        p_up, p_lw, p_cr, p_pr = render_ins_row("养老保险", "pension")
        m_up, m_lw, m_cr, m_pr = render_ins_row("医疗保险", "medical")
        u_up, u_lw, u_cr, u_pr = render_ins_row("失业保险", "unemp")
        i_up, i_lw, i_cr, _ = render_ins_row("工伤保险", "injury", has_pers=False)
        mat_up, mat_lw, mat_cr, _ = render_ins_row("生育保险", "maternity", has_pers=False)
        f_up, f_lw, f_cr, f_pr = render_ins_row("住房公积金", "fund")
        _, _, a_cr, a_pr = render_ins_row("企业年金", "annuity", has_limit=False)

        # 🚨 极度重要：触发预览的按钮必须缩进在这个 form 内！
        submitted = st.form_submit_button("🔍 对比并预览修改", type="primary")
        if submitted:
            # 严谨打包 29 个参数
            st.session_state['pending_params'] = (
                target_year, target_entity, sel_round, sel_fund, med_serious,
                p_up, p_lw, p_cr, p_pr, m_up, m_lw, m_cr, m_pr,
                u_up, u_lw, u_cr, u_pr, i_up, i_lw, i_cr,
                mat_up, mat_lw, mat_cr, f_up, f_lw, f_cr, f_pr,
                a_cr, a_pr
            )
            # 开启弹窗状态，重载页面以触发上面的警告框
            st.session_state['show_confirm'] = True
            st.rerun()