# ==============================================================================
# 文件路径: pages/3_payroll.py
# 功能描述: 薪酬核算与多平台分发工作台 (View 交互层 - 算力字典注入版)
# ==============================================================================

import streamlit as st
import pandas as pd
import os
import json
import io

# 设置页面标题和宽屏布局
st.set_page_config(page_title="薪酬核算与发放", layout="wide")

st.title("💸 薪酬核算与多平台分发中心")
st.caption(
    "🔒 核心流向：参数字典维护 ➡️ 本地基础备料 ➡️ 导入动态奖惩与生成双轨模板 ➡️ 线下财务算税 ➡️ 终极个税回灌与台账封账")

# 增加第五个 Tab：专门用于维护薪酬字典
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📂 第一步：专项奖池与动态预埋",
    "🧮 第二步：生成底表与双轨模板",
    "📥 第三步：财务个税回灌与结算",
    "📜 综合查询与发薪凭证",
    "⚙️ 全局参数与薪酬字典 (总阀门)"
])

# ------------------------------------------------------------------------------
# [新增] Tab 5: 全局参数与薪酬字典 (系统算钱的基准法则)
# ------------------------------------------------------------------------------
with tab5:
    st.subheader("🛠️ 薪酬算力字典与全局发条")
    st.info("💡 系统的『15号回溯引擎』将严格按照这里的标准，去计算员工调岗前后的基础薪资与绩效基数。")


    # 我们采用轻量级的 JSON 存储薪酬字典，速度最快且极易维护
    def load_payroll_dicts():
        dict_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'payroll_dicts.json')
        if not os.path.exists(dict_path):
            # 初始化默认的一套空字典模板
            default_dict = {
                "post_salary_map": {"11": 2500, "12": 3000, "23": 10180},  # 岗级 -> 岗位工资
                "t_level_map": {"T1": 1000, "T2": 2000},  # T级 -> 激励包基数
                "expert_allowance": {"一级专家": 1500, "二级专家": 860}  # 专家级别 -> 专家津贴
            }
            with open(dict_path, 'w', encoding='utf-8') as f:
                json.dump(default_dict, f, ensure_ascii=False, indent=4)
            return default_dict
        with open(dict_path, 'r', encoding='utf-8') as f:
            return json.load(f)


    def save_payroll_dicts(data):
        dict_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'payroll_dicts.json')
        with open(dict_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)


    curr_dicts = load_payroll_dicts()

    c_p1, c_p2, c_p3 = st.columns(3)

    with c_p1:
        st.write("💰 **1. 岗级 -> 岗位工资标准**")
        df_post = pd.DataFrame(list(curr_dicts["post_salary_map"].items()), columns=["岗级 (如:11)", "岗位工资 (元)"])
        edited_post = st.data_editor(df_post, num_rows="dynamic", use_container_width=True, key="edit_post")

    with c_p2:
        st.write("🚀 **2. T级 -> 激励包基数标准**")
        df_t = pd.DataFrame(list(curr_dicts["t_level_map"].items()), columns=["T级 (如:T1)", "激励包基数 (元)"])
        edited_t = st.data_editor(df_t, num_rows="dynamic", use_container_width=True, key="edit_t")

    with c_p3:
        st.write("🏅 **3. 专家级别 -> 专家津贴标准**")
        df_exp = pd.DataFrame(list(curr_dicts["expert_allowance"].items()), columns=["专家级别", "津贴金额 (元)"])
        edited_exp = st.data_editor(df_exp, num_rows="dynamic", use_container_width=True, key="edit_exp")

    if st.button("💾 覆盖保存全量薪酬字典", type="primary"):
        try:
            # 清洗并转换用户在前端编辑好的表格数据
            new_post_map = {str(row["岗级 (如:11)"]).strip(): float(row["岗位工资 (元)"]) for _, row in
                            edited_post.iterrows() if str(row["岗级 (如:11)"]).strip()}
            new_t_map = {str(row["T级 (如:T1)"]).strip(): float(row["激励包基数 (元)"]) for _, row in
                         edited_t.iterrows() if str(row["T级 (如:T1)"]).strip()}
            new_exp_map = {str(row["专家级别"]).strip(): float(row["津贴金额 (元)"]) for _, row in edited_exp.iterrows()
                           if str(row["专家级别"]).strip()}

            new_dicts = {
                "post_salary_map": new_post_map,
                "t_level_map": new_t_map,
                "expert_allowance": new_exp_map
            }
            save_payroll_dicts(new_dicts)
            st.success("✅ 薪酬底层算力字典已成功更新！『15号回溯引擎』将采用最新标准。")
        except Exception as e:
            st.error(f"❌ 数据格式错误，保存失败: {e} (请确保金额列填写的都是数字)")

# ------------------------------------------------------------------------------
# (其他 Tab 保持你上传的代码原样不动)
# ------------------------------------------------------------------------------
with tab1:
    st.subheader("🎁 专项奖金与特殊项目池")
    st.info("💡 提前把这个月各种乱七八糟的项目提成、临时发钱名目录入到这里。算工资时系统会自动打包吸收。")
    st.write("🔧 功能建设中：专项奖金批量导入与流水台账界面。")

with tab2:
    st.subheader("⚙️ 薪资主盘备料与模板生成")
    st.warning("⚠️ 启动引擎前，请确保本月【人员调动/调薪】已维护完毕，且【社保模块】已生成当期账单！")
    c1, c2 = st.columns(2)
    with c1:
        calc_month = st.text_input("📅 输入当前薪酬核算月份 (格式 YYYY-MM)", value="2026-03")
    if st.button("🚀 1. 抓取固定底薪与社保 (生成初版底稿)", type="primary"):
        st.success(f"正在构建 {calc_month} 薪酬盘：执行15号生死线判定... 抓取衰减发条... 倒吸大病社保...")
        st.info("🚧 算力引擎正在接入，该按钮即将生效。")
    st.divider()
    st.write("📤 **多平台模板分发 (需在上方完成备料后激活)**")
    cc1, cc2 = st.columns(2)
    with cc1:
        st.button("📥 导出【财务算税专用模板】", disabled=True, help="完全对齐财务软件的导入格式")
    with cc2:
        st.button("📥 导出【电信OA清册上传模板】", disabled=True, help="将复杂的本地项目折叠翻译为OA认识的字段")

with tab3:
    st.subheader("🏦 最终清算与封账引擎")
    st.info("💡 从财务拿到扣税金额后，在这里上传闭环，并输入 OA 生成的清册号。")
    tax_file = st.file_uploader("📤 1. 上传财务算税反馈表 (必须包含工号和扣税额)", type=["xlsx", "csv"])
    oa_number = st.text_input("🔢 2. 录入电信OA系统下发的【正式清册流水号】(留档凭证)")
    if st.button("🔥 3. 扣减个税算出实发，并全量封账同步台账！", type="primary"):
        st.info("🚧 算力收网中：正在执行 (应发 - 社保合并项 - 个税)... 准备覆写底层数据库...")

with tab4:
    st.subheader("🖨️ 历史薪酬总账与发卡清单")
    st.write("🔍 查询功能建设中：将提供1:1复刻原始报表的下载，以及银行代发清单的导出。")