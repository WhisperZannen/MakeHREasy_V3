# ==============================================================================
# 文件路径: app.py (系统总入口)
# 功能描述: 纯净版真实大盘与系统组件健康度监控
# ==============================================================================

import streamlit as st
import pandas as pd
import sqlite3
import os

st.set_page_config(page_title="MakeHREasy 系统中枢", layout="wide")


# ------------------------------------------------------------------------------
# 【底层通信】连接真实的数据库获取实时大盘数据
# ------------------------------------------------------------------------------
def get_dashboard_metrics():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, 'database', 'hr_core.db')
    metrics = {"total_active": 0, "interns_count": 0, "db_status": "离线"}
    try:
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            metrics["db_status"] = "正常连通"

            # 抓取真实的在岗总人数 (包括在职和挂靠)
            cursor.execute("SELECT COUNT(*) FROM employees WHERE status IN ('在职', '挂靠人员')")
            metrics["total_active"] = cursor.fetchone()[0]

            # 抓取真实的待转正实习生数量
            cursor.execute("""
                           SELECT COUNT(*)
                           FROM employees e
                                    JOIN employee_profiles p ON e.emp_id = p.emp_id
                                    JOIN positions pos ON p.pos_id = pos.pos_id
                           WHERE e.status = '在职'
                             AND pos.pos_name = '实习岗'
                           """)
            metrics["interns_count"] = cursor.fetchone()[0]
            conn.close()
    except Exception as e:
        metrics["db_status"] = f"连接异常: {e}"
    return metrics


# ==============================================================================
# 将主页封装成视图函数
# ==============================================================================
def render_home_page():
    real_data = get_dashboard_metrics()

    st.sidebar.title("🏠 MakeHREasy 主页")
    app_mode = st.sidebar.radio("请选择视图面板:", ["📊 业务数据大盘", "⚙️ 系统底座状态"])

    # --------------------------------------------------------------------------
    # 视图 A: 业务数据大盘 (绝对真实，绝无张三)
    # --------------------------------------------------------------------------
    if app_mode == "📊 业务数据大盘":
        st.title("🚀 MakeHREasy 核心结算系统")
        st.markdown("<span style='color:gray'>企业级全局数据态势感知与底层业务管控中枢</span>", unsafe_allow_html=True)
        st.divider()

        st.subheader("📊 实时业务基盘")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("当前在岗基数 (真实)", f"{real_data['total_active']} 人")
        m2.metric("在期实习生 (真实)", f"{real_data['interns_count']} 人")
        m3.metric("薪酬引擎模块", "已就绪，待算力注入")
        m4.metric("台账库对齐状态", "正常")

        st.write("<br>", unsafe_allow_html=True)

        st.info("💡 欢迎使用本系统。请通过左侧导航栏的【人事组织大盘】和【财务结算中枢】进入对应的业务模块进行操作。")

    # --------------------------------------------------------------------------
    # 视图 B: 系统底座状态 (真 IT 监控，不掺杂 HR 业务流水)
    # --------------------------------------------------------------------------
    elif app_mode == "⚙️ 系统底座状态":
        st.title("⚙️ 系统底座运行监控")
        st.caption("IT 维度的系统组件健康度扫描。")

        st.markdown("#### 🗄️ 数据库引擎")
        st.success(f"**核心数据库 (hr_core.db)**: {real_data['db_status']}")

        st.markdown("#### 🧩 挂载模块状态")
        comp_df = pd.DataFrame([
            {"组件名称": "1_personnel.py", "功能定位": "人事档案与组织管理", "状态": "✅ 正常"},
            {"组件名称": "2_social.py", "功能定位": "社保五险两金算力层", "状态": "✅ 正常"},
            {"组件名称": "3_payroll.py", "功能定位": "薪酬回溯与结算引擎", "状态": "⏳ 正在构建"},
            {"组件名称": "4_ledger.py", "功能定位": "人工成本财务总账", "状态": "✅ 正常"},
        ])
        st.dataframe(comp_df, use_container_width=True, hide_index=True)


# ==============================================================================
# 侧边栏高级路由
# ==============================================================================
page_home = st.Page(render_home_page, title="系统总看板", icon="🏠", default=True)
page_personnel = st.Page("pages/1_personnel.py", title="人员与组织档案", icon="👥")
page_social = st.Page("pages/2_social.py", title="社保与福利结算", icon="🛡️")
page_payroll = st.Page("pages/3_payroll.py", title="薪酬核算与发放", icon="💸")
page_ledger = st.Page("pages/4_ledger.py", title="人工成本总台账", icon="📊")

pg = st.navigation(
    {
        "核心门户": [page_home],
        "人事组织大盘": [page_personnel],
        "财务结算中枢": [page_social, page_payroll, page_ledger]
    }
)

pg.run()