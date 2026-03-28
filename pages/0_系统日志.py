import streamlit as st
from modules.utils import get_git_changelog

st.set_page_config(page_title="系统更新日志", layout="wide")

st.title("🕰️ 系统演化日志")
st.caption("基于 Git Commit 记录自动生成，记录项目每一次的成长与纠偏。")

changelog_df = get_git_changelog(limit=30)

if not changelog_df.empty:
    for _, row in changelog_df.iterrows():
        with st.container(border=True):
            col_t, col_m = st.columns([1, 4])
            with col_t:
                st.write(f"📅 **{row['时间']}**")
            with col_m:
                # 识别一些特殊的标签，给它加点颜色
                msg = row['更新内容']
                if "[FEAT]" in msg.upper() or "新增" in msg:
                    st.success(msg)
                elif "[FIX]" in msg.upper() or "修复" in msg:
                    st.warning(msg)
                else:
                    st.info(msg)
else:
    st.info("暂无提交记录或 Git 环境未配置。")