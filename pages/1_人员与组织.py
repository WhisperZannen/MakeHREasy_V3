# ==============================================================================
# 文件路径: pages/1_人员与组织.py
# 功能描述: 人员与组织架构的可视化管理页面 (V3.6 交互升级与高容错版)
# 实现了什么具体逻辑:
#   1. 解决了数据库为空时 Pandas 报 KeyError 的冷启动漏洞。
#   2. Sidebar 侧边栏路由接管，防止刷新跳页。
#   3. [核心修复] 彻底解决 Pandas numpy.int64 向底层 SQLite 传递时引发的“找不到ID”的类型穿透 Bug。
#   4. [核心修复] 补全导入模板中缺失的学历、学位、毕业院校、专业、毕业日期等学籍字段。
#   5. [核心优化] 剔除导入模板中冗余的“职级权重”，降低业务填写心智负担，后台静默兜底。
#   6. [核心重构] 废弃下拉框搜人修改，采用 data_editor 引入“行勾选联动回填”的极佳交互体验。
#   7. [核心重构] 历史轨迹视图全面升级，支持按变动类型、原部门、时间区间进行多维筛选。
# ==============================================================================

import streamlit as st
import pandas as pd
import io
from datetime import datetime

# 导入底层接口
from modules.core_dept import get_all_departments, add_department, update_department, soft_delete_department
from modules.core_position import get_all_positions, add_position, update_position
from modules.core_personnel import get_all_employees, add_employee, update_employee, update_employee_status, get_all_history, rollback_history

st.set_page_config(page_title="组织人事中枢", layout="wide")

# ==============================================================================
# 消息状态保持器
# ==============================================================================
if 'ui_msg' in st.session_state:
    if st.session_state.ui_msg_type == 'success': st.success(st.session_state.ui_msg)
    else: st.error(st.session_state.ui_msg)
    del st.session_state.ui_msg, st.session_state.ui_msg_type

def set_msg_and_rerun(msg, type='success'):
    st.session_state.ui_msg = msg
    st.session_state.ui_msg_type = type
    st.rerun()

# ==============================================================================
# 中文化映射
# ==============================================================================
DEPT_COL_MAP = {'dept_id': 'ID', 'dept_name': '部门名称', 'dept_category': '性质', 'parent_dept_id': '上级ID', 'sort_order': '权重', 'status': '状态'}
POS_COL_MAP = {'pos_id': 'ID', 'pos_name': '岗位名称', 'pos_category': '序列', 'sort_order': '权重', 'status': '状态'}
EMP_COL_MAP = {
    'emp_id': '工号', 'name': '姓名', 'id_card': '身份证号', 'dept_id': '部门ID', 'pos_id': '岗位ID',
    'post_rank': '岗级', 'post_grade': '档次', 'join_company_date': '入职日期', 'status': '状态',
    'pos_name': '岗位', 'tech_grade': 'T级', 'title_order': '职级权重',
    'education_level': '学历', 'degree': '学位', 'school_name': '毕业院校',
    'major': '专业', 'graduation_date': '毕业日期', 'first_job_date': '参加工作日期', 'dept_name': '部门'
}

def clean_str(val): return "" if pd.isna(val) or val is None or str(val).lower()=='nan' else str(val).strip()
def clean_date(d_val):
    try: return pd.to_datetime(d_val).strftime('%Y-%m-%d') if pd.notna(pd.to_datetime(d_val)) else None
    except: return None

def refresh_data():
    ok_d, d = get_all_departments(include_inactive=True)
    ok_p, p = get_all_positions(include_inactive=True)
    ok_e, e = get_all_employees(include_resigned=True)
    return (pd.DataFrame(d) if ok_d and d else pd.DataFrame(columns=list(DEPT_COL_MAP.keys())),
            pd.DataFrame(p) if ok_p and p else pd.DataFrame(columns=list(POS_COL_MAP.keys())),
            pd.DataFrame(e) if ok_e and e else pd.DataFrame(columns=list(EMP_COL_MAP.keys())))

df_depts, df_positions, df_emps = refresh_data()

def build_dept_tree(df, parent_id=None, level=0):
    tree = []
    if df.empty: return tree
    children = df[df['parent_dept_id'].isna() | (df['parent_dept_id'] == 0)] if (pd.isna(parent_id) or parent_id == 0) else df[df['parent_dept_id'] == parent_id]
    for _, row in children.iterrows():
        r_d = row.to_dict()
        r_d['层级展示名'] = "　　" * level + ("└─ " if level > 0 else "") + str(row['dept_name'])
        p_name = "无"
        if pd.notna(row['parent_dept_id']) and row['parent_dept_id'] != 0:
            pm = df[df['dept_id'] == row['parent_dept_id']]
            if not pm.empty: p_name = pm.iloc[0]['dept_name']
        r_d['上级名称'] = p_name
        tree.append(r_d)
        tree.extend(build_dept_tree(df, row['dept_id'], level + 1))
    return tree

# ==============================================================================
# 侧边栏导航
# ==============================================================================
st.sidebar.title("🎛️ 系统架构中枢")
current_page = st.sidebar.radio("请选择模块:", ["🏢 部门管理", "🎯 岗位字典", "👥 人员档案", "🕰️ 历史变动流水"])
st.title(current_page)

# ==============================================================================
# 模块 A: 🏢 部门管理
# ==============================================================================
if current_page == "🏢 部门管理":
    col_d1, col_d2 = st.columns([1, 2])
    with col_d1:
        st.subheader("📝 部门信息维护")
        edit_mode = st.radio("模式", ["新增部门", "修改现有部门"], horizontal=True)
        target_dept = None
        if edit_mode == "修改现有部门":
            if df_depts.empty: st.warning("无部门可修改")
            else:
                d_dict = {r['dept_id']: r['dept_name'] for _, r in df_depts.iterrows()}
                sel_d_id = st.selectbox("选择部门回填", options=list(d_dict.keys()), format_func=lambda x: d_dict[x])
                target_dept = df_depts[df_depts['dept_id'] == sel_d_id].iloc[0]

        with st.form("dept_form"):
            d_name = st.text_input("部门名称*", value=target_dept['dept_name'] if target_dept is not None else "")
            cat_opts = ["公司领导", "管控", "生产", "其他"]
            def_cat = target_dept['dept_category'] if target_dept is not None else "管控"
            d_cat = st.selectbox("性质*", cat_opts, index=cat_opts.index(def_cat) if def_cat in cat_opts else 1)
            d_sort = st.number_input("权重(越小越靠前)*", value=int(target_dept['sort_order']) if target_dept is not None else 999)

            v_p = df_depts[df_depts['status'] == 1]
            if target_dept is not None: v_p = v_p[v_p['dept_id'] != target_dept['dept_id']]
            p_opts = {0: "无(顶级)"}
            for _, r in v_p.iterrows(): p_opts[r['dept_id']] = r['dept_name']

            def_p = int(target_dept['parent_dept_id']) if target_dept is not None and pd.notna(target_dept['parent_dept_id']) else 0
            d_parent = st.selectbox("上级部门", options=list(p_opts.keys()), format_func=lambda x: p_opts[x], index=list(p_opts.keys()).index(def_p) if def_p in p_opts else 0)

            def_stat = "正常" if target_dept is None or target_dept['status'] == 1 else "已撤销"
            d_status = st.selectbox("状态", ["正常", "已撤销"], index=["正常", "已撤销"].index(def_stat))

            if st.form_submit_button("保存"):
                if not d_name: st.error("名称必填")
                else:
                    # [核心修复 2026-03-26] 强制将 target_dept['dept_id'] 和 d_parent 转换为 Python 原生 int
                    s_val = 1 if d_status == "正常" else 0
                    p_val = int(d_parent) if d_parent != 0 else None
                    if edit_mode == "新增部门":
                        success, msg = add_department(d_name, d_cat, p_val, d_sort)
                    else:
                        success, msg = update_department(int(target_dept['dept_id']), d_name, d_cat, p_val, d_sort, s_val)
                    if success: set_msg_and_rerun(msg)
                    else: st.error(msg)

        with st.expander("📥 批量导入 (无视乱序版)"):
            tmp = pd.DataFrame(columns=['部门名称', '性质', '上级名称', '权重'])
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine='openpyxl') as w: tmp.to_excel(w, index=False)
            st.download_button("下载模板", data=out.getvalue(), file_name="部门导入模板.xlsx")

            df_file = st.file_uploader("上传", type=["xlsx"], key="d_up")
            if df_file and st.button("开始导入"):
                in_df = pd.read_excel(df_file)
                # 两步走算法：第一步
                for _, r in in_df.iterrows():
                    nm = clean_str(r.get('部门名称'))
                    if nm: add_department(nm, clean_str(r.get('性质')) or "其他", None, int(r.get('权重', 999)) if pd.notna(r.get('权重')) else 999)

                # 两步走算法：第二步
                _, rd = get_all_departments(include_inactive=True); fdf = pd.DataFrame(rd)
                for _, r in in_df.iterrows():
                    nm = clean_str(r.get('部门名称')); pnm = clean_str(r.get('上级名称'))
                    if nm and pnm:
                        tc = fdf[fdf['dept_name'] == nm]; tp = fdf[fdf['dept_name'] == pnm]
                        if not tc.empty and not tp.empty:
                            # [核心修复 2026-03-26] 在写入父子关系时，强制洗壳，全部转为原生 int
                            update_department(
                                int(tc.iloc[0]['dept_id']), str(tc.iloc[0]['dept_name']), str(tc.iloc[0]['dept_category']),
                                int(tp.iloc[0]['dept_id']), int(tc.iloc[0]['sort_order']), int(tc.iloc[0]['status'])
                            )
                set_msg_and_rerun("部门架构与层级关系已全量生成完毕")

    with col_d2:
        st.subheader("📊 组织架构树")
        s1, s2 = st.columns([2, 1])
        with s1: d_s = st.text_input("🔍 搜索")
        with s2: st.write(""); show_i = st.checkbox("含已撤销")

        fdf = df_depts.copy()
        if not fdf.empty:
            if not show_i: fdf = fdf[fdf['status'] == 1]
            if d_s:
                m_ids = set()
                dh = fdf[fdf['dept_name'].str.contains(d_s, na=False)]['dept_id'].tolist()
                def trace(did, all_df):
                    m_ids.add(did); pid = all_df[all_df['dept_id']==did].iloc[0]['parent_dept_id']
                    if pd.notna(pid) and pid != 0: trace(pid, all_df)
                for h in dh: trace(h, df_depts)
                fdf = df_depts[df_depts['dept_id'].isin(m_ids)]
                if not show_i: fdf = fdf[fdf['status'] == 1]

            t_data = build_dept_tree(fdf)
            if t_data:
                tdf = pd.DataFrame(t_data)
                tdf['状态'] = tdf['status'].apply(lambda x: "正常" if x == 1 else "撤销")
                st.dataframe(tdf[['dept_id', '层级展示名', 'dept_category', '上级名称', 'sort_order', '状态']], use_container_width=True, hide_index=True)
            else: st.info("无匹配")

# ==============================================================================
# 模块 B: 🎯 岗位字典
# ==============================================================================
elif current_page == "🎯 岗位字典":
    col_p1, col_p2 = st.columns([1, 2])
    with col_p1:
        st.subheader("📝 岗位维护")
        p_mode = st.radio("模式", ["新增", "修改回填"], horizontal=True)
        t_pos = None
        if p_mode == "修改回填":
            if df_positions.empty: st.warning("无数据")
            else:
                p_dict = {r['pos_id']: r['pos_name'] for _, r in df_positions.iterrows()}
                s_pid = st.selectbox("选择", options=list(p_dict.keys()), format_func=lambda x: p_dict[x])
                t_pos = df_positions[df_positions['pos_id'] == s_pid].iloc[0]

        with st.form("pos_form"):
            p_name = st.text_input("岗位名称*", value=t_pos['pos_name'] if t_pos is not None else "")
            p_cat = st.text_input("岗位序列 (如技术/管理)", value=t_pos['pos_category'] if t_pos is not None else "通用序列")
            p_sort = st.number_input("权重", value=int(t_pos['sort_order']) if t_pos is not None else 999)

            p_stat = "正常" if t_pos is None or t_pos['status'] == 1 else "停用"
            p_s_val = st.selectbox("状态", ["正常", "停用"], index=["正常", "停用"].index(p_stat))

            if st.form_submit_button("保存"):
                if not p_name: st.error("必填")
                else:
                    sv = 1 if p_s_val == "正常" else 0
                    if p_mode == "新增": ok, msg = add_position(p_name, p_cat, p_sort)
                    else: ok, msg = update_position(int(t_pos['pos_id']), p_name, p_cat, p_sort, sv)
                    if ok: set_msg_and_rerun(msg)
                    else: st.error(msg)

    with col_p2:
        st.subheader("📊 岗位清单")
        if not df_positions.empty:
            sh_p = st.checkbox("含已停用")
            fp = df_positions if sh_p else df_positions[df_positions['status']==1]
            fp['状态'] = fp['status'].apply(lambda x: "正常" if x==1 else "停用")
            st.dataframe(fp[['pos_id', 'pos_name', 'pos_category', 'sort_order', '状态']], use_container_width=True, hide_index=True)

# ==============================================================================
# 模块 C: 👥 人员档案
# ==============================================================================
elif current_page == "👥 人员档案":
    st.subheader("🔍 人员档案管理台")
    st.info("💡 交互指南：在下方表格的首列勾选您要修改的员工，即可在最下方自动填满该人员的信息进行修改。")

    c1, c2, c3, c4 = st.columns(4)
    with c1: q_name = st.text_input("搜索姓名/工号")
    with c2: q_dept = st.multiselect("部门", options=df_depts[df_depts['status']==1]['dept_name'].tolist() if not df_depts.empty else [])
    with c3: q_status = st.multiselect("状态", options=["在职", "离职"], default=["在职"])
    with c4: st.write(""); export_btn = st.button("📥 导出当前筛选名单")

    f_df = df_emps.copy()
    if not f_df.empty:
        if q_name: f_df = f_df[f_df['name'].str.contains(q_name, na=False) | f_df['emp_id'].str.contains(q_name, na=False)]
        if q_dept: f_df = f_df[f_df['dept_name'].isin(q_dept)]
        if q_status: f_df = f_df[f_df['status'].isin(q_status)]

    disp = f_df.rename(columns=EMP_COL_MAP)

    # [核心重构 2026-03-26] 引入交互式打勾选择联动体验
    t_emp_selected = None
    if not disp.empty:
        ui_cols = ['工号', '姓名', '部门', '岗位', 'T级', '岗级', '档次', '状态']
        disp_ui = disp[ui_cols].copy()
        disp_ui.insert(0, "✅勾选修改", False)

        # 利用 data_editor 实现点击交互
        edited_df = st.data_editor(
            disp_ui,
            hide_index=True,
            use_container_width=True,
            column_config={"✅勾选修改": st.column_config.CheckboxColumn(required=True)}
        )

        # 捕捉被打勾的行
        selected_rows = edited_df[edited_df["✅勾选修改"] == True]
        if not selected_rows.empty:
            sel_id = selected_rows.iloc[0]['工号']
            t_emp_selected = df_emps[df_emps['emp_id'] == sel_id].iloc[0]
            st.success(f"已选中人员: {t_emp_selected['name']}，请在下方修改信息。")
    else:
        st.warning("暂无人员数据。")

    if export_btn:
        eo = ['工号', '姓名', '部门', '岗位', '身份证号', '岗级', '档次', 'T级', '入职日期', '学历', '学位', '毕业院校', '专业', '毕业日期', '参加工作日期', '状态']
        out_df = disp[[c for c in eo if c in disp.columns]] if not disp.empty else pd.DataFrame(columns=eo)
        ob = io.BytesIO()
        with pd.ExcelWriter(ob, engine='openpyxl') as w: out_df.to_excel(w, index=False, sheet_name='花名册')
        st.download_button("点击下载 Excel", data=ob.getvalue(), file_name=f"人员档案.xlsx")

    st.divider()

    with st.expander("📥 批量智能导入 (已剔除职级权重)"):
        t1, t2 = st.columns(2)
        # [核心优化 2026-03-26] 从模板中彻底移除 '职级权重'，并补全学籍字段，符合实战需求。
        icols = ['工号', '姓名', '所属部门', '岗位', '技术等级(T级)', '身份证号', '岗级', '档次', '参加工作日期', '入职日期', '学历', '学位', '毕业院校', '专业', '毕业日期']
        with t1:
            tmp = pd.DataFrame(columns=icols); tout = io.BytesIO()
            with pd.ExcelWriter(tout, engine='openpyxl') as w: tmp.to_excel(w, index=False)
            st.download_button("下载极简版模板", data=tout.getvalue(), file_name="人员导入模板.xlsx")
        with t2:
            uf = st.file_uploader("上传 Excel", type=["xlsx"])
            if uf and st.button("执行导入"):
                idf = pd.read_excel(uf); sc = 0; errs = []
                _, c_d = get_all_departments(True); d_df = pd.DataFrame(c_d)
                _, c_p = get_all_positions(True); p_df = pd.DataFrame(c_p)

                for idx, row in idf.iterrows():
                    i_d = clean_str(row.get('所属部门')); td = None
                    if i_d:
                        md = d_df[d_df['dept_name']==i_d]
                        if not md.empty: td = int(md.iloc[0]['dept_id'])
                        else:
                            add_department(i_d, "其他", None, 999); _, rd = get_all_departments(True); d_df = pd.DataFrame(rd)
                            td = int(d_df[d_df['dept_name']==i_d].iloc[0]['dept_id'])
                    else: errs.append(f"行{idx+2}: 部门空"); continue

                    i_p = clean_str(row.get('岗位')); tp = None
                    if i_p:
                        mp = p_df[p_df['pos_name']==i_p]
                        if not mp.empty: tp = int(mp.iloc[0]['pos_id'])
                        else:
                            add_position(i_p, "通用"); _, rp = get_all_positions(True); p_df = pd.DataFrame(rp)
                            tp = int(p_df[p_df['pos_name']==i_p].iloc[0]['pos_id'])

                    ed = {
                        'emp_id': clean_str(row.get('工号')), 'name': clean_str(row.get('姓名')),
                        'id_card': clean_str(row.get('身份证号')), 'dept_id': td,
                        'post_rank': int(row.get('岗级', 11)) if pd.notna(row.get('岗级')) else 11,
                        'post_grade': clean_str(row.get('档次')) or 'E',
                        'join_company_date': clean_date(row.get('入职日期'))
                    }
                    pd_info = {
                        'pos_id': tp, 'tech_grade': clean_str(row.get('技术等级(T级)')),
                        'title_order': 999, # 后台静默兜底赋 999
                        'education_level': clean_str(row.get('学历')),
                        'degree': clean_str(row.get('学位')),
                        'school_name': clean_str(row.get('毕业院校')),
                        'major': clean_str(row.get('专业')),
                        'graduation_date': clean_date(row.get('毕业日期')),
                        'first_job_date': clean_date(row.get('参加工作日期'))
                    }

                    ok, msg = add_employee(ed, pd_info, "初始批量导入")
                    if ok: sc += 1
                    else: errs.append(f"行{idx+2}: {msg}")
                if errs: st.error("\n".join(errs))
                else: set_msg_and_rerun(f"导入成功 {sc} 人")

    # --- 所见即回填维护区 ---
    st.subheader("📝 单条维护区")
    if t_emp_selected is None:
        st.info("🟡 当前处于【新增模式】。如需修改档案，请在上方表格中打勾选中对应员工。")

    with st.form("emp_form", clear_on_submit=True):
        f1, f2, f3 = st.columns(3)
        with f1:
            fid = st.text_input("工号*", value=t_emp_selected.get('emp_id',"") if t_emp_selected is not None else "", disabled=(t_emp_selected is not None))
            fname = st.text_input("姓名*", value=t_emp_selected.get('name',"") if t_emp_selected is not None else "")
            fidcard = st.text_input("身份证号", value=t_emp_selected.get('id_card',"") if t_emp_selected is not None and pd.notna(t_emp_selected.get('id_card')) else "")
        with f2:
            vd = df_depts[df_depts['status']==1]; dm = {r['dept_id']: r['dept_name'] for _, r in vd.iterrows()}
            def_d = t_emp_selected.get('dept_id') if t_emp_selected is not None and t_emp_selected.get('dept_id') in dm else (list(dm.keys())[0] if dm else None)
            fdept = st.selectbox("归属部门*", options=list(dm.keys()), format_func=lambda x: dm[x], index=list(dm.keys()).index(def_d) if def_d else 0) if dm else None

            frank = st.number_input("发薪岗级*", 1, 28, int(t_emp_selected.get('post_rank', 11)) if t_emp_selected is not None and pd.notna(t_emp_selected.get('post_rank')) else 11)
            g_opts = ["A","B","C","D","E","F","G","H","I","J"]
            def_g = t_emp_selected.get('post_grade') if t_emp_selected is not None and t_emp_selected.get('post_grade') in g_opts else "E"
            fgrade = st.selectbox("发薪档次*", g_opts, index=g_opts.index(def_g))
        with f3:
            vp = df_positions[df_positions['status']==1]; pm = {r['pos_id']: r['pos_name'] for _, r in vp.iterrows()}
            def_p = t_emp_selected.get('pos_id') if t_emp_selected is not None and pd.notna(t_emp_selected.get('pos_id')) and t_emp_selected.get('pos_id') in pm else (list(pm.keys())[0] if pm else None)
            fpos = st.selectbox("绑定岗位", options=list(pm.keys()), format_func=lambda x: pm[x], index=list(pm.keys()).index(def_p) if def_p else 0) if pm else None

            ftgrade = st.text_input("技术等级 (如 T1, T1-)", value=t_emp_selected.get('tech_grade',"") if t_emp_selected is not None and pd.notna(t_emp_selected.get('tech_grade')) else "")
            fjoin = st.date_input("入职日期", value=pd.to_datetime(t_emp_selected.get('join_company_date')) if t_emp_selected is not None and pd.notna(t_emp_selected.get('join_company_date')) else None)

        st.write("**--- 附加学籍与状态 ---**")
        fp1, fp2, fp3 = st.columns(3)
        with fp1:
            e_opts = ["", "中专", "大专", "本科", "硕士", "博士"]
            def_e = t_emp_selected.get('education_level') if t_emp_selected is not None and t_emp_selected.get('education_level') in e_opts else ""
            f_edu = st.selectbox("学历", e_opts, index=e_opts.index(def_e))
            f_degree = st.text_input("学位", value=t_emp_selected.get('degree', "") if t_emp_selected is not None and pd.notna(t_emp_selected.get('degree')) else "")
        with fp2:
            f_school = st.text_input("毕业院校", value=t_emp_selected.get('school_name', "") if t_emp_selected is not None and pd.notna(t_emp_selected.get('school_name')) else "")
            f_major = st.text_input("所学专业", value=t_emp_selected.get('major', "") if t_emp_selected is not None and pd.notna(t_emp_selected.get('major')) else "")
        with fp3:
            f_grad_date = st.date_input("毕业日期", value=pd.to_datetime(t_emp_selected.get('graduation_date')) if t_emp_selected is not None and pd.notna(t_emp_selected.get('graduation_date')) else None)
            f_first_work = st.date_input("参加工作日期", value=pd.to_datetime(t_emp_selected.get('first_job_date')) if t_emp_selected is not None and pd.notna(t_emp_selected.get('first_job_date')) else None)

        col_s1, col_s2 = st.columns([1, 2])
        with col_s1:
            s_opts = ["在职", "离职", "退休"]
            def_s = t_emp_selected.get('status') if t_emp_selected is not None and t_emp_selected.get('status') in s_opts else "在职"
            f_status = st.selectbox("人员状态", s_opts, index=s_opts.index(def_s))
        with col_s2:
            freason = st.text_input("变动说明 (若系统监测到核心算薪指标变动，将抓取此说明并生成快照)", placeholder="如: 2026年3月跨部门调动")

        if st.form_submit_button("执行保存与快照比对"):
            if not fid or not fname or fdept is None: st.error("工号、姓名、归属部门必填")
            else:
                # 强制转换为原生 int 防止穿透错误
                ed = {'emp_id': fid, 'name': fname, 'id_card': fidcard, 'dept_id': int(fdept), 'post_rank': int(frank), 'post_grade': fgrade, 'join_company_date': clean_date(fjoin)}
                pd_info = {'pos_id': int(fpos) if fpos else None, 'tech_grade': ftgrade, 'title_order': 999, 'education_level': f_edu, 'degree': f_degree, 'school_name': f_school, 'major': f_major, 'graduation_date': clean_date(f_grad_date), 'first_job_date': clean_date(f_first_work)}
                if t_emp_selected is not None: ok, msg = update_employee(fid, ed, pd_info, freason or "档案更新")
                else: ok, msg = add_employee(ed, pd_info, freason or "手工新增")
                if ok: set_msg_and_rerun(msg)
                else: st.error(msg)

# ==============================================================================
# 模块 D: 🕰️ 历史变动流水
# ==============================================================================
elif current_page == "🕰️ 历史变动流水":
    st.subheader("🕰️ 全局时空追踪大屏")
    st.info("💡 只有当员工的 [部门/岗位/T级/岗级/档次] 发生实质性变动时，系统才会拍下包含所有基准指标的全身快照，以确保薪酬回溯时上下文完整。")

    # [核心重构 2026-03-26] 引入多维筛选器
    ok, h_list = get_all_history()
    if ok and h_list:
        hdf = pd.DataFrame(h_list)

        hc1, hc2, hc3 = st.columns(3)
        with hc1:
            q_h_search = st.text_input("姓名或工号模糊匹配")
        with hc2:
            type_list = hdf['change_type'].unique().tolist()
            q_h_type = st.multiselect("变动类型筛选", options=type_list)
        with hc3:
            dept_list = [d for d in hdf['old_dept_name'].unique().tolist() if d]
            q_h_dept = st.multiselect("发生变动的原部门", options=dept_list)

        if q_h_search:
            hdf = hdf[hdf['emp_id'].str.contains(q_h_search, na=False) | hdf['emp_name'].str.contains(q_h_search, na=False)]
        if q_h_type: hdf = hdf[hdf['change_type'].isin(q_h_type)]
        if q_h_dept: hdf = hdf[hdf['old_dept_name'].isin(q_h_dept)]

        if not hdf.empty:
            for idx, row in hdf.iterrows():
                with st.container(border=True):
                    c1, c2, c3 = st.columns([1, 4, 1])
                    with c1:
                        st.write(f"**{row['emp_name']}** ({row['emp_id']})")
                        st.caption(f"[{row['change_type']}]")
                    with c2:
                        st.write(f"📍 **改前部门**: {row['old_dept_name'] or '-'} | **改后部门**: {row['new_dept_name'] or '-'}")
                        st.write(f"💼 **改前岗位**: {row['old_pos_name'] or '-'} | **改后岗位**: {row['new_pos_name'] or '-'}")
                        st.write(f"💰 **改前待遇基准**: {row['old_tech_grade'] or '无T'} / {row['old_post_rank'] or '-'}岗{row['old_post_grade'] or '-'}档 ➡️ **改后**: {row['new_tech_grade'] or '无T'} / {row['new_post_rank'] or '-'}岗{row['new_post_grade'] or '-'}档")
                        st.caption(f"说明: {row['change_reason']} | 变动发生时间: {row['change_date']}")
                    with c3:
                        if st.button("⏪ 撤销该变动", key=f"rb_{row['change_id']}", help="将该员工所有待遇基准立刻回退至改前状态"):
                            ro, rmsg = rollback_history(row['change_id'])
                            if ro: set_msg_and_rerun(rmsg)
                            else: st.error(rmsg)
        else:
            st.warning("查无匹配的历史快照。")
    else:
        st.info("系统底座尚未产生任何有效的人员薪酬变动快照。")