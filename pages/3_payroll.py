# ==============================================================================
# 文件路径: pages/3_payroll.py
# 功能描述: 薪酬核算与多平台分发工作台 (全量完整版 - 修复 SQL 语法冲突)
# ==============================================================================

import streamlit as st
import pandas as pd
import sqlite3
import os
import json
import datetime
from modules.core_social_security import _get_db_connection

st.set_page_config(page_title="薪酬核算与发放", layout="wide")

st.title("💸 薪酬核算与多平台分发中心")
st.caption("🔒 核心流向：参数字典维护 ➡️ 抓取社保与算力底表 ➡️ 个税回灌与最终结算 ➡️ 台账封账")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📂 第一步：专项奖池与动态预埋",
    "🧮 第二步：生成底表与绩效算力",
    "📥 第三步：清算、扣税与最终结账",
    "📜 综合查询与发薪凭证",
    "⚙️ 全局参数与薪酬字典 (总阀门)"
])


# ==============================================================================
# 加载与保存薪酬字典 (包含非线性二维矩阵)
# ==============================================================================
def load_payroll_dicts():
    dict_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'payroll_dicts.json')
    if not os.path.exists(dict_path):
        default_dict = {
            "salary_matrix": {
                "28": {"A": 11500, "B": 12420, "C": 13410, "D": 14490, "E": 15650, "F": 16900, "G": 18250, "H": 19710,
                       "I": 21290, "J": 22990},
                "27": {"A": 10000, "B": 10800, "C": 11660, "D": 12600, "E": 13600, "F": 14690, "G": 15870, "H": 17140,
                       "I": 18510, "J": 19990},
                "26": {"A": 8500, "B": 9180, "C": 9910, "D": 10710, "E": 11560, "F": 12490, "G": 13490, "H": 14570,
                       "I": 15730, "J": 16990},
                "25": {"A": 7500, "B": 8100, "C": 8750, "D": 9450, "E": 10200, "F": 11020, "G": 11900, "H": 12850,
                       "I": 13880, "J": 14990},
                "24": {"A": 6500, "B": 7020, "C": 7580, "D": 8190, "E": 8840, "F": 9550, "G": 10310, "H": 11140,
                       "I": 12030, "J": 12990},
                "23": {"A": 5500, "B": 5940, "C": 6420, "D": 6930, "E": 7480, "F": 8080, "G": 8730, "H": 9430,
                       "I": 10180, "J": 10990},
                "22": {"A": 4500, "B": 4860, "C": 5250, "D": 5670, "E": 6120, "F": 6610, "G": 7140, "H": 7710,
                       "I": 8330, "J": 9000},
                "21": {"A": 4150, "B": 4480, "C": 4840, "D": 5230, "E": 5650, "F": 6100, "G": 6590, "H": 7110,
                       "I": 7680, "J": 8300},
                "20": {"A": 3800, "B": 4100, "C": 4430, "D": 4790, "E": 5170, "F": 5580, "G": 6030, "H": 6510,
                       "I": 7030, "J": 7600},
                "19": {"A": 3450, "B": 3730, "C": 4020, "D": 4350, "E": 4690, "F": 5070, "G": 5470, "H": 5910,
                       "I": 6390, "J": 6900},
                "18": {"A": 3100, "B": 3350, "C": 3620, "D": 3910, "E": 4220, "F": 4550, "G": 4920, "H": 5310,
                       "I": 5740, "J": 6200},
                "17": {"A": 2750, "B": 2970, "C": 3210, "D": 3460, "E": 3740, "F": 4040, "G": 4360, "H": 4710,
                       "I": 5090, "J": 5500},
                "16": {"A": 2400, "B": 2590, "C": 2800, "D": 3020, "E": 3270, "F": 3530, "G": 3810, "H": 4110,
                       "I": 4440, "J": 4800},
                "15": {"A": 2200, "B": 2380, "C": 2570, "D": 2770, "E": 2990, "F": 3230, "G": 3490, "H": 3770,
                       "I": 4070, "J": 4400},
                "14": {"A": 2000, "B": 2160, "C": 2330, "D": 2520, "E": 2720, "F": 2940, "G": 3170, "H": 3430,
                       "I": 3700, "J": 4000},
                "13": {"A": 1800, "B": 1940, "C": 2100, "D": 2270, "E": 2450, "F": 2640, "G": 2860, "H": 3080,
                       "I": 3330, "J": 3600},
                "12": {"A": 1600, "B": 1730, "C": 1870, "D": 2020, "E": 2180, "F": 2350, "G": 2540, "H": 2740,
                       "I": 2960, "J": 3200},
                "11": {"A": 1400, "B": 1510, "C": 1630, "D": 1760, "E": 1900, "F": 2060, "G": 2220, "H": 2400,
                       "I": 2590, "J": 2800},
                "10": {"A": 1200, "B": 1300, "C": 1400, "D": 1510, "E": 1630, "F": 1760, "G": 1900, "H": 2060,
                       "I": 2220, "J": 2400},
                "9": {"A": 1000, "B": 1080, "C": 1170, "D": 1260, "E": 1360, "F": 1470, "G": 1590, "H": 1710, "I": 1850,
                      "J": 2000},
                "8": {"A": 950, "B": 1030, "C": 1110, "D": 1200, "E": 1290, "F": 1400, "G": 1510, "H": 1630, "I": 1760,
                      "J": 1900},
                "7": {"A": 900, "B": 970, "C": 1050, "D": 1130, "E": 1220, "F": 1320, "G": 1430, "H": 1540, "I": 1670,
                      "J": 1800},
                "6": {"A": 850, "B": 920, "C": 990, "D": 1070, "E": 1160, "F": 1250, "G": 1350, "H": 1460, "I": 1570,
                      "J": 1700},
                "5": {"A": 800, "B": 860, "C": 930, "D": 1010, "E": 1090, "F": 1180, "G": 1270, "H": 1370, "I": 1480,
                      "J": 1600},
                "4": {"A": 750, "B": 810, "C": 870, "D": 940, "E": 1020, "F": 1100, "G": 1190, "H": 1290, "I": 1390,
                      "J": 1500},
                "3": {"A": 700, "B": 760, "C": 820, "D": 880, "E": 950, "F": 1030, "G": 1110, "H": 1200, "I": 1300,
                      "J": 1400},
                "2": {"A": 650, "B": 700, "C": 760, "D": 820, "E": 880, "F": 960, "G": 1030, "H": 1110, "I": 1200,
                      "J": 1300},
                "1": {"A": 600, "B": 650, "C": 700, "D": 760, "E": 820, "F": 880, "G": 950, "H": 1030, "I": 1110,
                      "J": 1200}
            },
            "perf_matrix": {
                "20": {"base": 3700, "coef": 2.8},
                "19": {"base": 3750, "coef": 2.6},
                "18": {"base": 3450, "coef": 2.5},
                "17": {"base": 3300, "coef": 2.3},
                "16": {"base": 3150, "coef": 2.2},
                "15": {"base": 2840, "coef": 2.1},
                "14": {"base": 2700, "coef": 1.9},
                "13": {"base": 2550, "coef": 1.8},
                "12": {"base": 2000, "coef": 1.7},
                "11": {"base": 1500, "coef": 1.6}
            },
            "t_level_map": {"T1": 3900, "T2": 3300, "T3": 3000},
            "expert_allowance": {"一级专家": 1500, "二级专家": 860}
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

# ------------------------------------------------------------------------------
# Tab 1: 专项奖池与动态预埋 (明细账蓄水池)
# ------------------------------------------------------------------------------
with tab1:
    st.subheader("🎁 专项奖金与特殊项目池 (明细录入)")
    st.info("⚠️ 必须先在 Tab 2 执行过【抓取】，此处的汇总才会生效！")

    bonus_month = st.text_input("📅 奖金发放月份 (如: 2026-04)", value=datetime.date.today().strftime("%Y-%m"),
                                key="tab1_month")

    # 单笔录入表单
    with st.form("add_special_bonus_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            b_emp_id = st.text_input("工号 (必填)")
        with c2:
            b_proj_name = st.text_input("项目名称 (如: 13薪)")
        with c3:
            b_amount = st.number_input("金额 (元)", value=0.0)
        if st.form_submit_button("➕ 确认录入明细") and b_emp_id and b_proj_name:
            conn = _get_db_connection()
            cursor = conn.cursor()
            try:
                bonus_id = f"{bonus_month}_{b_emp_id}_{b_proj_name}"
                cursor.execute("""
                               INSERT INTO payroll_special_bonus (bonus_id, cost_month, emp_id, project_name, amount)
                               VALUES (?, ?, ?, ?, ?) ON CONFLICT(bonus_id) DO
                               UPDATE SET amount=excluded.amount
                               """, (bonus_id, bonus_month, b_emp_id, b_proj_name, b_amount))
                conn.commit()
                st.success(f"✅ 已记录: {b_emp_id} - {b_proj_name}")
            except Exception as e:
                st.error(f"录入错误: {e}")
            finally:
                conn.close()

    st.divider()

    # 查询与推送
    conn = _get_db_connection()
    df_list = pd.read_sql_query("""
                                SELECT b.emp_id AS '工号', e.name AS '姓名', b.project_name AS '名目', b.amount AS '金额'
                                FROM payroll_special_bonus b
                                         LEFT JOIN employees e ON b.emp_id = e.emp_id
                                WHERE b.cost_month = ?
                                """, conn, params=[bonus_month])

    if not df_list.empty:
        st.dataframe(df_list, use_container_width=True, hide_index=True)
        if st.button("🔄 汇总并强力推送到主账本 (Tab 3)", type="primary"):
            cursor = conn.cursor()
            # 改进后的 SQL：确保只更新已存在的月份记录，且汇总不为空
            update_sql = """
                         UPDATE payroll_monthly_records
                         SET special_bonus_total = (SELECT SUM(amount) \
                                                    FROM payroll_special_bonus \
                                                    WHERE emp_id = payroll_monthly_records.emp_id \
                                                      AND cost_month = payroll_monthly_records.cost_month)
                         WHERE cost_month = ? \
                         """
            cursor.execute(update_sql, (bonus_month,))
            conn.commit()
            conn.close()
            st.success("✅ 汇总成功！正在刷新数据...")
            # 【关键】强制 Streamlit 重新运行，确保 Tab 3 看到的是最新库数据
            st.rerun()
    else:
        st.caption("当前月份暂无明细。")
    conn.close()

# ------------------------------------------------------------------------------
# Tab 2: 生成底表与绩效算力 (引擎组装核心区)
# ------------------------------------------------------------------------------
with tab2:
    st.subheader("⚙️ 薪资主盘备料与绩效算力点火")
    st.warning("⚠️ 启动引擎前，请确保本月【人员调动/调薪】已维护完毕，且【社保模块】已生成当期正式账单！")

    default_month = datetime.date.today().strftime("%Y-%m")
    calc_month = st.text_input("📅 输入当前薪酬核算月份 (格式 YYYY-MM)", value=default_month)

    if st.button("🚀 1. 抓取固定底薪与社保代扣 (生成初版底表)", type="primary"):
        conn = _get_db_connection()
        cursor = conn.cursor()
        try:
            sql = """
                  SELECT e.emp_id, \
                         e.name, \
                         d.dept_name, \
                         IFNULL(e.post_rank, '无')                                         AS post_rank, \
                         IFNULL(e.post_grade, '无')                                        AS post_grade, \
                         IFNULL(p.tech_grade, '无')                                        AS tech_grade, \
                         IFNULL(s.pension_pers, 0.0)                                       AS ss_pension, \
                         IFNULL(s.medical_pers, 0.0) + IFNULL(s.medical_serious_pers, 0.0) AS ss_medical, \
                         IFNULL(s.unemp_pers, 0.0)                                         AS ss_unemp, \
                         IFNULL(s.fund_pers, 0.0)                                          AS ss_fund, \
                         IFNULL(s.annuity_pers, 0.0)                                       AS ss_annuity
                  FROM employees e
                           LEFT JOIN departments d ON e.dept_id = d.dept_id
                           LEFT JOIN employee_profiles p ON e.emp_id = p.emp_id
                           LEFT JOIN ss_monthly_records s ON e.emp_id = s.emp_id AND s.cost_month = ?
                  WHERE e.status = '在职' \
                  """
            base_df = pd.read_sql_query(sql, conn, params=[calc_month])

            count = 0
            for _, row in base_df.iterrows():
                eid = row['emp_id']
                rank = str(row['post_rank'])
                grade = str(row['post_grade'])
                t_grade = str(row['tech_grade'])

                base_sal = 0.0
                if rank in curr_dicts["salary_matrix"] and grade in curr_dicts["salary_matrix"][rank]:
                    base_sal = float(curr_dicts["salary_matrix"][rank][grade])

                perf_base_val = 0.0
                if rank in curr_dicts["perf_matrix"]:
                    perf_base_val = float(curr_dicts["perf_matrix"][rank]["base"]) * float(
                        curr_dicts["perf_matrix"][rank]["coef"])

                pack_base = 0.0
                if t_grade in curr_dicts["t_level_map"]:
                    pack_base = float(curr_dicts["t_level_map"][t_grade])

                rec_id = f"{calc_month}_{eid}"

                upsert_sql = """
                             INSERT INTO payroll_monthly_records (record_id, cost_month, emp_id, dept_name, \
                                                                  base_salary, perf_standard, perf_base, \
                                                                  ss_pension_pers, ss_medical_mix, ss_unemp_pers, \
                                                                  ss_fund_pers, ss_annuity_pers) \
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(record_id) DO \
                             UPDATE SET
                                 dept_name=excluded.dept_name, \
                                 base_salary=excluded.base_salary, \
                                 perf_standard=excluded.perf_standard, \
                                 perf_base=excluded.perf_base, \
                                 ss_pension_pers=excluded.ss_pension_pers, \
                                 ss_medical_mix=excluded.ss_medical_mix, \
                                 ss_unemp_pers=excluded.ss_unemp_pers, \
                                 ss_fund_pers=excluded.ss_fund_pers, \
                                 ss_annuity_pers=excluded.ss_annuity_pers \
                             """
                cursor.execute(upsert_sql, (
                    rec_id, calc_month, eid, row['dept_name'],
                    base_sal, perf_base_val, pack_base,
                    row['ss_pension'], row['ss_medical'], row['ss_unemp'], row['ss_fund'], row['ss_annuity']
                ))
                count += 1

            conn.commit()
            st.success(f"✅ 底盘备料成功！已根据非线性二维矩阵自动匹配 {count} 人的岗位工资与绩效基数，社保倒吸完毕！")
        except Exception as e:
            st.error(f"提取失败: {e}")
        finally:
            conn.close()

    st.divider()
    st.write("🔥 **2. 执行浮动绩效算力 (调整KPI后点火)**")

    conn = _get_db_connection()
    # SQL语法净化：使用双引号，去除了所有单引号和中文括号
    sql_perf = """
               SELECT p.emp_id           AS "工号", \
                      e.name             AS "姓名", \
                      p.base_salary      AS "系统查表_岗位工资", \
                      p.perf_standard    AS "系统查表_绩效基数", \
                      p.perf_base        AS "激励包基数", \
                      p.perf_pack_coef   AS "激励包倍数", \
                      p.perf_kpi_score   AS "本月KPI", \
                      p.perf_leader_coef AS "负责人系数", \
                      p.perf_salary_calc AS "已算出的绩效"
               FROM payroll_monthly_records p
                        JOIN employees e ON p.emp_id = e.emp_id
               WHERE p.cost_month = ? \
               """
    df_perf = pd.read_sql_query(sql_perf, conn, params=[calc_month])

    if not df_perf.empty:
        # 在 Python 层面为用户显示友好的列名，避开 SQL 解析雷区
        df_perf.rename(columns={"本月KPI": "本月KPI(修改这里)"}, inplace=True)

        edited_perf = st.data_editor(
            df_perf,
            column_config={
                "本月KPI(修改这里)": st.column_config.NumberColumn(min_value=0, max_value=150, step=1),
                "激励包倍数": st.column_config.NumberColumn(format="%.2f"),
                "负责人系数": st.column_config.NumberColumn(format="%.2f"),
            },
            disabled=["工号", "姓名", "系统查表_岗位工资", "系统查表_绩效基数", "激励包基数", "已算出的绩效"],
            use_container_width=True, hide_index=True
        )

        if st.button("🧮 计算理论绩效总额并入库"):
            cursor = conn.cursor()
            for _, row in edited_perf.iterrows():
                std = row['系统查表_绩效基数'] or 0
                p_base = row['激励包基数'] or 0
                p_coef = row['激励包倍数'] or 1.0
                kpi = row['本月KPI(修改这里)'] or 100
                l_coef = row['负责人系数'] or 1.0

                calc_val = round((std + p_base * p_coef) * (kpi / 100.0) * l_coef, 2)

                cursor.execute("""
                               UPDATE payroll_monthly_records
                               SET perf_kpi_score   = ?,
                                   perf_pack_coef   = ?,
                                   perf_leader_coef = ?,
                                   perf_salary_calc = ?
                               WHERE cost_month = ?
                                 AND emp_id = ?
                               """, (kpi, p_coef, l_coef, calc_val, calc_month, row['工号']))
            conn.commit()
            st.success(f"✅ 绩效核算完毕！底层数据库已刷新。")
            st.rerun()
    else:
        st.info("👆 请先在上方执行【抓取固定底薪与社保】按钮生成底表。")
    conn.close()

# ------------------------------------------------------------------------------
# Tab 3: 财务个税回灌与结算
# ------------------------------------------------------------------------------
with tab3:
    st.subheader("🏦 三/四期工程：手工调账、个税回灌与结账封盘")
    st.info("💡 系统将自动合并各项收入与扣款，并在扣除个税后形成打款实发数！")

    conn = _get_db_connection()
    # 终极 SQL 语法净化：绝不混入任何注释(--)和奇怪符号
    sql_final = """
                SELECT p.emp_id                                                   AS "工号", \
                       e.name                                                     AS "姓名", \
                       p.base_salary                                              AS "底薪", \
                       IFNULL(p.perf_salary_calc, 0)                              AS "已算绩效", \
                       IFNULL(p.history_clearance, 0)                             AS "历史清算", \
                       IFNULL(p.promotion_backpay, 0)                             AS "晋升补发", \
                       IFNULL(p.special_bonus_total, 0)                           AS "专项奖合计", \
                       (IFNULL(p.ss_pension_pers, 0) + IFNULL(p.ss_medical_mix, 0) + IFNULL(p.ss_unemp_pers, 0) + \
                        IFNULL(p.ss_fund_pers, 0) + IFNULL(p.ss_annuity_pers, 0)) AS "五险两金代扣", \
                       IFNULL(p.tax_deduction, 0)                                 AS "代扣个税", \
                       p.gross_salary_total                                       AS "系统算_应发总计", \
                       p.net_salary                                               AS "系统算_最终实发"
                FROM payroll_monthly_records p
                         JOIN employees e ON p.emp_id = e.emp_id
                WHERE p.cost_month = ? \
                """
    df_final = pd.read_sql_query(sql_final, conn, params=[calc_month])

    if not df_final.empty:
        # 在 Python 层面安全重命名，防爆引擎
        df_final.rename(columns={
            "历史清算": "历史清算(填负数)",
            "代扣个税": "代扣个税(录入)"
        }, inplace=True)

        st.write("👉 **手工补调与个税录入区 (修改数字后按回车保存)**")
        edited_final = st.data_editor(
            df_final,
            column_config={
                "历史清算(填负数)": st.column_config.NumberColumn(format="%.2f"),
                "晋升补发": st.column_config.NumberColumn(format="%.2f"),
                "专项奖合计": st.column_config.NumberColumn(format="%.2f"),
                "代扣个税(录入)": st.column_config.NumberColumn(format="%.2f"),
            },
            disabled=["工号", "姓名", "底薪", "已算绩效", "五险两金代扣", "系统算_应发总计", "系统算_最终实发"],
            use_container_width=True, hide_index=True
        )

        if st.button("🔥 终极大结账！生成实发工资！", type="primary"):
            cursor = conn.cursor()
            for _, row in edited_final.iterrows():
                gross = (row['底薪'] + row['已算绩效'] + row['晋升补发'] + row['专项奖合计'] + row['历史清算(填负数)'])
                net = gross - row['五险两金代扣'] - row['代扣个税(录入)']

                cursor.execute("""
                               UPDATE payroll_monthly_records
                               SET history_clearance   = ?,
                                   promotion_backpay   = ?,
                                   special_bonus_total = ?,
                                   tax_deduction       = ?,
                                   gross_salary_total  = ?,
                                   net_salary          = ?
                               WHERE cost_month = ?
                                 AND emp_id = ?
                               """, (
                                   row['历史清算(填负数)'], row['晋升补发'], row['专项奖合计'], row['代扣个税(录入)'],
                                   round(gross, 2), round(net, 2),
                                   calc_month, row['工号']
                               ))
            conn.commit()
            st.success("✅ 全员结账完毕！所有应发、代扣、实发数字已彻底封印入库，随时可推送台账。")
            st.rerun()
    else:
        st.warning("⚠️ 请先在 Tab 2 中生成底表。")
    conn.close()

# ------------------------------------------------------------------------------
# Tab 4: 综合查询与发薪凭证
# ------------------------------------------------------------------------------
with tab4:
    st.subheader("🖨️ 历史薪酬总账与发卡清单")
    st.write("🔍 查询功能建设中：将提供1:1复刻原始报表的下载，以及银行代发清单的导出。")

# ------------------------------------------------------------------------------
# Tab 5: 全局参数与薪酬字典
# ------------------------------------------------------------------------------
with tab5:
    st.subheader("🛠️ 非线性薪酬矩阵与算力参数")
    st.info("💡 这是系统『智能算钱引擎』的心脏！这里的参数完全从你上传的PDF规则中原汁原味破译而来。")

    c_p1, c_p2 = st.columns([1.5, 1])

    with c_p1:
        st.write("💰 **1. 岗位工资二维矩阵 (行:岗级, 列:档次)**")
        df_matrix = pd.DataFrame.from_dict(curr_dicts["salary_matrix"], orient='index')
        df_matrix.index.name = "岗级"
        df_matrix.reset_index(inplace=True)
        df_matrix["岗级数值"] = pd.to_numeric(df_matrix["岗级"])
        df_matrix = df_matrix.sort_values(by="岗级数值", ascending=False).drop(columns=["岗级数值"])
        edited_matrix = st.data_editor(df_matrix, use_container_width=True, hide_index=True)

    with c_p2:
        st.write("🚀 **2. 绩效标准 (包含手写基数)**")
        df_perf = pd.DataFrame.from_dict(curr_dicts["perf_matrix"], orient='index')
        df_perf.index.name = "岗级"
        df_perf.reset_index(inplace=True)
        df_perf.rename(columns={"base": "绩效基数(手写值)", "coef": "绩效系数"}, inplace=True)
        df_perf["岗级数值"] = pd.to_numeric(df_perf["岗级"])
        df_perf = df_perf.sort_values(by="岗级数值", ascending=False).drop(columns=["岗级数值"])
        edited_perf = st.data_editor(df_perf, use_container_width=True, hide_index=True)

        st.write("🏅 **3. T序列激励包与专家津贴**")
        df_t = pd.DataFrame(list(curr_dicts["t_level_map"].items()), columns=["级别", "金额"])
        edited_t = st.data_editor(df_t, num_rows="dynamic", use_container_width=True, hide_index=True)

    if st.button("💾 覆盖保存全量薪酬字典 (高危操作)", type="primary"):
        try:
            new_salary_matrix = {}
            for _, row in edited_matrix.iterrows():
                rank = str(row["岗级"])
                new_salary_matrix[rank] = {col: row[col] for col in edited_matrix.columns if
                                           col != "岗级" and not pd.isna(row[col])}

            new_perf_matrix = {}
            for _, row in edited_perf.iterrows():
                rank = str(row["岗级"])
                new_perf_matrix[rank] = {"base": float(row["绩效基数(手写值)"]), "coef": float(row["绩效系数"])}

            new_t_map = {str(row["级别"]).strip(): float(row["金额"]) for _, row in edited_t.iterrows() if
                         str(row["级别"]).strip()}

            new_dicts = {
                "salary_matrix": new_salary_matrix,
                "perf_matrix": new_perf_matrix,
                "t_level_map": new_t_map,
                "expert_allowance": curr_dicts.get("expert_allowance", {})
            }
            save_payroll_dicts(new_dicts)
            st.success("✅ 二维算力矩阵更新成功！")
        except Exception as e:
            st.error(f"❌ 数据格式错误，保存失败: {e}")