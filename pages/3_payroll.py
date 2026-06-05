# ==============================================================================
# 文件路径: pages/3_payroll.py
# 功能描述: 薪酬核算与多平台分发工作台 (全量完整版 - 修复 SQL 语法冲突)
# ==============================================================================

import streamlit as st
import pandas as pd
import sqlite3
import os
import io
import uuid
import json
import datetime
from modules.core_social_security import _get_db_connection

st.set_page_config(page_title="薪酬核算与发放", layout="wide")

st.title("💸 薪酬核算与多平台分发中心")
st.caption("🔒 核心流向：参数字典维护 ➡️ 抓取社保与算力底表 ➡️ 个税回灌与最终结算 ➡️ 台账封账")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📂 第一步：月度项目池导入",
    "🧮 第二步：生成底表与绩效算力",
    "📥 第三步：扣税与草稿结账",
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

# ==============================================================================
# 薪酬专用工具函数：把“人员排序岗级”转换成“薪酬计算岗级”
# ==============================================================================
def normalize_rank_for_payroll(raw_rank):
    """
    这个函数是干什么的？
    ------------------------------------------------------------
    它负责把人员模块里的岗级，转换成薪酬模块能识别的岗级。

    为什么需要这个函数？
    ------------------------------------------------------------
    你的人事模块里，岗级字段 post_rank 不只是用来算工资，
    还被你拿来做领导排序。

    例如：
    - 21
    - 21.5
    - 21.8
    - 21.99

    这些人在业务上仍然都是“21岗”，
    但是为了让公司副总、同岗级领导在名单里有先后顺序，
    你用小数点做了排序区分。

    所以：
    - 人员模块排序时，要保留 21.5、21.8、21.99；
    - 薪酬模块算钱时，要统一按 21 岗查工资矩阵。

    这个函数的核心原则：
    ------------------------------------------------------------
    只取整数部分，不四舍五入。

    为什么不能四舍五入？
    ------------------------------------------------------------
    因为 21.99 如果四舍五入会变成 22，
    但它真实薪酬岗级还是 21 岗。
    所以必须是：
    21.99 -> 21
    21.5  -> 21
    21.0  -> 21
    """

    # 第一步：处理空值。
    # 如果数据库里这个人的岗级是空的，就直接返回空字符串。
    # 返回空字符串的结果是：后面查工资矩阵时查不到，工资默认为 0。
    if raw_rank is None:
        return ""

    # 第二步：统一转成字符串，并去掉前后空格。
    # 例如：
    # 21.0      -> "21.0"
    # " 21.5 " -> "21.5"
    rank_text = str(raw_rank).strip()

    # 第三步：处理各种“看起来像空值”的内容。
    # Pandas / SQLite 有时会把空值显示成 nan、None、无。
    # 这些都不是有效岗级，直接返回空字符串。
    if rank_text in ["", "无", "None", "nan", "NaN"]:
        return ""

    try:
        # 第四步：先把文本转成小数。
        # 例如：
        # "21"   -> 21.0
        # "21.5" -> 21.5
        # "21.99"-> 21.99
        rank_number = float(rank_text)

        # 第五步：取整数部分。
        # 注意：int(21.99) 的结果是 21，不是 22。
        # 这正好符合我们的业务要求：
        # 小数点只用于排序，不参与薪酬岗级认定。
        rank_int = int(rank_number)

        # 第六步：转回字符串。
        # 为什么要转字符串？
        # 因为你的薪酬字典 salary_matrix / perf_matrix 的 key 是 "21"、"20" 这种字符串。
        return str(rank_int)

    except Exception:
        # 如果遇到完全无法识别的岗级，比如“副总岗”“二十一岗”这种文本，
        # 就返回空字符串，避免系统直接崩溃。
        return ""

# ------------------------------------------------------------------------------
# Tab 1: 月度项目池导入
# ------------------------------------------------------------------------------
with tab1:
    st.subheader("📂 月度薪酬项目池")
    st.info(
        "💡 这里用于导入每月不稳定的工资项目，例如岗位补/扣、专项奖、提成、考勤扣罚、清算、专家补贴等。"
        "系统会先保存明细流水，再按映射规则汇总到薪酬主账。"
    )

    # ==========================================================
    # 一、基础工具函数
    # ==========================================================

    def safe_money_to_float(value):
        """
        金额清洗函数。

        这个函数是干什么的？
        ------------------------------------------------------------
        Excel 里的金额可能有很多奇怪形态：

        1. 空白
        2. NaN
        3. 1,000.00
        4. " 300 "
        5. "-"
        6. None

        这些东西如果直接 float()，很容易报错。
        所以这里统一清洗成数字。

        返回值：
        ------------------------------------------------------------
        能识别就返回 float。
        不能识别就返回 0.0。
        """

        # 如果是空值，直接当 0。
        if value is None:
            return 0.0

        # pandas 的空值要用 pd.isna 判断。
        try:
            if pd.isna(value):
                return 0.0
        except Exception:
            pass

        # 转成字符串，去掉前后空格。
        text = str(value).strip()

        # 处理各种“看起来不是金额”的内容。
        if text in ["", "-", "—", "无", "None", "nan", "NaN"]:
            return 0.0

        # 去掉千分位逗号。
        # 例如 "1,200.50" -> "1200.50"
        text = text.replace(",", "")

        try:
            return float(text)
        except Exception:
            return 0.0


    def clean_emp_id(value):
        """
        工号清洗函数。

        为什么需要这个函数？
        ------------------------------------------------------------
        Excel 很喜欢把工号 42001943 读成 42001943.0。
        如果不清洗，系统会找不到对应员工。

        这个函数会把：
        42001943.0 -> 42001943
        " 42001943 " -> 42001943
        空值 -> ""
        """

        if value is None:
            return ""

        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass

        text = str(value).strip()

        # 处理 Excel 把整数工号读成小数的情况。
        if text.endswith(".0"):
            text = text[:-2]

        return text


    def load_item_mapping(conn):
        """
        读取项目映射表。

        映射表是什么？
        ------------------------------------------------------------
        payroll_item_mapping 表告诉系统：

        Excel 里的某一列，应该归到哪个工资字段。

        例如：
        岗位补/扣 -> position_adj
        专项奖 -> special_bonus_total
        清算 -> history_clearance
        专家补贴 -> expert_allowance
        """

        mapping_df = pd.read_sql_query(
            """
            SELECT source_column, item_type, target_field, item_name, sign_rule, enabled, remarks
            FROM payroll_item_mapping
            WHERE enabled = 1
            ORDER BY map_id ASC
            """,
            conn
        )

        return mapping_df


    # ==========================================================
    # 二、选择月份
    # ==========================================================

    item_month = st.text_input(
        "📅 项目归属月份",
        value=datetime.date.today().strftime("%Y-%m"),
        help="这里填这批项目要进入哪个月工资，例如 2026-05。"
    )

    conn = _get_db_connection()

    try:
        mapping_df = load_item_mapping(conn)
    except Exception as e:
        st.error(f"读取项目映射表失败：{e}")
        mapping_df = pd.DataFrame()

    # ==========================================================
    # 三、展示当前映射规则
    # ==========================================================

    with st.expander("🧭 当前项目映射规则", expanded=False):
        if mapping_df.empty:
            st.warning("当前没有启用的项目映射规则。请先检查 payroll_item_mapping 表。")
        else:
            show_map = mapping_df.rename(columns={
                "source_column": "Excel列名",
                "item_type": "项目类别",
                "target_field": "汇总目标字段",
                "item_name": "默认项目名称",
                "sign_rule": "符号规则",
                "remarks": "说明"
            })
            st.dataframe(
                show_map[["Excel列名", "项目类别", "汇总目标字段", "默认项目名称", "说明"]],
                use_container_width=True,
                hide_index=True
            )

    # ==========================================================
    # 四、下载导入模板
    # ==========================================================

    st.write("### 1️⃣ 下载项目导入模板")

    if mapping_df.empty:
        st.button("📥 下载模板", disabled=True)
    else:
        # 模板固定前几列。
        # 工号：必须填。
        # 姓名：不是强制，但建议保留，方便人工核对。
        # 备注：整行备注。
        base_cols = ["工号", "姓名"]

        # 根据映射表自动生成金额列。
        # 例如：岗位补/扣、专项奖、提成、考勤扣罚、清算、专家补贴。
        item_cols = mapping_df["source_column"].dropna().tolist()

        tail_cols = ["备注"]

        template_cols = base_cols + item_cols + tail_cols

        template_df = pd.DataFrame(columns=template_cols)

        template_io = io.BytesIO()

        with pd.ExcelWriter(template_io, engine="openpyxl") as writer:
            template_df.to_excel(writer, index=False, sheet_name="月度项目导入模板")

        st.download_button(
            label="📥 下载月度项目导入模板",
            data=template_io.getvalue(),
            file_name=f"月度薪酬项目导入模板_{item_month}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # ==========================================================
    # 五、上传并导入项目宽表
    # ==========================================================

    st.write("### 2️⃣ 上传工资项目宽表")

    uploaded_items_file = st.file_uploader(
        "上传 Excel 或 CSV",
        type=["xlsx", "csv"],
        key="payroll_items_upload"
    )

    overwrite_old_items = st.checkbox(
        "导入前先作废本月已有项目流水",
        value=False,
        help=(
            "如果勾选，系统会把当前月份已有的项目流水 is_active 改成 0，"
            "再导入新数据。适合整月重新导入。"
        )
    )

    if uploaded_items_file is not None:
        if st.button("🚀 执行导入：宽表拆成项目流水", type="primary"):

            try:
                # ------------------------------------------------------
                # 1. 读取上传文件
                # ------------------------------------------------------
                if uploaded_items_file.name.endswith(".csv"):
                    import_df = pd.read_csv(uploaded_items_file)
                    source_sheet_name = "CSV"
                else:
                    # 第一版先读取第一个 sheet。
                    # 后续如果你要支持多个 sheet，我们再扩展。
                    import_df = pd.read_excel(uploaded_items_file)
                    source_sheet_name = "第一个Sheet"

                if import_df.empty:
                    st.warning("上传的表是空的，没有可导入数据。")
                    st.stop()

                # ------------------------------------------------------
                # 2. 基础检查：必须有工号列
                # ------------------------------------------------------
                if "工号" not in import_df.columns:
                    st.error("导入失败：表里必须有【工号】这一列。")
                    st.stop()

                if mapping_df.empty:
                    st.error("导入失败：当前没有启用的项目映射规则。")
                    st.stop()

                cursor = conn.cursor()

                # ------------------------------------------------------
                # 3. 如果用户勾选覆盖，则先作废本月旧流水
                # ------------------------------------------------------
                if overwrite_old_items:
                    cursor.execute(
                        """
                        UPDATE payroll_monthly_items
                        SET is_active = 0
                        WHERE cost_month = ?
                        """,
                        (item_month,)
                    )

                # ------------------------------------------------------
                # 4. 生成本次导入批次号
                # ------------------------------------------------------
                import_batch_id = f"{item_month}_{uuid.uuid4().hex[:12]}"

                success_count = 0
                skipped_zero_count = 0
                skipped_no_emp_count = 0

                # ------------------------------------------------------
                # 5. 逐行读取人员，逐列拆项目
                # ------------------------------------------------------
                for _, row in import_df.iterrows():

                    emp_id = clean_emp_id(row.get("工号"))

                    # 没有工号的行直接跳过。
                    if not emp_id:
                        skipped_no_emp_count += 1
                        continue

                    emp_name_snapshot = str(row.get("姓名", "")).strip() if "姓名" in import_df.columns else ""

                    row_remarks = str(row.get("备注", "")).strip() if "备注" in import_df.columns else ""

                    # 遍历映射表。
                    # mapping_df 里每一行，代表 Excel 某一列应该怎么入账。
                    for _, mp in mapping_df.iterrows():

                        source_col = str(mp["source_column"]).strip()

                        # 如果上传的 Excel 里没有这个列，就跳过。
                        # 这样以后模板列多一点，也不会强制每张表都必须有所有列。
                        if source_col not in import_df.columns:
                            continue

                        amount = safe_money_to_float(row.get(source_col))

                        # 金额为 0 的项目不入库。
                        # 因为工资表通常很多空格，如果 0 也入库，流水会爆炸。
                        if amount == 0.0:
                            skipped_zero_count += 1
                            continue

                        item_id = uuid.uuid4().hex

                        item_type = str(mp["item_type"]).strip() if pd.notna(mp["item_type"]) else "其他"
                        target_field = str(mp["target_field"]).strip() if pd.notna(mp["target_field"]) else "special_bonus_total"

                        # 如果映射表里没有默认项目名，就用 Excel 列名当项目名。
                        item_name = str(mp["item_name"]).strip() if pd.notna(mp["item_name"]) and str(mp["item_name"]).strip() else source_col

                        direction = "加项" if amount > 0 else "减项"

                        cursor.execute(
                            """
                            INSERT INTO payroll_monthly_items (
                                item_id,
                                cost_month,
                                emp_id,
                                emp_name_snapshot,
                                item_type,
                                item_name,
                                amount,
                                target_field,
                                direction,
                                source_column,
                                source_sheet,
                                source_file,
                                import_batch_id,
                                remarks,
                                is_active
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                            """,
                            (
                                item_id,
                                item_month,
                                emp_id,
                                emp_name_snapshot,
                                item_type,
                                item_name,
                                amount,
                                target_field,
                                direction,
                                source_col,
                                source_sheet_name,
                                uploaded_items_file.name,
                                import_batch_id,
                                row_remarks
                            )
                        )

                        success_count += 1

                conn.commit()

                st.success(
                    f"✅ 导入完成！成功写入 {success_count} 条项目流水。"
                    f"跳过空金额 {skipped_zero_count} 个，跳过无工号行 {skipped_no_emp_count} 行。"
                )

                st.info(f"本次导入批次号：{import_batch_id}")

            except Exception as e:
                conn.rollback()
                st.error(f"导入失败：{e}")

    # ==========================================================
    # 六、查看本月项目流水
    # ==========================================================

    st.write("### 3️⃣ 本月项目流水查看")

    try:
        items_df = pd.read_sql_query(
            """
            SELECT
                cost_month,
                emp_id,
                emp_name_snapshot,
                item_type,
                item_name,
                amount,
                target_field,
                direction,
                source_column,
                remarks,
                import_batch_id
            FROM payroll_monthly_items
            WHERE cost_month = ?
              AND is_active = 1
            ORDER BY emp_id ASC, target_field ASC, item_type ASC
            """,
            conn,
            params=[item_month]
        )
    except Exception as e:
        st.error(f"读取项目流水失败：{e}")
        items_df = pd.DataFrame()

    if items_df.empty:
        st.caption("当前月份暂无项目流水。")
    else:
        show_items = items_df.rename(columns={
            "cost_month": "月份",
            "emp_id": "工号",
            "emp_name_snapshot": "姓名",
            "item_type": "项目类别",
            "item_name": "项目名称",
            "amount": "金额",
            "target_field": "汇总字段",
            "direction": "方向",
            "source_column": "来源列",
            "remarks": "备注",
            "import_batch_id": "导入批次"
        })

        st.dataframe(show_items, use_container_width=True, hide_index=True)

        # 汇总展示：按项目类别汇总。
        st.write("#### 📊 按项目类别汇总")
        summary_by_type = (
            show_items
            .groupby(["项目类别", "汇总字段"], as_index=False)["金额"]
            .sum()
            .sort_values(["汇总字段", "项目类别"])
        )

        st.dataframe(summary_by_type, use_container_width=True, hide_index=True)

    # ==========================================================
    # 七、汇总推送到薪酬主账
    # ==========================================================

    st.write("### 4️⃣ 汇总推送到薪酬主账")

    st.warning(
        "⚠️ 推送会用项目池的汇总结果覆盖薪酬主账中的相关字段。"
        "例如 position_adj、special_bonus_total、history_clearance 等。"
        "如果你在第三步手工改过这些字段，推送后可能会被项目池结果覆盖。"
    )

    if st.button("🔄 将本月项目池汇总推送到薪酬主账", type="primary"):

        try:
            cursor = conn.cursor()

            # ------------------------------------------------------
            # 1. 检查本月薪酬主账是否已经生成
            # ------------------------------------------------------
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM payroll_monthly_records
                WHERE cost_month = ?
                """,
                (item_month,)
            )

            main_count = cursor.fetchone()[0]

            if main_count == 0:
                st.error(
                    "推送失败：本月薪酬主账还没有生成。"
                    "请先到 Tab2 点击【抓取固定底薪与社保代扣】生成底表。"
                )
                st.stop()

            # ------------------------------------------------------
            # 2. 允许被项目池汇总覆盖的字段白名单
            # ------------------------------------------------------
            allowed_target_fields = [
                "position_adj",
                "expert_allowance",
                "special_bonus_total",
                "history_clearance",
                "promotion_backpay",
                "perf_adj"
            ]

            # ------------------------------------------------------
            # 3. 先把这些字段清零
            # ------------------------------------------------------
            # 为什么要清零？
            # ------------------------------------------------------
            # 假设你第一次导入专项奖 1000，推送后主账是 1000。
            # 第二次你删除了这条项目并重新导入，如果不清零，旧的 1000 可能残留。
            for field in allowed_target_fields:
                cursor.execute(
                    f"""
                    UPDATE payroll_monthly_records
                    SET {field} = 0.0
                    WHERE cost_month = ?
                    """,
                    (item_month,)
                )

            # ------------------------------------------------------
            # 4. 按 工号 + 目标字段 汇总项目池
            # ------------------------------------------------------
            sum_df = pd.read_sql_query(
                """
                SELECT
                    emp_id,
                    target_field,
                    SUM(amount) AS total_amount
                FROM payroll_monthly_items
                WHERE cost_month = ?
                  AND is_active = 1
                GROUP BY emp_id, target_field
                """,
                conn,
                params=[item_month]
            )

            update_count = 0
            skipped_field_count = 0

            for _, row in sum_df.iterrows():

                emp_id = clean_emp_id(row["emp_id"])
                target_field = str(row["target_field"]).strip()
                total_amount = safe_money_to_float(row["total_amount"])

                # 防止映射表里写了奇怪字段，避免 SQL 注入或误改其他字段。
                if target_field not in allowed_target_fields:
                    skipped_field_count += 1
                    continue

                cursor.execute(
                    f"""
                    UPDATE payroll_monthly_records
                    SET {target_field} = ?
                    WHERE cost_month = ?
                      AND emp_id = ?
                    """,
                    (total_amount, item_month, emp_id)
                )

                update_count += 1

            conn.commit()

            st.success(
                f"✅ 推送完成！更新 {update_count} 条员工字段汇总。"
                f"跳过不在白名单内的字段 {skipped_field_count} 条。"
            )

            st.info(
                "下一步请到 Tab3 重新执行【薪酬草稿应发/实发】计算。"
                "项目池推送只负责把岗位补/扣、专项奖惩、清算、专家调整等写进薪酬主账；"
                "应发和实发仍需要在 Tab3 里重新计算。"
            )

        except Exception as e:
            conn.rollback()
            st.error(f"推送失败：{e}")

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

                # 这里不能直接写 str(row['post_rank'])。
                # 原因：
                # 人员模块里的 post_rank 可能是 21.5、21.8、21.99。
                # 这些小数点在人员模块里是排序用的，不能删。
                # 但是薪酬矩阵里只有 21、22、23 这种整数岗级。
                #
                # 所以这里调用 normalize_rank_for_payroll()：
                # 21.5  -> "21"
                # 21.8  -> "21"
                # 21.99 -> "21"
                # 21.0  -> "21"
                rank = normalize_rank_for_payroll(row['post_rank'])

                # 档次仍然直接转成字符串。
                # 例如 A、B、C、D、E、F、G、H、I、J。
                grade = str(row['post_grade']).strip()

                # T级也直接转成字符串。
                # 例如 T1、T2、T3。
                t_grade = str(row['tech_grade']).strip()

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
        # ==========================================================
        # 绩效计算状态提醒
        # ==========================================================
        # 为什么要加这个提醒？
        # ----------------------------------------------------------
        # Tab2 里“生成底表”和“计算绩效”是两个动作。
        # 只生成底表时，系统只会写入岗位工资、绩效基数、激励包基数、社保扣款；
        # 但真正的绩效工资 perf_salary_calc 要点击下面的“计算理论绩效总额并入库”才会生成。
        #
        # 如果忘记点这个按钮，Tab3 里就会出现“有岗位工资，但没有绩效工资”的情况。
        # 所以这里提前给出状态提示，避免以后每个月漏操作。
        total_perf_rows = len(df_perf)
        zero_perf_rows = (df_perf["已算出的绩效"].fillna(0) == 0).sum()
        nonzero_perf_rows = total_perf_rows - zero_perf_rows

        c_status_1, c_status_2, c_status_3 = st.columns(3)
        c_status_1.metric("本月薪酬底表人数", f"{total_perf_rows} 人")
        c_status_2.metric("已计算绩效人数", f"{nonzero_perf_rows} 人")
        c_status_3.metric("绩效仍为0人数", f"{zero_perf_rows} 人")

        if zero_perf_rows > 0:
            st.warning(
                "⚠️ 当前仍有人员的【已算出的绩效】为 0。"
                "如果你还没有点击下方【计算理论绩效总额并入库】，请先点击。"
                "如果已经点击过，则需要检查这些人的绩效基数、激励包基数或岗位规则是否缺失。"
            )
        else:
            st.success("✅ 当前底表中的绩效工资均已计算。")

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
    st.subheader("🏦 财务个税回灌与薪酬草稿结账")

    # ==========================================================
    # 操作完成后的提示区
    # ==========================================================
    # 为什么要用 st.session_state？
    # ----------------------------------------------------------
    # 因为 Streamlit 点击按钮后，经常会重新运行整个页面。
    # 如果只是 st.success("完成")，提示可能一闪而过。
    #
    # session_state 可以把“刚刚完成了什么操作”暂存起来，
    # 页面刷新后还能继续显示提示。
    if "payroll_tab3_message" in st.session_state:
        st.success(st.session_state["payroll_tab3_message"])

        st.info(
            "下一步流程："
            "① 下载【发给财务算税底表】 → "
            "② 发给财务计算个税 → "
            "③ 导入财务回传个税 → "
            "④ 再次重新计算应发/实发 → "
            "⑤ 后续生成 OA 上传表。"
        )

        # 显示完以后删除，避免这个提示永远挂在页面上。
        del st.session_state["payroll_tab3_message"]

    st.info(
        "💡 这里不是正式封账，只是把岗位工资、岗位补/扣、专家调整、绩效、绩效补/扣、专项奖惩、清算、个税等合并，"
        "生成一版可用于核对、送财务算税、后续导出 OA 的薪酬草稿。"
    )

    # ==========================================================
    # 一、选择结账月份
    # ==========================================================

    # 这里单独设置一个月份输入框。
    # 原因：
    # Tab2 的 calc_month 虽然在同一个脚本里能读到，
    # 但 Tab3 自己放一个月份输入框，用户操作更直观，也更不容易误用。
    final_month = st.text_input(
        "📅 结账月份",
        value=calc_month,
        help="这里要和 Tab2 生成底表的月份一致，例如 2026-04。"
    )

    conn = _get_db_connection()

    # ==========================================================
    # 二、读取薪酬主账草稿
    # ==========================================================

    # 这段 SQL 负责把 payroll_monthly_records 里的关键工资字段取出来。
    #
    # 注意：
    # 1. 这里比上一版多取了五险两金的明细字段。
    # 2. 原因是：你给财务算税时，不能只给“五险两金合计”，最好把养老、医疗、失业、公积金、年金都拆出来。
    # 3. 个税是财务算，所以系统只负责导出底表、导入财务回传结果。
    sql_final = """
        SELECT
            p.emp_id                                                   AS "工号",
            e.name                                                     AS "姓名",
            p.dept_name                                                AS "部门",

            IFNULL(p.base_salary, 0)                                   AS "岗位工资",
            IFNULL(p.seniority_pay, 0)                                 AS "工龄工资",
            IFNULL(p.comp_subsidy, 0)                                  AS "综合补贴",
            IFNULL(p.telecom_subsidy, 0)                               AS "通讯补贴",
            IFNULL(p.position_adj, 0)                                  AS "岗位补/扣",
            IFNULL(p.expert_allowance, 0)                              AS "专家/特殊津贴",

            IFNULL(p.perf_salary_calc, 0)                              AS "已算绩效",
            IFNULL(p.perf_adj, 0)                                      AS "绩效补/扣",

            IFNULL(p.promotion_backpay, 0)                             AS "晋升补发",
            IFNULL(p.special_bonus_total, 0)                           AS "专项奖惩及提成合计",
            IFNULL(p.history_clearance, 0)                             AS "历史清算",

            IFNULL(p.ss_pension_pers, 0)                               AS "养老个人",
            IFNULL(p.ss_medical_mix, 0)                                AS "医疗个人含大病",
            IFNULL(p.ss_unemp_pers, 0)                                 AS "失业个人",
            IFNULL(p.ss_fund_pers, 0)                                  AS "公积金个人",
            IFNULL(p.ss_annuity_pers, 0)                               AS "年金个人",

            (IFNULL(p.ss_pension_pers, 0)
             + IFNULL(p.ss_medical_mix, 0)
             + IFNULL(p.ss_unemp_pers, 0)
             + IFNULL(p.ss_fund_pers, 0)
             + IFNULL(p.ss_annuity_pers, 0))                           AS "五险两金代扣",

            IFNULL(p.tax_deduction, 0)                                 AS "代扣个税",
            IFNULL(p.gross_salary_total, 0)                            AS "系统算_应发总计",
            IFNULL(p.net_salary, 0)                                    AS "系统算_最终实发"

        FROM payroll_monthly_records p
                 JOIN employees e ON p.emp_id = e.emp_id
        WHERE p.cost_month = ?
        ORDER BY p.dept_name ASC, p.emp_id ASC
    """

    df_final = pd.read_sql_query(sql_final, conn, params=[final_month])

    if df_final.empty:
        st.warning("⚠️ 当前月份没有薪酬主账。请先到 Tab2 生成底表。")
        conn.close()
    else:
        # ==========================================================
        # 三、内部计算函数
        # ==========================================================

        def calc_gross_from_row(row):
            """
            计算应发工资。

            这个函数是干什么的？
            ------------------------------------------------------------
            它把一行工资草稿里的各项收入/补扣相加，得到应发工资。

            为什么单独写成函数？
            ------------------------------------------------------------
            因为下面有三个地方都要用到这个逻辑：

            1. 页面展示时，给财务导出应发测算；
            2. 点击“重新计算薪酬草稿应发/实发”时；
            3. 导入财务个税后，重新计算实发时。

            单独写函数，可以避免三个地方公式不一致。
            """

            gross = (
                float(row["岗位工资"] or 0)
                + float(row["工龄工资"] or 0)
                + float(row["综合补贴"] or 0)
                + float(row["通讯补贴"] or 0)
                + float(row["岗位补/扣"] or 0)
                + float(row["专家/特殊津贴"] or 0)
                + float(row["已算绩效"] or 0)
                + float(row["绩效补/扣"] or 0)
                + float(row["晋升补发"] or 0)
                + float(row["专项奖惩及提成合计"] or 0)
                + float(row["历史清算"] or 0)
            )

            return round(gross, 2)


        def calc_social_total_from_row(row):
            """
            计算个人五险两金代扣合计。

            这里拆开养老、医疗、失业、公积金、年金后再相加。
            好处是：
            ------------------------------------------------------------
            如果以后你发现合计不对，可以直接看是哪一项错了。
            """

            total = (
                float(row["养老个人"] or 0)
                + float(row["医疗个人含大病"] or 0)
                + float(row["失业个人"] or 0)
                + float(row["公积金个人"] or 0)
                + float(row["年金个人"] or 0)
            )

            return round(total, 2)


        # ==========================================================
        # 四、页面指标预览
        # ==========================================================

        # 这里先复制一份，不直接改 df_final。
        preview_df = df_final.copy()

        # 计算当前页面上的理论应发和理论实发。
        # 注意：
        # 这里的“理论”只是用于页面预览；
        # 真正写入数据库，要点击后面的按钮。
        preview_df["页面测算_应发"] = preview_df.apply(calc_gross_from_row, axis=1)
        preview_df["页面测算_五险两金"] = preview_df.apply(calc_social_total_from_row, axis=1)
        preview_df["页面测算_实发"] = (
            preview_df["页面测算_应发"]
            - preview_df["页面测算_五险两金"]
            - preview_df["代扣个税"]
        ).round(2)

        metric_1, metric_2, metric_3, metric_4 = st.columns(4)
        metric_1.metric("草稿人数", f"{len(preview_df)} 人")
        metric_2.metric("页面测算应发合计", f"{preview_df['页面测算_应发'].sum():,.2f}")
        metric_3.metric("个人五险两金合计", f"{preview_df['页面测算_五险两金'].sum():,.2f}")
        metric_4.metric("页面测算实发合计", f"{preview_df['页面测算_实发'].sum():,.2f}")

        # ==========================================================
        # 五、导出给财务算税底表
        # ==========================================================

        st.write("### 1️⃣ 导出给财务算税底表")

        st.caption(
            "这张表用于发给财务算个税。它包含应发工资、个人社保公积金明细、五险两金合计。"
            "财务算完后，再把个税结果回传导入。"
        )

        finance_export_df = preview_df[[
            "工号",
            "姓名",
            "部门",

            "岗位工资",
            "工龄工资",
            "综合补贴",
            "通讯补贴",
            "岗位补/扣",
            "专家/特殊津贴",
            "已算绩效",
            "绩效补/扣",
            "晋升补发",
            "专项奖惩及提成合计",
            "历史清算",

            "页面测算_应发",

            "养老个人",
            "医疗个人含大病",
            "失业个人",
            "公积金个人",
            "年金个人",
            "页面测算_五险两金",

            "代扣个税",
            "页面测算_实发"
        ]].copy()

        # 改成更适合财务看的列名。
        finance_export_df.rename(columns={
            "页面测算_应发": "应发工资合计",
            "页面测算_五险两金": "五险两金个人合计",
            "页面测算_实发": "实发工资测算"
        }, inplace=True)

        tax_out = io.BytesIO()

        with pd.ExcelWriter(tax_out, engine="openpyxl") as writer:
            finance_export_df.to_excel(writer, index=False, sheet_name="发给财务算税底表")

            # 给财务回传模板也顺手放一个 sheet。
            # 财务只需要填工号、姓名、代扣个税即可。
            tax_template_df = finance_export_df[["工号", "姓名", "应发工资合计", "五险两金个人合计"]].copy()
            tax_template_df["代扣个税"] = 0.0
            tax_template_df["备注"] = ""

            tax_template_df.to_excel(writer, index=False, sheet_name="财务回传个税模板")

        st.download_button(
            label="📤 下载发给财务算税底表",
            data=tax_out.getvalue(),
            file_name=f"{final_month}_发给财务算税底表.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # ==========================================================
        # 六、导入财务回传个税
        # ==========================================================

        st.write("### 2️⃣ 导入财务回传个税")

        st.caption(
            "第一版只要求财务回传表里有【工号】和【代扣个税】两列。"
            "如果财务列名叫【个税】或【本月个税】，系统也会尝试识别。"
        )

        tax_file = st.file_uploader(
            "上传财务回传个税 Excel 或 CSV",
            type=["xlsx", "csv"],
            key="tax_import_upload"
        )

        if tax_file is not None:
            if st.button("📥 导入财务个税并重算实发", type="primary"):

                try:
                    # --------------------------------------------------
                    # 1. 读取财务回传文件
                    # --------------------------------------------------
                    if tax_file.name.endswith(".csv"):
                        tax_df = pd.read_csv(tax_file)
                    else:
                        tax_df = pd.read_excel(tax_file)

                    if tax_df.empty:
                        st.warning("上传的个税表是空的。")
                        st.stop()

                    if "工号" not in tax_df.columns:
                        st.error("导入失败：个税表必须有【工号】这一列。")
                        st.stop()

                    # --------------------------------------------------
                    # 2. 自动识别个税列
                    # --------------------------------------------------
                    possible_tax_cols = ["代扣个税", "个税", "本月个税", "应扣个税", "个人所得税"]

                    tax_col = None
                    for c in possible_tax_cols:
                        if c in tax_df.columns:
                            tax_col = c
                            break

                    if tax_col is None:
                        st.error(
                            "导入失败：没有找到个税列。"
                            "请确认表里有【代扣个税】、【个税】、【本月个税】、【应扣个税】或【个人所得税】其中之一。"
                        )
                        st.stop()

                    cursor = conn.cursor()

                    import_count = 0
                    skipped_no_emp = 0
                    skipped_not_in_main = 0

                    # --------------------------------------------------
                    # 3. 逐行导入个税
                    # --------------------------------------------------
                    for _, row in tax_df.iterrows():

                        emp_id = clean_emp_id(row.get("工号"))

                        if not emp_id:
                            skipped_no_emp += 1
                            continue

                        tax_amount = safe_money_to_float(row.get(tax_col))

                        # 检查这个人本月是否有薪酬主账。
                        cursor.execute(
                            """
                            SELECT
                                IFNULL(gross_salary_total, 0),
                                IFNULL(ss_pension_pers, 0),
                                IFNULL(ss_medical_mix, 0),
                                IFNULL(ss_unemp_pers, 0),
                                IFNULL(ss_fund_pers, 0),
                                IFNULL(ss_annuity_pers, 0)
                            FROM payroll_monthly_records
                            WHERE cost_month = ?
                              AND emp_id = ?
                            """,
                            (final_month, emp_id)
                        )

                        found = cursor.fetchone()

                        if found is None:
                            skipped_not_in_main += 1
                            continue

                        gross_salary = safe_money_to_float(found[0])
                        social_total = (
                            safe_money_to_float(found[1])
                            + safe_money_to_float(found[2])
                            + safe_money_to_float(found[3])
                            + safe_money_to_float(found[4])
                            + safe_money_to_float(found[5])
                        )

                        # 如果还没点击过“重新计算薪酬草稿应发/实发”，
                        # gross_salary_total 可能还是 0。
                        # 这里仍然照实计算，但后面会给提示。
                        net_salary = round(gross_salary - social_total - tax_amount, 2)

                        cursor.execute(
                            """
                            UPDATE payroll_monthly_records
                            SET tax_deduction = ?,
                                net_salary = ?
                            WHERE cost_month = ?
                              AND emp_id = ?
                            """,
                            (tax_amount, net_salary, final_month, emp_id)
                        )

                        import_count += 1

                    conn.commit()

                    st.success(
                        f"✅ 个税导入完成！成功导入 {import_count} 人。"
                        f"跳过无工号行 {skipped_no_emp} 行，"
                        f"跳过本月无薪酬主账人员 {skipped_not_in_main} 人。"
                    )

                    st.warning(
                        "提醒：如果你导入个税前还没有点击【重新计算薪酬草稿应发/实发】，"
                        "部分人员的应发可能仍为 0，实发也会不准。"
                        "建议先点一次下方草稿结账按钮，再导入个税；导入个税后如有手工调整，再重新结账一次。"
                    )

                    st.rerun()

                except Exception as e:
                    conn.rollback()
                    st.error(f"导入个税失败：{e}")

        # ==========================================================
        # 七、页面编辑区
        # ==========================================================

        st.write("### 3️⃣ 手工调整与草稿结账")

        st.caption(
            "这里可以人工改：岗位补/扣、专家/特殊津贴、绩效补/扣、晋升补发、专项奖惩及提成、历史清算、个税。"
            "底薪、已算绩效、五险两金代扣暂时锁定，避免误改。"
        )

        edited_final = st.data_editor(
            df_final,
            column_config={
                "岗位补/扣": st.column_config.NumberColumn(format="%.2f"),
                "专家/特殊津贴": st.column_config.NumberColumn(format="%.2f"),
                "绩效补/扣": st.column_config.NumberColumn(format="%.2f"),
                "晋升补发": st.column_config.NumberColumn(format="%.2f"),
                "专项奖惩及提成合计": st.column_config.NumberColumn(format="%.2f"),
                "历史清算": st.column_config.NumberColumn(format="%.2f"),
                "代扣个税": st.column_config.NumberColumn(format="%.2f"),
            },
            disabled=[
                "工号",
                "姓名",
                "部门",
                "岗位工资",
                "工龄工资",
                "综合补贴",
                "通讯补贴",
                "已算绩效",
                "养老个人",
                "医疗个人含大病",
                "失业个人",
                "公积金个人",
                "年金个人",
                "五险两金代扣",
                "系统算_应发总计",
                "系统算_最终实发"
            ],
            use_container_width=True,
            hide_index=True
        )

        # ==========================================================
        # 八、执行草稿结账
        # ==========================================================

        if st.button("🧮 重新计算薪酬草稿应发/实发", type="primary"):
            cursor = conn.cursor()

            update_count = 0

            for _, row in edited_final.iterrows():

                # 计算应发工资。
                gross = calc_gross_from_row(row)

                # 计算个人社保公积金代扣合计。
                social_total = calc_social_total_from_row(row)

                # 计算实发工资。
                net = (
                    gross
                    - social_total
                    - float(row["代扣个税"] or 0)
                )

                # 写回薪酬主账。
                cursor.execute(
                    """
                    UPDATE payroll_monthly_records
                    SET position_adj        = ?,
                        expert_allowance    = ?,
                        perf_adj            = ?,
                        promotion_backpay   = ?,
                        special_bonus_total = ?,
                        history_clearance   = ?,
                        tax_deduction       = ?,
                        gross_salary_total  = ?,
                        net_salary          = ?
                    WHERE cost_month = ?
                      AND emp_id = ?
                    """,
                    (
                        float(row["岗位补/扣"] or 0),
                        float(row["专家/特殊津贴"] or 0),
                        float(row["绩效补/扣"] or 0),
                        float(row["晋升补发"] or 0),
                        float(row["专项奖惩及提成合计"] or 0),
                        float(row["历史清算"] or 0),
                        float(row["代扣个税"] or 0),
                        round(gross, 2),
                        round(net, 2),
                        final_month,
                        row["工号"]
                    )
                )

                update_count += 1

            conn.commit()

            st.success(f"✅ 草稿结账完成！已重新计算 {update_count} 人的应发与实发。")
            st.rerun()

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