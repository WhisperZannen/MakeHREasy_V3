# ==============================================================================
# 文件路径: modules/core_payroll.py
# 功能描述: 薪酬核算模块核心算力引擎 (Model 层)
# 核心逻辑:
#   1. 管理薪酬数据的增删改查。
#   2. 承载 15 号生死线的薪资基座回溯算法。
#   3. 解析与合并 JSON 动态薪酬背包。
# ==============================================================================

import sqlite3 # 导入 sqlite3 用于操作本地数据库
import os # 导入 os 用于处理文件路径
import pandas as pd # 导入 pandas 用于处理内存中的表格数据
import json # 导入 json 用于打包和解包动态薪酬列

# ------------------------------------------------------------------------------
# 数据库连接池初始化
# ------------------------------------------------------------------------------
def _get_db_connection():
    # 获取当前文件所在目录的绝对路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 获取项目根目录
    project_root = os.path.dirname(current_dir)
    # 拼接出数据库文件的绝对路径
    db_path = os.path.join(project_root, 'database', 'hr_core.db')
    # 连接数据库
    conn = sqlite3.connect(db_path)
    # 开启外键约束开关，保证数据联动安全
    conn.execute("PRAGMA foreign_keys = ON;")
    # 设置行工厂，使查询结果可以通过列名访问 (类似字典)
    conn.row_factory = sqlite3.Row
    return conn

# ------------------------------------------------------------------------------
# 数据库表结构静默安检程序 (防止未运行 init_db 导致的崩溃)
# ------------------------------------------------------------------------------
def _ensure_payroll_schema():
    # 获取数据库连接
    conn = _get_db_connection()
    # 获取游标对象
    cursor = conn.cursor()
    try:
        # 如果 payroll_monthly_records 不存在，这里执行一条无害的探针查询
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='payroll_monthly_records'")
        # 抓取查询结果
        table_exists = cursor.fetchone()

        # 如果表不存在，说明用户可能忘记跑 init_db.py
        if not table_exists:
            print("🚨 警告：检测到薪酬底层表缺失，请确保已更新 init_db.py 并重新初始化数据库！")
    except Exception as e:
        # 捕捉并打印任何底层探针异常
        print(f"薪酬底层安检异常: {e}")
    finally:
        # 无论如何，最终安全关闭数据库连接
        conn.close()

# 在模块被加载时，强制执行一次静默安检
_ensure_payroll_schema()

# 后续我们将在这里追加: generate_payroll_base (一键备料函数), calc_perf_salary (绩效计算函数) 等。