# ==============================================================================
# 文件路径: modules/core_payroll.py
# 功能描述: 薪酬核算模块核心算力引擎 (Model 层)
# ==============================================================================

import sqlite3
import os
import json
import pandas as pd
from datetime import datetime


# ------------------------------------------------------------------------------
# 数据库与字典连接池
# ------------------------------------------------------------------------------
def _get_db_connection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    db_path = os.path.join(project_root, 'database', 'hr_core.db')
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn


def get_payroll_dictionaries():
    """读取保存在根目录的轻量级薪酬算力字典"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dict_path = os.path.join(os.path.dirname(current_dir), 'payroll_dicts.json')
    if os.path.exists(dict_path):
        with open(dict_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"post_salary_map": {}, "t_level_map": {}, "expert_allowance": {}}


# ------------------------------------------------------------------------------
# 【核心战役预埋】15 号生死线薪资底表生成器
# ------------------------------------------------------------------------------
def generate_payroll_base(target_month: str):
    """
    终极使命：
    1. 抓取本月在职人员（过滤历史快照，执行 15 号切片）。
    2. 根据提取的岗级、T级，去 payroll_dicts 翻译出本月应拿的基础钱数。
    3. 去 ss_monthly_records 倒吸本月的社保代扣数据（合并199和7）。
    """
    conn = _get_db_connection()
    dicts = get_payroll_dictionaries()

    try:
        # [战斗区] 下一步我们将在这里写入最复杂的 SQL 和 Pandas 合并逻辑
        # 提取历史轨迹 -> 判定日期 -> 锁定岗位/档次 -> 映射金钱 -> 拼装底表
        pass

    except Exception as e:
        print(f"底层算力崩溃: {e}")
    finally:
        conn.close()