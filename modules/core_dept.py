# ==============================================================================
# 文件路径: modules/core_dept.py
# 功能描述: 部门管理的底层数据接口 (支持状态恢复与部分修改版)
# 实现了什么具体逻辑:
#   1. 在修改部门时支持状态位 status 的直接调整。
#   2. 优化了 SQL 事务处理，确保 parent_dept_id 关联安全性。
# ==============================================================================

import sqlite3
import os

def _get_db_connection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    db_path = os.path.join(project_root, 'database', 'hr_core.db')
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn

def add_department(dept_name, dept_category, parent_dept_id=None, sort_order=999):
    # 实现了什么具体逻辑：参数化插入，防止 SQL 注入。
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO departments (dept_name, parent_dept_id, dept_category, sort_order)
            VALUES (?, ?, ?, ?)
        """, (dept_name, parent_dept_id, dept_category, sort_order))
        conn.commit()
        return True, f"成功新增部门: {dept_name}"
    except sqlite3.IntegrityError as e:
        if "UNIQUE" in str(e): return False, f"新增失败：部门名称 '{dept_name}' 已存在！"
        elif "FOREIGN KEY" in str(e): return False, f"新增失败：上级部门(ID:{parent_dept_id})不存在！"
        return False, str(e)
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def get_all_departments(include_inactive=False):
    # 实现了什么具体逻辑：强制按照政治位序 sort_order 排列。
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        status_sql = "" if include_inactive else "WHERE status = 1"
        cursor.execute(f"SELECT * FROM departments {status_sql} ORDER BY sort_order ASC, dept_id ASC")
        rows = cursor.fetchall()
        return True, [dict(row) for row in rows]
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def update_department(dept_id, new_name, new_category, new_parent_id=None, new_sort_order=999, new_status=1):
    # [增量详尽注释 2026-03-26]
    # 为什么这么改：响应你的需求，修改部门时支持把“已撤销”状态改回“正常(1)”。
    # 实现了什么具体逻辑：增加了 new_status 参数，并拦截“上级是自己”的逻辑谬误。
    if str(dept_id) == str(new_parent_id):
        return False, "修改失败：部门的上级绝对不能是它自己！"

    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE departments 
            SET dept_name = ?, dept_category = ?, parent_dept_id = ?, sort_order = ?, status = ?
            WHERE dept_id = ?
        """, (new_name, new_category, new_parent_id, new_sort_order, new_status, dept_id))

        if cursor.rowcount == 0: return False, f"修改失败：找不到 ID 为 {dept_id} 的部门。"
        conn.commit()
        return True, f"部门 '{new_name}' 信息已更新。"
    except sqlite3.IntegrityError as e:
        conn.rollback()
        if "UNIQUE" in str(e): return False, f"修改失败：部门名被占用！"
        elif "FOREIGN KEY" in str(e): return False, f"修改失败：上级部门不存在！"
        return False, str(e)
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def soft_delete_department(dept_id):
    # 实现了什么具体逻辑：物理保留，逻辑撤销。
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE departments SET status = 0 WHERE dept_id = ?", (dept_id,))
        if cursor.rowcount == 0: return False, "撤销失败：部门不存在。"
        conn.commit()
        return True, f"部门已成功标记为撤销。"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()