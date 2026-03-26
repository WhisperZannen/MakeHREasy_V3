# ==============================================================================
# 文件路径: modules/core_position.py
# 功能描述: 岗位字典的底层数据接口 (CRUD 操作)
# 实现了什么具体逻辑:
#   1. 提供岗位名称、序列、排序权重、状态的完整管理。
#   2. 为前端人员录入提供“岗位下拉列表”的数据支撑。
# ==============================================================================

import sqlite3
import os

def _get_db_connection():
    # 实现了什么具体逻辑：统一定位数据库物理位置。
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    db_path = os.path.join(project_root, 'database', 'hr_core.db')
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn

def add_position(pos_name, pos_category, sort_order=999):
    # 为什么这么改：封装岗位入库逻辑。
    # 实现了什么具体逻辑：参数化写入 positions 表。
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO positions (pos_name, pos_category, sort_order)
            VALUES (?, ?, ?)
        """, (pos_name, pos_category, sort_order))
        conn.commit()
        return True, f"成功新增岗位: {pos_name}"
    except sqlite3.IntegrityError:
        return False, f"新增失败：岗位名称 '{pos_name}' 已存在！"
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def get_all_positions(include_inactive=False):
    # 为什么这么改：提供给前端下拉选择。
    # 实现了什么具体逻辑：根据 include_inactive 决定是否拉取已停用的岗位，并强制按 sort_order 排序。
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        status_sql = "" if include_inactive else "WHERE status = 1"
        cursor.execute(f"SELECT * FROM positions {status_sql} ORDER BY sort_order ASC, pos_id ASC")
        rows = cursor.fetchall()
        return True, [dict(row) for row in rows]
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def update_position(pos_id, new_name, new_category, new_sort_order=999, new_status=1):
    # 为什么这么改：满足你提出的“修改状态（恢复正常）”需求。
    # 实现了什么具体逻辑：支持全字段覆写，包括 status。
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE positions 
            SET pos_name = ?, pos_category = ?, sort_order = ?, status = ?
            WHERE pos_id = ?
        """, (new_name, new_category, new_sort_order, new_status, pos_id))
        if cursor.rowcount == 0:
            return False, "修改失败：找不到该岗位记录。"
        conn.commit()
        return True, f"成功更新岗位: {new_name}"
    except sqlite3.IntegrityError:
        return False, f"修改失败：岗位名称 '{new_name}' 已被占用！"
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()