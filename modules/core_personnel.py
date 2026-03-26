# ==============================================================================
# 文件路径: modules/core_personnel.py
# 功能描述: 人员管理数据接口 (V3.15 生命周期全量捕获版)
# 实现了什么具体逻辑:
#   1. [逻辑升级] 将 status (状态) 正式纳入 Delta 差异比对。
#   2. 强化了快照生成逻辑：离职、退休、在职切换将产生与调岗调薪同等地位的流水记录。
#   3. 精准计时：继续保持 intern_start_date 的 SQL 穿透查询。
# ==============================================================================

import sqlite3
import os
from datetime import datetime

def _get_db_connection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    db_path = os.path.join(project_root, 'database', 'hr_core.db')
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn

def add_employee(emp_data, profile_data, reason="新员工入职", change_date=None):
    actual_date = change_date if change_date else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO employees (emp_id, name, id_card, dept_id, post_rank, post_grade, status, join_company_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (emp_data['emp_id'], emp_data['name'], emp_data['id_card'], emp_data['dept_id'], emp_data['post_rank'], emp_data['post_grade'], emp_data.get('status', '在职'), emp_data.get('join_company_date')))

        cursor.execute("""
            INSERT INTO employee_profiles (emp_id, pos_id, tech_grade, title_order, education_level, degree, school_name, major, graduation_date, first_job_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (emp_data['emp_id'], profile_data.get('pos_id'), profile_data.get('tech_grade'), profile_data.get('title_order', 999), profile_data.get('education_level'), profile_data.get('degree'), profile_data.get('school_name'), profile_data.get('major'), profile_data.get('graduation_date'), profile_data.get('first_job_date')))

        cursor.execute("""
            INSERT INTO personnel_changes (emp_id, change_type, new_dept_id, new_pos_id, new_tech_grade, new_post_rank, new_post_grade, change_date, change_reason) 
            VALUES (?, '入职', ?, ?, ?, ?, ?, ?, ?)
        """, (emp_data['emp_id'], emp_data['dept_id'], profile_data.get('pos_id'), profile_data.get('tech_grade'), emp_data['post_rank'], emp_data['post_grade'], actual_date, reason))

        conn.commit()
        return True, f"成功为 {emp_data['name']} 建立档案。"
    except Exception as e:
        conn.rollback(); return False, str(e)
    finally:
        conn.close()

def update_employee(emp_id, emp_data, profile_data, reason="档案更新", change_date=None):
    # [增量详尽注释 2026-03-26]
    # 为什么这么改：响应用户关于“离职/退休也算变动”的需求。
    # 实现了什么具体逻辑：在 Delta 比对中加入了 status 字段。
    # 只要 状态、部门、岗位、T级、岗级、档次 任一发生变化，均触发快照。
    actual_date = change_date if change_date else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT e.dept_id, e.post_rank, e.post_grade, e.status, p.pos_id, p.tech_grade, pos.pos_name as old_pos_name
            FROM employees e
            LEFT JOIN employee_profiles p ON e.emp_id = p.emp_id
            LEFT JOIN positions pos ON p.pos_id = pos.pos_id
            WHERE e.emp_id = ?
        """, (emp_id,))
        old = cursor.fetchone()

        trigger_snapshot = False
        change_tags = []
        if old:
            def is_diff(v1, v2): return str(v1).strip() != str(v2).strip()

            # [核心新增] 状态变动捕获
            if is_diff(old['status'], emp_data.get('status')):
                change_tags.append(f"变为{emp_data.get('status')}")

            if is_diff(old['dept_id'], emp_data['dept_id']): change_tags.append("跨部门调动")

            if is_diff(old['pos_id'], profile_data.get('pos_id')):
                if str(old['old_pos_name']) == '实习岗': change_tags.append("实习转正")
                else: change_tags.append("岗位调整")

            if is_diff(old['tech_grade'], profile_data.get('tech_grade')): change_tags.append("T级变动")
            if is_diff(old['post_rank'], emp_data['post_rank']): change_tags.append("岗级调整")
            if is_diff(old['post_grade'], emp_data['post_grade']): change_tags.append("档次调整")

            if change_tags:
                trigger_snapshot = True
                cursor.execute("""
                    INSERT INTO personnel_changes 
                    (emp_id, change_type, old_dept_id, new_dept_id, old_pos_id, new_pos_id, old_tech_grade, new_tech_grade, old_post_rank, new_post_rank, old_post_grade, new_post_grade, change_date, change_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (emp_id, " + ".join(change_tags), old['dept_id'], emp_data['dept_id'], old['pos_id'], profile_data.get('pos_id'), old['tech_grade'], profile_data.get('tech_grade'), old['post_rank'], emp_data['post_rank'], old['post_grade'], emp_data['post_grade'], actual_date, reason))

        # 物理更新，包含状态位
        cursor.execute("""
            UPDATE employees 
            SET name=?, id_card=?, dept_id=?, post_rank=?, post_grade=?, status=?, join_company_date=? 
            WHERE emp_id=?
        """, (emp_data['name'], emp_data['id_card'], emp_data['dept_id'], emp_data['post_rank'], emp_data['post_grade'], emp_data.get('status'), emp_data.get('join_company_date'), emp_id))

        cursor.execute("UPDATE employee_profiles SET pos_id=?, tech_grade=?, title_order=?, education_level=?, degree=?, school_name=?, major=?, graduation_date=?, first_job_date=? WHERE emp_id=?", (profile_data.get('pos_id'), profile_data.get('tech_grade'), profile_data.get('title_order', 999), profile_data.get('education_level'), profile_data.get('degree'), profile_data.get('school_name'), profile_data.get('major'), profile_data.get('graduation_date'), profile_data.get('first_job_date'), emp_id))

        conn.commit()
        return True, "人员档案及状态已成功更新。"
    except Exception as e:
        conn.rollback(); return False, str(e)
    finally:
        conn.close()

def get_all_employees(dept_id=None, include_resigned=False):
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        sql = """
            SELECT 
                a.*, b.pos_id, b.tech_grade, b.education_level, c.dept_name, p.pos_name,
                (SELECT h.change_date FROM personnel_changes h 
                 LEFT JOIN positions hp ON h.new_pos_id = hp.pos_id
                 WHERE h.emp_id = a.emp_id AND hp.pos_name = '实习岗'
                 ORDER BY h.change_date DESC LIMIT 1) as intern_start_date
            FROM employees a
            LEFT JOIN employee_profiles b ON a.emp_id = b.emp_id
            LEFT JOIN departments c ON a.dept_id = c.dept_id
            LEFT JOIN positions p ON b.pos_id = p.pos_id
            WHERE 1=1
        """
        params = []
        if not include_resigned: sql += " AND a.status = '在职'"
        if dept_id: sql += " AND a.dept_id = ?"; params.append(dept_id)
        sql += " ORDER BY c.sort_order ASC, a.post_rank DESC"
        cursor.execute(sql, params)
        return True, [dict(row) for row in rows] if (rows := cursor.fetchall()) else []
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def get_all_history():
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT h.*, e.name as emp_name, d1.dept_name as old_dept_name, d2.dept_name as new_dept_name, p1.pos_name as old_pos_name, p2.pos_name as new_pos_name
            FROM personnel_changes h
            LEFT JOIN employees e ON h.emp_id = e.emp_id
            LEFT JOIN departments d1 ON h.old_dept_id = d1.dept_id
            LEFT JOIN departments d2 ON h.new_dept_id = d2.dept_id
            LEFT JOIN positions p1 ON h.old_pos_id = p1.pos_id
            LEFT JOIN positions p2 ON h.new_pos_id = p2.pos_id
            ORDER BY h.change_date DESC
        """)
        return True, [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def rollback_history(change_id):
    # [增量详尽注释 2026-03-26]
    # 为什么这么改：回退时必须把 status 也恢复，否则无法撤销“误操作离职”。
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM personnel_changes WHERE change_id = ?", (change_id,))
        hist = cursor.fetchone()
        if not hist or hist['change_type'] == '入职': return False, "入职记录无法撤销"

        # 强制回滚状态为“在职”，并将待遇还原
        cursor.execute("""
            UPDATE employees SET dept_id=?, post_rank=?, post_grade=?, status='在职' 
            WHERE emp_id=?
        """, (hist['old_dept_id'], hist['old_post_rank'], hist['old_post_grade'], hist['emp_id']))

        cursor.execute("""
            UPDATE employee_profiles SET pos_id=?, tech_grade=? 
            WHERE emp_id=?
        """, (hist['old_pos_id'], hist['old_tech_grade'], hist['emp_id']))

        cursor.execute("DELETE FROM personnel_changes WHERE change_id = ?", (change_id,))
        conn.commit()
        return True, "回退成功，人员已恢复在职且待遇还原。"
    except Exception as e:
        conn.rollback(); return False, str(e)
    finally:
        conn.close()

def update_employee_status(emp_id, new_status, reason="手动状态调整", change_date=None):
    # 这一步现在被整合进了 update_employee，保留此接口作为兼容性兜底
    actual_date = change_date if change_date else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE employees SET status = ? WHERE emp_id = ?", (new_status, emp_id))
        conn.commit(); return True, f"状态已切换为: {new_status}"
    except Exception as e:
        conn.rollback(); return False, str(e)
    finally:
        conn.close()