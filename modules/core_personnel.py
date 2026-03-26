# ==============================================================================
# 文件路径: modules/core_personnel.py
# 功能描述: 人员管理的底层数据接口 (差异快照与时空回滚版)
# 实现了什么具体逻辑:
#   1. 跨表事务(Transaction)，确保主表与扩展表数据强一致。
#   2. [核心重构] 引入 Delta 比对算法，只记录真正发生变动的算薪字段。
#   3. [核心重构] 新增 rollback_history，支持撤销历史流水并将数据倒流回上一个状态。
#   4. 解决了联表查询 pos_id 丢失导致的 KeyError。
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

def add_employee(emp_data, profile_data, reason="新员工入职"):
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO employees 
            (emp_id, name, id_card, dept_id, post_rank, post_grade, join_company_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (emp_data['emp_id'], emp_data['name'], emp_data['id_card'], emp_data['dept_id'], emp_data['post_rank'], emp_data['post_grade'], emp_data.get('join_company_date')))

        cursor.execute("""
            INSERT INTO employee_profiles 
            (emp_id, pos_id, tech_grade, title_order, education_level, degree, school_name, major, graduation_date, first_job_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (emp_data['emp_id'], profile_data.get('pos_id'), profile_data.get('tech_grade'), profile_data.get('title_order', 999), profile_data.get('education_level'), profile_data.get('degree'), profile_data.get('school_name'), profile_data.get('major'), profile_data.get('graduation_date'), profile_data.get('first_job_date')))

        # 入职快照 (old 为空，new 为当前录入值)
        cursor.execute("""
            INSERT INTO personnel_changes 
            (emp_id, change_type, new_dept_id, new_pos_id, new_tech_grade, new_post_rank, new_post_grade, change_reason) 
            VALUES (?, '入职', ?, ?, ?, ?, ?, ?)
        """, (emp_data['emp_id'], emp_data['dept_id'], profile_data.get('pos_id'), profile_data.get('tech_grade'), emp_data['post_rank'], emp_data['post_grade'], reason))

        conn.commit()
        return True, f"成功为员工 {emp_data['name']} 建立完整档案。"
    except sqlite3.IntegrityError as e:
        conn.rollback()
        return False, f"数据库完整性拦截: {e}"
    except Exception as e:
        conn.rollback()
        return False, f"未知错误: {e}"
    finally:
        conn.close()

def update_employee(emp_id, emp_data, profile_data, reason="档案更新"):
    # [增量详尽注释 2026-03-26] Delta 差异算法
    # 为什么这么改：如果 HR 只是把学历从“大专”改成了“本科”，系统不应该生成一条混淆视听的“调岗调薪”记录。
    # 实现了什么具体逻辑：比对新旧数据，提炼出真正的变动类型。只有五大核心指标动了，才触发流水落表。
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        # 1. 抓取旧数据
        cursor.execute("""
            SELECT e.dept_id, e.post_rank, e.post_grade, p.pos_id, p.tech_grade 
            FROM employees e
            LEFT JOIN employee_profiles p ON e.emp_id = p.emp_id
            WHERE e.emp_id = ?
        """, (emp_id,))
        old_data = cursor.fetchone()

        # 2. 差异比对核心逻辑
        trigger_snapshot = False
        change_tags = []

        if old_data:
            if str(old_data['dept_id']) != str(emp_data['dept_id']): change_tags.append("跨部门调动")
            if str(old_data['pos_id']) != str(profile_data.get('pos_id')): change_tags.append("岗位调整")
            if str(old_data['tech_grade']) != str(profile_data.get('tech_grade')): change_tags.append("T级变动")
            if str(old_data['post_rank']) != str(emp_data['post_rank']): change_tags.append("岗级调整")
            if str(old_data['post_grade']) != str(emp_data['post_grade']): change_tags.append("档次调整")

            if change_tags:
                trigger_snapshot = True
                final_change_type = " + ".join(change_tags)

                # 写入包含前后对比的完整快照
                cursor.execute("""
                    INSERT INTO personnel_changes 
                    (emp_id, change_type, 
                     old_dept_id, new_dept_id, 
                     old_pos_id, new_pos_id, 
                     old_tech_grade, new_tech_grade, 
                     old_post_rank, new_post_rank, 
                     old_post_grade, new_post_grade, 
                     change_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    emp_id, final_change_type,
                    old_data['dept_id'], emp_data['dept_id'],
                    old_data['pos_id'], profile_data.get('pos_id'),
                    old_data['tech_grade'], profile_data.get('tech_grade'),
                    old_data['post_rank'], emp_data['post_rank'],
                    old_data['post_grade'], emp_data['post_grade'],
                    reason
                ))

        # 3. 无论是否触发快照，都执行实际数据的物理更新
        cursor.execute("""
            UPDATE employees 
            SET name = ?, id_card = ?, dept_id = ?, post_rank = ?, post_grade = ?, join_company_date = ?
            WHERE emp_id = ?
        """, (emp_data['name'], emp_data['id_card'], emp_data['dept_id'], emp_data['post_rank'], emp_data['post_grade'], emp_data.get('join_company_date'), emp_id))

        cursor.execute("""
            UPDATE employee_profiles 
            SET pos_id = ?, tech_grade = ?, title_order = ?, education_level = ?, degree = ?, school_name = ?, major = ?, graduation_date = ?, first_job_date = ?
            WHERE emp_id = ?
        """, (profile_data.get('pos_id'), profile_data.get('tech_grade'), profile_data.get('title_order', 999), profile_data.get('education_level'), profile_data.get('degree'), profile_data.get('school_name'), profile_data.get('major'), profile_data.get('graduation_date'), profile_data.get('first_job_date'), emp_id))

        if cursor.rowcount == 0: return False, f"修改失败：找不到员工。"
        conn.commit()

        msg_suffix = "，并已生成核心变动快照。" if trigger_snapshot else " (基础信息已更新，未涉及薪酬指标，无新快照产生)。"
        return True, f"档案已更新{msg_suffix}"

    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def get_all_employees(dept_id=None, include_resigned=False):
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        # [核心修复] 明确提取 b.pos_id 供前端回填
        sql = """
            SELECT 
                a.*, 
                b.pos_id, b.tech_grade, b.title_order, b.education_level, b.degree, b.school_name, b.major, b.graduation_date, b.first_job_date,
                c.dept_name, c.sort_order as dept_sort,
                p.pos_name, p.sort_order as pos_sort
            FROM employees a
            LEFT JOIN employee_profiles b ON a.emp_id = b.emp_id
            LEFT JOIN departments c ON a.dept_id = c.dept_id
            LEFT JOIN positions p ON b.pos_id = p.pos_id
            WHERE 1=1
        """
        params = []
        if not include_resigned: sql += " AND a.status = '在职'"
        if dept_id is not None: sql += " AND a.dept_id = ?"; params.append(dept_id)
        sql += " ORDER BY c.sort_order ASC, p.sort_order ASC, b.title_order ASC, a.post_rank DESC"
        cursor.execute(sql, params)
        return True, [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def get_all_history():
    # [增量详尽注释 2026-03-26]
    # 为什么这么改：支持全局流水展现。利用多重 LEFT JOIN 把新旧 ID 全部翻译为直观的中文名。
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                h.*, e.name as emp_name,
                d1.dept_name as old_dept_name, d2.dept_name as new_dept_name,
                p1.pos_name as old_pos_name, p2.pos_name as new_pos_name
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
    # [增量详尽注释 2026-03-26] 终极功能：时光倒流
    # 实现了什么具体逻辑：查出这条快照的 old_ 数据，强行写回员工主表，然后抹除该快照。
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM personnel_changes WHERE change_id = ?", (change_id,))
        hist = cursor.fetchone()
        if not hist: return False, "找不到该条历史轨迹。"

        # 只有在职人员允许撤销调岗调薪，如果类型是入职，则无法撤销（入职撤销就是直接删人）
        if hist['change_type'] == '入职':
            return False, "入职初始记录无法撤销回退。如需清理请联系管理员执行硬删除。"

        # 强行回滚
        cursor.execute("""
            UPDATE employees SET dept_id = ?, post_rank = ?, post_grade = ?, status = '在职'
            WHERE emp_id = ?
        """, (hist['old_dept_id'], hist['old_post_rank'], hist['old_post_grade'], hist['emp_id']))

        cursor.execute("""
            UPDATE employee_profiles SET pos_id = ?, tech_grade = ?
            WHERE emp_id = ?
        """, (hist['old_pos_id'], hist['old_tech_grade'], hist['emp_id']))

        # 抹除该条时空记录
        cursor.execute("DELETE FROM personnel_changes WHERE change_id = ?", (change_id,))

        conn.commit()
        return True, "已成功撤销该变动，人员数据已回退至修改前状态！"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def update_employee_status(emp_id, new_status, reason="状态变更"):
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT e.dept_id, e.post_rank, e.post_grade, p.pos_id, p.tech_grade 
            FROM employees e LEFT JOIN employee_profiles p ON e.emp_id = p.emp_id WHERE e.emp_id = ?
        """, (emp_id,))
        old = cursor.fetchone()
        if old:
            cursor.execute("""
                INSERT INTO personnel_changes 
                (emp_id, change_type, old_dept_id, old_pos_id, old_tech_grade, old_post_rank, old_post_grade, change_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (emp_id, f"变为{new_status}", old['dept_id'], old['pos_id'], old['tech_grade'], old['post_rank'], old['post_grade'], reason))

        cursor.execute("UPDATE employees SET status = ? WHERE emp_id = ?", (new_status, emp_id))
        conn.commit()
        return True, f"状态已更新为: {new_status}"
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()