# ==============================================================================
# 文件路径: modules/core_personnel.py
# 功能描述: 人员管理数据接口 (V3.20 底层稳固版)
# 实现了什么具体逻辑:
#   1. [防爆修复] 将 get_all_employees 中的海象运算符拆解，根除 UnboundLocalError 作用域泄漏。
#   2. [撤销修复] 将 rollback_history 的防卫判断，扩充支持“期初建档”与“新员工入职”。
#   3. 精准计时：继续保持 intern_start_date 的 SQL 穿透查询。
# ==============================================================================

import sqlite3
import os
import uuid
import calendar
from datetime import datetime

from modules.core_identity import employee_no_exists, normalize_employee_no


def _normalize_text(value):
    """统一空值及普通文本的比较口径。"""
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text in {"", "None", "nan", "NaN", "NaT"} else text


def _normalize_rank_value(value):
    """将 11、11.0 和字符串“11.0”统一为同一个岗级值。"""
    text = _normalize_text(value)
    if not text:
        return ""
    try:
        number = float(text)
        return str(int(number)) if number.is_integer() else format(number, ".12g")
    except (TypeError, ValueError):
        return text


def build_personnel_change_tags(old, emp_data, profile_data):
    """根据更新前后的真实字段值生成变动标签。"""
    tags = []
    if _normalize_text(old.get('status')) != _normalize_text(emp_data.get('status')):
        tags.append(f"变为{emp_data.get('status')}")
    if _normalize_rank_value(old.get('dept_id')) != _normalize_rank_value(emp_data.get('dept_id')):
        tags.append("跨部门调动")
    if _normalize_rank_value(old.get('pos_id')) != _normalize_rank_value(profile_data.get('pos_id')):
        tags.append("实习转正" if _normalize_text(old.get('old_pos_name')) == "实习岗" else "岗位调整")
    if _normalize_text(old.get('tech_grade')) != _normalize_text(profile_data.get('tech_grade')):
        tags.append("T级变动")
    if _normalize_rank_value(old.get('post_rank')) != _normalize_rank_value(emp_data.get('post_rank')):
        tags.append("岗级调整")
    if _normalize_text(old.get('post_grade')) != _normalize_text(emp_data.get('post_grade')):
        tags.append("档次调整")
    return tags


def rebuild_history_change_type(row):
    """依据历史行保存的新旧快照重建标签，保留无法从快照推导的状态类标签。"""
    original = _normalize_text(row.get('change_type'))
    if not original or any(keyword in original for keyword in ("入职", "建档")):
        return original

    parts = [part.strip() for part in original.split("+") if part.strip()]
    recognized = {"跨部门调动", "岗位调整", "实习转正", "T级变动", "岗级调整", "档次调整"}
    tags = [part for part in parts if part.startswith("变为") or part not in recognized]

    if _normalize_rank_value(row.get('old_dept_id')) != _normalize_rank_value(row.get('new_dept_id')):
        tags.append("跨部门调动")
    if _normalize_rank_value(row.get('old_pos_id')) != _normalize_rank_value(row.get('new_pos_id')):
        tags.append("实习转正" if _normalize_text(row.get('old_pos_name')) == "实习岗" else "岗位调整")
    if _normalize_text(row.get('old_tech_grade')) != _normalize_text(row.get('new_tech_grade')):
        tags.append("T级变动")
    if _normalize_rank_value(row.get('old_post_rank')) != _normalize_rank_value(row.get('new_post_rank')):
        tags.append("岗级调整")
    if _normalize_text(row.get('old_post_grade')) != _normalize_text(row.get('new_post_grade')):
        tags.append("档次调整")

    # 去重但保持业务展示顺序。
    return " + ".join(dict.fromkeys(tags)) or "档案更新"


def repair_personnel_change_types(conn=None):
    """修复历史流水中与新旧快照不一致的标签，不改动任何人员档案值。"""
    own_conn = conn is None
    conn = conn or _get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT h.*, p.pos_name AS old_pos_name
            FROM personnel_changes h
            LEFT JOIN positions p ON h.old_pos_id = p.pos_id
            ORDER BY h.change_id
            """
        ).fetchall()
        changes = []
        for source in rows:
            row = dict(source)
            corrected = rebuild_history_change_type(row)
            if corrected and corrected != row.get('change_type'):
                conn.execute(
                    "UPDATE personnel_changes SET change_type = ? WHERE change_id = ?",
                    (corrected, row['change_id']),
                )
                changes.append({
                    'change_id': row['change_id'],
                    'emp_id': row['emp_id'],
                    'before': row.get('change_type'),
                    'after': corrected,
                })
        conn.commit()
        return True, changes
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        if own_conn:
            conn.close()

def _get_db_connection():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    db_path = os.environ.get(
        'MAKE_HR_DB_PATH', os.path.join(project_root, 'database', 'hr_core.db')
    )
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn


def get_organization_integrity_issues():
    """返回会影响社保、薪酬和人工成本的当前组织关联异常。"""
    conn = _get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT e.emp_id, COALESCE(e.employee_no, '待分配') AS employee_no,
                   e.name,
                   CASE
                     WHEN d.dept_id IS NULL THEN '部门不存在'
                     WHEN d.status<>1 THEN '人员仍归属已撤销部门'
                     WHEN p.pos_id IS NULL THEN '岗位不存在'
                     WHEN p.status<>1 THEN '人员仍任已停用岗位'
                   END AS issue
            FROM employees e
            LEFT JOIN departments d ON d.dept_id=e.dept_id
            LEFT JOIN employee_profiles ep ON ep.emp_id=e.emp_id
            LEFT JOIN positions p ON p.pos_id=ep.pos_id
            WHERE e.status IN ('在职', '挂靠人员')
              AND (d.dept_id IS NULL OR d.status<>1 OR p.pos_id IS NULL OR p.status<>1)
            ORDER BY e.name
            """
        ).fetchall()
        issues = [dict(row) for row in rows]
        duplicated = conn.execute(
            """
            SELECT a.emp_id, COALESCE(e.employee_no, '待分配') AS employee_no,
                   e.name, COUNT(*) AS relation_count
            FROM employee_arrangements a
            JOIN employees e ON e.emp_id=a.emp_id
            WHERE a.status='active'
            GROUP BY a.emp_id, e.employee_no, e.name
            HAVING COUNT(*)>1
            ORDER BY e.name
            """
        ).fetchall()
        issues.extend({
            'emp_id': row['emp_id'],
            'employee_no': row['employee_no'],
            'name': row['name'],
            'issue': f"存在{row['relation_count']}条同时生效的特殊关系",
        } for row in duplicated)
        return issues
    finally:
        conn.close()


def _add_calendar_months(date_text, months):
    try:
        source = datetime.strptime(str(date_text)[:10], '%Y-%m-%d').date()
    except (TypeError, ValueError):
        return None
    index = source.year * 12 + source.month - 1 + int(months)
    year, month = divmod(index, 12)
    day = min(source.day, calendar.monthrange(year, month + 1)[1])
    return f'{year:04d}-{month + 1:02d}-{day:02d}'


def _prepare_lifecycle(profile_data, join_date, actual_date=None, was_intern=False):
    result = dict(profile_data)
    stage = result.get('employment_stage') or 'regular'
    education = _normalize_text(result.get('education_level'))
    if stage == 'intern' and not result.get('expected_regularization_date'):
        months = 3 if education in {'硕士', '研究生'} else 6
        result['expected_regularization_date'] = _add_calendar_months(join_date, months)
    if was_intern and stage == 'regular' and not result.get('actual_regularization_date'):
        result['actual_regularization_date'] = str(actual_date or datetime.now())[:10]
    return result


def _validate_active_assignment(conn, emp_data, profile_data):
    """在职及挂靠人员只能归入有效部门和岗位，离退历史池不受此限制。"""
    if emp_data.get('status', '在职') not in {'在职', '挂靠人员'}:
        return None
    dept = conn.execute(
        "SELECT dept_name, status FROM departments WHERE dept_id=?",
        (emp_data.get('dept_id'),),
    ).fetchone()
    if not dept:
        return "所选部门不存在"
    if int(dept['status'] or 0) != 1:
        return f"部门“{dept['dept_name']}”已经撤销，请选择有效部门"
    position = conn.execute(
        "SELECT pos_name, status FROM positions WHERE pos_id=?",
        (profile_data.get('pos_id'),),
    ).fetchone()
    if not position:
        return "所选岗位不存在"
    if int(position['status'] or 0) != 1:
        return f"岗位“{position['pos_name']}”已经停用，请选择有效岗位"
    return None


def add_employee(emp_data, profile_data, reason="新员工入职", change_date=None):
    join_date = emp_data.get('join_company_date')
    if join_date:
        initial_snapshot_time = f"{join_date} 00:00:00"
    else:
        initial_snapshot_time = change_date if change_date else datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    snapshot_type = "期初建档" if "导入" in reason else "新员工入职"

    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        assignment_error = _validate_active_assignment(conn, emp_data, profile_data)
        if assignment_error:
            return False, assignment_error
        employee_no = normalize_employee_no(
            emp_data.get('employee_no', emp_data.get('emp_id'))
        )
        if employee_no_exists(employee_no, conn=conn):
            return False, f"工号 {employee_no} 已被其他人员使用"
        internal_emp_id = str(
            emp_data.get('internal_emp_id')
            or emp_data.get('emp_id')
            or f"P-{uuid.uuid4().hex}"
        )
        profile_data = _prepare_lifecycle(
            profile_data, emp_data.get('join_company_date'), change_date
        )
        cursor.execute("""
                       INSERT INTO employees (emp_id, person_id, employee_no, name, id_card, dept_id, post_rank, post_grade, status,
                                              join_company_date)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       """, (internal_emp_id, str(uuid.uuid4()), employee_no, emp_data['name'], emp_data['id_card'], emp_data['dept_id'],
                             emp_data['post_rank'], emp_data['post_grade'], emp_data.get('status', '在职'),
                             emp_data.get('join_company_date')))
        cursor.execute("""
                       INSERT INTO employee_profiles (emp_id, pos_id, tech_grade, title_order, education_level, degree,
                                                      school_name, major, graduation_date, first_job_date,
                                                      employment_stage, first_employment,
                                                      expected_regularization_date, actual_regularization_date)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       """, (internal_emp_id, profile_data.get('pos_id'), profile_data.get('tech_grade'),
                             profile_data.get('title_order', 999), profile_data.get('education_level'),
                             profile_data.get('degree'), profile_data.get('school_name'), profile_data.get('major'),
                             profile_data.get('graduation_date'), profile_data.get('first_job_date'),
                             profile_data.get('employment_stage', 'regular'),
                             int(profile_data.get('first_employment', 0) or 0),
                             profile_data.get('expected_regularization_date'),
                             profile_data.get('actual_regularization_date')))
        cursor.execute("""
            INSERT OR IGNORE INTO ss_emp_matrix (
                emp_id, cost_center,
                pension_enabled, pension_account, medical_enabled, medical_account,
                unemp_enabled, unemp_account, injury_enabled, injury_account,
                maternity_enabled, maternity_account, fund_enabled, fund_account,
                annuity_enabled, annuity_account
            ) VALUES (?, '本级', 1, '省公众', 1, '省公司', 1, '省公众',
                      1, '省公司', 1, '省公司', 1, '省公众', 1, '省公司')
        """, (internal_emp_id,))
        cursor.execute("""
                       INSERT INTO personnel_changes (emp_id, change_type, new_dept_id, new_pos_id, new_tech_grade,
                                                      new_post_rank, new_post_grade, change_date, change_reason)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                       """, (internal_emp_id, snapshot_type, emp_data['dept_id'], profile_data.get('pos_id'),
                             profile_data.get('tech_grade'), emp_data['post_rank'], emp_data['post_grade'],
                             initial_snapshot_time, reason))
        conn.commit()
        return True, f"成功为 {emp_data['name']} 建立档案。"
    except Exception as e:
        conn.rollback();
        return False, str(e)
    finally:
        conn.close()

def update_employee(emp_id, emp_data, profile_data, reason="档案更新", change_date=None):
    actual_date = change_date if change_date else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        assignment_error = _validate_active_assignment(conn, emp_data, profile_data)
        if assignment_error:
            return False, assignment_error
        employee_no = normalize_employee_no(
            emp_data.get('employee_no', emp_data.get('emp_id'))
        )
        if employee_no_exists(employee_no, exclude_emp_id=emp_id, conn=conn):
            return False, f"工号 {employee_no} 已被其他人员使用"
        cursor.execute("""
            SELECT e.dept_id, e.post_rank, e.post_grade, e.status,
                   p.pos_id, p.tech_grade, p.employment_stage, p.first_employment,
                   p.expected_regularization_date, p.actual_regularization_date,
                   pos.pos_name as old_pos_name
            FROM employees e
            LEFT JOIN employee_profiles p ON e.emp_id = p.emp_id
            LEFT JOIN positions pos ON p.pos_id = pos.pos_id
            WHERE e.emp_id = ?
        """, (emp_id,))
        old = cursor.fetchone()

        if old:
            old_snapshot = dict(old)
            for field in (
                'employment_stage', 'first_employment',
                'expected_regularization_date', 'actual_regularization_date',
            ):
                if field not in profile_data:
                    profile_data[field] = old_snapshot.get(field)
            was_intern = _normalize_text(old_snapshot.get('old_pos_name')) == '实习岗'
            profile_data = _prepare_lifecycle(
                profile_data, emp_data.get('join_company_date'), actual_date,
                was_intern=was_intern,
            )
            change_tags = build_personnel_change_tags(old_snapshot, emp_data, profile_data)

            if change_tags:
                cursor.execute("""
                    INSERT INTO personnel_changes 
                    (emp_id, change_type, old_dept_id, new_dept_id, old_pos_id, new_pos_id, old_tech_grade, new_tech_grade, old_post_rank, new_post_rank, old_post_grade, new_post_grade, change_date, change_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (emp_id, " + ".join(change_tags), old['dept_id'], emp_data['dept_id'], old['pos_id'], profile_data.get('pos_id'), old['tech_grade'], profile_data.get('tech_grade'), old['post_rank'], emp_data['post_rank'], old['post_grade'], emp_data['post_grade'], actual_date, reason))

        cursor.execute("""
            UPDATE employees
            SET employee_no=?, name=?, id_card=?, dept_id=?, post_rank=?, post_grade=?, status=?, join_company_date=?
            WHERE emp_id=?
        """, (employee_no, emp_data['name'], emp_data['id_card'], emp_data['dept_id'], emp_data['post_rank'], emp_data['post_grade'], emp_data.get('status'), emp_data.get('join_company_date'), emp_id))
        if cursor.rowcount != 1:
            raise ValueError("未找到要更新的人员，请刷新人员名单后重试")

        cursor.execute("""
            UPDATE employee_profiles
            SET pos_id=?, tech_grade=?, title_order=?, education_level=?, degree=?,
                school_name=?, major=?, graduation_date=?, first_job_date=?,
                employment_stage=?, first_employment=?, expected_regularization_date=?,
                actual_regularization_date=?
            WHERE emp_id=?
        """, (
            profile_data.get('pos_id'), profile_data.get('tech_grade'),
            profile_data.get('title_order', 999), profile_data.get('education_level'),
            profile_data.get('degree'), profile_data.get('school_name'),
            profile_data.get('major'), profile_data.get('graduation_date'),
            profile_data.get('first_job_date'), profile_data.get('employment_stage', 'regular'),
            int(profile_data.get('first_employment', 0) or 0),
            profile_data.get('expected_regularization_date'),
            profile_data.get('actual_regularization_date'), emp_id,
        ))

        saved_row = cursor.execute(
            "SELECT employee_no FROM employees WHERE emp_id = ?", (emp_id,)
        ).fetchone()
        saved_employee_no = normalize_employee_no(saved_row['employee_no']) if saved_row else None
        if saved_employee_no != employee_no:
            raise ValueError("工号保存校验失败，数据库未写入预期值")

        conn.commit()
        return True, f"人员档案已更新，当前工号：{saved_employee_no or '待分配'}。"
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

        # [核心拆弹] 去除海象运算符 := ，根绝由于作用域提升导致的 UnboundLocalError
        rows = cursor.fetchall()
        return True, [dict(row) for row in rows] if rows else []
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def get_all_history():
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT h.*, e.employee_no, e.name as emp_name, d1.dept_name as old_dept_name, d2.dept_name as new_dept_name, p1.pos_name as old_pos_name, p2.pos_name as new_pos_name
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
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM personnel_changes WHERE change_id = ?", (change_id,))
        hist = cursor.fetchone()

        # [核心修复] 放宽限制检测词，拦截新员工入职、期初建档等无法撤销动作
        if not hist or any(x in hist['change_type'] for x in ['入职', '建档']):
            return False, "期初建档或入职记录无上级节点，无法直接撤销，请通过修改人员档案处理。"

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


def get_effective_department_snapshot(target_month, conn=None):
    """按15日规则返回某月每个人应归属的部门ID和当时部门名称。"""
    own_conn = conn is None
    conn = conn or _get_db_connection()
    try:
        if len(str(target_month)) < 7:
            raise ValueError("目标月份必须为 YYYY-MM")
        departments = {
            int(row['dept_id']): str(row['dept_name'])
            for row in conn.execute("SELECT dept_id, dept_name FROM departments").fetchall()
        }
        effective = {
            str(row['emp_id']).strip(): int(row['dept_id'])
            for row in conn.execute("SELECT emp_id, dept_id FROM employees").fetchall()
        }
        deadline = f"{str(target_month)[:7]}-15 23:59:59"
        changes = conn.execute(
            """
            SELECT emp_id, old_dept_id
            FROM personnel_changes
            WHERE change_date > ?
              AND old_dept_id IS NOT NULL AND new_dept_id IS NOT NULL
            ORDER BY change_date DESC, change_id DESC
            """,
            (deadline,),
        ).fetchall()
        for row in changes:
            emp_id = str(row['emp_id']).strip()
            if emp_id in effective:
                effective[emp_id] = int(row['old_dept_id'])
        return {
            emp_id: {
                'dept_id': dept_id,
                'dept_name': departments.get(dept_id, '未分配部门'),
            }
            for emp_id, dept_id in effective.items()
        }
    finally:
        if own_conn:
            conn.close()


def classify_department_snapshot_change(old_dept_id, old_dept_name, target_department):
    """判断人工成本台账的部门快照是否需要按当前组织档案刷新。"""
    if not target_department:
        return None

    old_id = _normalize_rank_value(old_dept_id)
    new_id = _normalize_rank_value(target_department.get('dept_id'))
    old_name = _normalize_text(old_dept_name)
    new_name = _normalize_text(target_department.get('dept_name'))
    reasons = []

    if old_id != new_id:
        reasons.append("补齐部门ID" if not old_id else "人员归属调整")
    if old_name != new_name:
        reasons.append("部门名称同步")

    if not reasons:
        return None
    return {
        'new_dept_id': int(float(new_id)),
        'new_dept_name': new_name,
        'reason': "、".join(reasons),
    }


def batch_transfer_department_members(
    emp_ids,
    target_dept_id,
    effective_date,
    reason,
    source_dept_id=None,
    deactivate_empty_source=False,
):
    """批量完成部门合并、撤销承接或拆分中的一组人员转移。"""
    selected_ids = [str(emp_id).strip() for emp_id in emp_ids if str(emp_id).strip()]
    if not selected_ids:
        return False, "请至少选择一名需要调整的人员"
    if not str(reason or '').strip():
        return False, "请填写组织调整说明"
    try:
        actual_date = datetime.strptime(
            str(effective_date)[:10], '%Y-%m-%d'
        ).strftime('%Y-%m-%d 00:00:00')
    except ValueError:
        return False, "生效日期必须为 YYYY-MM-DD"

    conn = _get_db_connection()
    try:
        target = conn.execute(
            "SELECT dept_name, status FROM departments WHERE dept_id = ?",
            (int(target_dept_id),),
        ).fetchone()
        if not target or int(target['status']) != 1:
            return False, "承接部门不存在或已经撤销"

        placeholders = ','.join(['?'] * len(selected_ids))
        rows = conn.execute(
            f"""
            SELECT e.emp_id, e.dept_id, e.post_rank, e.post_grade,
                   p.pos_id, p.tech_grade
            FROM employees e
            LEFT JOIN employee_profiles p ON e.emp_id = p.emp_id
            WHERE e.emp_id IN ({placeholders})
            """,
            selected_ids,
        ).fetchall()
        if len(rows) != len(set(selected_ids)):
            return False, "部分人员不存在，请刷新页面后重试"
        if source_dept_id is not None:
            unexpected = [
                row['emp_id'] for row in rows
                if int(row['dept_id']) != int(source_dept_id)
            ]
            if unexpected:
                return False, f"有 {len(unexpected)} 人已不在原部门，请刷新后重试"
        if any(int(row['dept_id']) == int(target_dept_id) for row in rows):
            return False, "所选人员中有人已经在承接部门"

        for row in rows:
            conn.execute(
                """
                INSERT INTO personnel_changes(
                    emp_id, change_type, old_dept_id, new_dept_id,
                    old_pos_id, new_pos_id, old_tech_grade, new_tech_grade,
                    old_post_rank, new_post_rank, old_post_grade, new_post_grade,
                    change_date, change_reason
                ) VALUES (?, '跨部门调动', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row['emp_id'], row['dept_id'], int(target_dept_id),
                    row['pos_id'], row['pos_id'], row['tech_grade'], row['tech_grade'],
                    row['post_rank'], row['post_rank'], row['post_grade'], row['post_grade'],
                    actual_date, str(reason).strip(),
                ),
            )
            conn.execute(
                "UPDATE employees SET dept_id = ? WHERE emp_id = ?",
                (int(target_dept_id), row['emp_id']),
            )
            conn.execute(
                """
                UPDATE employee_arrangements
                SET home_dept_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE emp_id = ? AND status = 'active'
                """,
                (int(target_dept_id), row['emp_id']),
            )

        source_deactivated = False
        if deactivate_empty_source and source_dept_id is not None:
            remaining = conn.execute(
                "SELECT COUNT(*) FROM employees WHERE dept_id = ?",
                (int(source_dept_id),),
            ).fetchone()[0]
            if remaining == 0:
                conn.execute(
                    "UPDATE departments SET status = 0 WHERE dept_id = ?",
                    (int(source_dept_id),),
                )
                source_deactivated = True
        conn.commit()
        suffix = "，原部门已撤销" if source_deactivated else ""
        return True, f"已调整 {len(rows)} 人到“{target['dept_name']}”{suffix}"
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()
