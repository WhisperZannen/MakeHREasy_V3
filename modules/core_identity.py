"""人员业务编码与内部关联键的兼容转换。"""

import os
import sqlite3
from typing import Optional


def _connect(conn=None):
    if conn is not None:
        return conn, False
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.environ.get(
        'MAKE_HR_DB_PATH', os.path.join(project_root, 'database', 'hr_core.db')
    )
    owned = sqlite3.connect(db_path)
    owned.row_factory = sqlite3.Row
    return owned, True


def normalize_employee_no(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text.endswith('.0') and text[:-2].isdigit():
        text = text[:-2]
    return None if text in {'', 'nan', 'None', 'NaN', '<NA>', 'NaT'} else text


def resolve_internal_emp_id(value, conn=None) -> Optional[str]:
    """把用户输入的工号转换为隐藏内部键；兼容历史代码传内部键。"""
    number = normalize_employee_no(value)
    if not number:
        return None
    db, owned = _connect(conn)
    try:
        row = db.execute(
            """
            SELECT emp_id FROM employees
            WHERE employee_no = ? OR emp_id = ?
            ORDER BY CASE WHEN employee_no = ? THEN 0 ELSE 1 END
            LIMIT 1
            """,
            (number, number, number),
        ).fetchone()
        return str(row['emp_id'] if hasattr(row, 'keys') else row[0]) if row else None
    finally:
        if owned:
            db.close()


def resolve_employee_reference(employee_no=None, id_card=None, name=None, conn=None) -> Optional[str]:
    """按工号、身份证号、唯一姓名依次识别人；不向导入模板暴露内部键。"""
    db, owned = _connect(conn)
    try:
        resolved = resolve_internal_emp_id(employee_no, db)
        if resolved:
            return resolved
        card = normalize_employee_no(id_card)
        if card:
            row = db.execute(
                "SELECT emp_id FROM employees WHERE id_card = ? LIMIT 1", (card,)
            ).fetchone()
            if row:
                return str(row['emp_id'] if hasattr(row, 'keys') else row[0])
        person_name = str(name or '').strip()
        if person_name:
            rows = db.execute(
                "SELECT emp_id FROM employees WHERE name = ?", (person_name,)
            ).fetchall()
            if len(rows) == 1:
                row = rows[0]
                return str(row['emp_id'] if hasattr(row, 'keys') else row[0])
        return None
    finally:
        if owned:
            db.close()


def get_employee_no(emp_id, conn=None, pending_label='待分配') -> str:
    db, owned = _connect(conn)
    try:
        row = db.execute(
            "SELECT employee_no FROM employees WHERE emp_id = ?", (str(emp_id),)
        ).fetchone()
        value = row['employee_no'] if row and hasattr(row, 'keys') else (row[0] if row else None)
        return normalize_employee_no(value) or pending_label
    finally:
        if owned:
            db.close()


def employee_no_exists(employee_no, exclude_emp_id=None, conn=None) -> bool:
    number = normalize_employee_no(employee_no)
    if not number:
        return False
    db, owned = _connect(conn)
    try:
        if exclude_emp_id is None:
            row = db.execute(
                "SELECT 1 FROM employees WHERE employee_no = ? LIMIT 1", (number,)
            ).fetchone()
        else:
            row = db.execute(
                "SELECT 1 FROM employees WHERE employee_no = ? AND emp_id <> ? LIMIT 1",
                (number, str(exclude_emp_id)),
            ).fetchone()
        return row is not None
    finally:
        if owned:
            db.close()
