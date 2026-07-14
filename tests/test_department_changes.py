import os
import sqlite3
import tempfile
import unittest


class DepartmentChangeTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "hr_test.db")
        from database.init_db import init_database

        init_database(self.db_path)
        os.environ["MAKE_HR_DB_PATH"] = self.db_path
        conn = sqlite3.connect(self.db_path)
        conn.execute("INSERT INTO departments(dept_name, dept_category) VALUES ('原部门', '生产')")
        conn.execute("INSERT INTO departments(dept_name, dept_category) VALUES ('新部门', '生产')")
        self.old_dept = conn.execute(
            "SELECT dept_id FROM departments WHERE dept_name='原部门'"
        ).fetchone()[0]
        self.new_dept = conn.execute(
            "SELECT dept_id FROM departments WHERE dept_name='新部门'"
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO employees(emp_id, name, dept_id, status)
            VALUES ('D001', '部门测试员工', ?, '在职')
            """,
            (self.old_dept,),
        )
        conn.execute("INSERT INTO employee_profiles(emp_id) VALUES ('D001')")
        conn.commit()
        conn.close()

    def tearDown(self):
        os.environ.pop("MAKE_HR_DB_PATH", None)
        self.temp_dir.cleanup()

    def test_batch_transfer_records_history_and_deactivates_empty_source(self):
        from modules.core_personnel import batch_transfer_department_members

        ok, message = batch_transfer_department_members(
            ['D001'], self.new_dept, '2026-07-15', '部门合并',
            self.old_dept, True,
        )
        self.assertTrue(ok, message)
        conn = sqlite3.connect(self.db_path)
        self.assertEqual(
            conn.execute("SELECT dept_id FROM employees WHERE emp_id='D001'").fetchone()[0],
            self.new_dept,
        )
        self.assertEqual(
            conn.execute("SELECT status FROM departments WHERE dept_id=?", (self.old_dept,)).fetchone()[0],
            0,
        )
        history = conn.execute(
            "SELECT old_dept_id, new_dept_id, change_date FROM personnel_changes WHERE emp_id='D001'"
        ).fetchone()
        conn.close()
        self.assertEqual(history[0:2], (self.old_dept, self.new_dept))
        self.assertTrue(history[2].startswith('2026-07-15'))

    def test_effective_department_obeys_fifteenth_day_cutoff(self):
        from modules.core_personnel import (
            batch_transfer_department_members,
            get_effective_department_snapshot,
        )

        ok, message = batch_transfer_department_members(
            ['D001'], self.new_dept, '2026-07-16', '16日后调动',
            self.old_dept, False,
        )
        self.assertTrue(ok, message)
        july = get_effective_department_snapshot('2026-07')
        august = get_effective_department_snapshot('2026-08')
        self.assertEqual(july['D001']['dept_id'], self.old_dept)
        self.assertEqual(august['D001']['dept_id'], self.new_dept)


if __name__ == '__main__':
    unittest.main()
