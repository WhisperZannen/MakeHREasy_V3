import os
import sqlite3
import tempfile
import unittest


class NewHireAssignmentTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "new_hire.db")
        os.environ["MAKE_HR_DB_PATH"] = self.db_path

        from database.init_db import init_database

        init_database(self.db_path)
        conn = self._connect()
        try:
            self.pending_dept_id = conn.execute(
                "SELECT dept_id FROM departments WHERE dept_name='新员工待分配池'"
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO departments(dept_name, dept_category, status) "
                "VALUES ('正式部门', '生产', 1)"
            )
            self.real_dept_id = conn.execute(
                "SELECT dept_id FROM departments WHERE dept_name='正式部门'"
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO positions(pos_name, pos_category, status) "
                "VALUES ('新员工测试岗', '专业', 1)"
            )
            self.pos_id = conn.execute(
                "SELECT pos_id FROM positions WHERE pos_name='新员工测试岗'"
            ).fetchone()[0]
            conn.commit()
        finally:
            conn.close()

    def tearDown(self):
        os.environ.pop("MAKE_HR_DB_PATH", None)
        self.temp_dir.cleanup()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _add_pending_hire(self):
        from modules.core_personnel import add_employee

        ok, message = add_employee(
            {
                "employee_no": None,
                "name": "七月新员工",
                "id_card": "NEW-HIRE-ID",
                "dept_id": self.pending_dept_id,
                "post_rank": 11,
                "post_grade": "A",
                "status": "在职",
                "join_company_date": "2026-07-20",
            },
            {
                "pos_id": self.pos_id,
                "tech_grade": "T1",
                "education_level": "本科",
                "employment_stage": "intern",
                "first_employment": 1,
                "payroll_start_month": "2026-08",
            },
            reason="新员工入职",
        )
        self.assertTrue(ok, message)
        conn = self._connect()
        try:
            return conn.execute(
                "SELECT emp_id FROM employees WHERE id_card='NEW-HIRE-ID'"
            ).fetchone()[0]
        finally:
            conn.close()

    def test_first_assignment_after_fifteenth_belongs_to_real_department(self):
        from modules.core_personnel import (
            get_effective_department_snapshot,
            update_employee,
        )

        emp_id = self._add_pending_hire()
        ok, message = update_employee(
            emp_id,
            {
                "employee_no": "NEW001",
                "name": "七月新员工",
                "id_card": "NEW-HIRE-ID",
                "dept_id": self.real_dept_id,
                "post_rank": 11,
                "post_grade": "A",
                "status": "在职",
                "join_company_date": "2026-07-20",
            },
            {
                "pos_id": self.pos_id,
                "tech_grade": "T1",
                "education_level": "本科",
                "employment_stage": "intern",
                "first_employment": 1,
                "payroll_start_month": "2026-08",
            },
            reason="正式部门确定",
            change_date="2026-07-20 00:00:00",
        )
        self.assertTrue(ok, message)

        july = get_effective_department_snapshot("2026-07")
        self.assertEqual(july[emp_id]["dept_name"], "正式部门")
        self.assertEqual(july[emp_id]["is_pending_pool"], 0)
        self.assertNotIn(emp_id, get_effective_department_snapshot("2026-06"))

        conn = self._connect()
        try:
            change = conn.execute(
                """
                SELECT change_type, change_date FROM personnel_changes
                WHERE emp_id=? ORDER BY change_id DESC LIMIT 1
                """,
                (emp_id,),
            ).fetchone()
            self.assertIn("首次部门分配", change["change_type"])
            self.assertTrue(str(change["change_date"]).startswith("2026-07-20"))
        finally:
            conn.close()

    def test_salary_is_deferred_but_social_identity_remains_and_backpay_is_separate(self):
        from modules.core_payroll import generate_payroll_draft, save_new_hire_backpay

        emp_id = self._add_pending_hire()
        july = generate_payroll_draft("2026-07", "2026-06")
        self.assertEqual(july["deferred"], 1)

        conn = self._connect()
        try:
            self.assertIsNone(conn.execute(
                "SELECT 1 FROM payroll_monthly_records WHERE cost_month='2026-07' AND emp_id=?",
                (emp_id,),
            ).fetchone())
            self.assertIsNotNone(conn.execute(
                "SELECT 1 FROM ss_emp_matrix WHERE emp_id=?", (emp_id,)
            ).fetchone())
        finally:
            conn.close()

        ok, message = save_new_hire_backpay(
            emp_id, "2026-07", "2026-08", 1234.56, "测试补发"
        )
        self.assertTrue(ok, message)
        august = generate_payroll_draft("2026-08", "2026-07")
        self.assertEqual(august["generated"], 1)
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT new_hire_backpay, gross_salary_total
                FROM payroll_monthly_records
                WHERE cost_month='2026-08' AND emp_id=?
                """,
                (emp_id,),
            ).fetchone()
            self.assertEqual(row["new_hire_backpay"], 1234.56)
            self.assertGreaterEqual(row["gross_salary_total"], 1234.56)
        finally:
            conn.close()

    def test_master_regularization_date_recalculates_after_stale_form_value(self):
        from modules.core_personnel import _prepare_lifecycle

        result = _prepare_lifecycle(
            {
                "employment_stage": "intern",
                "education_level": "硕士",
                "expected_regularization_date": "2027-01-22",
            },
            "2026-07-22",
        )
        self.assertEqual(result["expected_regularization_date"], "2026-10-22")


if __name__ == "__main__":
    unittest.main()
