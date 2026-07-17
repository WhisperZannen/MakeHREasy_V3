import os
import sqlite3
import tempfile
import unittest


class PayrollWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "payroll_workflow.db")
        os.environ["MAKE_HR_DB_PATH"] = self.db_path

        from database.init_db import init_database

        init_database(self.db_path)
        self._seed_employee()

    def tearDown(self):
        os.environ.pop("MAKE_HR_DB_PATH", None)
        self.temp_dir.cleanup()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _seed_employee(self):
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO departments(dept_name, dept_category, sort_order, status) "
                "VALUES ('原部门', '生产', 1, 1)"
            )
            old_dept_id = conn.execute(
                "SELECT dept_id FROM departments WHERE dept_name='原部门'"
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO departments(dept_name, dept_category, sort_order, status) "
                "VALUES ('新部门', '生产', 2, 1)"
            )
            new_dept_id = conn.execute(
                "SELECT dept_id FROM departments WHERE dept_name='新部门'"
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO positions(pos_name, pos_category, status) "
                "VALUES ('AI研发工程师', '专业', 1)"
            )
            pos_id = conn.execute("SELECT pos_id FROM positions").fetchone()[0]
            conn.execute(
                """
                INSERT INTO employees(
                    emp_id, person_id, employee_no, name, id_card, dept_id,
                    post_rank, post_grade, status, join_company_date
                ) VALUES ('E1', 'PERSON-1', '001', '测试员工', 'ID-1', ?,
                          17, 'A', '在职', '2026-01-01')
                """,
                (new_dept_id,),
            )
            conn.execute(
                """
                INSERT INTO employee_profiles(
                    emp_id, pos_id, tech_grade, employment_stage, first_employment
                ) VALUES ('E1', ?, 'T3', 'regular', 0)
                """,
                (pos_id,),
            )
            conn.execute(
                "INSERT INTO ss_emp_matrix(emp_id, cost_center) VALUES ('E1', '本级')"
            )
            conn.execute(
                """
                INSERT INTO personnel_changes(
                    emp_id, change_type, old_dept_id, new_dept_id,
                    old_pos_id, new_pos_id, old_tech_grade, new_tech_grade,
                    old_post_rank, new_post_rank, old_post_grade, new_post_grade,
                    change_date, change_reason
                ) VALUES (
                    'E1', '跨部门调动', ?, ?, ?, ?, 'T3', 'T3',
                    17, 17, 'A', 'A', '2026-07-16 09:00:00', '测试15日规则'
                )
                """,
                (old_dept_id, new_dept_id, pos_id, pos_id),
            )
            version_id = conn.execute(
                "SELECT rule_version_id FROM payroll_rule_versions LIMIT 1"
            ).fetchone()[0]
            conn.execute(
                """
                INSERT INTO payroll_position_rule_mappings(
                    rule_version_id, pos_id, payroll_category,
                    official_position_name, enabled
                ) VALUES (?, ?, 'professional', 'AI研发工程师', 1)
                """,
                (version_id, pos_id),
            )
            conn.commit()
        finally:
            conn.close()

    def test_transfer_after_fifteenth_uses_old_department(self):
        from modules.core_payroll import get_effective_payroll_snapshot

        snapshot = get_effective_payroll_snapshot("2026-07")["E1"]
        self.assertEqual(snapshot["dept_name"], "原部门")

    def test_talent_uses_highest_multiplier_and_totals_separate_female_item(self):
        from modules.core_payroll import (
            generate_payroll_draft,
            recalculate_payroll_totals,
            save_payroll_identity,
        )

        ok, message = save_payroll_identity(
            "E1", "talent", "province", "2026-01-01"
        )
        self.assertTrue(ok, message)
        ok, message = save_payroll_identity(
            "E1", "talent", "group", "2026-01-01"
        )
        self.assertTrue(ok, message)

        result = generate_payroll_draft("2026-07", "2026-06")
        self.assertEqual(result["generated"], 1)
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM payroll_monthly_records WHERE emp_id='E1'"
            ).fetchone()
            self.assertEqual(row["dept_name"], "原部门")
            self.assertEqual(row["base_salary"], 2750.0)
            self.assertEqual(row["perf_excel_coef"], 2.0)
            self.assertEqual(row["perf_salary_calc"], 16500.0)
            self.assertEqual(row["gross_salary_total"], 19250.0)

            conn.execute(
                "UPDATE payroll_monthly_records SET female_labor_subsidy=120 "
                "WHERE emp_id='E1'"
            )
            conn.commit()
        finally:
            conn.close()

        recalculate_payroll_totals("2026-07")
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT gross_salary_total, cash_payable_total, net_salary "
                "FROM payroll_monthly_records WHERE emp_id='E1'"
            ).fetchone()
            self.assertEqual(row["gross_salary_total"], 19250.0)
            self.assertEqual(row["cash_payable_total"], 19370.0)
            self.assertEqual(row["net_salary"], 19370.0)
        finally:
            conn.close()

    def test_ended_identity_remains_available_for_historical_regeneration(self):
        from modules.core_payroll import (
            end_payroll_identity,
            generate_payroll_draft,
            save_payroll_identity,
        )

        ok, message = save_payroll_identity(
            "E1", "technical_elite", "elite", "2026-01-01"
        )
        self.assertTrue(ok, message)
        conn = self._connect()
        try:
            identity_id = conn.execute(
                "SELECT identity_id FROM employee_payroll_identities"
            ).fetchone()[0]
        finally:
            conn.close()
        ok, message = end_payroll_identity(identity_id, "2026-08-31")
        self.assertTrue(ok, message)

        generate_payroll_draft("2026-07", "2026-06")
        conn = self._connect()
        try:
            expert_allowance = conn.execute(
                "SELECT expert_allowance FROM payroll_monthly_records WHERE emp_id='E1'"
            ).fetchone()[0]
            self.assertEqual(expert_allowance, 1250.0)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
