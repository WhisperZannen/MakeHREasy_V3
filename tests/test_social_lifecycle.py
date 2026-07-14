import os
import sqlite3
import tempfile
import unittest


class SocialLifecycleTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.temp_dir.name, 'hr_test.db')
        os.environ['MAKE_HR_DB_PATH'] = cls.db_path

        from database.init_db import init_database
        init_database(cls.db_path)

        conn = sqlite3.connect(cls.db_path)
        conn.execute(
            "INSERT INTO departments(dept_name, dept_category) VALUES ('测试部门', '生产')"
        )
        dept_id = conn.execute(
            "SELECT dept_id FROM departments WHERE dept_name='测试部门'"
        ).fetchone()[0]
        conn.execute("INSERT INTO positions(pos_name) VALUES ('实习岗')")
        pos_id = conn.execute(
            "SELECT pos_id FROM positions WHERE pos_name='实习岗'"
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO employees(emp_id, person_id, name, dept_id, status, join_company_date)
            VALUES ('I001', 'person-intern-1', '实习生', ?, '在职', '2026-07-15')
            """,
            (dept_id,),
        )
        conn.execute(
            """
            INSERT INTO employee_profiles(
                emp_id, pos_id, education_level, employment_stage, first_employment,
                expected_regularization_date
            ) VALUES ('I001', ?, '本科', 'intern', 1, '2027-01-15')
            """,
            (pos_id,),
        )
        conn.commit()
        conn.close()

        from modules import core_social_security
        cls.social = core_social_security

    @classmethod
    def tearDownClass(cls):
        os.environ.pop('MAKE_HR_DB_PATH', None)
        cls.temp_dir.cleanup()

    def test_fund_rounds_contribution_then_reverses_execution_base(self):
        base, company, personal = self.social.calc_insurance_item(
            'fund', 37108.33, 0, 0, 0.12, 0.12, 'exact', 'reverse_from_ss'
        )
        self.assertEqual(company, 4450.0)
        self.assertEqual(personal, 4450.0)
        self.assertEqual(base, 37083.33)

        base, company, _ = self.social.calc_insurance_item(
            'fund', 37125, 0, 0, 0.12, 0.12, 'exact', 'reverse_from_ss'
        )
        self.assertEqual(company, 4460.0)
        self.assertEqual(base, 37166.67)

    def test_independent_fund_base_remains_exact(self):
        base, company, personal = self.social.calc_insurance_item(
            'fund', 21977, 0, 0, 0.12, 0.12, 'round_to_ten', 'independent'
        )
        self.assertEqual(base, 21977)
        self.assertEqual(company, 2637.24)
        self.assertEqual(personal, 2637.24)

    def test_new_intern_gets_internal_id_expected_date_and_regularization(self):
        from modules.core_personnel import add_employee, update_employee

        conn = sqlite3.connect(self.db_path)
        dept_id = conn.execute(
            "SELECT dept_id FROM departments WHERE dept_name='测试部门'"
        ).fetchone()[0]
        intern_pos = conn.execute(
            "SELECT pos_id FROM positions WHERE pos_name='实习岗'"
        ).fetchone()[0]
        conn.execute("INSERT OR IGNORE INTO positions(pos_name) VALUES ('正式岗位')")
        regular_pos = conn.execute(
            "SELECT pos_id FROM positions WHERE pos_name='正式岗位'"
        ).fetchone()[0]
        conn.commit()
        conn.close()

        employee = {
            'emp_id': 'I002', 'name': '新增实习生', 'id_card': None,
            'dept_id': dept_id, 'post_rank': 11, 'post_grade': 'E',
            'status': '在职', 'join_company_date': '2026-07-31',
        }
        profile = {
            'pos_id': intern_pos, 'education_level': '本科',
            'employment_stage': 'intern', 'first_employment': 1,
        }
        ok, message = add_employee(employee, profile, change_date='2026-07-31 00:00:00')
        self.assertTrue(ok, message)

        conn = sqlite3.connect(self.db_path)
        person_id = conn.execute(
            "SELECT person_id FROM employees WHERE emp_id='I002'"
        ).fetchone()[0]
        lifecycle = conn.execute(
            """
            SELECT expected_regularization_date, employment_stage
            FROM employee_profiles WHERE emp_id='I002'
            """
        ).fetchone()
        annuity_enabled = conn.execute(
            "SELECT annuity_enabled FROM ss_emp_matrix WHERE emp_id='I002'"
        ).fetchone()[0]
        conn.close()
        self.assertTrue(person_id)
        self.assertEqual(lifecycle, ('2027-01-31', 'intern'))
        self.assertEqual(annuity_enabled, 1)

        profile.update({'pos_id': regular_pos, 'employment_stage': 'regular'})
        ok, message = update_employee(
            'I002', employee, profile, reason='实习转正',
            change_date='2027-02-01 00:00:00',
        )
        self.assertTrue(ok, message)
        conn = sqlite3.connect(self.db_path)
        actual = conn.execute(
            "SELECT actual_regularization_date FROM employee_profiles WHERE emp_id='I002'"
        ).fetchone()[0]
        change_type = conn.execute(
            "SELECT change_type FROM personnel_changes WHERE emp_id='I002' ORDER BY change_id DESC LIMIT 1"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(actual, '2027-02-01')
        self.assertIn('实习转正', change_type)

    def test_effective_month_policy_and_lifecycle(self):
        params = (
            '2026-07', '省公众', 'round_to_ten', 'reverse_from_ss', 7.0,
            50000, 4000, 0.16, 0.08,
            50000, 4000, 0.07, 0.02,
            50000, 4000, 0.007, 0.003,
            50000, 4000, 0.004,
            50000, 4000, 0.007,
            50000, 4000, 0.12, 0.12,
            0.08, 0.04, 34550, 4000,
            1, 1, 'round_to_ten',
        )
        ok, message = self.social.upsert_policy_rules(params)
        self.assertTrue(ok, message)
        rules = self.social.get_policy_rules('2026-08', '省公众')
        self.assertEqual(rules['effective_from_month'], '2026-07')

        enabled, _ = self.social.get_lifecycle_participation(
            'I001', 'fund', '2026-07', rules
        )
        self.assertFalse(enabled)
        enabled, _ = self.social.get_lifecycle_participation(
            'I001', 'fund', '2026-08', rules
        )
        self.assertTrue(enabled)
        enabled, _ = self.social.get_lifecycle_participation(
            'I001', 'annuity', '2026-12', rules
        )
        self.assertFalse(enabled)

        conn = sqlite3.connect(self.db_path)
        conn.execute(
            """
            UPDATE employee_profiles
            SET employment_stage='regular', actual_regularization_date='2027-01-15'
            WHERE emp_id='I001'
            """
        )
        conn.commit()
        conn.close()
        enabled, _ = self.social.get_lifecycle_participation(
            'I001', 'annuity', '2026-12', rules
        )
        self.assertFalse(enabled)
        enabled, _ = self.social.get_lifecycle_participation(
            'I001', 'annuity', '2027-01', rules
        )
        self.assertTrue(enabled)


if __name__ == '__main__':
    unittest.main()
