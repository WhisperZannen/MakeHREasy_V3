import os
import sqlite3
import tempfile
import unittest


class EmployeeIdentityTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, 'hr_identity_test.db')
        os.environ['MAKE_HR_DB_PATH'] = self.db_path

        from database.init_db import init_database

        init_database(self.db_path)
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO departments(dept_name, dept_category) VALUES ('测试部门', '生产')"
        )
        self.dept_id = conn.execute(
            "SELECT dept_id FROM departments WHERE dept_name='测试部门'"
        ).fetchone()[0]
        conn.execute("INSERT INTO positions(pos_name) VALUES ('测试岗位')")
        self.pos_id = conn.execute(
            "SELECT pos_id FROM positions WHERE pos_name='测试岗位'"
        ).fetchone()[0]
        conn.commit()
        conn.close()

    def tearDown(self):
        os.environ.pop('MAKE_HR_DB_PATH', None)
        self.temp_dir.cleanup()

    def _employee(self, name, employee_no=None, id_card=None):
        return {
            'employee_no': employee_no,
            'name': name,
            'id_card': id_card,
            'dept_id': self.dept_id,
            'post_rank': 11,
            'post_grade': 'A',
            'status': '在职',
            'join_company_date': '2026-07-15',
        }

    def _profile(self):
        return {
            'pos_id': self.pos_id,
            'tech_grade': 'T1',
            'employment_stage': 'regular',
        }

    def test_pending_employee_number_can_be_added_then_changed_without_broken_links(self):
        from database.init_db import init_database
        from modules.core_identity import resolve_employee_reference
        from modules.core_personnel import add_employee, update_employee

        ok, message = add_employee(
            self._employee('待发工号人员', id_card='420100200001010011'),
            self._profile(),
        )
        self.assertTrue(ok, message)

        conn = sqlite3.connect(self.db_path)
        internal_id, employee_no, person_id = conn.execute(
            "SELECT emp_id, employee_no, person_id FROM employees WHERE name='待发工号人员'"
        ).fetchone()
        matrix_emp_id = conn.execute(
            "SELECT emp_id FROM ss_emp_matrix WHERE emp_id = ?", (internal_id,)
        ).fetchone()[0]
        conn.close()

        self.assertTrue(internal_id.startswith('P-'))
        self.assertIsNone(employee_no)
        self.assertTrue(person_id)
        self.assertEqual(matrix_emp_id, internal_id)

        updated = self._employee(
            '待发工号人员', employee_no='42019999', id_card='420100200001010011'
        )
        ok, message = update_employee(internal_id, updated, self._profile())
        self.assertTrue(ok, message)

        conn = sqlite3.connect(self.db_path)
        saved = conn.execute(
            "SELECT emp_id, employee_no FROM employees WHERE name='待发工号人员'"
        ).fetchone()
        matrix_count = conn.execute(
            "SELECT COUNT(*) FROM ss_emp_matrix WHERE emp_id = ?", (internal_id,)
        ).fetchone()[0]
        conn.close()

        self.assertEqual(saved, (internal_id, '42019999'))
        self.assertEqual(matrix_count, 1)
        self.assertEqual(
            resolve_employee_reference(employee_no='42019999'), internal_id
        )

        # 再次启动数据库初始化也不能把“待分配工号”误填成隐藏内部键。
        ok, message = add_employee(
            self._employee('另一位待发工号人员', id_card='420100200001010022'),
            self._profile(),
        )
        self.assertTrue(ok, message)
        init_database(self.db_path)
        conn = sqlite3.connect(self.db_path)
        pending_no = conn.execute(
            "SELECT employee_no FROM employees WHERE name='另一位待发工号人员'"
        ).fetchone()[0]
        conn.close()
        self.assertIsNone(pending_no)

    def test_employee_number_remains_unique(self):
        from modules.core_personnel import add_employee

        ok, message = add_employee(
            self._employee('甲', employee_no='42018888'), self._profile()
        )
        self.assertTrue(ok, message)

        ok, message = add_employee(
            self._employee('乙', employee_no='42018888'), self._profile()
        )
        self.assertFalse(ok)
        self.assertIn('已被其他人员使用', message)


if __name__ == '__main__':
    unittest.main()
