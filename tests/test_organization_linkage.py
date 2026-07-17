import os
import sqlite3
import tempfile
import unittest


class OrganizationLinkageTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "organization_linkage.db")
        os.environ["MAKE_HR_DB_PATH"] = self.db_path

        from database.init_db import init_database

        init_database(self.db_path)
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO departments(dept_name, dept_category, status) "
                "VALUES ('测试部门', '生产', 1)"
            )
            self.dept_id = conn.execute(
                "SELECT dept_id FROM departments WHERE dept_name='测试部门'"
            ).fetchone()[0]
            for name in ('总经理', '副总经理', '总经理助理兼安全主任', '测试岗位'):
                conn.execute(
                    "INSERT INTO positions(pos_name, pos_category, status) "
                    "VALUES (?, '测试', 1)", (name,),
                )
            self.position_ids = dict(conn.execute(
                "SELECT pos_name, pos_id FROM positions"
            ).fetchall())
            conn.execute(
                """
                INSERT INTO employees(
                    emp_id, person_id, employee_no, name, id_card, dept_id,
                    post_rank, post_grade, status, join_company_date
                ) VALUES ('E1', 'P1', '001', '测试员工', 'ID1', ?,
                          17, 'A', '在职', '2026-01-01')
                """, (self.dept_id,),
            )
            conn.execute(
                "INSERT INTO employee_profiles(emp_id, pos_id, tech_grade) "
                "VALUES ('E1', ?, 'T1')",
                (self.position_ids['测试岗位'],),
            )
            version_id = conn.execute(
                "SELECT rule_version_id FROM payroll_rule_versions LIMIT 1"
            ).fetchone()[0]
            for name in ('总经理', '副总经理', '总经理助理兼安全主任'):
                conn.execute(
                    """
                    INSERT INTO payroll_position_rule_mappings(
                        rule_version_id, pos_id, payroll_category, enabled
                    ) VALUES (?, ?, 'company_leader', 1)
                    """, (version_id, self.position_ids[name]),
                )
            conn.commit()
            self.version_id = version_id
        finally:
            conn.close()

        # 规则初建时测试岗位尚不存在，再跑一次兼容迁移以回填岗位ID。
        init_database(self.db_path)

    def tearDown(self):
        os.environ.pop("MAKE_HR_DB_PATH", None)
        self.temp_dir.cleanup()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def test_stopped_leader_position_is_not_a_current_rule_blocker(self):
        from modules.core_payroll_rules import (
            get_company_leader_rules,
            validate_rule_version,
        )
        from modules.core_position import update_position

        before = validate_rule_version(self.version_id)
        leader_check = before[before['检查项'] == '公司领导省公司标准'].iloc[0]
        self.assertEqual(leader_check['结果'], '3个岗位待填写')

        ok, message = update_position(
            self.position_ids['总经理助理兼安全主任'],
            '总经理助理兼安全主任', '测试', 999, 0,
        )
        self.assertTrue(ok, message)

        rules = get_company_leader_rules(self.version_id)
        self.assertEqual(set(rules['公司领导岗位']), {'总经理', '副总经理'})
        after = validate_rule_version(self.version_id)
        leader_check = after[after['检查项'] == '公司领导省公司标准'].iloc[0]
        self.assertEqual(leader_check['结果'], '2个岗位待填写')

        conn = self._connect()
        try:
            historical_rule = conn.execute(
                """
                SELECT COUNT(*) FROM payroll_company_leader_rules
                WHERE rule_version_id=? AND leader_position_name='总经理助理兼安全主任'
                """, (self.version_id,),
            ).fetchone()[0]
            self.assertEqual(historical_rule, 1)
        finally:
            conn.close()

    def test_position_and_department_with_active_person_cannot_be_stopped(self):
        from modules.core_dept import update_department
        from modules.core_position import update_position

        ok, message = update_position(
            self.position_ids['测试岗位'], '测试岗位', '测试', 999, 0
        )
        self.assertFalse(ok)
        self.assertIn('仍有 1 名', message)

        ok, message = update_department(
            self.dept_id, '测试部门', '生产', None, 999, 0
        )
        self.assertFalse(ok)
        self.assertIn('仍有 1 名', message)

    def test_leader_rule_survives_position_rename_by_internal_id(self):
        from modules.core_payroll_rules import (
            calculate_rule_preview,
            get_company_leader_rules,
            save_company_leader_rules,
        )
        from modules.core_position import update_position

        rules = get_company_leader_rules(self.version_id)
        rules.loc[rules['公司领导岗位'] == '总经理', '省公司绩效标准'] = 9999
        ok, message = save_company_leader_rules(self.version_id, rules)
        self.assertTrue(ok, message)
        ok, message = update_position(
            self.position_ids['总经理'], '总经理（新名称）', '测试', 999, 1
        )
        self.assertTrue(ok, message)

        renamed_rules = get_company_leader_rules(self.version_id)
        self.assertIn('总经理（新名称）', set(renamed_rules['公司领导岗位']))
        preview = calculate_rule_preview(
            self.version_id, 17, 'A', 'company_leader',
            leader_position_name='总经理（新名称）',
        )
        self.assertEqual(preview['原绩效'], 9999.0)

    def test_mapping_count_only_includes_active_people(self):
        from modules.core_payroll_rules import get_position_mappings

        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO employees(
                    emp_id, person_id, employee_no, name, id_card, dept_id,
                    post_rank, post_grade, status
                ) VALUES ('E2', 'P2', '002', '离职员工', 'ID2', ?, 17, 'A', '离职')
                """, (self.dept_id,),
            )
            conn.execute(
                "INSERT INTO employee_profiles(emp_id, pos_id) VALUES ('E2', ?)",
                (self.position_ids['测试岗位'],),
            )
            conn.execute(
                """
                INSERT INTO payroll_position_rule_mappings(
                    rule_version_id, pos_id, payroll_category, enabled
                ) VALUES (?, ?, 'unclassified', 1)
                """, (self.version_id, self.position_ids['测试岗位']),
            )
            conn.commit()
        finally:
            conn.close()

        mappings = get_position_mappings(self.version_id)
        row = mappings[mappings['系统岗位'] == '测试岗位'].iloc[0]
        self.assertEqual(int(row['当前人数']), 1)

    def test_integrity_scan_detects_forced_cross_module_error(self):
        from modules.core_personnel import get_organization_integrity_issues

        self.assertEqual(get_organization_integrity_issues(), [])
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE positions SET status=0 WHERE pos_id=?",
                (self.position_ids['测试岗位'],),
            )
            conn.commit()
        finally:
            conn.close()
        issues = get_organization_integrity_issues()
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]['issue'], '人员仍任已停用岗位')


if __name__ == '__main__':
    unittest.main()
