import os
import tempfile
import unittest


class PayrollRuleDictionaryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.temp_dir.name, 'payroll_rules_test.db')
        os.environ['MAKE_HR_DB_PATH'] = cls.db_path

        from database.init_db import init_database

        init_database(cls.db_path)
        from modules import core_payroll_rules

        cls.rules = core_payroll_rules

    @classmethod
    def tearDownClass(cls):
        os.environ.pop('MAKE_HR_DB_PATH', None)
        cls.temp_dir.cleanup()

    def test_seeded_rule_tables_match_confirmed_formula(self):
        versions = self.rules.get_rule_versions()
        base_version = versions[versions['rule_name'] == '2026年薪酬基础规则'].iloc[0]
        version_id = int(base_version['rule_version_id'])
        self.assertEqual(float(base_version['original_perf_base']), 1500.0)
        self.assertEqual(float(base_version['incentive_base']), 3000.0)

        matrix = self.rules.get_salary_matrix(version_id)
        self.assertEqual(matrix.shape, (28, 11))
        self.assertEqual(
            float(matrix.loc[matrix['岗级'] == 17, 'A'].iloc[0]), 2750.0
        )

        original = self.rules.get_original_perf_rules(version_id)
        rank_16 = original[original['岗级'] == 16].iloc[0]
        self.assertEqual(rank_16['管理正职系数'], 2.9)
        self.assertEqual(rank_16['管理副职系数'], 2.4)

    def test_professional_and_management_preview(self):
        versions = self.rules.get_rule_versions()
        version_id = int(
            versions[versions['rule_name'] == '2026年薪酬基础规则']
            .iloc[0]['rule_version_id']
        )

        professional = self.rules.calculate_rule_preview(
            version_id, 17, 'A', 'professional',
            official_position_name='AI研发工程师', tech_grade='T3',
        )
        self.assertEqual(professional['岗位工资'], 2750.0)
        self.assertEqual(professional['原绩效'], 3450.0)
        self.assertEqual(professional['激励包'], 4800.0)
        self.assertEqual(professional['绩效基数'], 8250.0)
        self.assertEqual(professional['问题'], [])

        director = self.rules.calculate_rule_preview(
            version_id, 16, 'B', 'management',
            management_role='management_director',
        )
        self.assertEqual(director['岗位工资'], 2590.0)
        self.assertEqual(director['原绩效'], 4350.0)
        self.assertEqual(director['激励包'], 7500.0)
        self.assertEqual(director['绩效基数'], 11850.0)

        deputy = self.rules.calculate_rule_preview(
            version_id, 14, 'C', 'management',
            management_role='management_deputy',
        )
        self.assertEqual(deputy['原绩效'], 3600.0)
        self.assertEqual(deputy['激励包'], 6000.0)
        self.assertEqual(deputy['绩效基数'], 9600.0)

        advisor = self.rules.calculate_rule_preview(
            version_id, 20, 'A', 'management',
            management_role='senior_advisor',
        )
        self.assertEqual(advisor['原绩效'], 3480.0)
        self.assertEqual(advisor['激励包'], 6000.0)
        self.assertEqual(advisor['绩效基数'], 9480.0)

    def test_copying_version_keeps_rules_but_not_activation(self):
        versions = self.rules.get_rule_versions()
        source_id = int(
            versions[versions['rule_name'] == '2026年薪酬基础规则']
            .iloc[0]['rule_version_id']
        )
        ok, message = self.rules.copy_rule_version(
            source_id, '测试复制版本', '2027-01'
        )
        self.assertTrue(ok, message)
        copied = self.rules.get_rule_versions()
        copied_row = copied[copied['rule_name'] == '测试复制版本'].iloc[0]
        self.assertEqual(copied_row['status'], 'draft')
        copied_matrix = self.rules.get_salary_matrix(
            int(copied_row['rule_version_id'])
        )
        self.assertEqual(copied_matrix.shape, (28, 11))


if __name__ == '__main__':
    unittest.main()
