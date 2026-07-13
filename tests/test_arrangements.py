import os
import sqlite3
import tempfile
import unittest


class ArrangementRoutingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.temp_dir.name, "hr_test.db")

        from database.init_db import init_database

        init_database(cls.db_path)
        os.environ["MAKE_HR_DB_PATH"] = cls.db_path

        conn = sqlite3.connect(cls.db_path)
        conn.execute(
            "INSERT INTO departments(dept_name, dept_category) VALUES ('测试部门', '生产')"
        )
        dept_id = conn.execute(
            "SELECT dept_id FROM departments WHERE dept_name='测试部门'"
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO employees(emp_id, name, dept_id, status, join_company_date)
            VALUES ('E001', '测试员工', ?, '在职', '2026-01-01')
            """,
            (dept_id,),
        )
        conn.execute(
            "INSERT INTO employee_profiles(emp_id) VALUES ('E001')"
        )
        conn.execute(
            """
            INSERT INTO ss_emp_matrix(
                emp_id, cost_center, base_salary_avg,
                pension_enabled, pension_account,
                medical_enabled, medical_account,
                unemp_enabled, injury_enabled, maternity_enabled,
                fund_enabled, annuity_enabled
            ) VALUES ('E001', '本级', 5000, 1, '省公众', 0, '省公司', 0, 0, 0, 0, 0)
            """
        )
        conn.execute(
            """
            INSERT INTO ss_policy_rules(
                rule_year, manage_entity,
                pension_upper, pension_lower, pension_comp_rate, pension_pers_rate,
                rounding_mode, fund_calc_method
            ) VALUES ('2026', '省公众', 20000, 1000, 0.16, 0.08, 'exact', 'independent')
            """
        )
        conn.execute(
            """
            INSERT INTO business_entities(entity_code, entity_name, entity_type)
            VALUES ('branch:测试分公司', '测试分公司', '地市分公司')
            """
        )
        conn.commit()
        conn.close()

        from modules import core_arrangements, core_social_security

        cls.arrangements = core_arrangements
        cls.social = core_social_security

    @classmethod
    def tearDownClass(cls):
        os.environ.pop("MAKE_HR_DB_PATH", None)
        cls.temp_dir.cleanup()

    def test_effective_relation_controls_payroll_and_route(self):
        ok, message = self.arrangements.create_arrangement({
            "emp_id": "E001",
            "arrangement_type": "down_secondment",
            "contract_entity_code": "province_public",
            "payroll_entity_code": None,
            "home_dept_id": 1,
            "actual_work_unit_code": "branch:测试分公司",
            "related_branch_code": "branch:测试分公司",
            "accounting_entity_code": "province_public",
            "ultimate_cost_bearer_code": "branch:测试分公司",
            "start_date": "2026-07-01",
            "planned_end_date": "2028-06-30",
            "actual_end_date": None,
            "payroll_included": 0,
            "settlement_mode": "mixed_by_item",
            "settlement_cycle": "mixed",
            "status": "active",
            "source_document_no": "测试文件",
            "remarks": "测试",
        })
        self.assertTrue(ok, message)
        self.assertTrue(self.arrangements.is_payroll_included("E001", "2026-06"))
        self.assertFalse(self.arrangements.is_payroll_included("E001", "2026-07"))

        ok, message = self.arrangements.create_route_policy({
            "policy_name": "下沉医疗由派出单位代缴",
            "arrangement_type": "down_secondment",
            "contract_entity_code": "province_public",
            "insurance_item": "medical",
            "effective_from_month": "2026-07",
            "effective_to_month": None,
            "enabled_default": 1,
            "calculation_policy_entity": "province_public",
            "payer_entity_rule": "contract_entity",
            "payer_entity_code": None,
            "cost_bearer_rule": "ultimate_cost_bearer",
            "cost_bearer_code": None,
            "settlement_counterparty_code": "province_public",
            "settlement_mode": "annual_reimbursement",
            "settlement_cycle": "annual",
            "amount_source": "system_calculated",
            "payment_channel_code": "province_public:medical_group",
            "priority": 100,
            "active": 1,
            "remarks": "测试",
        })
        self.assertTrue(ok, message)

        route = self.arrangements.resolve_social_route(
            "E001", "medical", "2026-07",
            legacy_enabled=0,
            legacy_payer_name="省公司",
            legacy_cost_center="本级",
        )
        self.assertEqual(route["payer_entity_name"], "省公众")
        self.assertEqual(route["cost_bearer_name"], "测试分公司")
        self.assertEqual(route["settlement_mode"], "annual_reimbursement")

    def test_business_entity_dictionary_can_be_maintained(self):
        ok, message = self.arrangements.create_business_entity(
            "新增测试地市分公司", "地市分公司", "province_company"
        )
        self.assertTrue(ok, message)

        entities = self.arrangements.get_entities_dataframe(active_only=True)
        created = entities[entities["entity_name"] == "新增测试地市分公司"]
        self.assertEqual(len(created), 1)
        entity_code = created.iloc[0]["entity_code"]
        self.assertEqual(created.iloc[0]["parent_entity_name"], "省公司")

        ok, message = self.arrangements.set_business_entity_active(entity_code, False)
        self.assertTrue(ok, message)
        active_entities = self.arrangements.get_entities_dataframe(active_only=True)
        self.assertNotIn("新增测试地市分公司", active_entities["entity_name"].tolist())

        ok, message = self.arrangements.create_business_entity(
            "新增测试地市分公司", "地市分公司", "province_company"
        )
        self.assertTrue(ok, message)
        self.assertIn("重新启用", message)

    def test_monthly_save_dual_writes_and_respects_close(self):
        roster = {
            "工号": "E001", "姓名": "测试员工", "财务归属": "本级",
            "已录入原始基数": 5000.0, "独立公积金基数(选填)": 0.0,
            "养老参保(1是0否)": 1, "养老缴纳主体": "省公众",
            "医疗参保(1是0否)": 0, "医疗缴纳主体": "省公司",
            "失业参保(1是0否)": 0, "失业缴纳主体": "省公众",
            "工伤参保(1是0否)": 0, "工伤缴纳主体": "省公众",
            "生育参保(1是0否)": 0, "生育缴纳主体": "省公司",
            "公积金参保(1是0否)": 0, "公积金缴纳主体": "省公众",
            "年金参保(1是0否)": 0, "年金缴纳主体": "省公司",
        }
        bill = self.social.calculate_complete_bill(roster, "2026", "2026-06")
        self.assertEqual(bill["pension_企"], 800.0)
        self.assertEqual(bill["pension_个"], 400.0)

        import pandas as pd

        ok, message = self.social.save_monthly_ss_records(pd.DataFrame([bill]), "2026-06")
        self.assertTrue(ok, message)
        conn = sqlite3.connect(self.db_path)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM ss_monthly_records WHERE cost_month='2026-06'").fetchone()[0],
            1,
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM social_monthly_items WHERE cost_month='2026-06'").fetchone()[0],
            8,
        )
        conn.execute(
            "UPDATE ss_monthly_records SET close_status='closed' WHERE cost_month='2026-06'"
        )
        conn.commit()
        conn.close()

        ok, _ = self.social.save_monthly_ss_records(pd.DataFrame([bill]), "2026-06")
        self.assertFalse(ok)

    def test_labor_cost_report_codes_are_localized(self):
        import pandas as pd

        from modules.core_labor_cost import localize_labor_cost_codes

        report = localize_labor_cost_codes(pd.DataFrame([{
            "业务关系类型": "city_transfer",
            "划转方式": "annual_labor_cost_reallocation",
            "划转状态": "pending",
        }]))

        self.assertEqual(report.loc[0, "业务关系类型"], "地市工作转入")
        self.assertEqual(report.loc[0, "划转方式"], "年度全口径人工成本划转")
        self.assertEqual(report.loc[0, "划转状态"], "待划转")


if __name__ == "__main__":
    unittest.main()
