import os
import sqlite3
import tempfile
import unittest

import pandas as pd

from modules.core_labor_cost import prepare_finance_labor_precheck


def finance_frame(amounts):
    names = {
        '2211010398': '应付职工薪酬.实发数',
        '6400010100': '工资.工资薪金',
        '6400040200': '社会保险费.补充医疗保险',
        '6400070000': '工会经费',
        '6400080000': '职工教育经费',
        '6602690200': '管理费用.退休人员费用.养老金补贴',
        '6602690299': '管理费用.退休人员费用.其他费用',
    }
    return pd.DataFrame([
        {
            '科目编号': account_code,
            '科目名称': names.get(account_code, account_code),
            '本期借方发生额': amount,
        }
        for account_code, amount in amounts.items()
    ])


class FinanceLaborPrecheckTest(unittest.TestCase):
    def test_union_education_tail_and_women_fee_formula(self):
        ledger = pd.DataFrame([
            {
                '核算月份': '2026-06', '工号': 'E001', '姓名': '甲',
                '归属部门': '测试部', '人员状态': '在职',
                '岗位工资': 100.01, '养老保险-个人': 10.00,
                '女工劳保费': 10.00,
            },
            {
                '核算月份': '2026-06', '工号': 'E002', '姓名': '乙',
                '归属部门': '测试部', '人员状态': '在职',
                '岗位工资': 200.02, '养老保险-个人': 20.00,
                '女工劳保费': 0.00,
            },
        ])
        finance = finance_frame({
            '6400010100': 300.03,
            '2211010398': 280.03,
            '6400070000': 6.01,
            '6400080000': 4.50,
        })

        result = prepare_finance_labor_precheck(
            ledger,
            finance,
            tail_carrier_emp_id='E002',
        )
        processed = result['processed_ledger']

        self.assertAlmostEqual(processed['工会经费'].sum(), 6.01, places=2)
        self.assertAlmostEqual(processed['职工教育经费'].sum(), 4.50, places=2)
        self.assertEqual(
            processed.loc[processed['工号'] == 'E002', '工会经费'].iloc[0],
            4.01,
        )
        self.assertEqual(
            result['business_checks'].iloc[0]['核对状态'],
            '一致',
        )
        self.assertAlmostEqual(processed['个人实发'].sum(), 280.03, places=2)
        self.assertEqual(
            result['auto_actions'].iloc[0]['尾差承接人员'],
            '乙（E002）',
        )

    def test_bottom_sheet_alone_can_calculate_funds(self):
        ledger = pd.DataFrame([
            {
                '核算月份': '2026-06', '工号': 'E001', '姓名': '普通员工',
                '归属部门': '测试部', '人员状态': '在职', '岗位工资': 0.25,
            },
            {
                '核算月份': '2026-06', '工号': 'HR001', '姓名': '人力部主任',
                '归属部门': '人力资源部', '人员状态': '在职', '岗位工资': 0.25,
            },
        ])

        result = prepare_finance_labor_precheck(
            ledger,
            tail_carrier_emp_id='HR001',
        )
        processed = result['processed_ledger']

        self.assertTrue(result['monthly_reconciliation'].empty)
        self.assertTrue(result['ytd_reconciliation'].empty)
        self.assertAlmostEqual(processed['工会经费'].sum(), 0.01, places=2)
        self.assertAlmostEqual(processed['职工教育经费'].sum(), 0.01, places=2)
        self.assertEqual(
            processed.loc[processed['工号'] == 'HR001', '工会经费'].iloc[0],
            0.00,
        )
        self.assertEqual(
            processed.loc[processed['工号'] == 'HR001', '职工教育经费'].iloc[0],
            0.01,
        )
        self.assertTrue(
            result['auto_actions']['控制数来源']
            .eq('工资应发合计×计提比例')
            .all()
        )

    def test_retirement_cost_uses_internal_mapping_without_new_columns(self):
        ledger = pd.DataFrame([
            {
                '核算月份': '2026-06', '工号': 'E001', '姓名': '在职人员',
                '归属部门': '测试部', '人员状态': '在职',
                '岗位工资': 100.00, '补充医保费': 100.00,
            },
            {
                '核算月份': '2026-06', '工号': 'R001', '姓名': '退休人员',
                '归属部门': '离退休人员', '人员状态': '退休',
                '补充医保费': 50.00,
            },
        ])
        finance = finance_frame({
            '6400010100': 100.00,
            '2211010398': 100.00,
            '6400040200': 100.00,
            '6400070000': 2.00,
            '6400080000': 1.50,
            '6602690200': 50.00,
            '6602690299': 12.34,
        })

        result = prepare_finance_labor_precheck(
            ledger,
            finance,
            tail_carrier_emp_id='E001',
        )
        processed = result['processed_ledger']

        self.assertAlmostEqual(processed['补充医保费'].sum(), 150.00, places=2)
        self.assertNotIn('退休人员养老金补贴', processed.columns)
        reconciliation = result['monthly_reconciliation'].set_index('核对项目')
        self.assertAlmostEqual(reconciliation.loc['补充医疗保险', '台账金额'], 100.00)
        self.assertAlmostEqual(reconciliation.loc['退休人员养老金补贴', '台账金额'], 50.00)
        self.assertIn('6602690299', result['pending_accounts']['科目编号'].tolist())

    def test_database_schema_keeps_original_labor_columns(self):
        from database.init_db import init_database

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, 'hr_test.db')
            init_database(db_path)
            conn = sqlite3.connect(db_path)
            columns = {
                row[1]
                for row in conn.execute('PRAGMA table_info(labor_cost_ledger)').fetchall()
            }
            audit_tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'finance_labor_%'"
                ).fetchall()
            }
            conn.close()

        self.assertTrue({
            'welfare_recuperation',
            'retiree_pension_subsidy',
            'retiree_medical_expense',
            'retiree_other_expense',
        }.isdisjoint(columns))
        self.assertEqual(
            audit_tables,
            {'finance_labor_import_batches', 'finance_labor_reconciliation'},
        )


if __name__ == '__main__':
    unittest.main()
