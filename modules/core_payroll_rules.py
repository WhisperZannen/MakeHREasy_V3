"""薪酬总阀门：规则版本、字典维护、校验与试算。"""

import os
import sqlite3

import pandas as pd


CATEGORY_LABELS = {
    'unclassified': '待归类',
    'company_leader': '公司领导',
    'management': '管理岗位',
    'professional': '专业岗位',
}
CATEGORY_CODES = {label: code for code, label in CATEGORY_LABELS.items()}

ROLE_LABELS = {
    'management_director': '管理正职（主任/总监）',
    'management_deputy': '管理副职（副主任/副总监）',
    'management_assistant': '主任助理',
    'senior_advisor': '高级顾问（正职80%）',
}
ROLE_CODES = {label: code for code, label in ROLE_LABELS.items()}

ORIGINAL_CATEGORY_LABELS = {
    'professional': '专业岗位系数',
    'management_director': '管理正职系数',
    'management_deputy': '管理副职系数',
}
ORIGINAL_LABEL_CODES = {
    label: code for code, label in ORIGINAL_CATEGORY_LABELS.items()
}

RULE_STATUS_LABELS = {
    'draft': '草稿',
    'active': '已启用',
    'retired': '已停用',
}

IDENTITY_TYPE_LABELS = {
    'talent': '优才',
    'technical_elite': '技术精英',
    'province_expert': '省公司专家',
}
IDENTITY_LEVEL_LABELS = {
    ('talent', 'group'): '集团优才',
    ('talent', 'province'): '省级优才',
    ('technical_elite', 'elite'): '技术精英',
    ('technical_elite', 'chief'): '首席技术精英',
    ('province_expert', 'level_1'): '一级专家',
    ('province_expert', 'level_2'): '二级专家',
}


def _get_db_connection():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.environ.get(
        'MAKE_HR_DB_PATH', os.path.join(project_root, 'database', 'hr_core.db')
    )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def _number(value, allow_none=False):
    if value is None or pd.isna(value) or str(value).strip() == '':
        return None if allow_none else 0.0
    return float(value)


def get_rule_versions():
    conn = _get_db_connection()
    try:
        return pd.read_sql_query(
            '''
            SELECT * FROM payroll_rule_versions
            ORDER BY effective_from_month DESC, rule_version_id DESC
            ''',
            conn,
        )
    finally:
        conn.close()


def get_identity_rules(version_id):
    conn = _get_db_connection()
    try:
        df = pd.read_sql_query(
            '''
            SELECT identity_rule_id AS 规则ID, identity_type, identity_level,
                   calculation_mode,
                   performance_multiplier AS 绩效倍数,
                   annual_allowance AS 年度津贴,
                   monthly_share AS 月度发放比例,
                   annual_share AS 年度考评比例,
                   enabled AS 启用,
                   COALESCE(remarks, '') AS 说明
            FROM payroll_identity_rules
            WHERE rule_version_id=?
            ORDER BY identity_type, identity_level
            ''', conn, params=[int(version_id)],
        )
        if not df.empty:
            df['身份规则'] = df.apply(
                lambda row: IDENTITY_LEVEL_LABELS.get(
                    (row['identity_type'], row['identity_level']),
                    f"{row['identity_type']}:{row['identity_level']}",
                ), axis=1,
            )
        return df
    finally:
        conn.close()


def save_identity_rules(version_id, dataframe):
    conn = _get_db_connection()
    try:
        for _, row in dataframe.iterrows():
            conn.execute(
                '''
                UPDATE payroll_identity_rules
                SET performance_multiplier=?, annual_allowance=?,
                    monthly_share=?, annual_share=?, enabled=?, remarks=?
                WHERE identity_rule_id=? AND rule_version_id=?
                ''',
                (
                    _number(row.get('绩效倍数'), allow_none=True),
                    _number(row.get('年度津贴'), allow_none=True),
                    _number(row.get('月度发放比例'), allow_none=True),
                    _number(row.get('年度考评比例'), allow_none=True),
                    1 if bool(row.get('启用', True)) else 0,
                    str(row.get('说明') or '').strip(),
                    int(row['规则ID']), int(version_id),
                ),
            )
        conn.commit()
        return True, '人才身份待遇规则已保存'
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def update_rule_version(version_id, rule_name, effective_from_month,
                        original_perf_base, incentive_base, remarks=''):
    conn = _get_db_connection()
    try:
        if not str(rule_name).strip():
            return False, '规则名称不能为空'
        if len(str(effective_from_month).strip()) != 7:
            return False, '生效月份必须使用YYYY-MM格式'
        conn.execute(
            '''
            UPDATE payroll_rule_versions
            SET rule_name = ?, effective_from_month = ?,
                original_perf_base = ?, incentive_base = ?, remarks = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE rule_version_id = ?
            ''',
            (
                str(rule_name).strip(), str(effective_from_month).strip(),
                _number(original_perf_base), _number(incentive_base),
                str(remarks or '').strip(), int(version_id),
            ),
        )
        conn.commit()
        return True, '基础参数已保存'
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def activate_rule_version(version_id):
    checks = validate_rule_version(version_id)
    failed = checks[~checks['通过']]
    if not failed.empty:
        return False, '仍有未通过的规则检查，不能启用'
    conn = _get_db_connection()
    try:
        target = conn.execute(
            'SELECT effective_from_month FROM payroll_rule_versions WHERE rule_version_id = ?',
            (int(version_id),),
        ).fetchone()
        if not target:
            return False, '规则版本不存在'
        conn.execute(
            "UPDATE payroll_rule_versions SET status = 'retired' WHERE status = 'active'"
        )
        conn.execute(
            '''
            UPDATE payroll_rule_versions
            SET status = 'active', updated_at = CURRENT_TIMESTAMP
            WHERE rule_version_id = ?
            ''', (int(version_id),),
        )
        conn.commit()
        return True, '规则版本已启用'
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def copy_rule_version(source_version_id, rule_name, effective_from_month):
    conn = _get_db_connection()
    try:
        source = conn.execute(
            'SELECT * FROM payroll_rule_versions WHERE rule_version_id = ?',
            (int(source_version_id),),
        ).fetchone()
        if not source:
            return False, '源规则版本不存在'
        cursor = conn.execute(
            '''
            INSERT INTO payroll_rule_versions(
                rule_name, effective_from_month, status,
                original_perf_base, incentive_base, remarks
            ) VALUES (?, ?, 'draft', ?, ?, ?)
            ''',
            (
                str(rule_name).strip(), str(effective_from_month).strip(),
                source['original_perf_base'], source['incentive_base'],
                f"复制自版本{source_version_id}",
            ),
        )
        target_id = cursor.lastrowid
        copy_specs = {
            'payroll_salary_matrix_rules': (
                'post_rank, post_grade, amount',
                'post_rank, post_grade, amount',
            ),
            'payroll_original_perf_rules': (
                'employee_category, post_rank, coefficient',
                'employee_category, post_rank, coefficient',
            ),
            'payroll_management_incentive_rules': (
                'management_role, coefficient',
                'management_role, coefficient',
            ),
            'payroll_derived_management_rules': (
                'special_role, base_management_role, multiplier, remarks',
                'special_role, base_management_role, multiplier, remarks',
            ),
            'payroll_position_value_rules': (
                'official_position_name, base_coefficient, t1_coefficient, '
                't2_coefficient, t3_coefficient, t4_coefficient, t5_coefficient, '
                'source_order, remarks',
                'official_position_name, base_coefficient, t1_coefficient, '
                't2_coefficient, t3_coefficient, t4_coefficient, t5_coefficient, '
                'source_order, remarks',
            ),
            'payroll_company_leader_rules': (
                'pos_id, leader_position_name, standard_amount, remarks',
                'pos_id, leader_position_name, standard_amount, remarks',
            ),
            'payroll_position_rule_mappings': (
                'pos_id, payroll_category, management_role, official_position_name, '
                'enabled, remarks',
                'pos_id, payroll_category, management_role, official_position_name, '
                'enabled, remarks',
            ),
            'payroll_person_calculation_overrides': (
                'emp_id, calculation_mode, counterparty_name, remarks, enabled',
                'emp_id, calculation_mode, counterparty_name, remarks, enabled',
            ),
            'payroll_identity_rules': (
                'identity_type, identity_level, calculation_mode, '
                'performance_multiplier, annual_allowance, monthly_share, '
                'annual_share, parameters_json, enabled, remarks',
                'identity_type, identity_level, calculation_mode, '
                'performance_multiplier, annual_allowance, monthly_share, '
                'annual_share, parameters_json, enabled, remarks',
            ),
        }
        for table, (target_columns, source_columns) in copy_specs.items():
            conn.execute(
                f'''
                INSERT INTO {table}(rule_version_id, {target_columns})
                SELECT ?, {source_columns} FROM {table}
                WHERE rule_version_id = ?
                ''',
                (target_id, int(source_version_id)),
            )
        conn.commit()
        return True, f'已建立新草稿版本：{rule_name}'
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def get_salary_matrix(version_id):
    conn = _get_db_connection()
    try:
        rows = pd.read_sql_query(
            '''
            SELECT post_rank AS 岗级, post_grade AS 档次, amount AS 金额
            FROM payroll_salary_matrix_rules
            WHERE rule_version_id = ?
            ORDER BY post_rank DESC, post_grade
            ''',
            conn, params=[int(version_id)],
        )
        if rows.empty:
            return pd.DataFrame(columns=['岗级'] + list('ABCDEFGHIJ'))
        return (
            rows.pivot(index='岗级', columns='档次', values='金额')
            .reindex(columns=list('ABCDEFGHIJ')).reset_index()
            .sort_values('岗级', ascending=False)
        )
    finally:
        conn.close()


def save_salary_matrix(version_id, dataframe):
    conn = _get_db_connection()
    try:
        rows = []
        for _, row in dataframe.iterrows():
            rank = int(float(row['岗级']))
            for grade in 'ABCDEFGHIJ':
                amount = _number(row.get(grade), allow_none=True)
                if amount is not None:
                    rows.append((int(version_id), rank, grade, amount))
        if len(rows) != 280:
            return False, f'岗位工资表应有280个金额，当前为{len(rows)}个'
        conn.execute(
            'DELETE FROM payroll_salary_matrix_rules WHERE rule_version_id = ?',
            (int(version_id),),
        )
        conn.executemany(
            '''
            INSERT INTO payroll_salary_matrix_rules(
                rule_version_id, post_rank, post_grade, amount
            ) VALUES (?, ?, ?, ?)
            ''', rows,
        )
        conn.commit()
        return True, '岗位工资表已保存'
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def get_original_perf_rules(version_id):
    conn = _get_db_connection()
    try:
        rows = pd.read_sql_query(
            '''
            SELECT post_rank AS 岗级, employee_category, coefficient
            FROM payroll_original_perf_rules
            WHERE rule_version_id = ?
            ''',
            conn, params=[int(version_id)],
        )
        ranks = pd.DataFrame({'岗级': list(range(28, 0, -1))})
        if rows.empty:
            for label in ORIGINAL_CATEGORY_LABELS.values():
                ranks[label] = None
            return ranks
        pivot = rows.pivot(index='岗级', columns='employee_category', values='coefficient')
        pivot = pivot.rename(columns=ORIGINAL_CATEGORY_LABELS).reset_index()
        return ranks.merge(pivot, on='岗级', how='left')
    finally:
        conn.close()


def save_original_perf_rules(version_id, dataframe):
    conn = _get_db_connection()
    try:
        rows = []
        for _, row in dataframe.iterrows():
            rank = int(float(row['岗级']))
            for label, category in ORIGINAL_LABEL_CODES.items():
                value = _number(row.get(label), allow_none=True)
                if value is not None:
                    rows.append((int(version_id), category, rank, value))
        conn.execute(
            'DELETE FROM payroll_original_perf_rules WHERE rule_version_id = ?',
            (int(version_id),),
        )
        conn.executemany(
            '''
            INSERT INTO payroll_original_perf_rules(
                rule_version_id, employee_category, post_rank, coefficient
            ) VALUES (?, ?, ?, ?)
            ''', rows,
        )
        conn.commit()
        return True, '原绩效系数已保存'
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def get_management_incentive_rules(version_id):
    conn = _get_db_connection()
    try:
        df = pd.read_sql_query(
            '''
            SELECT management_role, coefficient AS 激励包系数
            FROM payroll_management_incentive_rules
            WHERE rule_version_id = ?
            ''', conn, params=[int(version_id)],
        )
        df['管理角色'] = df['management_role'].map(ROLE_LABELS)
        return df[['management_role', '管理角色', '激励包系数']]
    finally:
        conn.close()


def save_management_incentive_rules(version_id, dataframe):
    conn = _get_db_connection()
    try:
        rows = [
            (int(version_id), str(row['management_role']), _number(row['激励包系数']))
            for _, row in dataframe.iterrows()
        ]
        conn.execute(
            'DELETE FROM payroll_management_incentive_rules WHERE rule_version_id = ?',
            (int(version_id),),
        )
        conn.executemany(
            '''
            INSERT INTO payroll_management_incentive_rules(
                rule_version_id, management_role, coefficient
            ) VALUES (?, ?, ?)
            ''', rows,
        )
        conn.commit()
        return True, '管理岗位激励包系数已保存'
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def get_position_value_rules(version_id):
    conn = _get_db_connection()
    try:
        return pd.read_sql_query(
            '''
            SELECT official_position_name AS 文件岗位,
                   base_coefficient AS 基准系数,
                   t1_coefficient AS T1, t2_coefficient AS T2,
                   t3_coefficient AS T3, t4_coefficient AS T4,
                   t5_coefficient AS T5, source_order AS 文件序号,
                   COALESCE(remarks, '') AS 备注
            FROM payroll_position_value_rules
            WHERE rule_version_id = ?
            ORDER BY source_order
            ''', conn, params=[int(version_id)],
        )
    finally:
        conn.close()


def save_position_value_rules(version_id, dataframe):
    conn = _get_db_connection()
    try:
        rows = []
        for index, row in dataframe.iterrows():
            name = str(row.get('文件岗位') or '').strip()
            if not name:
                continue
            rows.append((
                int(version_id), name,
                _number(row.get('基准系数'), allow_none=True),
                _number(row.get('T1'), allow_none=True),
                _number(row.get('T2'), allow_none=True),
                _number(row.get('T3'), allow_none=True),
                _number(row.get('T4'), allow_none=True),
                _number(row.get('T5'), allow_none=True),
                int(_number(row.get('文件序号')) or index + 1),
                str(row.get('备注') or '').strip(),
            ))
        conn.execute(
            'DELETE FROM payroll_position_value_rules WHERE rule_version_id = ?',
            (int(version_id),),
        )
        conn.executemany(
            '''
            INSERT INTO payroll_position_value_rules(
                rule_version_id, official_position_name, base_coefficient,
                t1_coefficient, t2_coefficient, t3_coefficient,
                t4_coefficient, t5_coefficient, source_order, remarks
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', rows,
        )
        conn.commit()
        return True, '专业岗位价值系数已保存'
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def get_company_leader_rules(version_id):
    conn = _get_db_connection()
    try:
        return pd.read_sql_query(
            '''
            SELECT p.pos_id AS 岗位ID, p.pos_name AS 公司领导岗位,
                   r.standard_amount AS 省公司绩效标准,
                   COALESCE(r.remarks, '') AS 备注,
                   COUNT(CASE WHEN e.status = '在职' THEN 1 END) AS 当前在职人数
            FROM positions p
            JOIN payroll_position_rule_mappings m
              ON m.pos_id=p.pos_id AND m.rule_version_id=?
             AND m.payroll_category='company_leader' AND m.enabled=1
            LEFT JOIN payroll_company_leader_rules r
              ON r.rule_version_id=m.rule_version_id
             AND (r.pos_id=p.pos_id OR (r.pos_id IS NULL AND p.pos_name=r.leader_position_name))
            LEFT JOIN employee_profiles ep ON ep.pos_id = p.pos_id
            LEFT JOIN employees e ON e.emp_id = ep.emp_id
            WHERE p.status = 1
            GROUP BY p.pos_id, p.pos_name, r.standard_amount, r.remarks
            ORDER BY p.sort_order, p.pos_id
            ''', conn, params=[int(version_id)],
        )
    finally:
        conn.close()


def save_company_leader_rules(version_id, dataframe):
    conn = _get_db_connection()
    try:
        for _, row in dataframe.iterrows():
            name = str(row.get('公司领导岗位') or '').strip()
            pos_id = row.get('岗位ID')
            if not name or pos_id is None or pd.isna(pos_id):
                continue
            amount = _number(row.get('省公司绩效标准'), allow_none=True)
            remarks = str(row.get('备注') or '').strip()
            existing = conn.execute(
                '''
                SELECT leader_position_name
                FROM payroll_company_leader_rules
                WHERE rule_version_id=? AND pos_id=?
                ''', (int(version_id), int(pos_id)),
            ).fetchone()
            if existing:
                conn.execute(
                    '''
                    UPDATE payroll_company_leader_rules
                    SET leader_position_name=?, standard_amount=?, remarks=?
                    WHERE rule_version_id=? AND pos_id=?
                    ''', (name, amount, remarks, int(version_id), int(pos_id)),
                )
            else:
                conn.execute(
                    '''
                    INSERT INTO payroll_company_leader_rules(
                        rule_version_id, pos_id, leader_position_name,
                        standard_amount, remarks
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(rule_version_id, leader_position_name)
                    DO UPDATE SET pos_id=excluded.pos_id,
                                  standard_amount=excluded.standard_amount,
                                  remarks=excluded.remarks
                    ''', (int(version_id), int(pos_id), name, amount, remarks),
                )
        conn.commit()
        return True, '公司领导绩效标准已保存'
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def get_position_mappings(version_id):
    conn = _get_db_connection()
    try:
        df = pd.read_sql_query(
            '''
            SELECT p.pos_id AS 岗位ID, p.pos_name AS 系统岗位,
                   COALESCE(m.payroll_category, 'unclassified') AS payroll_category,
                   m.management_role,
                   m.official_position_name AS 对应文件岗位,
                   COALESCE(m.enabled, 1) AS enabled,
                   COUNT(CASE WHEN e.status = '在职' THEN 1 END) AS 当前人数
            FROM positions p
            LEFT JOIN payroll_position_rule_mappings m
              ON p.pos_id = m.pos_id AND m.rule_version_id = ?
            LEFT JOIN employee_profiles ep ON p.pos_id = ep.pos_id
            LEFT JOIN employees e ON e.emp_id = ep.emp_id
            WHERE p.status = 1
            GROUP BY p.pos_id, p.pos_name, m.payroll_category,
                     m.management_role, m.official_position_name, m.enabled
            ORDER BY p.sort_order, p.pos_id
            ''', conn, params=[int(version_id)],
        )
        df['薪酬分类'] = df['payroll_category'].map(CATEGORY_LABELS).fillna('待归类')
        df['管理角色'] = df['management_role'].map(ROLE_LABELS).fillna('')
        df['启用'] = df['enabled'].astype(int).eq(1)
        return df
    finally:
        conn.close()


def get_person_calculation_overrides(version_id):
    conn = _get_db_connection()
    try:
        return pd.read_sql_query(
            '''
            SELECT o.override_id AS 例外ID,
                   COALESCE(e.employee_no, '待分配') AS 工号,
                   e.name AS 姓名,
                   CASE o.calculation_mode
                     WHEN 'external_notice' THEN '外部函件核定'
                     ELSE o.calculation_mode
                   END AS 核定方式,
                   COALESCE(o.counterparty_name, '') AS 来函单位,
                   COALESCE(o.remarks, '') AS 说明,
                   o.enabled AS 启用
            FROM payroll_person_calculation_overrides o
            JOIN employees e ON o.emp_id = e.emp_id
            WHERE o.rule_version_id = ?
            ORDER BY e.name
            ''', conn, params=[int(version_id)],
        )
    finally:
        conn.close()


def save_position_mappings(version_id, dataframe):
    conn = _get_db_connection()
    try:
        rows = []
        for _, row in dataframe.iterrows():
            category = CATEGORY_CODES.get(str(row.get('薪酬分类')), 'unclassified')
            role = ROLE_CODES.get(str(row.get('管理角色'))) or None
            official = str(row.get('对应文件岗位') or '').strip() or None
            if category != 'management':
                role = None
            if category != 'professional':
                official = None
            rows.append((
                int(version_id), int(row['岗位ID']), category, role, official,
                1 if bool(row.get('启用', True)) else 0,
            ))
        conn.executemany(
            '''
            INSERT INTO payroll_position_rule_mappings(
                rule_version_id, pos_id, payroll_category,
                management_role, official_position_name, enabled
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(rule_version_id, pos_id) DO UPDATE SET
                payroll_category = excluded.payroll_category,
                management_role = excluded.management_role,
                official_position_name = excluded.official_position_name,
                enabled = excluded.enabled
            ''', rows,
        )
        conn.commit()
        return True, '岗位薪酬归类已保存'
    except Exception as exc:
        conn.rollback()
        return False, str(exc)
    finally:
        conn.close()


def validate_rule_version(version_id):
    conn = _get_db_connection()
    try:
        checks = []
        salary_cells = conn.execute(
            'SELECT COUNT(*) FROM payroll_salary_matrix_rules WHERE rule_version_id = ?',
            (int(version_id),),
        ).fetchone()[0]
        checks.append(('岗位工资表', salary_cells == 280, f'{salary_cells}/280个金额'))

        for category, label, expected in (
            ('management_director', '管理正职原绩效', 28),
            ('management_deputy', '管理副职原绩效', 28),
            ('professional', '专业岗位原绩效', 10),
        ):
            count = conn.execute(
                '''SELECT COUNT(*) FROM payroll_original_perf_rules
                   WHERE rule_version_id = ? AND employee_category = ?''',
                (int(version_id), category),
            ).fetchone()[0]
            checks.append((label, count == expected, f'{count}/{expected}个岗级'))

        active_unclassified = conn.execute(
            '''
            SELECT COUNT(DISTINCT ep.emp_id)
            FROM employee_profiles ep
            JOIN employees e ON ep.emp_id = e.emp_id AND e.status = '在职'
            LEFT JOIN payroll_position_rule_mappings m
              ON ep.pos_id = m.pos_id AND m.rule_version_id = ?
            LEFT JOIN payroll_person_calculation_overrides o
              ON e.emp_id = o.emp_id AND o.rule_version_id = ? AND o.enabled = 1
            WHERE COALESCE(m.payroll_category, 'unclassified') = 'unclassified'
              AND o.override_id IS NULL
            ''', (int(version_id), int(version_id)),
        ).fetchone()[0]
        checks.append((
            '在职人员岗位归类', active_unclassified == 0,
            f'{active_unclassified}人在未归类岗位',
        ))

        invalid_assignments = conn.execute(
            '''
            SELECT COUNT(DISTINCT e.emp_id)
            FROM employees e
            LEFT JOIN departments d ON d.dept_id=e.dept_id
            LEFT JOIN employee_profiles ep ON ep.emp_id=e.emp_id
            LEFT JOIN positions p ON p.pos_id=ep.pos_id
            WHERE e.status IN ('在职', '挂靠人员')
              AND (d.dept_id IS NULL OR d.status<>1 OR p.pos_id IS NULL OR p.status<>1)
            ''',
        ).fetchone()[0]
        checks.append((
            '当前人员组织有效性', invalid_assignments == 0,
            '全部有效' if invalid_assignments == 0 else f'{invalid_assignments}人存在停用或缺失的部门岗位',
        ))

        missing_leaders = conn.execute(
            '''
            SELECT COUNT(*)
            FROM positions p
            JOIN payroll_position_rule_mappings m
              ON m.pos_id=p.pos_id AND m.rule_version_id=?
             AND m.payroll_category='company_leader' AND m.enabled=1
            LEFT JOIN payroll_company_leader_rules r
              ON r.rule_version_id=m.rule_version_id
             AND (r.pos_id=p.pos_id OR (r.pos_id IS NULL AND r.leader_position_name=p.pos_name))
            WHERE p.status=1 AND r.standard_amount IS NULL
            ''', (int(version_id),),
        ).fetchone()[0]
        checks.append((
            '公司领导省公司标准', missing_leaders == 0,
            f'{missing_leaders}个岗位待填写',
        ))
        return pd.DataFrame(checks, columns=['检查项', '通过', '结果'])
    finally:
        conn.close()


def calculate_rule_preview(version_id, post_rank, post_grade, payroll_category,
                           management_role=None, official_position_name=None,
                           tech_grade=None, leader_position_name=None):
    conn = _get_db_connection()
    try:
        version = conn.execute(
            'SELECT * FROM payroll_rule_versions WHERE rule_version_id = ?',
            (int(version_id),),
        ).fetchone()
        if not version:
            raise ValueError('规则版本不存在')
        rank = int(float(post_rank))
        grade = str(post_grade).strip().upper()
        salary_row = conn.execute(
            '''
            SELECT amount FROM payroll_salary_matrix_rules
            WHERE rule_version_id = ? AND post_rank = ? AND post_grade = ?
            ''', (int(version_id), rank, grade),
        ).fetchone()
        position_salary = float(salary_row[0]) if salary_row else None
        issues = []
        original_coefficient = None
        original_performance = None
        incentive_coefficient = None
        incentive_amount = None

        if payroll_category == 'company_leader':
            row = conn.execute(
                '''
                SELECT r.standard_amount
                FROM payroll_company_leader_rules r
                LEFT JOIN positions p ON p.pos_id=r.pos_id
                WHERE r.rule_version_id = ?
                  AND (p.pos_name = ? OR r.leader_position_name = ?)
                LIMIT 1
                ''', (
                    int(version_id), str(leader_position_name or ''),
                    str(leader_position_name or ''),
                ),
            ).fetchone()
            original_performance = float(row[0]) if row and row[0] is not None else None
            if original_performance is None:
                issues.append('公司领导省公司绩效标准尚未填写')
        elif payroll_category == 'management':
            derived_multiplier = 1.0
            base_management_role = management_role
            if management_role == 'senior_advisor':
                derived = conn.execute(
                    '''
                    SELECT base_management_role, multiplier
                    FROM payroll_derived_management_rules
                    WHERE rule_version_id = ? AND special_role = ?
                    ''', (int(version_id), management_role),
                ).fetchone()
                if derived:
                    base_management_role = derived['base_management_role']
                    derived_multiplier = float(derived['multiplier'])
                else:
                    issues.append('高级顾问派生规则尚未配置')
            if base_management_role in {'management_director', 'management_deputy'}:
                row = conn.execute(
                    '''
                    SELECT coefficient FROM payroll_original_perf_rules
                    WHERE rule_version_id = ? AND employee_category = ? AND post_rank = ?
                    ''', (int(version_id), base_management_role, rank),
                ).fetchone()
                original_coefficient = float(row[0]) if row and row[0] is not None else None
                if original_coefficient is not None:
                    original_performance = round(
                        float(version['original_perf_base']) * original_coefficient
                        * derived_multiplier, 2
                    )
            else:
                issues.append('主任助理的原绩效口径尚未确认')
            row = conn.execute(
                '''
                SELECT coefficient FROM payroll_management_incentive_rules
                WHERE rule_version_id = ? AND management_role = ?
                ''', (int(version_id), str(base_management_role or '')),
            ).fetchone()
            incentive_coefficient = float(row[0]) if row else None
            if incentive_coefficient is not None and derived_multiplier != 1.0:
                incentive_coefficient = round(
                    incentive_coefficient * derived_multiplier, 6
                )
        elif payroll_category == 'professional':
            row = conn.execute(
                '''
                SELECT coefficient FROM payroll_original_perf_rules
                WHERE rule_version_id = ? AND employee_category = 'professional'
                  AND post_rank = ?
                ''', (int(version_id), rank),
            ).fetchone()
            original_coefficient = float(row[0]) if row and row[0] is not None else None
            if original_coefficient is not None:
                original_performance = round(
                    float(version['original_perf_base']) * original_coefficient, 2
                )
            grade_column = f"{str(tech_grade or '').lower()}_coefficient"
            if grade_column in {
                't1_coefficient', 't2_coefficient', 't3_coefficient',
                't4_coefficient', 't5_coefficient',
            }:
                row = conn.execute(
                    f'''
                    SELECT {grade_column} FROM payroll_position_value_rules
                    WHERE rule_version_id = ? AND official_position_name = ?
                    ''', (int(version_id), str(official_position_name or '')),
                ).fetchone()
                incentive_coefficient = (
                    float(row[0]) if row and row[0] is not None else None
                )
            if incentive_coefficient is None:
                issues.append('该文件岗位与T级组合没有有效系数')
        else:
            issues.append('岗位尚未归类')

        if original_performance is None and payroll_category != 'company_leader':
            issues.append('没有匹配到原绩效系数')
        if incentive_coefficient is not None:
            incentive_amount = round(
                float(version['incentive_base']) * incentive_coefficient, 2
            )
        performance_base = None
        if original_performance is not None:
            performance_base = round(
                original_performance + float(incentive_amount or 0), 2
            )
        return {
            '岗位工资': position_salary,
            '原绩效基础金额': float(version['original_perf_base']),
            '原绩效系数': original_coefficient,
            '原绩效': original_performance,
            '激励包基础金额': float(version['incentive_base']),
            '岗位价值系数': incentive_coefficient,
            '激励包': incentive_amount,
            '绩效基数': performance_base,
            '问题': list(dict.fromkeys(issues)),
        }
    finally:
        conn.close()
