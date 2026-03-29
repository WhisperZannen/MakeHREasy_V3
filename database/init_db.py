# ==============================================================================
# 文件路径: database/init_db.py
# 功能描述: 初始化核心数据底座 (V3.5 差异快照与时空回溯版 + 全新社保引擎)
# 执行逻辑:
#   1. 物理隔离部门、岗位、人员与历史轨迹。
#   2. 引入外键约束，确保人员必须挂靠在真实的部门和岗位下。
#   3. 为后续“按岗定薪”和“技术等级加成”提供底层字段支撑。
#   4. 历史表扩容，支持新旧状态对比与数据回滚机制。
#   5. [核心新增] 注入多主体社保与福利结算 6 张核心表。
# ==============================================================================

import sqlite3
import os

def init_database():
    # 获取当前脚本所在绝对路径，拼接数据库文件路径，防止在不同目录下运行脚本时生成的数据库文件位置错乱
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, 'hr_core.db')

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # 开启外键检查，防止出现无部门人员等“数据孤儿”
        cursor.execute("PRAGMA foreign_keys = ON;")

        # ======================================================================
        # 基础人事模块 (表 1 到 表 6)
        # ======================================================================

        # --- 表 1: 部门核心表 ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            dept_id INTEGER PRIMARY KEY AUTOINCREMENT,  -- 部门自增主键
            dept_name TEXT NOT NULL UNIQUE,             -- 部门名称（全局唯一）
            parent_dept_id INTEGER,                     -- 上级部门ID（支持无限极树状结构）
            dept_category TEXT NOT NULL,                -- 部门性质（公司领导/管控/生产/其他）
            sort_order INTEGER DEFAULT 999,             -- 排序权重，越小越靠前
            status INTEGER DEFAULT 1,                   -- 状态：1 正常，0 已撤销
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
            FOREIGN KEY (parent_dept_id) REFERENCES departments(dept_id)
        )
        """)

        # --- 表 2: 岗位核心表 ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            pos_id INTEGER PRIMARY KEY AUTOINCREMENT,   -- 岗位自增主键
            pos_name TEXT NOT NULL UNIQUE,              -- 岗位名称（全局唯一）
            pos_category TEXT,                          -- 岗位序列（如：通用序列、项目序列）
            sort_order INTEGER DEFAULT 999,             -- 排序权重
            status INTEGER DEFAULT 1,                   -- 状态：1 正常，0 停用
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # --- 表 3: 人员核心主表 ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            emp_id TEXT PRIMARY KEY,                    -- 工号，作为全系统的绝对主键
            name TEXT NOT NULL,                         -- 员工姓名
            id_card TEXT UNIQUE,                        -- 身份证号（脱敏前全量存储，唯一）
            dept_id INTEGER NOT NULL,                   -- 归属部门ID（强关联部门表）
            post_rank INTEGER,                          -- 岗级（如 11岗）
            post_grade TEXT,                            -- 档次（如 E档）
            status TEXT DEFAULT '在职',                 -- 人员状态：在职/离职/退休
            join_company_date DATE,                     -- 入职日期（用于推演转正、司龄计算）
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        )
        """)

        # --- 表 4: 人员档案扩展表 ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS employee_profiles (
            emp_id TEXT PRIMARY KEY,                    -- 关联人员主表的工号
            pos_id INTEGER,                             -- 岗位ID（强关联岗位表）
            tech_grade TEXT,                            -- 技术等级（T1-T5等，影响薪酬加成）
            title_order INTEGER DEFAULT 999,            -- 职称排序
            education_level TEXT,                       -- 最高学历（用于推演转正期：本专6个月，硕博3个月）
            degree TEXT,                                -- 学位
            school_name TEXT,                           -- 毕业院校
            major TEXT,                                 -- 专业
            graduation_date DATE,                       -- 毕业日期
            first_job_date DATE,                        -- 参加工作日期（用于核算社保连续工龄等）
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE,
            FOREIGN KEY (pos_id) REFERENCES positions(pos_id)
        )
        """)

        # --- 表 5: 人员历史轨迹表 (时空审计底座) ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS personnel_changes (
            change_id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_id TEXT NOT NULL,                        -- 发生变动的员工工号
            change_type TEXT,                            -- 变动类型(入职/调岗/调薪/离职/转正等)
            
            old_dept_id INTEGER,         
            new_dept_id INTEGER,                         -- 变动前后部门对比
            
            old_pos_id INTEGER,          
            new_pos_id INTEGER,                          -- 变动前后岗位对比
            
            old_tech_grade TEXT,         
            new_tech_grade TEXT,                         -- 变动前后技术等级对比
            
            old_post_rank INTEGER,       
            new_post_rank INTEGER,                       -- 变动前后岗级对比
            
            old_post_grade TEXT,         
            new_post_grade TEXT,                         -- 变动前后档次对比
            
            change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 变动实际生效时间
            change_reason TEXT,                          -- HR 填写的变动原因备注
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE
        )
        """)

        # --- 表 6: 专家特例表 ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS experts_plugin (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_id TEXT NOT NULL UNIQUE,                
            archive_post_rank INTEGER NOT NULL,          -- 档案保留岗级
            archive_post_grade TEXT NOT NULL,            -- 档案保留档次
            term_start_date DATE,                        -- 聘期开始时间
            term_end_date DATE,                          -- 聘期结束时间
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE
        )
        """)

        # ======================================================================
        # 财务数据底座 (表 7: 人工成本台账大宽表)
        # ======================================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS labor_cost_ledger (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- [时空与人员锚点]
                cost_month TEXT NOT NULL,                -- 核算月份（如 2026-01）
                emp_id TEXT NOT NULL,                    -- 工号
                emp_name TEXT NOT NULL,                  -- 固化账单生成时的姓名
                dept_name TEXT NOT NULL,                 -- 固化账单生成时的部门名称
                emp_status TEXT NOT NULL,                -- 固化账单生成时的状态
                
                -- [工资应发项]
                base_salary REAL DEFAULT 0.0,            -- 岗位工资
                seniority_pay REAL DEFAULT 0.0,          -- 工龄工资
                comp_subsidy REAL DEFAULT 0.0,           -- 综合补贴
                perf_float_subsidy REAL DEFAULT 0.0,     -- 岗位绩效浮动补贴
                telecom_subsidy REAL DEFAULT 0.0,        -- 通讯费
                other_base_pay REAL DEFAULT 0.0,         -- 其他岗位工资
                intern_subsidy REAL DEFAULT 0.0,         -- 实习补贴
                grad_allowance REAL DEFAULT 0.0,         -- 高校毕业生/专家津贴
                
                -- [绩效与奖金项]
                perf_standard REAL DEFAULT 0.0,          -- 绩效工资标准(参考)
                kpi_score REAL DEFAULT 0.0,              -- KPI得分(参考)
                eval_perf_pay REAL DEFAULT 0.0,          -- 考核绩效实际发钱
                commission_pay REAL DEFAULT 0.0,         -- 提成绩效
                other_month_perf REAL DEFAULT 0.0,       -- 其他月度绩效
                dynamic_perf_details TEXT DEFAULT '{}',  -- 动态绩效 JSON 扩展列
                
                special_award REAL DEFAULT 0.0,          -- 专项奖(含考勤扣罚)
                year_end_bonus REAL DEFAULT 0.0,         -- 年终绩效奖
                other_special_award REAL DEFAULT 0.0,    -- 其他专项奖
                dynamic_award_details TEXT DEFAULT '{}', -- 动态专项奖 JSON 扩展列
                
                gross_salary_total REAL DEFAULT 0.0,     -- 【工资应发合计】 (横向求和结果)
                
                -- [个人代扣代缴与实发项]
                pension_personal REAL DEFAULT 0.0,       -- 养老个人
                medical_personal REAL DEFAULT 0.0,       -- 医疗个人
                unemployment_personal REAL DEFAULT 0.0,  -- 失业个人
                provident_fund_personal REAL DEFAULT 0.0,-- 公积金个人
                annuity_personal REAL DEFAULT 0.0,       -- 年金个人
                tax_personal_month REAL DEFAULT 0.0,     -- 个税日常
                tax_personal_bonus REAL DEFAULT 0.0,     -- 个税年终奖
                net_salary REAL DEFAULT 0.0,             -- 【个人实发金额】 (应发合计 - 个人代扣总和)
                
                -- [企业统筹人工成本项]
                pension_company REAL DEFAULT 0.0,        -- 养老企业
                medical_company REAL DEFAULT 0.0,        -- 医疗企业
                unemployment_company REAL DEFAULT 0.0,   -- 失业企业
                work_injury_company REAL DEFAULT 0.0,    -- 工伤企业
                maternity_company REAL DEFAULT 0.0,      -- 生育企业
                provident_fund_company REAL DEFAULT 0.0, -- 公积金企业
                annuity_company REAL DEFAULT 0.0,        -- 年金企业
                
                -- [福利经费及其他人工成本项]
                meal_daily REAL DEFAULT 0.0,             -- 日常用餐
                meal_ot REAL DEFAULT 0.0,                -- 加班用餐
                welfare_condolence REAL DEFAULT 0.0,     -- 员工慰问费
                welfare_single_child REAL DEFAULT 0.0,   -- 独生子女补贴
                welfare_health_check REAL DEFAULT 0.0,   -- 员工体检费
                welfare_entry_check REAL DEFAULT 0.0,    -- 入职体检
                welfare_other REAL DEFAULT 0.0,          -- 其他福利
                allowance_heat REAL DEFAULT 0.0,         -- 防暑降温费
                allowance_women REAL DEFAULT 0.0,        -- 女工劳保费
                medical_supplement REAL DEFAULT 0.0,     -- 补充医保费
                union_funds REAL DEFAULT 0.0,            -- 工会经费
                edu_funds REAL DEFAULT 0.0,              -- 职工教育经费
                cost_adjustment REAL DEFAULT 0.0,        -- 经费尾差微调（强迫症平账专用）
                other_cost_total REAL DEFAULT 0.0,       -- 【其他人工成本合计】 (横向求和结果)
                
                -- [最终成本项]
                total_labor_cost REAL DEFAULT 0.0,       -- 【人工成本合计】 (应发合计 + 其他人工成本合计)
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(cost_month, emp_id)               -- 强制约束：同月同人只能有一条真实台账记录
            )
        """)

        # ======================================================================
        # 模块 3：社保与福利结算底座 (SS Core - 全量 6 张表)
        # 作用：承接武汉特色多主体代缴、地市结算、特例人员路由以及突发补缴业务
        # ======================================================================

        # ----------------------------------------------------------------------
        # 1. 缴费通道与账单路由表 (ss_accounts)
        # 业务场景：解决公司存在“省公众”、“中电数智”、“省公司医保专户”等多头缴费，月末对账困难的问题
        # ----------------------------------------------------------------------
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ss_accounts (
            account_id TEXT PRIMARY KEY,        -- 账户唯一编号（例如 ACC-01）
            account_name TEXT NOT NULL,         -- 账户名称（例如 "省公众_社保综合"）
            manage_entity TEXT,                 -- 归属主体（例如 "本级", "省公司", "中电数智"）
            report_group TEXT,                  -- 报表分组（方便月末按组生成打款申请单）
            status TEXT DEFAULT '启用'          -- 状态控制
        )
        ''')

        # ----------------------------------------------------------------------
        # 2. 地市公司结算字典 (ss_city_branches)
        # 业务场景：记录“虽然在我这交社保，但成本要找下面分公司报销”的单位名单
        # ----------------------------------------------------------------------
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ss_city_branches (
            branch_id TEXT PRIMARY KEY,         -- 结算实体编号
            branch_name TEXT UNIQUE NOT NULL,   -- 实体名称（例如 "孝感分公司"）
            contact_person TEXT,                -- 分公司对接人姓名（用于生成请款函）
            contact_phone TEXT,                 -- 分公司对接人电话
            status TEXT DEFAULT '启用'
        )
        ''')

        # ----------------------------------------------------------------------
        # 3. 政策规则与动态算力引擎表 (ss_policy_rules)
        # 业务场景：存放每年7月的基数上下限、费率，并控制全公司的“抹零取整”规则
        # ----------------------------------------------------------------------
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ss_policy_rules (
            rule_year TEXT PRIMARY KEY,         -- 生效年份，如 "2026"
            
            -- 【养老规则区】
            pension_upper REAL,                 -- 养老封顶基数
            pension_lower REAL,                 -- 养老保底基数
            pension_comp_rate REAL,             -- 企业养老费率
            pension_pers_rate REAL,             -- 个人养老费率
            
            -- 【医疗规则区】
            medical_upper REAL,                 -- 医疗封顶基数
            medical_lower REAL,                 -- 医疗保底基数
            medical_comp_rate REAL,             -- 企业医疗费率
            medical_pers_rate REAL,             -- 个人医疗费率
            medical_serious_fix REAL DEFAULT 7.0, -- 特例：大病医疗单独扣除的绝对固定金额（你要求的7元）
            
            -- 【失业规则区】
            unemp_upper REAL, 
            unemp_lower REAL, 
            unemp_comp_rate REAL, 
            unemp_pers_rate REAL,
            
            -- 【工伤规则区】 (仅企业负担)
            injury_upper REAL, 
            injury_lower REAL, 
            injury_comp_rate REAL,
            
            -- 【生育规则区】 (仅企业负担)
            maternity_upper REAL, 
            maternity_lower REAL, 
            maternity_comp_rate REAL,
            
            -- 【公积金规则区】
            fund_upper REAL, 
            fund_lower REAL, 
            fund_comp_rate REAL, 
            fund_pers_rate REAL,
            
            -- 【年金规则区】
            annuity_comp_rate REAL, 
            annuity_pers_rate REAL,
            
            -- 【全局引擎开关】
            rounding_mode TEXT DEFAULT 'round_to_yuan',    -- 社保取整规则（控制是否四舍五入到十位等）
            fund_calc_method TEXT DEFAULT 'reverse_from_ss' -- 公积金算法开关（控制是独立算，还是按社保取整后再用12%倒推）
        )
        ''')

        # ----------------------------------------------------------------------
        # 4. 全员参保配置表 / 基因矩阵 (ss_emp_matrix)
        # 业务场景：解决只交单险种、一建挂靠单独交工伤、不交医保等极度夹生的人员个体特例
        # ----------------------------------------------------------------------
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ss_emp_matrix (
            emp_id TEXT PRIMARY KEY,            -- 员工工号
            cost_center TEXT DEFAULT '本级',    -- 财务成本归属（如果是地市人员，填入对应地市名称）
            
            -- [基数三核心] 严格解耦原材料与两套成品基数，防止算法打架
            base_salary_avg REAL DEFAULT 0.0,   -- [原材料] 去年的月均应发绝对原始值（由系统自动测算写入）
            fund_base_avg REAL DEFAULT 0.0, -- [新增] 公积金独立基数特例通道，默认为0代表与社保同源
            ss_base_actual REAL DEFAULT 0.0,    -- [成品 A] 经过上下限卡位和十位取整后的【真实社保基数】
            fund_base_actual REAL DEFAULT 0.0,  -- [成品 B] 经过倒推法或者独立核算后的【真实公积金基数】
            
            -- [养老控制组]
            pension_enabled BOOLEAN DEFAULT 1,  -- 养老是否参保（1=是，0=否，应对不交养老的特例）
            pension_account TEXT,               -- 养老资金通道（本人的养老钱该交给哪个专户）
            
            -- [医疗控制组]
            medical_enabled BOOLEAN DEFAULT 1,
            medical_account TEXT,
            
            -- [失业控制组]
            unemp_enabled BOOLEAN DEFAULT 1,
            unemp_account TEXT,
            
            -- [工伤控制组]
            injury_enabled BOOLEAN DEFAULT 1,
            injury_account TEXT,                -- 工伤通道（一建特例员工单独配属到省公众市级账户的核心手段）
            
            -- [生育控制组]
            maternity_enabled BOOLEAN DEFAULT 1,
            maternity_account TEXT,
            
            -- [公积金控制组]
            fund_enabled BOOLEAN DEFAULT 1,
            fund_account TEXT,
            
            -- [年金控制组]
            annuity_enabled BOOLEAN DEFAULT 0,  -- 年金开关（默认为 0，等待引擎判定转正日期后自动推为 1）
            annuity_account TEXT,
                    
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id)
        )
        ''')

        # ----------------------------------------------------------------------
        # 5. 月度正常核算结果沉底表 (ss_monthly_records)
        # 业务场景：承载每月算出来的真实账单，向工资单输送个人扣款，向台账输送企业成本
        # ----------------------------------------------------------------------
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ss_monthly_records (
            record_id TEXT PRIMARY KEY,         -- 每笔账目的唯一主键 (格式如 2026-03_001)
            cost_month TEXT NOT NULL,           -- 核算月份
            emp_id TEXT NOT NULL,               -- 员工工号
            cost_center TEXT,                   -- 核算当月该员工的成本归属（固化快照，防止后期调动导致历史账目变样）
            
            -- [个人扣除资金池] (向算薪模块输出)
            pension_pers REAL DEFAULT 0.0,
            medical_pers REAL DEFAULT 0.0,
            medical_serious_pers REAL DEFAULT 0.0, -- 大病医疗扣款（绝对独立的一列，不和普通医疗混淆）
            unemp_pers REAL DEFAULT 0.0,
            fund_pers REAL DEFAULT 0.0,
            annuity_pers REAL DEFAULT 0.0,
            
            -- [企业统筹资金池] (向人工成本台账输出)
            pension_comp REAL DEFAULT 0.0,
            medical_comp REAL DEFAULT 0.0,
            unemp_comp REAL DEFAULT 0.0,
            injury_comp REAL DEFAULT 0.0,
            maternity_comp REAL DEFAULT 0.0,
            fund_comp REAL DEFAULT 0.0,
            annuity_comp REAL DEFAULT 0.0,
            
            -- [物理账单路由追踪器] (记录核算当月这笔钱最终给谁了，方便月末生成打款结算单)
            pension_route TEXT,
            medical_route TEXT,
            unemp_route TEXT,
            injury_route TEXT,
            maternity_route TEXT,
            fund_route TEXT,
            annuity_route TEXT,
            
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id),
            UNIQUE(cost_month, emp_id)          -- 约束：一人一月只允许产生一条常规账单
        )
        ''')

        # ----------------------------------------------------------------------
        # 6. 异步突发补缴与滞纳金账目表 (ss_retroactive_records)
        # 业务场景：处理新员工延迟入职补缴（跨月合并）、以及年底特批的单项年金补缴
        # ----------------------------------------------------------------------
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ss_retroactive_records (
            retro_id TEXT PRIMARY KEY,          -- 补缴记录唯一标识
            process_month TEXT NOT NULL,        -- 处理月份（这笔补缴款合并进哪个月的工资/台账里扣）
            emp_id TEXT NOT NULL,               -- 补缴人
            
            target_start_month TEXT,            -- 被补缴的时间段起点（例如：2月）
            target_end_month TEXT,              -- 被补缴的时间段终点（例如：4月）
            retro_type TEXT,                    -- 补缴原因/类型分类
            
            total_comp_retro REAL DEFAULT 0.0,  -- 该笔补缴产生的【企业本金】合计
            total_pers_retro REAL DEFAULT 0.0,  -- 该笔补缴产生的【个人本金】合计（当月要在工资条里额外扣掉）
            
            late_fee REAL DEFAULT 0.0,          -- 滞纳金（纯财务成本，由企业承担）
            other_penalty REAL DEFAULT 0.0,     -- 其他可能的罚息费用
            
            status TEXT DEFAULT '待推送到当期账单', -- 推送状态跟踪
            remarks TEXT,                       -- 备注
            
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id)
        )
        ''')

        conn.commit()
        print(f"✅ V3.5 差异快照底座与全量社保引擎初始化成功！")
    except sqlite3.Error as e:
        conn.rollback()
        print(f"❌ 初始化失败: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_database()