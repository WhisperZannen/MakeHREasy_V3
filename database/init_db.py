# ==============================================================================
# 文件路径: database/init_db.py
# 功能描述: 初始化核心数据底座 (全系统所有数据表的【唯一合法】创建地！)
# 严正声明: 绝对禁止在任何前端页面 (如 pages/xxx.py) 中执行 CREATE TABLE。
# ==============================================================================

import sqlite3
import os

def init_database():
    # 获取当前脚本所在绝对路径，拼接数据库文件路径，防止生成的数据库文件位置错乱
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, 'hr_core.db')

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # 开启外键检查功能。
        # 意义：强制要求人员不能是“孤儿”。员工挂靠的部门必须在部门表里存在，否则不让存入数据库。
        cursor.execute("PRAGMA foreign_keys = ON;")

        # ======================================================================
        # 模块 1：基础人事档案大盘 (容纳所有在职、退休、挂靠人员)
        # ======================================================================

        # --- 表 1: 部门核心表 (departments) ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            dept_id INTEGER PRIMARY KEY AUTOINCREMENT,  -- 部门的数字身份证(系统自动排号，永远不重复)
            dept_name TEXT NOT NULL UNIQUE,             -- 部门的名字 (如: 云网交付中心，绝对不能重名)
            parent_dept_id INTEGER,                     -- 它的上级部门是谁 (用来画组织架构图)
            dept_category TEXT NOT NULL,                -- 部门属性分类 (如: 管理支撑类、经营发展类)
            sort_order INTEGER DEFAULT 999,             -- 排序号 (数字越小，导出Excel时排得越靠前，比如领导部门写1)
            status INTEGER DEFAULT 1,                   -- 部门死活状态 (1代表部门还在，0代表已经被撤销了)
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 这个部门是哪天在系统里建的
            FOREIGN KEY (parent_dept_id) REFERENCES departments(dept_id)
        )
        """)

        # --- 表 2: 岗位核心表 (positions) ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            pos_id INTEGER PRIMARY KEY AUTOINCREMENT,   -- 岗位的数字身份证(系统自动排号)
            pos_name TEXT NOT NULL UNIQUE,              -- 岗位名称 (如: 客户经理、AI研发工程师)
            pos_category TEXT,                          -- 岗位属于哪个大类 (如: 通用序列、专业序列)
            sort_order INTEGER DEFAULT 999,             -- 排序号 (比如主任排1，副主任排2，普通员工排999)
            status INTEGER DEFAULT 1,                   -- 岗位状态 (1代表还有人在干，0代表这个岗位名作废了)
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- 岗位创建时间
        )
        """)

        # --- 表 3: 人员核心主表 (employees) ---
        # 系统的“户口本”。不管是真干活的，还是早就退休的，只要有钱的往来，就得在这登记。
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            emp_id TEXT PRIMARY KEY,                    -- 工号，全系统唯一标识！(如: 42001943)
            name TEXT NOT NULL,                         -- 员工姓名 (如: 周慧中)
            id_card TEXT UNIQUE,                        -- 身份证号 (防重名，发工资和报税的铁凭证)
            dept_id INTEGER NOT NULL,                   -- 当前属于哪个部门 (强关联表1的部门ID)
            post_rank INTEGER,                          -- 岗级数字 (如 11、20，直接决定基础岗位工资发多少)
            post_grade TEXT,                            -- 岗级档次字母 (如 A、B、I)
            status TEXT DEFAULT '在职',                 -- 人员状态极其重要！【在职】发工资，【离职/退休】只算台账或慰问，【挂靠人员】只走社保不发工资！
            join_company_date DATE,                     -- 来本公司报到的日期 (用来算工龄工资)
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        )
        """)

        # --- 表 4: 人员档案扩展表 (employee_profiles) ---
        # 户口本的“背面”，放那些会影响他拿多少提成、拿多少补贴的附加信息。
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS employee_profiles (
            emp_id TEXT PRIMARY KEY,                    -- 员工工号
            pos_id INTEGER,                             -- 当前的具体岗位 (强关联表2的岗位ID)
            tech_grade TEXT,                            -- 技术等级 (如 T1、T3，决定他的“激励包基数”是3900还是3000)
            title_order INTEGER DEFAULT 999,            -- 领导正副职排位 (正职排前面，副职排后面)
            education_level TEXT,                       -- 最高学历 (本科试用期6个月，硕士3个月，用于系统自动算转正时间)
            degree TEXT,                                -- 学位 (学士/硕士)
            school_name TEXT,                           -- 毕业院校 (比如是不是985/211，可能影响特殊津贴)
            major TEXT,                                 -- 所学专业
            graduation_date DATE,                       -- 毕业日期 (算大学生补贴期限用)
            first_job_date DATE,                        -- 人生第一次参加工作的日期 (算国家连读工龄用，比如离职了能领几个月失业金)
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE,
            FOREIGN KEY (pos_id) REFERENCES positions(pos_id)
        )
        """)

        # --- 表 5: 人员历史轨迹表 (personnel_changes) ---
        # 薪酬系统的“时光机”！张三从A部门调到B部门，或者3月份批了晋升但4月才发钱，全靠它来留底追溯。
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS personnel_changes (
            change_id INTEGER PRIMARY KEY AUTOINCREMENT, -- 变动记录号
            emp_id TEXT NOT NULL,                        -- 谁发生了变动
            change_type TEXT,                            -- 是什么变动 (入职/调岗/调薪/离职/转正)
            old_dept_id INTEGER,                         -- 变动前的老部门
            new_dept_id INTEGER,                         -- 变动后的新部门
            old_pos_id INTEGER,                          -- 变动前的老岗位
            new_pos_id INTEGER,                          -- 变动后的新岗位
            old_tech_grade TEXT,                         -- 变动前的老技术等级
            new_tech_grade TEXT,                         -- 变动后的新技术等级 (升T级了，下个月绩效基数要跟着涨)
            old_post_rank INTEGER,                       -- 变动前的老岗级
            new_post_rank INTEGER,                       -- 变动后的新岗级
            old_post_grade TEXT,                         -- 变动前的老档次
            new_post_grade TEXT,                         -- 变动后的新档次
            change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 变动在现实中【真实生效】的时间 (算跨月补发的锚点！)
            change_reason TEXT,                          -- HR写的备注原因
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE
        )
        """)

        # --- 表 6: 专家特例表 (experts_plugin) ---
        # 专门记录你们公司“享受专家津贴”的人，因为他们的档案里保留了旧岗级。
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS experts_plugin (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT, -- 特例记录号
            emp_id TEXT NOT NULL UNIQUE,                 -- 谁是专家
            archive_post_rank INTEGER NOT NULL,          -- 档案里保留的老岗级
            archive_post_grade TEXT NOT NULL,            -- 档案里保留的老档次
            term_start_date DATE,                        -- 专家聘期从哪天开始
            term_end_date DATE,                          -- 专家聘期到哪天结束 (到期自动停发津贴)
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE
        )
        """)


        # ======================================================================
        # 财务数据底座 (表 7: 人工成本台账大宽表)
        # 这张表是给财务看的，不仅记录了发给个人的钱，还记录了公司负担的所有其他成本。
        # ======================================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS labor_cost_ledger (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT, -- 台账流水号
                
                -- [时空与人员锚点：记录这笔成本是谁在哪个月发生的]
                cost_month TEXT NOT NULL,                -- 核算月份（如 2026-03）
                emp_id TEXT NOT NULL,                    -- 员工工号
                emp_name TEXT NOT NULL,                  -- 生成这笔台账时，这个人叫什么名字 (快照防改名)
                dept_name TEXT NOT NULL,                 -- 生成这笔台账时，这个人在哪个部门 (极其重要，防后期调动账目乱窜)
                emp_status TEXT NOT NULL,                -- 生成这笔台账时，这个人的状态是在职还是离职
                
                -- [工资应发项：发到员工口袋里的钱的明细]
                base_salary REAL DEFAULT 0.0,            -- 岗位工资
                seniority_pay REAL DEFAULT 0.0,          -- 工龄工资
                comp_subsidy REAL DEFAULT 0.0,           -- 综合补贴
                perf_float_subsidy REAL DEFAULT 0.0,     -- 岗位绩效浮动补贴
                telecom_subsidy REAL DEFAULT 0.0,        -- 通讯费
                other_base_pay REAL DEFAULT 0.0,         -- 其他岗位工资 (如果有其他杂项工资放这里)
                intern_subsidy REAL DEFAULT 0.0,         -- 实习生拿的实习补贴
                grad_allowance REAL DEFAULT 0.0,         -- 高校毕业生津贴 或 专家津贴
                
                -- [绩效与奖金项：也是发给员工的]
                perf_standard REAL DEFAULT 0.0,          -- 绩效工资标准 (理论上该发多少，作为参考)
                kpi_score REAL DEFAULT 0.0,              -- 他当月的KPI得分 (作为参考)
                eval_perf_pay REAL DEFAULT 0.0,          -- 考核绩效 (实际算出来发给他的绩效钱)
                commission_pay REAL DEFAULT 0.0,         -- 业务员的提成绩效
                other_month_perf REAL DEFAULT 0.0,       -- 其他类型的月度绩效
                dynamic_perf_details TEXT DEFAULT '{}',  -- 预留的动态口袋：如果有稀奇古怪的新绩效名目，打包存JSON里
                
                special_award REAL DEFAULT 0.0,          -- 专项奖 (里面可能还包含了考勤扣罚等杂项)
                year_end_bonus REAL DEFAULT 0.0,         -- 年终绩效大奖
                other_special_award REAL DEFAULT 0.0,    -- 其他说不清道不明的专项奖
                dynamic_award_details TEXT DEFAULT '{}', -- 预留的动态口袋：装临时新增的杂项奖金
                
                gross_salary_total REAL DEFAULT 0.0,     -- 【极其重要：工资应发合计】(把上面所有应发项加起来的总和)
                
                -- [个人代扣代缴与实发项：要从员工口袋里抠出来的钱]
                pension_personal REAL DEFAULT 0.0,       -- 养老保险：员工自己交的部分
                medical_personal REAL DEFAULT 0.0,       -- 医疗保险：员工自己交的部分 (合并了那7块钱大病)
                unemployment_personal REAL DEFAULT 0.0,  -- 失业保险：员工自己交的部分
                provident_fund_personal REAL DEFAULT 0.0,-- 公积金：员工自己交的部分
                annuity_personal REAL DEFAULT 0.0,       -- 企业年金：员工自己交的部分
                tax_personal_month REAL DEFAULT 0.0,     -- 每个月平时扣的个人所得税
                tax_personal_bonus REAL DEFAULT 0.0,     -- 年终奖单独计税扣的钱
                net_salary REAL DEFAULT 0.0,             -- 【极其重要：个人实发金额】(应发合计 减去 所有个人代扣 减去 个税，打到银行卡的钱)
                
                -- [企业统筹人工成本项：公司替员工出的钱，不扣工资，但全是公司成本]
                pension_company REAL DEFAULT 0.0,        -- 养老保险：公司出的钱 (通常是16%)
                medical_company REAL DEFAULT 0.0,        -- 医疗保险：公司出的钱 (通常是8%)
                unemployment_company REAL DEFAULT 0.0,   -- 失业保险：公司出的钱
                work_injury_company REAL DEFAULT 0.0,    -- 工伤保险：只有公司出，员工不掏钱
                maternity_company REAL DEFAULT 0.0,      -- 生育保险：只有公司出，员工不掏钱
                provident_fund_company REAL DEFAULT 0.0, -- 公积金：公司配缴的钱 (你们通常是12%)
                annuity_company REAL DEFAULT 0.0,        -- 企业年金：公司配缴的钱
                
                -- [福利经费及其他人工成本项：公司花在员工身上的各种杂费]
                meal_daily REAL DEFAULT 0.0,             -- 每天中午在食堂吃饭，公司补贴的钱
                meal_ot REAL DEFAULT 0.0,                -- 晚上加班，公司买盒饭的钱
                welfare_condolence REAL DEFAULT 0.0,     -- 员工生病住院，买果篮慰问的钱
                welfare_single_child REAL DEFAULT 0.0,   -- 发给独生子女的补贴费
                welfare_health_check REAL DEFAULT 0.0,   -- 组织员工去体检的费用
                welfare_entry_check REAL DEFAULT 0.0,    -- 报销新员工入职体检的费用
                welfare_other REAL DEFAULT 0.0,          -- 其他乱七八糟的福利费
                allowance_heat REAL DEFAULT 0.0,         -- 夏天发的高温防暑降温费
                allowance_women REAL DEFAULT 0.0,        -- 发给女职工的劳保费或者卫生费
                medical_supplement REAL DEFAULT 0.0,     -- 公司额外掏钱买的商业补充医疗保险
                union_funds REAL DEFAULT 0.0,            -- 按工资总额比例计提的工会经费
                edu_funds REAL DEFAULT 0.0,              -- 按工资总额比例计提的职工教育培训经费
                cost_adjustment REAL DEFAULT 0.0,        -- 经费尾差微调（如果算出来的总账差个几分钱对不上，财务强迫症用这个抹平）
                other_cost_total REAL DEFAULT 0.0,       -- 【其他人工成本合计】(上面企业社保 + 福利经费的全部加总)
                
                -- [终极底线成本]
                total_labor_cost REAL DEFAULT 0.0,       -- 【全口径人工成本合计】(发给员工的总应发 + 其他人工成本合计 = 养这个人总共花了公司多少钱)
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 记账时间
                UNIQUE(cost_month, emp_id)               -- 强制约束：同月同人只能有一条真实台账记录
            )
        """)

        # ======================================================================
        # 模块 3：社保与福利结算底座 (SS Core - 全量 4 张表)
        # 作用：承接武汉特色多主体代缴、地市结算、特例人员路由以及突发补缴业务
        # ======================================================================

        # --- 表 8: 政策规则与动态算力引擎表 (ss_policy_rules) ---
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ss_policy_rules (
            rule_year TEXT PRIMARY KEY,         -- 生效年份，如 "2026"
            
            pension_upper REAL,                 -- 养老金最高只能按这个基数交 (封顶)
            pension_lower REAL,                 -- 养老金最低必须按这个基数交 (保底)
            pension_comp_rate REAL,             -- 公司交养老金的百分比 (如 0.16)
            pension_pers_rate REAL,             -- 员工自己交养老金的百分比 (如 0.08)
            
            medical_upper REAL,                 -- 医保封顶基数
            medical_lower REAL,                 -- 医保保底基数
            medical_comp_rate REAL,             -- 公司交医保的百分比
            medical_pers_rate REAL,             -- 员工自己交医保的百分比 (如 0.02)
            medical_serious_fix REAL DEFAULT 7.0, -- 大病医疗固定扣款！(死数，就是你手工表里那绝对不变的7块钱)
            
            unemp_upper REAL,                   -- 失业封顶基数
            unemp_lower REAL,                   -- 失业保底基数
            unemp_comp_rate REAL,               -- 公司交失业的百分比
            unemp_pers_rate REAL,               -- 员工交失业的百分比
            
            injury_upper REAL,                  -- 工伤封顶基数
            injury_lower REAL,                  -- 工伤保底基数
            injury_comp_rate REAL,              -- 公司交工伤的百分比 (工伤没个人比例，全公司掏)
            
            maternity_upper REAL,               -- 生育封顶基数
            maternity_lower REAL,               -- 生育保底基数
            maternity_comp_rate REAL,           -- 公司交生育的百分比 (生育也没个人比例)
            
            fund_upper REAL,                    -- 公积金最高基数限制
            fund_lower REAL,                    -- 公积金最低基数限制
            fund_comp_rate REAL,                -- 公司交公积金的百分比 (通常0.12)
            fund_pers_rate REAL,                -- 员工交公积金的百分比 (通常0.12)
            
            annuity_comp_rate REAL,             -- 公司交企业年金的百分比
            annuity_pers_rate REAL,             -- 员工交企业年金的百分比
            
            rounding_mode TEXT DEFAULT 'round_to_yuan',    -- 【核心开关】社保算出小数怎么办？(精确到分/四舍五入到元/逢角进元等)
            fund_calc_method TEXT DEFAULT 'reverse_from_ss' -- 【核心开关】公积金算法 (是基数×12%独立算，还是用社保取整后的基数倒推逢元进十)
        )
        ''')

        # --- 表 9: 全员参保配置表 / 基因矩阵 (ss_emp_matrix) ---
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ss_emp_matrix (
            emp_id TEXT PRIMARY KEY,            -- 员工工号
            cost_center TEXT DEFAULT '本级',    -- 这笔社保的钱该让谁出 (如: 总公司、某地市分公司)
            
            base_salary_avg REAL DEFAULT 0.0,   -- [原材料] 他去年的月平均工资 (社保局核定基数用的原始数字)
            fund_base_avg REAL DEFAULT 0.0,     -- [原材料] 专门留给公积金基数和社保基数不一样的人填的特殊原始基数
            ss_base_actual REAL DEFAULT 0.0,    -- [成品] 经过封顶、保底、取整后，系统算出来的【真实执行社保基数】
            fund_base_actual REAL DEFAULT 0.0,  -- [成品] 算出来的【真实执行公积金基数】
            
            pension_enabled BOOLEAN DEFAULT 1,  -- 养老保险开关 (1代表这人交养老，0代表不交养老)
            pension_account TEXT,               -- 养老保险通道 (他的养老钱最后打给省公司账户，还是省公众账户)
            
            medical_enabled BOOLEAN DEFAULT 1,  -- 医保开关
            medical_account TEXT,               -- 医保打款通道
            
            unemp_enabled BOOLEAN DEFAULT 1,    -- 失业开关
            unemp_account TEXT,                 -- 失业打款通道
            
            injury_enabled BOOLEAN DEFAULT 1,   -- 工伤开关
            injury_account TEXT,                -- 工伤打款通道 (解决某些挂靠人员只单独交个工伤险的问题)
            
            maternity_enabled BOOLEAN DEFAULT 1,-- 生育开关
            maternity_account TEXT,             -- 生育打款通道
            
            fund_enabled BOOLEAN DEFAULT 1,     -- 公积金开关
            fund_account TEXT,                  -- 公积金打款通道
            
            annuity_enabled BOOLEAN DEFAULT 0,  -- 年金开关 (默认0不交，等转正满年限了系统自动开到1)
            annuity_account TEXT,               -- 年金打款通道
                    
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id)
        )
        ''')

        # --- 表 10: 月度正常社保核算结果沉底表 (ss_monthly_records) ---
        # 极其重要！社保当月算死后存进这里。以后工资表扣社保的钱，100%强制从这张表里抓，一分钱不能错！
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ss_monthly_records (
            record_id TEXT PRIMARY KEY,         -- 账单流水号 (如 2026-03_42001943)
            cost_month TEXT NOT NULL,           -- 这是哪个月的社保 (如 2026-03)
            emp_id TEXT NOT NULL,               -- 员工工号
            cost_center TEXT,                   -- 核算当月，这笔社保钱算在哪个部门头上 (快照留存)
            
            -- [个人扣除资金池] 这些钱要在工资条里扣掉！
            pension_pers REAL DEFAULT 0.0,      -- 个人扣养老的钱
            medical_pers REAL DEFAULT 0.0,      -- 个人扣医保的钱 (通常是2%算出来的基本医疗)
            medical_serious_pers REAL DEFAULT 0.0, -- 个人扣大病的钱 (就是那固定7块钱)
            unemp_pers REAL DEFAULT 0.0,        -- 个人扣失业的钱
            fund_pers REAL DEFAULT 0.0,         -- 个人扣公积金的钱
            annuity_pers REAL DEFAULT 0.0,      -- 个人扣年金的钱
            
            -- [企业统筹资金池] 这些钱公司出，做成本台账用！
            pension_comp REAL DEFAULT 0.0,      -- 公司出养老的钱
            medical_comp REAL DEFAULT 0.0,      -- 公司出医保的钱
            unemp_comp REAL DEFAULT 0.0,        -- 公司出失业的钱
            injury_comp REAL DEFAULT 0.0,       -- 公司出工伤的钱
            maternity_comp REAL DEFAULT 0.0,    -- 公司出生育的钱
            fund_comp REAL DEFAULT 0.0,         -- 公司出公积金的钱
            annuity_comp REAL DEFAULT 0.0,      -- 公司出年金的钱
            
            -- [物理账单路由追踪器] 记录这笔钱该给哪个主体！
            pension_route TEXT,                 -- 他的养老金交给了谁 (如：中电数智)
            medical_route TEXT,                 -- 他的医保交给了谁
            unemp_route TEXT,                   -- 他的失业交给了谁
            injury_route TEXT,                  -- 他的工伤交给了谁
            maternity_route TEXT,               -- 他的生育交给了谁
            fund_route TEXT,                    -- 他的公积金交给了谁
            annuity_route TEXT,                 -- 他的年金交给了谁
            
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id),
            UNIQUE(cost_month, emp_id)          -- 约束：同一个人同个月只能产生一张正常社保单
        )
        ''')

        # --- 表 11: 异步突发补缴与滞纳金账目表 (ss_retroactive_records) ---
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ss_retroactive_records (
            retro_id TEXT PRIMARY KEY,          -- 补缴记录单号
            process_month TEXT NOT NULL,        -- 这笔补缴款准备塞进哪个月的工资里去扣钱 (比如4月)
            emp_id TEXT NOT NULL,               -- 谁被补缴了
            target_start_month TEXT,            -- 这是补哪个月起的社保 (比如漏交了2月份)
            target_end_month TEXT,              -- 补交到哪个月为止 (比如补交了2到3月的)
            retro_type TEXT,                    -- 补交的是什么险种 (如：养老保险)
            total_comp_retro REAL DEFAULT 0.0,  -- 补缴带来的企业成本是多少 (公司自己认倒霉掏的本金)
            total_pers_retro REAL DEFAULT 0.0,  -- 补缴带来的个人扣款是多少 (要从他下个月工资里额外强行扣走的本金！)
            late_fee REAL DEFAULT 0.0,          -- 晚交产生的滞纳金 (纯财务成本，公司全掏，不扣个人)
            other_penalty REAL DEFAULT 0.0,     -- 其他政府罚单费用
            status TEXT DEFAULT '待推送到当期账单', -- 这个补缴单有没有被工资表吸走执行
            remarks TEXT,                       -- 手工记下的原因 (如：新员工入职晚了统一补交)
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id)
        )
        ''')


        # ======================================================================
        # 模块 4：全新薪酬核算大一统中心 (完全复刻你的变态级签字用表)
        # ======================================================================

        # --- 表 12: 薪酬月度主账单表 (payroll_monthly_records) ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payroll_monthly_records (
                record_id TEXT PRIMARY KEY,                 -- 账单流水号 (格式: 核算月_工号)
                cost_month TEXT NOT NULL,                   -- 这是发哪个月的工资 (YYYY-MM)
                emp_id TEXT NOT NULL,                       -- 员工工号
                dept_name TEXT,                             -- 这个人发钱时算在哪个部门 (部门快照)
                
                -- [第一部分：静态与固定发钱区]
                base_salary REAL DEFAULT 0.0,               -- 岗位工资 (如果没转正可能会打折)
                seniority_pay REAL DEFAULT 0.0,             -- 工龄工资
                comp_subsidy REAL DEFAULT 0.0,              -- 综合补贴
                telecom_subsidy REAL DEFAULT 0.0,           -- 通讯补贴
                position_adj REAL DEFAULT 0.0,              -- 岗位补/扣发 (如行一/行二非领导的固定差额等手敲金额)
                expert_allowance REAL DEFAULT 0.0,          -- 专家津贴 或 高校毕业生津贴
                
                -- [第二部分：极度复杂的浮动绩效发钱区]
                perf_base REAL DEFAULT 0.0,                 -- 绩效的原始基准盘 (原始标准 + 激励包基数)
                perf_kpi_score REAL DEFAULT 100.0,          -- 本月 KPI/KCI 分数 (100分就是满绩效)
                perf_pack_coef REAL DEFAULT 1.0,            -- 激励包的翻倍系数 (比如乘以1.3倍)
                perf_leader_coef REAL DEFAULT 1.0,          -- 领导专属的激励倍数
                perf_excel_coef REAL DEFAULT 1.0,           -- 优才政策倍数 (默认不翻倍为1)
                perf_salary_calc REAL DEFAULT 0.0,          -- 【绩效考核工资】(由上面几个系数乘出来的最终绩效金额)
                perf_adj REAL DEFAULT 0.0,                  -- 绩效补/扣发 (跨期滞后导致的手工干预调差额)
                
                -- [第三部分：混沌挂载层 (临时或单次的乱七八糟的钱)]
                dynamic_additions TEXT DEFAULT '{}',        -- 动态加项 (把零散的名目转成JSON格式无限加塞，不占表列)
                dynamic_deductions TEXT DEFAULT '{}',       -- 动态减项 (如考勤扣款等临时名目放这里)
                special_bonus_total REAL DEFAULT 0.0,       -- 专项奖金合计 (从外挂奖金池里把零散奖金打包加总到这一个格子里)
                
                -- [第四部分：大结账与扣款收网区]
                gross_salary_total REAL DEFAULT 0.0,        -- 【应发工资合计】(把上面所有应该发给他的钱加总，拿去算个税的起点基数！)
                
                -- （下方的社保扣款必须从 表10、表11 强行吸过来！不能在薪酬里自己乱算！）
                ss_pension_pers REAL DEFAULT 0.0,           -- 代扣养老保险
                ss_medical_mix REAL DEFAULT 0.0,            -- 代扣医保合并项 (把基本医疗和大病的7块钱揉在一起给你展示)
                ss_unemp_pers REAL DEFAULT 0.0,             -- 代扣失业保险
                ss_fund_pers REAL DEFAULT 0.0,              -- 代扣公积金
                ss_annuity_pers REAL DEFAULT 0.0,           -- 代扣企业年金
                
                tax_deduction REAL DEFAULT 0.0,             -- 代扣个税 (财务用专门工具算好后塞回来的扣税金额)
                net_salary REAL DEFAULT 0.0,                -- 【个人实发工资】(应发合计 减去 五险两金扣款 减去 个税，最终打给银行卡的钱)
                
                -- [状态与防改锚点]
                audit_status TEXT DEFAULT '草稿',            -- 账单算到了哪一步 (草稿 / 待算税 / 已封账)
                oa_clearing_no TEXT,                        -- 电信OA系统要求的清册编号 (对接OA备用)
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 这行数据最后一次变动的时间
                FOREIGN KEY (emp_id) REFERENCES employees(emp_id),
                UNIQUE(cost_month, emp_id)                  -- 同月同人只发一张主流水单
            )
        ''')

        # --- 表 13: 专项奖金外挂池 (payroll_special_bonus) ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payroll_special_bonus (
                bonus_id TEXT PRIMARY KEY,                  -- 奖项记录编号
                cost_month TEXT NOT NULL,                   -- 这笔奖金要跟着哪个月的工资一起发
                emp_id TEXT NOT NULL,                       -- 发给谁
                project_name TEXT NOT NULL,                 -- 这个奖金叫什么名字 (如: 13薪(1月), 年终奖预发)
                amount REAL DEFAULT 0.0,                    -- 奖金多少钱
                import_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- HR哪天录进来的
                FOREIGN KEY (emp_id) REFERENCES employees(emp_id)
            )
        ''')

        # --- 表 14: 动态长效发条规则表 (payroll_allowance_rules) ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payroll_allowance_rules (
                rule_id INTEGER PRIMARY KEY AUTOINCREMENT,  -- 规则编号
                emp_id TEXT NOT NULL,                       -- 谁享受这个规则
                allowance_type TEXT NOT NULL,               -- 这是什么规则 (如: 大学生补贴, 专家配平金)
                monthly_amount REAL NOT NULL,               -- 每月系统应该自动给他加多少钱
                start_month TEXT NOT NULL,                  -- 从哪个月开始发 (如 2026-01)
                end_month TEXT,                             -- 到哪个月截止停发 (如留空代表一直发到死)
                is_active INTEGER DEFAULT 1,                -- HR能不能强制掐断它 (1有效, 0强行停用)
                remarks TEXT,                               -- 备注留档
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