# ==============================================================================
# 文件路径: database/init_db.py
# 功能描述: 初始化核心数据底座 (全系统所有数据表的【唯一合法】创建地！)
# 严正声明: 绝对禁止在任何前端页面 (如 pages/xxx.py) 中执行 CREATE TABLE。
# ==============================================================================

import sqlite3
import os

def ensure_payroll_schema_patch(cursor):
    """
    薪酬主表结构补丁函数。

    这个函数是干什么的？
    ------------------------------------------------------------
    它专门解决“旧数据库已经存在，但是缺少新字段”的问题。

    为什么不能只靠 CREATE TABLE IF NOT EXISTS？
    ------------------------------------------------------------
    因为 SQLite 看到表已经存在后，就不会重新创建，也不会自动补字段。
    所以旧数据库缺什么字段，它还是继续缺什么字段。

    这个函数怎么工作？
    ------------------------------------------------------------
    1. 先读取 payroll_monthly_records 当前有哪些字段；
    2. 再检查我们需要的字段是否存在；
    3. 如果不存在，就用 ALTER TABLE 自动补上；
    4. 如果已经存在，就什么都不做，避免重复添加时报错。

    参数 cursor 是什么？
    ------------------------------------------------------------
    cursor 是 SQLite 的“执行 SQL 的笔”。
    init_database() 里面已经创建了 cursor，所以我们直接把它传进来用。
    """

    # 读取 payroll_monthly_records 表的字段信息。
    # PRAGMA table_info(表名) 是 SQLite 专门用来查看表结构的命令。
    cursor.execute("PRAGMA table_info(payroll_monthly_records)")

    # cursor.fetchall() 会返回很多行，每一行代表一个字段。
    # 每行里第 2 个位置，也就是 col[1]，是字段名。
    # 这里把所有字段名提取出来，放进一个 set 集合里。
    # set 的好处是查找很快，而且不会重复。
    existing_columns = {col[1] for col in cursor.fetchall()}

    # 这里定义“我们希望薪酬主表必须拥有的字段”。
    # 字典的 key 是字段名。
    # 字典的 value 是字段类型和默认值。
    required_columns = {
        "perf_standard": "REAL DEFAULT 0.0",
        # 绩效工资标准。
        # 薪酬底表生成时会写这个字段。

        "history_clearance": "REAL DEFAULT 0.0",
        # 历史清算。
        # 最终结账页面会读取/更新这个字段。

        "promotion_backpay": "REAL DEFAULT 0.0",
        # 晋升补发。
        # 最终结账页面会读取/更新这个字段。
    }

    # 逐个检查必需字段。
    for column_name, column_sql in required_columns.items():

        # 如果字段已经存在，就跳过。
        # 这样重复运行 init_db.py 也不会报错。
        if column_name in existing_columns:
            continue

        # 如果字段不存在，就补上。
        # ALTER TABLE 表名 ADD COLUMN 字段名 字段类型 默认值
        cursor.execute(
            f"ALTER TABLE payroll_monthly_records ADD COLUMN {column_name} {column_sql}"
        )

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
        # 这张表是“每个月每个人的一张工资主账单”。
        # 你可以把它理解成：某个人某个月工资到底怎么算出来的，所有关键数字都放在这里。
        #
        # 为什么这次要修改这里：
        # 之前 Gemini 后来给数据库补过字段，但是 init_db.py 没同步。
        # 结果就是：你手上这个被补过的数据库能跑；重新冷启动生成的新数据库会缺字段。
        #
        # 本次新增/补齐的关键字段：
        # 1. perf_standard：绩效工资标准。薪酬页面生成底表时会写入这个字段。
        # 2. history_clearance：历史清算。薪酬最终结账页面会读取/更新这个字段。
        # 3. promotion_backpay：晋升补发。薪酬最终结账页面会读取/更新这个字段。
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payroll_monthly_records (
                -- ==============================================================
                -- 一、主键与基础锚点区
                -- ==============================================================

                record_id TEXT PRIMARY KEY,
                -- 账单流水号。
                -- 设计格式一般是：核算月份_工号，例如：2026-04_42001943。
                -- 它的作用是唯一锁定“某个人某个月”的工资记录。

                cost_month TEXT NOT NULL,
                -- 工资核算月份，格式建议固定为 YYYY-MM，例如：2026-04。
                -- 注意：这里不是发放日期，而是这张工资表归属哪个核算月。

                emp_id TEXT NOT NULL,
                -- 员工工号。
                -- 用来关联 employees 人员主表。

                dept_name TEXT,
                -- 部门名称快照。
                -- 为什么不用 dept_id？
                -- 因为工资是历史账，员工以后可能调部门。
                -- 这里保存当月生成工资时的部门名称，防止以后人员调动导致历史工资乱跳。


                -- ==============================================================
                -- 二、固定工资与固定补贴区
                -- ==============================================================

                base_salary REAL DEFAULT 0.0,
                -- 岗位工资。
                -- 通常由“岗级 + 档次”去薪酬字典/工资矩阵里查出来。

                seniority_pay REAL DEFAULT 0.0,
                -- 工龄工资。
                -- 目前薪酬页面还没完全启用，但表里先保留，后面可以按参加工作时间/入职时间计算。

                comp_subsidy REAL DEFAULT 0.0,
                -- 综合补贴。
                -- 用于存放相对固定的综合性补贴项目。

                telecom_subsidy REAL DEFAULT 0.0,
                -- 通讯补贴。
                -- 如果你们单位有固定通讯费，可以放这里。

                position_adj REAL DEFAULT 0.0,
                -- 岗位补发/扣发。
                -- 比如某些岗位差额、岗位调整补扣、特殊岗位补贴，可以放这里。

                expert_allowance REAL DEFAULT 0.0,
                -- 专家津贴或高校毕业生津贴。
                -- 这个字段用于放长期规则类津贴，也可以后续由 payroll_allowance_rules 自动汇总过来。


                -- ==============================================================
                -- 三、绩效工资计算区
                -- ==============================================================

                perf_standard REAL DEFAULT 0.0,
                -- 【本次补齐字段】
                -- 绩效工资标准。
                -- pages/3_payroll.py 里生成底表时会写入这个字段。
                -- 如果这里没有这个字段，冷启动数据库后，薪酬模块会报 no such column 或 INSERT 字段不存在。

                perf_base REAL DEFAULT 0.0,
                -- 激励包基数。
                -- 目前代码里会根据 T级，比如 T1/T2/T3，从薪酬字典里查出激励包金额。

                perf_kpi_score REAL DEFAULT 100.0,
                -- 本月 KPI/KCI 得分。
                -- 默认 100 分，表示满绩效。
                -- 页面上可以人工改，比如 95、110、120。

                perf_pack_coef REAL DEFAULT 1.0,
                -- 激励包倍数。
                -- 比如激励包按 1.0、1.2、1.3 倍发放。

                perf_leader_coef REAL DEFAULT 1.0,
                -- 负责人/领导系数。
                -- 用于领导岗位或负责人特殊绩效系数。

                perf_excel_coef REAL DEFAULT 1.0,
                -- 优才、专项政策或外部 Excel 倍数。
                -- 当前页面暂时未重点使用，但保留给后续复杂政策。

                perf_salary_calc REAL DEFAULT 0.0,
                -- 系统计算出的绩效工资。
                -- 目前页面里的大致公式是：
                -- （绩效标准 + 激励包基数 × 激励包倍数）× KPI / 100 × 负责人系数。

                perf_adj REAL DEFAULT 0.0,
                -- 绩效补发/扣发。
                -- 用于处理跨月绩效调整、考核补扣、历史月份补差等情况。


                -- ==============================================================
                -- 四、专项奖金与动态加减项区
                -- ==============================================================

                dynamic_additions TEXT DEFAULT '{}',
                -- 动态加项，JSON 文本。
                -- 为什么用 TEXT 存 JSON？
                -- 因为工资里经常会突然冒出临时项目，不可能每来一个项目就改一次数据库表。
                -- 比如：临时补贴、一次性补发、特殊奖励，都可以塞进这个 JSON。

                dynamic_deductions TEXT DEFAULT '{}',
                -- 动态减项，JSON 文本。
                -- 比如考勤扣款、其他扣款、临时扣罚等，可以放这里。

                special_bonus_total REAL DEFAULT 0.0,
                -- 专项奖金合计。
                -- payroll_special_bonus 明细表里可能有很多条奖金。
                -- 汇总后会推送到这里，作为当月工资的一部分。

                history_clearance REAL DEFAULT 0.0,
                -- 【本次补齐字段】
                -- 历史清算。
                -- 用于处理历史遗留补扣款。
                -- 例如：上月多发了，本月扣回来，可以填负数；
                -- 或者以前少发了，本月补回来，可以填正数。
                -- pages/3_payroll.py 最终结账页面会读取和更新它。

                promotion_backpay REAL DEFAULT 0.0,
                -- 【本次补齐字段】
                -- 晋升补发。
                -- 用于处理岗级/档次/岗位晋升后产生的补发金额。
                -- 例如：3月批了晋升，4月工资才补发差额，就可以放这里。


                -- ==============================================================
                -- 五、应发工资汇总区
                -- ==============================================================

                gross_salary_total REAL DEFAULT 0.0,
                -- 应发工资合计。
                -- 这是个人扣社保、扣个税之前的工资总额。
                -- 后续个税申报、工资表汇总、人工成本台账都很依赖这个字段。


                -- ==============================================================
                -- 六、社保公积金个人代扣区
                -- ==============================================================

                ss_pension_pers REAL DEFAULT 0.0,
                -- 个人养老保险扣款。
                -- 注意：这个数应该从 ss_monthly_records 社保月度记录里倒吸，不应该在薪酬模块重新计算。

                ss_medical_mix REAL DEFAULT 0.0,
                -- 个人医疗保险扣款合并项。
                -- 这里通常等于：基本医疗个人部分 + 大病医疗固定扣款。
                -- 例如你前面一直强调的 199 和 7，在薪酬展示时可以合并扣。

                ss_unemp_pers REAL DEFAULT 0.0,
                -- 个人失业保险扣款。
                -- 同样来自社保模块。

                ss_fund_pers REAL DEFAULT 0.0,
                -- 个人住房公积金扣款。
                -- 同样来自社保模块。

                ss_annuity_pers REAL DEFAULT 0.0,
                -- 个人企业年金扣款。
                -- 同样来自社保模块。


                -- ==============================================================
                -- 七、个税与实发区
                -- ==============================================================

                tax_deduction REAL DEFAULT 0.0,
                -- 代扣个人所得税。
                -- 当前系统设计里，个税不是系统自动算，而是由外部税务/财务工具算好后回灌。

                net_salary REAL DEFAULT 0.0,
                -- 实发工资。
                -- 一般公式是：
                -- 应发工资合计 - 五险两金个人扣款 - 个税。
                -- 这个就是最终打到员工银行卡的钱。


                -- ==============================================================
                -- 八、流程状态与外部系统对接区
                -- ==============================================================

                audit_status TEXT DEFAULT '草稿',
                -- 工资单状态。
                -- 例如：草稿 / 待算税 / 已封账。
                -- 后续可以用它控制工资表能不能继续改。

                oa_clearing_no TEXT,
                -- OA 清册编号。
                -- 如果后续要对接电信 OA 或者走线下清册编号，可以放这里。

                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                -- 更新时间。
                -- 用来记录这条工资账单最后一次被系统写入的时间。


                -- ==============================================================
                -- 九、约束区
                -- ==============================================================

                FOREIGN KEY (emp_id) REFERENCES employees(emp_id),
                -- 外键约束。
                -- 表示这条工资记录必须对应 employees 表里的一个员工。

                UNIQUE(cost_month, emp_id)
                -- 唯一约束。
                -- 同一个员工同一个月份，只能有一条工资主账单。
                -- 防止同月同人重复生成两条工资。
            )
        ''')

        ensure_payroll_schema_patch(cursor)

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