# ==============================================================================
# 文件路径: database/init_db.py
# 功能描述: 初始化核心数据底座 (V3.5 差异快照与时空回溯版)
# 执行逻辑:
#   1. 物理隔离部门、岗位、人员与历史轨迹。
#   2. 引入外键约束，确保人员必须挂靠在真实的部门和岗位下。
#   3. 为后续“按岗定薪”和“技术等级加成”提供底层字段支撑。
#   4. [核心重构] 历史表扩容，支持新旧状态对比与数据回滚机制。
# ==============================================================================

import sqlite3
import os

def init_database():
    # 为什么这么改：使用绝对路径，防止在不同目录下运行脚本时生成的数据库文件位置错乱。
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, 'hr_core.db')

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # 实现了什么具体逻辑：开启外键检查，防止数据孤儿。
        cursor.execute("PRAGMA foreign_keys = ON;")

        # --- 表 1: 部门核心表 ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            dept_id INTEGER PRIMARY KEY AUTOINCREMENT,  
            dept_name TEXT NOT NULL UNIQUE,             
            parent_dept_id INTEGER,                     
            dept_category TEXT NOT NULL,                
            sort_order INTEGER DEFAULT 999,
            status INTEGER DEFAULT 1,                   
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
            FOREIGN KEY (parent_dept_id) REFERENCES departments(dept_id)
        )
        """)

        # --- 表 2: 岗位核心表 ---
        # 为什么这么改：岗位不能仅作为字符串存在，独立成表后可支持岗位系数。
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            pos_id INTEGER PRIMARY KEY AUTOINCREMENT,
            pos_name TEXT NOT NULL UNIQUE,              
            pos_category TEXT,                          
            sort_order INTEGER DEFAULT 999,             
            status INTEGER DEFAULT 1,                   
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # --- 表 3: 人员核心主表 ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            emp_id TEXT PRIMARY KEY,                    
            name TEXT NOT NULL,                         
            id_card TEXT UNIQUE,                        
            dept_id INTEGER NOT NULL,                   
            post_rank INTEGER,                          
            post_grade TEXT,                            
            status TEXT DEFAULT '在职',                 
            join_company_date DATE,                     
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        )
        """)

        # --- 表 4: 人员档案扩展表 ---
        # 为什么这么改：响应“技术等级 T1-T5”需求。
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS employee_profiles (
            emp_id TEXT PRIMARY KEY,                    
            pos_id INTEGER,                             
            tech_grade TEXT,                            
            title_order INTEGER DEFAULT 999,
            education_level TEXT,                       
            degree TEXT,                                
            school_name TEXT,                           
            major TEXT,                                 
            graduation_date DATE,                       
            first_job_date DATE,                        
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE,
            FOREIGN KEY (pos_id) REFERENCES positions(pos_id)
        )
        """)

        # --- 表 5: 人员历史轨迹表 ---
        # [增量详尽注释 2026-03-26]
        # 为什么这么改：光记录 old 字段无法得知用户改成什么了。工资回溯必须要有明确的前后对比。
        # 实现了什么具体逻辑：全面增加 new_ 字段，并确保能支持时空回滚。
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS personnel_changes (
            change_id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_id TEXT NOT NULL,
            change_type TEXT,            -- 变动类型(入职/调岗/调薪/离职等)
            
            old_dept_id INTEGER,         
            new_dept_id INTEGER,         -- [新增] 变动后部门
            
            old_pos_id INTEGER,          
            new_pos_id INTEGER,          -- [新增] 变动后岗位
            
            old_tech_grade TEXT,         
            new_tech_grade TEXT,         -- [新增] 变动后技术等级
            
            old_post_rank INTEGER,       
            new_post_rank INTEGER,       -- [新增] 变动后岗级
            
            old_post_grade TEXT,         
            new_post_grade TEXT,         -- [新增] 变动后档次
            
            change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
            change_reason TEXT,          -- 变动说明
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE
        )
        """)

        # --- 表 6: 专家特例表 ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS experts_plugin (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_id TEXT NOT NULL UNIQUE,                
            archive_post_rank INTEGER NOT NULL,         
            archive_post_grade TEXT NOT NULL,           
            term_start_date DATE,                       
            term_end_date DATE,                         
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE
        )
        """)

# ==============================================================================
        # 核心基建：人工成本台账大宽表 (Ledger) - 终极全颗粒度+动态扩展版
        # 实现了什么具体逻辑：
        #   1. 五险两金全量剥离：个人 5 项，企业 7 项。
        #   2. 经费彻底分流：职工教育经费、工会经费独立核算。
        #   3. 个税精准拆解：日常个税与年终奖专属个税分离，适配中国税务申报双通道。
        #   4. JSON 动态暗线与空值防御机制全面保留。
        # ==============================================================================

        # 执行建表 SQL 语句
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS labor_cost_ledger (
                -- 记录的唯一主键，由数据库自动递增生成
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- [区块 1] 时空锚点区
                -- 核算月份，格式强制要求为类似 '2026-01' 的字符串
                cost_month TEXT NOT NULL,
                -- 员工工号，导入时的唯一身份标识
                emp_id TEXT NOT NULL,
                -- 员工姓名，作为文本固化在当月账目中
                emp_name TEXT NOT NULL,
                -- 归属部门，直接读取 Excel 导入的部门名称
                dept_name TEXT NOT NULL,
                -- 员工状态，标明生成该笔账目时员工是在职、离职还是公共账目
                emp_status TEXT NOT NULL,
                
                -- [区块 2] 工资应发区
                -- 岗位工资的基础金额，默认值为 0.0
                base_salary REAL DEFAULT 0.0,
                -- 员工随工龄增长的工龄工资，默认值为 0.0
                seniority_pay REAL DEFAULT 0.0,
                -- 每月固定的综合补贴金额，默认值为 0.0
                comp_subsidy REAL DEFAULT 0.0,
                -- 与岗位及绩效挂钩的浮动补贴，默认值为 0.0
                perf_float_subsidy REAL DEFAULT 0.0,
                -- 员工的通讯费报销或补贴，默认值为 0.0
                telecom_subsidy REAL DEFAULT 0.0,
                -- 其他未分类的常规岗位工资，默认值为 0.0
                other_base_pay REAL DEFAULT 0.0,
                
                -- 给予实习生的专项生活补贴，默认值为 0.0
                intern_subsidy REAL DEFAULT 0.0,
                -- 给予高校毕业生或引进专家的津贴，默认值为 0.0
                grad_allowance REAL DEFAULT 0.0,
                
                -- 绩效工资的标准基数，仅作记录不参与汇总运算，默认值为 0.0
                perf_standard REAL DEFAULT 0.0,
                -- 员工当月的 KPI 考核得分，仅作记录，默认值为 0.0
                kpi_score REAL DEFAULT 0.0,
                -- 根据 KPI 得分核算出的实际绩效工资，默认值为 0.0
                eval_perf_pay REAL DEFAULT 0.0,
                -- 随业务量浮动的提成类绩效工资，默认值为 0.0
                commission_pay REAL DEFAULT 0.0,
                -- 其他月度发生的常规绩效工资，默认值为 0.0
                other_month_perf REAL DEFAULT 0.0,
                -- 动态绩效明细，使用 JSON 文本格式存储，默认为空字典字符串
                dynamic_perf_details TEXT DEFAULT '{}',
                
                -- 专项奖金，包含考勤扣款也会折算在此处，默认值为 0.0
                special_award REAL DEFAULT 0.0,
                -- 年底发放的全年一次性绩效奖金，默认值为 0.0
                year_end_bonus REAL DEFAULT 0.0,
                -- 其他未分类的临时专项奖金，默认值为 0.0
                other_special_award REAL DEFAULT 0.0,
                -- 动态奖励明细，使用 JSON 文本格式存储，默认为空字典字符串
                dynamic_award_details TEXT DEFAULT '{}',
                
                -- 工资应发合计，由区块2内所有有效发放金额相加得出，默认值为 0.0
                gross_salary_total REAL DEFAULT 0.0,
                
                -- [区块 3] 个人扣除与实发区
                -- 员工个人承担的养老保险扣款，默认值为 0.0
                pension_personal REAL DEFAULT 0.0,
                -- 员工个人承担的医疗保险扣款，默认值为 0.0
                medical_personal REAL DEFAULT 0.0,
                -- 员工个人承担的失业保险扣款，默认值为 0.0
                unemployment_personal REAL DEFAULT 0.0,
                -- 员工个人承担的住房公积金扣款，默认值为 0.0
                provident_fund_personal REAL DEFAULT 0.0,
                -- 员工个人承担的企业年金扣款，默认值为 0.0
                annuity_personal REAL DEFAULT 0.0,
                -- 员工日常综合所得部分的个人所得税，默认值为 0.0
                tax_personal_month REAL DEFAULT 0.0,
                -- 员工年终奖单独计税部分的个人所得税，默认值为 0.0
                tax_personal_bonus REAL DEFAULT 0.0,
                -- 员工当月最终进入银行卡的实发总额，默认值为 0.0
                net_salary REAL DEFAULT 0.0,
                
                -- [区块 4] 其他人工成本区
                -- 企业承担的养老保险统筹金额，默认值为 0.0
                pension_company REAL DEFAULT 0.0,
                -- 企业承担的医疗保险统筹金额，默认值为 0.0
                medical_company REAL DEFAULT 0.0,
                -- 企业承担的失业保险统筹金额，默认值为 0.0
                unemployment_company REAL DEFAULT 0.0,
                -- 企业承担的工伤保险统筹金额，默认值为 0.0
                work_injury_company REAL DEFAULT 0.0,
                -- 企业承担的生育保险统筹金额，默认值为 0.0
                maternity_company REAL DEFAULT 0.0,
                -- 企业承担的住房公积金金额，默认值为 0.0
                provident_fund_company REAL DEFAULT 0.0,
                -- 企业承担的年金金额，默认值为 0.0
                annuity_company REAL DEFAULT 0.0,
                
                -- 分摊至该员工的日常工作用餐成本，默认值为 0.0
                meal_daily REAL DEFAULT 0.0,
                -- 分摊至该员工的加班用餐成本，默认值为 0.0
                meal_ot REAL DEFAULT 0.0,
                -- 发放给员工的各项慰问金费用，默认值为 0.0
                welfare_condolence REAL DEFAULT 0.0,
                -- 依法发放的独生子女专项补贴，默认值为 0.0
                welfare_single_child REAL DEFAULT 0.0,
                -- 员工年度常规体检产生的费用，默认值为 0.0
                welfare_health_check REAL DEFAULT 0.0,
                -- 新员工入职产生的体检报销费用，默认值为 0.0
                welfare_entry_check REAL DEFAULT 0.0,
                -- 其他未分类的临时福利费用，默认值为 0.0
                welfare_other REAL DEFAULT 0.0,
                -- 夏季发放的防暑降温专项补贴，默认值为 0.0
                allowance_heat REAL DEFAULT 0.0,
                -- 针对女职工发放的特殊劳保费用，默认值为 0.0
                allowance_women REAL DEFAULT 0.0,
                -- 企业为员工统一缴纳的补充医疗保险费用，默认值为 0.0
                medical_supplement REAL DEFAULT 0.0,
                -- 按比例提取并划拨给工会的专用经费，默认值为 0.0
                union_funds REAL DEFAULT 0.0,
                -- 按比例提取的职工教育与培训专用经费，默认值为 0.0
                edu_funds REAL DEFAULT 0.0,
                
                -- 用于人工抹平财务几分钱误差的微调金额，默认值为 0.0
                cost_adjustment REAL DEFAULT 0.0,
                
                -- [新增核心列] 其他人工成本合计，由区块4各项(含平账)相加得出，默认值为 0.0
                other_cost_total REAL DEFAULT 0.0,
                
                -- [区块 5] 成本终局算总区
                -- 人工成本最终合计总数 (等于 gross_salary_total + other_cost_total)，默认值为 0.0
                total_labor_cost REAL DEFAULT 0.0,
                
                -- 记录该条台账数据写入数据库的系统当前时间戳
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- 设置复合唯一约束，确保同月同工号只能有一条记录以支持更新操作
                UNIQUE(cost_month, emp_id)
            )
        """)

        conn.commit()
        print(f"✅ V3.5 差异快照底座初始化成功！")
    except sqlite3.Error as e:
        conn.rollback()
        print(f"❌ 初始化失败: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_database()