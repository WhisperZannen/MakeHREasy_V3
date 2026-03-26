# ==============================================================================
# 文件路径: database/init_db.py
# 功能描述: 初始化极简核心数据底座 (主表 + 扩展表 + 特例表)
# 执行逻辑:
#   1. 连接 SQLite 数据库 (自动创建 hr_core.db)。
#   2. 强制开启外键约束，保证底层数据的物理一致性。
#   3. 创建 departments (部门核心)
#   4. 创建 employees (发薪核心主表 - 极致精简)
#   5. 创建 employee_profiles (人事档案扩展表 - 1对1隔离通用数据)
#   6. 创建 experts_plugin (专家特例表 - 插件化隔离极少数双轨制数据)
# ==============================================================================

import sqlite3
import os

def init_database():
    # 1. 绝对路径定位：确保无论在哪运行该脚本，db文件都精准生成在 database/ 目录下
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, 'hr_core.db')

    # 2. 建立数据库物理连接
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # 3. 核心防御：强制开启 SQLite 外键约束 (默认关闭)
        # 意义：从数据库引擎层面，杜绝“员工被分配到一个不存在的部门”这种致命幽灵数据。
        cursor.execute("PRAGMA foreign_keys = ON;")

        # ======================================================================
        # 表 1: 部门核心表 (departments)
        # 业务逻辑: 仅记录公司组织架构树，不掺杂任何清算系数等业务数据。
        # ======================================================================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            dept_id INTEGER PRIMARY KEY AUTOINCREMENT,  -- 唯一锚点：自增整数，部门改名不影响关联
            dept_name TEXT NOT NULL UNIQUE,             -- 部门名称：全局唯一，不可重名
            parent_dept_id INTEGER,                     -- 父级ID：用于构建上下级树状关系
            dept_category TEXT NOT NULL,                -- 部门属性：如生产、管控(后续分类清算用)
            status INTEGER DEFAULT 1,                   -- 状态位：1正常, 0撤销 (严禁物理删除部门)
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 数据落表时间
            
            -- 外键声明：父级部门必须存在于本表中
            FOREIGN KEY (parent_dept_id) REFERENCES departments(dept_id)
        )
        """)

        # ======================================================================
        # 表 2: 人员核心主表 (employees)
        # 业务逻辑: 【算薪绝密底座】。只保留身份识别与当期算薪必须的硬核字段。
        # ======================================================================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            emp_id TEXT PRIMARY KEY,                    -- 物理主键：员工工号 (绝对不随调岗变动)
            name TEXT NOT NULL,                         -- 姓名
            id_card TEXT UNIQUE,                        -- 身份证号 (全局唯一，防重复录入)
            dept_id INTEGER NOT NULL,                   -- 归属部门：必须对应真实部门
            post_rank INTEGER,                          -- 【核心】实际发放岗级 (如 17)
            post_grade TEXT,                            -- 【核心】实际发放档次 (如 'A')
            status TEXT DEFAULT '在职',                 -- 状态 (在职/离职)
            join_company_date DATE,                     -- 本公司入职日期 (计算司龄基础)
            
            -- 外键声明：人员挂靠的部门ID必须在 departments 表中真实存在
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        )
        """)

        # ======================================================================
        # 表 3: 人员档案扩展表 (employee_profiles)
        # 业务逻辑: 【垂直拆分】。存放竞聘、盘点、工龄等 HR 日常管理所需，但算薪不依赖的数据。
        # 架构优势: 随时可增删字段，绝不污染发薪主表。
        # ======================================================================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS employee_profiles (
            emp_id TEXT PRIMARY KEY,                    -- 主键同为主表工号，形成绝对的 1对1 关系
            job_title TEXT,                             -- 岗位名称 (如：项目经理、研发工程师)
            education_level TEXT,                       -- 学历 (如：本科、硕士)
            degree TEXT,                                -- 学位 (如：学士、工程硕士)
            school_name TEXT,                           -- 毕业院校
            
            -- [增量详尽注释 2026-03-26]
            -- 为什么这么改：学历和学校只能证明教育层级和平台，在人才选聘和盘点中，"所学专业"是评估员工是否具备特定岗位底层知识结构(如研发科班、财务科班)的核心依据，必须作为独立字段记录。
            -- 实现了什么具体逻辑：在扩展表中新增 major (文本类型) 字段，与学历、学校形成完整的高校教育背景数据链。
            major TEXT,                                 -- 所学专业 (如：计算机科学与技术、人力资源管理)
            
            graduation_date DATE,                       -- 毕业时间
            first_job_date DATE,                        -- 首次参加工作时间 (计算法定年假、社会工龄)
            
            -- 外键级联：主表如果物理删除了该员工，这边的档案扩展记录自动跟随销毁
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE
        )
        """)

        # ======================================================================
        # 表 4: 专家特例表 (experts_plugin)
        # 业务逻辑: 【插件化设计】。仅容纳全公司极少数专家的“封存档案”特例，不让主表陪跑。
        # ======================================================================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS experts_plugin (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT, -- 插件表独立自增主键
            emp_id TEXT NOT NULL UNIQUE,                 -- 专家工号 (确保同一时间仅一条有效特例)
            archive_post_rank INTEGER NOT NULL,          -- 封存的底层档案岗级 (如 11)
            archive_post_grade TEXT NOT NULL,            -- 封存的底层档案档次 (如 'B')
            term_start_date DATE,                        -- 专家聘期开始时间
            term_end_date DATE,                          -- 专家聘期结束时间
            
            -- 外键级联：防幽灵数据
            FOREIGN KEY (emp_id) REFERENCES employees(emp_id) ON DELETE CASCADE
        )
        """)

        # 4. 提交全部物理创建操作
        conn.commit()
        print(f"✅ V3 极简核心数据底座初始化成功！数据库路径: {db_path}")

    except sqlite3.Error as e:
        # 5. 失败回滚：任何一张表创建失败，全部撤销，防止出现半残缺的数据库结构
        conn.rollback()
        print(f"❌ 数据库初始化失败，底层 SQL 报错信息: {e}")

    finally:
        # 6. 安全释放：解除对 db 文件的物理占用
        conn.close()

if __name__ == "__main__":
    init_database()