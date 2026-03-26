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

        conn.commit()
        print(f"✅ V3.5 差异快照底座初始化成功！")
    except sqlite3.Error as e:
        conn.rollback()
        print(f"❌ 初始化失败: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_database()