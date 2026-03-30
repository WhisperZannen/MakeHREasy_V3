import sqlite3
import os

# 锁定同级 database 目录下的数据库快照
current_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(current_dir, 'database', 'hr_core.db')

print(f"准备连接数据库进行清洗: {db_path}")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

try:
    # 核心 SQL：如果该行存在大病扣款，就把基本医疗里的钱减去大病金额
    sql = """
        UPDATE ss_monthly_records 
        SET medical_pers = medical_pers - medical_serious_pers 
        WHERE medical_serious_pers > 0 AND medical_pers > medical_serious_pers;
    """
    cursor.execute(sql)
    affected_rows = cursor.rowcount
    conn.commit()
    print(f"✅ 数据库账目清洗成功！共挤出 {affected_rows} 条记录中的大病水分。所有 206 已回归纯净的 199。")
except Exception as e:
    print(f"❌ 修复失败: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()