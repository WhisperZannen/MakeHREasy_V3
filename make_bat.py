import sys
import os

# 构建无敌版 bat 脚本的内容
bat_content = f"""@echo off
:: 强行把运行目录锁定在当前项目文件夹，绝不迷路！
cd /d "%~dp0"

echo [MakeHREasy] Igniting Core Engine...
echo Using Python at: {sys.executable}
echo --------------------------------------------------

:: 使用你的专属绝对路径，直接暴力拉起系统！无视任何环境配置！
"{sys.executable}" -m streamlit run app.py

pause
"""

# 用系统默认编码写入，彻底断绝乱码可能
with open("run.bat", "w") as f:
    f.write(bat_content)

print(f"✅ 无敌版 run.bat 已成功生成！")
print(f"👉 它已经记住了你的真身路径：{sys.executable}")
print("你可以去双击桌面上的 run.bat 了！")