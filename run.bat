@echo off
:: 强行把运行目录锁定在当前项目文件夹，绝不迷路！
cd /d "%~dp0"

echo [MakeHREasy] Igniting Core Engine...
echo Using Python at: C:\Users\73157\AppData\Local\Programs\Python\Python314\python.exe
echo --------------------------------------------------

:: 使用你的专属绝对路径，直接暴力拉起系统！无视任何环境配置！
"C:\Users\73157\AppData\Local\Programs\Python\Python314\python.exe" -m streamlit run app.py

pause
