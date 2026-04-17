@echo off
cd /d "%~dp0"
echo [MakeHREasy] System Initializing...
echo --------------------------------------------------

:: Check if environment exists, if yes, skip to RUN
if exist ".venv\Scripts\activate.bat" goto RUN

echo [MakeHREasy] Building New Environment (.venv)...
python -m venv .venv

echo [MakeHREasy] Installing Core Modules...
call .venv\Scripts\activate.bat
pip install streamlit pandas openpyxl python-docx -i https://pypi.tuna.tsinghua.edu.cn/simple
goto RUN

:RUN
echo [MakeHREasy] Environment Ready. Igniting Engine...
call .venv\Scripts\activate.bat
python -m streamlit run app.py

pause