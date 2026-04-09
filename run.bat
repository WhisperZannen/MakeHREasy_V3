@echo off
:: 告诉黑框：去执行虚拟环境里的激活脚本！
call .venv\Scripts\activate

:: 激活成功后，再执行运行命令！
streamlit run app.py
pause