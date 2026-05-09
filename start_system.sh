#!/bin/bash

# 1. 自动定位到脚本所在文件夹
cd "$(dirname "$0")"

echo "🚀 [MakeHREasy] 正在唤醒 Mac 核心引擎..."
echo "--------------------------------------------------"

# 2. 检查 Python 环境
# Mac 自带 Python，但我们需要确保使用 python3
PYTHON_EXE=$(which python3)

if [ -z "$PYTHON_EXE" ]; then
    echo "❌ 错误：未检测到 Python3。请先安装 Python 3.10 以上版本。"
    exit 1
fi

# 3. 处理虚拟环境 (.venv)
if [ ! -d ".venv" ]; then
    echo "📦 正在为 Mac 锻造局部隔离环境 (.venv)..."
    python3 -m venv .venv
    source .venv/bin/activate
    echo "📥 正在极速安装核心零件 (清华源加速)..."
    pip install streamlit pandas openpyxl python-docx xlsxwriter -i https://pypi.tuna.tsinghua.edu.cn/simple
else
    echo "✅ 环境已就绪，正在接入算力池..."
    source .venv/bin/activate
fi

# 4. 启动项目
echo "--------------------------------------------------"
echo "⏳ 正在拉起 Streamlit 视图，请稍后..."
streamlit run app.py