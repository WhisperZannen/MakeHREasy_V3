import subprocess
import pandas as pd


def get_git_changelog(limit=20):
    """
    通过 subprocess 调用系统 git 命令，获取最近的提交记录
    """
    try:
        # 指令解释：
        # -n {limit}: 只取最近的 N 条
        # --pretty=format: "%ad | %s" -> 格式化为 "日期 | 提交内容"
        # --date=format:"%Y-%m-%d %H:%M" -> 优化日期格式
        cmd = ["git", "log", f"-n {limit}", "--pretty=format:%ad | %s", "--date=format:%Y-%m-%d %H:%M"]
        result = subprocess.check_output(cmd, encoding='utf-8')

        # 将结果解析为列表
        logs = []
        for line in result.strip().split('\n'):
            if '|' in line:
                date_str, msg = line.split('|', 1)
                logs.append({"时间": date_str.strip(), "更新内容": msg.strip()})

        return pd.DataFrame(logs)
    except Exception as e:
        # 如果环境没 Git 或报错，返回空表并打印
        print(f"Git Log 获取失败: {e}")
        return pd.DataFrame(columns=["时间", "更新内容"])