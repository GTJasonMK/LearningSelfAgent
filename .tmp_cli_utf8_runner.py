import subprocess
import sys
cmd = [
    r'E:\Code\LearningSelfAgent\.venv-win\Scripts\python.exe',
    r'E:\Code\LearningSelfAgent\scripts\lsa.py',
    '--json',
    'ask',
    '请你帮我收集最近三个月的黄金的价格数据，单位元/克，并保存为csv文件',
]
res = subprocess.run(cmd)
sys.exit(res.returncode)
