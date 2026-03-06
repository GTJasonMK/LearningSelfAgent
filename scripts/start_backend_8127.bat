@echo off
cd /d E:\Code\LearningSelfAgent
start "" /b E:\Code\LearningSelfAgent\.venv-win\Scripts\python.exe -m uvicorn backend.src.main:app --host 127.0.0.1 --port 8127 > E:\Code\LearningSelfAgent\backend\data\uvicorn_8127.log 2>&1
exit /b 0
