@echo off
setlocal
echo ========================================
echo LearningSelfAgent reset agent data
echo ========================================
set "ROOT=%~dp0.."

rem Prefer a Windows-only venv to avoid WSL/Windows .venv contamination:
rem if pyvenv.cfg home=/usr/bin, Windows python.exe will fail with "No Python at /usr/bin\\python.exe".
set "PY_EXE=%ROOT%\.venv-win\Scripts\python.exe"
if exist "%PY_EXE%" (
  echo Using Python: %PY_EXE%
  goto :run
)

set "PY_EXE=%ROOT%\.venv\Scripts\python.exe"
if exist "%PY_EXE%" (
  rem 若该 .venv 来自 WSL（pyvenv.cfg 里 home=/usr/bin），Windows 下会报 “No Python at /usr/bin\\python.exe”。
  findstr /i "home = /usr" "%ROOT%\.venv\pyvenv.cfg" >nul 2>nul
  if not errorlevel 1 (
    echo Detected WSL venv at %ROOT%\.venv. Skipping.
    set "PY_EXE="
  )
)

if defined PY_EXE (
  echo Using Python: %PY_EXE%
  goto :run
)

rem Fallbacks: prefer Python Launcher if available (common on Windows)
where py >nul 2>nul
if %errorlevel%==0 (
  echo Using Python Launcher: py -3
  py -3 "%~dp0reset_agent_data.py"
  pause
  goto :eof
)

rem Last fallback: rely on PATH
set "PY_EXE=python"
echo Using Python (PATH): %PY_EXE%

:run
"%PY_EXE%" "%~dp0reset_agent_data.py"
pause
