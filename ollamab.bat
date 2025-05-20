@echo off
setlocal
chcp 65001 2>nul >nul
setlocal Enabledelayedexpansion

for %%I in ("%~dp0.") do set "OWNPY_DIR=%%~fI"
set "PYTHON_SCRIPT=%OWNPY_DIR%\main.py"
rem echo %PYTHON_SCRIPT%

rem pyollamab: venv virtual environment directory
call %OWNPY_DIR%\pyollamab\Scripts\activate.bat
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo error: Virtual environment's python.exe not found.
    echo expected path: "%OWNPY_DIR%\pyollamab\Scripts\python.exe"
    pause
    exit /b 1
)
which pip
which python

echo ---------------------------------
python "%PYTHON_SCRIPT%" %*

endlocal
