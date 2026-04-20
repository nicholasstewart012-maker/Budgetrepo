@echo off
setlocal

rem Start Spark backend and frontend from the workspace root.
set "ROOT=%~dp0"
set "BACKEND_DIR=%ROOT%backend"
set "FRONTEND_DIR=%ROOT%spark-react"
set "BACKEND_PY=%BACKEND_DIR%\venv\Scripts\python.exe"
set "UVICORN_HOST=0.0.0.0"
set "UVICORN_PORT=8000"

if exist "%BACKEND_PY%" (
    set "PYTHON_CMD=%BACKEND_PY%"
) else (
    set "PYTHON_CMD=python"
)

if not exist "%BACKEND_DIR%\main.py" (
    echo Backend entrypoint not found: "%BACKEND_DIR%\main.py"
    exit /b 1
)

if not exist "%FRONTEND_DIR%\package.json" (
    echo Frontend package.json not found: "%FRONTEND_DIR%\package.json"
    exit /b 1
)

start "Spark Backend" cmd /k "cd /d ""%BACKEND_DIR%"" && ""%PYTHON_CMD%"" -m uvicorn main:app --host %UVICORN_HOST% --port %UVICORN_PORT% --reload"
start "Spark Frontend" cmd /k "cd /d ""%FRONTEND_DIR%"" && npm run dev"

echo Spark backend and frontend are starting.
