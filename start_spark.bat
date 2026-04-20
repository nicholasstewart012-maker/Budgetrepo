@echo off
setlocal enabledelayedexpansion

rem Launch Spark backend and frontend from the workspace root.
set "ROOT=%~dp0"
set "BACKEND_DIR=%ROOT%backend"
set "FRONTEND_DIR=%ROOT%spark-react"
set "BACKEND_PY=%BACKEND_DIR%\venv\Scripts\python.exe"
set "UVICORN_HOST=0.0.0.0"
set "UVICORN_PORT=8000"
set "UVICORN_RELOAD=1"
set "HEALTH_URL=http://127.0.0.1:8000/health"
set "WAIT_SECONDS=90"

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

echo Starting Spark backend...
if "%UVICORN_RELOAD%"=="1" (
    start "Spark Backend" cmd /k "cd /d ""%BACKEND_DIR%"" && ""%PYTHON_CMD%"" -m uvicorn main:app --host %UVICORN_HOST% --port %UVICORN_PORT% --reload"
) else (
    start "Spark Backend" cmd /k "cd /d ""%BACKEND_DIR%"" && ""%PYTHON_CMD%"" -m uvicorn main:app --host %UVICORN_HOST% --port %UVICORN_PORT%"
)

echo Waiting for backend health check at %HEALTH_URL% ...
set /a ELAPSED=0
:wait_backend
powershell -NoProfile -Command "try { (Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 '%HEALTH_URL%').StatusCode -eq 200 } catch { $false }" | findstr /i "True" >nul
if not errorlevel 1 goto backend_ready

timeout /t 1 /nobreak >nul
set /a ELAPSED+=1
if %ELAPSED% LSS %WAIT_SECONDS% goto wait_backend

echo Backend did not become healthy within %WAIT_SECONDS% seconds.
echo You can keep waiting in the backend window, but the frontend will not be started automatically.
exit /b 1

:backend_ready
echo Backend is healthy. Starting frontend...
echo Starting Spark frontend...
start "Spark Frontend" cmd /k "cd /d ""%FRONTEND_DIR%"" && npm run dev"

echo.
echo Spark is starting in two separate windows.
echo Frontend: http://localhost:5173
echo Backend:  http://localhost:8000
echo.
