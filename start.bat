@echo off
:: ============================================================
:: JobWise — Start the web app
:: Double-click this file each time you want to use JobWise.
:: Keep this window open while you are using the app.
:: Close this window (or press Ctrl+C) to stop the server.
:: ============================================================

cd /d "%~dp0"

if not exist "venv\Scripts\python.exe" (
    echo.
    echo  JobWise has not been set up yet.
    echo  Please double-click setup.bat first.
    echo.
    pause
    exit /b 1
)

echo.
echo  Starting JobWise...
echo  Your browser will open automatically in a few seconds.
echo.
echo  Keep this window open while using the app.
echo  Close this window to stop the server.
echo.

:: Open browser after a short delay (runs in background, doesn't block)
start "" /b cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:6868"

call venv\Scripts\activate.bat
python run_web.py

echo.
echo  Server stopped.
pause
