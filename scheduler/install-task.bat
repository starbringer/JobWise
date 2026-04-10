@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "VENV_DIR=%SCRIPT_DIR%venv"
set "TASK_PREFIX=JobWise"

:: Must run as Administrator
net session >nul 2>&1
if errorlevel 1 (
    echo ERROR: This script must be run as Administrator.
    echo Right-click install-task.bat and choose "Run as administrator".
    pause
    exit /b 1
)

:: Remove any existing JobWise* tasks
echo Removing any existing JobWise tasks...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "Get-ScheduledTask -TaskName 'JobWise*' -ErrorAction SilentlyContinue | ForEach-Object {" ^
  "  Unregister-ScheduledTask -TaskName $_.TaskName -Confirm:$false;" ^
  "  Write-Host ('  Removed: ' + $_.TaskName)" ^
  "}"

:: Create one WakeToRun task per configured run time.
:: Each task wakes the PC from sleep, runs the full pipeline, then exits.
:: Windows manages returning to sleep based on system power settings.
echo.
echo Reading run times from config/config.yaml and installing tasks...
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%install-tasks.ps1" -ScriptDir "%SCRIPT_DIR:~0,-1%"

if errorlevel 1 (
    echo ERROR: Failed to install scheduled tasks.
    pause
    exit /b 1
)

echo.
echo Installation complete.
echo   Tasks run daily at each time in scheduler.run_times (config/config.yaml).
echo   WakeToRun: PC wakes from sleep automatically at each run time.
echo   After the pipeline finishes the PC returns to sleep per its power settings.
echo   Logs written to: %SCRIPT_DIR%logs\scheduler.log
echo.
echo   NOTE: Re-run install-task.bat after changing run_times in config.yaml.
echo.
echo To manage:
echo   Run now   : schtasks /run /tn "JobWise_1100"  (adjust suffix for your time)
echo   Status    : schtasks /query /tn "JobWise*"
echo   Uninstall : uninstall-task.bat
echo.

set /p START_NOW="Run the pipeline now? [Y/n]: "
if /i not "%START_NOW%"=="n" (
    :: Run the first JobWise task found
    for /f "tokens=2 delims=:" %%t in ('schtasks /query /fo list ^| findstr /i "Task Name:.*JobWise"') do (
        set "FIRST_TASK=%%t"
        goto :run_now
    )
    :run_now
    set "FIRST_TASK=%FIRST_TASK: =%"
    if defined FIRST_TASK (
        schtasks /run /tn "%FIRST_TASK%" >nul
        echo Pipeline started: %FIRST_TASK%
    )
)

endlocal
