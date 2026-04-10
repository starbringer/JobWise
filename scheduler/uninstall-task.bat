@echo off
setlocal

net session >nul 2>&1
if errorlevel 1 (
    echo ERROR: This script must be run as Administrator.
    echo Right-click the file and select "Run as administrator".
    echo.
    set /p _=Press Enter to close...
    exit /b 1
)

echo ------------------------------------------------------------
echo  JobWise Uninstaller
echo ------------------------------------------------------------
echo.

echo [1/2] Removing all JobWise* scheduled tasks...

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$tasks = Get-ScheduledTask -TaskName 'JobWise*' -ErrorAction SilentlyContinue;" ^
  "if ($tasks) {" ^
  "  $tasks | ForEach-Object {" ^
  "    Write-Host ('  Removing: ' + $_.TaskName);" ^
  "    Unregister-ScheduledTask -TaskName $_.TaskName -Confirm:$false" ^
  "  }" ^
  "} else {" ^
  "  Write-Host '  No JobWise tasks found in Task Scheduler.'" ^
  "}"

echo.
echo [2/2] Verifying...

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$remaining = Get-ScheduledTask -TaskName 'JobWise*' -ErrorAction SilentlyContinue;" ^
  "if ($remaining) {" ^
  "  Write-Host 'WARNING: Some tasks still remain:';" ^
  "  $remaining | ForEach-Object { Write-Host ('  ' + $_.TaskName) };" ^
  "  Write-Host 'Remove them manually via Task Scheduler (taskschd.msc).'" ^
  "} else {" ^
  "  Write-Host 'Confirmed: all JobWise tasks removed.'" ^
  "}"

echo.
echo ------------------------------------------------------------
echo  Done.
echo ------------------------------------------------------------
echo.

set /p _=Press Enter to close...

endlocal
