@echo off
:: ============================================================
:: JobWise Setup — One-Click Installer
:: Double-click this file to set up JobWise for the first time.
:: ============================================================

cd /d "%~dp0"

:: Check Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo  Python was not found on this computer.
    echo.
    echo  Please install Python 3.11 or later from:
    echo    https://www.python.org/downloads/
    echo.
    echo  IMPORTANT: During installation, tick the checkbox that says
    echo             "Add Python to PATH" before clicking Install.
    echo.
    echo  After installing Python, double-click setup.bat again.
    echo.
    pause
    exit /b 1
)

python setup_wizard.py
if errorlevel 1 (
    echo.
    echo  Setup did not complete successfully.
    echo  See the messages above for details.
    echo.
    pause
)
