; ============================================================
; JobWise — Windows Installer (NSIS)
; Built by GitHub Actions and attached to each GitHub Release.
; Users download JobWise-Setup.exe and double-click it.
; No Git, no manual downloads, no command line needed.
; ============================================================

!include "MUI2.nsh"
!include "LogicLib.nsh"

; ── Metadata ────────────────────────────────────────────────
Name "JobWise"
OutFile "..\..\JobWise-Setup.exe"
InstallDir "$LOCALAPPDATA\JobWise"
InstallDirRegKey HKCU "Software\JobWise" "InstallDir"
RequestExecutionLevel user   ; no admin required

; ── UI pages ────────────────────────────────────────────────
!define MUI_ABORTWARNING
!define MUI_WELCOMEPAGE_TITLE "Welcome to JobWise"
!define MUI_WELCOMEPAGE_TEXT "This wizard will install JobWise on your computer.$\r$\n$\r$\nJobWise is a self-hosted, AI-powered job search assistant. After installation a setup wizard will walk you through getting your API keys and creating your profile.$\r$\n$\r$\nClick Next to continue."
!define MUI_FINISHPAGE_TITLE "Installation Complete"
!define MUI_FINISHPAGE_TEXT "JobWise has been installed.$\r$\n$\r$\nThe Setup Wizard will now open to finish configuring your account. Follow the on-screen instructions.$\r$\n$\r$\nTo open JobWise in future, double-click the shortcut on your Desktop."
!define MUI_FINISHPAGE_RUN "$INSTDIR\start.bat"
!define MUI_FINISHPAGE_RUN_TEXT "Start JobWise now"

!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_LANGUAGE "English"

; ── Install section ──────────────────────────────────────────
Section "JobWise" SecMain
  SectionIn RO   ; required section

  SetOutPath "$INSTDIR"

  ; ── Copy all project files ─────────────────────────────
  ; (GitHub Actions pre-cleans the source tree: no venv, no .git, no .env)
  File /r "source\*.*"

  ; ── Desktop & Start Menu shortcuts ────────────────────
  CreateShortCut "$DESKTOP\JobWise.lnk" "$INSTDIR\start.bat" "" "$INSTDIR\docs\img\icon.ico" 0
  CreateDirectory "$SMPROGRAMS\JobWise"
  CreateShortCut "$SMPROGRAMS\JobWise\JobWise.lnk"       "$INSTDIR\start.bat"         "" "$INSTDIR\docs\img\icon.ico" 0
  CreateShortCut "$SMPROGRAMS\JobWise\Uninstall.lnk"     "$INSTDIR\Uninstall.exe"
  CreateShortCut "$SMPROGRAMS\JobWise\JobWise Folder.lnk" "$INSTDIR"

  ; ── Write uninstaller ─────────────────────────────────
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\JobWise" "DisplayName"          "JobWise"
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\JobWise" "UninstallString"       "$INSTDIR\Uninstall.exe"
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\JobWise" "InstallLocation"       "$INSTDIR"
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\JobWise" "Publisher"             "JobWise"
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\JobWise" "NoModify"              "1"
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\JobWise" "NoRepair"              "1"
  WriteUninstaller "$INSTDIR\Uninstall.exe"

  ; ── Check Python ──────────────────────────────────────
  nsExec::ExecToStack 'python --version'
  Pop $0   ; exit code
  ${If} $0 != 0
    ; Python not found — try winget (built into Windows 10/11)
    DetailPrint "Python not found. Installing via winget..."
    nsExec::ExecToLog 'winget install -e --id Python.Python.3.11 --accept-source-agreements --accept-package-agreements'
    Pop $0
    ${If} $0 != 0
      MessageBox MB_OK|MB_ICONINFORMATION \
        "Python could not be installed automatically.$\r$\n$\r$\nPlease install Python 3.11 (or later) from:$\r$\n  https://www.python.org/downloads/$\r$\n$\r$\nIMPORTANT: tick 'Add Python to PATH' during installation.$\r$\n$\r$\nAfter installing Python, double-click start.bat in$\r$\n  $INSTDIR$\r$\nto launch the Setup Wizard."
      Goto done
    ${EndIf}
  ${EndIf}

  ; ── Run setup wizard ──────────────────────────────────
  DetailPrint "Running JobWise Setup Wizard..."
  ExecWait '"$SYSDIR\cmd.exe" /k "cd /d "$INSTDIR" && python setup_wizard.py"'

  done:
SectionEnd

; ── Uninstall section ────────────────────────────────────────
Section "Uninstall"
  ; Remove files (leave data/profiles intact so user doesn't lose their data)
  RMDir /r "$INSTDIR\src"
  RMDir /r "$INSTDIR\web"
  RMDir /r "$INSTDIR\scheduler"
  RMDir /r "$INSTDIR\tests"
  RMDir /r "$INSTDIR\docs"
  RMDir /r "$INSTDIR\config"
  RMDir /r "$INSTDIR\venv"
  RMDir /r "$INSTDIR\__pycache__"
  Delete   "$INSTDIR\*.py"
  Delete   "$INSTDIR\*.bat"
  Delete   "$INSTDIR\*.sh"
  Delete   "$INSTDIR\*.txt"
  Delete   "$INSTDIR\*.md"
  Delete   "$INSTDIR\LICENSE"
  Delete   "$INSTDIR\Uninstall.exe"

  ; Remove shortcuts
  Delete "$DESKTOP\JobWise.lnk"
  RMDir /r "$SMPROGRAMS\JobWise"

  ; Remove registry key
  DeleteRegKey HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\JobWise"
  DeleteRegKey HKCU "Software\JobWise"

  ; Note: $INSTDIR itself, profiles/, and data/ are intentionally kept so
  ; the user's saved jobs and profile are not lost on uninstall.
  MessageBox MB_OK "JobWise has been uninstalled.$\r$\nYour profiles and job data in$\r$\n  $INSTDIR$\r$\nhave been kept. Delete that folder manually if you want to remove them."
SectionEnd
