# install-tasks.ps1 — Creates one WakeToRun scheduled task per run_time configured
# in config/config.yaml. Each task wakes the PC from sleep, runs run_scheduled.py
# (which fetches jobs and scores them), then exits. Windows returns the PC to sleep
# per its own power settings after the task completes.
#
# Called by install-task.bat. Run as Administrator.

param(
    [string]$ScriptDir = $PSScriptRoot
)

# ScriptDir is the scheduler/ subfolder; project root is one level up
$ProjectRoot = Split-Path $ScriptDir -Parent
$ConfigFile  = Join-Path $ProjectRoot "config\config.yaml"
$Pythonw     = Join-Path $ProjectRoot "venv\Scripts\pythonw.exe"
$RunScript   = Join-Path $ScriptDir "run_scheduled.py"
$TaskPrefix  = "JobFinder"

# ── Validate prerequisites ─────────────────────────────────────────────────────
if (-not (Test-Path $ConfigFile)) {
    Write-Error "config.yaml not found: $ConfigFile"
    exit 1
}

if (-not (Test-Path $Pythonw)) {
    Write-Error "pythonw.exe not found: $Pythonw — run setup.bat first."
    exit 1
}

# ── Parse run_times from config.yaml ──────────────────────────────────────────
$yaml  = Get-Content $ConfigFile -Raw
$match = [regex]::Match($yaml, 'run_times:\s*\[([^\]]+)\]')
if (-not $match.Success) {
    Write-Error "Could not parse run_times from config.yaml"
    exit 1
}

$times = $match.Groups[1].Value -split ',' |
    ForEach-Object { $_.Trim().Trim('"').Trim("'") } |
    Where-Object { $_ -match '^\d{1,2}:\d{2}$' }

if (-not $times) {
    Write-Error "No valid run_times found in config.yaml (expected format: HH:MM)"
    exit 1
}

# ── Create one task per run time ───────────────────────────────────────────────
foreach ($t in $times) {
    $parts    = $t -split ':'
    $hour     = [int]$parts[0]
    $min      = [int]$parts[1]
    $taskName = "{0}_{1:D2}{2:D2}" -f $TaskPrefix, $hour, $min
    $timeStr  = "{0:D2}:{1:D2}:00" -f $hour, $min

    $action    = New-ScheduledTaskAction `
                    -Execute $Pythonw `
                    -Argument "`"$RunScript`"" `
                    -WorkingDirectory $ProjectRoot

    $trigger   = New-ScheduledTaskTrigger -Daily -At $timeStr

    # WakeToRun: Windows wakes the PC from sleep to run this task.
    # ExecutionTimeLimit: kill if still running after 30 minutes (safety net).
    $settings  = New-ScheduledTaskSettingsSet `
                    -WakeToRun `
                    -ExecutionTimeLimit '00:30:00' `
                    -RunOnlyIfNetworkAvailable `
                    -StartWhenAvailable   # run at next opportunity if PC was off at trigger time

    $principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest

    Register-ScheduledTask `
        -TaskName  $taskName `
        -Action    $action `
        -Trigger   $trigger `
        -Settings  $settings `
        -Principal $principal `
        -Force | Out-Null

    Write-Host "  Installed '$taskName' — runs daily at $timeStr, wakes PC from sleep."
}
