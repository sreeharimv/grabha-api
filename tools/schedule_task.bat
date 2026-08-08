@echo off
:: Registers the Grabha Cookie Refresh scheduled task (see schedule_task.ps1)
:: Runs every 3 days at 9 AM, on every logon, and catches up a missed run
:: automatically (StartWhenAvailable) instead of waiting for the next slot.
:: Run this from an Administrator Command Prompt.

powershell.exe -ExecutionPolicy Bypass -File "%~dp0schedule_task.ps1"

if %errorlevel% == 0 (
    echo [grabha] Task scheduled successfully.
) else (
    echo [grabha] Failed to schedule task. Run as Administrator.
)
pause
