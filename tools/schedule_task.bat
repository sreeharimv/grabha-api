@echo off
:: Schedules refresh_cookies.py to run every 3 days at 9 AM
:: Also runs at login so missed runs are caught if machine was off
:: Run this from an Administrator Command Prompt

schtasks /create /tn "Grabha Cookie Refresh" ^
  /tr "wsl python3 /home/sreeh007/wslprojects/grabha/tools/refresh_cookies.py" ^
  /sc daily /mo 3 /st 09:00 ^
  /rl highest /f ^
  /ru "%USERNAME%"

schtasks /create /tn "Grabha Cookie Refresh (Logon)" ^
  /tr "wsl python3 /home/sreeh007/wslprojects/grabha/tools/refresh_cookies.py" ^
  /sc onlogon ^
  /rl highest /f ^
  /ru "%USERNAME%"

if %errorlevel% == 0 (
    echo [grabha] Tasks scheduled successfully.
    echo [grabha] Runs every 3 days at 9:00 AM + on every login.
) else (
    echo [grabha] Failed to schedule task. Run as Administrator.
)
pause
