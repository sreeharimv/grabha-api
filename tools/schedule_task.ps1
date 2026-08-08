# Grabha Cookie Refresh — scheduled task registration
# Invoked by schedule_task.bat. Run as Administrator.
#
# Registers a single task with two triggers (every 3 days at 9 AM, and on
# every logon) plus StartWhenAvailable, so a run missed because the laptop
# was off gets caught up automatically at the next logon instead of waiting
# up to 3 more days for the next scheduled slot.

$ErrorActionPreference = 'Stop'

$TaskName     = 'Grabha Cookie Refresh'
$OldLogonTask = 'Grabha Cookie Refresh (Logon)'
$ScriptPath   = '/home/sreeh007/wslprojects/grabha/grabha-api/tools/refresh_cookies.py'

# Remove the previous split registration (daily task + separate logon task)
# so it doesn't keep running alongside this merged one.
Unregister-ScheduledTask -TaskName $OldLogonTask -Confirm:$false -ErrorAction SilentlyContinue
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

$Action = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument "-WindowStyle Hidden -NonInteractive -Command `"wsl python3 $ScriptPath`""

$TriggerDaily = New-ScheduledTaskTrigger -Daily -DaysInterval 3 -At 9:00AM
$TriggerLogon = New-ScheduledTaskTrigger -AtLogOn

$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -RunOnlyIfNetworkAvailable:$false

$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

Register-ScheduledTask -TaskName $TaskName `
    -Action $Action -Trigger @($TriggerDaily, $TriggerLogon) `
    -Settings $Settings -Principal $Principal -Force | Out-Null

Write-Host "[grabha] '$TaskName' registered: fires every 3 days at 9:00 AM and on every logon, and now catches up automatically if a run was missed while the laptop was off."
