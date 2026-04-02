param(
  [string]$TaskName = "FastNTFS",
  [string]$InstallDir = "$env:ProgramData\FastNTFS",
  [string]$BinDir = "$HOME\bin",
  [string]$Volumes = "",
  [string]$Address = "127.0.0.1:7788",
  [int]$FlushSeconds = 10,
  [int]$MaxRecords = 50000000,
  [switch]$SkipRebuild,
  [string]$DbPath = ""
)

$ErrorActionPreference = "Stop"

function Assert-Admin {
  $id = [Security.Principal.WindowsIdentity]::GetCurrent()
  $p = New-Object Security.Principal.WindowsPrincipal($id)
  if (-not $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Please run this script in an elevated (Administrator) PowerShell."
  }
}

function Resolve-Volumes {
  param([string]$VolumesRaw)

  if ($VolumesRaw -and $VolumesRaw.Trim()) {
    $parts = $VolumesRaw.Split(",") | ForEach-Object { $_.Trim().TrimEnd(":").ToUpper() } | Where-Object { $_ }
    $valid = @()
    foreach ($p in $parts) {
      if ($p -match "^[A-Z]$") {
        if ($valid -notcontains $p) {
          $valid += $p
        }
      } else {
        throw "Invalid volume token: $p"
      }
    }
    if ($valid.Count -eq 0) {
      throw "No valid volumes provided."
    }
    return ($valid -join ",")
  }

  # Default: select all local NTFS drives.
  $drives = Get-CimInstance Win32_LogicalDisk |
    Where-Object { $_.DriveType -eq 3 -and $_.FileSystem -eq "NTFS" -and $_.DeviceID -match "^[A-Z]:$" } |
    Select-Object -ExpandProperty DeviceID

  $letters = @($drives | ForEach-Object { $_.TrimEnd(":").ToUpper() } | Sort-Object -Unique)
  if ($letters.Count -eq 0) {
    throw "No NTFS local drives found. Please pass -Volumes explicitly."
  }
  Write-Host "Auto-selected NTFS volumes: $($letters -join ',')"
  return ($letters -join ",")
}

Assert-Admin

$RepoDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Volumes = Resolve-Volumes -VolumesRaw $Volumes
if (-not $DbPath) {
  $DbPath = Join-Path $InstallDir "everything_mvp.db"
}

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$LogDir = Join-Path $InstallDir "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
New-Item -ItemType Directory -Force -Path $BinDir | Out-Null

$ExePath = Join-Path $BinDir "fast-ntfs.exe"
Write-Host "[1/5] Building binary..."
Push-Location $RepoDir
try {
  go build -o $ExePath .
} finally {
  Pop-Location
}

if (-not (Test-Path $ExePath)) {
  throw "Build failed: $ExePath not found."
}

if (-not $SkipRebuild) {
  Write-Host "[2/5] Rebuilding full index snapshot..."
  & $ExePath rebuild --volumes $Volumes --max-records $MaxRecords --db $DbPath
  if ($LASTEXITCODE -ne 0) {
    throw "Rebuild failed with exit code $LASTEXITCODE"
  }
} else {
  Write-Host "[2/5] SkipRebuild enabled, skipping full snapshot rebuild."
}

Write-Host "[3/5] Writing service metadata..."
@"
TaskName=$TaskName
InstallDir=$InstallDir
Volumes=$Volumes
Address=$Address
FlushSeconds=$FlushSeconds
MaxRecords=$MaxRecords
SkipRebuild=$SkipRebuild
DbPath=$DbPath
ExePath=$ExePath
InstalledAt=$(Get-Date -Format s)
"@ | Set-Content -Encoding UTF8 (Join-Path $InstallDir "install.info")

Write-Host "[4/5] Registering scheduled task..."
$argList = "serve --volumes $Volumes --db `"$DbPath`" --addr $Address --flush-seconds $FlushSeconds"
$action = New-ScheduledTaskAction -Execute $ExePath -Argument $argList -WorkingDirectory $InstallDir
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -RunLevel Highest -LogonType ServiceAccount
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 0) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings | Out-Null

Write-Host "[5/5] Starting task..."
Start-ScheduledTask -TaskName $TaskName
Start-Sleep -Seconds 2

$task = Get-ScheduledTask -TaskName $TaskName
$info = Get-ScheduledTaskInfo -TaskName $TaskName

Write-Host "Installed successfully."
Write-Host "TaskName: $TaskName"
Write-Host "State:    $($task.State)"
Write-Host "LastRun:  $($info.LastRunTime)"
Write-Host "DbPath:   $DbPath"
Write-Host "Address:  $Address"
Write-Host "Check:    http://$Address/status"
