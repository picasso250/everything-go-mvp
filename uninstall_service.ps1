param(
  [string]$TaskName = "FastNTFS",
  [switch]$RemoveFiles,
  [string]$InstallDir = "$env:ProgramData\FastNTFS",
  [string]$BinDir = "$HOME\bin"
)

$ErrorActionPreference = "Stop"

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
  Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
  Write-Host "Removed scheduled task: $TaskName"
} else {
  Write-Host "Task not found: $TaskName"
}

if ($RemoveFiles) {
  $ExePath = Join-Path $BinDir "fast-ntfs.exe"
  if (Test-Path $InstallDir) {
    Remove-Item -LiteralPath $InstallDir -Recurse -Force
    Write-Host "Removed install directory: $InstallDir"
  } else {
    Write-Host "Install directory not found: $InstallDir"
  }

  if (Test-Path $ExePath) {
    Remove-Item -LiteralPath $ExePath -Force
    Write-Host "Removed binary: $ExePath"
  } else {
    Write-Host "Binary not found: $ExePath"
  }
}
