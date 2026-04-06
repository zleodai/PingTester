param(
    [string]$InternetTarget = "8.8.8.8",
    [double]$Interval = 1.0,
    [int]$TimeoutMs = 1000,
    [int]$HighPingThresholdMs = 150,
    [int]$Port = 8765,
    [int]$HistorySize = 0,
    [string]$LogFile = "ping-log.csv",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptPath = Join-Path $PSScriptRoot "ping_dashboard.py"

if (Get-Command py -ErrorAction SilentlyContinue) {
    $pythonExe = "py"
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $pythonExe = "python"
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    $pythonExe = "python3"
} else {
    Write-Error "Python was not found on PATH. Install Python 3 or add it to PATH."
}

$pythonArgs = @(
    $scriptPath,
    $InternetTarget,
    "--interval", $Interval.ToString([System.Globalization.CultureInfo]::InvariantCulture),
    "--timeout-ms", $TimeoutMs,
    "--high-ping-threshold-ms", $HighPingThresholdMs,
    "--port", $Port,
    "--history-size", $HistorySize,
    "--log-file", $LogFile
) + $ExtraArgs

Write-Host "Starting ping dashboard..."
Write-Host "Gateway target: 10.0.0.1"
Write-Host "Internet target: $InternetTarget"
Write-Host "Dashboard: http://127.0.0.1:$Port"

& $pythonExe @pythonArgs
