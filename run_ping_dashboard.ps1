param(
    [string]$InternetTarget = "8.8.8.8",
    [double]$Interval = 1.0,
    [int]$TimeoutMs = 1000,
    [int]$HighPingThresholdMs = 150,
    [int]$Port = 8765,
    [string]$DatabaseFile = "ping-monitor.db",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptPath = Join-Path $PSScriptRoot "ping_dashboard.py"

if (Get-Command py -ErrorAction SilentlyContinue) {
    $pythonCommand = @("py", "-3.12")
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $pythonCommand = @("python")
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    $pythonCommand = @("python3")
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
    "--database-file", $DatabaseFile
) + $ExtraArgs

Write-Host "Starting ping dashboard..."
Write-Host "Gateway target: 10.0.0.1"
Write-Host "Internet target: $InternetTarget"
Write-Host "Database file: $DatabaseFile"
Write-Host "Dashboard: http://127.0.0.1:$Port"

$pythonCommandArgs = if ($pythonCommand.Length -gt 1) { $pythonCommand[1..($pythonCommand.Length - 1)] } else { @() }
& $pythonCommand[0] $pythonCommandArgs @pythonArgs
