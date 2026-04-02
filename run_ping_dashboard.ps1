param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PingArgs
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

& $pythonExe $scriptPath @PingArgs
