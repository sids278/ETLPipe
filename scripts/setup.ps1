# One-time local setup for ch-sync (Windows PowerShell).
#   powershell -ExecutionPolicy Bypass -File scripts\setup.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
Write-Host "==> ch-sync setup in $(Get-Location)"

# 1. Python >= 3.10
$py = if ($env:PYTHON) { $env:PYTHON } elseif (Get-Command py -ErrorAction SilentlyContinue) { "py" } else { "python" }
& $py -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)"
if ($LASTEXITCODE -ne 0) { throw "Python 3.10+ is required (https://www.python.org/downloads/)" }

# 2. Virtual environment + dependencies
if (-not (Test-Path .venv)) {
    Write-Host "==> creating .venv"
    & $py -m venv .venv
}
$venvPy = ".\.venv\Scripts\python.exe"
Write-Host "==> installing dependencies"
& $venvPy -m pip install --quiet --upgrade pip
& $venvPy -m pip install --quiet -e ".[dev]"

# 3. Config + secrets (never overwritten)
if (-not (Test-Path config.yaml)) {
    Copy-Item config.example.yaml config.yaml
    Write-Host "==> created config.yaml (edit server names and tables)"
}
if (-not (Test-Path .env)) {
    @"
# Secrets for ch-sync (loaded automatically; also used by docker compose). Do not commit.
MSSQL_PASSWORD=change-me
CH_PASSWORD=change-me
"@ | Set-Content -Encoding UTF8 .env
    Write-Host "==> created .env (put real passwords here)"
}

# 4. Microsoft ODBC Driver 18
if (Get-OdbcDriver -Name "ODBC Driver 18 for SQL Server" -ErrorAction SilentlyContinue) {
    Write-Host "==> ODBC Driver 18 for SQL Server: found"
} else {
    Write-Warning "'ODBC Driver 18 for SQL Server' not found. Install: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server"
}

# 5. Unit tests as a sanity check
Write-Host "==> running unit tests"
& $venvPy -m pytest -q tests\test_units.py

Write-Host @"

Setup complete. Next steps:
  1. Edit .env        -> real passwords
  2. Edit config.yaml -> SQL Server / ClickHouse hosts and your tables
  3. Run sql\01_enable_change_tracking.sql on SQL Server (once)
  4. .\.venv\Scripts\Activate.ps1
     ch-sync check --ddl     # validate connections + preview ClickHouse tables
     ch-sync run             # start syncing
     ch-sync status          # in another terminal
"@
