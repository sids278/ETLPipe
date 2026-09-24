#!/usr/bin/env bash
# One-time local setup for ch-sync (Linux / macOS).
#   ./scripts/setup.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> ch-sync setup in $(pwd)"

# 1. Python >= 3.10
PY="${PYTHON:-python3}"
command -v "$PY" >/dev/null || { echo "ERROR: $PY not found. Install Python 3.10+."; exit 1; }
"$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' \
  || { echo "ERROR: Python 3.10+ required, found $("$PY" -V)"; exit 1; }

# 2. Virtual environment + dependencies
if [ ! -d .venv ]; then
  echo "==> creating .venv"
  "$PY" -m venv .venv
fi
. .venv/bin/activate
echo "==> installing dependencies"
pip install --quiet --upgrade pip
pip install --quiet -e ".[dev]"

# 3. Config + secrets (never overwritten)
if [ ! -f config.yaml ]; then
  cp config.example.yaml config.yaml
  echo "==> created config.yaml (edit server names and tables)"
fi
if [ ! -f .env ]; then
  cat > .env <<'ENV'
# Secrets for ch-sync (loaded automatically; also used by docker compose). Do not commit.
MSSQL_PASSWORD=change-me
CH_PASSWORD=change-me
ENV
  chmod 600 .env
  echo "==> created .env (put real passwords here)"
fi

# 4. Microsoft ODBC Driver 18 (needed to talk to SQL Server)
if command -v odbcinst >/dev/null && odbcinst -q -d 2>/dev/null | grep -q "ODBC Driver 18 for SQL Server"; then
  echo "==> ODBC Driver 18 for SQL Server: found"
else
  echo "WARNING: 'ODBC Driver 18 for SQL Server' not found."
  echo "         Install: https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server"
  echo "         (or skip this and use Docker: docker compose up -d --build)"
fi

# 5. Unit tests as a sanity check
echo "==> running unit tests"
pytest -q tests/test_units.py

cat <<'NEXT'

Setup complete. Next steps:
  1. Edit .env        -> real passwords
  2. Edit config.yaml -> SQL Server / ClickHouse hosts and your tables
  3. Run sql/01_enable_change_tracking.sql on SQL Server (once)
  4. source .venv/bin/activate
     ch-sync check --ddl     # validate connections + preview ClickHouse tables
     ch-sync run             # start syncing
     ch-sync status          # in another terminal
NEXT
