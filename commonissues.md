# Common issues

Problems hit while setting up the load-test environment and API, with the exact error, cause and fix.
Search this file for the error text you see.

---

## Docker and SQL Server

### 1. SQL Server container exits immediately on an Apple Silicon Mac

**Symptom** (`docker logs <mssql container>`):
```
<jemalloc>: (This is the expected behaviour if you are running under QEMU)
/opt/mssql/bin/sqlservr: Invalid mapping of address 0x4004353000 in reserved address space below 0x400000000000.
```
Container state: `Exited (1)`.

**Cause:** Microsoft publishes SQL Server images only for amd64 (Intel). On an arm64 Mac, Docker Desktop emulates amd64 with QEMU by default, and SQL Server crashes under it.

**Fix (pick one):**
- Docker Desktop → Settings → General → turn on **Use Virtualization framework** and **Use Rosetta for x86_64/amd64 emulation**. On old versions (e.g. 4.20) it's under **Features in development**; better to update Docker Desktop first.
- Use OrbStack instead of Docker Desktop (it uses Rosetta by default).
- Run the databases in a **GitHub Codespace** (Intel machine, no emulation). This is what the project uses now.

SQL Server has no native macOS version, and Azure SQL Edge (arm64) was retired in September 2025.

### 2. Not enough memory for Docker

**Symptom:** `docker info` shows about 3.8 GB; containers get slow or are killed under load.

**Cause:** SQL Server needs at least 2 GB, and ClickHouse needs about the same plus room for merges.

**Fix:** Docker Desktop → Settings → Resources → Memory **8 GB or more**, and 60 GB+ disk for millions of rows.

### 3. SQL Server container won't start: weak password

**Symptom:** `/health` says `Failed to connect to localhost:1433 - Could not connect (sequence)` even after waiting; `docker compose ps -a mssql` shows `Exited`.

**Cause:** `MSSQL_PASSWORD` in the repo-root `.env` didn't meet SQL Server's password policy. `scripts/setup.sh` creates `.env` with the placeholder `change-me`, and `docker compose` reads that file automatically.

**Fix:** `MSSQL_PASSWORD` needs 8+ characters using at least 3 of: uppercase, lowercase, digits, symbols. Then recreate the container (see issue 5):
```bash
printf 'MSSQL_PASSWORD=Str0ng_Passw0rd\nCH_USER=default\nCH_PASSWORD=ch_pass\n' > .env
```
```bash
docker compose up -d --force-recreate mssql clickhouse
```

### 4. ClickHouse: authentication failed

**Symptom:**
```
default: Authentication failed: password is incorrect, or there is no user with such name
```

**Cause:** The container and the client used different passwords. The container took `CH_PASSWORD=change-me` from the root `.env` (issue 3), while the API sent `ch_pass` (or an empty password when `api/.env` was missing).

**Fix:** Keep passwords in **one place**, the repo-root `.env`. `docker compose`, ch-sync and the API (`api/env.js`) all read it. Then recreate the containers (issue 5).

### 5. Changed a password in `.env`, but nothing changed

**Cause:** A container reads its password only when it is **created**. `docker compose up -d` leaves an existing container running with its old settings.

**Fix:**
```bash
docker compose up -d --force-recreate mssql clickhouse
```
There is no volume, so recreating deletes the data in the containers.

### 6. `/health` fails right after starting the containers

**Symptom:** `Connection lost - socket hang up` for SQL Server; `docker compose ps` shows `health: starting`.

**Cause:** The databases were still booting. SQL Server takes about 30–60 seconds, ClickHouse about 5–10.

**Fix:** Wait for `healthy`, then retry:
```bash
watch -n 2 docker compose ps
```

### 7. ClickHouse is a different version than the repo expects

**Symptom:** `SELECT version()` returns `26.8.x`, while `docker-compose.yml` says `clickhouse/clickhouse-server:24.8`.

**Cause:** `docker-compose.override.yml` switches the image to `clickhouse:latest`, and compose loads the override automatically.

**Fix:** Nothing, if that's intended. The repo's tests were written against 24.8; remove the override to go back to it.

---

## Azure SQL Database (the route we abandoned)

### 8. "Location" keeps loading on the Create SQL Database Server form

**Symptom:** The Location dropdown spins forever; OK stays greyed out.

**Cause:** The Azure account had **no subscription**. The Subscriptions page showed `Subscriptions : Filtered (0 of 0)`. Signing in creates a directory (e.g. `…onmicrosoft.com`), but resources need a subscription, and regions are loaded from it.

**Fix:** Finish the free-account sign-up (phone and card verification) at https://azure.microsoft.com/free/, sign out and back in, then check Subscriptions shows one as **Active**. If it's active but still stuck, register the `Microsoft.Sql` resource provider (Subscription → Resource providers).

### 9. "Validation failed. Required information is missing or not valid."

**Cause:** Same as issue 8: the server was never created, so the database form had no server. The tab with the real problem has a red dot.

Also: the server form reset **Authentication method** to *Microsoft Entra-only*. ch-sync needs **Use SQL authentication** (username and password).

---

## Tooling

### 10. `odbcinst: command not found`

**Cause:** Only the `curl … packages-microsoft-prod.deb` step ran. It downloads the repository config but installs nothing.

**Fix:** Run the remaining steps:
```bash
sudo dpkg -i packages-microsoft-prod.deb && rm packages-microsoft-prod.deb
```
```bash
sudo apt-get update && sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
```
```bash
odbcinst -q -d
```

### 11. `git push` fails, or a Codespace asks to fork the repo

**Cause:** The GitHub CLI on the Mac is signed in as a different account (`anusha202024`) with READ-only access to `sids278/ETLPipe`.

**Fix:** `gh auth login` as `sids278`, and create Codespaces while signed in to github.com as `sids278`.

---

## Node API

### 12. `npm error Missing script: "dev"`

**Cause:** `--watch` was added to the `start` script instead of adding a new `dev` script.

**Fix:** `api/package.json`:
```json
"scripts": {
  "start": "node server.js",
  "dev": "node --watch server.js"
}
```

### 13. `Error [ERR_MODULE_NOT_FOUND]: Cannot find package 'dotenv'`

**Cause:** Packages weren't installed (no `node_modules`). This happens on every fresh clone and in a new Codespace, because `node_modules` isn't committed.

**Fix:** In `api/`:
```bash
npm install
```

### 14. `does not provide an export named 'default'`

**Cause:** `server.js` does `import app from './app.js'`, but `app.js` was empty or lacked `export default app`. It also happens in a Codespace that hasn't pulled the latest code.

**Fix:** End `app.js` with `export default app;`, and `git pull` in the Codespace.

### 15. Passwords were `undefined` even though `.env` had them

**Cause:** In ES modules, all `import` statements run **before** the rest of the file. Calling `dotenv.config()` in the body of `server.js` would run after `app.js` and `db/*.js` had already read `process.env`.

**Fix:** Load env in its own module and import it first: `server.js` starts with `import './env.js';`. `api/env.js` loads `api/.env`, then the root `.env`. The first file wins, and variables already set in the shell win over both.

### 16. `body-parser` works but isn't in `package.json`

**Cause:** It's only there because Express installs it for itself, so it can disappear on an Express upgrade.

**Fix:** Use Express's built-in parser: `app.use(express.json())`.

### 17. `/health` returns 503 with both databases down, on the Mac

**Cause:** The API ran on the Mac, but the databases run in the Codespace. Nothing on the Mac listened on 1433 or 8123.

**Fix:** Run the API in the Codespace. Or, for development only, forward the ports to the Mac: `gh codespace ports forward 1433:1433 8123:8123` (as `sids278`). Don't use timings measured over the tunnel.

### 18. ClickHouse error message is empty (`"error": ""`)

**Cause:** Refused connections arrive as an `AggregateError`, whose `message` is empty.

**Fix:** Already handled: `describeError()` in `services/healthService.js` falls back to the error code, e.g. `ECONNREFUSED: connect ECONNREFUSED 127.0.0.1:8123`.

---

## Shell and git

### 19. `curl -s localhost:3000/health | jq` prints nothing

**Cause:** The API wasn't running. `-s` hides curl's own errors, including "connection refused".

**Fix:** Use `-sS` to keep errors visible, and make sure `npm run dev` is running in another terminal:
```bash
curl -sS localhost:3000/health | jq
```

### 20. `printf '…' > .env` "gave nothing"

**Not an error:** `>` sends the output to the file instead of the screen. Check with `cat .env`.

### 21. Git shows 9,000+ changed files

**Cause:** `api/node_modules/` wasn't in `.gitignore`, which only had Python entries.

**Fix:** `node_modules/` was added to `.gitignore`. Commit `package.json` and `package-lock.json` instead; `npm install` rebuilds `node_modules`.

---

## Data generation

### 22. Mockaroo can't generate millions of rows

**Cause:** The free tier allows about 1,000 rows per request and a limited number of requests per day.

**Fix:** Use Mockaroo only for designing schemas or small samples. Generate volume locally (Faker) or on the server (set-based T-SQL, like `sql/02_demo_seed.sql`), which is fastest.
