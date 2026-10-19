# Local Development Setup

## Prerequisites

- **R 4.4.2+** — installed at `C:\Program Files\R\R-4.4.2\`
- **Node.js 18+** — with npm
- **Git Bash** (or similar shell)
- **PostgreSQL credentials** — `app/.Renviron` file (gitignored) with readonly DB access

### R Packages (install once)

```r
install.packages(c("plumber", "pool", "RPostgres", "DBI", "jsonlite", "dplyr", "tidyr"))
```

### Node Dependencies (install once)

```bash
cd frontend-v2
npm install
```

---

## Quick Start (2 terminals)

### Terminal 1 — Plumber API (port 3002)

```bash
cd frontend-v2/server
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" run.R
```

You should see:
```
Running plumber API at http://127.0.0.1:3002
Running swagger Docs at http://127.0.0.1:3002/__docs__/
```

### Terminal 2 — Vite Dev Server (port 5173)

```bash
cd frontend-v2
npm run dev
```

You should see:
```
VITE v7.x ready
➜ Local: http://localhost:5173/
```

### Open the App

Navigate to **http://localhost:5173/** in your browser.

Vite proxies all `/api/*` requests to the Plumber server on port 3002 automatically.

---

## Architecture

```
Browser (localhost:5173)
   │
   ├── Static assets ──→ Vite dev server (port 5173)
   │
   └── /api/* requests ──→ Proxy ──→ Plumber/R API (port 3002)
                                         │
                                         └──→ PostgreSQL (Supabase, port 6543)
```

---

## Environment Variables

### Required: `app/.Renviron`

The Plumber server reads DB credentials from `app/.Renviron` (same file the Shiny app uses). This file is gitignored. Required keys:

```
PG_HOST=<supabase-pooler-host>
PG_PORT=6543
PG_DB=postgres
PG_USER=<readonly-user>
PG_PASS=<password>
PG_SSLMODE=require
POOL_MAX=3
```

### Optional: Plumber Config

Set these as environment variables before starting Plumber:

| Variable | Default | Description |
|----------|---------|-------------|
| `PLUMBER_HOST` | `127.0.0.1` | Bind address |
| `PLUMBER_PORT` | `3002` | API port |
| `FRONTEND_ALLOWED_ORIGINS` | `http://localhost:5173,...` | CORS allowlist |
| `FRONTEND_API_KEY` | *(empty = disabled)* | Optional API key |
| `FRONTEND_PROFILE_TIMING` | `0` | Set `1` to log endpoint timing |

---

## Troubleshooting

### "address already in use" when starting Plumber

Another process is on port 3002. Find and kill it:

```bash
netstat -ano | findstr ":3002"
# Note the PID (last column)
taskkill //PID <pid> //F
```

### Vite starts on wrong port (5174, 5175, etc.)

Other Vite processes are occupying 5173. Kill them all:

```bash
netstat -ano | findstr ":517"
# Kill each PID
taskkill //PID <pid> //F
```

Then restart `npm run dev`.

### API returns 404 for new endpoints

The Plumber server is running an old version of `plumber.R`. Kill and restart it (Terminal 1).

### "Could not find app/.Renviron" error

Run Plumber from within the repo directory tree. The server walks up from the working directory to find `app/.Renviron`.

---

## Running the Shiny App (separate from React)

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app')"
```

Opens on http://127.0.0.1:3838 by default.

---

## API Documentation

With Plumber running, visit **http://localhost:3002/__docs__/** for the Swagger UI with all available endpoints.
