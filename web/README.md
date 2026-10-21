# Game Logs web slice

Next.js screen for Israeli League, EuroLeague and EuroCup. Reads the existing
Shiny database views with the existing `app_readonly` login. No database migrations
or role changes are needed. Gameflow remains a later migration phase.

## Local development (PowerShell)

Use Node.js 22 or newer:

```powershell
cd web
npm.cmd ci
npm.cmd run dev
```

The development launcher reads the existing `../app/.Renviron` without copying it.
Only server code uses `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASS` and
`PG_SSLMODE`; never prefix these with `NEXT_PUBLIC_`. Hosted deployments receive
them through Vercel environment variables. SSL follows Shiny's `require` setting;
`PG_SSLMODE=verify-full` and `PG_SSLROOTCERT` support a supplied project CA file.

Open `/il/game-logs?season=2026`, `/euroleague/game-logs?season=2025`, or
`/eurocup/game-logs?season=2025` for the seasons used in validation.

## Checks

Generate the ignored, offline golden JSON once from the repository root:

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/export_goldens_json.R
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/export_web_catalogues.R
```

From `web/`:

```powershell
npm.cmd test
npm.cmd run typecheck
npm.cmd run lint
npm.cmd run build
npx.cmd playwright install chromium
npm.cmd run e2e -- game-logs --workers=2
```

The browser golden tests intercept the data API and compare stored Shiny cells
at 390px and 1440px widths. Explicitly enable read-only live checks with:

```powershell
$env:LIVE_DB_SMOKE='1'
npm.cmd run e2e -- live-smoke --workers=1
Remove-Item Env:LIVE_DB_SMOKE
```

## On/Off parity checks

On/Off uses the same tab kit and TanStack Table at phone and desktop widths.
Stored R fixtures live in `app/tests/goldens/{fixtures,out}/onoff`; recording
them is an explicit live, read-only operation (do not run another app query as
`app_readonly` while recording):

```powershell
# Repository root: only when intentionally refreshing the Shiny oracle
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/capture_goldens.R --tab onoff --record
# Offline export of those stored expected frames
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/export_onoff_goldens_json.R
```

With the shared development server running on port 3100, from `web/`:

```powershell
# Opt-in live reads of the route answers, saved separately from expected frames
node scripts/capture-onoff-answers.mjs
npm.cmd test
node node_modules/@playwright/test/cli.js test onoff --workers=2
```

Both JSON exports and captured route answers are ignored local test artifacts.
The numeric tests compare every stored numeric field (including percentile ranks
and luck/sample fields), keyed by team and player name. Browser tests compare
visible values, default ordering, row counts and interactions at 390 and 1440 px.
Shiny testServer does not apply auto-slider updates, so parity presets explicitly
set 100 / 300; browser tests restore those bars after the initial auto adjustment.
The opt-in live smoke suite also covers On/Off server rendering in EuroCup.

## Preview deployment

The linked project is `ibpl-game-logs`. Use the Vercel CLI from `web/` and deploy
with `vercel.cmd deploy --environment preview`. The credentials are configured
only for Preview. `.vercelignore` excludes local environment files and generated
golden data. Check the resulting deployment's environment: Vercel classified the
new project's first deployment as production despite the explicit preview flag.

Only computed table rows reach the browser. A bounded server cache and the Vercel
CDN each use a 60-second TTL. See [execution status](../docs/phase0_execution_status.md)
for verified results, current preview URL, remaining migration scope and setup notes.
