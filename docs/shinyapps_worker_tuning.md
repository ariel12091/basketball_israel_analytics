# shinyapps.io Worker Tuning

Goal: reduce the impact of Shiny's single-threaded R process. One user's slow
filtered-path query (e.g. `onoff_compute` with clutch/opponent-rank filters)
blocks every other session sharing the same worker process. Spreading sessions
across more workers earlier means fewer users are frozen by any one query.

## Where

shinyapps.io dashboard → **ibpl-stats / onoff-shiny** → **Settings**.

## Recommended settings

| Setting (Settings → Advanced) | Default | Recommended | Why |
|---|---|---|---|
| Max Worker Processes | 3 | 3 (keep) | Upper bound of R processes per instance. |
| Max Connections (per worker) | 50 | **10** | Fewer sessions share one R process, so a slow query blocks at most ~9 other users instead of ~49. |
| Worker Load Factor | 0.5 | **0.3** | Spawns the next worker earlier (at 30% of max connections) instead of packing sessions onto one process. |
| Instance Idle Timeout (General) | 5 min | 5 min (keep) | Keep startup latency low without burning hours. |

Instance size (Settings → General): with the shared season-MV cache
(`cached_season_df` in `app/R/global.R`), memory no longer grows one MV copy
per session, so the current size should have comfortable headroom. Only bump
it if the dashboard's memory metrics show pressure.

## Database connection math

Each worker process creates its own pool (`POOL_MAX=3`, `minSize=0`,
15s idle timeout in `global.R`). 3 workers × 3 = **9 max connections** through
the Supabase pooler (port 6543) — well within limits. If Max Worker Processes
is ever raised, re-check this product.

## Related app-side levers (already in the repo)

- `APP_IDLE_CLOSE_SESSION` (defaults to `true` in `app/R/global.R`): closes
  sessions idle past `APP_IDLE_TIMEOUT_SEC` (default **600s**) so idle tabs free
  their worker connection slot and stop consuming active hours. Returning opens
  the stored bookmark, so filters come back. These live in code, not
  `.Renviron` — that file is gitignored *and* deployed, so tuning values there
  override the committed default invisibly. `app.R` logs the resolved config at
  startup; check that line before concluding a timeout is misbehaving.

  shinyapps.io also applies its own idle disconnect from the dashboard, and
  that is fine — Shiny's native disconnect UI is suppressed unconditionally in
  `app.css`, so whichever side drops the connection the user sees the app's own
  paused pill, never the default overlay. The only thing the ordering changes is
  cleanliness: when our timer fires first the session is closed deliberately and
  the bookmark is already stored, so returning restores filters.
- `GL_DATA_CACHE_MAX_MB` / `GL_DATA_CACHE_MAX_AGE_SEC`: size/age of the
  process-wide data cache used by the shared season-MV pulls (Tabs 1, 4, 5).
- Escalation path if concurrency still hurts after tuning: move the slowest
  filtered-path reactives to `ExtendedTask` + `promises`/`mirai` so long
  queries stop blocking the process event loop.

Note: worker/connection settings are per-instance and applied from the
dashboard immediately (no redeploy needed). Free/Starter plans are limited to
1 instance; the settings above still apply within that instance.
