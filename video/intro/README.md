# IBPL intro video

Everything the film says and does is in `script.json`. Edit that, then re-run.

1. Start the app locally (project skill `run-shiny-local`, `IBPL_CACHE_UI=false`, port 3838).
2. `node record.mjs --check` — every step must print `ok`. Fix selectors with `node probe.mjs <chapter> "<selector>"`.
3. `node record.mjs` (or `--chapter <id>` to redo one chapter)
4. `node overlays.mjs`
5. `node review.mjs` — `out/captions_review.md`, both languages side by side
6. `node compose.mjs` (or `--chapter <id>` + `node verify.mjs --still <step>` for a quick look)
7. `node verify.mjs` — durations, plus a frame per step in `out/verify/` to inspect

Outputs: `out/intro_{en,he}.mp4`, `out/short_{en,he}.mp4`, `out/chapters_{en,he}.txt` (paste into the YouTube description).
Numbers in captions are read from the page at record time, so re-recording after an ETL run updates them.

Notes:
- View modes (Summary / Four Factors / ...) are switched from each navbar tab's hover menu (`.thm-item`), not radio buttons.
- App tables use `scrollX`; the director looks cells up through `$.fn.dataTable.tables()`.
- A chapter with `"global_setup": false` skips the top-level `setup` (the phone chapter cannot reach the navbar season picker).

Tests: `node --test --test-concurrency=1 "test/*.test.mjs"` (needs network for CDN fonts/DataTables in fixtures, and ffmpeg on PATH).
