# Frontend Scaffold (P0 Start)

This directory contains the initial React web scaffold for parity migration from Shiny.

## Included now

1. Desktop-first app shell with route tabs:
   - `/onoff`
   - `/lineups`
   - `/teams`
   - `/gamelogs`
2. Responsive filter panel (desktop pinned, mobile toggle).
3. Typed shared filter model.
4. GN min/max vs Last N mutual-exclusion utility.
5. Per-tab reset-to-default behavior.

## Run locally

```bash
cd frontend
npm install
npm run dev
```

## Next P0 steps

1. Add API client and endpoint contracts.
2. Build Tab 1 Summary/Four-Factors parity table.
3. Port conditional formatting rules.
4. Add fixture-based parity checks against Shiny outputs.
