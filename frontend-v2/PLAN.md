# Frontend v2 — Implementation Plan

## Current State: Static Prototype (Complete)

`index.html` is a single-file HTML/CSS/JS prototype with mock data for all 4 tabs.
Open in browser — no build step needed. All interactions work (tab switching, Summary/FF toggle, filter drawer, lineup modal, group size pills, clutch accordion).

### What's Built
- [x] Design system: dark editorial theme, DM Sans + JetBrains Mono, amber accent
- [x] Top bar with filter toggle + badge count
- [x] Tab navigation (4 tabs with active indicator)
- [x] Active filter chips with remove buttons
- [x] Slide-out filter drawer (Time / Game / Eligibility / Opponent Strength)
- [x] Tab 1: On/Off Impact — Summary (15 players, heat maps, shot splits) + Four Factors (5 players, range tracks)
- [x] Tab 2: Lineup Data — Summary (10 lineups, clickable names, TOTAL row) + Four Factors (7 lineups)
- [x] Tab 2: Group size pills (2-5) + Clutch time accordion
- [x] Tab 2: Lineup click → modal game log overlay (5 mock games)
- [x] Tab 3: Team Ratings — Summary (12 teams, W-L records, rank pills) + Four Factors
- [x] Tab 3: Clutch time accordion
- [x] Tab 4: Game Logs — Summary (18 games, W/L badges, score cells, shot splits) + Four Factors (9 games)
- [x] Tab 4: Team dropdown filter (All Teams + specific)
- [x] Responsive: mobile drawer fullscreen, tab overflow scroll
- [x] Shot split legend (Summary-only, per tab)
- [x] Tooltips on column headers

---

## Phase 1: Polish & Interactivity (~2-3 hours)

Enhance the static prototype to feel fully interactive with mock data.

- [x] **Column sorting** — click any header to sort ascending/descending, show arrow indicator (FIXED: proper key mapping)
- [x] **Filter drawer → table interaction** — selecting a team in drawer filters the displayed data
- [x] **Min possessions slider** — live-filter rows as slider changes (Tab 1 + Tab 2)
- [x] **Game logs team filter** — `onchange` event filters rows to selected team (Tab 4 dropdown works)
- [x] **Group size pills** — switch between different mock lineup sets (2/3/4/5-man) — Tab 2 only
- [x] **Chip sync** — adding/removing filters updates chips bar and badge count
- [x] **CSV export** — CSV button generates + downloads table data (all tabs, both modes)
- [x] **Keyboard shortcuts** — Escape closes modal/drawer
- [x] **Mobile: collapse filter sections** — sections start closed on small screens

## Phase 2: React Migration (~1-2 days)

Convert prototype into a proper React + TypeScript app with component architecture.

```
frontend-v2/
├── src/
│   ├── app/
│   │   ├── App.tsx              Routes + AppShell
│   │   └── layout/
│   │       └── AppShell.tsx     TopBar + TabNav + FilterDrawer + Content
│   ├── features/
│   │   ├── filters/
│   │   │   ├── FilterDrawer.tsx
│   │   │   ├── FilterChips.tsx
│   │   │   └── store.ts         Filter context/state
│   │   ├── tables/
│   │   │   ├── DataTable.tsx    Reusable table with sorting, sticky cols
│   │   │   ├── ShotCell.tsx
│   │   │   ├── FFCell.tsx       Range track cell
│   │   │   ├── FFValueCell.tsx  Simple colored value
│   │   │   ├── RankPill.tsx
│   │   │   ├── WLBadge.tsx
│   │   │   └── HeatCell.tsx
│   │   └── navigation/
│   │       └── MainTabs.tsx
│   ├── pages/
│   │   ├── OnOffPage.tsx
│   │   ├── LineupsPage.tsx
│   │   ├── TeamsPage.tsx
│   │   └── GameLogsPage.tsx
│   ├── hooks/
│   │   ├── useApi.ts           Fetch wrapper
│   │   └── useSorting.ts       Column sort state
│   ├── styles/
│   │   ├── tokens.css          CSS custom properties (from prototype)
│   │   ├── table.css
│   │   └── layout.css
│   └── types/
│       └── index.ts            Player, Lineup, Team, GameLog types
├── index.html
├── package.json
├── tsconfig.json
└── vite.config.ts
```

Key decisions:
- **No heavy table library** — custom `<DataTable>` component using `<table>` (keeps the visual fidelity of the prototype)
- **react-select** for multi-select dropdowns (team/opponent/player filters)
- **CSS Modules or vanilla CSS** — no Tailwind (design is too custom)
- **No state library** — React context + `useReducer` for filter state

## Phase 3: API Integration (~1 day)

Connect React frontend to the PostgreSQL backend.

### Option A: Direct Supabase Client (Simplest)
```ts
import { createClient } from '@supabase/supabase-js'
const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY)
// Call PostgreSQL functions directly
const { data } = await supabase.rpc('onoff_compute', { p_game_year: 2026, ... })
```
- Uses existing PL/pgSQL functions unchanged
- Row-level security via anon key (readonly)
- No backend needed

### Option B: Thin R/Plumber API
```r
# plumber.R
#* @get /api/onoff
function(game_year, ...) {
  dbGetQuery(pool, "SELECT * FROM onoff_compute($1, ...)", params)
}
```
- Reuses existing R pool + query logic
- Can run alongside Shiny or replace it
- Adds a server dependency

### Option C: Edge Functions (Supabase)
- TypeScript functions on Supabase Edge
- Cold start latency (~200ms)
- Most "modern" but requires rewriting SQL call layer

**Recommendation:** Option A (Supabase client) for fastest path. The SQL functions already do all the heavy lifting — the frontend just needs to call them with the right parameters.

## Phase 4: Deployment (~half day)

- [ ] Vite build → static files
- [ ] Deploy to Vercel / Netlify / Supabase hosting (free tier)
- [ ] Environment variables for Supabase URL + anon key
- [ ] Custom domain (optional: `stats.ibpl.co.il` or similar)
- [ ] Set up CI: build on push, deploy on main merge

## Design Tokens Reference

```css
--bg-root: #080c14       --text-primary: #e8ecf4
--bg-surface: #0e1420    --text-secondary: #8a96ab
--bg-card: #141c2b       --text-muted: #5a6578
--bg-elevated: #1a2436   --accent: #e8a435
--bg-hover: #1e2a3e      --positive: #34d399
--bg-active: #243350     --negative: #f87171
--fg2: #5b8abd           --fg3: #d4843e
```

Fonts: `DM Sans` (body, 300-700), `JetBrains Mono` (data/code, 400-700)
