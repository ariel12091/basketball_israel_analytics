# Intro Video — Design

Date: 2026-10-06. Status: approved in conversation, awaiting spec review.

## Goal

An introduction movie for IBPL Analytics that teaches someone who has never seen
the site, especially a non-technical fan, how to get answers out of it without
effort. It must cover every user-facing feature at a level a first-time visitor
can act on.

## Deliverables

| File | Content |
|---|---|
| `intro_en.mp4`, `intro_he.mp4` | Full tutorial, ~4.5 min, 1920x1080, 30 fps |
| `short_en.mp4`, `short_he.mp4` | Highlight cut, <= 60 s, 1080x1080 |
| `chapters_en.txt`, `chapters_he.txt` | YouTube chapter markers for the tutorial |

Captions only: no voiceover. A quiet instrumental bed is optional and only if
the user supplies a royalty-free track; otherwise silent.

## Decisions (made with the user)

- **Approach A:** scripted Playwright recording of the real app + ffmpeg
  overlay compositing. Footage recorded once; both languages overlaid on it.
- **Record against local `runApp()`**, which reads the production DB, so numbers
  match the live site. Avoids Connect Cloud cold starts and the 5 s idle kill.
- **Story thread:** Maccabi Tel Aviv, season 2025-26 (`game_year = 2026`).
  2026-27 is too young to have full tables. Measured 2026-10-06: 35-4, net
  rating +20.9 (league #1), offense #1. On/off lesson: Jimmy Clark III +17.0
  Net RTG Diff (mostly defense, -8.9) while the heaviest-minute players sit near
  zero (Sorkin -3.1, Hoard -1.0) -- on a +21 team even bench units win, so
  minutes != impact.
- **Languages:** English and Hebrew. Hebrew captions are RTL; numbers and player
  names stay Latin inside them. The user reviews the Hebrew.

## Chapter outline (tutorial)

Chapters are framed by the Home cards' questions, not by tab names.

| # | Chapter | Teaches | Length |
|---|---|---|---|
| 0 | Cold open | "What really happens when your best player sits?" + title | 10 s |
| 1 | Finding your way | League chooser (Israeli/EuroLeague/EuroCup), season selector, Home team pick + "Set as default", question cards, glossary button | 25 s |
| 2 | Who is helping my team? (On/Off) | Read Net RTG Diff + colours; "3PT luck"/"small sample" tags + tooltip; Summary vs Four Factors + "est. ±X pts"; min possessions; dates / last N / opponent filters | 50 s |
| 3 | Which lineups are working? | Group size 2-5; player chips On / Any of / Off and "with at least k of"; clutch filter; click a lineup -> game-by-game modal | 50 s |
| 4 | How is my team performing? (Team Ratings) | Off/Def/Net vs league, rank arrows; Summary / Four Factors / Shot Profile / Traditional | 35 s |
| 5 | What happened last night? (Game Logs) | Pick a game; score, lineups, stats; game-flow view | 35 s |
| 6 | How are individual players doing? (Player Stats) | Totals / Per Game / Per 60 Poss / Per 30 Min and why per-possession is fairer; clutch | 30 s |
| 7 | Starters vs bench (Compare) | A vs B; Teams / Lineups / Players modes; presets | 35 s |
| 8 | EuroLeague & EuroCup | Same questions, same tabs; leagues never ranked against each other | 20 s |
| 9 | Pro tips | Tap a filter chip to clear; hover headers for meaning; CSV export; "Show Filters" on mobile | 20 s |
| 10 | Outro | URL | 5 s |

**Short cut:** hook, then ~6 beats of 6-8 s (Net RTG Diff + tag, lineup chips,
team ranks, game flow, Compare starters vs bench, EuroLeague switch), then URL.

**Verify, don't assume:** that Game Logs on `main` shows the game-flow ribbon,
and that a Compare preset is literally "starters vs bench". If either is false,
cut the beat rather than invent it. Likewise demonstrate the "3PT luck" /
"small sample" tooltip only on a Maccabi (or other) row that actually carries
the tag.

## Visual style

- App's own look: dark editorial, DM Sans text, JetBrains Mono numbers, amber
  `#e8a435` accent.
- App recorded at a 1600x900 viewport, scaled to 1920x1080.
- Exactly three overlay elements:
  1. **Caption bar**, lower third: dark translucent panel, amber edge, one
     sentence of <= ~12 words. Mirrored for Hebrew (amber edge right, RTL).
  2. **Highlight ring**: amber rounded rectangle fading in around the element
     being explained, placed from that element's real bounding box captured at
     recording time.
  3. **Chapter card**, 1.5 s: the question in large type, the tab icon, "n/9".
- Camera: gentle zoom (<= 1.6x) into the explained region, driven by the same
  bounding boxes; no mid-action cuts.
- Pacing: >= 1.5 s hold after each action before the next caption; one idea per
  caption; visible, enlarged, smoothed cursor.
- Short cut uses the same elements at faster pacing, cropped square around the
  highlighted region.

## Build

Location `video/intro/` on branch `infra/intro-video`. Scripts committed;
`video/intro/out/` gitignored (videos never go into git or the Connect bundle).

| File | Job |
|---|---|
| `script.json` | Single source of truth: chapters -> steps `{action, target selector, caption_en, caption_he, hold, zoom}`. Editing the film = editing this file. |
| `record.mjs` | Playwright drives the local app (`IBPL_CACHE_UI=false`), season 2026, Maccabi Tel Aviv; one clip per chapter; writes a timeline of step timestamps + element bounding boxes. Injects a visible smoothed cursor (headless recording has none). |
| `overlays.mjs` | Renders caption bars, rings and chapter cards from one HTML template to transparent PNGs, per language. |
| `compose.mjs` | Timeline -> ffmpeg filter graph (zooms, timed overlays, concat) -> the deliverables. |

**Numbers in captions are read from the page at recording time**, never typed
in: captions are templates (e.g. `Clark III: {net} with him on the floor`)
filled from the on-screen cell. The video cannot contradict the app, and a
re-record after the next ETL updates the numbers itself.

Tooling present on this machine (checked 2026-10-06): ffmpeg 2025-01 full
build, Playwright Chromium 1223, Node via nvm4w (run npm/npx through
PowerShell, not the Bash tool).

## Verification

1. Before recording: the `run-shiny-local` health check (non-zero
   `nav-link`/`nav-item`, every local asset 200).
2. Approval checkpoint: one still per language (On/Off chapter, Clark III cell
   highlighted) signed off by the user before the full render.
3. Hebrew/English caption table reviewed by the user in one pass.
4. After rendering: extract the mid-frame of every step in both languages and
   inspect each one -- ring on the right element, no clipped caption, no loading
   skeleton or empty dropdown, Hebrew reads RTL.
5. `ffprobe` durations: tutorial ~4.5 min, short <= 60 s.

## Out of scope

- A "Watch the tour" link on Home -- a separate small change once the video is
  hosted (e.g. YouTube).
- Voiceover (the timed script makes adding one later possible without
  re-recording).
- Mobile-layout recording beyond the single "Show Filters" pro tip.
