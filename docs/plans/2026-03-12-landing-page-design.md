# Landing Page Design
_2026-03-12_

## Problem

The app is built for users who already know what question to ask. Coaches and GMs land on Tab 1 with no guidance — too many filters, too many columns, no clear entry point.

## Goal

A "start here" experience that routes any user to the right tab with smart defaults, without requiring them to understand the app's structure first.

## Design

### New "Home" tab

First tab in the navbar. Makes zero DB calls — it only sets filter state and navigates.

### Team selector (optional)

A single dropdown at the top of the landing page. Pre-populated with teams for the current season. If set, all card navigations carry that team filter into the target tab. Skippable — if left blank, tabs open with their existing defaults.

### 4 question cards (2×2 grid)

| Card | Question | Target | Pre-applied defaults |
|------|----------|--------|----------------------|
| 1 | Who is helping my team? | Tab 1: On/Off | auto min-poss on, team filter if set |
| 2 | Which lineups are working? | Tab 2: Lineups | group size 5, auto min-poss on, team filter if set |
| 3 | How is my team performing? | Tab 3: Team Ratings | summary view, team filter if set |
| 4 | What happened in last night's game? | Tab 4: Game Logs | most recent game pre-selected, team filter if set |

Each card has: a short question as title, one line of explanation, a "→ Go" button.

### Navigation mechanism

Card click → `updateTabsetPanel()` to switch to the target tab + `updateSelectizeInput()` / reactive value updates to apply the pre-set filters.

## Out of scope

- No new DB queries on the landing page
- No personalization or login
- React frontend (Shiny only for now)
- Tab 5 (Player Stats) — excluded from cards for now, can be added later
