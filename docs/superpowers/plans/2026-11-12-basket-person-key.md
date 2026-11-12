# Basket person key Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tie every basket.co.il registration to one person across seasons, using the past-seasons links on each player's basket.co.il profile page. Store the linked ids as an array on the registration row, derive a `basket_person_id`, and let the identity matcher use a person's English name from another registration when the current row has none.

**Architecture:**
- **What the page gives:** each profile page (`player.asp?PlayerId=N`) carries a past-seasons stats table, whose rows link a season label (`2020-21`) to that season's PlayerId. Most pages also carry a "player in the previous season" link (`id="player-prev-year"`).
- **Where it is stored:** the PlayerIds a page links to, other than its own, go into one new column on the registration the page belongs to: `basket_player_registration.linked_basket_player_ids integer[]`. There is no new table. The season and team of each linked id are not kept: nothing uses them, and for 2022+ the season can be recovered from the id (each season's ids are a separate ascending block).
- **The person key:** a pure R function takes the connected components of those links and writes `basket_person_id` = the smallest PlayerId in the component. That id is the earliest registration basket.co.il knows, so the key stays put as later seasons are added.
- **Matcher use:** a registration with no English name borrows one from another registration of the same person. The name is only a candidate: the existing match rules decide.

**Tech Stack:** R 4.4.2, DBI/RPostgres, PostgreSQL (port 5432 for writes), testthat, GitHub Actions.

**Spec:** this conversation, 2026-11-12. The user pointed at basket.co.il's past-seasons section on `player.asp?PlayerId=21797`, said "Plan", and then chose the array column over a links table.

### Facts measured 2026-11-12 (from the cache, no requests)

- **Coverage of the cache:** 1,340 cached profiles (2022 to 2027) all carry the past-seasons table. The previous-season link appears on 758 of them.
- **Links:** they give 3,779 distinct id-to-id links, reaching back to 2000-01.
- **Accuracy:** 460 of those links join two consecutive seasons of our table. On all 460, Hebrew name and date of birth are equal, so there are 0 false links.
- **Gaps:**
  - Name plus date of birth finds 477 consecutive-season pairs. The 17 Basket does not link are all 2027 team-page-only players, whose profiles were never fetched.
  - In all, 48 roster-only registrations have no cached profile.
- **A second id within one season:** Roy Paretsky has two 2027 ids (26116 and 28237), and Yiftach Ziv's 28233 links to his other 2027 id, 26020. Two registrations of one person in one season is real data.
- **The unmatched players:** 4 registrations are unmatched today (28237-28240), all with a NULL English name. Two of them, Paretsky and Nicholls, have a 2026 registration with an English name.
- **Markup to parse:**
  - history rows: `<a href="player.asp?PlayerId=12490">2020-21</a></td> <td ...><a href="team.asp?TeamId=1018">`;
  - previous-season link: `<li id="player-prev-year" ...><a href="player.asp?PlayerId=19720">`.
- **No Basket ids are stored today** other than each registration's own `basket_player_id` (checked in `information_schema`).

## Global Constraints

- Build on `basket_player_registration`, created by `docs/superpowers/plans/2026-11-12-basket-player-registration-split.md` (PR #31). Branch `etl/basket-person` off `main` after PR #31 merges, or off `etl/basket-rosters` if it hasn't merged yet.
- Every database change is a script that dry-runs by default and commits only with its own `CONFIRM_*=1` on port 5432, after the user has seen the dry run and said yes. Adding a column needs no security re-apply: `app_readonly`'s SELECT covers the table.
- Load from the cache first. New requests are limited to the roster-only players' profiles: 48 today. Use the scraper's user agent, at least 5 s between requests, and cache every page.
- Every link is checked against name and date of birth before it is trusted (see Task 2).
- `docs/shared_definitions.md` gets a "Basket person key" row naming its owner.
- Use base apply functions, not purrr. Write any file containing backslashes with Write/Edit, never with a heredoc.

## Review Focus

1. **A link joins two different people** (a site error, or a recycled id). Two registrations in one component that disagree on date of birth (both non-NULL) must stop the person-key write and name both ids. Pinned in Task 3 by `test_person_ids_refuse_conflicting_dob`.
2. **A newly fetched profile bridges two existing components.** The merged component takes the smaller key, and the script prints every registration whose key changed. Pinned in Task 3 by `test_person_ids_merge_takes_smaller`.
3. **An array id points outside our table** (before 2022). It still joins the component, so it can be the key. Pinned in Task 3 by `test_person_ids_use_outside_ids`.
4. **A profile with no links** (a brand-new player), or a failed profile fetch for a roster-only player.
   - No links: the array is empty (`'{}'`), not NULL. NULL means "page never read".
   - The registration keeps a component of its own, keyed by its own id, and the run does not fail.
   - Pinned in Task 1 by `test_links_empty_page` and in Task 4 by `test_roster_profile_fetch_failure_is_skipped`.
5. **The borrowed English name matches nobody, or matches two identities.** The existing rules decide: no hit stays unmatched, two hits stay ambiguous. A row's own English name always wins over a borrowed one. Pinned in Task 5 by `test_borrowed_name_is_only_a_candidate`.

---

### Task 1: Parse a profile page's linked ids

**Files:**
- Modify: `scripts/backfill_basket_shooting_history.R`. Add `parse_basket_player_links(html, own_id)` next to `parse_basket_player_profile_html` (`:580`).
- Test: `etl/tests/test_basket_shooting_history_parser.R`.

**Interfaces:**
- Produces `parse_basket_player_links(html, own_id) -> integer vector`: the distinct PlayerIds in the past-seasons rows and the previous-season link, excluding `own_id`, sorted ascending. `integer(0)` when there are none.

- [ ] **Step 1: Write the failing tests.**
  - **`test_links_rows`:** the fixture is the two history rows quoted in "Facts", plus `<a href="player.asp?PlayerId=11214">2019-20</a>`, plus the previous-season `<li>` and a history row for the page's own id 21797. With `own_id = 21797`, expect `c(11214L, 12490L, 19720L)`.
  - **`test_links_empty_page`:** `parse_basket_player_links("<p></p>", 1L)` is `integer(0)`.
  - **`test_links_cached_21797`:** skipped unless `exports/cache/basket_shooting_history/profiles/2026/21797.html` exists. The result contains 12490 and 19720 and not 21797.
- [ ] **Step 2: Run the tests and see them fail.** `cd etl/tests && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('test_basket_shooting_history_parser.R')"`. Expected: FAIL, the function is not found.
- [ ] **Step 3: Implement it** with two `(?s)` patterns built from the markup quoted in "Facts". Only ids inside a history row (`PlayerId=N">YYYY-YY</a>`) or inside the `player-prev-year` `<li>` count. The page has other `PlayerId` links (team-mates), which must not.
- [ ] **Step 4: Run the tests.** Expected: PASS, and the existing 66 still pass.
- [ ] **Step 5: Commit** `etl: parse basket.co.il's past-seasons links off a profile page`.

### Task 2: The `linked_basket_player_ids` column, filled from the cache

**Files:**
- Create: `sql/migrations/<date>_basket_registration_links.sql`. It runs `ALTER TABLE basketball_test.basket_player_registration ADD COLUMN linked_basket_player_ids integer[]` and `ADD COLUMN basket_person_id integer`, adds a GIN index on `linked_basket_player_ids` and a btree index on `basket_person_id`, and sets column comments.
- Create: `scripts/load_basket_registration_links.R` (dry run by default; `CONFIRM_BASKET_LINKS=1` commits).

**Interfaces:**
- Produces the columns. `linked_basket_player_ids` is NULL when the page was never read and `'{}'` when it was read and has no links. `basket_person_id` is filled by Task 3.
- Produces `write_basket_links(con, links)`, used by Tasks 2 and 4. `links` is `data.frame(game_year, basket_player_id, linked_basket_player_ids = I(list of integer))`. The function UPDATEs by key, and stops unless it touched exactly `nrow(links)` rows.

- [ ] **Step 1: Write the migration.**
- [ ] **Step 2: Write the load script.** In one transaction, it applies the migration and then, for every registration with a cached profile `profiles/<season>/<id>.html`, parses it (Task 1) and writes the array. Gates, each one `stop()` on failure:
  - every registration with a cached profile got an array;
  - for every link whose both ends are registrations, Hebrew name and date of birth are equal (0 disagreements; measured 460/460);
  - the number of registrations with links, without links and unread is printed per season.
- [ ] **Step 3: Dry run.** Expected: 1,339 arrays (the stats rows), 48 unread (the roster-only rows), 0 disagreements, `ROLLED BACK`.
- [ ] **Step 4: STOP for the user's yes, then apply.** Expected: `COMMITTED`; `app_readonly` can still SELECT the table.
- [ ] **Step 5: Commit** the SQL and the script: `sql: basket_player_registration keeps basket.co.il's cross-season links`.

### Task 3: Derive `basket_person_id`

**Files:**
- Create: `etl/basket_person_ids.R` (pure function plus a `main`; dry run by default, `--write` commits).
- Test: `etl/tests/test_basket_person_ids.R`.

**Interfaces:**
- Consumes the Task 2 columns.
- Produces `assign_basket_person_ids(registrations) -> data.frame(game_year, basket_player_id, basket_person_id)`:
  - `registrations` has `game_year, basket_player_id, date_of_birth, linked_basket_player_ids` (a list column);
  - the links are undirected, and the key is the minimum id in the component;
  - it stops when one component holds two non-NA dates of birth that differ.

- [ ] **Step 1: Write the failing tests:**
  - `test_person_ids_chain`: ids 10, 20 and 30 with arrays 30→{20} and 20→{10} all get 10.
  - `test_person_ids_merge_takes_smaller`: {10, 20} and {5, 40} joined by 40→{20} all get 5.
  - `test_person_ids_use_outside_ids`: registration 20 with array {7}, where 7 has no registration, gets 7.
  - `test_person_ids_two_ids_one_season`: 2027 ids 26116 and 28237, both with array {21808}, get 21808.
  - `test_person_ids_refuse_conflicting_dob`: two linked registrations with dates of birth 1999-01-01 and 2000-01-01 give an error naming both ids.
  - `test_person_ids_alone`: a registration with a NULL array, and one with `integer(0)`, each get their own id.
- [ ] **Step 2: Run them and see them fail.** Expected: the function is not found.
- [ ] **Step 3: Implement it** with union-find over the integer ids, in base R.
- [ ] **Step 4: Run the tests.** Expected: PASS.
- [ ] **Step 5: Write `main`.** It reads the registrations, assigns the keys, and prints:
  - the number of persons;
  - the number of registrations per person (a histogram);
  - every registration whose key changes from its stored value.

  With `--write`, it updates `basket_person_id` in one transaction and reads the values back.
- [ ] **Step 6: Dry run.** Expected:
  - Paretsky's 26116 and 28237 share 21808's key;
  - the 460 linked pairs share keys;
  - no conflicting date of birth.
- [ ] **Step 7: STOP for the user's yes, then `--write`.**
- [ ] **Step 8: Commit** `etl: basket_person_id ties a player's Basket registrations across seasons`.

### Task 4: Keep the links current

**Files:**
- Modify: `scripts/backfill_basket_shooting_history.R`. `write_scrape`'s profile UPDATE also sets `linked_basket_player_ids` from `parse_basket_player_links()` for every profile it parsed, on the registration rows only (not on `basket_player_season`).
- Modify: `scripts/basket_rosters.R`. After `write_basket_rosters`, it fetches the profiles of the season's `source = 'roster'` registrations through the existing `scrape_basket_profiles()` (`:657`) with `pause_seconds = 5`, and writes only their arrays with `write_basket_links()`. It does not write their facts: a roster row keeps one source. A failed fetch prints a warning and leaves that row's array as it was.
- Modify: `.github/workflows/basket-refresh.yml`. Add the step `Rscript etl/basket_person_ids.R --write` after the roster step and before the identity link, with the comment "Ties registrations across seasons; the matcher reads it."
- Test: `etl/tests/test_basket_rosters.R`.

- [ ] **Step 1: Write the failing test.** In `test_roster_profile_fetch_failure_is_skipped`, the link builder for roster rows, given one fetched page and one failed fetch, returns one row (the fetched page's), and the failure produces a warning, not an error.
- [ ] **Step 2: Run it and see it fail; implement; run it again.** Expected: PASS, along with every existing roster test.
- [ ] **Step 3: Dry-run the roster script.** The first run downloads the 48 roster-only profiles, so check that the run prints 48 at most, 5 s apart (about 4 minutes) before letting it go. Expected: about 48 arrays, and links covering the 17 pairs found by name and date of birth.
- [ ] **Step 4: Run the roster script with `--write`, then `basket_person_ids.R` (dry run, then `--write`),** each after the user's yes. Expected: the 17 pairs now share keys.
- [ ] **Step 5: Commit** `etl: the Basket refresh keeps cross-season links and person keys current`.

### Task 5: The matcher borrows a person's English name

**Files:**
- Modify: `etl/basket_identity_match.R`. `fetch_basket_registrations` (`:99`) also returns `basket_person_id`, plus `borrowed_name_en`: the most recent non-NULL `player_name_en` among the person's other registrations. `match_registration` (`:140`) tries `norm_en` from `borrowed_name_en` only when `player_name_en` is NA. The result's `matched_on` stays `"name_en"`. A registration matched this way is flagged in the report as "English name from Basket <season> registration <id>".
- Test: `etl/tests/test_basket_identity_match.R` (create it if absent; source the matcher with `main` not run).
- Modify: `docs/shared_definitions.md`. Add a "Basket person key" row:
  - owner: `etl/basket_person_ids.R`, from `linked_basket_player_ids`;
  - rule: the smallest PlayerId in the linked component, and a date-of-birth conflict stops the write;
  - consumer: the matcher's borrowed English name, its only one;
  - display only: nothing is computed from it.

- [ ] **Step 1: Write the failing tests.**
  - `test_borrowed_name_matches`: a registration with `player_name_en` NA and borrowed "Roy Paretsky", in a team pool holding the identity "ROY PARETSKY", matches it.
  - `test_borrowed_name_is_only_a_candidate`: a borrowed name with no hit stays unmatched; a borrowed name with two hits gives the existing ambiguous outcome; a row with its own English name ignores the borrowed one.
- [ ] **Step 2: Run them and see them fail; implement; run them again.** Expected: PASS.
- [ ] **Step 3: Dry-run the matcher.** `Rscript etl/basket_identity_match.R --seasons=2025,2026,2027`. Expected:
  - 2025 and 2026 unchanged (100%);
  - 2027: at least 28237 (Paretsky) and 28240 (Nicholls) move from unmatched to verified, and no existing link changes.
- [ ] **Step 4: STOP for the user's yes, then run with `--write`.** The display names view picks the new links up on its own. Check that its 2027 heights do not drop below 222.
- [ ] **Step 5: Commit** the matcher, its test and the contract row: `etl: the identity matcher borrows a person's English name across seasons`. Push, open a PR, and merge only on "deploy".
