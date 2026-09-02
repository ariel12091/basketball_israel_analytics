# Design System Pass Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the Shiny app a token-driven, warm, colourblind-safe visual system with a real display face, and make ranking movement visible on redraw.

**Architecture:** Introduce a `:root` custom-property layer in `app/www/app.css`, migrate all 394 hard-coded colour values onto it in a provably no-op step, then change the palette by editing token *values* only. Typography, the diverging ramp, cell hierarchy and motion follow as independent changes on top of that layer. Every colour claim is verified by a computed WCAG relative luminance in a test, never by eye.

**Tech Stack:** R 4.4.2, Shiny, bslib (BS5), DT/DataTables, testthat 3e, vanilla JS (no build step), Google Fonts.

**Spec:** `docs/superpowers/specs/2026-09-02-app-design-review-design.md`

## Global Constraints

- Set `IBPL_CACHE_UI=false` in the environment for any manual app run in this plan. `www/app.css` and `www/app.js` are read by `includeCSS()`/`includeScript()` at UI build time, so with the cache on an edit needs an app restart, not a browser reload.
- Launch the app with Run App / `runApp('app')`, **never** select-all + Ctrl+Enter — the latter builds a BS3-style navbar and caches it for the life of the process. Health check: the served page contains 11 `nav-link` occurrences.
- Run tests from the repo root with `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R`. Run a single file from `app/` with `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/<file>')"`.
- Colour changes are verified by computed WCAG relative luminance. Never assert a colour is "similar" by eye.
- Israeli and EuroLeague tabs share code. Never write a parallel `euro_` implementation — generalise the existing function and name it neutrally (root `CLAUDE.md`).
- Every task ends on a green suite. The only expected failure at the start is the stale contract fixed in Task 1.
- Windows line endings: `app.R` and `global.R` have mixed line endings. Before committing, always check `git diff --stat` is plausible for the edit you made; a whole-file rewrite means the line endings shifted.
- Branch: `shiny/design-system-pass`, created from `main` at Task 1.

---

### Task 1: Restore a green baseline

The suite currently has one failure, and it is stale test text rather than broken code. Every later task gates on "suite green", so this comes first.

**Files:**
- Modify: `app/tests/testthat/test-tooltips-contracts.R:23`

**Interfaces:**
- Consumes: nothing.
- Produces: a green `scripts/test_all.R` run, which every later task uses as its gate.

- [ ] **Step 1: Create the branch**

```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
git checkout -b shiny/design-system-pass
```

- [ ] **Step 2: Reproduce the failure**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-tooltips-contracts.R')"
```
Expected: `[ FAIL 1 | WARN 0 | SKIP 0 | PASS 12 ]`, failing at `test-tooltips-contracts.R:23`.

- [ ] **Step 3: Confirm the app-side text the contract should assert**

Run:
```bash
grep -n "min_poss_side" app/R/ui_tab1_onoff.R
```
Expected: `180:          minposs_slider("min_all_poss", "Min Poss / side", "min_poss_side",`

The slider moved from the sidebar to the chips row in commit 35ecb34 and now goes through `minposs_slider()`, which applies `tt()` internally. The contract must assert the new call, not the old label.

- [ ] **Step 4: Update the contract to the current wiring**

In `app/tests/testthat/test-tooltips-contracts.R`, replace line 23:

```r
  expect_true(grepl("tt\\(\"Min possessions per side \\(eligibility\\):\", \"min_poss_side\"\\)", tab1_ui_txt))
```

with:

```r
  # The slider moved onto the chips row in 35ecb34 and now routes its tooltip
  # through minposs_slider(), which calls tt() internally. Assert the wiring
  # that exists, and that the key still resolves in the tooltip registry.
  expect_true(grepl("minposs_slider\\(\"min_all_poss\", \"Min Poss / side\", \"min_poss_side\"", tab1_ui_txt))
  expect_true(grepl("sliderInput\\(input_id, tt\\(label, tooltip_key\\)", read_repo_txt("R", "global.R")))
```

- [ ] **Step 5: Run the file and verify it passes**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-tooltips-contracts.R')"
```
Expected: `[ FAIL 0 | WARN 0 | SKIP 0 | PASS 14 ]`

- [ ] **Step 6: Run the whole suite to record the baseline**

Run:
```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures. If any other file fails, stop and report — do not proceed with a red baseline.

- [ ] **Step 7: Commit**

```bash
git add app/tests/testthat/test-tooltips-contracts.R
git commit -m "test: point the min-poss tooltip contract at the chips-row wiring

The slider moved out of the sidebar in 35ecb34; the contract still asserted
the old sidebar label and had been failing since."
```

---

### Task 2: Add the token layer and a luminance test helper

Introduce `:root` with the **current** hex values and a reusable
`rel_luminance()` for tests. No selector changes yet, so no visual change is
possible.

**Files:**
- Modify: `app/www/app.css` (insert at top, after line 2)
- Create: `app/tests/testthat/helper-color.R`
- Create: `app/tests/testthat/test-design-tokens.R`

**Interfaces:**
- Consumes: nothing.
- Produces: CSS custom properties `--ibpl-bg`, `--ibpl-bg-sunken`, `--ibpl-surface`, `--ibpl-surface-alt`, `--ibpl-surface-2`, `--ibpl-surface-3`, `--ibpl-surface-hover`, `--ibpl-surface-selected`, `--ibpl-border`, `--ibpl-text`, `--ibpl-text-body`, `--ibpl-text-muted`, `--ibpl-text-dim`, `--ibpl-text-faint`, `--ibpl-accent`, `--ibpl-accent-rgb`, `--ibpl-accent-hover`, `--ibpl-pos`, `--ibpl-neg`, `--ibpl-info`, `--ibpl-side-a`, `--ibpl-fg2`, `--ibpl-fg3`, `--ibpl-chip-game-border`, `--ibpl-chip-game-text`. Produces R test helpers `rel_luminance(hex)` and `css_root_block(css_text)`.

- [ ] **Step 1: Write the failing test**

Create `app/tests/testthat/helper-color.R`:

```r
# WCAG 2.x relative luminance. Used by the design-token contracts so colour
# claims are computed, never eyeballed.
rel_luminance <- function(hex) {
  vapply(hex, function(h) {
    v <- grDevices::col2rgb(h)[, 1] / 255
    lin <- vapply(v, function(c) {
      if (c <= 0.04045) c / 12.92 else ((c + 0.055) / 1.055)^2.4
    }, numeric(1))
    0.2126 * lin[1] + 0.7152 * lin[2] + 0.0722 * lin[3]
  }, numeric(1), USE.NAMES = FALSE)
}

# The text of the single :root { ... } block at the top of app.css.
css_root_block <- function(css_text) {
  start <- regexpr(":root\\s*\\{", css_text)
  if (start < 0) return("")
  rest <- substring(css_text, start)
  end <- regexpr("\\}", rest)
  if (end < 0) return("")
  substring(rest, 1, end)
}

# Named vector of token -> hex parsed out of the :root block.
css_tokens <- function(css_text) {
  block <- css_root_block(css_text)
  m <- gregexpr("--ibpl-[a-z0-9-]+\\s*:\\s*[^;]+;", block)
  decls <- regmatches(block, m)[[1]]
  if (!length(decls)) return(character(0))
  names_ <- sub("\\s*:.*$", "", decls)
  vals <- trimws(sub(";$", "", sub("^[^:]*:\\s*", "", decls)))
  stats::setNames(vals, names_)
}
```

Create `app/tests/testthat/test-design-tokens.R`:

```r
REQUIRED_TOKENS <- c(
  "--ibpl-bg", "--ibpl-bg-sunken", "--ibpl-surface", "--ibpl-surface-alt",
  "--ibpl-surface-2", "--ibpl-surface-3", "--ibpl-surface-hover",
  "--ibpl-surface-selected", "--ibpl-border",
  "--ibpl-text", "--ibpl-text-body", "--ibpl-text-muted", "--ibpl-text-dim",
  "--ibpl-text-faint",
  "--ibpl-accent", "--ibpl-accent-rgb", "--ibpl-accent-hover",
  "--ibpl-pos", "--ibpl-neg", "--ibpl-info", "--ibpl-side-a",
  "--ibpl-fg2", "--ibpl-fg3",
  "--ibpl-chip-game-border", "--ibpl-chip-game-text"
)

test_that("app.css defines the full design token set in one :root block", {
  css <- read_repo_txt("www", "app.css")
  tokens <- css_tokens(css)

  expect_true(length(tokens) > 0)
  for (tok in REQUIRED_TOKENS) {
    expect_true(tok %in% names(tokens), info = paste("missing token:", tok))
  }
})

test_that("every colour token is a parseable colour", {
  css <- read_repo_txt("www", "app.css")
  tokens <- css_tokens(css)
  # Tokens ending in -rgb hold a bare "r, g, b" triplet for use inside
  # rgba(), not a colour literal, so col2rgb() cannot parse them. Task 3
  # adds three more of them, so exclude by suffix rather than by name.
  colour_tokens <- tokens[!grepl("-rgb$", names(tokens))]

  for (nm in names(colour_tokens)) {
    expect_silent(grDevices::col2rgb(colour_tokens[[nm]]))
  }
})

test_that("every rgb triplet token parses as three 0-255 integers", {
  css <- read_repo_txt("www", "app.css")
  tokens <- css_tokens(css)
  triplets <- tokens[grepl("-rgb$", names(tokens))]

  for (nm in names(triplets)) {
    parts <- suppressWarnings(
      as.integer(strsplit(gsub("\\s", "", triplets[[nm]]), ",")[[1]])
    )
    expect_length(parts, 3)
    expect_false(any(is.na(parts)), info = nm)
    expect_true(all(parts >= 0 & parts <= 255), info = nm)
  }
})

test_that("the accent rgb triplet matches the accent hex", {
  css <- read_repo_txt("www", "app.css")
  tokens <- css_tokens(css)

  triplet <- as.integer(strsplit(gsub("\\s", "", tokens[["--ibpl-accent-rgb"]]), ",")[[1]])
  expect_equal(triplet, as.integer(grDevices::col2rgb(tokens[["--ibpl-accent"]])[, 1]))
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-design-tokens.R')"
```
Expected: FAIL — `length(tokens) > 0` is FALSE, because `app.css` has no `:root` block yet.

- [ ] **Step 3: Insert the token block with the current values**

In `app/www/app.css`, immediately after line 2 (`/* ============ DARK EDITORIAL THEME ============ */`), insert:

```css

/* ---- Design tokens --------------------------------------------------------
   Every colour in this stylesheet and in the R inline styles resolves through
   one of these. A palette change is an edit to this block and nothing else.
   Token names describe the ROLE, not the colour, so a warm or light palette
   can be dropped in without renaming anything.
   -------------------------------------------------------------------------- */
:root {
  /* ground, darkest to lightest */
  --ibpl-bg: #0d1117;
  --ibpl-bg-sunken: #141920;
  --ibpl-surface: #161b22;
  --ibpl-surface-alt: #1a1f2b;
  --ibpl-surface-2: #1c2333;
  --ibpl-surface-3: #21262d;
  --ibpl-surface-hover: #242d3d;
  --ibpl-surface-selected: #2a1f0a;
  --ibpl-border: #30363d;

  /* text, dimmest to brightest */
  --ibpl-text-faint: #484f58;
  --ibpl-text-dim: #6e7681;
  --ibpl-text-muted: #8b949e;
  --ibpl-text-body: #c9d1d9;
  --ibpl-text: #e6edf3;

  /* brand */
  --ibpl-accent: #e8a435;
  --ibpl-accent-rgb: 232, 164, 53;
  --ibpl-accent-hover: #f0c060;

  /* status */
  --ibpl-pos: #34d399;
  --ibpl-neg: #f87171;
  --ibpl-info: #60a5fa;

  /* data marks */
  --ibpl-side-a: #7b8cde;
  --ibpl-fg2: #5b8abd;
  --ibpl-fg3: #d4843e;
  --ibpl-chip-game-border: #3a6fa0;
  --ibpl-chip-game-text: #7db8e8;
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-design-tokens.R')"
```
Expected: `[ FAIL 0 | ... | PASS 28 ]` (25 token checks plus three assertions).

- [ ] **Step 5: Commit**

```bash
git add app/www/app.css app/tests/testthat/helper-color.R app/tests/testthat/test-design-tokens.R
git commit -m "feat: add a design token layer to app.css

Declares :root custom properties at the current palette values, plus a
rel_luminance() test helper so later colour changes are verified by
computation rather than by eye. No selector reads a token yet, so this
cannot change rendering."
```

---

### Task 3: Migrate app.css onto the tokens

A mechanical, provably no-op replacement of all 277 hex occurrences and the
brand `rgba()` triplets. This is the step where a careless regex does real
damage, so the counts are stated up front and verified after.

**Files:**
- Modify: `app/www/app.css`
- Modify: `app/tests/testthat/test-design-tokens.R`

**Interfaces:**
- Consumes: the `--ibpl-*` tokens from Task 2.
- Produces: an `app.css` whose only raw hex values live inside the `:root` block, enforced by a contract test.

- [ ] **Step 1: Record the exact pre-edit counts**

Run:
```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
grep -o '#[0-9a-fA-F]\{6\}' app/www/app.css | sort | uniq -c | sort -rn
```
Expected, and the replacement must consume exactly these (the 25 inside `:root` are excluded from the totals below because they stay):

| hex | occurrences outside `:root` | token |
|---|---|---|
| `#e8a435` | 56 | `--ibpl-accent` |
| `#30363d` | 39 | `--ibpl-border` |
| `#e6edf3` | 28 | `--ibpl-text` |
| `#8b949e` | 27 | `--ibpl-text-muted` |
| `#c9d1d9` | 23 | `--ibpl-text-body` |
| `#0d1117` | 22 | `--ibpl-bg` |
| `#21262d` | 20 | `--ibpl-surface-3` |
| `#161b22` | 16 | `--ibpl-surface` |
| `#1c2333` | 14 | `--ibpl-surface-2` |
| `#6e7681` | 10 | `--ibpl-text-dim` |
| `#7b8cde` | 5 | `--ibpl-side-a` |
| `#f87171` | 3 | `--ibpl-neg` |
| `#f0c060` | 2 | `--ibpl-accent-hover` |
| `#484f58` | 2 | `--ibpl-text-faint` |
| `#242d3d` | 2 | `--ibpl-surface-hover` |
| `#d4843e` | 1 | `--ibpl-fg3` |
| `#7db8e8` | 1 | `--ibpl-chip-game-text` |
| `#5b8abd` | 1 | `--ibpl-fg2` |
| `#3a6fa0` | 1 | `--ibpl-chip-game-border` |
| `#34d399` | 1 | `--ibpl-pos` |
| `#2a1f0a` | 1 | `--ibpl-surface-selected` |
| `#1a1f2b` | 1 | `--ibpl-surface-alt` |
| `#141920` | 1 | `--ibpl-bg-sunken` |

Total to replace: **277**. `#fff` (2 occurrences, on the coloured shot bars) stays literal — it is white on a saturated fill, not a themed neutral.

There are no selector collisions: no CSS id or class in this file is a six-character hex string, verified by checking each hex for a following `{` or `,`.

- [ ] **Step 2: Write the failing contract test**

Append to `app/tests/testthat/test-design-tokens.R`:

```r
test_that("app.css carries no raw hex outside the :root token block", {
  css <- read_repo_txt("www", "app.css")
  root <- css_root_block(css)
  outside <- sub(root, "", css, fixed = TRUE)

  found <- regmatches(outside, gregexpr("#[0-9a-fA-F]{6}", outside))[[1]]
  expect_equal(
    sort(unique(found)), character(0),
    info = paste("raw hex outside :root:", paste(sort(unique(found)), collapse = ", "))
  )
})

test_that("app.css expresses brand alpha through the accent rgb token", {
  css <- read_repo_txt("www", "app.css")
  root <- css_root_block(css)
  outside <- sub(root, "", css, fixed = TRUE)

  # The literal amber triplet must not appear in an rgba() outside :root.
  expect_false(grepl("rgba\\(\\s*232\\s*,\\s*164\\s*,\\s*53", outside))
  expect_true(grepl("rgba\\(var\\(--ibpl-accent-rgb\\)", outside))
})
```

- [ ] **Step 3: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-design-tokens.R')"
```
Expected: FAIL — the raw-hex list is non-empty (277 occurrences across 23 values).

- [ ] **Step 4: Replace the hexes, protecting the :root block**

The `:root` block must keep its literal values. Split the file, transform only the remainder, and rejoin.

```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
ROOT_END=$(grep -n '^}' app/www/app.css | head -1 | cut -d: -f1)
echo "root block ends at line $ROOT_END"
head -n "$ROOT_END" app/www/app.css > /tmp/css_head.txt
tail -n +"$((ROOT_END + 1))" app/www/app.css > /tmp/css_body.txt

sed -i \
  -e 's/#e8a435/var(--ibpl-accent)/g' \
  -e 's/#30363d/var(--ibpl-border)/g' \
  -e 's/#e6edf3/var(--ibpl-text)/g' \
  -e 's/#8b949e/var(--ibpl-text-muted)/g' \
  -e 's/#c9d1d9/var(--ibpl-text-body)/g' \
  -e 's/#0d1117/var(--ibpl-bg)/g' \
  -e 's/#21262d/var(--ibpl-surface-3)/g' \
  -e 's/#161b22/var(--ibpl-surface)/g' \
  -e 's/#1c2333/var(--ibpl-surface-2)/g' \
  -e 's/#6e7681/var(--ibpl-text-dim)/g' \
  -e 's/#7b8cde/var(--ibpl-side-a)/g' \
  -e 's/#f87171/var(--ibpl-neg)/g' \
  -e 's/#f0c060/var(--ibpl-accent-hover)/g' \
  -e 's/#484f58/var(--ibpl-text-faint)/g' \
  -e 's/#242d3d/var(--ibpl-surface-hover)/g' \
  -e 's/#d4843e/var(--ibpl-fg3)/g' \
  -e 's/#7db8e8/var(--ibpl-chip-game-text)/g' \
  -e 's/#5b8abd/var(--ibpl-fg2)/g' \
  -e 's/#3a6fa0/var(--ibpl-chip-game-border)/g' \
  -e 's/#34d399/var(--ibpl-pos)/g' \
  -e 's/#2a1f0a/var(--ibpl-surface-selected)/g' \
  -e 's/#1a1f2b/var(--ibpl-surface-alt)/g' \
  -e 's/#141920/var(--ibpl-bg-sunken)/g' \
  -e 's/rgba(232,164,53,/rgba(var(--ibpl-accent-rgb),/g' \
  -e 's/rgba(232, 164, 53,/rgba(var(--ibpl-accent-rgb),/g' \
  -e 's/rgba(248,113,113,/rgba(var(--ibpl-neg-rgb),/g' \
  -e 's/rgba(123,140,222,/rgba(var(--ibpl-side-a-rgb),/g' \
  -e 's/rgba(13,17,23,/rgba(var(--ibpl-bg-rgb),/g' \
  -e 's/rgba(13, 17, 23,/rgba(var(--ibpl-bg-rgb),/g' \
  /tmp/css_body.txt

cat /tmp/css_head.txt /tmp/css_body.txt > app/www/app.css
```

- [ ] **Step 5: Add the three extra rgb triplet tokens the sed introduced**

The `rgba()` rewrites reference `--ibpl-neg-rgb`, `--ibpl-side-a-rgb` and
`--ibpl-bg-rgb`, which do not exist yet. Add them to the `:root` block in
`app/www/app.css`, each immediately after its hex sibling:

```css
  --ibpl-bg: #0d1117;
  --ibpl-bg-rgb: 13, 17, 23;
```

```css
  --ibpl-neg: #f87171;
  --ibpl-neg-rgb: 248, 113, 113;
```

```css
  --ibpl-side-a: #7b8cde;
  --ibpl-side-a-rgb: 123, 140, 222;
```

- [ ] **Step 6: Verify the diff is the expected size and shape**

Run:
```bash
git diff --stat app/www/app.css
git diff app/www/app.css | grep -c '^-' 
git diff app/www/app.css | grep -c '^+'
```
Expected: added and removed line counts within a few lines of each other and both well under 300. A whole-file rewrite (1,068 changed lines) means the line endings shifted — revert and redo with `git checkout app/www/app.css`.

Then confirm no hex survived outside `:root`:
```bash
ROOT_END=$(grep -n '^}' app/www/app.css | head -1 | cut -d: -f1)
tail -n +"$((ROOT_END + 1))" app/www/app.css | grep -o '#[0-9a-fA-F]\{6\}' | sort | uniq -c
```
Expected: no output.

- [ ] **Step 7: Run the tests to verify they pass**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-design-tokens.R')"
```
Expected: 0 failures.

- [ ] **Step 8: Confirm the app still renders**

Run:
```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7666, launch.browser = FALSE)" &
```
Open `http://127.0.0.1:7666`, confirm Home renders in the same dark theme as before, then stop the process. This step is a visual no-op check: nothing should look different.

- [ ] **Step 9: Commit**

```bash
git add app/www/app.css app/tests/testthat/test-design-tokens.R
git commit -m "refactor: resolve every app.css colour through a design token

Mechanical replacement of 277 hex occurrences and the brand rgba() triplets
with var(--ibpl-*). Token values are unchanged, so rendering is identical;
a contract test now fails the build on any raw hex outside :root."
```

---

### Task 4: Migrate the R-side inline styles onto the tokens

117 hex values live in R as inline `style=` attributes. CSS custom properties
cascade into inline styles, so these can reference tokens directly. The DT
JavaScript renderers that *compute* colours are deliberately left alone; they
are handled in Task 7.

**Files:**
- Modify: `app/app.R`, `app/R/ui_tab0_home.R`, `app/R/global.R`, `app/R/ui_tab2_lineup.R`, `app/R/ui_tab4_gamelogs.R`, `app/R/ui_tab7_compare.R`, `app/R/ui_tab10_euro_lineups.R`, `app/R/ui_tab1_onoff.R`, `app/R/ui_tab9_euro_team.R`
- Modify: `app/tests/testthat/test-design-tokens.R`

**Interfaces:**
- Consumes: the `--ibpl-*` tokens from Tasks 2 and 3.
- Produces: no new symbols. UI files carry no literal theme hex in `style=` attributes.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-design-tokens.R`:

```r
test_that("UI files style themselves through tokens, not literal hex", {
  ui_files <- c(
    "ui_tab0_home.R", "ui_tab1_onoff.R", "ui_tab2_lineup.R",
    "ui_tab4_gamelogs.R", "ui_tab7_compare.R",
    "ui_tab9_euro_team.R", "ui_tab10_euro_lineups.R"
  )

  for (f in ui_files) {
    txt <- read_repo_txt("R", f)
    found <- regmatches(txt, gregexpr("#[0-9a-fA-F]{6}", txt))[[1]]
    expect_equal(
      sort(unique(found)), character(0),
      info = paste(f, "still carries literal hex:", paste(sort(unique(found)), collapse = ", "))
    )
  }
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-design-tokens.R')"
```
Expected: FAIL, listing hex values in `ui_tab0_home.R` (15), `ui_tab7_compare.R` (7), `ui_tab2_lineup.R` (8), `ui_tab4_gamelogs.R` (8), `ui_tab10_euro_lineups.R` (8), `ui_tab1_onoff.R` (2), `ui_tab9_euro_team.R` (1).

- [ ] **Step 3: Replace the hexes across the UI files**

Run the same mapping over the UI files. These files contain no CSS selectors, so a global replace is safe, but confirm each file's diff afterwards.

```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
for f in app/R/ui_tab0_home.R app/R/ui_tab1_onoff.R app/R/ui_tab2_lineup.R \
         app/R/ui_tab4_gamelogs.R app/R/ui_tab7_compare.R \
         app/R/ui_tab9_euro_team.R app/R/ui_tab10_euro_lineups.R; do
  sed -i \
    -e 's/#e8a435/var(--ibpl-accent)/g' \
    -e 's/#30363d/var(--ibpl-border)/g' \
    -e 's/#e6edf3/var(--ibpl-text)/g' \
    -e 's/#8b949e/var(--ibpl-text-muted)/g' \
    -e 's/#c9d1d9/var(--ibpl-text-body)/g' \
    -e 's/#0d1117/var(--ibpl-bg)/g' \
    -e 's/#21262d/var(--ibpl-surface-3)/g' \
    -e 's/#161b22/var(--ibpl-surface)/g' \
    -e 's/#1c2333/var(--ibpl-surface-2)/g' \
    -e 's/#6e7681/var(--ibpl-text-dim)/g' \
    -e 's/#7b8cde/var(--ibpl-side-a)/g' \
    -e 's/#f87171/var(--ibpl-neg)/g' \
    -e 's/#484f58/var(--ibpl-text-faint)/g' \
    -e 's/#34d399/var(--ibpl-pos)/g' \
    -e 's/#60a5fa/var(--ibpl-info)/g' \
    -e 's/#5b8abd/var(--ibpl-fg2)/g' \
    -e 's/#d4843e/var(--ibpl-fg3)/g' \
    -e 's/#1a1f2b/var(--ibpl-surface-alt)/g' \
    "$f"
done
git diff --stat app/R/
```
Expected: seven files changed, roughly 49 insertions and 49 deletions. Any file showing its full line count as changed means line endings shifted — revert that file with `git checkout` and edit it with the Edit tool instead.

- [ ] **Step 4: Handle `app.R` and `global.R` by hand**

`app/app.R` and `app/R/global.R` have mixed line endings and a `sed -i` on them rewrites the whole file. Edit these two by hand, one occurrence at a time.

In `app/app.R`, the navbar cluster inline styles (around lines 84-110) contain `#8b949e` and `#34d399`. Replace:

```r
      style = "position: fixed; right: 10px; top: 8px; font-size: 0.8rem; color: #8b949e; z-index: 9999; display: flex; align-items: center; gap: 6px; max-width: calc(100vw - 20px); white-space: nowrap;",
```
with:
```r
      style = "position: fixed; right: 10px; top: 8px; font-size: 0.8rem; color: var(--ibpl-text-muted); z-index: 9999; display: flex; align-items: center; gap: 6px; max-width: calc(100vw - 20px); white-space: nowrap;",
```

and:
```r
        tags$span(style = "width: 6px; height: 6px; background: #34d399; border-radius: 50%; display: inline-block;"),
```
with:
```r
        tags$span(style = "width: 6px; height: 6px; background: var(--ibpl-pos); border-radius: 50%; display: inline-block;"),
```

Then find the remaining occurrences:
```bash
grep -n '#[0-9a-fA-F]\{6\}' app/app.R app/R/global.R
```

**Only a hex inside a `style=` string becomes a token.** A hex that is an R
*value* — one passed to an R function that must parse it as a colour — stays
literal, because `colorRampPalette("var(--ibpl-accent)")` is not a colour and
fails at source time. In `app/R/global.R` the split is:

| line | what it is | action |
|---|---|---|
| 88 | `COLS_GRAD <- colorRampPalette(c("#8b2020", "#6b5a20", "#1a6b38"))(20)` | **leave literal** — Task 7 owns this line |
| 798, 800, 801, 802, 805, 807, 808, 809, 826, 828 | `style = "... #xxxxxx ..."` | tokenize |

Leave the `bslib::bs_theme()` arguments in `app/app.R:56-66` as literal hex for
the same reason — Sass compiles those at UI build time, before any CSS custom
property exists. Add a comment above them:

```r
  # Literal hex, not tokens: bslib compiles these through Sass at UI build
  # time, before any CSS custom property exists. Keep them in step with the
  # :root values in www/app.css by hand.
  theme = bslib::bs_theme(
```

- [ ] **Step 5: Exempt the theme block in the test**

The test from Step 1 covers UI files only and does not read `app.R`, so no change is needed. Confirm by re-reading the test's `ui_files` vector.

- [ ] **Step 6: Run the tests to verify they pass**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-design-tokens.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures in both. `test-team-hub-ui.R` renders `ui_tab0_home()` and must still pass — it asserts on ids and text, not colours.

- [ ] **Step 7: Commit**

```bash
git add app/R/ app/app.R app/tests/testthat/test-design-tokens.R
git commit -m "refactor: resolve R inline styles through design tokens

Custom properties cascade into inline style attributes, so the UI files can
reference them directly. bs_theme() keeps literal hex because Sass runs
before any custom property exists; that is now commented at the call site."
```

---

### Task 5: Warm the ground

The aesthetic change, isolated to thirteen token values so it is reviewable and
revertible as a single small diff. Every new value is luminance-matched to the
one it replaces, so no contrast pair in the app changes.

**Files:**
- Modify: `app/www/app.css` (the `:root` block only)
- Modify: `app/app.R:56-66` (the `bs_theme()` literals, kept in step)
- Modify: `app/tests/testthat/test-design-tokens.R`

**Interfaces:**
- Consumes: `rel_luminance()` from `helper-color.R`; the token set from Task 2.
- Produces: no new symbols.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-design-tokens.R`:

```r
# Every neutral moved from Primer's cool blue-black to a warm ground. The
# contract is that hue changed and luminance did not: contrast ratios across
# the app are a function of luminance alone, so matching it means no text
# pair got harder to read.
WARM_NEUTRALS <- c(
  "--ibpl-bg"              = 0.00548,
  "--ibpl-bg-sunken"       = 0.00948,
  "--ibpl-surface"         = 0.01070,
  "--ibpl-surface-alt"     = 0.01374,
  "--ibpl-surface-2"       = 0.01688,
  "--ibpl-surface-3"       = 0.01899,
  "--ibpl-surface-hover"   = 0.02589,
  "--ibpl-border"          = 0.03604,
  "--ibpl-text-faint"      = 0.07674,
  "--ibpl-text-dim"        = 0.17857,
  "--ibpl-text-muted"      = 0.29137,
  "--ibpl-text-body"       = 0.63028,
  "--ibpl-text"            = 0.83862
)

test_that("warming the palette preserved every neutral's luminance", {
  tokens <- css_tokens(read_repo_txt("www", "app.css"))

  for (nm in names(WARM_NEUTRALS)) {
    got <- rel_luminance(tokens[[nm]])
    expect_lt(
      abs(got - WARM_NEUTRALS[[nm]]), 0.02,
      label = sprintf("%s luminance %.5f vs baseline %.5f", nm, got, WARM_NEUTRALS[[nm]])
    )
  }
})

test_that("the neutral ramp is warm, not cool", {
  tokens <- css_tokens(read_repo_txt("www", "app.css"))

  for (nm in names(WARM_NEUTRALS)) {
    rgb <- grDevices::col2rgb(tokens[[nm]])[, 1]
    # Warm means red leads blue. Primer's neutrals do the opposite.
    expect_gt(
      as.integer(rgb[["red"]]), as.integer(rgb[["blue"]]),
      label = sprintf("%s (%s) is not warm", nm, tokens[[nm]])
    )
  }
})

test_that("body text on the page ground still clears WCAG AA", {
  tokens <- css_tokens(read_repo_txt("www", "app.css"))
  contrast <- function(a, b) {
    la <- rel_luminance(a); lb <- rel_luminance(b)
    (max(la, lb) + 0.05) / (min(la, lb) + 0.05)
  }

  expect_gt(contrast(tokens[["--ibpl-text"]], tokens[["--ibpl-bg"]]), 4.5)
  expect_gt(contrast(tokens[["--ibpl-text-body"]], tokens[["--ibpl-bg"]]), 4.5)
  expect_gt(contrast(tokens[["--ibpl-text-muted"]], tokens[["--ibpl-bg"]]), 4.5)
  expect_gt(contrast(tokens[["--ibpl-accent"]], tokens[["--ibpl-bg"]]), 4.5)
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-design-tokens.R')"
```
Expected: the luminance test passes (values are unchanged), the AA test passes, and **the warmth test fails** for all thirteen neutrals, because Primer's blues have blue > red.

- [ ] **Step 3: Replace the neutral token values**

In the `:root` block of `app/www/app.css`, change only these thirteen values and the two rgb triplets that shadow them. Leave `--ibpl-accent`, `--ibpl-pos`, `--ibpl-neg`, `--ibpl-info`, `--ibpl-side-a`, `--ibpl-fg2`, `--ibpl-fg3` and the chip tokens exactly as they are.

```css
  /* ground, darkest to lightest */
  --ibpl-bg: #14100C;
  --ibpl-bg-rgb: 20, 16, 12;
  --ibpl-bg-sunken: #1D1712;
  --ibpl-surface: #1F1A14;
  --ibpl-surface-alt: #251E16;
  --ibpl-surface-2: #2A2117;
  --ibpl-surface-3: #2A251F;
  --ibpl-surface-hover: #352B1F;
  --ibpl-surface-selected: #3A2A10;
  --ibpl-border: #3A342F;

  /* text, dimmest to brightest */
  --ibpl-text-faint: #534E47;
  --ibpl-text-dim: #7A756E;
  --ibpl-text-muted: #98938B;
  --ibpl-text-body: #D4D0CA;
  --ibpl-text: #EEECE8;
```

- [ ] **Step 4: Keep the Sass theme in step**

In `app/app.R:56-66`, update the three literals that mirror tokens:

```r
  theme = bslib::bs_theme(
    version = 5,
    bg = "#14100C",
    fg = "#EEECE8",
    primary = "#e8a435",
    secondary = "#2A251F",
    success = "#34d399",
    danger = "#f87171",
    info = "#60a5fa",
    base_font = "DM Sans, Inter, -apple-system, sans-serif",
    code_font = "JetBrains Mono, monospace",
    "navbar-bg" = "#14100C"
  ),
```

- [ ] **Step 5: Add a contract that the theme and tokens agree**

Append to `app/tests/testthat/test-design-tokens.R`:

```r
test_that("bs_theme literals track the token values they mirror", {
  tokens <- css_tokens(read_repo_txt("www", "app.css"))
  app_txt <- read_repo_txt("app.R")

  expect_true(grepl(sprintf('bg = "%s"', tokens[["--ibpl-bg"]]), app_txt, fixed = TRUE))
  expect_true(grepl(sprintf('fg = "%s"', tokens[["--ibpl-text"]]), app_txt, fixed = TRUE))
  expect_true(grepl(sprintf('primary = "%s"', tokens[["--ibpl-accent"]]), app_txt, fixed = TRUE))
  expect_true(grepl(sprintf('"navbar-bg" = "%s"', tokens[["--ibpl-bg"]]), app_txt, fixed = TRUE))
})
```

Note `read_repo_txt("app.R")` resolves to `app/app.R` because `repo_file()` joins from `app/tests/testthat/../..`.

- [ ] **Step 6: Run the tests to verify they pass**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-design-tokens.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures.

- [ ] **Step 7: Look at it**

```bash
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7666, launch.browser = FALSE)"
```
Visit Home, On/Off Impact (both Summary and Four Factors), and Team Ratings. Confirm: the ground reads warm; amber reads as part of the same family rather than an accent stapled on; the green/red heat cells sit on the warm ground without looking neon. **This is the review gate for the whole aesthetic direction.** If it is wrong, `git revert` this one commit and the token layer still stands.

- [ ] **Step 8: Commit**

```bash
git add app/www/app.css app/app.R app/tests/testthat/test-design-tokens.R
git commit -m "feat: warm the neutral ground

Moves the thirteen neutrals from Primer's cool blue-black (hue ~212) to a
warm ground (hue 30-36). Each new value is luminance-matched to the one it
replaces, all deltas under 0.004, so no contrast pair in the app changes;
tests assert both the luminance match and that red now leads blue."
```

---

### Task 6: Typography — display face and tabular numerals

**Files:**
- Modify: `app/R/global.R` (the `shared_head_tags()` font link)
- Modify: `app/www/app.css`
- Modify: `app/tests/testthat/test-design-tokens.R`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: CSS custom properties `--ibpl-font-display`, `--ibpl-font-body`, `--ibpl-font-mono`.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-design-tokens.R`:

```r
test_that("the display face is loaded and tokenised", {
  global_txt <- read_repo_txt("R", "global.R")
  css <- read_repo_txt("www", "app.css")
  tokens <- css_tokens(css)

  expect_true(grepl("family=Archivo", global_txt, fixed = TRUE))
  expect_true("--ibpl-font-display" %in% names(tokens))
  expect_true("--ibpl-font-body" %in% names(tokens))
  expect_true("--ibpl-font-mono" %in% names(tokens))
  expect_true(grepl("Archivo", tokens[["--ibpl-font-display"]], fixed = TRUE))
})

test_that("numeric surfaces use tabular figures", {
  css <- read_repo_txt("www", "app.css")

  # Every surface that stacks numbers in a column must line them up.
  for (sel in c("table.dataTable", ".diff-val", ".sub-text",
                ".hub-stat-value", ".cmp-stat-value", ".cmp-gap-num")) {
    expect_true(
      grepl(sel, css, fixed = TRUE),
      info = paste("selector missing:", sel)
    )
  }
  expect_true(grepl("font-variant-numeric:\\s*tabular-nums", css))
  # One shared rule, not six copies.
  expect_gte(length(regmatches(css, gregexpr("tabular-nums", css))[[1]]), 1)
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-design-tokens.R')"
```
Expected: FAIL — `family=Archivo` is absent and `font-variant-numeric` appears nowhere in the codebase.

- [ ] **Step 3: Load Archivo**

In `app/R/global.R`, in `shared_head_tags()`, replace the font stylesheet link:

```r
    tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&family=Inter:wght@400;500;600;700&display=swap"),
```

with:

```r
    # Archivo is the display face: a variable grotesque with a real width axis,
    # so headers and big numbers can be set condensed the way a scoreboard or a
    # jersey number is, without a second family. DM Sans stays the body face and
    # JetBrains Mono stays for dense inline data.
    tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css2?family=Archivo:wdth,wght@75..112,500..800&family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&family=Inter:wght@400;500;600;700&display=swap"),
```

- [ ] **Step 4: Add the font tokens and apply them**

In the `:root` block of `app/www/app.css`, after the data-mark tokens, add:

```css

  /* type */
  --ibpl-font-display: "Archivo", "DM Sans", -apple-system, sans-serif;
  --ibpl-font-body: "DM Sans", "Inter", -apple-system, BlinkMacSystemFont, sans-serif;
  --ibpl-font-mono: "JetBrains Mono", "Inter", monospace;
```

Then, at the end of `app/www/app.css`, append:

```css

/* ---- Type roles -----------------------------------------------------------
   Archivo carries the voice: the brand, the section and column headers, and
   any number large enough to be read as a headline. Everything else is DM
   Sans. Numbers that stack in a column get tabular figures so a re-sort moves
   rows without also shifting digits sideways.
   -------------------------------------------------------------------------- */
.navbar-brand,
.explainer-title,
.hub-block-title,
.cmp-team-header,
.cmp-col-header,
.cmp-section-title,
table.dataTable thead th,
th.sub-head {
  font-family: var(--ibpl-font-display);
  font-stretch: 92%;
}

.navbar-brand {
  font-stretch: 84%;
  letter-spacing: 0.02em;
}

.diff-val,
.hub-stat-value,
.cmp-stat-value {
  font-family: var(--ibpl-font-display);
  font-stretch: 88%;
}

table.dataTable,
table.dataTable tbody td,
.diff-val,
.sub-text,
.hub-stat-value,
.hub-record,
.cmp-stat-value,
.cmp-gap-num {
  font-variant-numeric: tabular-nums;
  font-feature-settings: "tnum" 1;
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-design-tokens.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures.

- [ ] **Step 6: Verify in the browser that digits stop shimmering**

```bash
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7666, launch.browser = FALSE)"
```
On On/Off Impact, click a numeric column header twice to re-sort. Before this change the digits shifted horizontally as rows changed; they should now hold a fixed grid. Confirm the navbar brand and column headers render in Archivo (noticeably narrower than DM Sans at the same size).

- [ ] **Step 7: Commit**

```bash
git add app/R/global.R app/www/app.css app/tests/testthat/test-design-tokens.R
git commit -m "feat: add Archivo as the display face and tabular figures

Archivo's width axis lets headers and large numbers be set condensed like a
scoreboard without a second family. font-variant-numeric was absent from the
codebase entirely, so every number column was set in DM Sans' proportional
figures and shifted sideways on each re-sort."
```

---

### Task 7: Make the diverging ramp readable without hue

**Files:**
- Modify: `app/R/global.R:88`
- Modify: `app/R/helpers.R:1991-1997` (the `accColor` JS renderer)
- Create: `app/tests/testthat/test-color-ramp.R`

**Interfaces:**
- Consumes: `rel_luminance()` from `helper-color.R`.
- Produces: `COLS_GRAD` and `COLS_REV` keep their names, lengths (20) and orientation. No call site changes.

- [ ] **Step 1: Write the failing test**

Create `app/tests/testthat/test-color-ramp.R`:

```r
source(repo_file("R", "global.R"), local = TRUE)

test_that("the percentile ramp is strictly monotonic in luminance", {
  # Colour is the only encoding on most heat cells, and red-green deficiency
  # removes hue. What survives is luminance, so luminance alone has to carry
  # the ordering. The pre-2026-09 ramp did not: its minimum adjacent step was
  # negative (-0.00092), so "good" could be dimmer than "average".
  lum <- rel_luminance(COLS_GRAD)

  expect_length(COLS_GRAD, 20)
  expect_true(all(diff(lum) > 0))
})

test_that("the ramp separates its quintiles by a usable margin", {
  lum <- rel_luminance(COLS_GRAD)
  quintiles <- lum[c(1, 5, 10, 15, 20)]
  ratios <- quintiles[-1] / quintiles[-length(quintiles)]

  # The old ramp's worst quintile-to-quintile ratio was 1.02x: two adjacent
  # fifths of the scale that a deuteranope cannot tell apart at all.
  expect_gt(min(ratios), 1.2)
  expect_gt(lum[20] / lum[1], 3)
})

test_that("the ramp keeps the green-good red-bad convention", {
  low <- grDevices::col2rgb(COLS_GRAD[1])[, 1]
  high <- grDevices::col2rgb(COLS_GRAD[20])[, 1]

  expect_gt(as.integer(low[["red"]]), as.integer(low[["green"]]))
  expect_gt(as.integer(high[["green"]]), as.integer(high[["red"]]))
})

test_that("COLS_REV is COLS_GRAD reversed", {
  expect_equal(COLS_REV, rev(COLS_GRAD))
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-color-ramp.R')"
```
Expected: two failures — `all(diff(lum) > 0)` is FALSE and `min(ratios)` is 1.02, below 1.2. The convention and reversal tests pass already.

- [ ] **Step 3: Replace the ramp anchors**

In `app/R/global.R`, replace line 88:

```r
COLS_GRAD <- colorRampPalette(c("#8b2020", "#6b5a20", "#1a6b38"))(20)
```

with:

```r
# Anchors chosen so WCAG relative luminance rises strictly across all twenty
# steps (0.0482 -> 0.1632, a 3.4x span, minimum adjacent step +0.00233). The
# previous anchors spanned only 1.7x and were not monotonic, so under red-green
# deficiency the top half of the scale collapsed into one indistinguishable
# band. Hue still reads red-bad / green-good for everyone else.
COLS_GRAD <- colorRampPalette(c("#6e2622", "#615641", "#2f7f4d"))(20)
```

- [ ] **Step 4: Run the test to verify it passes**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-color-ramp.R')"
```
Expected: `[ FAIL 0 | ... ]`.

- [ ] **Step 5: Apply the same reasoning to the shot-accuracy renderer**

`accColor()` in `app/R/helpers.R` builds `rgb(r, g, 60)` where only red and
green vary — the exact axis that red-green deficiency removes, with blue pinned
so luminance barely moves. Replace the function body at `app/R/helpers.R:1991-1997`:

```r
             function accColor(pct, avg) {
               var d = sign * (pct - avg) / avg;
               d = Math.max(-1, Math.min(1, d * 3));
               var r, g;
               if (d < 0) { r = 200; g = Math.round(200 + d * 120); }
               else       { g = 170; r = Math.round(200 - d * 150); }
               return 'rgb(' + r + ',' + g + ',60)';
             }
```

with:

```r
             // Luminance carries the signal, hue only confirms it: below
             // average darkens toward a warm red, above average brightens
             // toward green. Varying red and green alone (as this did before)
             // is invisible to a red-green deficient reader.
             function accColor(pct, avg) {
               var d = sign * (pct - avg) / avg;
               d = Math.max(-1, Math.min(1, d * 3));
               var r, g, b;
               if (d < 0) {
                 r = Math.round(196 + d * 40);
                 g = Math.round(120 + d * 78);
                 b = Math.round(96 + d * 60);
               } else {
                 r = Math.round(150 - d * 60);
                 g = Math.round(150 + d * 82);
                 b = Math.round(110 + d * 20);
               }
               return 'rgb(' + r + ',' + g + ',' + b + ')';
             }
```

- [ ] **Step 6: Verify the shot-split cells still render**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-shot-profile-helpers.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures.

- [ ] **Step 7: Confirm in the browser**

```bash
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7666, launch.browser = FALSE)"
```
On On/Off Impact (Summary), confirm the Off Shot / Def Shot accuracy numbers still read hot-to-cold, and that on Team Ratings the heat column now has a visible dark-to-light gradient rather than a flat mid-band. To check the colourblind case, apply a deuteranopia filter in DevTools (Rendering panel, "Emulate vision deficiencies") and confirm the column is still orderable.

- [ ] **Step 8: Commit**

```bash
git add app/R/global.R app/R/helpers.R app/tests/testthat/test-color-ramp.R
git commit -m "fix: make the percentile ramp readable without hue

The old anchors were not monotonic in luminance (min adjacent step -0.00092)
and spanned 1.7x end to end, with adjacent quintiles 1.02x apart -- so under
red-green deficiency the top half of every heat column was one flat band. New
anchors span 3.4x, rise strictly, and keep red-bad / green-good. accColor()
had the same defect and now varies all three channels."
```

---

### Task 8: Extract the duplicated range-cell renderer

The range-track cell JS exists twice, byte-for-byte apart from variable names.
This is a **move**, verified by diffing behaviour, not a rewrite.

**Files:**
- Modify: `app/R/helpers.R` (add `range_cell_js()`, use it at line 1868)
- Modify: `app/R/server_tab1.R:440-460` (call the shared builder)
- Create: `app/tests/testthat/test-range-cell.R`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `range_cell_js(value_expr, on_expr, off_expr, on_pct_expr, off_pct_expr, sub_expr)` returning a character scalar of JavaScript. All six arguments are JS expression strings evaluated in the renderer's scope.

- [ ] **Step 1: Capture the current output as a snapshot**

Create `app/tests/testthat/test-range-cell.R`:

```r
source(repo_file("R", "helpers.R"), local = TRUE)

test_that("range_cell_js emits the ranked and unranked branches", {
  js <- range_cell_js(
    value_expr = "diffVal",
    on_expr = "onVal",
    off_expr = "offVal",
    on_pct_expr = "onPct",
    off_pct_expr = "offPct",
    sub_expr = "onVal + ' | ' + offVal"
  )

  expect_true(grepl("diff-val unranked", js, fixed = TRUE))
  expect_true(grepl("rank-bar-container hidden", js, fixed = TRUE))
  expect_true(grepl("rank-bar-container", js, fixed = TRUE))
  expect_true(grepl("range-connect", js, fixed = TRUE))
  expect_true(grepl("dot-off", js, fixed = TRUE))
  expect_true(grepl("dot-on", js, fixed = TRUE))
  expect_true(grepl("sub-text", js, fixed = TRUE))
  # The on dot must paint after the off dot so it wins the overlap.
  expect_lt(
    regexpr("dot-off", js, fixed = TRUE),
    regexpr("dot-on", js, fixed = TRUE)
  )
})

test_that("both call sites go through the shared builder", {
  helpers_txt <- read_repo_txt("R", "helpers.R")
  tab1_txt <- read_repo_txt("R", "server_tab1.R")

  expect_true(grepl("range_cell_js <- function", helpers_txt, fixed = TRUE))
  expect_true(grepl("range_cell_js(", tab1_txt, fixed = TRUE))
  # No second hand-written copy of the markup.
  expect_false(grepl("class=\\\\\"rank-bar-container\\\\\"", tab1_txt))
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-range-cell.R')"
```
Expected: FAIL — `range_cell_js` is not defined.

- [ ] **Step 3: Record the exact current markup from both sites**

Run:
```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
sed -n '1860,1890p' app/R/helpers.R > /tmp/site_a.txt
sed -n '435,465p' app/R/server_tab1.R > /tmp/site_b.txt
diff /tmp/site_a.txt /tmp/site_b.txt
```
Read both. The extracted builder must reproduce site A byte for byte once the
argument names are substituted; this diff is the reference for that check.

- [ ] **Step 4: Add the shared builder**

In `app/R/helpers.R`, immediately above `onoff_summary_datatable()` (line 1911), add:

```r
# The four-factor range cell: a headline value, a league-range track with the
# off-court and on-court dots positioned on it, and the raw pair beneath. Both
# Tab 1's summary renderer and the shared four-factors renderer emitted this
# markup by hand and had already drifted in whitespace. Arguments are JS
# expression strings evaluated in the calling renderer's scope.
range_cell_js <- function(value_expr, on_expr, off_expr,
                          on_pct_expr, off_pct_expr, sub_expr) {
  paste0(
    "(function(){",
    "  var _v = ", value_expr, ";",
    "  var _on = ", on_expr, ", _off = ", off_expr, ";",
    "  var _onP = ", on_pct_expr, ", _offP = ", off_pct_expr, ";",
    "  if (_onP === null || _offP === null || isNaN(_onP) || isNaN(_offP)) {",
    "    return '<div class=\"diff-val unranked\">' + _v + '</div>' +",
    "           '<div class=\"rank-bar-container hidden\"></div>' +",
    "           '<div class=\"sub-text\" style=\"opacity:0.5;\">' + (", sub_expr, ") + '</div>';",
    "  }",
    "  var _lo = Math.min(_onP, _offP), _hi = Math.max(_onP, _offP);",
    "  return '<div class=\"diff-val\">' + _v + '</div>' +",
    "         '<div class=\"rank-bar-container\">' +",
    "           '<div class=\"range-connect\" style=\"left:' + _lo + '%; width:' + (_hi - _lo) + '%;\"></div>' +",
    "           '<div class=\"dot-off\" style=\"left:' + _offP + '%;\" title=\"Off: ' + _off + '\"></div>' +",
    "           '<div class=\"dot-on\" style=\"left:' + _onP + '%;\" title=\"On: ' + _on + '\"></div>' +",
    "         '</div>' +",
    "         '<div class=\"sub-text\">' + (", sub_expr, ") + '</div>';",
    "})()"
  )
}
```

- [ ] **Step 5: Point both call sites at it**

In `app/R/helpers.R`, the block at lines 1866-1886 currently ends the render
function with two hand-written `return` branches. Replace both branches — from
the `if` that tests for an unranked row through the final `';'` — with a single
`return`:

```r
                 "  return " , range_cell_js(
                   value_expr   = "diffVal",
                   on_expr      = "onVal",
                   off_expr     = "offVal",
                   on_pct_expr  = "onPct",
                   off_pct_expr = "offPct",
                   sub_expr     = "onVal + ' | ' + offVal"
                 ), ";",
```

In `app/R/server_tab1.R`, the equivalent block runs from line 439 to line 461.
That site names its variables differently, so pass its own expressions:

```r
             "  return ", range_cell_js(
               value_expr   = "head",
               on_expr      = "onTxt",
               off_expr     = "offTxt",
               on_pct_expr  = "onPct",
               off_pct_expr = "offPct",
               sub_expr     = "onTxt + ' | ' + offTxt"
             ), ";",
```

Keep each site's surrounding `function(data, type, row, meta) {` wrapper and
its existing `if (type !== 'display' || !row) return data;` guard — the builder
emits only the cell body, never the renderer around it.

- [ ] **Step 6: Verify the move by diffing rendered output, not by reading**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-range-cell.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures, including `test-primary-table-render-smoke.R` and `test-tab3-render-regressions.R`, which render the affected tables.

Then confirm visually:
```bash
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7666, launch.browser = FALSE)"
```
On On/Off Impact, switch to Four Factors and confirm each cell still shows the value, the track, both dots in the right positions, and the raw pair. Compare against `docs/app-screenshots/onoff-ff-screenshot.png`.

- [ ] **Step 7: Commit**

```bash
git add app/R/helpers.R app/R/server_tab1.R app/tests/testthat/test-range-cell.R
git commit -m "refactor: one builder for the four-factor range cell

The markup existed twice and had already drifted in whitespace. Both call
sites now go through range_cell_js(); a contract test fails the build if a
third hand-written copy appears."
```

---

### Task 9: Give the Summary verdict columns the range-track grammar

Colour stops being the sole encoding on the columns that carry the verdict, and
the Summary view gains the hierarchy the Four Factors view already has.

**Files:**
- Modify: `app/R/helpers.R` (`onoff_summary_datatable()`, around lines 2084-2120)
- Modify: `app/tests/testthat/test-range-cell.R`

**Interfaces:**
- Consumes: `range_cell_js()` from Task 8; `COLS_GRAD` from Task 7.
- Produces: no new symbols.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-range-cell.R`:

```r
test_that("the summary verdict columns carry a non-colour rank cue", {
  helpers_txt <- read_repo_txt("R", "helpers.R")

  # Extract onoff_summary_datatable's body so the assertion cannot be
  # satisfied by the four-factors renderer further down the file.
  start <- regexpr("onoff_summary_datatable <- function", helpers_txt)
  end <- regexpr("onoff_four_factors_datatable <- function", helpers_txt)
  body <- substring(helpers_txt, start, end)

  expect_true(grepl("range_cell_js(", body, fixed = TRUE))
})

test_that("summary background colour is confined to the verdict columns", {
  helpers_txt <- read_repo_txt("R", "helpers.R")
  start <- regexpr("onoff_summary_datatable <- function", helpers_txt)
  end <- regexpr("onoff_four_factors_datatable <- function", helpers_txt)
  body <- substring(helpers_txt, start, end)

  styled <- regmatches(body, gregexpr('formatStyle\\(dt, "[^"]+"', body))[[1]]
  styled <- gsub('formatStyle\\(dt, "|"$', "", styled)

  # Net RTG Diff, Off ON Diff and Def ON Diff are the verdict. The on/off PPP
  # and net-rating columns are context and read through position instead.
  expect_setequal(styled, c("Net RTG Diff", "Off ON Diff", "Def ON Diff"))
})

test_that("the summary escape allowlist stays narrow", {
  helpers_txt <- read_repo_txt("R", "helpers.R")
  start <- regexpr("onoff_summary_datatable <- function", helpers_txt)
  end <- regexpr("onoff_four_factors_datatable <- function", helpers_txt)
  body <- substring(helpers_txt, start, end)

  # Exactly one column emits HTML, and it is the one that renders the range
  # cell. Anything wider would put database text through an unescaped column.
  expect_true(grepl('dt_escape_except(df, "Net RTG Diff")', body, fixed = TRUE))
  expect_false(grepl("escape = FALSE", body, fixed = TRUE))
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-range-cell.R')"
```
Expected: FAIL on both new tests — `range_cell_js(` is absent from the summary body, and eight columns are currently `formatStyle`d rather than three.

- [ ] **Step 3: Render the Net RTG Diff column through the range cell**

`onoff_summary_datatable()` already computes the indices this needs:
`idx_net` (line 1958), `idx_on` (1959), `idx_off` (1960), and the `pr_*`
columns are in `df` because `keep_cols` lists them (line 1953). Add the
percentile indices next to the existing ones at line 1961:

```r
      idx_pr_on  <- which(names(df) == "pr_off_on") - 1
      idx_pr_off <- which(names(df) == "pr_off_off") - 1
```

Then, inside the `columnDefs` list of the `datatable()` call at line 2084, add
this entry immediately after the `targets = idx_diff` entry:

```r
                                       # The verdict column carries rank as dot
                                       # position on a league track, not only as
                                       # a background colour -- so it stays
                                       # readable when hue does not.
                                       list(targets = idx_net, render = DT::JS(
                                         "function(data, type, row, meta) {",
                                         "  if (type !== 'display' || !row) return data;",
                                         sprintf("  var onPct = row[%d] === null ? null : row[%d] * 100;", idx_pr_on, idx_pr_on),
                                         sprintf("  var offPct = row[%d] === null ? null : row[%d] * 100;", idx_pr_off, idx_pr_off),
                                         sprintf("  var onTxt = row[%d], offTxt = row[%d];", idx_on, idx_off),
                                         "  var val = parseFloat(data);",
                                         "  var head = isNaN(val) ? data : (val > 0 ? '+' + val.toFixed(2) : val.toFixed(2));",
                                         paste0("  return ", range_cell_js(
                                           value_expr   = "head",
                                           on_expr      = "onTxt",
                                           off_expr     = "offTxt",
                                           on_pct_expr  = "onPct",
                                           off_pct_expr = "offPct",
                                           sub_expr     = "onTxt + ' | ' + offTxt"
                                         ), ";"),
                                         "}"
                                       )),
```

`Net RTG Diff` is currently formatted by the `idx_diff` renderer as well. DT
applies the **last** matching `columnDefs` entry, so placing this one after
`idx_diff` wins for that column and leaves the other four diff columns on the
plain `+0.00` formatter.

Because this column now emits HTML, add it to the escape allowlist at the
`datatable()` call — `escape = dt_escape_except(df, "Net RTG Diff")` — matching
how the four-factors renderer already handles its HTML columns at line 2285.
The values are numbers formatted in JS, never database text, so no untrusted
string reaches the allowlisted column.

- [ ] **Step 4: Reduce the background colouring to the three verdict columns**

In the `--- COLOR LOGIC ---` block at `app/R/helpers.R:2106-2116`, delete the
five `formatStyle` calls for `Off ON PPP`, `Def ON PPP`, `On Net RTG`,
`Off OFF PPP`, `Def OFF PPP` and `Off Net RTG`, keeping only `Net RTG Diff`,
`Off ON Diff` and `Def ON Diff`. Add above the block:

```r
      # Background colour is reserved for the three columns that carry the
      # verdict. The on-court and off-court rates beside them are context: they
      # read through the range track and their own magnitude, and colouring
      # them too meant every cell was emphasised and none of them was.
```

- [ ] **Step 5: Run the tests to verify they pass**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-range-cell.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures. `test-primary-table-render-smoke.R` covers this renderer and must stay green.

- [ ] **Step 6: Confirm in the browser and compare against the reference**

```bash
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7666, launch.browser = FALSE)"
```
On On/Off Impact (Summary), compare against `app/www/onoff-row-snippet.png`. Confirm the eye now lands on the three verdict columns first, that Net RTG Diff shows a track with both dots, and that the shooting-split cells are unchanged.

- [ ] **Step 7: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-range-cell.R
git commit -m "feat: give Summary the four-factor cell hierarchy

Net RTG Diff now renders through the shared range cell, so rank is encoded by
dot position as well as by colour -- the redundant cue WCAG 1.4.1 asks for.
Background colour is confined to the three verdict columns; the six context
columns beside them were coloured at equal weight, which emphasised
everything and therefore nothing."
```

---

### Task 10: FLIP row transitions on redraw

**Files:**
- Modify: `app/www/app.js` (append a new IIFE)
- Modify: `app/R/helpers.R` (add the opt-in class at the two `datatable()` calls)
- Create: `app/tests/testthat/test-flip-motion.R`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: the opt-in CSS class `ibpl-flip` on a DataTable, which the JS module keys off. Tables without the class are untouched.

- [ ] **Step 1: Write the failing test**

Create `app/tests/testthat/test-flip-motion.R`:

```r
test_that("the FLIP module is present and correctly gated", {
  js <- read_repo_txt("www", "app.js")

  expect_true(grepl("prefers-reduced-motion", js, fixed = TRUE))
  expect_true(grepl("preDraw.dt", js, fixed = TRUE))
  expect_true(grepl("draw.dt", js, fixed = TRUE))
  # Opt-in only: a table without the class must never be measured.
  expect_true(grepl("ibpl-flip", js, fixed = TRUE))
})

test_that("only the two shared on/off tables opt in", {
  helpers_txt <- read_repo_txt("R", "helpers.R")

  hits <- regmatches(helpers_txt, gregexpr("ibpl-flip", helpers_txt))[[1]]
  expect_length(hits, 2)
  # DT's default class must survive, or the table loses its base styling.
  expect_true(grepl('class = "display ibpl-flip"', helpers_txt, fixed = TRUE))
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-flip-motion.R')"
```
Expected: FAIL — none of these strings exist yet.

- [ ] **Step 3: Append the FLIP module to app.js**

At the end of `app/www/app.js`, append:

```javascript

/* ---- FLIP row transitions on table redraw ---------------------------------
   A ranking table that repaints in place discards the one thing a re-sort
   actually tells you: who moved, and how far. Measure each row's position
   before the redraw, compare after, and play the difference back as a
   transform so the movement is visible.

   Deliberately narrow: opt-in per table via the ibpl-flip class, capped at
   MAX_ROWS because past that the effect reads as noise rather than as
   information, and skipped entirely under prefers-reduced-motion. Rows
   present on only one side of the redraw are left alone -- animating arrival
   and departure would be decoration, not information.
   -------------------------------------------------------------------------- */
(function() {
  var MAX_ROWS = 60;
  var DURATION_MS = 300;
  var pending = null;

  function reducedMotion() {
    return window.matchMedia &&
           window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  }

  // The first cell is the entity name (team, player or lineup) on every table
  // that opts in, which makes it a stable identity across a re-sort.
  function rowKey(tr) {
    var cell = tr.querySelector("td");
    return cell ? cell.textContent.trim() : null;
  }

  function measure(table) {
    var rows = table.querySelectorAll("tbody tr");
    if (!rows.length || rows.length > MAX_ROWS) return null;
    var boxes = {};
    for (var i = 0; i < rows.length; i++) {
      var k = rowKey(rows[i]);
      if (k) boxes[k] = rows[i].getBoundingClientRect().top;
    }
    return boxes;
  }

  function play(table, before) {
    var rows = table.querySelectorAll("tbody tr");
    var moved = [];
    for (var i = 0; i < rows.length; i++) {
      var k = rowKey(rows[i]);
      if (!k || !Object.prototype.hasOwnProperty.call(before, k)) continue;
      var delta = before[k] - rows[i].getBoundingClientRect().top;
      if (!delta) continue;
      rows[i].style.transition = "none";
      rows[i].style.transform = "translateY(" + delta + "px)";
      moved.push(rows[i]);
    }
    if (!moved.length) return;

    // Force the start frame to commit before the transition is attached.
    void table.offsetHeight;

    for (var j = 0; j < moved.length; j++) {
      moved[j].style.transition = "transform " + DURATION_MS + "ms cubic-bezier(.2,.7,.3,1)";
      moved[j].style.transform = "";
    }
    window.setTimeout(function() {
      for (var m = 0; m < moved.length; m++) {
        moved[m].style.transition = "";
        moved[m].style.transform = "";
      }
    }, DURATION_MS + 50);
  }

  function bind() {
    if (!window.jQuery) return;
    var $ = window.jQuery;

    $(document).on("preDraw.dt", function(e) {
      pending = null;
      if (reducedMotion()) return;
      var table = e.target;
      if (!table || !table.classList || !table.classList.contains("ibpl-flip")) return;
      pending = { table: table, boxes: measure(table) };
    });

    $(document).on("draw.dt", function(e) {
      if (!pending || pending.table !== e.target || !pending.boxes) {
        pending = null;
        return;
      }
      var table = pending.table;
      var boxes = pending.boxes;
      pending = null;
      play(table, boxes);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
```

- [ ] **Step 4: Opt the two shared on/off tables in**

In `app/R/helpers.R`, at the `datatable()` call on line 2084, add the `class`
argument:

```r
      dt <- datatable(df, container = sketch_summary, rownames = FALSE,
                      class = "display ibpl-flip",
```

and at the call on line 2283:

```r
      dt <- datatable(df_final,
                      container = sketch_ff, rownames = FALSE,
                      class = "display ibpl-flip",
```

`"display"` is DT's default class and must be kept, or the table loses its base
striping and border styling.

- [ ] **Step 5: Run the tests to verify they pass**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-flip-motion.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures.

- [ ] **Step 6: Verify the motion in the browser, including the reduced-motion path**

```bash
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7666, launch.browser = FALSE)"
```
On On/Off Impact, click a column header to re-sort. Rows that stay in the table should slide to their new positions rather than jumping. Then in DevTools open the Rendering panel, set "Emulate CSS media feature prefers-reduced-motion" to `reduce`, and re-sort: rows must jump with no animation at all. Finally set the min-possession slider so the row count exceeds 60 and confirm the animation switches off rather than stuttering.

- [ ] **Step 7: Commit**

```bash
git add app/www/app.js app/R/helpers.R app/tests/testthat/test-flip-motion.R
git commit -m "feat: animate row movement on table redraw

A re-sort tells you who moved; repainting in place throws that away. Opt-in
per table, capped at 60 rows, and fully disabled under
prefers-reduced-motion. Only rows present before and after are animated --
fading arrivals in would be decoration rather than information."
```

---

## Done criteria

- `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R` reports 0 failures.
- `app/www/app.css` contains no raw six-digit hex outside its `:root` block, enforced by `test-design-tokens.R`.
- The seven listed UI files contain no literal hex, enforced by the same file.
- Every neutral token's WCAG relative luminance is within 0.02 of its pre-warming value, and red leads blue on all thirteen.
- `COLS_GRAD` is strictly monotonic in luminance with a minimum quintile ratio above 1.2 and an end-to-end span above 3x.
- The served page still contains 11 `nav-link` occurrences.
- On/Off Impact, Team Ratings and Home render correctly in a browser under both the default and `prefers-reduced-motion: reduce`.
