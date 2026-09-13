test_that("the viewport meta allows pinch zoom", {
  shared_head_tags <- global_defs("shared_head_tags")$shared_head_tags
  # shared_head_tags() returns a bare tags$head(...): htmltools::renderTags()
  # hoists head content into $head (via takeHeads()), leaving $html empty for
  # this input regardless of content -- so the meta tag is asserted on $head.
  html <- htmltools::renderTags(shared_head_tags())$head

  expect_match(html, "width=device-width", fixed = TRUE)
  # maximum-scale=1 blocks pinch zoom, which is an accessibility failure and is
  # unnecessary once the layout actually fits.
  expect_false(grepl("maximum-scale", html, fixed = TRUE))
})

test_that("the mobile layer is loaded after app.css and app.js", {
  app_r <- read_repo_txt("app.R")

  expect_true(grepl("www/mobile.css", app_r, fixed = TRUE))
  expect_true(grepl("www/mobile.js", app_r, fixed = TRUE))
  # Load order: app.css carries load-bearing !important rules the mobile layer
  # must be able to override without removing them.
  expect_lt(
    regexpr("www/app.css", app_r, fixed = TRUE),
    regexpr("www/mobile.css", app_r, fixed = TRUE)
  )
  expect_lt(
    regexpr("www/app.js", app_r, fixed = TRUE),
    regexpr("www/mobile.js", app_r, fixed = TRUE)
  )
  # Both includes must actually carry the kill switch, not just exist -- an
  # always-on, ungated include would still satisfy every check above.
  expect_true(grepl(
    'if (IBPL_MOBILE) includeCSS("www/mobile.css")', app_r, fixed = TRUE
  ))
  expect_true(grepl(
    'if (IBPL_MOBILE) includeScript("www/mobile.js")', app_r, fixed = TRUE
  ))
})

test_that("the mobile layer kill switch resolves the right default per env value", {
  # A string check on global.R (does it contain "IBPL_MOBILE" and the
  # Sys.getenv() call) would pass unchanged even if the %in% set were
  # inverted or a stray "!" flipped the default to off -- silently disabling
  # the whole mobile layer in production. global_defs() re-parses and
  # re-evaluates global.R's top-level IBPL_MOBILE assignment fresh on every
  # call, so lifting it under a controlled env var actually exercises the
  # resolved logical, not just its source text. Re-lift inside each case so
  # each resolution sees its own env var value.
  resolve <- function(value) {
    withr::with_envvar(c(IBPL_MOBILE = value), {
      global_defs("IBPL_MOBILE")$IBPL_MOBILE
    })
  }

  expect_true(resolve(NA))            # unset -> default "true"
  expect_true(resolve("true"))
  expect_false(resolve("false"))
  expect_false(resolve("0"))
  expect_false(resolve("no"))
  expect_false(resolve("FALSE"))       # case-insensitive
  expect_false(resolve(" false "))     # trimmed
  expect_true(resolve("banana"))       # fail-open on garbage, like IBPL_CACHE_UI
})

test_that("mode is carried by a body class, not a bare media query", {
  css <- read_repo_txt("www", "mobile.css")
  js <- read_repo_txt("www", "mobile.js")

  expect_true(grepl("body.ibpl-mobile", css, fixed = TRUE))
  expect_true(grepl("max-width: 767.98px", js, fixed = TRUE))
  # A bare @media in mobile.css would be a second source of truth for "are we
  # mobile", which could disagree with the class the JS sets.
  expect_false(grepl("@media (max-width", css, fixed = TRUE))
})

test_that("mobile rules live only in mobile.css", {
  app_css <- read_repo_txt("www", "app.css")
  mobile_css <- read_repo_txt("www", "mobile.css")

  # The three blocks that used to be scattered through app.css.
  expect_false(grepl("@media (max-width: 768px)", app_css, fixed = TRUE))
  expect_true(grepl(".chips-filters-toggle", mobile_css, fixed = TRUE))
  expect_true(grepl(".irs-handle", mobile_css, fixed = TRUE))
  expect_true(grepl(".chips-row-controls", mobile_css, fixed = TRUE))

  # Block 3 used 767px, not 768px, so the assertion above can't catch it: a
  # regression that left "@media (max-width: 767px) { .chips-filters-toggle
  # { display: none; } }" in app.css while ALSO migrating it into mobile.css
  # would pass every check above (the rule would just exist in both files).
  # Assert on the specific combination that was moved, not on either string
  # alone -- app.css still legitimately has an unrelated 767px media block
  # (.hub-stat-row) and unrelated .chips-filters-toggle base rules outside
  # any media query, and both must keep passing.
  expect_false(grepl(
    "@media \\(max-width: 767px\\)[\\s\\S]{0,120}\\.chips-filters-toggle \\{ display: none; \\}",
    app_css, perl = TRUE
  ))

  # Non-mobile media queries must NOT be dragged along.
  expect_true(grepl("prefers-reduced-motion", app_css, fixed = TRUE))
})

# ---- R1 (2026-09-13 rework): every column visible, identity column pinned
# ----------------------------------------------------------------------------
# Replaces the caret/child-row/priority-column tests above: real-device
# testing rejected that design ("doesn't offer any advantage of the table,
# which is literally compare the teams/players"). The replacement is CSS-only
# (all 41 datatable() calls already set scrollX = TRUE, so no R change), so
# what's testable here is the ABSENCE of the deleted machinery and the
# PRESENCE of the sticky-column rule -- the scroll/pin/alignment behaviour
# itself needs a real browser and is verified there, not here.

test_that("the caret/child-row/priority-column machinery is gone, not disabled", {
  js <- read_repo_txt("www", "mobile.js")
  css <- read_repo_txt("www", "mobile.css")

  expect_false(grepl("IBPL_MOBILE_TABLE", js, fixed = TRUE))
  expect_false(grepl("ibpl-m-caret", js, fixed = TRUE))
  expect_false(grepl("keepSet", js, fixed = TRUE))
  expect_false(grepl("detailHtml", js, fixed = TRUE))
  expect_false(grepl("ibpl-m-caret", css, fixed = TRUE))
  expect_false(grepl("ibpl-m-detail", css, fixed = TRUE))
})

test_that("the identity column is pinned with CSS, covering both scrollHead and scrollBody", {
  css <- read_repo_txt("www", "mobile.css")

  # DT's scrollX splits the header and body into separate tables; both need
  # the sticky rule or the pinned header drifts from the pinned body.
  expect_true(grepl(".dataTables_wrapper table.dataTable > thead > tr > th:first-child", css, fixed = TRUE))
  expect_true(grepl(".dataTables_wrapper table.dataTable > tbody > tr > td:first-child", css, fixed = TRUE))
  expect_true(grepl("position: sticky", css, fixed = TRUE))
  expect_true(grepl("left: 0", css, fixed = TRUE))
  # An opaque background is the whole point -- a transparent sticky column
  # lets scrolled cells show through underneath it.
  expect_true(grepl("background: var(--ibpl-surface) !important", css, fixed = TRUE))
})

test_that("the navbar collapses into a burger", {
  app_r <- read_repo_txt("app.R")

  # Default is FALSE, so 7 tabs wrap or overflow on a phone without this.
  expect_true(grepl("collapsible = TRUE", app_r, fixed = TRUE))
})

# ---- R4 (2026-09-13 rework): view mode lives in the burger menu ----------
# Replaces the two tests above. promote() (which moved the raw
# radios/select above the table) is gone -- "on a phone the bare radios read
# as a stray form control". The replacement drives the SAME inputs through
# app.js's existing per-tab ".tab-hover-menu" (CFG array, app.js:1469, which
# already covers both the nine radio-based tabs and the two type: "select"
# ones -- Player Stats and Team Ratings -- generically), just rendered
# in-flow for the active tab instead of on :hover. There is no per-tab code
# left in mobile.js/mobile.css for this at all, radios or select alike, which
# is itself worth asserting: it means nothing here can drift out of sync with
# app.js's own CFG the way the promoted-copy machinery could.

test_that("promote() and the promoted-radio machinery are gone, not disabled", {
  js <- read_repo_txt("www", "mobile.js")
  css <- read_repo_txt("www", "mobile.css")

  expect_false(grepl("function promote(", js, fixed = TRUE))
  expect_false(grepl("ibpl-m-viewmode", js, fixed = TRUE))
  expect_false(grepl("ibpl-m-viewmode", css, fixed = TRUE))
  # relocateCluster() is untouched R4-adjacent navbar work and must survive.
  expect_true(grepl("function relocateCluster(", js, fixed = TRUE))
})

test_that("the active tab's hover menu renders in the page flow instead of on hover", {
  css <- read_repo_txt("www", "mobile.css")

  # Hidden by default (hover does not exist on touch)...
  expect_true(grepl("body.ibpl-mobile .tab-hover-menu { display: none !important; }", css, fixed = TRUE))
  # ...then shown in-flow for whichever tab is actually active, however that
  # tab marks itself active (BS5 puts .active on the link itself; app.css
  # also defends the older .nav-item.active pattern).
  expect_true(grepl(":has(.nav-link.active) .tab-hover-menu", css, fixed = TRUE))
  expect_true(grepl("display: block !important", css, fixed = TRUE))
  expect_true(grepl("position: static", css, fixed = TRUE))
})

test_that("the fixed navbar cluster is unfixed on mobile", {
  css <- read_repo_txt("www", "mobile.css")

  # app.R:93 sets position:fixed inline; it would sit on top of the burger.
  expect_true(grepl("navbar_right_cluster", css, fixed = TRUE))
  expect_true(grepl("position: static !important", css, fixed = TRUE))
  # body.league-* sets these to inline-flex; the override must not break the
  # league filtering that decides WHICH season selector shows.
  expect_true(grepl(".league-nav-il", css, fixed = TRUE))
  # Static positioning alone does not put a header node inside the burger.
  expect_true(grepl(".navbar-collapse", read_repo_txt("www", "mobile.js"), fixed = TRUE))
})

# ---- R2 (2026-09-13 rework): filters expand inline, never an overlay -----
# Replaces the sheet-routing test above: "show filters should never be a
# popup". mobile.js no longer intercepts the "Show Filters" toggle at all --
# Bootstrap's native collapse plugin owns show/hide exactly as it did before
# this layer existed, so there's nothing left here to test except that the
# interception is genuinely gone (not just unreachable) and that the
# expanded panel still gets mobile-specific sizing.

test_that("the filter panel is native Bootstrap collapse, not a sheet", {
  js <- read_repo_txt("www", "mobile.js")
  css <- read_repo_txt("www", "mobile.css")

  expect_false(grepl('data-bs-toggle="collapse"', js, fixed = TRUE))
  expect_false(grepl('IBPL_MOBILE_SHEET.open("Filters"', js, fixed = TRUE))
  # Matching the shared "-filters" id shape means no per-tab R edit for the
  # mobile sizing that IS still applied.
  expect_true(grepl('[id$="-filters"]', css, fixed = TRUE))
  expect_true(grepl("min-height: var(--ibpl-m-tap)", css, fixed = TRUE))
})

# ---- R5 (2026-09-13 rework): min possessions is secondary, not a headline
# control -- demoted in place, same id and behaviour, only its visual weight
# and its order relative to the tab's other chips-row control changes.

test_that("min possessions is visually demoted, not removed or renamed", {
  css <- read_repo_txt("www", "mobile.css")

  expect_true(grepl(".chips-row-controls .minposs-compact", css, fixed = TRUE))
  expect_true(grepl("order: 2", css, fixed = TRUE))
  # Smaller label, tighter vertical space -- but this is styling only. The
  # slider's id and behaviour live in global.R's minposs_slider(), untouched.
  expect_true(grepl(".minposs-compact .control-label", css, fixed = TRUE))
  expect_true(grepl("font-size: 0.65rem", css, fixed = TRUE))
})

test_that("the sheet is a reusable component", {
  js <- read_repo_txt("www", "mobile.js")
  css <- read_repo_txt("www", "mobile.css")

  expect_true(grepl("IBPL_MOBILE_SHEET", js, fixed = TRUE))
  expect_true(grepl(".ibpl-m-sheet", css, fixed = TRUE))
  # dvh, with a vh fallback declared first, so browser chrome does not crop
  # the footer. Asserted generically here: this task introduces 85dvh for the
  # sheet, and Task 6 adds 100dvh for modals with its own ordering assertion.
  expect_true(grepl("dvh", css, fixed = TRUE))
})

test_that("the sheet body is cleared on open, AFTER close() restores any moved node", {
  js <- read_repo_txt("www", "mobile.js")

  # Task 6 appends tooltip text into the sheet body directly. close() only
  # restores MOVED nodes, so without an explicit clear that text accumulates
  # across opens and leaks into the filter sheet.
  expect_true(grepl('body.innerHTML = ""', js, fixed = TRUE))

  # Ordering is the whole point, not just presence: clearing BEFORE close()
  # would wipe out a still-moved node via innerHTML instead of returning it
  # to the page. Isolate open()'s own body (everything between its signature
  # and the next function's) so this can't be satisfied by the two strings
  # appearing anywhere else in the file.
  start <- regexpr("function open(title, node) {", js, fixed = TRUE)
  end <- regexpr("function close() {", js, fixed = TRUE)
  expect_gt(start, 0)
  expect_gt(end, start)
  open_fn <- substr(js, start, end - 1)

  close_pos <- regexpr("close();", open_fn, fixed = TRUE)
  clear_pos <- regexpr('body.innerHTML = ""', open_fn, fixed = TRUE)
  expect_gt(close_pos, 0)
  expect_gt(clear_pos, 0)
  expect_lt(close_pos, clear_pos)
})

test_that("every tab still has its own filter toggle", {
  ui_files <- list.files(repo_file("R"), pattern = "^ui_tab.*\\.R$", full.names = TRUE)
  toggles <- sum(vapply(ui_files, function(f) {
    sum(grepl("d-md-none", readLines(f, warn = FALSE), fixed = TRUE))
  }, integer(1)))

  # 11 tab UIs each carry one. A drop here means a tab lost its filters.
  expect_gte(toggles, 11L)
})

test_that("xl modals go full screen on mobile", {
  css <- read_repo_txt("www", "mobile.css")

  expect_true(grepl(".modal-dialog", css, fixed = TRUE))
  # dvh, not vh -- mobile browser chrome would crop the footer.
  expect_true(grepl("100dvh", css, fixed = TRUE))
  # A vh fallback must be declared FIRST for browsers without dvh.
  expect_lt(
    regexpr("height: 100vh", css, fixed = TRUE),
    regexpr("height: 100dvh", css, fixed = TRUE)
  )
})

test_that("both tooltip mechanisms get a tap path", {
  js <- read_repo_txt("www", "mobile.js")

  # Native title on th, written by HEADER_TOOLTIP_JS.
  expect_true(grepl("th[title]", js, fixed = TRUE))
  # data-tooltip on tt() labels -- a different mechanism, own selector.
  expect_true(grepl("[data-tooltip]", js, fixed = TRUE))
  expect_true(grepl("IBPL_MOBILE_SHEET.open", js, fixed = TRUE))
})

test_that("HEADER_TOOLTIP_JS is unchanged", {
  global_r <- read_repo_txt("R", "global.R")

  # The mobile layer reads the title attribute; it does not change how it is
  # written. Touching this would affect desktop too.
  expect_true(grepl("cell.attr('title', tips[txt])", global_r, fixed = TRUE))
})

test_that("the stat-filter popover keeps its input ids", {
  helpers <- read_repo_txt("R", "helpers.R")

  # Relocating the popover BODY must not rename inputs, or
  # apply_stat_filters() and every observer break.
  expect_true(grepl('paste0(prefix, "_stat_filter_col")', helpers, fixed = TRUE))
  expect_true(grepl('paste0(prefix, "_stat_filter_value")', helpers, fixed = TRUE))
})

test_that("the info-dot tap is intercepted in the capture phase", {
  js <- read_repo_txt("www", "mobile.js")

  # DataTables' sort listener is bound directly on <th>, closer to the
  # target than a bubble-phase document handler, so stopPropagation() there
  # always runs too late. Verified live (Task 6 fix round 1): a bubble-phase
  # $(document).on() let an info-dot tap also re-sort the column.
  start <- regexpr(
    'var $info = $(e.target).closest(".ibpl-m-th-info");',
    js, fixed = TRUE
  )
  expect_gt(start, 0)
  rest <- substring(js, start)
  end <- regexpr("}, true);", rest, fixed = TRUE)
  # A revert to bubble phase ($(document).on(...)) would end this block with
  # "});" instead, so this forward search would fail to find "}, true);"
  # before running off the end of the block (or find nothing at all).
  expect_gt(end, 0)
})

test_that("the filter-chip-add tap is intercepted in the capture phase, in its own component", {
  js <- read_repo_txt("www", "mobile.js")

  # R3 (2026-09-13 rework) gave the stat-filter popover its own inline
  # component instead of routing it through the sheet; it no longer shares a
  # listener with the info-dot (each selector now gets its own capture-phase
  # block), but the same underlying hazard applies: Bootstrap binds its
  # popover show/hide directly on the trigger, closer to the target than a
  # bubble-phase document handler.
  start <- regexpr(
    'var $trigger = $(e.target).closest(".filter-chip-add");',
    js, fixed = TRUE
  )
  expect_gt(start, 0)
  rest <- substring(js, start)
  end <- regexpr("}, true);", rest, fixed = TRUE)
  expect_gt(end, 0)
  block <- substring(rest, 1, end + nchar("}, true);") - 1)

  # Confirms this is its OWN listener, not sharing a block with the info-dot
  # handler any more.
  expect_false(grepl("ibpl-m-th-info", block, fixed = TRUE))
})

test_that("the stat-filter panel is moved inline, restored on desktop, and cleaned up on rerender", {
  js <- read_repo_txt("www", "mobile.js")
  css <- read_repo_txt("www", "mobile.css")

  # Moved (not cloned) into a wrapper this component owns, so the
  # selectInput/radioButtons/numericInput inside keep their ids.
  expect_true(grepl('wrap.setAttribute("data-ibpl-m-filteradd", prefix)', js, fixed = TRUE))
  expect_true(grepl('wrap.appendChild(panel)', js, fixed = TRUE))
  # Desktop must keep working: a held panel is restored to its <bslib-popover>
  # origin before mobile mode is left.
  expect_true(grepl("ibpl:mobilechange", js, fixed = TRUE))
  start <- regexpr("function restore(prefix)", js, fixed = TRUE)
  expect_gt(start, 0)
  # The chips row re-renders wholesale on every Add/Remove, orphaning a held
  # panel's origin the same way it used to orphan the sheet's -- detect and
  # discard it via shiny:value, the same shape as IBPL_MOBILE_SHEET.
  expect_true(grepl('$(document).on("shiny:value"', js, fixed = TRUE))
  expect_true(grepl("!document.contains(origin.parent)", js, fixed = TRUE))

  expect_true(grepl(".ibpl-m-filteradd", css, fixed = TRUE))
})

test_that("[data-tooltip] taps do not call preventDefault", {
  js <- read_repo_txt("www", "mobile.js")

  # preventDefault() here suppressed the native label-click toggle on every
  # checkbox/radio a tt() label wraps (ts_clutch_enabled, tst_clutch_enabled,
  # cmp_a_clutch, cmp_b_clutch, and more across the sidebars) -- verified
  # live (Task 6 fix round 1): tapping "Enable clutch filter"'s tooltip text
  # left the checkbox unchecked and closed an already-open Filters sheet.
  # Scope to this handler's own body -- the same way the sheet's open()/
  # close() ordering test above scopes to open() -- so this can't pass on
  # preventDefault() calls elsewhere in the file (the th-info and
  # filter-chip-add handlers legitimately call it, to stop DataTables/
  # Bootstrap's own listeners).
  start <- regexpr(
    '$(document).on("click", "[data-tooltip]", function (e) {',
    js, fixed = TRUE
  )
  expect_gt(start, 0)
  rest <- substring(js, start)
  end <- regexpr("});", rest, fixed = TRUE)
  expect_gt(end, 0)
  handler <- substring(rest, 1, end + 2)

  expect_true(grepl("textSheet", handler, fixed = TRUE))
  expect_false(grepl("preventDefault", handler, fixed = TRUE))
})

test_that("[data-tooltip] ignores untrusted clicks (R4 regression: synthetic radio.click() bubbles into a tooltip label)", {
  js <- read_repo_txt("www", "mobile.js")

  # R4 makes the burger menu drive a REAL radio via a synthetic
  # radio.click() (app.js updateInput()). Several view-mode choice labels
  # (e.g. onoff_view_mode's "Four Factors") carry their own data-tooltip --
  # tt() puts it on the <label> itself, an ANCESTOR of the <input> -- so that
  # synthetic click bubbles straight into this delegated handler and pops
  # the tooltip sheet open on every mode switch made from the menu. Verified
  # live with a capture-phase click logger: none of the clicks in that
  # sequence (burger toggle, thm-item, nav-link, radio input) were a real
  # tap on the tooltip label, yet the sheet opened. A real finger tap is
  # always isTrusted; app.js's own programmatic clicks never are.
  start <- regexpr(
    '$(document).on("click", "[data-tooltip]", function (e) {',
    js, fixed = TRUE
  )
  expect_gt(start, 0)
  rest <- substring(js, start)
  end <- regexpr("});", rest, fixed = TRUE)
  expect_gt(end, 0)
  handler <- substring(rest, 1, end + 2)

  expect_true(grepl("e.isTrusted", handler, fixed = TRUE))
})

test_that("the sheet auto-closes when its held content's origin has been detached", {
  js <- read_repo_txt("www", "mobile.js")

  # The stat-filter popover's home output (a renderUI) regenerates its
  # entire trigger+content on every filter add/remove, orphaning whatever
  # the sheet is currently holding and creating a duplicate-id node --
  # verified live (Task 6 fix round 1): two `.on-stat-popover` nodes existed
  # at once, and Shiny logged its own "IDs were repeated" warning. Without
  # this listener the sheet keeps showing the stale orphan indefinitely.
  expect_true(grepl('window.jQuery(document).on("shiny:value"', js, fixed = TRUE))
  expect_true(grepl("!document.contains(origin.parent)", js, fixed = TRUE))
})

# ---- Task 7: Compare ------------------------------------------------------

test_that("Compare keeps A and B adjacent on mobile", {
  css <- read_repo_txt("www", "mobile.css")

  # Three col-4 cards would give each ~120px. A and B two-up, Gap full width.
  expect_true(grepl("cmp-summary", css, fixed = TRUE))
})

# The Compare-specific column-priority override ("the Compare table override
# names the real columns") and the promoted-copy CSS for #cmp_mode ("Compare's
# hidden mode radio is reachable on mobile") are gone along with the rest of
# IBPL_MOBILE_TABLE and promote() -- see the R1 and R4 sections above.
# Compare needs no special-casing under either replacement: R1 shows every
# column for every table generically, and R4's tab-hover-menu CFG entry for
# "compare" (app.js:1475) already drives #cmp_mode the same way every other
# tab's view mode is driven now.

# ---- Task 8: Gameflow ------------------------------------------------------

# "the gameflow link survives the column priority rule" is gone with the rest
# of IBPL_MOBILE_TABLE's gl_table/eurogl_table overrides (R1, above): every
# column is visible now, so Gameflow needs no priority rule to protect it.

test_that("the ribbon keeps its designed width on mobile", {
  helpers <- read_repo_txt("R", "helpers.R")
  css <- read_repo_txt("www", "mobile.css")

  # Hardcoding the width in CSS would silently drift from the R geometry.
  # Pin them together: this test fails if RIBBON_WIDTH ever changes.
  m <- regmatches(helpers, regexpr("RIBBON_WIDTH <- [0-9]+", helpers))
  expect_length(m, 1L)
  w <- sub("RIBBON_WIDTH <- ", "", m)

  expect_true(grepl(paste0("min-width: ", w, "px"), css, fixed = TRUE))
  # Scrolling, not scaling: app.css sets width:100%, which is what shrinks the
  # 11px labels to 3.8px at 390px.
  expect_true(grepl("overflow-x: auto", css, fixed = TRUE))
})
