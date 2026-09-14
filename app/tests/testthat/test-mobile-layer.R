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
  # R6 (2026-09-13 hardening): both now open an inline strip, not a sheet.
  expect_true(grepl("function toggleHeaderStrip(", js, fixed = TRUE))
  expect_true(grepl("function toggleLabelStrip(", js, fixed = TRUE))
})

# ---- R6 (2026-09-13 hardening): header info-dot hit area + sheet removal --
# "No popups on mobile" was already the rule for the filter panel (R2) and
# the stat-filter popover (R3); the bottom sheet was the one remaining
# exception. It is deleted entirely here, along with the real bug that
# survived every prior synthetic-click check: .ibpl-m-th-info rendered at
# 14x14 against this file's own 44px minimum tap target, so a thumb missed
# it, landed on the th, and sorted the column instead of opening the
# explanation. The replacement is an inline strip pushed into the page flow,
# never an overlay -- see docs on ".ibpl-m-strip" in mobile.css and
# "buildStrip" in mobile.js. The scroll/tap/push-down behaviour itself needs
# a real browser and is verified there, not here.

test_that("the bottom sheet component is deleted entirely, not just unused", {
  js <- read_repo_txt("www", "mobile.js")
  css <- read_repo_txt("www", "mobile.css")

  expect_false(grepl("IBPL_MOBILE_SHEET", js, fixed = TRUE))
  expect_false(grepl("ibpl-m-sheet", js, fixed = TRUE))
  expect_false(grepl("ibpl-m-sheet", css, fixed = TRUE))
  expect_false(grepl("textSheet", js, fixed = TRUE))
})

test_that("the header info-dot's real hit box is the 44px tap minimum, not the visible dot", {
  css <- read_repo_txt("www", "mobile.css")

  # The element getBoundingClientRect() measures (.ibpl-m-th-info itself)
  # must be sized to the shared --ibpl-m-tap minimum...
  start <- regexpr("body.ibpl-mobile .ibpl-m-th-info {", css, fixed = TRUE)
  expect_gt(start, 0)
  rest <- substring(css, start)
  end <- regexpr("\\}", rest)
  expect_gt(end, 0)
  info_rule <- substring(rest, 1, end)

  expect_true(grepl("width: var(--ibpl-m-tap)", info_rule, fixed = TRUE))
  expect_true(grepl("height: var(--ibpl-m-tap)", info_rule, fixed = TRUE))

  # ...while the VISIBLE dot stays a separate, small ::before -- 32 columns
  # do not each get a 44px circle.
  before_start <- regexpr("body.ibpl-mobile .ibpl-m-th-info::before {", css, fixed = TRUE)
  expect_gt(before_start, 0)
  before_rest <- substring(css, before_start)
  before_end <- regexpr("\\}", before_rest)
  before_rule <- substring(before_rest, 1, before_end)

  expect_true(grepl("width: 14px", before_rule, fixed = TRUE))
  expect_true(grepl("height: 14px", before_rule, fixed = TRUE))
  expect_true(grepl("border-radius: 50%", before_rule, fixed = TRUE))
})

test_that("the header and label strips push content down, never float over it", {
  css <- read_repo_txt("www", "mobile.css")

  start <- regexpr("body.ibpl-mobile .ibpl-m-strip {", css, fixed = TRUE)
  expect_gt(start, 0)
  rest <- substring(css, start)
  end <- regexpr("\\}", rest)
  strip_rule <- substring(rest, 1, end)

  # No overlay positioning at all -- this is the entire point of replacing
  # the sheet, which was fixed-positioned with a backdrop.
  expect_false(grepl("position: fixed", strip_rule, fixed = TRUE))
  expect_false(grepl("position: absolute", strip_rule, fixed = TRUE))
  expect_false(grepl("z-index", strip_rule, fixed = TRUE))

  # A dismiss control exists (close button in JS, sized to the tap minimum
  # in CSS) rather than requiring an outside-tap/Escape dismissal pattern
  # that only a floating overlay could offer.
  expect_true(grepl(".ibpl-m-strip-close", css, fixed = TRUE))
})

test_that("the table header strip anchors outside .dataTables_scrollBody", {
  js <- read_repo_txt("www", "mobile.js")

  # scrollX splits the header/body into separate tables; the strip must
  # anchor on .dataTables_scrollHead (or the bare table, for a dom: "t"
  # table with no scroll split) so it never ends up a scrollBody child that
  # would slide out of view when the table scrolls horizontally.
  expect_true(grepl('wrapper.querySelector(".dataTables_scrollHead")', js, fixed = TRUE))
  # Mentioning scrollBody in a comment (explaining what to avoid) is fine;
  # querying or inserting into it is not.
  expect_false(grepl('querySelector(".dataTables_scrollBody")', js, fixed = TRUE))
  expect_false(grepl("dataTables_scrollBody\"); anchor.appendChild", js, fixed = TRUE))
})

test_that("tapping the same header dot again dismisses the strip", {
  js <- read_repo_txt("www", "mobile.js")

  start <- regexpr("function toggleHeaderStrip(th, label, text) {", js, fixed = TRUE)
  expect_gt(start, 0)
  rest <- substring(js, start)
  # Function body ends at the first line that is just a closing brace.
  end <- regexpr("\n\\}", rest)
  expect_gt(end, 0)
  fn_body <- substring(rest, 1, end)

  expect_true(grepl("ibpl-m-strip-open", fn_body, fixed = TRUE))
  expect_true(grepl("closeHeaderStrip(", fn_body, fixed = TRUE))
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
  # left the checkbox unchecked and closed an already-open Filters sheet
  # (the sheet is gone since R6, but the underlying browser-default hazard
  # -- ANY preventDefault() during dispatch suppresses the native toggle --
  # is unchanged). Scope to this handler's own body so this can't pass on
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

  expect_true(grepl("toggleLabelStrip", handler, fixed = TRUE))
  expect_false(grepl("preventDefault", handler, fixed = TRUE))
})

test_that("[data-tooltip] ignores untrusted clicks (R4 regression: synthetic radio.click() bubbles into a tooltip label)", {
  js <- read_repo_txt("www", "mobile.js")

  # R4 makes the burger menu drive a REAL radio via a synthetic
  # radio.click() (app.js updateInput()). Several view-mode choice labels
  # (e.g. onoff_view_mode's "Four Factors") carry their own data-tooltip --
  # tt() puts it on the <label> itself, an ANCESTOR of the <input> -- so that
  # synthetic click bubbles straight into this delegated handler and pops
  # the strip open on every mode switch made from the menu. Verified live
  # with a capture-phase click logger: none of the clicks in that sequence
  # (burger toggle, thm-item, nav-link, radio input) were a real tap on the
  # tooltip label, yet it opened anyway. A real finger tap is always
  # isTrusted; app.js's own programmatic clicks never are.
  #
  # e.originalEvent.isTrusted, not bare e.isTrusted (real bug, found live
  # while building the R6 inline strip): jQuery's Event object does not
  # forward isTrusted onto itself, so e.isTrusted reads back undefined for
  # every click -- real or synthetic -- and a bare check would silently
  # discard every tap, breaking this affordance for real users too.
  start <- regexpr(
    '$(document).on("click", "[data-tooltip]", function (e) {',
    js, fixed = TRUE
  )
  expect_gt(start, 0)
  rest <- substring(js, start)
  end <- regexpr("});", rest, fixed = TRUE)
  expect_gt(end, 0)
  handler <- substring(rest, 1, end + 2)

  expect_true(grepl("e.originalEvent.isTrusted", handler, fixed = TRUE))
  expect_false(grepl("if (!e.isTrusted)", handler, fixed = TRUE))
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

# The mobile gameflow keeps its compact source SVG and derives one viewport
# per quarter from its period bounds. The full timeline remains available.

test_that("a phone gameflow requests the compact ribbon layout", {
  mod <- read_repo_txt("R", "mod_ribbon_modal.R")
  css <- read_repo_txt("www", "mobile.css")

  expect_true(grepl("layout = ribbon_layout(compact = mobile)", mod, fixed = TRUE))
  # The compact chart is sized to its container, never pinned to the
  # desktop's 1070px.
  expect_false(grepl("min-width: 1070px", css, fixed = TRUE))
  expect_true(grepl(".ibpl-ribbon.is-compact {", css, fixed = TRUE))
})

test_that("mobile gameflow overview covers every played period", {
  margin <- data.frame(elapsed = c(0, 600, 1200, 2400, 2700),
                       margin = c(0, 4, -3, 2, 5))
  html <- as.character(ribbon_mobile_overview_ui(margin, ribbon_period_bounds(5)))
  expect_identical(lengths(regmatches(html, gregexpr('class="ibpl-ribbon-quarter-jump"', html))), 5L)
  expect_match(html, 'aria-label="Jump to OT1"', fixed = TRUE)
  expect_match(html, 'class="ibpl-ribbon-overview-line" d="M ', fixed = TRUE)
})

test_that("the gameflow panel never hides its own Shiny output while loading", {
  css <- read_repo_txt("www", "mobile.css")
  js <- read_repo_txt("www", "mobile.js")

  # Shiny suspends an output it sees as hidden and never sends its value:
  # hiding .shiny-html-output during is-loading left the first gameflow on
  # "Loading gameflow..." forever. Only the stale result may be hidden.
  expect_false(grepl("is-loading .shiny-html-output", css, fixed = TRUE))
  expect_true(grepl("is-loading .ibpl-ribbon-inline-result", css, fixed = TRUE))
  # The panel starts [hidden]; Shiny re-checks visibility on "shown".
  expect_true(grepl('jQuery(panel).trigger("shown")', js, fixed = TRUE))
})

test_that("the compact ribbon's tap handling lives in app.js", {
  js <- read_repo_txt("www", "app.js")

  # A tap resolves to the nearest row/stint, not only an exact bar hit.
  expect_true(grepl("function laneAtPoint(svg, clientX, clientY)", js, fixed = TRUE))
  # Focus is decided from selection state AFTER selection. Toggling on
  # is-active cancelled every touch tap (the emulated mouseover had already
  # set it).
  expect_false(grepl('setFocus(svg, lane.classList.contains("is-active") ? null : lane)', js, fixed = TRUE))
  sel <- regexpr("else setSelection(svg, lane);", js, fixed = TRUE)
  foc <- regexpr("setFocus(svg, wasSelected", js, fixed = TRUE)
  expect_gt(sel, 0)
  expect_gt(foc, sel)
  # The compact detail is an HTML card after the SVG, not a foreignObject.
  expect_true(grepl('"ibpl-ribbon-detail is-card"', js, fixed = TRUE))
})
