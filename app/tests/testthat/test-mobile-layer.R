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

test_that("the table layer keeps a caret, not a row tap", {
  js <- read_repo_txt("www", "mobile.js")

  # Compare and Tab 2 already bind tbody tr clicks for their own modals.
  expect_true(grepl("ibpl-m-caret", js, fixed = TRUE))
  expect_false(grepl('on("click", "table.dataTable > tbody > tr"', js, fixed = TRUE))
})

test_that("the table layer re-applies on every draw and guards re-entry", {
  js <- read_repo_txt("www", "mobile.js")

  # DT re-renders every cell on sort, page and filter, so per-cell state is
  # gone by the next draw.
  expect_true(grepl("draw.dt", js, fixed = TRUE))
  # column().visible() triggers a redraw, which fires draw.dt, which recurses.
  expect_true(grepl("applying", js, fixed = TRUE))
  # Hiding columns desyncs the header from the body without this.
  expect_true(grepl("columns.adjust()", js, fixed = TRUE))
})

test_that("priority overrides are keyed by column name, not index", {
  js <- read_repo_txt("www", "mobile.js")

  # One DT output id serves several view modes with different column sets, so
  # an index-keyed override would corrupt the other modes.
  expect_true(grepl("IBPL_MOBILE_TABLE", js, fixed = TRUE))
  expect_true(grepl("indexOf", js, fixed = TRUE))
  expect_true(grepl("render(\"display\")", js, fixed = TRUE))
})

test_that("the navbar collapses into a burger", {
  app_r <- read_repo_txt("app.R")

  # Default is FALSE, so 7 tabs wrap or overflow on a phone without this.
  expect_true(grepl("collapsible = TRUE", app_r, fixed = TRUE))
})

test_that("mobile drives the real view-mode radios, not the hover menu", {
  js <- read_repo_txt("www", "mobile.js")
  css <- read_repo_txt("www", "mobile.css")
  app_css <- read_repo_txt("www", "app.css")

  # Desktop deliberately hides these live Shiny inputs. Moving them without
  # overriding that rule would leave every mobile mode control invisible.
  expect_true(grepl(".view-mode-container {", app_css, fixed = TRUE))
  expect_true(grepl("display: none !important", app_css, fixed = TRUE))
  expect_true(grepl(".view-mode-container", js, fixed = TRUE))
  expect_true(grepl(
    "body.ibpl-mobile .ibpl-m-viewmode .view-mode-container { display: block !important; }",
    css, fixed = TRUE
  ))
  expect_true(grepl("tab-hover-menu", css, fixed = TRUE))
})

test_that("Player Stats moves its hidden select into the mobile mode control", {
  ui <- read_repo_txt("R", "ui_tab5_traditional.R")
  js <- read_repo_txt("www", "mobile.js")
  css <- read_repo_txt("www", "mobile.css")

  expect_true(grepl('"ts_display_mode"', ui, fixed = TRUE))
  expect_true(grepl('querySelector("#ts_display_mode")', js, fixed = TRUE))
  expect_true(grepl('select.closest(".shiny-input-container")', js, fixed = TRUE))
  expect_true(grepl(".ibpl-m-viewmode select", css, fixed = TRUE))
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

test_that("the filter sheet is generic over all 11 tabs", {
  js <- read_repo_txt("www", "mobile.js")

  # Matching the shared toggle shape means no per-tab R edit.
  expect_true(grepl('data-bs-target$=', js, fixed = TRUE))
  expect_true(grepl("-filters", js, fixed = TRUE))
  # Bootstrap keeps owning show/hide so the button, aria-expanded and the
  # chips-bar wiring all keep working untouched.
  expect_false(grepl("classList.remove(\"collapse\")", js, fixed = TRUE))
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

test_that("the info-dot and filter-chip-add taps are intercepted in the capture phase", {
  js <- read_repo_txt("www", "mobile.js")

  # DataTables' sort listener is bound directly on <th>, and Bootstrap's
  # popover-toggle listener directly on the trigger -- both closer to the
  # target than a bubble-phase document handler, so stopPropagation() there
  # always runs too late. Verified live (Task 6 fix round 1): a bubble-phase
  # $(document).on() let an info-dot tap also re-sort the column, and let
  # closing the sheet leave a real floating Bootstrap popover on screen.
  # Scope to THIS listener (identified by the .closest(".ibpl-m-th-info")
  # call, unique to it) so the assertion can't be satisfied by the caret's
  # own, separate capture-phase listener earlier in the file (mobile.js's
  # "Mobile table layer" section).
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
  block <- substring(rest, 1, end + nchar("}, true);") - 1)

  # Confirms this is the SAME listener handling both selectors together, not
  # just any capture-phase block.
  expect_true(grepl(".filter-chip-add", block, fixed = TRUE))
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
