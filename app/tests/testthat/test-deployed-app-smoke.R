library(testthat)

skip_if_not(Sys.getenv("RUN_DEPLOYED_SMOKE", "0") == "1")
skip_if_not_installed("chromote")

app_url <- Sys.getenv("APP_URL", unset = "")
if (!nzchar(app_url)) skip("APP_URL is not set")

eval_js <- function(page, js) {
  out <- page$Runtime$evaluate(js, returnByValue = TRUE, awaitPromise = TRUE)
  out$result$value
}

wait_for_js <- function(page, js, timeout_sec = 60, label = js) {
  deadline <- Sys.time() + timeout_sec
  last <- NULL
  repeat {
    last <- tryCatch(eval_js(page, js), error = function(e) structure(FALSE, error = conditionMessage(e)))
    ok <- if (is.list(last) && !is.null(last$ok)) isTRUE(last$ok) else isTRUE(last)
    if (ok) return(last)
    if (Sys.time() >= deadline) break
    Sys.sleep(1)
  }
  fail(paste("Timed out waiting for", label, "last:", paste(capture.output(str(last)), collapse = " ")))
}

click_tab <- function(page, tab_value) {
  js <- sprintf(
    "(function() {
       var el = document.querySelector('[data-value=\"%s\"]');
       if (!el) return false;
       el.click();
       return true;
     })()",
    tab_value
  )
  wait_for_js(page, js, timeout_sec = 30, label = paste("tab", tab_value))
}

set_select <- function(page, input_id, value) {
  js <- sprintf(
    "(function() {
       var el = document.getElementById('%s');
       if (!el) return true;
       el.value = '%s';
       el.dispatchEvent(new Event('change', { bubbles: true }));
       if (window.jQuery) window.jQuery(el).trigger('change');
       return true;
     })()",
    input_id,
    value
  )
  wait_for_js(page, js, timeout_sec = 30, label = paste(input_id, value))
}

wait_for_table_data <- function(page, output_id) {
  js <- sprintf(
    "(function() {
       var out = document.getElementById('%s');
       if (!out) return { ok: false, reason: 'missing output' };
       var text = out.innerText || '';
       var rows = out.querySelectorAll('tbody tr').length;
       var bad = /render error|no data for current filters|no rows match stat filters|no data available in table/i.test(text);
       return { ok: rows > 0 && !bad, rows: rows, text: text.slice(0, 500) };
     })()",
    output_id
  )
  wait_for_js(page, js, timeout_sec = 90, label = paste("data in", output_id))
}

test_that("deployed app shows data in primary tab tables", {
  page <- chromote::ChromoteSession$new(width = 1440, height = 1000)
  on.exit(page$close(), add = TRUE)

  page$Page$navigate(app_url)
  page$Page$loadEventFired(wait_ = TRUE)
  wait_for_js(page, "document.body && document.body.innerText.length > 0", timeout_sec = 60, label = "app body")

  checks <- list(
    list(tab = "onoff", output = "onoff_dt"),
    list(tab = "lineup_data", output = "ld_table"),
    list(tab = "team_ratings", output = "tr_table", input = "tr_view_mode", value = "Four Factors"),
    list(tab = "team_ratings", output = "tr_table", input = "tr_view_mode", value = "Traditional"),
    list(tab = "game_logs", output = "gl_table"),
    list(tab = "traditional_stats", output = "ts_table"),
    list(tab = "team_stats", output = "tst_table")
  )

  for (check in checks) {
    click_tab(page, check$tab)
    if (!is.null(check$input)) set_select(page, check$input, check$value)
    wait_for_table_data(page, check$output)
  }
})
