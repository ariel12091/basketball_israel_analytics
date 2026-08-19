# Re-vendor the web fonts and icon font into app/www/.
#
# The app used to load these from fonts.googleapis.com and cdn.jsdelivr.net.
# Both were cross-origin, neither carried an integrity hash, and both sat in
# the window before the Shiny websocket connects. Self-hosting removes all
# three problems. Run this only to refresh or change the font set; the vendored
# output is committed.
#
#   "$RSCRIPT" scripts/vendor_web_fonts.R
#
# Inter is intentionally absent: app.css only ever names it as a fallback
# behind DM Sans, so shipping four weights of it bought nothing.

suppressPackageStartupMessages(library(curl))

WWW        <- "app/www"
FONT_DIR   <- file.path(WWW, "fonts")
ICON_DIR   <- file.path(WWW, "bootstrap-icons")
GOOGLE_CSS <- paste0(
  "https://fonts.googleapis.com/css2",
  "?family=DM+Sans:wght@400;500;600;700",
  "&family=JetBrains+Mono:wght@400;500",
  "&display=swap"
)
ICON_BASE <- "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/"

# Google serves woff2 only to a browser-like UA; an R default UA gets ttf.
fetch <- function(url, binary = FALSE) {
  h <- new_handle()
  handle_setheaders(h, `User-Agent` = paste(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  ))
  res <- curl_fetch_memory(url, handle = h)
  if (res$status_code != 200) stop("HTTP ", res$status_code, " for ", url)
  if (binary) res$content else rawToChar(res$content)
}

# writeLines() opens a text connection on Windows and rewrites every newline
# as CRLF, which makes this script non-reproducible and seeds CRLF into
# generated CSS. Write bytes instead.
write_text_lf <- function(lines, path) {
  con <- file(path, open = "wb")
  on.exit(close(con))
  writeLines(lines, con, sep = "\n")
}

dir.create(FONT_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(ICON_DIR, "fonts"), recursive = TRUE, showWarnings = FALSE)

# ---- Google fonts ----------------------------------------------------------
# Each @font-face is preceded by a /* subset */ comment. Every subset is kept:
# unicode-range means the browser downloads only the ones a page needs, so
# extra subsets cost nothing at runtime but cover non-Latin names.
css    <- fetch(GOOGLE_CSS)
blocks <- regmatches(css, gregexpr("/\\*\\s*[a-z0-9-]+\\s*\\*/\\s*@font-face\\s*\\{[^}]*\\}", css))[[1]]
if (!length(blocks)) stop("No @font-face blocks parsed - did the Google CSS format change?")

one_match <- function(x, re) sub(re, "\\1", regmatches(x, regexpr(re, x)))

rewritten <- vapply(blocks, function(block) {
  subset <- one_match(block, "/\\*\\s*([a-z0-9-]+)\\s*\\*/")
  family <- one_match(block, "font-family:\\s*'([^']+)'")
  weight <- one_match(block, "font-weight:\\s*([0-9]+)")
  url    <- one_match(block, "url\\((https://[^)]+)\\)")
  name   <- sprintf("%s-%s-%s.woff2", tolower(gsub(" ", "-", family)), weight, subset)
  writeBin(fetch(url, binary = TRUE), file.path(FONT_DIR, name))
  message(sprintf("  %-42s", name))
  sub(url, paste0("fonts/", name), block, fixed = TRUE)
}, character(1))

write_text_lf(c(
  sprintf("/* Vendored from Google Fonts on %s by scripts/vendor_web_fonts.R.", Sys.Date()),
  "   Do not edit by hand -- re-run the script instead. */",
  "",
  rewritten
), file.path(WWW, "fonts.css"))

# ---- bootstrap-icons -------------------------------------------------------
# The stylesheet is kept whole rather than subset to the icons in use today, so
# adding an icon later does not mean re-vendoring.
icon_css <- fetch(paste0(ICON_BASE, "bootstrap-icons.min.css"))
refs <- unique(regmatches(icon_css, gregexpr("fonts/bootstrap-icons\\.woff2?", icon_css))[[1]])
for (rel in refs) {
  writeBin(fetch(paste0(ICON_BASE, rel), binary = TRUE), file.path(ICON_DIR, rel))
  message(sprintf("  %-42s", rel))
}
write_text_lf(icon_css, file.path(ICON_DIR, "bootstrap-icons.min.css"))

message("Done. shared_head_tags() in app/R/global.R links these; nothing else should reference a font CDN.")
