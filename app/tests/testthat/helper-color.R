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

# Strip CSS comments before scanning for raw hex. A comment documenting a
# measured contrast ratio (e.g. "composited over --ibpl-bg the own bar is
# #C88E2F (contrast 6.64:1)") is prose, not a declaration, and must not trip
# the raw-hex guard. `.*?` (non-greedy) with the `(?s)` DOTALL flag matches
# each comment up to its OWN `*/` -- a greedy `/\*.*\*/` would instead run to
# the LAST `*/` in the file, silently deleting any real declarations sitting
# between two comments and blinding the guard to violations there.
strip_css_comments <- function(css_text) {
  gsub("(?s)/\\*.*?\\*/", "", css_text, perl = TRUE)
}

# Raw hex codes appearing outside the :root token block, after stripping
# comments. Shared by the real app.css check and by the counter-test that
# proves the comment-stripping still catches a genuine violation.
hex_outside_root <- function(css_text) {
  clean <- strip_css_comments(css_text)
  root <- css_root_block(clean)
  outside <- sub(root, "", clean, fixed = TRUE)
  sort(unique(regmatches(outside, gregexpr("#[0-9a-fA-F]{6}", outside))[[1]]))
}
