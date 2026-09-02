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
