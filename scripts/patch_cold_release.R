# Carries the possession-end backfills into a copy of the cold-storage release's Parquet files.
#
#   Rscript scripts/patch_cold_release.R <dir> <out>
#
# <dir> holds possessions.parquet and pws.parquet as downloaded from the `cold-storage/latest`
# release. Every exports/cold/backup-*/targets.csv (a backfill's saved targets; a backup folder
# without one is skipped) names rows whose final_end_poss the backfill set to FALSE in the database;
# this flips the same rows in the two files and writes them to <out> with the original Arrow
# schema. The files are then read back and the script exits 1 unless the schema and row order are
# unchanged, only final_end_poss changed, only target rows that were TRUE flipped, and no target
# end remains. It uploads nothing and writes only under <out>: replacing the release is a separate,
# deliberate step.

args <- commandArgs(trailingOnly = TRUE)
stopifnot("usage: Rscript scripts/patch_cold_release.R <dir> <out>" = length(args) == 2L)
src <- args[[1]]; out <- args[[2]]
stopifnot(dir.exists(src), normalizePath(src, mustWork = TRUE) != normalizePath(out, mustWork = FALSE))
suppressPackageStartupMessages(library(arrow))
say <- function(...) cat(sprintf(...), "\n", sep = "")

folders <- sort(Sys.glob("exports/cold/backup-*"))
folders <- folders[file.exists(file.path(folders, "targets.csv"))]
stopifnot("no exports/cold/backup-*/targets.csv found" = length(folders) > 0L)
say("Targets from %d folders:", length(folders))
targets <- do.call(rbind, lapply(folders, function(f) {
  t <- utils::read.csv(file.path(f, "targets.csv"))[c("game_id", "id")]
  say("  %s: %d targets", f, nrow(t))
  t
}))
tkey <- unique(paste(targets$game_id, targets$id))
say("%d distinct target ends", length(tkey))

dir.create(out, recursive = TRUE, showWarnings = FALSE)
failures <- character(0)
for (t in c("possessions", "pws")) {
  f_in <- file.path(src, paste0(t, ".parquet")); f_out <- file.path(out, paste0(t, ".parquet"))
  tbl <- arrow::read_parquet(f_in, as_data_frame = FALSE, mmap = FALSE)
  schema <- tbl$schema
  d <- as.data.frame(tbl)
  stopifnot(all(c("game_id", "id", "final_end_poss") %in% names(d)))
  hit <- paste(d$game_id, d$id) %in% tkey & d$final_end_poss %in% TRUE
  patched <- d$final_end_poss
  patched[hit] <- FALSE
  i <- match("final_end_poss", names(d))
  new_col <- arrow::chunked_array(arrow::Array$create(patched, type = schema$GetFieldByName("final_end_poss")$type))
  arrow::write_parquet(tbl$SetColumn(i - 1L, schema$GetFieldByName("final_end_poss"), new_col), f_out)

  # Read the written file back and check it against the original.
  b <- arrow::read_parquet(f_out, as_data_frame = FALSE, mmap = FALSE)
  bd <- as.data.frame(b)
  chk <- function(ok, msg) if (!isTRUE(ok)) failures <<- c(failures, sprintf("%s: %s", t, msg))
  chk(b$schema$Equals(schema, check_metadata = FALSE), "schema differs")
  chk(identical(nrow(bd), nrow(d)), "row count differs")
  chk(identical(bd[c("game_id", "id")], d[c("game_id", "id")]), "row order differs")
  other <- setdiff(names(d), "final_end_poss")
  chk(identical(bd[other], d[other]), "a column other than final_end_poss changed")
  changed <- which(!mapply(identical, bd$final_end_poss, d$final_end_poss))
  chk(identical(changed, which(hit)), sprintf("%d rows changed, %d expected", length(changed), sum(hit)))
  chk(all(d$final_end_poss[changed] %in% TRUE) && all(!bd$final_end_poss[changed] %in% TRUE), "a changed row was not TRUE -> FALSE")
  chk(!any(paste(bd$game_id, bd$id) %in% tkey & bd$final_end_poss %in% TRUE), "a target end remains")
  say("%s: %d rows, %d ends flipped, %d target keys not present as ends", t, nrow(d), sum(hit),
      sum(!tkey %in% paste(d$game_id, d$id)[d$final_end_poss %in% TRUE]))
}
if (length(failures)) {
  say("PATCH CHECKS FAILED:"); for (m in failures) say("  - %s", m); quit(status = 1L)
}
say("Read-back checks pass. Patched files are in %s; nothing was uploaded.", out)
