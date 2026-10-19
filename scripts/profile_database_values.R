readRenviron("etl/.Renviron")
suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE")
)
on.exit(dbDisconnect(con), add = TRUE)
invisible(dbExecute(con, "SET statement_timeout = '120s'"))

requested <- commandArgs(trailingOnly = TRUE)
low_card_all <- identical(requested, "--low-card-all")
r_summary <- length(requested) > 1 && identical(requested[[1]], "--r-summary")
if (r_summary) requested <- requested[-1]

catalog <- dbGetQuery(
  con,
  "SELECT c.relname AS relation,
          c.relkind,
          c.reltuples::bigint AS estimated_rows,
          a.attnum AS ordinal,
          a.attname AS column_name,
          pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
          t.typcategory
     FROM pg_attribute a
     JOIN pg_class c ON c.oid = a.attrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
     JOIN pg_type t ON t.oid = a.atttypid
    WHERE n.nspname = 'basketball_test'
      AND c.relkind IN ('r', 'm', 'v', 'p')
      AND c.relname NOT LIKE '__stage_%'
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY c.relname, a.attnum"
)

stats <- dbGetQuery(
  con,
  "SELECT tablename AS relation,
          attname AS column_name,
          null_frac,
          n_distinct,
          left(to_json(most_common_vals)::text, 300) AS common_values,
          to_json(most_common_freqs)::text AS common_freqs,
          to_json(histogram_bounds)::text AS histogram_bounds
     FROM pg_stats
    WHERE schemaname = 'basketball_test'"
)

if (identical(requested, "--stats-coverage")) {
  coverage <- do.call(
    rbind,
    lapply(
      split(catalog, catalog$relation),
      function(relation_catalog) data.frame(
        relation = relation_catalog$relation[[1]],
        relkind = relation_catalog$relkind[[1]],
        estimated_rows = relation_catalog$estimated_rows[[1]],
        catalog_columns = nrow(relation_catalog),
        stringsAsFactors = FALSE
      )
    )
  )
  stats_counts <- table(stats$relation)
  coverage$stats_columns <- as.integer(stats_counts[coverage$relation])
  coverage$stats_columns[is.na(coverage$stats_columns)] <- 0L
  coverage <- coverage[order(coverage$relation), ]
  write.table(coverage, row.names = FALSE, quote = FALSE, sep = " | ")
  quit(save = "no", status = 0)
}

if (!length(requested)) stop("Pass --stats-coverage or relation names.")

if (low_card_all) {
  requested <- unique(catalog$relation)
}

quote_relation <- function(relation_name) {
  paste0(
    as.character(dbQuoteIdentifier(con, "basketball_test")),
    ".",
    as.character(dbQuoteIdentifier(con, relation_name))
  )
}

quote_column <- function(column_name) {
  as.character(dbQuoteIdentifier(con, column_name))
}

estimate_distinct <- function(n_distinct, estimated_rows) {
  if (is.na(n_distinct)) return(NA_real_)
  if (n_distinct >= 0) return(n_distinct)
  if (is.na(estimated_rows) || estimated_rows < 0) return(NA_real_)
  -n_distinct * estimated_rows
}

format_value_counts <- function(values) {
  if (!nrow(values)) return("<no rows>")
  paste(
    sprintf(
      "%s (%s)",
      ifelse(is.na(values$value), "NULL", values$value),
      format(values$n, scientific = FALSE, trim = TRUE)
    ),
    collapse = "; "
  )
}

for (relation_name in requested) {
  cols <- catalog[catalog$relation == relation_name, , drop = FALSE]
  if (!nrow(cols)) {
    cat(sprintf("\n### %s\n<missing relation>\n", relation_name))
    next
  }

  rel_stats <- merge(
    cols,
    stats[stats$relation == relation_name, , drop = FALSE],
    by = c("relation", "column_name"),
    all.x = TRUE,
    sort = FALSE
  )
  rel_stats <- rel_stats[match(cols$column_name, rel_stats$column_name), ]
  estimated_rows <- as.numeric(cols$estimated_rows[[1]])
  relkind <- cols$relkind[[1]]
  exact_small <- identical(relkind, "v") || is.na(estimated_rows) || estimated_rows < 0 || estimated_rows <= 5000
  exact_rows <- NA_real_

  if (r_summary) {
    if (!exact_small) {
      stop(sprintf(
        "--r-summary only reads relations estimated at 5,000 rows or fewer; %s is estimated at %s rows",
        relation_name,
        format(estimated_rows, scientific = FALSE, trim = TRUE)
      ))
    }
    relation_data <- dbGetQuery(con, sprintf("SELECT * FROM %s", quote_relation(relation_name)))
    cat(sprintf("\n### R summary(): %s | rows=%d\n", relation_name, nrow(relation_data)))
    print(summary(relation_data))
    next
  }

  if (exact_small) {
    qrel <- quote_relation(relation_name)
    aggregates <- c("COUNT(*)::numeric AS row_count")
    for (i in seq_len(nrow(cols))) {
      qcol <- quote_column(cols$column_name[[i]])
      aggregates <- c(
        aggregates,
        sprintf("COUNT(%s)::numeric AS nn_%d", qcol, i),
        sprintf("COUNT(DISTINCT %s::text)::numeric AS nd_%d", qcol, i)
      )
    }
    exact <- dbGetQuery(con, sprintf("SELECT %s FROM %s", paste(aggregates, collapse = ", "), qrel))
    exact_rows <- as.numeric(exact$row_count[[1]])
    for (i in seq_len(nrow(cols))) {
      rel_stats$null_frac[[i]] <- if (exact_rows > 0) {
        1 - as.numeric(exact[[paste0("nn_", i)]][[1]]) / exact_rows
      } else {
        NA_real_
      }
      rel_stats$n_distinct[[i]] <- as.numeric(exact[[paste0("nd_", i)]][[1]])
    }
    estimated_rows <- exact_rows
  }

  rel_stats$distinct_est <- mapply(
    estimate_distinct,
    rel_stats$n_distinct,
    MoreArgs = list(estimated_rows = estimated_rows)
  )

  range_min <- rep(NA_character_, nrow(cols))
  range_max <- rep(NA_character_, nrow(cols))
  range_indices <- which(cols$typcategory %in% c("N", "D") | grepl("\\[\\]$", cols$data_type))
  if (length(range_indices) && (estimated_rows != 0 || exact_small)) {
    range_expr <- character()
    for (i in range_indices) {
      qcol <- quote_column(cols$column_name[[i]])
      if (grepl("\\[\\]$", cols$data_type[[i]])) {
        range_expr <- c(
          range_expr,
          sprintf("MIN(cardinality(%s))::text AS min_%d", qcol, i),
          sprintf("MAX(cardinality(%s))::text AS max_%d", qcol, i)
        )
      } else {
        range_expr <- c(
          range_expr,
          sprintf("MIN(%s)::text AS min_%d", qcol, i),
          sprintf("MAX(%s)::text AS max_%d", qcol, i)
        )
      }
    }
    ranges <- dbGetQuery(
      con,
      sprintf("SELECT %s FROM %s", paste(range_expr, collapse = ", "), quote_relation(relation_name))
    )
    for (i in range_indices) {
      range_min[[i]] <- as.character(ranges[[paste0("min_", i)]][[1]])
      range_max[[i]] <- as.character(ranges[[paste0("max_", i)]][[1]])
    }
  }

  low_indices <- which(
    (!is.na(rel_stats$distinct_est) & rel_stats$distinct_est <= 12) |
      cols$data_type == "boolean"
  )
  exact_values <- setNames(rep(NA_character_, nrow(cols)), cols$column_name)
  if (length(low_indices)) {
    values_sql <- paste(
      vapply(
        low_indices,
        function(i) sprintf(
          "(%s, t.%s::text)",
          as.character(dbQuoteString(con, cols$column_name[[i]])),
          quote_column(cols$column_name[[i]])
        ),
        character(1)
      ),
      collapse = ", "
    )
    low_values <- dbGetQuery(
      con,
      sprintf(
        "SELECT v.column_name, v.value, COUNT(*)::numeric AS n
           FROM %s t
           CROSS JOIN LATERAL (VALUES %s) v(column_name, value)
          GROUP BY v.column_name, v.value
          ORDER BY v.column_name, v.value NULLS FIRST",
        quote_relation(relation_name),
        values_sql
      )
    )
    for (i in low_indices) {
      value_rows <- low_values[low_values$column_name == cols$column_name[[i]], , drop = FALSE]
      if (sum(!is.na(value_rows$value)) <= 10) {
        exact_values[[cols$column_name[[i]]]] <- format_value_counts(value_rows)
        rel_stats$distinct_est[[i]] <- sum(!is.na(value_rows$value))
      }
    }
  }

  if (low_card_all) {
    cat(sprintf("\n### %s | rows=%s%s\n", relation_name,
                format(estimated_rows, scientific = FALSE, trim = TRUE),
                if (exact_small) " exact" else " estimated"))
    if (isTRUE(estimated_rows == 0)) {
      cat(sprintf("<all %d columns have no live rows>\n", nrow(cols)))
      next
    }
    for (i in seq_len(nrow(cols))) {
      exact_domain <- exact_values[[cols$column_name[[i]]]]
      if (!is.na(exact_domain) && nzchar(exact_domain)) {
        cat(sprintf("%s | %s\n", cols$column_name[[i]], exact_domain))
      }
    }
    next
  }

  cat(sprintf(
    "\n### %s | kind=%s | rows=%s%s\n",
    relation_name,
    relkind,
    format(estimated_rows, scientific = FALSE, trim = TRUE),
    if (exact_small) " exact" else " estimated"
  ))
  cat("column | type | null_pct | distinct | exact_values_if_10_or_less | min_or_array_len | max_or_array_len | common_values_sample\n")
  for (i in seq_len(nrow(cols))) {
    null_pct <- if (is.na(rel_stats$null_frac[[i]])) "NA" else sprintf("%.2f", 100 * rel_stats$null_frac[[i]])
    distinct_value <- if (is.na(rel_stats$distinct_est[[i]])) "NA" else format(round(rel_stats$distinct_est[[i]]), scientific = FALSE, trim = TRUE)
    exact_domain <- exact_values[[cols$column_name[[i]]]]
    if (is.na(exact_domain)) exact_domain <- ""
    common <- rel_stats$common_values[[i]]
    if (is.na(common) || identical(common, "null")) common <- ""
    cat(paste(
      cols$column_name[[i]],
      cols$data_type[[i]],
      null_pct,
      distinct_value,
      exact_domain,
      ifelse(is.na(range_min[[i]]), "", range_min[[i]]),
      ifelse(is.na(range_max[[i]]), "", range_max[[i]]),
      common,
      sep = " | "
    ), "\n", sep = "")
  }
}
