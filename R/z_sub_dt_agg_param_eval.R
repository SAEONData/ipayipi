#' @title 'dt' processing pipeline: genearte aggregation criteria.
#' @description Used in the processing pipeline to generate aggregation criteria.
#' @param sfc List of file paths to the temporary station file directory. Generated using `ipayipi::open_sf_con()`.
#' @param full_eval Logical indicating whether to perform a full evaluation or not. A partial evaluation only produces a string single string for a summary of the function's parameters. Defaults to `FALSE`.
#' @param f_params For the partial evaluation multiple phenomena can be described using `ipayipi::agg_params()`. If left blank defaults will be used on all phenomena harvested.
#' @param ppsij Pipe-process summary table (stage 'i', step 'j') for the function. If provided this data will be used to overwrite similar arguments provided in the function. This must be provided for the full function evaluation.
#' @param agg_offset String describing period of offset from the rounded time interval used to aggregate data. For example, if aggregating rainfall data from five minute to daily records, but staggered so that daily rainfall is totalled from 8 am to 8 pm the next day the `agg_offset` must be set to `"8 hours"`.  When `full_eval == TRUE` the function overwrites this parameter in favor of `agg_offset` extracted from the partial evaluation.
#' @param agg_parameters Used to set custom aggregation parameters via `ipayipi::aggs()`.
#' @param all_phens Defaults to `TRUE` whereby all phenomena in the working data table of the pipeline stage-step will be aggregated. If `FALSE` then only the 'phen_names' listed in `agg_parameters` will be aggregated.
#'  _Note_:
#'   - phenomena with variable types (`var_type`) character (`chr`) and factor (`fac`) are not processed by this function.
#'   - phenomena names (`phen_name`) ending with '_sn' (denoting a sensor number/identifier) are not aggregated in this function.
#' @param agg_intervals Vector of time intervals (character strings) used to aggregate data.
#' @param agg_dt_suffix Suffix to use when naming aggregated data output tables. The naming convention will take this format for agg interval 'i': 'dt_' \& `agg_interval`[i] \& 'agg_dt_suffix'.
#' @param ignore_nas If `NA` values are ignores any periods or time interval of the data with any NA values will result in an `NA` aggregation value. _This does not apply for non-default functions supplied by the partial evaluation_.
#' @param gap_tbl Regex string used to match tables for which to extract gap data from. Defatuls to "raw_".
#' @param gap_phens  Logical or vector of phenomena/string names. If TRUE then the station 'gaps', i.e. the 'gap' table will be joined to the aggregated table. If a vector of string/s is supplied then these are used to filter the gap table before joining this to aggregated data.
#' @author Paul J. Gordijn
#' @details
#'  Default aggregation functions based on measure, variable types, and the unit of measurement. Function info is stored in the package data, "sts_agg_functions" --- _see examples_. Note that columns/fields with character of factor variable types are not processed by this function.
#' 
#' By default aggregation will be done using the finest temporal resoltution data available (where multiple tables were harvested).
#'
#'
#' @md
#' @examples
#' # load the table from the function data folder to see default aggregation
#' # functions
#' ipayipi::sts_agg_functions
#'
#' @export
agg_param_eval <- function(
  sfc = NULL,
  full_eval = FALSE,
  f_params = NULL,
  ppsij = NULL,
  agg_offset = "0 secs",
  all_phens = TRUE,
  agg_intervals = NULL,
  agg_dt_suffix = "_agg",
  agg_parameters = NULL,
  gaps = TRUE,
  ignore_nas = FALSE,
  ...
) {
  "%ilike%" <- "agg_f" <- "measure" <- "phen_name" <- "stage" <-
    "phen_out_name" <- ":=" <- "table_name" <- "ppsid" <-
    "diff_secs" <- "var_type" <- NULL
  # agg_offset = c("0 secs", "0 secs")
  # all_phens = TRUE
  # ignore_nas = FALSE
  # agg_intervals = NULL # list of time intervals used to aggregate data
  # agg_dt_suffix = "_agg" # suffix to use when naming the ourput agg tables
  # agg_parameters = NULL # argument genereated using `agg_params` nested within `agg()`
  # gaps = TRUE 

  d_args <- list(agg_intervals = NULL, agg_dt_suffix = "_agg",
    ignore_nas = FALSE, all_phens = TRUE, agg_offset = "0 secs",
    agg_parameters = NULL, gaps = TRUE
  )

  # table to be passed as variable for default aggregation functions
  ftbl <- ipayipi::sts_agg_functions
  # full and partial eval
  if (!full_eval) { # partial evaluation
    # basic argument checks
    a_args <- list(agg_intervals = agg_intervals,
      agg_dt_suffix = agg_dt_suffix, ignore_nas = ignore_nas,
      all_phens = all_phens, agg_offset = agg_offset,
      agg_parameters = agg_parameters, gaps = gaps
    )
    x <- lapply(seq_along(a_args), function(i) {
      if (all(sapply(a_args[[i]], function(x) x %in% d_args[[i]]))) {
        return(NULL)
      } else {
        return(a_args[[i]])
      }
    })
    names(x) <- names(a_args)
    x <- x[!sapply(x, is.null)]

    # standardise agg period names
    if ("agg_intervals" %in% names(x)) {
      x$agg_intervals <- sapply(x$agg_intervals, function(x) {
        gsub(" ", "_", sts_interval_name(x)[["sts_intv"]])
      })
      names(x$agg_intervals) <- NULL
    }

    v_names <- names(x)
    dt_parse <- sapply(seq_along(v_names), function(i) {
      xp <- x[[v_names[i]]]
      if (v_names[i] %in% "agg_parameters") {
        xp <- lapply(names(x[[v_names[i]]]), function(d) {
          dvn <- gsub(" ", "", deparse1(x[[v_names[i]]][[d]]))
          dvn <- gsub(",class=c(\"agg_params\",\"list\"))", "", dvn,
            fixed = TRUE
          )
          dvn <- gsub("structure(list", "", dvn, fixed = TRUE)
          dvn <- paste0("aggs(", d, "=.", dvn, ")")
          return(dvn)
        })
      } else {
        xp <- paste0(v_names[i], "=", deparse1(xp), collapse = "")
        xp <- gsub(" ", "", xp)
        xp <- paste("agg_options(", xp, ")", sep = "")
      }
      return(xp)
    })
    dt_parse <- unlist(dt_parse, recursive = FALSE)
    class(dt_parse) <- c(class(dt_parse), "dt_agg_params")
  } else { # full evaluation --------------------------------------------------
    # eval text arguments from partial evaluation
    f_params <- ppsij$f_params
    agg_parameters <- f_params[
      sapply(f_params, function(x) grepl("^aggs\\(", x, fixed = FALSE))
    ]
    agg_parameters <- lapply(agg_parameters, function(x) {
      x <- gsub("^aggs\\(|\\)\\)$", "", x)
      x <- sub(".\\(", "list\\(", x)
      x <- paste0(x, ")")
    })
    agg_parameters <- paste(agg_parameters, collapse = ",", sep = "")
    agg_parameters <- paste0("list(", agg_parameters, ")")
    agg_parameters <- eval(parse(text = agg_parameters))
    xd <- data.table::rbindlist(agg_parameters)
    if (nrow(xd) > 0) {
      xd$phen_name <- names(agg_parameters)
    }

    f_params <- f_params[
      !sapply(f_params, function(x) grepl("^aggs\\(", x, fixed = FALSE))
    ]
    f_params <- lapply(
      f_params, function(x) sub("agg_options\\(", "list\\(", x)
    )
    f_params <- lapply(f_params, function(x) eval(parse(text = x)))
    f_params <- unlist(f_params, recursive = FALSE)

    # read in arguments from partial evaluation
    for (i in seq_along(f_params)) assign(names(f_params[i]), f_params[[i]])

    # read phens_dt
    phens_dt <- sf_dta_read(sfc = sfc, tv = "phens_dt")[["phens_dt"]]

    # yet to implement --- filter phens
    if (!all_phens && nrow(xd) > 0) {
      phens_dt <- phens_dt[phen_name %in% xd$phen_name]
    }

    # generate default agg_info table based on phens_dt
    phens_dt <- phens_dt[
      !is.na(measure) | !is.null(measure) # require measure definition
    ][ppsid %ilike% paste0(ppsij$dt_n[1], "_") # must be same dt stage
    ][table_name %ilike% unique(ppsij$input_dt) # must be same input tbl
    ][!var_type %in% c("chr", "fac") # not aggr character/factors
    ][!phen_name %ilike% ".*_sn$"] # not aggr serial numbers
    phens_dt$stage <- as.integer(substr(phens_dt$ppsid, 1,
      unlist(gregexpr("_", phens_dt$ppsid))[1] - 1
    ))
    phens_dt <- phens_dt[stage == ppsij$dt_n[1]][, -c("stage"), with = FALSE]
    # get agg functions
    p <- merge(x = phens_dt, y = ftbl, by = "measure", all.x = TRUE)
    # check phen_dt for info and generate functions accordingly
    # generate f_param list component 'agg_info'
    p$agg_f <- data.table::fifelse(!is.na(p$units), p$f_continuous,
      rep(NA_character_, nrow(p))
    )
    p$agg_f <- data.table::fifelse(p$units %in% c("deg", "degrees", "degree"),
      p$f_circular, p$agg_f
    )
    p$agg_f <- data.table::fifelse(p$measure %ilike% "logical",
      p$f_logical, p$agg_f
    )
    p$units <- data.table::fifelse(
      p$agg_f %ilike% c("circular") & p$units == "deg", "degrees", p$units
    )
    p[agg_f %ilike% c("circular")]$agg_f <- sub(
      pattern = "<>", replacement = paste0(
        "x, u = \"", p[agg_f %ilike% c("circular")]$units, "\"",
        ", ignore_nas = ", ignore_nas
      )[1], x = p[agg_f %ilike% c("circular")]$agg_f
    )
    p[!agg_f %ilike% c("circular")]$agg_f <- sub(
      pattern = "<>", replacement = paste0("x, na.rm = ", ignore_nas)[1],
      x = p[!agg_f %ilike% c("circular")]$agg_f
    )

    # update p based on agg_info
    agg_details <- data.table::data.table(ppsid = NA_character_,
      phid = NA_integer_, phen_name = NA_character_, phen_out_name =
        NA_character_, agg_f = NA_character_, units = NA_character_, measure =
        NA_character_, var_type = NA_character_, ignore_nas = ignore_nas,
      record_interval_type = NA_character_, orig_record_interval =
        NA_character_, dt_record_interval = NA_character_,
      orig_table_name = NA_character_, table_name = NA_character_
    )[0, ]
    xd <- rbind(xd, agg_details, fill = TRUE)
    names(xd)  <- paste0("c_", names(xd))
    p <- p[, names(p)[names(p) %in% names(agg_details)], with = FALSE]
    p$ignore_nas <- ignore_nas
    p <- rbind(p, agg_details, fill = TRUE)
    # update defaults in p per user input in xd
    p <- merge(x = p, y = xd, by.x = "phen_name", by.y = "c_phen_name",
      all.x = TRUE
    )
    p$phen_out_name <- data.table::fifelse(is.na(p$phen_out_name),
      p$phen_name, p$phen_out_name
    )
    p$phen_out_name <- data.table::fifelse(!is.na(p$c_phen_out_name),
      p$c_phen_out_name, p$phen_out_name
    )
    p$agg_f <- data.table::fifelse(!is.na(p$c_agg_f), p$c_agg_f, p$agg_f)
    p$units <- data.table::fifelse(!is.na(p$c_units), p$c_units, p$units)
    p$measure <- data.table::fifelse(!is.na(p$c_measure), p$c_measure,
      p$measure
    )
    p$var_type <- data.table::fifelse(!is.na(p$c_var_type), p$c_var_type,
      p$var_type
    )
    p <- p[, names(p)[!sapply(names(p), function(x) substr(x, 1, 2) == "c_")],
      with = FALSE
    ]
    p$ppsid <- rep(paste0(ppsij$dt_n[1], "_", ppsij$dtp_n[1]), nrow(p))
    data.table::setcolorder(p, neworder = names(agg_details))
    p$orig_record_interval <- gsub(" ", "_", p$orig_record_interval)
    p <- unique(p) # p now has agg parameters in tabular format

    # update phens_dt with new phen_out_name phens
    nn <- c("orig_record_interval_secs", "dt_record_interval_secs",
      "diff_secs"
    )
    phen_dt_new <- p[, c(names(phens_dt)[!names(phens_dt) %in% nn],
        "phen_out_name"
      ), with = FALSE
    ]
    phen_dt_new <- phen_dt_new[, phen_name := phen_out_name][
      , -c("phen_out_name"), with = FALSE
    ]
    phen_dt_new$ppsid <- rep(paste0(ppsij$dt_n[1], "_", ppsij$dtp_n[1]),
      nrow(phen_dt_new)
    )

    # multiply new phen dt per agg intervals
    rps <- lapply(agg_intervals, function(x) {
      nphen_dt <- phen_dt_new
      nphen_dt$dt_record_interval <- x
      np <- p
      np$dt_record_interval <- x
      ris <- sts_interval_name(x)[["dfft_secs"]]
      nphen_dt$orig_record_interval_secs <-
        sapply(nphen_dt$orig_record_interval, function(x) {
          sts_interval_name(x)[["dfft_secs"]]
        })
      nphen_dt$dt_record_interval_secs <-
        sapply(nphen_dt$dt_record_interval, function(x) {
          sts_interval_name(x)[["dfft_secs"]]
        })
      nphen_dt$orig_record_interval_secs <- data.table::fifelse(
        is.na(nphen_dt$orig_record_interval_secs) |
          nphen_dt$orig_record_interval %in% "discnt",
        0, nphen_dt$orig_record_interval_secs
      )
      nphen_dt$diff_secs <- nphen_dt$dt_record_interval_secs -
        nphen_dt$orig_record_interval_secs
      nphen_dt_split <- split.data.frame(
        nphen_dt, f = factor(nphen_dt$phen_name)
      )
      nphen_dt_split <- lapply(nphen_dt_split, function(x) {
        x <- x[diff_secs >= 0]
        # changed the min to max in line below so that aggregation is
        # preformed using the finest possible record interval
        if (nrow(x) > 0) x <- x[diff_secs == max(diff_secs, na.rm = TRUE)]
        return(x)
      })
      nphen_dt <- data.table::rbindlist(nphen_dt_split)
      if (is.na(ris)) rit <- "event_based"
      if (!ris %in% c("event_based", "discnt")) rit <- "continuous"

      # row for gid phen
      gaprow <- nphen_dt[1, ]
      gaprow <- transform(gaprow, phen_name = "gap", units = "logi", measure =
          "smp", var_type = "logi", orig_table_name = "gaps",
        record_interval_type = rit
      )

      if (nrow(nphen_dt) > 0) {
        if (gaps) {
          nphen_dt <- data.table::rbindlist(list(nphen_dt, gaprow),
            use.names = TRUE
          )
        }
        nphen_dt$table_name <- paste0("dt_", nphen_dt$dt_record_interval[1],
          agg_dt_suffix, collapse = ""
        )
        nphen_dt$record_interval_type <- rit
        np <- np[paste(np$orig_table_name, np$phen_out_name) %in%
            paste(nphen_dt$orig_table_name, nphen_dt$phen_name)
        ]
        np[, table_name := nphen_dt$table_name[1]]
        np$record_interval_type <- rit
        np$dt_record_interval <- x
        np <- np[, names(np)[!names(np) %in% nn], with = FALSE]
      } else {
        nphen_dt <- NULL
        np <- NULL
      }
      return(list(phens_dt_new = nphen_dt, p = np))
    })
    rps <- unlist(rps, recursive = FALSE)
    rp <- data.table::rbindlist(rps[names(rps) %in% "p"])[
      !is.na(agg_f)
    ][order(table_name, phen_name)]
    rphen <- data.table::rbindlist(rps[names(rps) %in% "phens_dt_new"])[
      phen_name %in% c(rp$phen_out_name, "gid")
    ]
    if (nrow(rphen) == 0) {
      message(paste0("No new aggregate phens described. \n This will likely ",
        "cause problems in further processing. \n Ensure all phenomena",
        "parameters are described, e.g., especially the 'measure' required",
        "for matching an aggregation function."
      ))
      message("Proposed new phenomena below")
      print(phen_dt_new)
    }
    dt_parse <- list(phens_dt = rphen, f_params = list(agg_params = rp))
  }
  return(dt_parse)
}