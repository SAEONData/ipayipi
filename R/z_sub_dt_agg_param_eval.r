#' @title 'dt' processing pipeline: genearte aggregation criteria.
#' @description Used in the processing pipeline to generate aggregation
#'  criteria. Data is aggregated by the time interval specified for the
#'  harvested data (_in_ `ipayipi::hsf_param_eval()`).
#' @param full_eval Logical indicating whether to perform a full evaluation
#'  or not. A partial evaluation only produces a string single string for
#'  a summary of the function's parameters. Defaults to `FALSE`.
#' @param f_summary Summary information used in full evaluation. Includes
#'  any tables in the station file with keywords, "phens" or "summary".
#' @param f_params For the partial evaluation multiple phenomena can
#'  be described using `ipayipi::agg_params()`. If left blank defaults will
#'  be used on all phenomena harvested.
#' @param agg_offset Character vector of length two. Strings describe period
#'  of offset from the rounded time interval used to aggregate data. For
#'  example, if aggregating rainfall data from five minute to daily records,
#'  but staggered so that daily rainfall is totalled from 8 am to 8 pm the
#'  next day the `agg_offset` must be set to `c("8 hours", "8 hours")`.
#'  When `full_eval == TRUE` the function overwrites this parameter in favor
#'  of `agg_offset` extracted from the partial evaluation.
#' @param all_phens Defaults to `FALSE`. If `TRUE`, all harvested phenomena will
#'  be aggregated, despite the lesser number of phenomena listed/described in
#'  the partial evaluation.
#' @param ignore_nas Logical that defaults to `FALSE`. When `TRUE` `NA` values
#'  will be ignored in aggregation functions. _This does not apply for
#'  non-default functions supplied by the partial evaluation_.
#' @param phens_summary A summary of the phens table of the particular
#'  phenomena that will be aggregated by `ipayipi::dt_agg()`.
#' @param ignore_nas If `NA` values are ignores any periods or time
#'  interval of the data with any NA values will result in an `NA`
#'  aggregation value. Important if missing values have not been
#'  interpolated.
#' @param eval_seq Dynamic list of tables accessed throughout the evaluation
#'  process. The final evaluation tables are appended to the station file.
#' @param sf Station file object.
#' @author Paul J. Gordijn
#' @details
#'  Default aggregation functions based on measure, variable types, and
#'   the unit of measurement. Function info is stored in the package
#'   data, "sts_agg_functions" --- _see examples_.
#'
#' @md
#' @examples
#' # load the table from the function data folder to see default aggregation
#' # functions
#' ipayipi::sts_agg_functions
#'
#' @export
agg_param_eval <- function(
  full_eval = FALSE,
  f_summary = NULL,
  f_params = NULL,
  agg_offset = c("0 secs", "0 secs"),
  all_phens = FALSE,
  ignore_nas = FALSE,
  eval_seq = NULL,
  ppsij = NULL,
  sf = NULL,
  ...) {
  "%ilike%" <- "agg_f" <- "measure" <- "phen_name" <-
    "phen_out_name" <- ":=" <- NULL
  x <- list(...)
  # table to be passed as variable for default aggregation functions
  ftbl <- ipayipi::sts_agg_functions
  # full and partial eval
  if (!full_eval) { # partial evaluation
    # basic argument checks
    v_names <- names(x)
    dt_parse <- sapply(seq_along(v_names), function(xi) {
      xp <- x[[v_names[xi]]]$aggs
      xp <- paste0(v_names[xi], " = ", deparse1(xp), collapse = "")
      xp <- gsub(pattern = "list", replacement = "agg_params", x = xp)
      return(xp)
    })
    if (length(x) == 0) all_phens <- TRUE
    if (all_phens[1]) {
      o <- "agg_options(all_phens = TRUE)"
    } else {
      o <- "agg_options()"
    }
    if (any(agg_offset != c("0 secs", "0 secs"))) {
      oi <- paste0(", agg_offset = c(\"", agg_offset[1],
        "\", \"", agg_offset[2], "\"))")
      o <- gsub(pattern = ")", replacement = oi, x = o)
      o <- gsub(pattern = "s\\(, ", replacement = "s\\(", x = o)
    }
    if (ignore_nas) {
      oi <- paste0(", ignore_nas = TRUE)")
      o <- gsub(pattern = ")", replacement = oi, x = o)
      o <- gsub(pattern = "s\\(, ", replacement = "s\\(", x = o)
    }
    if (o %in% "agg_options()") o <- NULL
    dt_parse <- c(dt_parse, o)
    class(dt_parse) <- c(class(dt_parse), "dt_agg_params")
  } else { # full evaluation
    # open ppsij and generate variables
    f_params <- ppsij$f_params
    f_param_options <- f_params[f_params %ilike% "agg_options"][1]
    f_params <- f_params[!f_params %ilike% "agg_options"]
    f_params <- lapply(f_params, function(x) {
      eval(parse(text = paste0("list(", x, ")")))
    })
    f_params <- unlist(f_params, recursive = FALSE)
    v_names <- names(f_params)
    xd <- lapply(v_names, function(x) {
      xd <- data.table::as.data.table(f_params[[x]]$aggs)
      xd$phen_name <- x
      return(xd)
    })
    # get agg options
    f_param_options <- gsub(pattern = "agg_options\\(", replacement = "list\\(",
      x = f_param_options)
    f_param_options <- eval(parse(text = f_param_options))
    agg_options <- c("agg_offset", "all_phens", "ignore_nas")
    agg_options_in <- agg_options[sapply(agg_options, function(x) {
      x %in% names(f_param_options)
    })]
    for (i in seq_along(agg_options_in)) {
      assign(agg_options_in[i], f_param_options[[agg_options_in[i]]])
    }

    phens_dt <- eval_seq$phens_dt
    xd <- data.table::rbindlist(xd, fill = TRUE)
    if (!all_phens) phens_dt <- phens_dt[phen_name %in% xd$phen_name]

    # generate default agg_info table based on phens_dt
    phens_dt <- phens_dt[!is.na(measure) | is.null(measure)]
    phens_dt$stage <- as.integer(substr(phens_dt$ppsid, 1, unlist(
      gregexpr("_", phens_dt$ppsid))[1] - 1))
    phens_dt <- phens_dt[stage == ppsij$dt_n[1]][, -c("stage"), with = FALSE]
    p <- merge(x = phens_dt, y = ftbl, by = "measure", all.x = TRUE)
    # check phen_dt for info and generate functions accordingly
    # generate f_param list component 'agg_info'
    p$agg_f <- data.table::fifelse(!is.na(p$units), p$f_continuous,
      rep(NA_character_, nrow(p)))
    p$agg_f <- data.table::fifelse(p$units %in% c("deg", "degrees", "degree"),
      p$f_circular, p$agg_f)
    p$agg_f <- data.table::fifelse(p$measure %ilike% "logical",
      p$f_logical, p$agg_f)
    p$units <- data.table::fifelse(p$agg_f %ilike% c("circular") &
      p$units == "deg", "degrees", p$units)
    p[agg_f %ilike% c("circular")]$agg_f <- sub(pattern = "<>", replacement =
      paste0("x, u = \"", p[agg_f %ilike% c("circular")]$units, "\"",
        ", ignore_nas = ", ignore_nas),
        x = p[agg_f %ilike% c("circular")]$agg_f)
    p[!agg_f %ilike% c("circular")]$agg_f <- sub(pattern = "<>", replacement =
      paste0("x, na.rm = ", ignore_nas),
        x = p[!agg_f %ilike% c("circular")]$agg_f)

    # update p based on agg_info
    agg_details <- data.table::data.table(ppsid = NA_character_,
      phid = NA_integer_, phen_name = NA_character_, phen_out_name =
      NA_character_, agg_f = NA_character_, units = NA_character_, measure =
      NA_character_, var_type = NA_character_, ignore_nas = ignore_nas,
      record_interval_type = NA_character_, orig_record_interval =
      NA_character_, dt_record_interval = NA_character_,
      orig_table_name = NA_character_, table_name = NA_character_)[0, ]
    xd <- rbind(xd, agg_details, fill = TRUE)
    names(xd)  <- paste0("c_", names(xd))
    p <- p[, names(p)[names(p) %in% names(agg_details)], with = FALSE]
    p$ignore_nas <- ignore_nas
    p <- rbind(p, agg_details, fill = TRUE)
    # update defaults in p per user input in xd
    p <- merge(x = p, y = xd, by.x = "phen_name", by.y = "c_phen_name",
      all.x = TRUE)
    p$phen_out_name <- data.table::fifelse(is.na(p$phen_out_name),
      p$phen_name, p$phen_out_name)
    p$phen_out_name <- data.table::fifelse(!is.na(p$c_phen_out_name),
      p$c_phen_out_name, p$phen_out_name)
    p$agg_f <- data.table::fifelse(!is.na(p$c_agg_f), p$c_agg_f, p$agg_f)
    p$units <- data.table::fifelse(!is.na(p$c_units), p$c_units, p$units)
    p$measure <- data.table::fifelse(!is.na(p$c_measure), p$c_measure,
      p$measure)
    p$var_type <- data.table::fifelse(!is.na(p$c_var_type), p$c_var_type,
      p$var_type)
    p <- p[, names(p)[!sapply(names(p), function(x) substr(x, 1, 2) == "c_")],
      with = FALSE]
    p$ppsid <- rep(paste0(ppsij$dt_n[1], "_", ppsij$dtp_n[1]),
      nrow(p))
    data.table::setcolorder(p, neworder = names(agg_details))

    # update phens_dt with new phen_out_name phens
    phen_dt_new <- p[, c(names(phens_dt), "phen_out_name"), with = FALSE]
    phen_dt_new <- phen_dt_new[, phen_name := phen_out_name][
      , -c("phen_out_name"), with = FALSE]
    phen_dt_new$ppsid <- rep(paste0(ppsij$dt_n[1], "_", ppsij$dtp_n[1]),
      nrow(phen_dt_new))

    dt_parse <- list(phens_dt = rbind(eval_seq$phens_dt, phen_dt_new),
      f_params = list(agg_params = p))
  }
  return(dt_parse)
}
