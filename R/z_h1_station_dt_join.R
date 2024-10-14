#' @title Joins or merges 'x' and 'y' tables.
#' @description Joins two data tables based on a set of criteria.
#' @param join One of the following join types:
#'  1. _full_join_. Joins the 'x' and 'y' tables using keyed column. Retains all the matching and unmatching rows of 'x' and 'y' tables.
#'  1. _left_join_. Adds the matching rows of 'x' to 'y' by keyed columns.
#'  1. _right_join_. Adds the matching rows of 'y' to 'x' by keyed columns.
#'  1. _inner_join_. Retains only rows of 'x' and 'y' that match by keyed columns.
#' Keys are the columns of the 'x' (left) and 'y' (right) tables used for finding matches for a join. Because `ipayipi` is based on date-time data the default keys are assumed to be "date_time" if not provided. Unlike conventional joins where rows are only matched if there is an exact match, for 'fuzzy' or 'non-equi' joins are flexible. When using a 'fuzzy' join an inequality sign must be provided in the key list. _See below__.
#' @param x_key Name(s) of the column(s) to key by the 'x' table. Vector of length one or two for conventional or fuzzy or non-equi joins, respectiely.
#' @param y_key Same as above but for the 'y' or right table.
#' @param x_key Name(s) of the column(s) to key by the 'x' table. Vector of length one or two for conventional or fuzzy or non-equi joins, respectively. Defaults to `date_time`.
#' @param y_key Same as above but for the 'y' or right table. Defaults to `date_time`.
#' @param fuzzy A numeric (seconds) vector of length one or two.
#' Fuzzy time-series joins require four keys in total: two keys for the 'x', and two for the 'y' table'. The nature of how these keys are join are controlled by arguments such as `inq` below.
#' Both keys can be provided using the `x_key` and `y_key` arguments. But, if only one key is specified in the `x_key` and `y_key` arguments, the second key for each table can be generated based on the values provided here. For each 'x' and 'y' table, given the `fuzzy` argument is provided, the first and second keys for a table are calculated as:
#'
#' \eqn{x_key[1] = x_key[1] - fuzzy[1]}
#' \eqn{x_key[2] = x_key[2] + fuzzy[1]}
#' \eqn{y_key[1] = y_key[1] - fuzzy[2]}
#' \eqn{y_key[2] = y_key[2] + fuzzy[2]}
#'
#' Note that if only one key is provided in the `x_key` and `y_key` arguments then the first value will be recycled for the second.
#'
#'  Internally, the default names for these are xd1 and xd2, and yd1 and yd2, for 'x' and 'y' tables, respectively.
#'
#' 
#' For 'fuzzy' joins where two keys are provided for each 'x' and 'y' table the join syntax is compiled like this:
#'  '1st x key' 'x inequality' '1st y key', '2nd y key' 'x inequality' '2nd x key'.
#' @param inq Inequality signs, vector of two (i.e., ">", ">=", "<", "<=") for fuzzy (non-equi) joins. Defaults to NULL.
#' @param y_phen_names Names of phenomena (columns) to retain post the join.
#'
#'
#' _Custom data.table arguments_
#' `data.table` has great documentation for these arguments.
#' @param nomatch In outer joins the rows for which no match was found can be retained by setting this argument to NA. If this is set to NULL 'no match' rows are not retained. By default an inner_join has this argument set to NULL. For other joins this is set to NA. Setting the value here will override the default.
#' @param mult Where multiple rows of the right table match the left, this argument controls how more than one match is handled. If specified as "all" (default), all matching rows are returned. Otherwise the "first" or "last" matching rows will be returned.
#' @param roll Can't get better than the `data.table` documentation here.
#' @param rollends Can't get better than the `data.table` documentation here.
#' @param allow.cartesian Can't get better than the `data.table` documentation here.
#' @param time_seq If data is continuous this can be set to TRUE to run time-series sensitive joining of data. Time-series sensitive joining on ensures the date-time series is continuous, i.e., the record intervals kept constant, and unique (no duplicate time stamps).
#' @param ri Record interval of the 'y' or 'x' for left and right joins, respectively, or for both in a time-sensitive full join (_see_ `time_seq`). Single character string of a standardised time-series/record interval. Record intervals can be standardised using `ipayipi::sts_interval_name()`.
#' @param phen_dt If a phenomena table is provided frokm the 'ipayipi' data processing pipeline, columns with listed standardised data types will be standardised accordingly using `ipayipi::phen_vars_sts()`.
#' @param over_right Controls whether 'x' or 'y' table data are overwritten during a full, time-sensitive join (when __seq_time == TRUE__). When TRUE the 'y' table is overwritten by overlapping (by time-series index) data in the 'x' table.
#' @return A list containing the processed data sets 'dts_dt'.
#' @author Paul J. Gordijn
#' details
#'
#' @export
dt_join <- function(
  join = "full_join",
  x_tbl = NULL,
  y_tbl = NULL,
  x_key = "date_time",
  y_key = "date_time",
  fuzzy = NULL,
  inq = NULL,
  y_phen_names = NULL,
  nomatch = NA,
  mult = "all",
  roll = FALSE,
  rollends = FALSE,
  allow.cartesian = FALSE,
  time_seq = NULL,
  ri = NULL,
  f_params = NULL,
  ppsij = NULL,
  sfc = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  "%ilike%" <- NULL
  # evaluate the f_params
  f_params <- eval(parse(text = f_params))
  if (!is.null(ppsij)) {
    # extract parts of ppsij and assign values in env
    ppsi_names <- names(ppsij)[!names(ppsij) %in% c("n", "f", "f_params")]
    for (k in seq_along(ppsi_names)) {
      assign(ppsi_names[k], ppsij[[ppsi_names[k]]][1])
    }
  }
  # extract parts of f_params and assign values in environment
  join_param_names <- names(f_params)
  for (k in seq_along(join_param_names)) {
    assign(join_param_names[k], f_params[[join_param_names[k]]])
  }
  # assign env join params to list
  d_args <- list(join = "full_join", x_key = "date_time", y_key = "date_time",
    nomatch = NA, mult = "all", roll = FALSE, rollends = FALSE,
    allow.cartesian = FALSE
  )

  # extract non-default args for f_params
  if (join == "full_join") join <- "fj"
  j_args <- lapply(seq_along(d_args), function(i) {
    if (d_args[[i]] %in% get(names(d_args)[i]) || is.null(d_args[[i]])) {
      r <- NULL
    } else {
      r <- get(names(d_args)[i])
    }
    return(r)
  })
  names(j_args) <- names(d_args)
  if (j_args$join == "fj") j_args$join <- "full_join"
  j_args <- j_args[!sapply(j_args, function(x) is.null(x))]
  # read in the available data
  sfcn <- names(sfc)
  wrk_dta <- sfcn[sfcn %in% "dt_working"]
  pstep <- paste0("^", ppsij$dt_n[1], "_.+_hsf_table_")
  hsf_dta <- sfcn[sfcn %ilike% pstep]
  hsf_dta <- hsf_dta[order(hsf_dta)]

  if (length(wrk_dta) > 0) {
    x_tbl <- ipayipi::sf_dta_read(sfc = sfc, tv = "dt_working")
    gx <- ipayipi::sf_dta_read(sfc = sfc, tv = "gaps")[["gaps"]]
    gx <- gx[table_name %in% ppsij$output_dt[1]]
  } else {
    x_tbl_n <- hsf_dta[length(hsf_dta) - 1]
    x_tbl <- ipayipi::sf_dta_read(sfc = sfc, tv = x_tbl_n)
    gx <- x_tbl[[x_tbl_n]]$gaps
  }
  if (length(hsf_dta) > 0) {
    y_tbl_n <- hsf_dta[length(hsf_dta)]
    y_tbl <- ipayipi::sf_dta_read(sfc = sfc, tv = y_tbl_n, tmp = TRUE)
    gy <- y_tbl[[y_tbl_n]]$gaps
  }
  x_tbl <- ipayipi::dt_dta_filter(dta_link = x_tbl, ppsij = ppsij)
  y_tbl <- ipayipi::dt_dta_filter(dta_link = y_tbl, ppsij = ppsij)
  x_tbl <- ipayipi::dt_dta_open(dta_link = x_tbl[[1]])
  y_tbl <- ipayipi::dt_dta_open(dta_link = y_tbl[[1]])

  args <- c(j_args, list(x_tbl = x_tbl, y_tbl = y_tbl, fuzzy = fuzzy,
    time_seq = time_seq
  ))

  ipayipi::msg("Pre-join x & y data", xtra_v)
  if (xtra_v) {
    print(head(x_tbl))
    print(head(y_tbl))
  }
  dt_working <- do.call(what = "mhlanga", args = args)
  ipayipi::msg("Post-join data", xtra_v)
  if (xtra_v) {
    ipayipi::msg("Head:", xtra_v)
    print(head(dt_working))
    ipayipi::msg("Tail:", xtra_v)
    print(tail(dt_working))
  }
  # dttm foor ppsij
  if (all(
    "date_time" %in% names(dt_working),
    nrow(dt_working) > 0
  )) {
    ppsij$start_dttm <- min(dt_working$date_time, na.rm = TRUE)
    ppsij$end_dttm <- max(dt_working$date_time, na.rm = TRUE)
  }
  # remove harvested data sets
  if (length(hsf_dta) > 0) {
    lapply(sfc[names(sfc) %in% hsf_dta], function(x) {
      unlink(x, recursive = TRUE)
    })
  }
  unlink(file.path(dirname(sfc)[1], "dt_working"), recursive = TRUE)
  ipayipi::sf_dta_chunkr(dta_room = file.path(dirname(sfc)[1], "dt_working"),
    chunk_i = NULL, rechunk = FALSE, dta_sets = list(dt_working),
    tn = "dt_working", ri = ppsij$time_interval[1],
    verbose = verbose
  )

  # gaps ----
  output_dt_n <- ppsij$output_dt[1]
  if (!is.null(gx)) gx$table_name <- output_dt_n
  if (!is.null(gy)) gy$table_name <- output_dt_n
  ng <- unique(rbind(gx, gy, fill = TRUE))
  g <- sf_dta_read(sfc = sfc, tv = "gaps")[["gaps"]]
  if (nrow(ng) > 0) g <- g[!table_name %in% output_dt_n]
  g <- rbind(g, ng, fill = TRUE)
  g <- g[order(table_name, phen, gap_type, gap_start, gap_end)]
  g <- unique(g)
  saveRDS(g, file = file.path(sfc["gaps"]))

  return(list(ppsij = ppsij))
}