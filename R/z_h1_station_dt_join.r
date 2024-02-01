#' @title Perform data processing calculations
#' @description Calculations are parsed to data.table.
#' @param station_file Name of the station being processed.
#' @param dta_in Arugment for pipeline processing. Instead of providing the 'x' and 'y' tables to `x_tbl` and `y_tbl` The pipeline feeds data to this argument. A list of data to be processed.
#' param input_dt 
#' param output_dt 
#' param f_params
#' @param f_summary A summary table of function parameters. This
#'  summary table is stored in the station file object. If there
#'  is a summary table for the function, differences between the
#'  `f_params` and table are checked and the 'f_summary' table
#'  is updated---triggering a re-calculation of the whole time series instead
#'  of for a slice of the series.
#' @return A list containing the processed data sets 'dts_dt'.
#' @author Paul J. Gordijn
#' details
#' 
#' @export
dt_join <- function(
  join = "full_join",
  x_tbl = NULL,
  y_tbl = NULL,
  y_phen_names = NULL,
  x_key = "date_time",
  y_key = "date_time",
  nomatch = NA,
  mult = "all",
  roll = FALSE,
  rollends = FALSE,
  allow.cartesian = FALSE,
  time_seq = NULL,
  fuzzy = NULL,
  station_file = NULL,
  dta_in = NULL, # alternate to providing data list with x and y tables
  input_dt = NULL,
  output_dt = NULL,
  output_dt_preffix = "dt_",
  output_dt_suffix = NULL,
  f_params = NULL,
  dt_working = NULL,
  ppsij = ppsij,
  ...) {
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
    if (exists(join_param_names[k])) {
      assign(join_param_names[k],
        f_params[[join_param_names[k]]])
    }
  }
  # assign env join params to list
  d_args <- list(join = "full_join", x_key = "date_time", y_key = "date_time",
    nomatch = NA, mult = "all", roll = FALSE, rollends = FALSE,
    allow.cartesian = FALSE)
  
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
  if (!is.null(dt_working)) {
    dta_in <- dt_working
    names(dta_in)
    # get x a y tables
    if ("hsf_dts" %in% names(dta_in)) y_tbl <- dta_in[["hsf_dts"]][[1]]
    if ("dt" %in% names(dta_in)) x_tbl <- dta_in[["dt"]]
  } else {
    x_tbl <- dta_in[[x_tbl]]
    y_tbl <- dta_in[[y_tbl]]
  }
  dts <- list(x = x, y = y)
  # time_seq prep ----
  if (length(time_seq) == 1) time_seq <- c(time_seq, time_seq)
  # develop on (key) syntax
  if (length(fuzzy) == 1) fuzzy <- c(fuzzy, fuzzy)

  x[y, on = .(x_key, y_key)]
  # full_join ----

  # left_join ----
  # develop syntax
  

  # right join ----

  # inner_join ----

  if ("hsf_dts" %in% names(dta_in)) {
    dta_in <- dta_in[["hsf_dts"]][[1]]
  } else {
    dta_in <- dta_in[["dt"]]
  }
  dt <- data.table::as.data.table(dta_in)
  f_params <- paste0(substr(f_params, 1,
    unlist(gregexpr(", ipip\\(", f_params))[1] - 1), "]", collapse = "")
  dt <- eval(parse(text = f_params))
  return(list(dt = dt))
}