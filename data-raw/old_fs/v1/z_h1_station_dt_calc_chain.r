#' @title Perform data processing calculations
#' @description Calculations are parsed to data.table.
#' @param station_file Name of the station being processed.
#' @param dta_in A list of data to be processed.
#' @param input_dt 
#' @param output_dt 
#' @param f_params
#' @param f_summary A summary table of function parameters. This
#'  summary table is stored in the station file object. If there
#'  is a summary table for the function, differences between the
#'  `f_params` and table are checked and the 'f_summary' table
#'  is updated---triggering a re-calculation of the whole time series instead
#'  of for a slice of the series.
#' @return A list containing the processed data sets 'dts_dt'.
#' @author Paul J. Gordijn
#' @details
#' 
#' @export
dt_calc <- function(
  station_file = NULL,
  dta_in = NULL,
  input_dt = NULL,
  output_dt = NULL,
  output_dt_preffix = "dt_",
  output_dt_suffix = NULL,
  f_params = NULL,
  dt_working = NULL,
  ...) {
  # read in the available data
  if (!is.null(dt_working)) {
    dta_in <- dt_working
  }
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