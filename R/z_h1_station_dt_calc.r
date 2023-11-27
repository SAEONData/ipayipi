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
  "dt" <- NULL
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

  # organise f_params
  # extract ipip arguments from the seperate changes
  f_ipips <- lapply(f_params, function(x) {
    xpos <- regexpr(", ipip\\(", x)[1]
    xr <- substr(x, xpos, nchar(x) - 1)
    if (xpos > 0) {
      x <- eval(parse(text = gsub(", ipip\\(", "list\\(", xr)))
      if (!"fork_table" %in% names(x)) x <- NULL
      x <- x[sapply(names(x), function(x) x %in% "fork_table")]
      x <- unlist(x, recursive = TRUE)
    } else {
      x <- NULL
    }
  })
  # remove special ipip aruments for the evaluation
  f_params <- lapply(f_params, function(x) sub(", ipip\\(.*]$", "]", x))
  # chain together fork tables
  fk_params <- lapply(seq_along(f_params), function(i) {
    ic <- f_params
    if (!is.null(f_ipips[[i]])) {
      ic[[i]] <- paste0("[!", substr(ic[[i]], 2, nchar(ic[[i]])), collapse = "")
      r <- paste0(ic[1:i], collapse = "")
    } else {
      r <- NULL
    }
    return(r)
  })
  names(fk_params) <- f_ipips
  fk_params <- fk_params[sapply(fk_params, function(x) !is.null(x))]
  dt_fk_params <- lapply(fk_params, function(x) eval(parse(text = x)))
  dt_f_params <- list(dt = eval(parse(text = paste0(f_params, collapse = ""))))
  return(c(dt_f_params, dt_fk_params))

}