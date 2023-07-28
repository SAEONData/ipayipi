#' @title Perform data processing calculations
#' @description Calculations are parsed to data.table.
#' @param station_file Name of the station being processed.
#' @param input_dt 
#' @param output_dt 
#' @param f_params
#' @param f_summary A summary table of function parameters. This
#'  summary table is stored in the station file object. If there
#'  is a summary table for the function, differences between the
#'  `f_params` and table are checked and the 'f_summary' table
#'  is updated---triggering a re-calculation of the whole time series instead
#'  of for a slice of the series.
#' @author Paul J. Gordijn
#' @details
#'
#' @export
dt_calc_chain <- function(
  station_file = NULL,
  input_dt = NULL,
  output_dt = NULL,
  ext_dir = NULL,
  ext_station = NULL,
  ext_station_table = NULL,
  output_dt_preffix = "dt_",
  output_dt_suffix = NULL,
  f_params = NULL,
  f_summary = NULL,
  ...) {
  # need two dt_calc sub functions
  f_params <- dt_calc_eval(
    t_lag = chainer(
      dt_syn_ac = 'date_time + lubridate::as.period(1, unit = \"secs\")'),
    false_tip = chainer(
      dt_syn_ac = "fifelse(date_time == t_lag, TRUE, FALSE)",
      fork_table = TRUE, fork_table_name = "false_tips",
      temp_var = FALSE, measure = "smp", units = "false_tip",
      var_type = "lg"))

  # parse the dt_calc_chain input
  v_names <- names(f_params)
  dt_parse <- sapply(seq_along(v_names), function(ii) {
    x <- paste0("[",
      # before 1st comma
      f_params[[v_names[ii]]]$dt_syn_bc, ", ",
      # after 1st comma
      if (!is.null(f_params[[v_names[ii]]]$dt_syn_ac)) {
        paste0(v_names[ii], " := ", collapse = "")
      } else {
        ""
      },
      # after 2nd comma
      f_params[[v_names[ii]]]$dt_syn_ac,
      if (!is.null(f_params[[v_names[ii]]]$dt_syn_exc)) ", " else "",
      f_params[[v_names[ii]]]$dt_syn_exc, "]")
    if (ii == 1) {
      x <- paste0("dt_pro", x)
    }
    return(x)
  })
  dt_parse <- paste(dt_parse, collapse = "")

  if (class(input_dt) == "processing") {
    dt_pro <- dt_pro
    start_dttm <- min(main_agg$date_time)
    end_dttm <- max(main_agg$date_time)
  }
  # check and read input data
  if (is.null(input_dt) && is.character(input_dt) && !is.null(station_file)) {
    dt_pro <- readRDS(station_file)[["input_dt"]]
  }
  if (is.null(input_dt)) stop("No input data!")

  # check the function summary table and update if necesseary.
  if (!is.null(f_summary)) {

  }
  # parse data table syntax to the data table
  dt_pro <- eval(parse(text = ))




  # return the results and summary info to be used in the pipe line
  return(list(main_agg = i_dt, main_agg_name = output_dt,
    start_dttm = min(main_agg$date_time), end_dttm = max(main_agg$date_time)))
}