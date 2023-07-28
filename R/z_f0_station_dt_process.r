#' @title Pipeline data processing
#' @description Processes data in sequential steps
#' @param station_file Name of the station being processed.
#' @param ipip_room Directory where station data is stored.
#' @param pipe_process
#' @param output_dt_preffix The output table preffix which defaults to
#'  "dt_".
#' @param output_dt_suffix A custom suffix to be appended to the output
#'  tables name.
#' @param time_intervals Vector of strings describing time intervals.
#'  Each element of the vector corresponds to the aggregation of data
#'  into a new table. Each string element should begin with an integer,
#'  then the standard period, e.g., "1 month" or "10 mins". The period must
#'  be recognised by `ipayipi::sts_interval_name()`.
#' @param overwrite_pipe_memory Logical. If TRUE then extant pipeline steps,
#'  which are summarised in the 'pipe_process_summary' data table (*see
#'  details*), are modified by arguments in the pipe_process argument.
#' @author Paul J. Gordijn
#' @keywords data pipeline; data processing; processing steps
#' @details This function forms the basis of setting up a sequential data
#'  processing pipeline. This allows the extraction and preparation of
#'  raw, or other data from a data table in an 'ipayipi' station file,
#'  and further processing of this data.
#'
#'  Different portions of the pipeline are summarised in the
#'  'pipe_process_summary' data table. The sequential steps in the processing
#'  pipeline are numbered by the field `dt_1`.
#'
#'  1. `dt_n`: Sequential number of data tables produced by the pipeline.
#'       Seperate data tables will be processed following this order.
#'  2. `dt_name`: The name of the data processing table.
#'  3. `dtp_n`: Sequential numbering of pipeline process within a data table.
#'  4. `f`: The processing function.
#'  5. 'start_dttm': Earliest date time for which data have been processed in
#'   this table.
#'  6. 'end_dttm': Latest date time for which data have been processed in this
#'   table.
#'  
#' - ***Aggregating data by time periods***:
#'  
#' @export
dt_process <- function(
  station_file = NULL,
  ipip_room = ".",
  pipe_process = NULL,
  time_intervals = NULL,
  output_dt_preffix = "dt_",
  output_dt_suffix = NULL,
  overwrite_pipe_memory = TRUE,
  ...
) {
  # processes supported
  # data aggregation
  # data harvesting
  #  - within station
  #  - external station [list of priorities --- fill NAs]
  #  - merge harvested data
  #   - merge options
  # data calculations
  #  - in data.table env
  #  - chaining option\
  #  - fork table option
  # data join
  #  - joins harvested data
  # data formatting
  #  - remove columns
  #  - sort columns
  station_file <- "pipe_data_met/mcp/ipip_room/mcp_vasi_science_centre_aws.ipip"
  pipe_seq <- pipe_seq(p = pdt(
    p_step(
      dt_n = 1,
      dtp_n = 1,
      f = "dt_harvest",
      f_params = hsf_param_eval(phen_names = "rain_tot"),
      time_interval = "month"),
    # p_dt(
    #   dt_n = 1,
    #   dtp_n = 2,
    #   f = "dt_agg",
    #   f_params = f_param_eval(
    #     agg_params = agg_param_eval(
    #       rain_tot = ipayipi::params(
    #         agg_function = "mean(x)", units = "mm", table_name = "raw_5_mins")),
    #   input_dt = NULL,
    #   output_dt = NULL,
    #   time_interval = NULL)),
    p_step(
      dt_n = 1,
      dtp_n = 3,
      f = "dt_calc_chain",
      f_params = calc_param_eval(
          t_lag = chainer(
            dt_syn_ac = 'date_time + lubridate::as.period(1, unit = \"secs\")'),
          false_tip = chainer(
            dt_syn_ac = "fifelse(date_time == t_lag, TRUE, FALSE)",
            temp_var = FALSE, measure = "smp", units = "false_tip",
            var_type = "lg"),
          false_fork = chainer(dt_syn_bc = "false_tip == FALSE",
            fork_table = "false_tips"))),
    p_step(
      f = "dt_join", f_params = join_param_eval(
      join_type = "inner_left", y_table = "field")),
    p_step(f = "dt_calc_chain", dt_n = 2,
      f_params = calc_param_eval(
          vzum = chainer(
            dt_syn_ac = 'date_time + lubridate::as.period(1, unit = \"secs\")'),
          )),
    p_step(f = "dt_calc_chain", dt_n = 2,
      f_params = calc_param_eval(
          t_lag = chainer(
            dt_syn_ac = 'date_time + lubridate::as.period(1, unit = \"secs\")'),
          ))))
  output_dt_preffix = "dt_"
  join_param_eval(join_type = "inner_left", y_table = "field")
  
  # read function summary tables
  # open output_dt and associate table summary
  sf <- readRDS(station_file)
  sf_names <- names(sf)
  f_summary <- sf[sf_names %ilike% "summary|phens"]
  f_summary$sf_names <- sf_names
  dt_names <- sf_names[sf_names %ilike% output_dt_preffix]

  # get dttm max min dates
  sf_slice <- lapply(seq_along(dt_names), function(i) {
    return(list(
      sf_min = min(sf[[dt_names[i]]]$date_time),
      sf_max = max(sf[[dt_names[i]]]$date_time))
    )
  })
  names(sf_slice) <- dt_names

  # save station file in a temporary location
  sf_tmp_fn <- tempfile(pattern = "ipip_", tmpdir = tempdir())
  saveRDS(sf, file = sf_tmp_fn)

  ## standardise the overall pipe process summary
  pps <- f_summary$pipe_process_summary
  pp <- pipe_process(pipe_seq = pipe_seq, pipe_memory = pps)
  pps <- pp$pipe_seq

  # update calibrations and other station metadata
  #  functionality to be added

  # evaluate the function parameters
  pps <- split(pps, f = factor(pps$dt_n))

  # set up paired functions
  ff <- list(
    list("dt_harvest", "dt_calc_chain", "dt_agg", "dt_join"),
    list("hsf_param_eval", "calc_param_eval", "dt_agg_eval", "dt_join_eval")
  )
  # full evaluation of the pipeline then save evaluated f_params to function
  #  tables
  ii <- lapply(seq_along(pps), function(i) {
    ppsi <- pps[[i]]
    jj <- lapply(seq_along(nrow(ppsi)), function(j) {
      # get function and prepare arguments
      f <- ppsi$f[j]
      f <- ff[[2]][ff[[1]] %in% f][[1]]
      if (!f %in% c("calc_param_eval")) {
        f_params <- eval(parse(text = ppsi$f_params[j]))
      } else {
        f_params <- NULL
      }
      class(f_params) <- c("f_params", "list")
      args <- list(
        station_file = station_file,
        f_params = f_params,
        f_summary = f_summary,
        sf_tmp_fn = sf_tmp_fn,
        ppsi = ppsi[dt_n == i & dtp_n == j, ],
        full_eval = TRUE)
      o <- do.call(what = f, args = args)
      return(o)
    })

    # save o as readable tables for harvest function
    # no need to do evaluation if these tables already exist.
    # save tables as list name f_params and another phen_dt table

    # how to deal with harvests and sequential steps? Either harvest
    # and push to function or harvest as sepearte step.
    # In favor of seperate harvest step.

  })



  lapply(seq_along(pps), function(i) {
    ppsi <- pps[[i]]
    # check date_time for data slice

    # processes here should not be run in parallel. stations can be run
    #  in parallel
    lapply(seq_along(nrow(ppsi)), function(j) {
      f <- ppsi$f[j]
      # remove special arugments from parameter lists
      f_params <- eval(parse(text = ppsi$f_params[j]))
      class(f_params) <- c("f_params", "list")
      tmp <- tempfile(pattern = "ipip_", tmpdir = tempdir())
      o <- lapply(station_file,
        f_params = f_params,
        time_interval = ppsi$time_interval[j],
        f_summary = f_summary,
        input_dt = ppsi$input_dt[j],
        output_dt = ppsi$output_dt[j],
        sf_tmp_fn = sf_tmp_fn,
        dtp_n = j,
        FUN = f)
      
      # - update 'process pipe summary' dates
      # - ** update 'process pipe phenomena'
      # - update 'pipe function' table (e.g., for aggregation or harvesting etc)
      # - save data
      
    })
  })

  return(station_file)
}
