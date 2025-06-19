#' @title Highlight 'gaps' (missing data) in station data
#' @description The period between the time a logger is stopped, removed, or discommissioned, and when a logger beings recording data again, counts as missing data. However, for event-based data, where the frequency of recordings is temporarily erratic, identifying gaps is more tricky. This function sets some rules when identifying gaps in event-based time-series data.
#' @inheritParams logger_data_import_batch
#' @inheritParams append_station
#' @inheritParams dta_list
#' @param gap_problem_thresh_s A duration threshold (in seconds) beyond which a gap, in event-based time series records, is considered problematic --- a 'true' gap. These gaps require infilling or 'patching'. The default for event-based data is six hours (i.e., 6 * 60 * 60 seconds). If the 'record_interval_type' is not 'mixed' or 'event_based', i.e. it is only continuous, then the 'record_interval' parameter is used as the `gap_problem_thresh_s`.
#' @param event_thresh_s A gap can also be specified in the event metadata by providing only a single date-time stamp and an 'event_threshold_s' (in seconds). By taking this date-time stamp the function approximates the start and end-time of the data gap by adding and subtracting the given `event_thresh_s` to the date-time stamp, respectively. If the `event_thresh_s` is supplied in the 'meta_events' data (_see_ `meta_to_station()`) then the value supplied therein is used in preference to the `event_thresh_s` argument in this function. The default event threshold here is ten minutes, i.e., 10 * 60 seconds.
#' @param meta_events Optional. The name of the data.table in the station file with event metadata. Defaults to NA.
#' @param tbl_n Regular expression used to select the table to be evaluated for gaps. Defaults to the 'raw' tables with the regex expression '^raw_*'.
#' @param phen_eval Logical. Decides whether to evaluate phenomena gaps. If FALSE this function will return NULL.
#' @param phens Vector. Phenomena names to focus on for evaluation. If NULL all phenomena are processed.
#' @details
#' There are a number of problems that arise in time-series data. One of the simplest issues are data gaps, that is, periods where no data were recorded by a logger, or an individual sensor attached to a logger. Moreover, data can be declared 'missing' if it is erraneous. This function helps users identify gaps in continuous and discontinuous data types. Below is an overview of this function.
#'
#'  1. Derive gap table from raw data data summary information.
#'    If the gap period is longer than the record interval in continuous data the gap is declared problematic, i.e., `problem_gap == TRUE`. For discontinuous data, where the gap period extends beyond the `gap_problem_thresh_s` the gap is _'automatically'_ declared to be problematic.
#'  1. Integrate event metadata into the gap table (`meta_events` table).
#'    If a 'problem gap' has been declared in event metadata then this information is transferrred into the gap table. Also we can add notes to gaps declared automatically in the step above. Note that any gap declared in the metadata requires appropriate metadata describing the gap. These include:
#'    - 'eid': a unique event identification number (integer),
#'    - 'qa': Quality assessment. Logical. Only evaluates `qa == TRUE`,
#'    - 'station/location station': character string identifying the staiton name,
#'    - 'date_time': The date and time of the event (POSIX date time with format to match the raw data date_time),
#'    - 'event_type': Stanardised character string. For declaring a gap period use 'manual_gap'. For adding notes to a gap period use 'gap_notes' --- _see notes on this below_,
#'    - 'phen': If not provided this will default to 'logger', or else a specific phenomena name can be provided (standardised from the phens table). If this value is 'logger' we will assume the logger (with all sensors attached) was not functioning over the declared period,
#'    - 'table_name': A specific table the gap period is applied to. If no value is provided this will default to all raw data,
#'    - 'gap_problem_thresh_s': Can be provided to overwrite the `gap_problem_thresh_s` provided in the function arguments,
#'    - 'event_thresh_s': The event threshhold in seconds (_see start_dttm and end_dttm fields below),
#'    - 'problem_gap': Logical. If `TRUE` then the gap period declared is considered to be problematic. If `FALSE` a gap period will not be considered problematic. This is useful for discontinuous data. For example, if it was not raining during the period our logger had been stopped --- the 'automatically' identified gap, between the stop and start of the logger recording rainfall events (discontinuous record interval) --- the gap is not problematic. Therefore, we can declare `problem_gap` to be `FALSE` in the event metadata. Appropriate notes will be appended from the event metadata to the gap table in all cases.
#'    - 'notes': Character field for general note taking,
#'    - 'start_dttm': *Start date time of the manual gap period, and
#'    - 'end_dttm': *End date time of the manual gap period.
#'
#' * note that if start and end date times are not provided the 'date_time' field must be provided. Using adding and substracting the `event_thresh_s` to `date_time` will be used to generate `start_dttm` and `end_dttm` fields in this case.
#'
#' The final gap table summarises gap data merged with the above event metadata.
#'
#'
#' @keywords data gaps; data pipeline; missing data; event data;
#' @author Paul J. Gordijn
#' @return Names of stations with Updated gap period tables.
#' @export
gap_eval_batch <- function(
  pipe_house = NULL,
  gap_problem_thresh_s = 6 * 60 * 60,
  event_thresh_s = 10 * 60,
  phens = NULL,
  phen_eval = FALSE,
  meta_events = "meta_events",
  tbl_n = "^raw_*",
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  ...
) {
  # assignments
  station_ext <- ".ipip"

  # get list of station names in the ipip directory
  station_files <- ipayipi::dta_list(
    input_dir = pipe_house$d4_ipip_room, file_ext = station_ext, prompt = FALSE,
    recurr = FALSE, unwanted = unwanted, wanted = wanted
  )

  if (length(station_files) == 0) {
    cli::cli_abort(c("No station files detected!",
      "i" = "Station files should be housed in the \'d4_ipip_room\' here:",
      " " = "{.var pipe_house$d4_ipip_room}"
    ))
  }

  if (verbose || xtra_v || chunk_v) cli::cli_h1(
    "Evaluating gaps in {length(station_files)} station file{?s}"
  )

  # generate gap table for each station file
  gaps <- lapply(seq_along(station_files), function(i) {
    sfc <- sf_open_con(pipe_house = pipe_house, station_file = station_files[i])
    g <- ipayipi::gap_eval(
      pipe_house = pipe_house, station_file = station_files[i],
      sfc = sfc, gap_problem_thresh_s = gap_problem_thresh_s, event_thresh_s =
        event_thresh_s, meta_events = meta_events, tbl_n = tbl_n,
      verbose = verbose, phens = phens, phen_eval = phen_eval
    )
    sfc <- sf_open_con(sfc = sfc)
    sf_dta_rm(sfc = sfc, rm = "gaps")
    sl <- sf_dta_wr(dta_room = file.path(dirname(sfc[2]), "gaps"), dta = g$gaps,
      tn = "gaps", rit = "event_based", ri = "discnt", chunk_v = chunk_v
    )
    sf_log_update(sfc = sfc, log_update = sl)
    if (verbose || xtra_v || chunk_v) cli::cli_inform(c(
      "v" = "{i}: {gsub(station_ext, \'\', station_files[i])}"
    ))
    invisible(station_files[i])
  })
  if (verbose || xtra_v || chunk_v) cli::cli_h1("")
  invisible(gaps)
}
