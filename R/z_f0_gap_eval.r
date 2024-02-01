#' @title Highlight 'gaps' (missing data) in station data
#' @description The period between the time a logger is stopped, removed, or discommissioned, and when a logger beings recording data again, counts as missing data. However, for event-based data, where the frequency of recordings is temporarily erratic, identifying gaps is more tricky. This function sets some rules when identifying gaps in event-based time-series data.
#' @param pipe_house Required. List of pipeline directories. __See__ `ipayipi::ipip_init()` __for details__.
#' @param station_file Standardised __ipayipi__ station file.
#' @param gap_problem_thresh_s A duration threshold (in seconds) beyond which a gap, in event-based time series records, is considered problematic --- a 'true' gap. These gaps require infilling or 'patching'. The default for event-based data is six hours (i.e., 6 * 60 * 60 seconds). If the 'record_interval_type' is not 'mixed' or 'event_based', i.e. it is only continuous, then the 'record_interval' parameter is used as the `gap_problem_thresh_s`.
#' @param event_thresh_s A gap can also be specified in the event metadata by providing only a single date-time stamp and an 'event_threshold_s' (in seconds). By taking this date-time stamp the function approximates the start and end-time of the data gap by adding and subtracting the given `event_thresh_s` to the date-time stamp, respectively. If the `event_thresh_s` is supplied in the 'meta_events' data (_see_ `meta_to_station()`) then the value supplied therein is used in preference to the `event_thresh_s` argument in this function. The default event threshold here is ten minutes, i.e., 10 * 60 seconds.
#' @param keep_open Logical. Keep _hidden_ 'station_file' open for ease of access. Defaults to `FALSE`.
#' @details There are a number of different data problems that arise in time-series data. One of the simplest issues are data gaps, that is, periods where no data were recorded by a logger, or an individual sensor attached to a logger. Moreover, data can be declared 'missing' if it is erraneous. This function helps users identify gaps in continuous and discontinuous data types.
#' 
#'  different types problems with the data taking the following steps to
#'  identify and classify data 'gaps' (i.e., missing data).
#'  1. Trim any manually declared data gaps by the start and end dates of the
#'   extant rainfall data (start and end dates taken from the data summary).
#'  1. Generate a table of data 'gaps' between each download and deploy event.
#'   This type of data gap, i.e., the 'gap_type' is categorized as 'auto' in
#'   the final output. The rest of the gap types dealt with are manually
#'   declared data gaps.
#'  1. If there has been event data appended to the station data the function
#'   will search therein for any manually declared gaps, that is, the
#'   'event_type' 'logger_declare_error_segment'. If these segments extend
#'   'over' any 'auto' gaps they are trimmed to preserve the record of the
#'   'auto' gap. If any 'auto' gaps fall within an manually declared error
#'   segment then, they too, are considered a problem gap, even if the duration
#'   of the 'auto' gap is short enough to not be considered a problem gap.
#'  1. Within the pipeline notes can be appended to a gap using the
#'   'logger_gap_notes' 'event_type'. To do this a date-time stamp must be
#'   provided which falls within the gap of interest. When appending the notes
#'   ***the function will check whether the 'problem_gap' field was marked***.
#'   *If this field was FALSE then the gap is not considered a problematic gap*
#'   , i.e., the user is condident that no rainfall occurred during the 'gap'
#'   period. If the ***raining*** field was TRUE in the 'event_data' then the
#'   gap is considered problematic.
#'  1. A final 'gap' table is provided in the R data object. This table will
#'   be used to highlight no data when aggregating the data. Furthermore, in
#'   infilling exercises, the gaps provide a means for highlighting where
#'   'patching' is required. Below is a description of the gap table fields:
#'
#'   |Column  |Description |
#'   |--------|------------|
#'   |gid     |Unique identifier for each gap.  |
#'   |euid    |Event ID number imported from manually declared error segments. |
#'   |gap_start|The start date-time of a gap. |
#'   |gap_end |The end date-time of the gap. |
#'   |dt_diff_s |The time duration in seconds of the gap. |
#'   |gap_problem_thresh_s|The threshold duration beyond which a gap is
#'     considered problematic.|
#'   |problem_gap    |Logical field indicating whether a gap is considered to be
#'    problematic.|
#'   |Notes    | Notes describing the gap. The function will append any notes
#'    imported from the event data, if existent. |
#'
#' @keywords data gaps; data pipeline; missing data; event data;
#' @author Paul J. Gordijn
#' @return A table describing data 'gaps'
#' @export  
gap_eval <- function(
  pipe_house = NULL,
  station_file = NULL,
  gap_problem_thresh_s = 6 * 60 * 60,
  event_thresh_s = 10 * 60,
  keep_open = FALSE,
  ...
) {
  "dt_diff_s" <- "table_name" <- "start_dttm" <- "end_dttm" <- NULL

  # open station connection
  sfc <- ipayipi::open_sf_con(pipe_house = pipe_house,
    station_file = station_file)

  # generate gap table for each raw data table
  ds <- ipayipi::sf_read(sfc = sfc, pipe_house = pipe_house,
    tv = "data_summary", tmp = TRUE, station_file = station_file)[[
      "data_summary"]]
  ds$risec <- sapply(ds$record_interval, function(x) {
    sts_interval_name(x)[["dfft_secs"]]
  })

  tn <- unique(ds$table_name)

  seq_dt <- lapply(tn, function(x) {
    dx <- ds[table_name == x][
      order(start_dttm, end_dttm)
    ]
    # get rit and ri
    rit <- dx$record_interval_type
    if (!all("mixed" %in% rit, "event_based" %in% rit)) {
      ri <- dx$record_interval[1]
      ri <- sts_interval_name(ri)[["dfft_secs"]]
      gap_problem_thresh_s <- ri
    }
    l <- nrow(dx)
    if (l == 1) {
      seq_dt <- data.table::data.table(
        gid = seq_len(1),
        euid = NA,
        gap_type = "auto",
        phen = "logger",
        table_name = x,
        gap_start = dx$end_dttm[1],
        gap_end = dx$start_dttm[1],
        dt_diff_s = difftime(dx$start_dttm[1],
          dx$end_dttm[1], units = "secs"),
        gap_problem_thresh_s = gap_problem_thresh_s,
        problem_gap = TRUE,
        notes = as.character(NA)
      )
      seq_dt <- seq_dt[0, ]
    } else {
      seq_dt <- data.table::data.table(
        gid = seq_len(l - 1),
        euid = NA,
        gap_type = "auto",
        phen = "logger",
        table_name = x,
        gap_start = dx$end_dttm[1:(l - 1)],
        gap_end = dx$start_dttm[2:l],
        dt_diff_s = difftime(dx$start_dttm[2:l],
          dx$end_dttm[1:(l - 1)], units = "secs"),
        gap_problem_thresh_s = gap_problem_thresh_s,
        problem_gap = TRUE,
        notes = NA_character_
      )
    }
    seq_dt <- seq_dt[dt_diff_s >= 0]
    seq_dt[dt_diff_s <= gap_problem_thresh_s, "problem_gap"] <- FALSE
    seq_dt <- seq_dt[problem_gap == TRUE]
    return(seq_dt)
  })
  sf <- list(gaps = data.table::rbindlist(seq_dt))
  ipayipi::write_station(pipe_house = pipe_house, station_file =
    station_file, sf = sf, append = TRUE, overwrite = TRUE,
    keep_open = keep_open)

  invisible(sf)
}
