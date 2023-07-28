#' @title Open and check rainfall event metadata database.
#' @description Reads standardised metadata and appends records the matching
#'  station.
#' @param input_dir Directory in which to search for the metadata database.
#' @param meta_file_name File name of the standardised rainfall metadata
#'  database.
#' @param dt_format The date-time format of the event metadata database.
#' @param tz  The time zone of the event metadata database that must match
#'  the data in question.
#' @keywords rainfall data processing; metadata; data pipeline; supplementary
#'  data; field notes
#' @author Paul J. Gordijn
#' @return Standardised data table of events.
#' @details Reads in an events database or sheet in 'csv' format. Checks that
#'  column names have been standardised. Transforms the date-time columns to
#'  a standardised format --- ** this format and timezone must match that used
#'  by the data pipeline **.
#' @export
rain_meta_read <- function(
  input_dir = NULL,
  meta_file_name = "event_db.rmds",
  dt_format = "%Y-%m-%d %H:%M:%S",
  tz = "Africa/Johannesburg",
  ...
) {
  if (!file.exists(file.path(input_dir, meta_file_name))) {
    stop("The events metadata database does not exist!")
  }
  edb <- readRDS(file.path(input_dir, meta_file_name))
  ev_names <- c("uid", "qa", "date_time", "location_station", "event_type",
    "event_thresh_s", "battery", "memory_used", "data_recorder", "raining",
    "flag_problem", "notes", "start_dt", "end_dt", "data_capturer")
  if (any(names(edb) != ev_names)) {
    stop("Names of event metadata columns are not aligned with standard!")
  }
  edb <- transform(edb,
    uid = as.integer(uid),
    qa = as.logical(qa),
    date_time = as.POSIXct(
      strptime(edb$date_time, format = dt_format), tz = tz),
    location_station = as.character(location_station),
    event_type = as.character(event_type),
    event_thresh_s = as.numeric(event_thresh_s),
    battery = as.numeric(battery),
    memory_used = as.numeric(memory_used),
    data_recorder = as.character(data_recorder),
    raining = as.logical(raining),
    flag_problem = as.logical(flag_problem),
    notes = as.character(notes),
    start_dt = as.POSIXct(
      strptime(edb$start_dt, format = dt_format), tz = tz),
    end_dt = as.POSIXct(
      strptime(edb$end_dt, format = dt_format), tz = tz),
    data_capturer = as.character(data_capturer))

  # check if all date-time values have been supplied.
  edb_checks <- c("logger_declare_error_segment")
  edb_i <- edb[event_type %in% edb_checks]
  edb_ii <- edb_i[is.na(event_thresh_s) & is.na(start_dt)]
  edb_iii <- edb_i[is.na(event_thresh_s) & is.na(start_dt) & is.na(start_dt)]
  if (nrow(edb_ii) > 0 | nrow(edb_iii) > 0) {
    stop("Missing threshold event time or start and end date-time values!")
  }
  # change na logicals to FALSE
  edb[is.na(qa), "qa"] <- FALSE
  edb[is.na(raining), "raining"] <- FALSE
  edb[is.na(flag_problem), "flag_problem"] <- FALSE

  invisible(edb)
}