#' @title Function to read in the events table
#' @description This function reads the dipper and casing height readings
#'  (retrieve log) which are used for drift correction of water levels.
#'  This data is appended to the __R__ groundwater data files.
#' @author Paul J. Gordijn
#' @keywords dipper readings
#' @param input_dir Directory in which the R solonist data files and retrieve
#'  log csv files are located.
#' @param recurr Should sub-directories be included in the search for the datum
#'  log csv file - TRUE/FALSE.
#' @param dt_format The function guesses the date-time format from a vector of
#'  format types supplied to this argument. The 'guessing' is done via
#'  `lubridate::parse_date_time()`. `lubridate::parse_date_time()` prioritizes
#'  the 'guessing' of date-time formats in the order vector of formats
#'  supplied. The default vector of date-time formats supplied should work
#'  well for most logger outputs.
#' @param dt_tz Recognized time-zone (character string) of the data locale. The
#'  default for the package is South African, i.e., "Africa/Johannesburg" which
#'  is equivalent to "SAST".
#' @export
gw_read_retrieve_log <- function(
  input_dir = NULL,
  recurr = FALSE,
  dt_format = c(
    "Ymd HMS", "Ymd IMSp",
    "ymd HMS", "ymd IMSp",
    "mdY HMS", "mdy IMSp",
    "dmY HMS", "dmy IMSp"),
  dt_tz = "Africa/Johannesburg",
  ...) {

  # check if there is a retrieve_log
  if (file.exists(file.path(input_dir, "retrieve_log.csv"))) {
    rum <- read.csv(file.path(input_dir, "retrieve_log.csv"),
      header = TRUE, stringsAsFactors = FALSE,
      na.strings = c("", " ", "NA"))
  } else {
    stop(paste0("No retrieve_log file found at ", input_dir),
      " - function aborted")
  }
  # check column names
  if (length(colnames(rum)) == 26) {
    if (all(colnames(rum) == c(
      "EvID",
      "Lock",
      "QA",
      "Event_type",
      "Date_time",
      "Date",
      "Download_time",
      "Recorder",
      "Lat",
      "Lon",
      "Location",
      "Borehole_name",
      "Logger_SN",
      "Logger_stopped",
      "Next_log_dt",
      "Next_log_tm",
      "Log_interval",
      "Logger_time_synced",
      "Data_download_check",
      "Logger_programmer",
      "Dipper_reading_recorder",
      "Dipper_reading_m",
      "Casing_ht_m",
      "Depth_to_logger_m",
      "Equipment_condition",
      "Notes"))) {
    } else {
      message(paste0("Column headers of imported retrieve log not matched",
        " - function aborted"))
      rum <- NULL
      return(rum)
    }
  } else {
    message("Column headers do not match - function aborted")
    rum <- NULL
    return(rum)
  }
  n_rows <- nrow(rum)
  rum <- rum[c(1:n_rows), ]
  rum <- suppressWarnings(data.table::data.table(
    EvID = as.numeric(rum$EvID),
    Lock = as.logical(rum$Lock),
    QA = as.logical(rum$QA),
    Event_type = as.factor(rum$Event_type),
    Date_time = lubridate::parse_date_time(x = rum$Date_time,
      orders = dt_format, tz = dt_tz),
    Date = as.Date(as.character(rum$Date)),
    Download_time = as.character(rum$Download_time),
    Recorder = as.character(rum$Recorder),
    Lat = as.numeric(rum$Lat),
    Lon = as.numeric(rum$Lon),
    Location = as.character(rum$Location),
    Borehole_name = as.character(rum$Borehole_name),
    Logger_SN = as.character(rum$Logger_SN),
    Logger_stopped = as.factor(rum$Logger_stopped),
    Next_log_dt = as.Date(as.character(rum$Next_log_dt)),
    Next_log_tm = as.character(rum$Next_log_tm),
    Log_interval = as.character(rum$Log_interval),
    Logger_time_synced = as.factor(rum$Logger_time_synced),
    Data_download_check = as.factor(rum$Data_download_check),
    Logger_programmer = as.character(rum$Logger_programmer),
    Dipper_reading_recorder = as.character(rum$Dipper_reading_recorder),
    Dipper_reading_m = as.numeric(rum$Dipper_reading_m),
    Casing_ht_m = as.numeric(rum$Casing_ht_m),
    Depth_to_logger_m = as.numeric(rum$Depth_to_logger_m),
    Equipment_condition = as.character(rum$Equipment_condition),
    Notes = as.character(rum$Notes)))
  rum_test <- rum[which(rum$QA == TRUE), ]
  if (anyNA(rum_test$Date_time) == TRUE) {
    message("There is missing Date_time data in the retrieve_log.")
  }
  return(rum)
}