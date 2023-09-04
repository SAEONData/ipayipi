#' @title Read datum log csv
#' @description Function to read in a datum log csv file and checks variable
#'  formats. The datum log contains the borehole metadata with survey heights
#'  i.e. altitude, collar/casing height and depth to well etc. This data is
#'  appended to the __R__ groundwater data files.
#' @keywords datum, altitude
#' @author Paul J. Gordijn
#' @param input_dir Directory in which the R solonist data files and datum log
#'  csv files are located.
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
gw_read_datum_log <- function(
  input_dir = NULL,
  recurr = FALSE,
  dt_format = c(
    "Ymd HMS", "Ymd IMSp",
    "ymd HMS", "ymd IMSp",
    "mdY HMS", "mdy IMSp",
    "dmY HMS", "dmy IMSp"),
  dt_tz = "Africa/Johannesburg",
  ...) {
  # check if there is a datum log
  if (file.exists(file.path(input_dir, "datum_log.csv"))) {
    dum <- read.csv(file.path(input_dir, "datum_log.csv"),
      header = TRUE, stringsAsFactors = FALSE,
      na.strings = c("", " ", "NA"))
  } else {
    stop(paste0("No datum_log file found at ",
      input_dir, " - function aborted"))
  }
  # check import data
  if (length(colnames(dum)) == 15) {
    if (all(colnames(dum) == c(
    "dID",
    "Lock",
    "QA",
    "File",
    "Date_time",
    "Method",
    "Location",
    "Borehole",
    "Datum_ESPG",
    "X",
    "Y",
    "H_masl",
    "Well_depth_m",
    "Log",
    "Notes"))) { # do nothing
    } else {
      dum <- NULL
      message("Column headers do not match - function aborted")
      return(dum)
    }
  } else {
  dum <- NULL
  message("Column headers of the datum log not matched - function aborted")
  return(dum)
  }

  # Subset dum based on the number of rows with an id value
  n_rows <- max(as.integer(dum$dID), na.rm = TRUE)
  dum <- dum[c(1:n_rows), ]
  dum <- data.table::data.table(
  dID = as.integer(as.character(dum$dID)),
  Lock = as.logical(as.character(dum$Lock)),
  QA = as.logical(as.character(dum$QA)),
  File = as.character(dum$File),
  Date_time = lubridate::parse_date_time(
    x = dum$Date_time, orders = dt_format, tz = dt_tz),
  Method = as.character(dum$Method),
  Location = as.character(dum$Location),
  Borehole = as.character(dum$Borehole),
  Datum_ESPG = as.character(dum$Datum_ESPG),
  X = as.numeric(dum$X),
  Y = as.numeric(dum$Y),
  H_masl = as.numeric(dum$H_masl),
  Well_depth_m = as.numeric(dum$Well_depth_m),
  Log = as.logical(as.character(dum$Log)),
  Notes = as.character(dum$Notes))
  dum_test <- dum[which(dum$QA == TRUE), ]
  if (anyNA(dum_test$Date_time) == TRUE) {
    message("There is missing Date_time data in the datum_log.")
    }
  return(dum)
}
