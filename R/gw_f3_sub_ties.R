#' @title Generates dummy dipper points (ties).
#' @md
#' @description Generated tie points are used as artificial calibration
#'  points (dipper readings) for drift correction.
#' @param file Standardised 'ipayipi' water level file. The file must be
#'  updated with the latest calibration measurements using
#'  `ipayipi::gw_physiCally()`.
#' @param tie_type Must be supplied with one of the following options
#'  (strings):
#'  1. "dummy" --- This option generates a dummy-dipper reading using
#'   linear interpolation. Only one date-time value must be provided; this will
#'   be the point at which the dummy dipper (calibration) measurement will be
#'   created. In the linear interpolation the two closest surrounding
#'   calibration measurements will be used. Any dummy-dipper reading generated
#'   will be used in subsequent drift correction.
#' @param recurr Setting this to TRUE will enable searching
#'  for R data files in sub directories i.e., a recursive search.
#' @param prompted If TRUE, a command line prompt will be used to
#'  enable selection of which files in the working directory require
#'  drift correction.
#' @param dt_format The function guesses the date-time format from a vector of
#'  format types supplied to this argument. The 'guessing' is done via
#'  `lubridate::parse_date_time()`. `lubridate::parse_date_time()` prioritizes
#'  the 'guessing' of date-time formats in the order vector of formats
#'  supplied. The default vector of date-time formats supplied should work
#'  well for most logger outputs.
#' @param dt_tz Recognized time-zone (character string) of the data locale. The
#'  default for the package is South African, i.e., "Africa/Johannesburg" which
#'  is equivalent to "SAST".
#' @details Tie points can be used to correct for level shifts in time series
#'  data. This function will attempt to automatically generate tie points using:
#'  1. Available calibration measurements,
#'  1. The drift corrected water level, or
#'  1. Something else ...
#' @keywords drift correction; linear drift; tie points; dummy dipper readings
#' @return Table of dummy dipper readings, or tie points.
#' @export
gw_ties <- function(
  file = NULL,
  tie_type = NULL,
  tie_datetime = NULL,
  dt_format = c(
    "Ymd HMS", "Ymd IMSp",
    "ymd HMS", "ymd IMSp",
    "mdY HMS", "mdy IMSp",
    "dmY HMS", "dmy IMSp"),
  dt_tz = "Africa/Johannesburg",
  ...
) {
  if (class(file) != "level_file") {
    stop("Please use an ipayipi standardised file!")
  }
  if (tie_type == "dummy_drift" && length(tie_datetime) == 1) {
    ctbl <- file$log_retrieve
    tie_datetime <- lubridate::ymd_hms(tie_datetime, tz = dt_tz)
    ctbl <- ctbl[Date_time != tie_datetime][
      !Date_time < file$log_t$Date_time[1] |
        !Date_time > file$log_t$Date_time[nrow(file$log_t)]
    ]
    ctbl <- ctbl[!Notes %in% "auto_dummy_drift"]
    ctbl_i <- data.table::data.table(
      id = as.integer(max(ctbl$id) + 1, na.rm = TRUE),
      Date_time = tie_datetime,
      QA = as.logical(TRUE),
      Dipper_reading_m = as.numeric(NA),
      Casing_ht_m = as.numeric(NA),
      Depth_to_logger_m = as.numeric(NA),
      Notes = "auto_dummy_drift"
    )
    ctbl <- ctbl[!Date_time %in% tie_datetime]
    ctbl <- rbind(ctbl, ctbl_i)[order(Date_time)]
    ci_row <- which(ctbl$Date_time == tie_datetime)
    st_row <- ci_row - 1
    ed_row <- ci_row + 1
    if (nrow(ctbl) > 2) { # only do this if there is a calibration re
      if (st_row < 1) { # there is no first calibration
        start_dt <- file$log_t$Date_time[1]
        wlevel <- file$log_t$t_level_m[1]
        ctbl_i <- data.table::data.table(
          id = as.integer(max(ctbl$id) + 1, na.rm = TRUE),
          Date_time = start_dt,
          QA = as.logical(TRUE),
          Dipper_reading_m = as.numeric(NA),
          Casing_ht_m = as.numeric(NA),
          Depth_to_logger_m = as.numeric(NA),
          Notes = "auto_dummy_drift_false-start"
        )
        ctbl <- rbind(ctbl, ctbl_i)[order(Date_time)]
        ci_row <- which(ctbl$Date_time == lubridate::parse_date_time(
        x = tie_datetime, orders = dt_format, tz = dt_tz))
        st_row <- ci_row - 1
        ed_row <- ci_row + 1
      } else {
        start_dt <- ctbl$Date_time[st_row]
        wlevel <- NA
      }
      if (ed_row > nrow(ctbl)) { # there is no second calibration
        end_dt <- file$log_t$Date_time[nrow(file$log_t)]
        wlevel <- file$log_t$t_level_m[nrow(file$log_t)]
        ctbl_i <- data.table::data.table(
          id = as.integer(max(ctbl$id) + 1, na.rm = TRUE),
          Date_time = end_dt,
          QA = as.logical(TRUE),
          Dipper_reading_m = as.numeric(NA),
          Casing_ht_m = as.numeric(NA),
          Depth_to_logger_m = as.numeric(NA),
          Notes = "auto_dummy_drift_false-end"
        )
        ctbl <- rbind(ctbl, ctbl_i)[order(Date_time)]
      } else {
        end_dt <- ctbl$Date_time[ed_row]
        wlevel <- NA
      }
      ctbl <- ctbl[Date_time >= start_dt & Date_time <= end_dt]
      # join the datum table
      ctbl$dummy_dt <- ctbl$Date_time
      datum <- subset(file$log_datum, select = -c(id, X, Y, Datum))
      data.table::setkey(ctbl, "Date_time", "dummy_dt")
      data.table::setkey(datum, "Start", "End")
      ctbl <- data.table::foverlaps(
        x = ctbl, y = datum, mult = "all", type = "within")
      if (is.na(ctbl$Casing_ht_m[1])) ctbl$Casing_ht_m[1] <-
        ctbl$Casing_ht_m[3]
      if (is.na(ctbl$Casing_ht_m[3])) ctbl$Casing_ht_m[3] <-
        ctbl$Casing_ht_m[1]
      if (is.na(ctbl$Dipper_reading_m[3])) ctbl$Dipper_reading_m[3] <-
        ctbl$Casing_ht_m[3] + ctbl$H_masl[3] - wlevel
      if (is.na(ctbl$Dipper_reading_m[1])) ctbl$Dipper_reading_m[1] <-
        ctbl$Casing_ht_m[1] + ctbl$H_masl[1] - wlevel
      ctbl$diff_dt <- c(
        as.numeric(ctbl$Date_time[2]) - as.numeric(ctbl$Date_time[1]),
        NA,
        as.numeric(ctbl$Date_time[3]) - as.numeric(ctbl$Date_time[2]))
      ctbl$Casing_ht_m[2] <- ctbl[diff_dt == min(ctbl$diff_dt, na.rm = TRUE)
        ]$Casing_ht_m

      # use linear interpolation to generate dipper reading
      d3 <- (ctbl$H_masl[3] + ctbl$Casing_ht_m[3]) - ctbl$Dipper_reading_m[3]
      d1 <- (ctbl$H_masl[1] + ctbl$Casing_ht_m[1]) - ctbl$Dipper_reading_m[1]
      m <- (d3 - d1) /
        (as.numeric(ctbl$Date_time[3]) - as.numeric(ctbl$Date_time[1]))
      c <- d1 - (m * as.numeric(ctbl$Date_time[1]))
      d2 <- (m * as.numeric(ctbl$Date_time[2])) + c
      ctbl$Dipper_reading_m[2] <- (ctbl$H_masl[2] + ctbl$Casing_ht_m[2]) - d2
      ctbl <- subset(ctbl, select = names(file$log_retrieve))[2]
    }
  } else {
    ctbl <- data.table::data.table(
      id = as.integer(NA),
      Date_time = as.POSIXct(as.character(NA)),
      QA = as.logical(TRUE),
      Dipper_reading_m = as.numeric(NA),
      Casing_ht_m = as.numeric(NA),
      Depth_to_logger_m = as.numeric(NA),
      Notes = as.character(NA)
    )[0, ]
  }
  return(ctbl)
}