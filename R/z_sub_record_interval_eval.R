#' @title Evaluates the consistency of record interval date-time values
#' @param dt vector of POSIXct values with defined time zone.
#' @param dt_format The input date-time format of the time series, e.g., "%y-%m-%d %H:%M:%S". See ?base::strptime() for details.
#' @param dta_in Input data must have the same number of rows as dt. If inconsistent time intervals are detected then the input data will be filtered out and returned.
#' @param remove_prompt Logical. Activate a readline prompt to choose whether or not filter our records from `dta_in` with inconsistent record intervals. If TRUE then the `max_rows` argument is ignored.
#' @param record_interval_type If there is only one data record the this parameter will be used as the default. This must be either "event_based" or "continuous".
#' @param max_rows Maximum length of `dt` vector to use for evaluating the record interval. Defaults to 1000. If `remove_prompt` is TRUE, this argument is ignored.
#' @description This function tests record intervals from a data.table with a time stamp. The test then describes the data as "continuous" for regular time intervals, "event_based" for irregular or discontinuous recordings. A combination of "continuous" and "event_based" is described as "mixed". Note monthly data are considered to be 'continuous'.
#' @details If the record iterval is set to "continuous" then the function checks a series of date-time values for inconsistencies in record intervals. Data records can be removed if the data is provided and the user selects to do so in the `remove_prompt`. For data to be considered 'continuous' 98% of intervals in the data table must be continuous, _and_ more than 98% of date-time stamp second values must equals zero. If between 20 and 98% (and time values include second values other than zero) of intervals are regular then the 'series' is classified as "mixed". If < 20% of intervals are regular, then the series is considered to be "event_based".
#'  There is a special case for monthly time difference intervals. These are handled by the `mondate` package. If seconds and minutes are equal to zero and the difference between sequential date-time values are 28, 29, 30, and 31 days, then the series is described as a 'continuous' monthly date-time series.
#' @return List containing a logical list of 1) the adjusted date-time values
#'  that will not include irregular time values if these have been removed
#'  2) interval checks (whether or not the record interval was continuous, if
#'  in deed the `record_interval_type` was set to 'continuous'), 3) the modal
#'  record interval given as an object of class 'diff time', 4) the record
#'  interval as a diff time character value, the new filtered data with
#'  inconsistent record interval data removed, 6) the filtered date time
#'  values, and 6) the interval type, e.g., "continuous" or "event_based".
#' @export
#' @author Paul J. Gordijn
record_interval_eval <- function(
  dt = NULL,
  dta_in = NULL,
  remove_prompt = FALSE,
  record_interval_type = NULL,
  max_rows = 1000,
  ...
) {
  "second" <- NULL
  # determine record interval
  getmode <- function(v) {
    uniqv <- unique(v)
    uniqv[which.max(tabulate(match(v, uniqv)))]
  }
  #' only work with the full data set if remove_prompt is T
  if (length(dt) > max_rows && !remove_prompt) {
    dtt <- dt[seq_len(max_rows)]
  } else {
    dtt <- dt
  }
  if (length(dtt) < 2) {
    if (record_interval_type %in% c("continuous", "mixed", "event_based")) {
      record_interval_type <- record_interval_type
    } else {
      stop("Undetermined interval type. Only one data record!")
    }
    ri_eval <- list(
      dt = dt,
      interval_checks = NA,
      record_interval_type = record_interval_type,
      record_interval = "discnt",
      record_interval_difftime = NA,
      new_data = dta_in
    )
  } else {
    # monthly data
    if (any(diff(dtt) %in% c(28:31)) &&
        attr(diff(dtt), "units") %in% c("days") &&
        all(data.table::second(dtt) == 0) &&
        all(data.table::minute(dtt) == 0)
    ) {
      record_interval <- mondate::as.difftime(1, units = "months")
      ri_cks <- rep(TRUE, length(dtt))
      record_interval_type <- "continuous"
    } else {
      # other record intervals
      # extract modal record interval
      record_interval <- mondate::as.difftime(getmode(diff(dtt)),
        units = attr(diff(dtt), "units")
      )
      ri_cks <- diff(dtt) == record_interval
      # continuous
      if ((length(ri_cks[ri_cks == TRUE]) / length(ri_cks)) > 0.98 &&
          (sum(second(dtt) %in% 0) / length(dtt)) > 0.98
      ) {
        record_interval_type <- "continuous"
        record_interval <- mondate::as.difftime(getmode(diff(dtt[ri_cks])),
          units = attr(diff(dtt[ri_cks]), "units")
        )
        # mixed
      } else if ((length(ri_cks[ri_cks == TRUE]) / length(ri_cks)) < 0.98 &&
          (length(ri_cks[ri_cks == TRUE]) / length(ri_cks)) > 0.2 &&
          length(dt[ri_cks]) > 4 && any(second(dt) %in% 0)
      ) {
        record_interval_type <- "mixed"
        record_interval <- mondate::as.difftime(getmode(diff(dt[ri_cks])),
          units = attr(diff(dt[ri_cks]), "units")
        )
        # discontinuous
      } else {
        record_interval_type <- "event_based"
        record_interval <- "discnt"
      }
    }
    if (1 %in% which(ri_cks == FALSE)) {
      ri_cks[1] <- TRUE
      ri_cks <- c(FALSE, ri_cks)
    } else {
      ri_cks <- c(TRUE, ri_cks)
    }
    if (all(any(!ri_cks), remove_prompt,
      record_interval_type == "continuous"
    )) {
      message(paste0("Warning! There are inconsistent record intervals!"))
      chosen <- function() {
        n <- readline(prompt = paste0("Would you like to remove the data with",
          " inconsistent time intervals? (Y/n)  ", collapse = ""
        ))
        if (!n %in% c("Y", "n")) chosen()
        if (n == "Y") {
          new_dta <- dta_in[ri_cks, ]
          new_dt <- dtt[ri_cks]
          message("The following data rows were removed!")
          print(dtt[!ri_cks])
          print(dta_in[!ri_cks, ])
        }
        if (n == "n") {
          new_dta <- dta_in
          new_dt <- dtt
        }
        return(list(new_dta = new_dta, new_dt = new_dt))
      }
      dta_dt <- chosen()
      dta <- dta_dt$new_dta
      dt <- dta_dt$new_dt
    } else {
      dta <- dta_in
      dt <- dt
      ri_cks <- rep_len(
        TRUE, length.out = if (is.null(nrow(dta_in))) 0 else nrow(dta_in)
      )
    }
    if (record_interval_type == "event_based") {
      record_interval_chr <- "discnt"
    }
    # make character representation of diff time object
    if (!record_interval %in% c("discnt")) {
      ri <- paste0(as.character(record_interval), " ",
        attr(record_interval, "units")
      )
      record_interval_chr <- ipayipi::sts_interval_name(ri)[["sts_intv"]]
      record_interval_chr <- gsub(" ", "_", record_interval_chr)
    }
    ri_eval <- list(
      dt = dt,
      interval_checks = ri_cks,
      record_interval_type = record_interval_type,
      record_interval = record_interval_chr,
      record_interval_difftime = record_interval,
      new_data = dta
    )
  }
  return(ri_eval)
}
