#' @title Evaluates the consistency of record intervals
#' @param intv String describing time duration or 'difftime' object. If
#'  a string the string should begin with an interger and then the standard
#'  inverval period. Recognised interval periods are described in the details.
#' @description Standardises the string and provides a numerical equivalent
#'  to the time period in seconds.
#' @details Function used internally. Note that if the input is in months
#'  the returned time in seconds is based on a 28 day month. Also the
#'  seconds in a year are estimated based on a `lubridate` year period.
#'  These abviously differ depening on the exact period the interval covers,
#'  e.g., a leap year.
#'
#'  _Table_ of recognised interval string inputs.
#'  |Recognised inputs | Synonums |
#'  |------------------|----------|
#'  |"secs" |"sec|secs" |
#'  |"mins" |"min|mins" |
#'  |"hours" |"hour|hours|hr" |
#'  |"days" |"day|days" |
#'  |"weeks" | "wk|week" |
#'  |"months" |"month|mon|months|monthly" |
#'  |"years" |"yr|year" |
#'
#' @return Tabulated details on the time duration.
#' @export
#' @author Paul J. Gordijn
sts_interval_name <- function(
  intv = NULL,
  ...
) {
  "%ilike%" <- NULL
  if (is.null(intv)) intv <- NA
  # reference table for standardisation
  time_units <- data.table::data.table(
    unit_difftime = c("secs", "mins", "hours", "days", "weeks", "months",
      "years"
    ),
    unit_df_like = c("sec|secs|seconds", "min|mins|minutes",
      "hour|hours|hrs|hourly", "day|days|daily|daily",
      "wks|weeks|weekly", "months|mons|months|monthly",
      "yrs|years|yearly|annually"
    )
  )
  if ("difftime" %in% class(intv)) {
    intv <- paste0(intv, "_", attr(intv, "units"))
  }
  if (length(grep("_", intv)) > 0) s <- "_" else s <- " "
  tu <- as.numeric(substr(intv, 1, unlist(gregexpr(s, intv))[1] - 1))
  if (is.na(tu)) tu <- 1
  u <- substr(intv, unlist(gregexpr(s, intv))[1] + 1, nchar(intv))
  u <- time_units[time_units$unit_df_like %ilike% u]$unit_difftime[1]
  if (!is.na(u)) {
    seq_int <- mondate::as.difftime(as.numeric(tu), units = u)
    if (u %in% c("months")) {
      dfft_secs <- as.numeric(
        mondate::as.difftime(28, units = "days"), units = "secs"
      ) * tu
    }
    if (u %in% "years") {
      dfft_secs <- as.numeric(
        lubridate::as.period("year"), units = "secs"
      ) * tu
    }
    if (!u %in% c("months", "years")) {
      dfft_secs <- as.numeric(
        mondate::as.difftime(as.numeric(seq_int), units = u), units = "secs"
      ) * tu
    }
  } else {
    seq_int <- NA
    dfft_secs <- as.numeric(NA, units = "secs")
    if (intv %in% "discnt" || intv %in% "dicnt") intv <- intv
  }
  int_dt <- data.table::data.table(
    intv = intv,
    orig_units = u,
    sts_intv = NA_character_,
    dfft = as.numeric(seq_int),
    dfft_units = attr(seq_int, "units"),
    dfft_secs = dfft_secs
  )
  if (any(is.na(intv), all(is.na(u), !intv %in% "discnt"))) {
    intv <- "NA or NULL"
    stop(paste0("The time interval (", intv, ") for data aggregation is not ",
      "recognised by ipayipi::sts_interval_name"
    ))
  }
  if (intv %in% "discnt") {
    int_dt$sts_intv <- intv
  } else {
    int_dt$sts_intv <- paste(int_dt$dfft, int_dt$dfft_units)
    # round up standard to next time unit if possible
    #' seconds to minutes
    if (all(int_dt$dfft_secs / 60 == round(int_dt$dfft_secs / 60, digits = 0),
      int_dt$dfft_units %in% "secs", int_dt$dfft_secs / 60 > 60
    )) {
      int_dt$dfft_units <- "mins"
      int_dt$dfft <- as.numeric(
        int_dt$dfft_secs / 60, units = int_dt$dfft_units
      )
    }
    #' minutes to hours
    if (all(int_dt$dfft_secs / 60 == round(int_dt$dfft_secs / 60, digits = 0),
      int_dt$dfft_units %in% "mins", int_dt$dfft_secs / 60 > 60
    )) {
      int_dt$dfft_units <- "hours"
      int_dt$dfft <- as.numeric(
        int_dt$dfft_secs / 60 / 60, units = int_dt$dfft_units
      )
    }
    #' hours to days
    if (all(int_dt$dfft_secs / 60 / 60 / 24 ==
        round(int_dt$dfft_secs / 60 / 60 / 24, digits = 0),
      int_dt$dfft_units %in% "hours", int_dt$dfft_secs / 60 / 60 / 24 > 1
    )) {
      int_dt$dfft_units <- "days"
      int_dt$dfft <- as.numeric(
        int_dt$dfft_secs / 60 / 60 / 24, units = int_dt$dfft_units
      )
    }
    #' days to weeks
    if (all(int_dt$dfft_secs / 60 / 60 / 24 / 7 ==
        round(int_dt$dfft_secs / 60 / 60 / 24 / 7, digits = 0),
      int_dt$dfft_units %in% "days",
      int_dt$dfft_secs / 60 / 60 / 24 / 7 > 1
    )) {
      int_dt$dfft_units <- "weeks"
      int_dt$dfft <- as.numeric(
        int_dt$dfft_secs / 60 / 60 / 24 / 7, units = int_dt$dfft_units
      )
    }
    int_dt$sts_intv <- paste0(int_dt$dfft, " ", int_dt$dfft_units)
  }
  return(int_dt)
}