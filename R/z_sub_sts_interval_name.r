#' @title Evaluates the consistency of record intervals
#' @param intv String describing time duration or 'difftime' object.
#' @description Standardises the string and provides a numerical equivalent
#'  to the time period in seconds.
#' @details Function used internally. Note that if the input is in months
#'  the returned time in seconds is based on a 28 day month.
#' @return Tabulated details on the time duration.
#' @export
#' @author Paul J. Gordijn
sts_interval_name <- function(
  intv = NULL,
  ...
) {
  # reference table for standardisation
  time_units <- data.table::data.table(
    unit_difftime = c("secs", "mins", "hours", "days", "months", "years"),
    unit_df_like = c("sec|secs", "min|mins", "hour|hours", "day|days",
    "month|mon|months", "yr|year")
  )
  if (class(intv) == "difftime") {
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
      dfft_secs <- as.numeric(mondate::as.difftime(28, units = "days"),
        units = "secs")
    } else {
      dfft_secs <- as.numeric(mondate::as.difftime(as.numeric(seq_int),
        units = u), units = "secs")
    }
  } else {
    seq_int <- NA
    dfft_secs <- NA
  }
  int_dt <- data.table::data.table(
    intv = intv,
    orig_units = u,
    dfft = as.numeric(seq_int),
    dfft_units = attr(seq_int, "units"),
    dfft_secs = dfft_secs
  )
  return(int_dt)
}