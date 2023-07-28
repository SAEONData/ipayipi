#' @title Ascribe aggregation functions using raw data phenomena tables.
#' @param phens_summary A summary of the phens table of the particular
#'  phenomena that will be aggregated by `ipayipi::dt_agg()`.
#' @param ignore_nas If `NA` values are ignores any periods or time
#'  interval of the data with any NA values will result in an `NA`
#'  aggregation value. Important if missing values have not been
#'  interpolated.
#' @author Paul J. Gordijn
#' @export
agg_param_eval <- function(
  phens_summary = NULL,
  ignore_nas = FALSE,
  ...) {
  p <- phens_summary
  ftbl <- data.table::data.table(
    measure = c("smp", "avg", "tot", "min", "max", "sd"),
    f_continuous_desc = c("mean", "mean", "sum", "min", "max", "mean"),
    f_continuous = c("mean(<>)", "mean(<>)", "sum(<>)", "min(<>)", "max(<>)",
      "mean(<>)"),
    f_factor = c("modal", NA, NA, NA, NA, NA),
    f_circular = c("ipayipi::circular_mean(<>)", "ipayipi::circular_mean(<>)",
      "sum(<>)", "min(<>)", "max(<>)", "mean(<>)")
  )
  p <- merge(x = p, y = ftbl, by = "measure", all.x = TRUE)
  p$f <- data.table::fifelse(!is.na(p$units), p$f_continuous,
    rep(NA_character_, nrow(p)))
  p$f <- data.table::fifelse(p$units %in% c("deg", "degrees", "degree"),
    p$f_circular, p$f)
  p$units <- data.table::fifelse(p$f %ilike% c("circular") & p$units == "deg",
    "degrees", p$units)
  p[f %ilike% c("circular")]$f <- sub(pattern = "<>", replacement =
    paste0('x, u = \'', p[f %ilike% c("circular")]$units, '\'',
      ', ignore_nas = ', ignore_nas), x = p[f %ilike% c("circular")]$f)
  p[!f %ilike% c("circular")]$f <- sub(pattern = "<>", replacement =
    paste0("x, na.rm = ", ignore_nas), x = p[!f %ilike% c("circular")]$f)

  return(p)
}
