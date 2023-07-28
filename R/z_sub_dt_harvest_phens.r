#' @title Aggregate organisation for phenomena
#' @description Function to organise a list or a data.table of
#'  aggregation parameters.
#' @export
harvest_phens <- function(
  phen = NULL
) {
  aggs <- ipayipi::aggs(rain_tot = ipayipi::agg_params(
    agg_function = "mean(x)", units = "mm", table_name = "raw_5_mins"))
  # if list format --- convert to table
  if ("f_params" %in% class(aggs)) {
    agg_offset <- aggs$agg_offset
    aggs <- aggs[!names(aggs) %in% "agg_offset"]
    phen_names <- sapply(aggs, function(x) names(x[1]))
    a <- lapply(aggs, function(x) {
      data.table::as.data.table(x[[1]])
    })
    agg_dt <- data.table::rbindlist(a)
    agg_dt$phen_name <- phen_names
  } else if (is.data.frame(aggs) || data.table::is.data.table(aggs)) {
    agg_dt <- aggs
  } else {
    stop("Supply aggregation functions as a list or data.table")
  }
  stnd_agg_cols <- c("phen_name", "output_phen_name", "agg_function",
    "units", "measure", "var_type", "na_ignore", "table_name")
  # check all names are standard
  if (any(!names(agg_dt) %in% stnd_agg_cols)) {
    abn <- names(agg_dt)[!names(agg_dt) %in% stnd_agg_cols]
    stop(paste0(abn, " agg parameters are not recognised"))
  } else {
    agg_dt[, stnd_agg_cols[!stnd_agg_cols %in% names(agg_dt)]
      := rep(NA_character_, .N)]
    data.table::setcolorder(agg_dt, stnd_agg_cols)
  }
  return(list(agg_dt = agg_dt, agg_offset = agg_offset))
}