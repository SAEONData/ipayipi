#' @title Extend date time series data
#' @description Internnal function to generate time series data tables to
#'  fill in missing date time values in a list of non-overlapping data sets
#'  with their date and time value in a column named "date_time".
#' @export
#' @author Paul J. Gordijn
dttm_extend_long <- function(
  data_sets = NULL,
  ri = NULL,
  remove_overlap = FALSE,
  ...
) {
  # remove data sets with no rows
  dts <- data_sets[sapply(data_sets, function(x) nrow(x) > 0)]
  #order data sets by time
  dts <- dts[order(sapply(dts, function(x) max(x$date_time)))]
  l <- length(dts)
  dtsnd <- lapply(seq_along(dts), function(i) {
      if (all(i > 2, i > 1, i < l, !"event_based" %in% ri)) {
        max_i1 <- min(dts[[i]]$date_time)
        min_i2 <- min(dts[[i + 1]]$date_time)
        dt_seq <- seq(from = max_i1, to = min_i2, by = ri)
        dt_seq <- dt_seq[2:(l - 1)]
        dt_seq <- data.table::data.table(date_time = dt_seq)
      } else {
        dt_seq <- NULL
      }
    return(dt_seq)
  })
  dtsnd <- data.table::rbindlist(dtsnd)
}
