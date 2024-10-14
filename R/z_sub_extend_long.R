#' @title Generate date-time data for sequence gaps
#' @description Function to generate time-series data tables with 'date_time' sequences in a list of non-overlapping data sets.
#' @param data_sets List of time-series data sets with a 'date_time' column.
#' @param intra_check Logical. If `TRUE` the function will join each dataset to a continuous date_time sequence. If `FALSE`, only the gaps between listed datasets will be made continuous using the standardised record interval `ri`.
#' @param ri Standardised record interval. Function will not process event-based/'discontinuous' ('discnt') data; requires a regular time-interval character string standardised by `ipayipi::record_interval_eval()`.
#' @export
#' @author Paul J. Gordijn
dttm_extend_long <- function(
  data_sets = NULL,
  intra_check = FALSE,
  ri = NULL,
  ...
) {
  "dttm" <- "date_time" <- NULL
  # check the record interval
  ri <- ipayipi::sts_interval_name(ri)[["sts_intv"]]
  # remove data sets with no rows
  dts <- data_sets[sapply(data_sets, function(x) nrow(x) > 0)]
  #order data sets by time
  dts <- dts[order(sapply(dts, function(x) max(x$date_time)))]
  # for continuous data make sure date sequence is continuous
  if (!"discnt" %in% ri && intra_check) {
    dts <- future.apply::future_lapply(dts, function(x) {
      col_order <- names(x)
      start_dttm <- min(x$date_time)
      end_dttm <- max(x$date_time)
      start_dttm <- lubridate::round_date(start_dttm, unit = ri)
      end_dttm <- lubridate::round_date(end_dttm, unit = ri)
      dt_seq <- seq(from = start_dttm, to = end_dttm, by = ri)
      dt_seq <- data.table::data.table(dttm = dt_seq)
      dt_seq <- dt_seq[order(dttm)]
      x$date_time <- lubridate::round_date(x$date_time, unit = ri)
      x <- merge(x = dt_seq, y = x, by.x = "dttm", by.y = "date_time",
        all.x = TRUE, all.y = FALSE
      )
      x <- x[, !names(x)[names(x) %in% "date_time"], with = FALSE]
      data.table::setnames(x, old = "dttm", new = "date_time")
      data.table::setcolorder(x, neworder = col_order)
      return(x)
    })
  }
  l <- length(dts)
  dtsnd <- lapply(seq_along(dts), function(i) {
    if (all(i > 2, i > 1, i < l, !"discnt" %in% ri)) {
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
  dtsnd <- data.table::rbindlist(c(dtsnd, dts), use.names = TRUE, fill = TRUE)
  dtsnd <- dtsnd[order(date_time)]
  return(dtsnd)
}
