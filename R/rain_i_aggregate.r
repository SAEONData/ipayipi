#' @title Summarize rainfall data by multiple time periods
#' @description Reads rainfall data from the `clean_rain_hobo()` function
#'  then aggregates the data by specified time periods.
#' @param input_file Output from `clean_rain_hobo`
#' @param output_pref A string for the file name prefix. The time periods given
#'  in the `aggs` argument will form the suffex. The default preffix used in
#'  the data pipeline is 'agg_'. The 'agg_' preffix must be used for further
#'  processing.
#' @param aggs vector of strings giving the time periods to use for aggregating
#'  the data.
#' @param csv_out If TRUE the function will write a csv to the working
#'  for each set of aggregated data.
#' @param ignore_nas Choose whether to ignore gaps and other `NA` values when
#'  aggregating the data by sum.
#' @param agg_offset A vector of two strings that can be coerced to a
#'  `lubridate` period. These can be used to offset the date-time from
#'  which aggregations are totalled. For example, for rainfall totals estimated
#'  from 8 am to 8pm the `agg_offset` should be set to c(8 hours, 8 hours).
#' @keywords hoboware, tipping bucket rain gauge, data aggregation
#' @export
#' @author Paul J Gordijn
#' @return Returns the "SAEON_rain_data" object with appended rainfall data
#'  aggregations.
#' @details The function uses `data.table` to summarize the data by the time
#'  periods of interest (generated with the help of `lubridate`). The data
#'  processing and writing to csv can be done using parallel processing by
#'  setting the cores argument to greater than one (but not exceeding the
#'  number of cores on the system).
#'  The data will be saved as a 'data.table' in the **ipayipi** rainfall object
#'  with the given `output_pref` --- if this value is changed the processing
#'  of data within the pipeline will be obscured.
#'   
rain_agg <- function(
  input_file = NULL,
  output_pref = "agg_",
  aggs = c("5 mins", "day", "month"),
  csv_out = FALSE,
  ignore_nas = FALSE,
  agg_offset = c("0 secs", "0 secs"),
  ...) {
  # check input data
  if (class(input_file) != "SAEON_rainfall_data") {
    stop("Only spruced rainfall data accepted. Use clean_rain_hobo()")
  }
  if ("rain_data" %in% names(input_file)) {
    # aggregate rainfall data by time intervals
    rnnr <- lapply(aggs, function(x) {
      rn <- input_file$rain_data
      rn_app <- data.table::data.table(
        id = c(0, nrow(rn) + 1),
        date_time = c(input_file$data_summary$start_dt[1],
          input_file$data_summary$end_dt[nrow(input_file$data_summary)]),
        rain_mm = c(0, 0)
      )
      rn <- rbind(rn, rn_app)[order(date_time), ]
      rn$date_time_floor <- lubridate::floor_date(rn$date_time, x) +
        lubridate::as.period(agg_offset[1])
      rn$date_time_ceiling <- lubridate::ceiling_date(rn$date_time, x) - 0.1 +
        lubridate::as.period(agg_offset[2])
      rn <- rn[, rain_mm := sum(rain_mm, na.rm = ignore_nas),
        by = date_time_floor]
      dt_mx <- max(rn$date_time_floor)
      dt_mn <- min(rn$date_time_floor)
      dtsq <- data.table::data.table(
        date_time1 = seq(from = dt_mn, to = dt_mx, by = x))
      rn <- merge(x = dtsq, y = rn, by.x = "date_time1",
        by.y = "date_time_floor", all = TRUE, sort = TRUE)
      rn[which(is.na(rn$rain_mm)), "rain_mm"]  <- 0
      rn$date_time_ceiling <- rn$date_time1 +
        as.numeric(lubridate::duration(x)) - 0.1

      # check if there are problem gaps that should be allocated NA values
      if ("gaps" %in% names(input_file) & !ignore_nas) {
        gaps <- input_file$gaps
        data.table::setkey(gaps, gap_start, gap_end)
        data.table::setkey(rn, date_time1, date_time_ceiling)
        rn1 <- data.table::foverlaps(rn, gaps, nomatch = NA, mult = "all")
        rn1$gap <- data.table::fifelse(
          !is.na(rn1$gid) & rn1$problem_gap == TRUE, TRUE, FALSE)
        rn1$rain_mm <- data.table::fifelse(rn1$gap == TRUE, as.numeric(NA),
          rn1$rain_mm)
        rn1 <- rn1[, names(rn), with = FALSE]
        rn <- rn1
      }
      rn <- rn[, c("date_time1", "rain_mm")]
      data.table::setnames(rn, "date_time1", "date_time")
      rn <- unique(rn, by = "date_time")
      return(rn)
    })
    for (i in seq_along(rnnr)) {
      input_file[[paste0(output_pref, aggs[i])]] <- rnnr[[i]]
    }
    # save as csv's
    if (csv_out) {
      vv <- lapply(seq_along(rnnr), function(x) {
        fn <- paste0(output_pref, "_", names(rnnr[x]), ".csv")
        write.csv(x = rnnr[[x]], file = fn)
        invisible(fn)
      })
      rm(vv)
    }
    invisible(input_file)
  } else {
    cr_msg <- ipayipi::padr(core_message =
      paste0(" No hobo rain_data available for ",
        input_file$data_summary$ptitle_standard[1], collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(-1, 1))
    return(message(cr_msg))
  }
}
