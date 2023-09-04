#' @title Aggregate time series data by a time interval
#' @description This function aggregates 'ipayipi' formatted data by
#'  time periods. The requries an evaluated pipeline.
#' @param station_file The file path of the station being assessed.
#' @param dta_in A list of data tables to be aggregated. The tables
#'  must come from the same station, but may have different record intervals.
#' @param f_params Function parameters evaluated by
#'  `ipayipi::agg_param_eval()`.
#' @param phens_dt The phenomena table describing each of the phenomena in the
#'  `dta_in` parameter. This information is used in the aggregation process.
#' @param sf_tmp_fn A temporary file name used for the station file being
#'  processed.
#' @param ppsij Table derived from `ipayipi::pipe_seq()` showing the summarised
#'  function parameters.
#' @param f_summary List of tables with data summaries.
#' @param sf Station file object.
#' @param dt_working List of data tables in the processing pipeline.
#' @param agg_offset Character vector of length two. Strings describe period
#'  of offset from the rounded time interval used to aggregate data. For
#'  example, if aggregating rainfall data from five minute to daily records,
#'  but staggered so that daily rainfall is totalled from 8 am to 8 pm the
#'  next day the `agg_offset` must be set to `c("8 hours", "8 hours")`.
#' @details Function has to work within the `ipayipi` pipeline as parameters
#'  for aggregation are determined during the pipeline evaluation process.
#'
#'  Processing is done whilst keeping a minimum amount of data in memory. The
#'   station file being worked on is saved in a temporary location so if any
#'   errors occur in the processing the original file will not be overwritten.
#'
#'  If an offset on the aggregation is used then the secon value of the offset
#'   is added to the 'date_time' stamp for the data.tables 'date_time' value.
#'  Aggregation defaults are determined by `ipayipi::agg_param_eval()`. Default
#'  aggregation functions are determined by the phenomena measure parameter ---
#'  see the function for more details.
#'
#' @author Paul J. Gordijn
#' @export
#'
dt_agg <- function(
  station_file = station_file,
  dta_in = NULL,
  f_params = NULL,
  phens_dt = NULL,
  sf_tmp_fn = NULL,
  ppsij = ppsij,
  f_summary = NULL,
  sf = NULL,
  dt_working = NULL,
  agg_offset = c("0 sec", "0 sec"),
  ...) {
  "%ilike%" <- ".N" <- "date_time_floor" <- "phen_name" <- "table_name" <-
    "agg_f" <- ".SD" <- NULL

  if ("hsf_dts" %in% names(dt_working)) {
    dta_in <- dt_working[["hsf_dts"]]
  }
  if ("work_dt" %in% names(dt_working)) {
    dta_in <- dt_working[["work_dt"]]
  }
  # extract agg_options
  if (ppsij[.N]$f_params %ilike% "agg_options") {
    agg_options <- gsub(pattern = "agg_options", replacement = "list",
      x = ppsij[.N]$f_params)
    agg_options <- eval(parse(text = agg_options))
    for (i in seq_along(agg_options)) {
      assign(names(agg_options)[[i]], agg_options[[names(agg_options)[[i]]]])
    }
  }
  # filter out phens not included in the aggregation
  dta <- lapply(dta_in, function(x) {
    x <- x[, c("date_time",
      names(x)[names(x) %in% unique(f_params$phen_name)]), with = FALSE]
  })
  # establish record interval info & start and end dttm
  filt_t <- do.call("c", list(
    min(do.call("c", lapply(dta, function(x) min(x$date_time)))),
    max(do.call("c", lapply(dta, function(x) max(x$date_time))))))
  # if the data is discrete the start and end dates of the logger start/ending
  # need to be used, not the first and last event dttms
  if (any(ppsij$time_interval %ilike% "event_based|discnt")) {
    dsmin <- lapply(names(dta), function(x) {
      min(f_summary$data_summary[table_name == x]$start_dttm)
    })
    dsmin <- min(do.call("c", dsmin))
    dsmax <- lapply(names(dta), function(x) {
      max(f_summary$data_summary[table_name == x]$end_dttm)
    })
    dsmax <- max(do.call("c", dsmax))
    filt_t[1] <- min(do.call("c", list(dsmin, filt_t[1])))
    filt_t[2] <- min(do.call("c", list(dsmax, filt_t[2])))
  }
  ti <- ppsij$time_interval[1]
  #create sequence for merging aggregate data sets
  start_dttm <- lubridate::floor_date(filt_t[1], unit = ti) +
    lubridate::as.period(agg_offset[1])
  end_dttm <- lubridate::ceiling_date(filt_t[2], unit = ti) +
    lubridate::as.period(agg_offset[2])
  main_agg <- data.table::data.table(
    date_time = seq(from = start_dttm, to = end_dttm, by = ti) +
      lubridate::as.period(agg_offset[2])
  )
  data.table::setkey(main_agg, "date_time")

  # add ceiling and floor date_times for aggregation periods
  dta <- lapply(dta, function(x) {
    x$date_time_floor <- lubridate::floor_date(x$date_time,
      unit = ti) + lubridate::as.period(agg_offset[1])
    x$date_time_ceiling <- lubridate::ceiling_date(x$date_time,
      unit = ti) - 0.1 + lubridate::as.period(agg_offset[2])
    data.table::setkey(x, "date_time_floor")
    invisible(x)
  })

  # run through the raw data and generate aggregations
  dta <- lapply(dta, function(x) {
    vn <- names(x)[names(x) %in% f_params$phen_name]
    px <- f_params[phen_name %in% vn]
    dtx <- lapply(unique(px$agg_f), function(z) {
      cols <- c("date_time", "date_time_floor", "date_time_ceiling",
        px[agg_f %in% z]$phen_name)
      xz <- subset(x, select = cols)
      xz <- xz[, lapply(.SD, function(x) eval(parse(text = z))),
        by = date_time_floor, .SDcols = cols[!cols %ilike% "date_time"]]
      data.table::setnames(xz, old = "date_time_floor", new = "date_time")
      data.table::setkey(xz, "date_time")
      return(xz)
    })
    dtx <- Reduce(function(...) merge(..., all = TRUE), dtx)# merge data tables
    return(dtx)
  })
  # if missing date_time values merge to continuous sequence
  dta[["main_agg"]] <- main_agg
  dta <- Reduce(function(...) merge(..., all = TRUE), dta)# merge data tables
  dta <- unique(dta, by = "date_time")
  data.table::setcolorder(dta, c("date_time", f_params$phen_name))
  data.table::setnames(dta, old = names(dta), new = c("date_time",
    f_params$phen_out_name))

  # trim rows with NA values at begining and end
  if (anyNA(dta[.N, ])) dta <- dta[!.N, ]
  if (anyNA(dta[1, ])) dta <- dta[1, ]

  # return the results and summary info to be used in the pipe line
  x <- list(dt = dta)
  return(x)
}