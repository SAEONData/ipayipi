#' @title Aggregate time series data by a time interval
#' @description This function aggregates 'ipayipi' formatted data by
#'  time periods.
#' @param input_dt Only 'ipayipi' raw data types supported. The function
#'  relies on the data summaries and phenomena tables of 'station files'
#'  for aggregating data.
#' @param output_dt Output table name. If `NULL` then generic names are
#'  used. Preferable to use generic names for pipeline processing.
#' @param time_interval Table parsed by `ipayipi::sts_interval_name()`
#'  desecribing the time interval for data aggregation.
#' @param f_params The special function parameters _see_
#'  `ipayipi::agg_params()` and `ipayipi::p_agg_dt()`.
#' @author Paul J. Gordijn
#' @details If the `input_data` parameter is not supplied then the function
#'  determines which raw data tables to extract data from. When this happens
#'  The function first prioritises continuous raw data with record intervals
#'  closest to the specified `time_interval`. If continuous data is not
#'  available then the function uses discontinuous data. The function stores
#'  which data has been extracted in the station_file object.
#'
#'  _`agg\_f`_
#'  The _`agg\_f`_ parameter can be passed as a `data.frame` or `data.table`,
#'   or as a list that will be coerced to a table. The data table should have
#'   all required (*) fields as a minimum. If the empty fields cannot be
#'   extracted from the station file phenomena summary these are set to
#'   defaut values where possible.
#'  |*`phen_name` |`output_phen_name` |`agg_function` |`units` |`measure` |`var_type` |`na_ignore` |`table_name` |
#'  |------------|-------------------|--------|--------|----------|-----------|-------------|-------------|
#'  | i | i | i | i | i | i | i |
#'  | i + 1 | i + 1 | i + 1 | i + 1 | i + 1 | i + 1 | i + 1 | i + 1 |
#'
#'
#' @export
dt_agg <- function(
  station_file = station_file,
  input_dt = NULL,
  output_dt = NULL,
  time_interval = NULL,
  f_params = NULL,
  f_summary = f_summary,
  ...) {
  # function overview
  # - get data (raw and phen summary) for aggregation [one time_interval per dt_n]
  # - get aggregation functions [agg_eval full]
  # - aggregate data by function
  # class(time_interval)
  # print(time_interval)
  # station_file <- "pipe_data_met/mcp/ipip_room/mcp_vasi_science_centre_aws.ipip"
  # # time_interval <- "year"
  # input_dt = "raw"
  # output_dt = NULL
  # f_params = f_params

  # to do
  #  -- alternate phen system and tabulation thereof
  #  -- sorting out filter dates and adding values to event_based/discnt data

  # read summary tables into memory
  if (is.null(f_summary)) {
    sf_phens <- readRDS(station_file)$phens
    sf_dta_summ <- readRDS(station_file)$data_summary
    sf_names <- names(readRDS(station_file))
  } else {
    sf_phens <- f_summary$phens
    sf_dta_summ <- f_summary$data_summary
    sf_names <- f_summary$sf_names
  }

  # organise the phenomena table --- this is used for agg functions
  if (is.null(f_params)) {
    phens <- unique(sf_phens$phen_name)
    agg_offset <- c("0 secs", "0 secs")
  } else {
    if ("f_params" %in% class(f_params)) {
      f_params <- ipayipi::agg_params(aggs = f_params)
      agg_offset <- f_params$agg_offset
    }
    phens <- unique(f_params$agg_dt$phen_name)
  }
  # extract phens from raw data and check time intervals
  if (input_dt == "raw") {
    p <- sf_phens[phen_name %ilike% paste0(phens, sep = "", collapse = "|")
      , c("phen_name", "units", "measure", "var_type", "table_name")]
    # add 'record intervals' to the phen table to determine from which
    # raw data tables to extract data from
    s <- unique(sf_dta_summ, by = "table_name")
    s <- s[, c("record_interval_type", "record_interval", "table_name")]
    sx <- lapply(s$record_interval, ipayipi::sts_interval_name)
    sx <- data.table::rbindlist(sx)
    s <- cbind(s, sx)
    p <- merge(x = p, y = s, all.y = TRUE, by = "table_name")
    time_interval <- ipayipi::sts_interval_name(time_interval)
    p$agg_intv <- time_interval$dfft_secs
    p$dfft_diff <- p$agg_intv - p$dfft_secs
    # can only aggregate if time intervals are more lengthy than raw data
    if (all(p$dfft_diff < 0)) {
      stop(paste0(time_interval$intv, " has a shorter duration than ",
        "available raw data!", collapse = ""))
    }
    p <- p[dfft_diff >= 0]
    data.table::setorderv(p, cols = c("phen_name", "dfft_diff"))
    p <- lapply(split.data.frame(p, f = factor(p$phen_name)), function(x) {
      x$decis <- ifelse(x$dfft_diff == min(x$dfft_diff), TRUE, FALSE)
      x <- x[decis == TRUE][1, ]
      invisible(x)
    })
    p <- data.table::rbindlist(p)
    input_dt <- unique(p$table_name)
  }
  # --- modify p based on agg_f not being null
  # generate p for tables not in raw data
  if (any(!input_dt %in% sf_names)) {
    stop("Input data not found in station file")
  }

  # check for extant processed data and if so retreive start and end date times
  if (all(!is.null(output_dt) && output_dt %in% sf_names)) {
    sf_output <- readRDS(station_file[[output_dt]])
    filt_t <- c(sf_output[1, ]$date_time, sf_output[.N]$date_time)
    rm(sf_output)
    # need to extend back the start time based on the aggregation interval
    #  ----avoids na values associated with parital date time aggregation
    #  sequences
    filt_t[2] <- lubridate::floor_date(filt_t[2], unit =
      paste(time_interval$dfft, time_interval$dfft_units)) +
        lubridate::as.period(agg_offset[1])
  } else {
    filt_t <- c(NA, NA)
  }
  i_dt <- lapply(input_dt, function(x) {
    if (anyNA(filt_t)) { # extract start and end date-times of data
      sf_raw <- readRDS(station_file)[[x]]
      filt_t <- c(sf_raw[1, ]$date_time, sf_raw[.N]$date_time)
      # account for event based raw data start and end times
      if (
        "event_based" %in% sf_dta_summ[table_name == x]$record_interval_type) {
        filt_t[1] <- data.table::fifelse(
          filt_t[1] > min(sf_dta_summ[table_name == x]$start_dttm),
            min(sf_dta_summ[table_name == x]$start_dttm), filt_t[1])
        filt_t[2] <- data.table::fifelse(
          filt_t[2] > max(sf_dta_summ[table_name == x]$end_dttm),
            max(sf_dta_summ[table_name == x]$end_dttm), filt_t[2])
      }
    }
    # modify the filter start and end date times by the begining
    # extract data and filter by start and end dates
    z <- sf_raw[date_time >= filt_t[1]][date_time <= filt_t[2]]
    z <- subset(z, select = names(z)[
      names(z) %in% c("date_time", p[table_name == x]$phen_name)])
    invisible(list(z, filt_t))
  })
  names(i_dt) <- input_dt
  # extact filt_t date_times
  filt_t <- lapply(i_dt, function(x) x[[2]])

  #create sequence for merging aggregate data sets
  floor_ceiling_p <- paste(time_interval$dfft, time_interval$dfft_units)
  start_dt <- lubridate::floor_date(filt_t[[1]][1], unit = floor_ceiling_p)
  end_dt <- lubridate::ceiling_date(filt_t[[1]][2], unit = floor_ceiling_p)
  main_agg <- data.table::data.table(
    date_time = seq(from = start_dt, to = end_dt, by = floor_ceiling_p)
  )
  data.table::setkey(main_agg, "date_time")

  # add ceiling and floor date_times for aggregation periods
  i_dt <- lapply(i_dt, function(x) {
    x <- x[[1]]
    x$date_time_floor <- lubridate::floor_date(x$date_time,
      unit = floor_ceiling_p) + lubridate::as.period(agg_offset[1])
    x$date_time_ceiling <- lubridate::ceiling_date(x$date_time,
      unit = floor_ceiling_p) - 0.1 + lubridate::as.period(agg_offset[2])
    data.table::setkey(x, "date_time_floor")
    invisible(x)
  })
  # extract aggregation functions if not default
  pp <- ipayipi::p_agg_dt(phens_summary = p)

  # run through the raw data and generate aggregations
  i_dt <- lapply(i_dt, function(x) {
    vn <- names(x)[names(x) %in% pp$phen_name]
    px <- pp[phen_name %in% vn]
    dtx <- lapply(unique(px$f), function(z) {
      cols <- c("date_time", "date_time_floor", "date_time_ceiling",
        px[f %in% z]$phen_name)
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
  i_dt[["main_agg"]] <- main_agg
  i_dt <- Reduce(function(...) merge(..., all = TRUE), i_dt)# merge data tables
  i_dt <- unique(i_dt, by = "date_time")
  data.table::setcolorder(i_dt, c("date_time",
    names(i_dt)[order(names(i_dt))][!names(i_dt) %in% "date_time"]))

  # trim rows with NA values at begining and end
  if (anyNA(i_dt[.N, ])) i_dt <- i_dt[!.N, ]
  if (anyNA(i_dt[1, ])) i_dt <- i_dt[1, ]

  # return the results and summary info to be used in the pipe line
  x <- list(dt_pro = i_dt, main_agg_name = output_dt,
    start_dttm = min(i_dt$date_time), end_dttm = max(i_dt$date_time))
  class(x) <- "processing"
  return(x)
  # for aggregation may need to remove rows with NA values and adjust dates
}