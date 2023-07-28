#' @title Harvest data from another station or table.
#' @description Used to extract data from another table, station, or source
#'  and ...
#' @param station_file Name of the station being processed.
#' @param ext_dir The path of the directory in which to search for the
#'  external data and/or data table.
#' @param ext_station The name (or keyword) of the station from which to extract
#'  data.
#' @param output_dt The output table name where harvested data is extracted too.
#' @param time_interval The desirsed time_tinterval associated with the
#'  extracted data. If this is `NULL` then the table is extracted as is.
#' @param f_params A vector of phenomena name to be extracted from the
#'  harvested data table. If NULL all column names are extracted.
#' @author Paul J. Gordijn
#' @details
#'
#' @export
dt_harvest <- function(
  station_file = station_file,
  output_dt = NULL,
  ext_dir = NULL,
  ext_station = NULL,
  ext_station_table = NULL,
  output_dt_preffix = "dt_",
  output_dt_suffix = NULL,
  f_params = NULL,
  f_summary = NULL,
  time_interval = NULL,
  recurr = TRUE,
  ...) {
  # station_file <- "pipe_data_met/mcp/ipip_room/mcp_vasi_science_centre_aws.ipip"
  # ext_dir <- "."
  # ext_station <- "pipe_data_rainfall/rain_generic/ipip_room/mcp_science centre.ipip"
  # ext_station <- NULL
  # f_params <- c("rain_tot")

  h <- ipayipi::hsf_params(
    f_params = f_params, full_eval = TRUE, f_summary = f_summary,
    ppsi = ppsi)
  
  # temp to read args
  for (z in seq_along(names(args))) {
    assign(names(args)[z], args[[z]])
  }
  # harvest options
  # - whole table as is (time_interval = NULL)
  # - target table structure
  #   - determine harvest phens based on sf table time interval
  # - phens
  #   - determine phens based on harvest data summary and target time_interval
  # - filter date_time values of harvest


  # file and table connection
  # harvest phen checks
  # date and time filtering
  # merging data

  # station file and summary info tables
  if (exists("tmp")) { # tmp passed in pipe process
    sf <- readRDS(tmp)
    f_summary <- sf$f_summary
  }
  if (is.null(f_summary)) {
    sf <- readRDS(station_file)
    sf_names <- names(sf)
    f_summary <- sf[sf_names %ilike% "summary|phens"]
    f_summary$sf_names <- sf_names
    rm(sf)
  } else {
    sf_phens <- f_summary$phens
    sf_dta_summ <- f_summary$data_summary
    sf_names <- f_summary$sf_names
    sf_ti <- time_interval
  }

  # open external file and summary info
  if (is.null(ext_station)) {
    ext_station <- station_file
    ef_names <- sf_names
    ef_dta_summ <- sf_dta_summ
    ef_phens <- sf_phens
    ef_f_summary <- f_summary
  } else {
    ef <- readRDS(ext_station)
    ef_phens <- ef$phens
    ef_names <- names(ef)
    ef_dta_summ <- ef$data_summary
    ef_f_summary <- ef[ef_names %ilike% "summary|phens"]
  }

  # organise the raw data table options
  #  - explicit raw
  #  - generic raw
  #  - other

  # organise phenomena to determine input dt more exactly
  # explicit and generic raw phenomena summaries
  if (input_dt %ilike% "raw") {
    p <- ef_phens[, # filter for generic data table name
      c("phid", "phen_name", "units", "measure", "var_type", "table_name")]
    if (input_dt != "raw") {
      p <- p[table_name == input_dt] # filter for specific raw data table name
    }
  }
  if (input_dt %ilike% "dt_") {
    
  }
  # extract phenomena names from the phenomena table
  if (is.null(f_params) || is.na(f_params)) {
    phens <- unique(ef_phens$phen_name)
  } else {
    if ("f_params" %in% class(f_params)) {
      phens <- unique(f_params$hsf_params$phen_names)
    }
  }

  # filter the phenomena table by the phens
  p <- p[phen_name %in% phens]

  # organise the phenomena table --- this is used to determine
  #  which raw tables to draw from

  # extract phens from raw data and check time intervals
  # harvest data in raw tables
  # - extract phenomenon data and info and sort which tables to choose from
  # harvest data in other (e.g., metadata or dt_ tables) tables
  # - extract assocaited info (if starts with 'dt_')

  # add 'record intervals' to the phen table to determine from which
  # raw data tables to extract data from
  s <- unique(ef_dta_summ, by = "table_name")
  s <- s[, c("record_interval_type", "record_interval", "table_name")]
  sx <- lapply(s$record_interval, ipayipi::sts_interval_name)
  sx <- data.table::rbindlist(sx)
  s <- cbind(s, sx)
  p <- merge(x = p, y = s, all.y = TRUE, by = "table_name")
  agg_time_interval <- ipayipi::sts_interval_name(time_interval)
  if ("event_based" %in% p$record_interval_type) {
    dfft_dta <- as.numeric(
      x = mondate::as.difftime(
        max(ef_dta_summ$end_dttm) - min(ef_dta_summ$start_dttm)),
      units = "secs"
    )
    p <- subset(p, select = -dfft_secs)
    p <- transform(p, dfft_secs = dfft_dta)
  }
  p$agg_intv <- agg_time_interval$dfft_secs
  p$dfft_diff <- p$agg_intv - p$dfft_secs

  # can only aggregate if time intervals are more lengthy than raw data
  if (all(p$dfft_diff < 0)) {
    stop(paste0(agg_time_interval$intv, " has a shorter duration than ",
      "available raw data!", collapse = ""))
  }
  p <- p[dfft_diff >= 0]
  data.table::setorderv(p, cols = c("phid", "phen_name", "dfft_diff"))
  p <- lapply(split.data.frame(p, f = factor(p$phen_name)), function(x) {
    x$decis <- ifelse(x$dfft_diff == min(x$dfft_diff), TRUE, FALSE)
    x <- x[decis == TRUE][1, ]
    invisible(x)
  })
  p <- data.table::rbindlist(p)
  input_dt <- unique(p$table_name)

  # open harvested data
  ef <- readRDS(ext_station)

  # filter by dates

  # generate summary information
  phens_dt <- data.table::data.table(
    phid = p$phid,
    phen_name = p$phen_name,
    units = p$units,
    measure = p$measure,
    var_type = p$var_type,
    record_interval_type = p$record_interval_type,
    orig_record_interval = p$record_interval,
    dt_record_interval = gsub(pattern = " ", replacement = "_",
      x = agg_time_interval$sts_intv)
  )


  # return the results and summary info to be used in the pipe line
  return(list(dt_pro = , main_agg_name = output_dt,
    start_dttm = min(main_agg$date_time), end_dttm = max(main_agg$date_time)))
}