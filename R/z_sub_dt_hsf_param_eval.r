#' @title Harvest station data
#' @description Sets up a data harvest from a desired station file.
#' @param station_file The path and name of the main station to
#'  which data is being harvested.
#' @param hsf_station The path and name of the station, or a
#'  keyword, to search for the station from which data will be
#'  harvested. _If `NULL` the `hsf_station` is set to
#'  `station_file`, i.e., data will be harvested from the
#'  `station_file`._
#' @param hsf_dir The directory in which to search for the
#'  `hsf_station`.
#' @param hsf_table The name of the table from which to harvest
#'  data. If the keyword "raw" is used data will extracted from
#'  stations 'raw' data.
#' @param time_interval The desired time interval of the `output_dt`.
#'  The full evaluation will extract phenomena from raw data tables
#'  based accordingly.
#' @param phen_names Names of phenomena to extract from the specified
#'  tables. If `NULL` all phenomena possible will be harvested. If the
#'  desired `time_interval` is shorter than the time interval or
#'  frequency of recordings of the data to be harvested, phenomena will
#'  not be harvested.
#' @param recurr Logical. Whether to search recursively in directories
#'  for the `hsf_station`. Parsed to `ipayipi::dta_list()`.
#' @param harvest_station_ext Parsed to `ipayipi::dta_list()`. Defaults
#'  to ".ipip". Must include the period (".").
#' @param prompt Parsed to `ipayipi::dta_list()`. Set to `TRUE` to use
#'  interactive harvest station selection.
#' @param single_out Forces through an interactive process the singling
#'  out of a harvest station. Useful for example where `ipayipi::dta_list()`
#'  returns from than one option.
#' @param full_eval Defaults to `FALSE`.
#' @param f_summary
#' @param ppsi Summary pipe process table for the function. If provided this
#'  data will be used to overwrite similar arguments provided in the function.
#'  This must be provided for the full function evaluation.
#' @param sf
#' @export
hsf_param_eval <- function(
  station_file = NULL,
  hsf_station = NULL,
  harvest_dir = ".",
  hsf_table = "raw",
  time_interval = NULL,
  phen_names = NULL,
  recurr = TRUE,
  harvest_station_ext = ".ipip",
  prompt = FALSE,
  single_out = TRUE,
  full_eval = FALSE,
  f_summary = NULL,
  ppsi = NULL,
  sf = NULL,
  ...
) {
  # partial function evaluation --- returns shorter hsf_param list
  #  full evaluation returns a list of available phenomena and
  #  their phenomena details.
  if (full_eval != TRUE) {
    if (!is.null(hsf_station)) {
      hsfn <- ipayipi::dta_list(input_dir = harvest_dir, recurr = recurr,
        file_ext = ".ipip", wanted = hsf_station,
        unwanted = NULL, prompt = prompt, single_out = single_out)
      hsfn <- file.path(harvest_dir, station_file)
    } else {
      hsfn <- station_file
    }
    hsf_params <- list(
      station_file = station_file,
      hsf_station = hsf_station,
      hsf_table = hsf_table,
      phen_names = phen_names
    )
    f_params <- hsf_params[!sapply(hsf_params, is.null)]
    class(f_params) <- c(class(f_params), "dt_harvest_params")
  } else { # full function evaluation
    if (!is.null(ppsi)) {
      # extract parts of ppsi and assign values in env
      ppsi_names <- names(ppsi)[!names(ppsi) %in% c("n", "f", "f_params")]
      for (k in seq_along(ppsi_names)) {
        assign(ppsi_names[k], ppsi[[ppsi_names[k]]][1])
      }
    }

    # extract parts of f_params and assign values in environment
    hsf_param_names <- names(f_params)
    for (k in seq_along(hsf_param_names)) {
      if (exists(hsf_param_names[k])) {
        assign(hsf_param_names[k],
          f_params[[hsf_param_names[k]]])
      }
    }
    if (is.null(hsf_station) || is.na(hsf_station)) hsf_station <- station_file
    hsfn <- hsf_station
    # open hsf file if different from sf
    if (basename(hsfn) != basename(station_file)) {
      hsf <- attempt::try_catch(expr = readRDS(hsfn), .w = ~stop)
      hsf_names <- names(hsf)
      hsf_summary <- hsf[hsf_names %ilike% "summary|phens"]
    } else {
      hsf_summary <- f_summary
      hsf_names <- sf_names
    }
    # initial phenomena name organisation
    if (is.null(phen_names[1]) || is.na(phen_names[1])) {
      phen_names <- unique(hsf_summary$phens$phen_name)
    }
    if (exists("input_dt")) hsf_table <- input_dt
    # check hsf_table -- name of the input table
    if (hsf_table %ilike% "raw") {
      # filter for generic data table name, 'raw'
      p <- hsf_summary$phens[
        table_name %ilike% "raw" & phen_name %in% phen_names,
        c("phid", "phen_name", "units", "measure", "var_type", "table_name")]
      # filter for specific raw data table name
      if (hsf_table != "raw") {
        p <- p[table_name == hsf_table & phen_name %in% phen_names]
      }
      phen_names <- unique(p$phen_name)
      phen_names <- phen_names[order(phen_names)]
    }
    # add 'record intervals' to the phen table to determine from which
    #  raw data tables to extract data from
    s <- unique(hsf_summary$data_summary, by = "table_name")
    s <- s[, c("record_interval_type", "record_interval", "table_name")]
    sx <- lapply(s$record_interval, ipayipi::sts_interval_name)
    sx <- data.table::rbindlist(sx)
    s <- cbind(s, sx)
    p <- merge(x = p, y = s, all.y = TRUE, by = "table_name")
    agg_time_interval <- ipayipi::sts_interval_name(time_interval)
    if ("event_based" %in% p$record_interval_type) {
      dfft_dta <- as.numeric(x = mondate::as.difftime(
          max(hsf_summary$data_summary$end_dttm) -
            min(hsf_summary$data_summary$start_dttm)),
        units = "secs"
      )
      p <- subset(p, select = -dfft_secs)
      p <- transform(p, dfft_secs = dfft_dta)
    }
    p$agg_intv <- agg_time_interval$dfft_secs
    p$dfft_diff <- p$agg_intv - p$dfft_secs
    # can only aggregate if time intervals are more lengthy than raw data
    if (all(p$dfft_diff < 0)) {
      stop(paste0(time_interval, " has a shorter duration than ",
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
    # summarise p info for actual harvesting
    # - dt_phen summary
    # - dt_harvest parameters
    phens_dt <- data.table::data.table(
      ppsid = paste(dt_n, dtp_n, sep = "_"),
      phid = p$phid,
      phen_name = p$phen_name,
      units = p$units,
      measure = p$measure,
      var_type = p$var_type,
      record_interval_type = p$record_interval_type,
      orig_record_interval = p$record_interval,
      dt_record_interval = gsub(pattern = " ", replacement = "_",
        x = agg_time_interval$sts_intv),
      orig_table_name = p$table_name,
      table_name = output_dt
    )
    hsf_params <- data.table::data.table(
      ppsid = paste(dt_n, dtp_n, sep = "_"),
      station_file = station_file,
      hsf_station = hsf_station,
      hsf_table = hsf_table,
      phen_names = phen_names
    )
    f_params <- list(f_params = hsf_params, phens_dt = phens_dt)
  }
  return(f_params)
}