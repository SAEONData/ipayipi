#' @title Imbibe xle file
#' @description Reads xml formatted solonist files into r for 'ipayipi'
#' @details Only continuous (not event type) Solonist data files have been tested with this function. There is a default `data_setup` that is used for reading Solonist xle files. The `XML` package is used to read xml formatted Solonist 'xle' files.
#' @param file_path Recognized time-zone (character string) of the data locale. The default for the package is South African, i.e., "Africa/Johannesburg" which is equivalent to "SAST".
#' @param dt_format The function attempts to work out the date-time format from a vector of format types supplied to this argument. The testing is done via `lubridate::parse_date_time()`. `lubridate::parse_date_time()` prioritizes the tesing of date-time formats in the order vector of formats supplied. The default vector of date-time formats supplied should work well for most logger outputs.
#' @param dt_tz Recognized time-zone (character string) of the data locale. The default for the package is South African, i.e., "Africa/Johannesburg" which is equivalent to "SAST".
#' @param data_setup The solonist default data setup for 'xle' files is stored as package data: `ipayipi::solonist`. This is the only data setup that is in the early stages of testing for Solonist data loggers.
#' @export
#' @author Paul J Gordijn
#' @keywords imbibe data; xml format; solonist
#' #return
imbibe_xml <- function(
  file_path = NULL,
  data_setup = NULL,
  dt_format = c(
    "Ymd HMOS", "Ymd HMS",
    "Ymd IMOSp", "Ymd IMSp",
    "ymd HMOS", "ymd HMS",
    "ymd IMOSp", "ymd IMSp",
    "mdY HMOS", "mdY HMS",
    "mdy HMOS", "mdy HMS",
    "mdy IMOSp", "mdy IMSp",
    "dmY HMOS", "dmY HMS",
    "dmy IMOSp", "dmy IMSp"
  ),
  dt_tz = "Africa/Johannesburg",
  ...
) {
  "%ilike%" <- ":=" <- ".SD" <- NULL

  # Parse xml data to R
  xm <- attempt::attempt(XML::xmlParse(file_path, trim = TRUE))

  # stop imbibe if no data
  if (length(xm[data_setup$data_tree]) == 0) {
    # need error return not stop
  }

  # Extract logger metadata
  #  File info
  xle_fi <- data.table::data.table(trim = TRUE,
    XML::xmlToDataFrame(
      xm[data_setup$log_file_tree], stringsAsFactors = FALSE
    )
  )
  xle_fi[xle_fi == ""] <- NA
  fds <- c("company", "licence", "date", "time", "filename", "created")
  dtb <- lapply(fds, function(x) {
    if (any(names(xle_fi) %ilike% x)) {
      xi <- xle_fi[[names(xle_fi)[names(xle_fi) %ilike% x][1]]]
    } else {
      xi <- NA_character_
    }
    xi <- as.character(xi)
  })
  xle_fi <- data.table::as.data.table(dtb)
  names(xle_fi) <- fds
  xle_fi[is.na(date)] <- ""
  xle_fi[is.na(time)] <- ""
  xle_fi$date_time <- lubridate::parse_date_time(
    x = as.character(paste0(xle_fi$date, " ", xle_fi$time)),
    orders = dt_format, tz = dt_tz
  )
  fds <- c("company", "licence", "date_time", "filename", "created")
  xle_fi <- xle_fi[, fds, with = FALSE]

  # Logger info
  xle_li <- data.table::data.table(XML::xmlToDataFrame(
    xm[data_setup$log_meta_tree], stringsAsFactors = FALSE
  ))
  fds <- c("instrument_type", "model", "state", "serial",
    "battery_l|battery_c", "battery_v", "channel", "firmware"
  )
  dtb <- lapply(fds, function(x) {
    if (any(names(xle_li) %ilike% x)) {
      xi <- xle_li[[names(xle_li)[names(xle_li) %ilike% x][1]]]
    } else {
      xi <- NA_character_
    }
    xi <- as.character(xi)
  })
  fds <- c("instrument_type", "model", "state", "serial", "battery_l",
    "battery_v", "channel", "firmware"
  )
  names(dtb) <- fds
  xle_li <- data.table::as.data.table(dtb)

  # Logger header
  xle_hi <- data.table::as.data.table(XML::xmlToDataFrame(
    xm[data_setup$instrument_info_tree],
    stringsAsFactors = FALSE
  ))
  xle_hi[xle_hi == ""] <- NA_character_
  fds <- c("project_id", "location", "latitude", "longitude", "sample_rate",
    "sample_mode", "event_ch", "event_threshold", "schedule", "start_time",
    "stop_time", "num_log"
  )
  dtb <- lapply(fds, function(x) {
    if (any(names(xle_hi) %ilike% x)) {
      xi <- xle_hi[[names(xle_hi)[names(xle_hi) %ilike% x][1]]]
    } else {
      xi <- NA_character_
    }
    xi <- as.character(xi)
  })
  names(dtb) <- fds
  xle_hi <- data.table::as.data.table(dtb, stringsAsFactors = FALSE)
  fds <- c("start_time", "stop_time")
  xle_hi <- xle_hi[, (fds) := lapply(.SD, function(x) {
    lubridate::parse_date_time(x, orders = dt_format, tz = dt_tz)
    }), .SDcols = fds
  ]
  xle_hi$sample_rate <- as.character(
    readr::parse_number(xle_hi$sample_rate) / 100
  )
  xle_hi$sample_rate <- ipayipi::sts_interval_name(
    paste(xle_hi$sample_rate, "sec")
  )[["sts_intv"]]

  # Get the phenomena (channels) recorded by the logger
  phens <- xm[data_setup$phen_info_tree]
  phensi <- lapply(phens, function(x) {
    # get phen name
    zp <- gsub("[[:blank:]+$]", "",
      XML::xmlValue(x["Identification"]$Identification)
    )
    # Extract channel unit
    unit <- XML::xmlValue(x["Unit"]$Unit)
    pi <- data.table::data.table(phen_name = zp, units = unit)
    return(pi)
  })
  phens <- data.table::rbindlist(phensi)
  # Extract any parameter information embedded in each channel
  phens$offset <- sapply(xm["//Body_xle/*/Parameters"], function(x) {
    xi <- XML::xmlToList(x)[["Offset"]][["Val"]]
    if (is.null(xi)) xi <- "0"
    xi <- readr::parse_number(xi)
    return(xi)
  })
  # finalize the phenomena table
  phens <- data.table::data.table(
    phid = seq_along(phens$phen_name),
    phen_name = phens$phen_name,
    units = phens$phen_unit,
    measure = "no_spec",
    var_type = "no_spec",
    offset = phens$offset,
    sensor_id = NA_character_
  )

  # get xml time series data
  dttree <- data_setup$date_time_tree
  dttm <- sapply(
    dttree, function(x) future.apply::future_lapply(xm[x], XML::xmlValue)
  )
  dttm <- data.table::data.table(dttm)
  dttm <- paste0(dttm[[1]], dttm[[2]], sep = " ")
  dttm <- lubridate::parse_date_time(dttm, orders = dt_format, tz = dt_tz)

  dta <- future.apply::future_lapply(seq_along(phens$phen_name), function(i) {
    tbl <- data.table::data.table(
      sapply(xm[paste0("//Body_xle/Data/Log/ch", i)], XML::xmlValue)
    )
    names(tbl) <- phens$phen_name[i]
    return(tbl)
  })
  dta <- do.call(cbind, dta)
  dta$date_time <- dttm
  dta$id <- seq_len(nrow(dta))
  pn <- c("id", "date_time", phens$phen_name)
  pn <- pn[!is.na(pn)]
  dta <- dta[, pn, with = FALSE]

  # finalize the data_summary
  data_summary <- data.table::data.table(
    dsid = as.integer(1),
    file_format = data_setup$file_format,
    uz_station = xle_hi$project_id,
    location = xle_hi$location,
    station = NA_character_,
    stnd_title = NA_character_,
    start_dttm = xle_hi$start_time,
    end_dttm = xle_fi$date_time,
    logger_type = xle_li[[data_setup$logger_type]],
    logger_title = xle_li[[data_setup$logger_title]],
    logger_sn = xle_li[[data_setup$logger_sn]],
    logger_os = xle_li[[data_setup$logger_os]],
    logger_program_name = xle_fi[[data_setup$logger_program_name]],
    logger_program_sig = NA_character_,
    uz_record_interval_type = "continuous",
    uz_record_interval = xle_hi[["sample_rate"]],
    record_interval_type = NA_character_,
    record_interval = NA_character_,
    uz_table_name = data_setup$table_name,
    table_name = NA_character_,
    nomvet_name = NA_character_,
    file_origin = NA_character_
  )
  ipayipi_data_raw <- list(data_summary = data_summary, raw_data = dta,
    phens = phens
  )
  return(list(ipayipi_data_raw = ipayipi_data_raw, err = FALSE))
}
