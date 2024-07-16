#' @title Clean hobo rainfall logger data
#' @description Reads Hoboware csv outputs from tipping bucket raingauges and
#'  and performs some basic data cleaning. *The function has a special mode
#'  for which input parameters may vary*.
#' @param input_format Format of the input file. Can be one of two formats:
#'  1. either a csv file which has been exported from hoboware, or 2., *a
#'  special case for reading a rainfall `master file' (`mf') from excel
#'  which has been imported in R using `ipayipi::rain_master()`*.
#' @param input_file The path and file name of the csv file to be read. *If
#'  the `input_format` is 'mf' then a segment (list item) from a master
#'  rainfall file is passed to this parameter.*
#' @param input_file_name Parameter only used *if* input format is 'mf'---
#'  otherwise this parameter should be set to `NULL`. When importing using
#'  the 'mf' format the file_name parameter needs to be set to the excel
#'  file from which rainfall data was extracted. The given value will be
#'  captured as the 'import_file_name' in the 'data_summary' produced for
#'  the import data. Will be appended to metadata.
#' @param false_tip_thresh Numeric field; units in seconds. The time buffer
#'  in seconds around logger interferance events (e.g., logger connections/
#'  downloads) in which any tips (logs) should be considered false. The
#'  default used by SAEON is ten minutes, that is, 10x60=600 seconds.
#' @param tip_value Numeric field; units in millimeters. The amount of
#'  rainfall (in mm) a single tip of the tipping bucket represents. This
#'  defaults so 0.254 mm.
#' @param dt_format The date-time format of the input data. **Take care to
#'  input data time formats**. NB! This may appear different depending on your
#'  computer system and software settings.  The default used is "%x %r" ---
#'  check `?base::strptime` for a list of available formats.
#' @param tz Timezone format of the datetime values. For South Africa we use
#'  the character string "Africa/Johannesburg" or "Africa/Johannesburg" for
#'  short.
#' @param temp Has the hobo logger been recording temperature? Logical
#'  variable. Defaults to FALSE.
#' @param instrument_type The type of instrument wherein the hobo logger was
#'  deployed. For the commonly used *Texus tipping-bucket rainguage* use the
#'  code "txrg".
#' @param ptitle The plot title should only be supplied if there is no plot
#'  title in the hobo file which is being exported, or when using
#'  `rain_master()`. The supplied title will overwrite the export files
#'  plot title. Defaults to `NA`.
#' @keywords hoboware, tipping bucket rain gauge, data cleaning
#' @author Paul J. Gordijn
#' @return list of class "SAEON_rainfall_data". Contains data and logger
#'  metadata.
#' @details This function reads into R a csv output file from Hoboware software.
#'  The interference events, that is, when loggers are, "detached", "attached",
#'  "connected" to a host, and when a "file ends", as exported from Hoboware
#'  are summarized and stored in a seperate table in the output list. *The date-
#'  time format of the files are also standardised and this should match the
#'  csv file being imported to prevent incorrect date-time value conversions.*
#'  The function performs a number of checks on the data to make sure that it is
#'  in the correct format:
#'  1. If there is no plot title supplied in the `ptitle` input parameter then
#'   the function will check for a plot title in the hobo file export which is
#'   being read. The plot title should *only be supplied by the user* for a
#'   specific reason, e.g., using `rain_master()`. The plot title is the
#'   station--instrument name and in hobo file exports is populated by the user
#'   and programed to be in the first line of the first row and column of the
#'   data table (csv or tsv) export.
#'   If there is no plot title in the document the function requires the user to
#'   have entered this value in the function input.
#'  2. The function will check that all the correct columns have been supplied
#'   in the hobo file export. If there is a column missing then the function
#'   will print a warning message in the terminal. If critical columns are
#'   missing or if there are no columns in addition to the critical ones, the
#'   function will terminate. The following columns are considered critical:
#'   "id", "date_time", and "cumm_rain".
#'   To detect the columns in the rainfall file being imported the following
#'   synonyms (search words) are used (note that the temperature column is
#'   optional).
#'
#'   |Column  |Synonyms  |Description |
#'   |--------|----------|------------|
#'   |id      |#         |unique identifier from import file.  |
#'   |date_time|date; dt |The date and time. |
#'   |cumm_rain|rainfall; rain; precipitation | Cummulative rainfall. |
#'   |temp    |temp      | Temperature in degrees celcius (optional).|
#'   |detached|detached  |Detachment of logger from device.|
#'   |attached|attached  |Attachment of logger to device.|
#'   |host    |host      |Connection to a host.|
#'   |file_end|'end of file'|The end of a download file.|
#'
#'   If non-critical columns are missing then they are populated with `NA`
#'   values (except the temperature column which is optional).
#'  3. If the date-time format has been incorrectly specified the resulting
#'   coersion of the date-time column using the `base::as.POSIXct()` can
#'   produce `NA` values. If this occurs the function will terminate and a
#'   message printed in the terminal.
#'
#'  **Note that when using this function in "mf" mode only one station can be
#'  processed at a time.**
#' @export
rain_hobo_clean <- function(
  input_format = ".csv",
  input_file = NULL,
  input_file_name = NULL,
  false_tip_thresh = 10 * 60,
  tip_value = 0.254,
  dt_format = c(
    "Ymd HMOS", "Ymd HMS",
    "Ymd IMOSp", "Ymd IMSp",
    "ymd HMOS", "ymd HMS",
    "ymd IMOSp", "ymd IMSp",
    "mdY HMOS", "mdY HMS",
    "mdy HMOS",  "mdy HMS",
    "mdy IMOSp",  "mdy IMSp",
    "dmY HMOS", "dmY HMS",
    "dmy IMOSp", "dmy IMSp"),
  tz = "Africa/Johannesburg",
  temp = TRUE,
  instrument_type = NA,
  ptitle = NA,
  ...
) {
  if (!input_format %ilike% "." && !input_format %ilike% "mf") {
    stop("Please include the period in input_format")
  }
  ## get input file
  if (input_format == ".csv") {
    hb <- data.table::as.data.table(read.csv(
      input_file, header = FALSE, colClasses = "character"))
    input_file_name <- input_file
  } else {
    hb <- as.data.frame(input_file)
  }

  # declare standard column names
  if (temp == FALSE) {
    hb_names <- c(
      "id",
      "date_time",
      "cumm_rain",
      "temp", # currently not measured at grasslands node
      "detached",
      "attached",
      "host",
      "file_end")
    } else {
      hb_names <- c(
        "id",
        "date_time",
        "cumm_rain",
        "temp", # currently not measured at grasslands node
        "detached",
        "attached",
        "host",
        "file_end")
    }

  ## skip first line?
  # the first line either has the 'plot title' or does not, or there are
  # no column headers we check whether this here
  if (hb[1, 1] == 1 && hb[2, 1] == 2 && length(names(hb)) >= 7) {
    message(paste0(" No column names"))
    stop(paste0(" Failed to process a ", input_file_name, "!"))
  }

  # check for plot title---if the ptitle is NA then a value must be supplied
  # if a value is supplied it is used in preference to any title given in
  # the hobo export
  if (hb[1, 1] %ilike% "title: ") {
    ptitle_found <- substr(hb[1, 1], 13, nchar(hb[1, 1]))
    hb <- hb[-1, ] # remove the first row
  } else if (names(hb)[1] %ilike% "title..") {
    ptitle_found <- substr(names(hb[1]), 16, nchar(names(hb[1])))
    names(hb) <- as.character(hb[1, ])
    hb <- hb[-1, ] # remove the first row
  } else ptitle_found <- NA
  if (is.na(ptitle) && is.na(ptitle_found)) {
    message("No plot title detected or supplied.")
  }
  if (is.na(ptitle) && !is.na(ptitle_found)) {
    ptitle <- ptitle_found
  }

  # if there is no plot title then check whether the first row of data
  # contains the column names. The hash symbol should be the name of
  # column one.
  if (hb[1, 1] == "#") {
    names(hb) <- as.character(hb[1, ]) # assign column names
    hb <- hb[-1, ] # remove the first row
  }

  cr_msg <- padr(core_message =
    paste0("Processing ", basename(input_file_name), ":", collapse = ""),
    wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(-1, 2))
  message(cr_msg)
  ## check column headers and standardize
  # the columns may be named differently according to various download
  # preferences in hoboware and changes in firmware --- this needs to be
  # accounted for when organising the data.
  pats <- c(# search patterns to organise columns
    "#|id", "date|dt", "event|rainfall|rain|precipitation", "temp",
    "detached", "attached", "host",
    "end of file")
  if (!temp) pats <- pats[!pats %in% c("temp")]
  v_name <- c(# final variable name in same seq as pats
    "id", "date_time", "cumm_rain", "temp",
    "detached", "attached", "host",
    "file_end")
  if (!temp) v_name <- v_name[!v_name %in% c("temp")]
  # fill in blanks with available columns where duplicates exist
  for (j in seq_along(pats)) {
    col_id <- names(hb) %ilike% pats[j]
    col_id <- seq_along(names(hb))[col_id]
    if (length(col_id) > 1) {
      for (i in seq_len(length(col_id) - 1)) {
        hb[][[col_id[i]]] <- data.table::fifelse(
          hb[][[col_id[i]]][] == "" | hb[][[col_id[i]]][] == " ",
          hb[][[col_id[i + 1]]], hb[][[col_id[i]]]
        )
      }
      cr_msg <- padr(core_message =
          paste0("Duplicate column detected. ",
          "Filling blanks with detected values: !", pats[j], "!",
            collapse = ""),
        wdth = 80, pad_char = " ", pad_extras = c("!", "", "", "!"),
        force_extras = FALSE, justf = c(1, 0))
      message(cr_msg)
    } else if (length(col_id) == 0) {
      cr_msg <- padr(core_message =
          paste0(" Warning: Missing column(s): !",
            pats[j], "!", collapse = ""),
        wdth = 80, pad_char = " ", pad_extras = c("!", "", "", "!"),
        force_extras = FALSE, justf = c(1, 0))
      message(cr_msg)
    }
  }

  jvar <- sapply(seq_along(pats), FUN = function(z) {
    col_id <- names(hb) %ilike% pats[z]
    col_id <- seq_along(names(hb))[col_id]
    if (length(col_id) > 1) col_id <- col_id[1]
    return(col_id)
  })
  names(jvar) <- v_name
  cols_detected <- sapply(jvar, function(j) {
    is.integer(j) & length(j) != 0
  })

  # stop the function if there are critical columns missing
  if (!all(cols_detected[1:3]) | # the critical columns
      # at least one other column in addition to the critical ones
      length(cols_detected[which(cols_detected == TRUE)]) < 4) {
    stop(paste0(" Critical columns in data missing in: ",
          basename(input_file_name), "!", collapse = ""))
  }

  ## generate table of missing columns
  if (!all(cols_detected)) {
    missing_cols <- lapply(
      seq_along(names(which(!cols_detected))), function(m) {
        dt_missing <- rep(NA, nrow(hb))
        names(dt_missing) <- names(which(!cols_detected))[m]
        dt_missing <- data.table::as.data.table(dt_missing)
        return(dt_missing)
    })
    missing_cols <- do.call(cbind, missing_cols)
    names(missing_cols) <- names(which(!cols_detected))
  } else {
    missing_cols <- NULL
  }

  # extract serial numbers
  lgrs_sns <- names(hb) %ilike%  "LGR.S.N.."
  if (any(lgrs_sns)) {
    names(lgrs_sns) <- seq_along(lgrs_sns)
    ex_lgr <- as.integer(names(lgrs_sns[which(lgrs_sns)])[1])
    logg_sn <- substr(names(hb)[ex_lgr], # logger serial number
      unlist(gregexpr("LGR.S.N..", names(hb)[ex_lgr])) + 9,
      unlist(gregexec("LGR.S.N..", names(hb)[ex_lgr])) + 16)
  } else {
    stop(paste0(" No logger serial number in: ",
      input_file_name, "!", collapse = ""))
  }

  sens_sns <- names(hb) %ilike%  "SEN.S.N.."
  if (any(sens_sns)) {
    names(sens_sns) <- seq_along(sens_sns)
    ex_lgr <- as.integer(names(sens_sns[which(sens_sns)])[1])
    sens_sn <- substr(names(hb)[ex_lgr], # logger serial number
      unlist(gregexpr("SEN.S.N..", names(hb)[ex_lgr])) + 9,
      unlist(gregexec("SEN.S.N..", names(hb)[ex_lgr])) + 16)
  } else {
    stop(paste0(" No sensor serial number in: ",
      input_file_name, "!", collapse = ""))
  }

  # add missing columns to hb and order appropriately
  hb <- data.table::as.data.table(hb)
  names(jvar) <- seq_along(jvar)
  hb_cols <- as.vector(as.integer(unlist(jvar)))
  hb <- hb[, ..hb_cols]
  names_hb <- as.integer(names(jvar[jvar > 0])[!is.na(names(jvar[jvar > 0]))])
  names(hb) <- v_name[names_hb]
  hb <- cbind(hb, missing_cols)
  hb <- hb[, ..v_name]

  hb[detached %ilike% "log", "detached"]  <- "logged"
  hb[attached %ilike% "log", "attached"]  <- "logged"
  hb[host %ilike% "log", "host"]  <- "logged"
  hb[file_end %ilike% "log", "file_end"]  <- "logged"
  hb[, names(hb) := lapply(.SD,
    function(x) replace(x, x == "" | x == " ", NA))]

  # if (!suppressWarnings(
  #     is.na(as.numeric(as.character(hb[1, "date_time"]))))) {
  #   hb$date_time <- as.POSIXct(
  #     as.numeric(hb$date_time) * (60 * 60 * 24) - (60 * 60 * 2),
  #     origin = "1899-12-30",
  #     tz = tz
  #   )
  # } else {
  #   hb <- transform(hb,
  #     date_time = as.POSIXct(hb$date_time, format = dt_format, tz = tz))
  # }
  date_time <- attempt::attempt(lubridate::parse_date_time(x = hb$date_time,
    orders = dt_format, tz = tz))

  if (attempt::is_try_error(date_time)) {
    stop(paste0(" Date time formatting problem: ",
          input_file_name, "!", collapse = ""))
  } else {
    hb$date_time <- date_time
  }

  # format the data columns
  hb <- transform(hb,
    id = as.integer(id),
    cumm_rain = as.numeric(cumm_rain),
    detached = as.factor(detached),
    attached = as.factor(attached),
    host = as.factor(host),
    file_end = as.factor(file_end)
  )
  if ("temp" %in% names(hb)) {
    hb <- transform(hb, temp = as.numeric(hb$temp))
  }

  # make interference table
  f_tip_tbl1 <- subset(hb,
    detached == "logged" |
    attached == "logged" |
    host == "logged" |
    file_end == "logged")
    # logger interfererance table
  logg_interfere  <- subset(f_tip_tbl1,
    select = c("id", "date_time", "detached", "attached", "host", "file_end")
  )
  logg_interfere[is.na(detached) & is.na(attached) & is.na(host) &
      is.na(file_end), "nas"] <- TRUE
  logg_interfere <- unique(logg_interfere)
  logg_interfere$dup1 <-
    duplicated(logg_interfere, by = c("id", "date_time"))
  logg_interfere$dup2 <-
        duplicated(logg_interfere, by = c("id", "date_time"),
          fromLast = TRUE)
  logg_interfere <-
    logg_interfere[nas != TRUE & dup1 != TRUE | dup2 != TRUE,
      -c("nas", "dup1", "dup2"), with = FALSE]

  # data summary table
  start_dt <- min(c(min(hb$date_time)), min(logg_interfere$date_time))
  end_dt <- max(c(max(hb$date_time)), max(logg_interfere$date_time))
  data_summary <- data.table::data.table(
    ptitle_standard = NA,
    location = NA,
    station = NA,
    start_dt = as.POSIXct(start_dt),
    end_dt = as.POSIXct(end_dt),
    instrument_type = instrument_type,
    logger_sn = logg_sn,
    sensor_sn = sens_sn,
    tip_value_mm = tip_value,
    import_file_name = input_file_name,
    ptitle_original = ptitle,
    nomvet_name = as.character(NA)
  )
  # remove the temperature column and generated NA values in the
  # cumm rain column
  if ("temp" %in% names(hb)) {
    hb <- hb[-which(is.na(hb$cumm_rain) & !is.na(hb$temp)), ][
    , -c("temp"), with = FALSE][!is.na(cumm_rain)]
  }

  # make table list
  hb <- hb[, -c("detached", "attached", "host", "file_end")][
    !is.na(cumm_rain)]
  spruced_hobo <- list(data_summary, logg_interfere, hb)
  names(spruced_hobo) <- c(
    "data_summary", "logg_interfere", "tip_data") # "false_tips"
  class(spruced_hobo) <- "SAEON_rainfall_data"
  cr_msg <- padr(core_message =
    paste0("Converted file for dates:"),
    wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(-1, 4))
  message(cr_msg)
  cr_msg <- padr(core_message =
    paste0(start_dt, " --> ", end_dt, collapse = ""),
    wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(-1, 4))
  message(cr_msg)
  return(spruced_hobo)
}
