#' @title Barometric compensation
#' @description Use barometric data to compensate for atmospheric pressure
#' induced variability in pressure transducer measurements.
#' @details This function, being designed as part of the package pipeline,
#' follows the following steps.
#'
#'  * Connect to the desired baro logger file. The connection will be made
#' automatically if a previous entry has been made in the log_baro_log table.
#' Otherwise, if working within the designed pipeline, the function will extract
#' the barologger information from the rdta_log file in the working directory.
#'  * Optional setting of start and end dates which you would like to
#' compensate. These dates will be used to query data from the barologger file.
#' If left NULL the min max date_time ranges of the respective level file and
#' barologger data file will be used - see below.
#'  *  Set the start and end dates to match the time ranges of input data
#'  and/or the customised input dates.
#'  * Before barometrically compensating the data, the level data is pushed
#'  to a new table where transformations/calculations are performed. The
#'  date-time values of the level data are used as an id column in querying the
#'  barometric data. If in this query there are more than a user specified
#'  limit of NA values (e.g. resulting from mismatchning date-time values) the
#'  query will stop. The user will have to manually correct the mismatching
#'  date-time values using a floor function or something similar to minimise
#'  the discrepancy. For applying this function automatically to all RDS files
#'  in the solonist_dta folder, the batch_baro_comp function can be used.
#' @md
#' @param input_file Required input level file - either as character or object
#' in the R environment.
#' @param baro_file Optional specificed barologger file. If not specified then
#' the level file is checked for the last used baro file. Or the file is
#' queried from the rdta_log if this option is set to TRUE below. Can be either
#' character or object in the R environment.
#' @param pull_baro If TRUE the function will automatically search for baro data
#' in the working directory. The transfer log will be opened and the barologger
#' therein used. If FALSE then a baro_file must be specified.
#' @param overwrite_baro Overwrite previous barologger data in the input_file.
#' If users have cleaned (removed outliers etc) the baro data in the level file
#' then this should be set to F. Note that when overwriting, data gaps can
#' still be infilled - of potential duplicates, the originals will only be
#' overwritten if overwrite_baro is set to TRUE.
#' @param overwrite_comp Overwrite and recalculate compensated data in the
#' input_file.
#' @param wk_dir If reading RDS files, input directory is required.
#' @param InasRDS Is the input format RDS (the alternate option is an object in
#' the R env).
#' @param rdta_log If true the rdta.log will be pulled from the working
#' directory and used to query the barologger name by serial number for the
#' baro log.
#' @param start_t Optional query dates for baro data extraction. Format
#' strictly "%Y-%m-%d %H:%M:%OS".
#' @param end_t See __start_t__.
#' @param join_tolerance To join the barologger and levellogger data an
#' interval join is performed. With greater tolerance, the time interval for
#' the join increases however, the tolerance will automatically be set to be
#' 90 % of the smallest sample rate which has been used for a borehole or site.
#' @param na_thresh When joining the barologger and levellogger data, NAs are
#' coerced if Date_time stamps don't match. A warning is generated if the
#' number of NA's exceeds this threshold.
#' @param overwrite_t_bt_level Overwrite the transformed baro data? If TRUE,
#' the transformed bt level data will be overwritten. If False, this data will
#' not be overwritten.
#' @param dt_format The function guesses the date-time format from a vector of
#'  format types supplied to this argument. The 'guessing' is done via
#'  `lubridate::parse_date_time()`. `lubridate::parse_date_time()` prioritizes
#'  the 'guessing' of date-time formats in the order vector of formats
#'  supplied. The default vector of date-time formats supplied should work
#'  well for most logger outputs.
#' @param dt_tz Recognized time-zone (character string) of the data locale. The
#'  default for the package is South African, i.e., "Africa/Johannesburg" which
#'  is equivalent to "SAST".
#' @export
#' @author Paul J Gordijn
#' @keywords compensation; barometric
#' @return Screen print of the solonist logger file that was compensated.
#' @examples
#' ##' This function is used within the 'batch_baro' function. It can be used
#' #' to compensate individual level logger files as well.
#' #' An example will be provided once data has been added to the package.
gw_baro_comp <- function(
  input_file = NULL,
  baro_file = NULL,
  pull_baro = FALSE,
  overwrite_baro = FALSE,
  overwrite_comp = FALSE,
  wk_dir = ".",
  InasRDS = TRUE,
  rdta_log = TRUE,
  start_t = NULL,
  end_t = NULL,
  join_tolerance = NULL,
  na_thresh = 5,
  overwrite_t_bt_level = FALSE,
  dt_format = c(
    "Ymd HMS", "Ymd IMSp",
    "ymd HMS", "ymd IMSp",
    "mdY HMS", "mdy IMSp",
    "dmY HMS", "dmy IMSp"),
  dt_tz = "Africa/Johannesburg",
  ...) {
  #' 2 Import data files - baro and water level
  #' 2.1 rds import
  if (InasRDS == TRUE) {
    #' Try and read in the rds file
    input_file <- gsub(pattern = ".rds", replacement = "", x = input_file)
    working <- attempt::try_catch(
      readRDS(file.path(wk_dir, paste0(basename(input_file), ".rds"))),
        .e = ~ stop(.x),
        .w = ~ warning(
          paste0(input_file, " was not found!"),
        .x
      )
    )
    if (attr(working$log_t$Date_time, "tz") == "" ||
      attr(working$log_t$Date_time, "tzone") == "") {
      attr(working$log_t$Date_time, "tz") <- dt_tz
      attr(working$log_t$Date_time, "tzone") <- dt_tz
    }
    #' Read in rdta_log file - used to query the barologger file name
    if (rdta_log == TRUE) {
      if (file.exists(file.path(wk_dir, "rdta_log.rds"))) {
        rdta_log_file <- readRDS(file.path(wk_dir, "rdta_log.rds"))
      } else {
        stop("There is no rdta_log file in the working directory.")
      }
    }

    #' Get and organise barologger data
    #'  if pull_baro = TRUE then get the transfer log file.
    #'  This RDS file has a column for specifying what barologger file
    #'  should be used. If there is none specified, then the last
    #'  barologger file that was used in the level file will be pulled.
    #'  the level file R object
    if (pull_baro == TRUE) {
      #' First we check the rdta_log for a baro file name.
      if (rdta_log == TRUE) {
        rdta_log_file$lev_file <-
          paste0(rdta_log_file$Location, "_", rdta_log_file$Borehole)
        rdta_log_file$Baro_name <- as.character(rdta_log_file$Baro_name)
        baro_file <-
          subset(rdta_log_file, lev_file == input_file)[, "Baro_name"]
        if (is.na(baro_file)) {
          message(paste0(
            "Please specify a barologger file that ",
            "can be used for the barometric compensation of ",
            input_file
        ))
        stop("Barologger file missing")
        }
        baro <- readRDS(file.path(
          wk_dir, paste0(basename(as.character(baro_file)), ".rds")))
      }
      if (is.na(baro_file)) {
        #' get the last baro_file name that was used.
          bb <- length(working$log_baro_log$Baro_name)
        if (bb > 0) {
          baro_file <- working$log_baro_log$Baro_name[bb]
          rm(bb)
        } else {
        stop(paste0(
          "There is no barologger history in the input",
          "file to trace the correct barologger file for pulling."
        ))
        }
      }
    }
    #' If pull_baro == F then check whether we have a user declared baro_file
    if (pull_baro == FALSE) {
      if (is.null(baro_file)) {
        message(paste0(
          "Please specify a barologger ",
          "file that can be used for the barometric ",
          "compensation of ", input_file
        ))
        stop("Barologger file missing")
      } else {
        baro_file <- gsub(pattern = ".rds", replacement = "", x = baro_file)
      }
    }
    #' Read in the barologger data
    baro_file <- gsub(pattern = ".rds", replacement = "", x = baro_file)
    baro <- readRDS(file.path(wk_dir, paste0(basename(baro_file), ".rds")))
    # check tz attributes -- the barologger data has tz
    attr(baro$log_data$Date_time, "tzone")
  }
  #' 2.2 r env data import option
  if (InasRDS == FALSE) {
    if (!is.null(input_file)) {
      working <- input_file
    } else {
      stop("Please specify a barologger file input.")
    }
    if (!is.null(baro_file)) {
      baro <- baro_file
    } else {
      stop("Please specify a barologger file input.")
    }
    # check level file tz
    attr(working$log_t$Date_time, "tz")
    attr(working$log_data$Date_time, "tz")
  }
  cr_msg <- padr(core_message = paste0(" Opened: ", input_file, " and ",
    baro_file, " ", collapse = ""), wdth = 80, pad_char = "-",
    pad_extras = c("|", "", "", "|"), force_extras = TRUE,
    justf = c(-1, 3))
  message(cr_msg)
  #' Check whether data classes have been defined correctly
  if (class(working) != c("level_file")) {
    cr_msg <- padr(core_message = "The level file class is incorrect",
      pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = TRUE, justf = c(-1, 3), wdth = 80)
    message(cr_msg)
    cr_msg <- padr(core_message = "Please use the pipeline design class",
      pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = TRUE, justf = c(-1, 3), wdth = 80)
    stop("Please use the pipeline design class")
  }
  if (class(baro) != c("baro_file")) {
    cr_msg <- padr(core_message = "The barologger file class is incorrect",
      pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = TRUE, justf = c(-1, 3), wdth = 80)
    message("The barologger file class is incorrect")
    cr_msg <- padr(core_message = "Please use the pipeline design class",
      pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = TRUE, justf = c(-1, 3), wdth = 80)
    stop("Please use the pipeline design class")
  }

  #' Check whether there is baro and working data in the chosen files
  if (nrow(baro$log_data) == 0) stop("No baro logger data")
  if (nrow(working$log_data) == 0) stop("No level logger data")

  #' 3 Append and join data
  #' The approach taken to append and join data is based on a number of
  #' criteria.
  #'  Users can specify whether they would like to overwrite the existing
  #'  barologger data in the level file. This would remove any edits they
  #'  have made in cleaning the data. However, this would be desirable if
  #'  you needed to fill gaps in the data. An interval cleaning function
  #'  would therefore be desirable to clean intervals automatically.
  #
  #' This section takes the following steps
  #'  3.1. Get: start and end dates
  #'   Aim: Get the start and end dates of the data to be added from the
  #'         input baro data.
  #'         This is based on the data ranges of the input baro data,
  #'         the water level data in the level file, optional user specified
  #'         dates and date logic e.g. is the end date older than the start
  #'         date?
  #'        In this section a baro log is also created so that the baro data
  #'        transferred to the level file can be traced.
  #'     3.1.1 - get start & end dates of the input baro data
  #'     3.1.2 - get start & end dates of the baro data in the level logger
  #'             file
  #'     3.1.3 - generate baro name log of the level logger file
  #'     3.1.4 - get the start and end dates of the water
  #'             level log in the input level file
  #'     3.1.5 - use user optional input dates to trim start and end dates
  #'             of start and end times
  #'     3.1.6 - trim the start and end dates by the range of date time in
  #'             the input level data (1.4)
  #'     3.1.7 - trim start and end dates by the range of date time in the
  #'             input baro data file
  #'    3.1.8 - veryify start and end dates
  #'  3.2. Filter: Start -> End baro and level data
  #'     3.2.1 - filter baro data in the level file
  #'     3.2.2 - filter input baro log
  #'     3.2.3 - Filter input baro data
  #'  3.3. Merge data
  #'     3.3.1 - Merge baro data
  #'     3.3.2 - Remove duplicates whereever necessary
  #'     3.3.3 - Generate an updated baro log for the level file
  #'  3.4 Join baro and level data
  #'     3.4.1 - remove temperature column in baro table
  #'     3.4.2 - Create a temporary baro-level join file
  #'     3.4.3 - Check for join errors
  #'  3.5 Joined data --> level_file
  #'  4.1 Compensation
  #'     4.1.1 - Coerce NA"s to zero"s for calculations
  #'     4.1.2 - Query baro model offset
  #'     4.1.3 - Calculate barometrically compensated water level
  #' 5 SAVE DATA

  cr_msg <- padr(core_message = paste0(" Compensating: ", input_file,
      "... ", collapse = ""), pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = TRUE, justf = c(-1, 3), wdth = 80)
  message(cr_msg)
  #' 3.1 Get: start and end dates
  #'  *3.1.1 - Get start and end date times of the new input baro data
  baro_in_start_temp <- min(baro$log_data$Date_time, na.rm = TRUE)
  baro_in_end_temp <- max(baro$log_data$Date_time, na.rm = TRUE)

  #'  *3.1.2 - get start & end dates of the baro data in the level logger file
  #' If there is barologger data in the level logger file
  #'  retreive the start and end dates.
  if (nrow(working$log_baro_data) > 0) {
    #' var to indicate whether there is baro data in the level file
    level_baro <- TRUE
    baro_lev_start_temp <- min(working$log_baro_data$Date_time, na.rm = TRUE)
    baro_lev_end_temp <- max(working$log_baro_data$Date_time, na.rm = TRUE)

    #' *3.1.3 - Add metadata via data.table overlap join query to
    #'          the level logger barologger info - this will be seperated
    #'          later into a log and data file.
    lev_baro_log <- working$log_baro_log
    lev_baro <- working$log_baro_data
    cols <- c(colnames(lev_baro), "Baro_SN", "Baro_Md")
    data.table::setDT(lev_baro)
    data.table::setDT(lev_baro_log)
    lev_baro[, dummi := Date_time]
    data.table::setkey(lev_baro_log, Start, End)
    data.table::setkey(lev_baro, Date_time, dummi)
    lev_baro <- data.table::foverlaps(lev_baro, lev_baro_log,
        nomatch = 0, mult = "first")
    lev_baro <- lev_baro[, cols, with = FALSE]
    lev_baro[, orig := as.logical(TRUE)]
  }
  #' F means that there is no baro data in the level logger file R object
  if (nrow(working$log_baro_data) == 0) {
      lev_baro <- working$log_baro_data
      lev_baro[, ":=" (Baro_SN = as.character(),
      Baro_Md = as.character(), orig = as.logical())]
      level_baro <- FALSE
  }

  #' *3.1.4 - get the start and end dates of the water level
  #'          log in the input level file
  start_temp <- min(working$log_data$Date_time, na.rm = TRUE)
  end_temp <- max(working$log_data$Date_time, na.rm = TRUE)

  #' *3.1.5 - use user optional input dates to trim
  #'          start and end dates of start and end times
  #' If no start and end is defined use min and max date times...
  if (!is.null(start_t)) {
    start_t <- lubridate::parse_date_time(x = as.character(start_t),
      orders = dt_format, tz = dt_tz)
  } else {
    start_t <- start_temp
  }

  if (start_t < start_temp) start_t <- start_temp

  if (!is.null(end_t)) {
    end_t <- lubridate::parse_date_time(x = as.character(end_t),
      orders = dt_format, tz = dt_tz)
  } else {
    end_t <- end_temp
  }

  #' *3.1.6 - trim the start and end dates by the range of date time
  #'          in the input level data (3.1.4)
  if (end_t > end_temp) end_t <- end_temp

  #' *3.1.7 - trim start and end dates by the range of date time
  #'           in the input baro data file
  #' Trim the start and end date time values if these extend beyond the
  #'  barologger data date time values
  start_temp <- baro_in_start_temp
  end_temp <- baro_in_end_temp
  if (start_t < start_temp) start_t <- start_temp
  if (end_t > end_temp) end_t <- end_temp

  #' *3.1.8 - veryify start and end dates
  #' Check whether the start and end date times make sense
  #'  --continue with compensation if so...
  if (start_t < end_t || end_t > start_t) {
    #' 3.2 Filter: Start -> End baro and level data
    #' Now that the start and end dates have been checked we can filter the
    #'  data sets by the adjusted start and end date-times before appending
    #'  these.
    #'  For processing with overwrite = TRUE, the data in the level files must
    #'  be removed (filtered) first. If overwrite is false, the data can be
    #'  merged and the non-distinct records removed.

    #' *3.2.1 - filter baro data in the level file
    #'  First work with the input baro data.
    #'  If there is baro data in the level logger file [level_baro == TRUE]
    #'  this needs to be filtered if overwrite_baro is true.
    if (level_baro == TRUE) {
      #' level baro was a variable defined above indicating the presence of
      #' baro data in the level file.
      if (overwrite_baro == TRUE) {
        #' Filter the temporary baro log data generated in the query above
        #'  lev_baro is the baro data in the input (working) level file
        lev_baro <- subset(lev_baro,
          subset = Date_time < start_t | Date_time > end_t
        )
      }
    }

    #' Now create a joined log and data file for the input barologger data
    baro_baro <- baro$log_data
    data.table::setDT(baro_baro)
    baro_log_info <- baro$xle_LoggerInfo
    data.table::setDT(baro_log_info)
    baro_log_info <- baro_log_info[,
      c("Start", "End", "Serial_number", "Model_number")][
        order(Start, End)]
    baro_baro <- baro_baro[, dummi1 := Date_time][order(Date_time)]
    data.table::setkey(baro_baro, Date_time, dummi1)
    data.table::setkey(baro_log_info, Start, End)
    baro_baro <- data.table::foverlaps(baro_baro, baro_log_info, nomatch = 0,
      mult = "first")
    baro_baro <- baro_baro[, c(
        "Date_time", "level_kpa", "temperature_degc",
        "Serial_number", "Model_number"
    )]
    baro_baro <- data.table::setnames(baro_baro,
      new = c("Baro_SN", "Baro_Md"),
      old = c("Serial_number", "Model_number")
    )
    #' baro_baro = the data to import into the level/working file
    #'  this is therefore not original
    baro_baro <- baro_baro[, orig := as.logical(FALSE)]

    #' *3.2.2 - filter input baro data
    #' Filter the input baro data by the start and end dates in the input.
    baro_baro <- subset(baro_baro,
      subset = Date_time >= start_t & Date_time <= end_t)

    #' *3.2.3 - Append filtered baro data
    #' Now append the actual baro data into the working level file
    #'  If there is baro data in the level file and we don't want to
    #'  overwrite it
    if (level_baro == TRUE && overwrite_baro == FALSE) {
      baro_lev_start_temp <- min(lev_baro$Date_time, na.rm = TRUE)
      baro_lev_end_temp <- max(lev_baro$Date_time, na.rm = TRUE)
      #' Filter the baro in data to remove duplicate records
      baro_baro <- subset(baro_baro, subset =
        Date_time < baro_lev_start_temp | Date_time > baro_lev_end_temp)
    }

    #' 3.3 Merge baro data
    #' *3.3.1 - Merge baro data
    #' If there is baro data in the level file append this data
    if (level_baro == TRUE) baro_baro <- rbind(baro_baro, lev_baro)
    baro_baro <- baro_baro[order(Date_time)]
    #' *3.3.2 - Remove duplicates whereever necessary
    #'  If overwrite_baro is set to TRUE then original baro data duplicates
    #'   in the working or levellogger file
    #'   will be overwritten. If overwirte_baro is FALSE then only unoriginal
    #'   duplicates are removed.
    #' Identify duplicate records (by date-time)
    #' Remove duplicates
    if (anyDuplicated(baro_baro$Date_time) > 0) {
      baro_baro$dup_f <- as.logical(
        duplicated(baro_baro$Date_time, fromLast = TRUE))
      baro_baro$dup_l <- as.logical(
        duplicated(baro_baro$Date_time, fromLast = FALSE))
      if (overwrite_baro == TRUE) baro_baro <- baro_baro[!(dup_l)]
      if (overwrite_baro == FALSE) baro_baro <- baro_baro[!(dup_f)]
      baro_baro <- baro_baro[, !c("dup_f", "dup_l")]
    }

    #' *3.3.3 - Generate an updated baro log for the level file
    #' Summarise the baro_baro file into a log format
    baro_baro_log <- baro_baro
    data.table::setDT(baro_baro_log)
    baro_baro_log <-
      baro_baro_log[, .(Start = min(Date_time, na.rm = TRUE),
        End = max(Date_time, na.rm = TRUE)),
          by = .(Baro_SN, Baro_Md, orig)
      ]
    baro_baro_log <-
      baro_baro_log[, c("Start", "End", "Baro_SN", "Baro_Md", "orig")]

    #' Create a table of barologger names and serial numbers that can be joined
    #' to the baro_baro_log
    #' If there is an rdta_log then use this to generate a table that can be
    #'  used in a query to identify logger details.
    if (rdta_log == TRUE) {
      colnames(rdta_log_file)[3] <- "Baro_SN"
      rdta_log_file <-
        transform(rdta_log_file,
          Baro_name = as.character(paste0(Location, "_", Borehole))
        )
      rdta_log_file <- rdta_log_file[, c("Baro_SN", "Baro_name")]
    }

    qbaro <- data.table::data.table(
      Baro_SN = as.character(baro$xle_LoggerInfo$Serial_number),
      Baro_name =
        paste0(
          baro$xle_LoggerHeader$Location[nrow(baro$xle_LoggerHeader)],
          "_",
          baro$xle_LoggerHeader$Project_ID[nrow(baro$xle_LoggerHeader)]
        )
    )
    qbaro <- qbaro[order(Baro_name, Baro_SN)]
    qbaro <- unique(qbaro)
    qbaro_log <- working$log_baro_log[, c("Baro_SN", "Baro_name")]
    if (rdta_log == TRUE) {
      qbaro_log <- rbind(rdta_log_file, qbaro_log)
    }
    qbaro_log <- rbind(qbaro, qbaro_log)
    qbaro_log <- qbaro_log[order(Baro_name, Baro_SN)]
    qbaro_log <- unique(qbaro_log)
    if (nrow(qbaro_log) > 0) {
      baro_baro_log <- merge(baro_baro_log, qbaro_log,
        by = "Baro_SN", all.x = TRUE)
      baro_baro_log <-
        baro_baro_log[, c("Start", "End",
          "Baro_SN", "Baro_name", "Baro_Md")]
      baro_baro_log <- baro_baro_log[order(Start, End)]
    }

    #' Attached the newly generated baro_baro_log into the level or working file
    #' Insert updated baro data and baro log into the level file (working file)
    working$log_baro_data <-
      baro_baro[, c("Date_time", "level_kpa", "temperature_degc")]
    working$log_baro_log <- baro_baro_log

    #' 3.4 Join baro and level data
    #'   Now the updated baro data is in the working file.
    #'   Join baro and level data into a single r table

    #' *3.4.1 - remove temperature column in baro table
    #'  First, the temperature column in the barometric table is removed
    temp_working_log_baro_data <-
      working$log_baro_data[, !c("temperature_degc")]

    #' *3.4.2 - Create a temporary baro-level join file
    #' Join the baro data to the baro$log_data under another name -
    #'  "merged_work_baro"
    #' Before the merge the working data is filtered by the
    #' start and end date times
    merged_work_baro <- subset(working$log_data,
      subset = Date_time >= start_t & Date_time <= end_t)

    #' The data will be joined to a generated sequence to match the
    #'  shortest possible shared sample rate of the barologger and level logger.
    s_rate <- max(c(working$xle_LoggerHeader$Sample_rate_s,
      baro$xle_LoggerHeader$Sample_rate_s, na.rm = TRUE))
    s_rate_min <- min(c(working$xle_LoggerHeader$Sample_rate_s,
      baro$xle_LoggerHeader$Sample_rate_s, na.rm = TRUE
    ))
    if (is.null(join_tolerance)) {
      join_tolerance <- s_rate_min * 0.9
    }
    if (join_tolerance > s_rate_min * 0.9) {
      cr_msg <- padr(core_message = paste0(
        "The entered join_tolerance was decreased to",
        " 9/10 of the minimum sample rate", collapse = ""),
        pad_char = " ", pad_extras = c("|", "", "", "|"),
        force_extras = TRUE, justf = c(-1, 3), wdth = 80)
      message(cr_msg)
      join_tolerance <- s_rate_min * 0.9
      cr_msg <- padr(core_message = paste0("Join tolerance now at ",
        join_tolerance, " seconds.", collapse = ""),
        pad_char = " ", pad_extras = c("|", "", "", "|"),
        force_extras = TRUE, justf = c(-1, 3), wdth = 80)
      message(cr_msg)
    }
    dt <- seq.POSIXt(from = start_t, to = end_t, by = paste0(s_rate, " sec"))
    dt_dummi_s <- dt - join_tolerance * 0.5
    dt_dummi_e <- dt + join_tolerance * 0.5
    dt_df <- data.table::data.table(
      dt = as.POSIXct(dt), dt_dummi_s = as.POSIXct(dt_dummi_s),
      dt_dummi_e = as.POSIXct(dt_dummi_e)
    )
    attr(dt, "tzone")
    merged_work_baro$dummi_tl <- merged_work_baro$Date_time
    temp_working_log_baro_data$dummi_tb <- temp_working_log_baro_data$Date_time
    data.table::setkey(merged_work_baro, Date_time, dummi_tl)
    data.table::setkey(temp_working_log_baro_data, Date_time, dummi_tb)
    data.table::setkey(dt_df, dt_dummi_s, dt_dummi_e)
    dt_df <- data.table::foverlaps(dt_df, merged_work_baro,
      mult = "first", nomatch = NA)
    data.table::setkey(dt_df, dt_dummi_s, dt_dummi_e)
    dt_df <- data.table::foverlaps(dt_df, temp_working_log_baro_data,
      mult = "first", nomatch = NA
    )

    merged_work_baro <- data.table::data.table(
      Date_time = as.POSIXct(dt_df$dt),
      ut1_level_m = dt_df$level_m,
      level_kpa = as.numeric(dt_df$level_kpa),
      bt_level_m = as.numeric(rep(0, nrow(dt_df))),
      bt_Outlier = as.character(rep(NA, nrow(dt_df))),
      bt_Outlier_rule = as.character(rep(NA, nrow(dt_df))),
      t_bt_level_m = as.numeric(rep(0, nrow(dt_df))),
      Drift_offset_m = as.numeric(rep(0, nrow(dt_df))),
      t_Outlier = as.logical(rep(FALSE, nrow(dt_df))),
      t_Outlier_rule = as.character(rep(NA, nrow(dt_df))),
      t_level_m = as.numeric(rep(NA, nrow(dt_df)))
    )

    #' *3.4.3 - Check for join errors
    #' Count the number of NA values in the merged data
    #'  NAs will result from mismatching dates.
    #'  This function will not correct the dates, rather the
    #'  input baro data and or level logger data needs to be fixed.
    nas <- length(merged_work_baro$level_kpa[which(
      is.na(merged_work_baro$level_kpa))])
    baroname <- baro$xle_LoggerHeader$Project_ID[1]
    levelname <- working$xle_LoggerHeader$Project_ID[1]
    if (nas > na_thresh) {
      cr_msg <- padr(core_message = paste0(nas,
        " NAs coerced: baro-level join"),
        pad_char = " ", pad_extras = c("|", "", "", "|"),
        force_extras = NULL, justf = c(-1, 3), wdth = 80)
      message(cr_msg)
      cr_msg <- padr(core_message = paste0(
        "DT mismatch/missing data/different sampling rates",
        collapse = NULL), pad_char = " ", pad_extras = c("|", "", "", "|"),
        force_extras = TRUE, justf = c(-1, 3), wdth = 80)
      message(cr_msg)
    }

    #' 3.5 Joined data --> level_file
    data.table::setDT(working$log_t)
    #' Remove data from the "working" file if overwrite_comp is T
    if (overwrite_comp == TRUE) {
      working$log_t <- subset(working$log_t,
        subset = Date_time < start_t | Date_time > end_t)
      working$log_t <-
        rbind(working$log_t, merged_work_baro)[order(Date_time)]
    }
    #' remove overlapping data from the temp merged file if
    #'  overwrite compensated data is FALSE
    if (overwrite_comp == FALSE) {
      if (nrow(working$log_t) > 0) {
        merged_work_baro <-
          subset(merged_work_baro,
            subset = Date_time < min(working$log_t$Date_time,
              na.rm = TRUE) & Date_time >
              max(working$log_t$Date_time, na.rm = TRUE)
        )
      }
      working$log_t <-
        rbind(working$log_t, merged_work_baro)[order(Date_time)]
    }

    #' 4.1 Compensation
    #'  The barometric compensation can begin.
    #'  Things to check.
    #'   1. Whether the user wanted to overwrite existing barometric
    #'       compensation values. This would have been specificed as
    #'       overwrite_comp T or F
    #'   2. Whether there are any offsets necessary for updated the
    #'      calculated level. For older loggers there was a 9.5m offset -
    #'      will have to watch for this.
    #
    #' *4.1.1 Now that the data is appended in the working$log_t table
    #'  I can proceed with the barometric compensation.
    temp_workking_log_t <- working$log_t
    attr(working$log_t$Date_time, "tzone")

    #' *4.1.2 - Query baro model offset
    #' Need to query the baro model names to add a 9.5 m offset for
    #'   older models. This is done with data.tables foverlaps function
    #' First the tables are set to data.table's
    data.table::setDT(temp_workking_log_t)
    #' Get the column names which will be used to define the final table
    cols <- colnames(temp_workking_log_t)
    #' This is the table that we will be joing to the log_t data
    log_info <- working$xle_LoggerInfo
    attr(log_info$Start, "tzone")
    attr(log_info$End, "tzone")
    data.table::setDT(log_info)
    #' A dummi column is set to define the interval of interest for
    #' the log_t table
    temp_workking_log_t <- temp_workking_log_t[, dummi2 := Date_time]
    #' Data.table needs identified keys for the joining operation,
    #'  these are the time intervals
    #'  for each row in both tables
    data.table::setkey(temp_workking_log_t, Date_time, dummi2)
    data.table::setkey(log_info, Start, End)
    attr(temp_workking_log_t$Date_time, "tzone")
    attr(temp_workking_log_t$dummi2, "tzone")
    attr(log_info$Start, "tzone")
    attr(log_info$End, "tzone")
    #' The overlap join and selecting columns
    temp_workking_log_t <- data.table::foverlaps(temp_workking_log_t,
      log_info, nomatch = NA, mult = "first")
    
    temp_workking_log_t <-
      temp_workking_log_t[, c(cols, "Instrument_type"), with = FALSE]
    temp_workking_log_t <- temp_workking_log_t[, !c("dummi2"), with = FALSE]
    #' Generate an offest column using the level logger model
    old_mods <- c("LT_Jr", "LT_Gold")
    temp_workking_log_t$Md_offset <- as.numeric(
      ifelse(temp_workking_log_t$Instrument_type %in% old_mods,
        9.5, 0))


    #' *4.1.3 - Calculate barometrically compensated water level
    #' Add the compensated values to the working object
    temp_workking_log_t$bt_level_m <- temp_workking_log_t$ut1_level_m -
      (0.101972 * temp_workking_log_t$level_kpa) +
      temp_workking_log_t$Md_offset
    if (overwrite_t_bt_level == TRUE) {
      temp_workking_log_t$t_bt_level_m <- temp_workking_log_t$bt_level_m
      temp_workking_log_t$t_level_m <- temp_workking_log_t$t_bt_level_m +
        temp_workking_log_t$Drift_offset_m
    }
    if (overwrite_t_bt_level == FALSE) {
      temp_workking_log_t$t_bt_level_m <- ifelse(
        is.na(temp_workking_log_t$t_bt_level_m),
          temp_workking_log_t$bt_level_m, temp_workking_log_t$t_bt_level_m)
      temp_workking_log_t$t_level_m <- temp_workking_log_t$t_bt_level_m +
        temp_workking_log_t$Drift_offset_m
    }

    #' Remove the queried Baro model numbers and model offset columns
    working$log_t <- temp_workking_log_t[
      , !c("Instrument_type", "Md_offset"), with = FALSE]

    #' *5 Save data
    #' Save the working level file that has the compensated values.
    if (InasRDS == TRUE) {
      saveRDS(working, file.path(wk_dir, paste0(input_file, ".rds")))
      cr_msg <- padr(core_message = paste0(" ", input_file, " compensated",
        collapse = ""), pad_char = " ", pad_extras = c("|", "", "", "|"),
        force_extras = TRUE, justf = c(-1, 3), wdth = 80)
      return(message(cr_msg))
    } else {
      return(working)
    }
  } else {
  cr_msg <- padr(core_message = paste0("In ", input_file,
    ": no overlapping barometric data for compensation", collapse = ""),
    pad_char = " ", pad_extras = c("|", "", "", "|"), force_extras = TRUE,
    justf = c(-1, 4))
  message(cr_msg)
  }
}