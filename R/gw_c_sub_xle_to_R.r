#' @title Convert xle to R data
#' @description A function that converts xle solonist files into R objects.
#' @details The conversion produces a list object with appropriate logger
#' metadata plus the continuous data stored on the logger.  Some design
#' functionality of this function must be recognised as being inspired by the
#' ecoflux package. The nuance here being that the final object is stored as a
#' customised native R object plus a lot more standardisation of the data is
#' performed. An optional log is also created which is used for file managment.
#' Note that the __measurement units__ are standardised in this funciton to the
#' SI system. Other units could be incorporated in the code as and when needed.
#' Currently only psi units are converted to kPa (applies to barologger files).
#'
#' The R object created contains all logger metadata. The class of the R object
#' is either "baro_file" for level loggers or "level_file" for water level log
#' files. Files are classes based on the phenomena units.
#' The user can choose whether to save the file to rds format the default file
#' name is generated automatically, however this may be overwritten with a
#' custom file name.
#' @param xlefile xle file to convert into R format. Requires that the xle file
#' has been standardised by the pipeline functions xle_import, xle_nomenclature
#' , xle_rename and xle_transfer.
#' @param save_rdta Logical. Save the data in RDS format.
#' @param filename Desired output RDS filename.
#' @param overwrite Overwrite existing files with the same name? Logical.
#' @param app_dates If the filename is null the file name of the output object
#' will be automatically generated. If this logical parameter is set to TRUE,
#' the start and end dates of the data logger file will be appended to the
#' automatically generated file name.
#' @param app_data Do you want to append this data to an existing file from the
#' same station? Logical, defaults to TRUE.
#' @param input_dir Data where the standardised xle files are stored. I the
#' pipeline this is the "nomvet" folder.
#' @param output_dir Folder where the output RDS files are saved too.
#' @param transfer_log Logical. Generate a log of which files have been
#' converted.
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
#' @keywords convert file
#' @return R environment data file.
gw_xle_to_R <- function(
  xlefile = NULL,
  save_rdta = FALSE,
  filename = NULL,
  overwrite = FALSE,
  app_dates = TRUE,
  app_data = FALSE,
  input_dir = ".",
  output_dir = ".",
  transfer_log = FALSE,
  dt_format = c(
    "Ymd HMS", "Ymd IMSp",
    "ymd HMS", "ymd IMSp",
    "mdY HMS", "mdy IMSp",
    "dmY HMS", "dmy IMSp"),
  dt_tz = "Africa/Johannesburg",
  ...) {

  cr_msg <- padr(core_message = paste0("Processing: ", xlefile,
      collapse = ""), wdth = 80, pad_char = "-",
    pad_extras = c("|-", " ", " ", "-|"),
    force_extras = TRUE, justf = c(-1, 4))
  message(cr_msg)

  # Parse xml data to R
  testxml <- attempt::try_catch(XML::xmlParse(file.path(input_dir, xlefile)),
    .e = ~ stop(.x),
    .w = ~ warning(paste0(
    "XML content inappropriate or does",
    " not exist. Try xle_import to standardise encoding",
    " to UTF-8. "
    ), .x)
  )

  # Check to see if the file contains any data
  if (length(testxml["//Body_xle/Data/Log"]) == 0) {
    stop(paste0(
    "The file ",
    file,
    " is empty"
    ))
  }

  # Extract logger metadata
  #  File into
  xle_fi <- data.table::data.table(
    XML::xmlToDataFrame(testxml["//Body_xle/File_info"],
      stringsAsFactors = FALSE))
  xle_fi[xle_fi == ""] <- NA
  fds <- c("Company", "LICENCE", "Date", "Time", "FileName", "Created_by")
  dtb <- sapply(fds, function(x) {
    if (x %in% c(colnames(xle_fi))) {
      fds <- xle_fi[[x]]
    } else {
      fds <- NA
      message(paste0(x, " missing from file information in ", xlefile,
        collapse = ""))
    }
  })
  dtb <- as.data.frame.list(dtb, stringsAsFactors = FALSE)
  if ("Date" %in% c(colnames(dtb)) && "Time" %in% c(colnames(dtb))) {
    dtb$Date_time <- lubridate::parse_date_time(
      x = as.character(paste0(dtb$Date, " ", dtb$Time)),
      orders = dt_format, tz = dt_tz
    )
  }
  xle_FileInfo <- data.table::data.table(
    Company = as.character(dtb$Company),
    Licence = as.character(dtb$LICENCE),
    Date_time = dtb$Date_time,
    FileName = as.character(dtb$FileName),
    Created_by = as.character(dtb$Created_by)
  )

  # Logger info
  xle_fi <-
    data.table::data.table(
      XML::xmlToDataFrame(testxml["//Body_xle/Instrument_info"],
        stringsAsFactors = FALSE
    ))
  data.table::setnames(xle_fi, old = "Battery_charge", new = "Battery_level",
    skip_absent = TRUE)
  xle_fi[xle_fi == ""] <- NA
  fds <- c(
    "Instrument_type", "Model_number", "Instrument_state",
    "Serial_number", "Battery_level", "Channel", "Firmware"
  )
  dtb <- sapply(fds, function(x) {
    if (x %in% c(colnames(xle_fi))) {
      fds <- xle_fi[[x]]
    } else {
    fds <- NA
    message(paste0(x, " missing from file information in ", xlefile,
      collapse = ""))
    }
  })
  dtb <- as.data.frame.list(dtb, stringsAsFactors = FALSE)
  xle_LoggerInfo <- data.table::data.table(
    Instrument_type = as.character(dtb$Instrument_type),
    Model_number = gsub("[[:blank:]]", "", as.character(dtb$Model_number)),
    Instrument_state = as.character(dtb$Instrument_state),
    Serial_number = as.character(dtb$Serial_number),
    Battery_level = readr::parse_number(as.character(dtb$Battery_level)),
    Channel = as.numeric(as.character(dtb$Channel)),
    Firmware = as.character(dtb$Firmware)
  )

  # Logger header
  xle_fi <- data.table::as.data.table(XML::xmlToDataFrame(
    testxml["//Body_xle/Instrument_info_data_header"],
    stringsAsFactors = FALSE
  ))
  xle_fi[xle_fi == ""] <- NA
  fds <- c(
    "Project_ID", "Location", "Latitude", "Longitude", "Sample_rate",
    "Sample_mode", "Event_ch", "Event_threshold", "Schedule", "Start_time",
    "Stop_time", "Num_log"
  )
  if ("Latitude" %in% c(colnames(xle_fi))) {
    xle_fi$Latitude <- gsub(",", ".", xle_fi$Latitude)
    xle_fi$Latitude <- as.numeric(as.character(xle_fi$Latitude))
  }
  if ("Longtitude" %in% c(colnames(xle_fi))) {
    xle_fi$Longtitude <- gsub(",", ".", xle_fi$Longtitude)
    xle_fi$Longtitude <- as.numeric(as.character(xle_fi$Longtitude))
    data.table::setnames(xle_fi, old = "Longtitude", new = "Longitude")
  }
  if ("Longitude" %in% c(colnames(xle_fi))) {
    xle_fi$Longitude <- gsub(",", ".", xle_fi$Longitude)
    xle_fi$Longitude <- as.numeric(as.character(xle_fi$Longitude))
  }
  if ("Event_threshold" %in% c(colnames(xle_fi))) {
    xle_fi$Event_threshold <- gsub(",", ".", xle_fi$Event_threshold)
    xle_fi$Event_threshold <-
      as.numeric(as.character(xle_fi$Event_threshold))
  }
  if ("Schedule" %in% c(colnames(xle_fi))) {
    # The error handling function here avoids a complex shedule recording
    # that cannot be coerced to data time format. The result is that the
    # schedule is preserved as a character string.
    xle_fi$Schedule <- attempt::try_catch(
      as.numeric(xle_fi$Schedule),
      .e = ~ as.character(xle_fi$Schedule)
    )
  }
  dtb <- sapply(fds, function(x) {
    if (x %in% c(colnames(xle_fi))) {
      fds <- xle_fi[[x]]
    } else {
    fds <- NA
    message(paste0(x, " missing from file information in ", xlefile,
      collapse = ""))
    }
  })
  dtb <- as.data.frame.list(dtb, stringsAsFactors = FALSE)
  xle_LoggerHeader <- data.table::data.table(
    Project_ID = as.character(dtb$Project_ID),
    Location = as.character(dtb$Location),
    Latitude = as.character(dtb$Latitude),
    Longitude = as.character(dtb$Longitude),
    Sample_rate_s = as.numeric(as.character(dtb$Sample_rate)) / 100,
    Sample_mode = as.character(dtb$Sample_mode),
    Event_ch = as.character(dtb$Event_ch),
    Event_threshold = as.character(dtb$Event_threshold),
    Schedule = as.character(dtb$Schedule),
    Start_time = lubridate::parse_date_time(x = as.character(dtb$Start_time),
      orders = dt_format, tz = dt_tz),
    Stop_time = lubridate::parse_date_time(x = as.character(dtb$Stop_time),
      orders = dt_format, tz = dt_tz),
    Num_log = as.numeric(dtb$Num_log)
  )

  # Add on Start and End times to the logger info table
  xle_LoggerInfo$Start <- xle_LoggerHeader$Start_time
  xle_LoggerInfo$End <- xle_LoggerHeader$Stop_time
  xle_FileInfo$Start <- xle_LoggerHeader$Start_time
  xle_FileInfo$End <- xle_LoggerHeader$Stop_time

  # Variables to use later...
  loc_name <- xle_LoggerHeader$Location
  bh_name <- xle_LoggerHeader$Project_ID
  st_dt <- xle_LoggerHeader$Start_time
  ed_dt <- xle_FileInfo$Date_time

  # Get the phenomena (channels) recorded by the logger
  phens <- testxml["//Body_xle/*[starts-with(name(), 'Ch')]"]
  phenomena <- sapply(phens, function(x) {
    # Extract channel ID and strip trailing whitespace that was
    # found in some files
    id <- gsub(
      "[[:blank:]+$]", "",
      XML::xmlValue(x["Identification"]$Identification)
    )
    # Extract channel unit
    unit <- XML::xmlValue(x["Unit"]$Unit)
    # Combine channel ID and unit into a single standardized name
    phen_name <- tolower(paste0(id, "_", unit))
    phen_name <- gsub("[[:blank:]]", "_", phen_name)
    return(phen_name)
  })

  # Extract any parameter information embedded in each channel
  params <- lapply(testxml["//Body_xle/*/Parameters"], XML::xmlToList)
  # name params
  names(params) <- phenomena
  # Extract parameter values for each phenomenon and combine in a table
  xle_phenomena <- sapply(params, function(x) {
      Vars <- as.character(names(x))
      p <- as.data.frame(cbind(Vars, t(as.data.frame(x))))
    if ("Val" %in% colnames(p)) {
      p$Val <- gsub(",", ".", p$Val)
      p$Val <- as.numeric(as.character(p$Val))
    }
    if (length(p) > 1) {
      p$Start <- st_dt
      p$End <- ed_dt
    }
    if (nrow(p) > 0) {
      rownames(p) <- as.integer(c(seq_len(nrow(p))))
    }
    return(p)
  })

  # get xle log data
  log_data <- data.table::data.table(
    Date_time = lubridate::parse_date_time(x = as.character(paste(
      sapply(testxml["//Body_xle/Data/Log/Date"], XML::xmlValue),
      sapply(testxml["//Body_xle/Data/Log/Time"], XML::xmlValue)
    )), orders = dt_format, tz = dt_tz)
  )

  for (ph in seq_along(xle_phenomena)) {
    chan <- paste0("ch", ph)
    log_data[, phenomena[ph]] <-
      sapply(
        testxml[paste0("//Body_xle/Data/Log/", chan)],
        function(x) {
          x <- gsub(pattern = ",", replacement = ".",
            x = XML::xmlValue(x))
          x <- as.numeric(x)
        }
      )
  }

  # Unit standardisation
  loghd <- colnames(log_data)
  # Convert level_psi to level_kpa - this applies to barologger files
  nayyay <- grep("level_psi", loghd)
  if (length(nayyay) != 0) {
    log_data[, nayyay] <-
      log_data[, nayyay, with = FALSE] * 6.89476
    colnames(log_data)[nayyay] <- "level_kpa"
    phen_n <- grep("level_psi", names(xle_phenomena))
    names(xle_phenomena)[phen_n] <-
    gsub(
      pattern = "level_psi", replacement = "level_kpa",
      x = names(xle_phenomena)[phen_n]
    )
  }
  # Convert level_cm to level_m - this applies to level files
  nayyay <- grep("level_cm", loghd)
  if (length(nayyay) != 0) {
    log_data[, nayyay] <- log_data[, nayyay, with = FALSE] / 100
    colnames(log_data)[nayyay] <- "level_m"
    phen_n <- grep("level_cm", names(xle_phenomena))
    names(xle_phenomena)[phen_n] <- gsub(
      pattern = "level_cm", replacement = "level_m",
      x = names(xle_phenomena)[phen_n]
    )
    xle_phenomena$level_m[xle_phenomena$level_m$Vars == "Offset",
      ][, "Val"] <- xle_phenomena$level_m[
        xle_phenomena$level_m$Vars ==
      "Offset", ][, "Val"] / 100
    xle_phenomena$level_m[xle_phenomena$level_m$Vars ==
      "Offset", ][, "Unit"] <- "m"
  }
  # Check whether there is conductivity data
  nayyay <- grep("conductivity_micros_per_cm", loghd)
  if (length(nayyay) != 0) {
    colnames(log_data)[nayyay] <- "conduct_mS_per_cm"
    phen_n <- grep("conduct_mS_per_cm", names(xle_phenomena))
    names(xle_phenomena)[phen_n] <-
      gsub(
        pattern = "conductivity_micros_per_cm",
        replacement = "conduct_mS_per_cm",
        x = names(xle_phenomena)[phen_n]
      )
  }
  # check for alternative spelling of this phenomenon
  nayyay <- grep("conduc_ivi_y_micros_per_cm", loghd)
  if (length(nayyay) != 0) {
    colnames(log_data)[nayyay] <- "conduct_mS_per_cm"
    phen_n <- grep("conduc_ivi_y_micros_per_cm", names(xle_phenomena))
    names(xle_phenomena)[phen_n] <-
      gsub(
        pattern = "conduc_ivi_y_micros_per_cm",
        replacement = "conduct_mS_per_cm",
        x = names(xle_phenomena)[phen_n]
      )
  }

  # Flag barologger files
  if ("level_kpa" %in% colnames(log_data)) {
    level_class <- "baro_file"
    log_data <- transform(log_data,
      Date_time = Date_time,
      level_kpa = as.numeric(level_kpa),
      temperature_degc = as.numeric(temperature_degc)
    )
  }
  if (!"level_kpa" %in% colnames(log_data)) {
    level_class <- "level_file"
    if (!"conduct_mS_per_cm" %in% colnames(log_data)) {
      log_data <- transform(log_data,
        conduct_mS_per_cm = rep(NA, length(log_data$Date_time))
      )
    }
    if (sum(is.na(log_data$conduct_mS_per_cm)) > 0) {
      log_data$conduct_mS_per_cm <- rep(NA, length(log_data$Date_time))
    }
    log_data <- transform(log_data,
      Date_time = Date_time,
      level_m = as.numeric(level_m),
      temperature_degc = as.numeric(temperature_degc),
      conduct_mS_per_cm = as.numeric(conduct_mS_per_cm)
    )
  }
  log_data <- unique(log_data[order(Date_time)], by = "Date_time")

  # Add start and end dates to logger data metadata
  xle_FileInfo$Start <- min(log_data$Date_time)
  xle_FileInfo$End <- max(log_data$Date_time)
  xle_LoggerInfo$Start <- min(log_data$Date_time)
  xle_LoggerInfo$End <- max(log_data$Date_time)

  # Generate empty tables for the new log files. The files below are used
  # for transforming the data into their final product.
  log_retrieve <- data.table::data.table(
    id = as.integer(),
    Date_time = as.POSIXct(character()),
    QA = logical(),
    Dipper_reading_m = double(),
    Casing_ht_m = double(),
    Depth_to_logger_m = double(),
    Notes = character()
  )
  log_baro_log <- data.table::data.table(
    Start = as.POSIXct(character()),
    End = as.POSIXct(character()),
    Baro_SN = character(),
    Baro_name = character(),
    Baro_Md = character()
  )
  log_baro_data <- data.table::data.table(
    Date_time = as.POSIXct(character()),
    level_kpa = double(),
    temperature_degc = double()
  )
  log_datum <- data.table::data.table(
    id = as.integer(),
    Start = as.POSIXct(character()),
    End = as.POSIXct(character()),
    Datum = character(),
    X = double(),
    Y = double(),
    H_masl = double()
  )
  log_drift <- data.table::data.table(
    Start = as.POSIXct(character()),
    End = as.POSIXct(character()),
    assumed_drift = factor(),
    n = integer(),
    f = double()
  )
  log_t_man_out <- data.table::data.table(
    Start = as.POSIXct(character()),
    End = as.POSIXct(character()),
    Notes = as.character()
  )
  log_t <- data.table::data.table(
    Date_time = as.POSIXct(character()),
    ut1_level_m = double(),
    level_kpa = double(),
    bt_level_m = double(),
    bt_Outlier = character(),
    bt_Outlier_rule = character(),
    t_bt_level_m = double(),
    Drift_offset_m = double(),
    t_Outlier = character(),
    t_Outlier_rule = character(),
    t_level_m = double()
  )

  # Remove unsupported phenomena - these can be added later if necessary
  supported_phens <- c(
    "level_m", "temperature_degc",
    "level_kpa", "conduct_mS_per_cm"
  )
  current_phens <- names(xle_phenomena)
  keep_phens <- intersect(supported_phens, current_phens)
  xle_phenomena <- xle_phenomena[keep_phens]

  # Creat empty xle_phenomena tables and import phenomena
  level_m <- data.table::data.table(
    Vars = as.character(),
    Val = as.numeric(),
    Unit = as.character(),
    Start = as.POSIXct(as.character()),
    End = as.POSIXct(as.character())
  )
  if ("level_m" %in% names(xle_phenomena)) {
    l <- list(xle_phenomena$level_m, level_m)
    xle_phenomena$level_m <-
      data.table::rbindlist(l, use.names = TRUE, fill = TRUE)
  }
  temperature_degc <- data.table::data.table(
    Vars = as.character(),
    Val = as.numeric(),
    Unit = as.character(),
    Start = as.POSIXct(as.character()),
    End = as.POSIXct(as.character())
  )
  if ("temperature_degc" %in% names(xle_phenomena)) {
    l <- list(xle_phenomena$temperature_degc, temperature_degc)
    xle_phenomena$temperature_degc <-
        data.table::rbindlist(l, use.names = TRUE, fill = TRUE)
  }
  level_kpa <- data.table::data.table(
    Vars = as.character(),
    Val = as.numeric(),
    Unit = as.character(),
    Start = as.POSIXct(as.character()),
    End = as.POSIXct(as.character())
  )
  if ("level_kpa" %in% names(xle_phenomena)) {
    l <- list(xle_phenomena$level_kpa, level_kpa)
    xle_phenomena$level_kpa <-
      data.table::rbindlist(l, use.names = TRUE, fill = TRUE)
  }
  conduct_mS_per_cm <- data.table::data.table(
    Vars = as.character(),
    Val = as.numeric(),
    Unit = as.character(),
    Start = as.POSIXct(as.character()),
    End = as.POSIXct(as.character())
  )
  if ("conduct_mS_per_cm" %in% names(xle_phenomena)) {
    l <- list(xle_phenomena$conduct_mS_per_cm, conduct_mS_per_cm)
    xle_phenomena$conduct_mS_per_cm <-
      data.table::rbindlist(l, use.names = TRUE, fill = TRUE)
  }

  # Combine log data and logger metadata into a single xle_data object
  xle_data <- list(
    xle_FileInfo = xle_FileInfo,
    xle_LoggerInfo = xle_LoggerInfo,
    xle_LoggerHeader = xle_LoggerHeader,
    xle_phenomena = xle_phenomena,
    log_data = log_data,
    log_retrieve = log_retrieve,
    log_baro_log = log_baro_log,
    log_baro_data = log_baro_data,
    log_datum = log_datum,
    log_drift = log_drift,
    log_t_man_out = log_t_man_out,
    log_t = log_t
  )

  class(xle_data) <- level_class
  if (!is.null(filename)) {
      filename <- gsub(".rds", "", filename)
  } else if (app_dates == TRUE) {
    filename <- paste0(
      loc_name, "_", bh_name, "_",
      gsub("-", "", as.character(as.Date(st_dt))), "-",
      gsub("-", "", as.character(as.Date(ed_dt)))
    )
  } else {
    filename <- paste0(loc_name, "_", bh_name)
  }

  # Only overwrite an existing file with the same name if "overwrite" == T
  # Appends data if overwrite is true plus there is a duplicate file name
  # Before writing the file to rds check whether there is an
  #  existing log file and if not create one
  transfer_log_vet <-
    list.files(
      path = output_dir, pattern = "transfer_log.rds",
      recursive = FALSE, full.names = TRUE
    )
  if (length(transfer_log_vet) > 0) {
      transfer_log <- readRDS(file.path(output_dir, "transfer_log.rds"))
    if (sum(
        grep(pattern = basename(xlefile), x = transfer_log$xle_file)) > 0) {
      overwrite <- FALSE
    }
  } else if (length(transfer_log_vet) == 0) {
    saveRDS(data.table::data.table(
      transfer_time = as.POSIXct(character()),
      xle_file = character(), from = character(),
      to = character()), file.path(output_dir, "transfer_log.rds")
    )
    # If there is a log file we need to check whether
    #  this has already been transferred.
    transfer_log <- readRDS(file.path(output_dir, "transfer_log.rds"))
  }

  # Only log if log_it is set to TRUE
  log_it <- FALSE

  # Don't overwrite (overwrite = F)
  if (length(list.files(path = output_dir, pattern = filename)) > 0
      && app_data == TRUE && overwrite == FALSE) {
    message(paste0(
      "There is already a file named ",
      "'", filename, "'.", " File not saved",
      collapse = ""))
    message("Set overwrite = TRUE ?")
  } else if (length(
          list.files(path = output_dir, pattern =
            paste0(filename, ".rds"))) > 0
        && overwrite == TRUE && app_data == FALSE) {
      saveRDS(xle_data, file.path(output_dir, paste0(filename, ".rds")))
      log_it <- TRUE
  } else if (length(list.files(path = output_dir, pattern =
      paste0(filename, ".rds"))) > 0 && overwrite == TRUE && app_data == TRUE) {
    new_file <- xle_data
    old_file <- readRDS(file.path(output_dir, paste0(filename, ".rds")))
    xle_data <- gw_rdta_append(
      new_file = new_file, old_file = old_file,
      InasRDS = FALSE, RDS_name = filename,
      save_rdta = TRUE, new_dir = output_dir
      )
    log_it <- TRUE
  } else if (
    length(list.files(path = output_dir,
      pattern = paste0(filename, ".rds"))) == 0 & overwrite == TRUE) {
    saveRDS(xle_data, file.path(output_dir, paste0(filename, ".rds")))
    log_it <- TRUE
  }

  # Indicate whether the transfer was performed and log details
  if (log_it == TRUE) {
    log_entry <- data.table::data.table(
      transfer_time = Sys.time(),
      xle_file = basename(xlefile),
      from = input_dir, to = output_dir
    )
    logging <- readRDS(file.path(output_dir, "transfer_log.rds"))
    logging <- rbind(logging, log_entry, fill = TRUE)
    saveRDS(logging, file.path(output_dir, "transfer_log.rds"))
    cr_msg <- padr(core_message = paste0("Transfer log updated for: ",
      xlefile, collapse = ""), wdth = 80, pad_char = " ",
      pad_extras = c("|   ", " ", " ", " |"), force_extras = TRUE,
      justf = c(-1, 2))
    message(cr_msg)
  } else {
    cr_msg <- padr(core_message = paste0("File: ", xlefile,
      " was not transformed", collapse = ""), wdth = 80,
      pad_char = " ", pad_extras = c("! ", " ", " ", " !"),
      force_extras = TRUE, justf = c(-1, 2))
    message(cr_msg)
  }
  cr_msg <- padr(core_message = paste0("Finished: ", xlefile, collapse = "")
    , wdth = 80, pad_char = "-", pad_extras = c("|-", " ", " ", "-|"),
    force_extras = TRUE, justf = c(-1, 4))
  message(cr_msg)
  return(xle_data)
}
