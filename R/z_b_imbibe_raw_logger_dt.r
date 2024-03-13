#' @title Imbibes logger data exports
#' @md
#' @description Function to read in 'flat' loggers files into R. A first step
#'  towards processing data in `ipayipi`.
#' @param pipe_house List of pipeline directories. __See__
#'  `ipayipi::ipip_init()` __for details__.
#' @param file_path Path and name of file (excluding the file extension).
#' @param file_ext The file extension defaults of the raw logger data files. This can be left as `NULL` so 'all' files but those with extensions that cannot be imbibed (".ipr|.ipi|.iph|.xls|.rps|.rns|.ods|.doc").
#' @param col_dlm The column delimter which is fed to `data.table::fread()`. Defaults to NULL. When `NULL` the function uses `data.table::fread` ability to 'guess' the delimeter.
#' @param dt_format The function attempts to work out the date-time format
#'  from a vector of format types supplied to this argument. The testing is
#'  done via `lubridate::parse_date_time()`. `lubridate::parse_date_time()`
#'  prioritizes the tesing of date-time formats in the order vector of
#'  formats supplied. The default vector of date-time formats supplied should
#'  work well for most logger outputs.
#' @param dt_tz Recognized time-zone (character string) of the data locale. The
#'  default for the package is South African, i.e., "Africa/Johannesburg" which
#'  is equivalent to "SAST".
#' @param record_interval_type If there are is no discrete record interval set
#'  in the logger program, i.e., the sampling is event-based, then this
#'  parameter must be set to "event_based". By default this function has this
#'  parameter set to "continuous", but the record interval is scrutinized by
#'  'ipayipi::record_interval_eval()' --- see the help files for this function
#'  for more information.
#'  The parameter supplied here is only used if there is only one data record
#'  and the record interval cannot be evaluated by
#'  `ipayipi::record_interval_eval()`.
#' @param data_setup List of options used to extract data and metadata from
#'  instrument data outputs. Mandatory fields are indicated with an '*'.
#'   File header options include*^1^:
#'   1. *__file_format__ -- the native/raw file format.
#'   1. *__station_title__ -- the supplied instrument station title.
#'   1. location -- the standardised location (name) of the station.
#'   1. *__logger_type__ -- the type of logger.
#'   1. *__logger_sn__ -- the serial number of the logger.
#'   1. logger_os -- the operating system (or firmware version) on the logger.
#'   1. logger_program_name -- the name of the program installed on the logger
#'       (also 'DLD name' on Cambel Scientific systems).
#'   1. logger_programe_sig -- signature of the logger program (also 'DLD
#'       signature' on Cambel Scientific systems).
#'   1. logger_title -- the custom name given to a logger by the programmer.
#'   1. table_name -- the generic name of the table containing data.
#'
#'   *^1^ These options must be supplied as a charater string or the row and
#'     column index provided as for example, rici(ri = 1, ci = 2). See
#'     `?ipayipi::rici()` for more details.
#'
#'   1. date_time -- Only the column index must be provided here as an integer,
#'      e.g., 3.
#'
#'  File phenomena information and data *^2^:
#'   1. *__phen_name__ -- a list of row and column numbers corresponding to the
#'       names of phenomena (variables).
#'   1. phen_unit -- a list of row and column numbers corresponding to the
#'       names of phenomena units.
#'   1. phen_var_type -- a vector of character strings designating the type of
#'       variable for each phenomenon.
#'   1. phen_measure -- a list of row and column numbers corresponding to the
#'       type of measurement calculated by the logger for each phenomena, e.g.,
#'       an 'average', 'sample', 'minimum', etc.
#'   1. phen_offset -- a list of offset values that have been pre-applied to
#'       the data, i.e, the offset is only noted and not used to transform the
#'       data.
#'   1. sensor_id -- a list of row and column numbers corresponding to sensor
#'       unique id values. Otherwise a vector of character strings designating
#'       the type of 'sensor_id' for each phenomenon.
#'
#'  *^2^ These options must be supplied as using `ipayipi::rng_rici()`, or input
#'    as a vector of character strings with the actual values. If using
#'    `ipayipi::rng_rici()`, at least the row in which phenomena details are
#'    found and the columns wherein these lie are required. See
#'    ?ipayipi::rng_rici() for more details.
#'
#'   1. *__data_row__ -- a single integer value designating the row where
#'     phenomena data begin from.
#'   1. *__id_col__ -- a single integer value designating a data row unique
#'     identifier row.
#' @param remove_prompt Logical; passed to `ipayipi::record_interval_eval()`.
#'  Activate a readline prompt to choose whether or not filter our records from
#'  `dta_in` with inconsistent record intervals.
#' @param logg_interfere_type Two options here: "remote" or "on_site". Each
#'  time a logger is visited is counted as a logger interference event.
#'  Type _'remote'_ occurs when data is downloaded remotely. Type _'on_site'_
#'  is when data was downloaded on site. _See details_ ...
#' @param verbose Logical passed to `attempt::attempt()` which reads the logger
#'  text file in with either `data.table::fread()` or base R. Also whether to 
#'  print progress.
#' @details This function uses `data.table::fread` which is optimized for
#'  processing 'big data'. Apart from usual the usual options which can be
#'  parsed to `data.table::fread` this function generates some standardised
#'  metadata to complement the read from a logger data table (if
#'  `data.table::fread()` is unsuccessful `base::read.csv()` is used). This
#'  metadata may vary from one logger output to another. To cater for this
#'  variation this function requires a `data_setup` to be completed. Once setup
#'  this can be used as a standard for further imports.
#'  This function also attempts to check whether the recording interval in the
#'  data date-time stamp has been consistent. A prompt is called if there are
#'  inconsistent time intervals between record events, and data rows with
#'  inconsistent time intervals will be removed if approved.
#'  A basic check is performed to check the success of converting date-time
#'  values to a recognised format in R (i.e., POSIXct).
#'  Regarding the `logg_interfere_type` parameter. Owing to potential
#'  interference of sensors etc when downloading data 'on site' or logger
#'  related issues when data is sent/obtained remotely, the date-time stamps
#'  of these events must be preserved. A `logg_interfere` data table is
#'  generated for this purpose and stored with the data. This data cannot
#'  necessarily be extracted from the 'data_summary' once data has been
#'  appended as some of this data will be overwritten during the appending
#'  process. The purpose of the 'logg_interfere' table is to retain this
#'  information, which is used by `ipayipi` for further processing.
#' @return A list of class "ipayipi_raw_data" that contains a 'data_summary',
#'  'phens' (phenomena), and 'raw_data' tables (data.table).
#' @export
#' @author Paul J. Gordijn
imbibe_raw_logger_dt <- function(
  pipe_house = NULL,
  file_path = NULL,
  file_ext = NULL,
  col_dlm = NULL,
  dt_format = c(
    "Ymd HMS", "Ymd IMSp", "ymd HMS", "ymd IMSp", "mdY HMS", "mdy IMSp",
    "dmY HMS", "dmy IMSp", "Ymd HMOS", "Ymd IMOSp", "ymd HMOS", "ymd IMOSp",
    "mdY HMOS", "mdy IMOSp", "dmY HMOS", "dmy IMOSp"),
  dt_tz = "Africa/Johannesburg",
  record_interval_type = "continuous",
  data_setup = NULL,
  remove_prompt = FALSE,
  logg_interfere_type = "on_site",
  verbose = TRUE,
  ...
) {
  "%ilike%" <- "phen_name" <- NULL
  if (is.null(file_ext)) {
    file_ext <- tools::file_ext(file_path)
    file_ext <- paste0("\\.", sub(pattern = "\\.", replacement = "", file_ext))
  }
  if (is.null(col_dlm) && !is.null(file_ext)) {
    fx <- file_ext
    col_dlm <- ipayipi::file_read_meta[file_ext %ilike% fx]$sep[1]
  }
  file <- attempt::attempt(data.table::fread(file = file_path, header = FALSE,
    check.names = FALSE, blank.lines.skip = FALSE, sep = col_dlm,
    showProgress = verbose, strip.white = FALSE, fill = TRUE),
    silent = !verbose)
  # if there was an error then we try and read the file using base r
  if (attempt::is_try_error(file) || ncol(file) == 1) {
    # use auto fread
    dyno_read <- function(file_path = NULL, ...) {
      file <- data.table::fread(file_path, header = FALSE, ...)
      l <- R.utils::countLines(file_path)[1]
      file_head <- readLines(file_path)[1:(l - nrow(file))]
      #if (all(col_dlm %in% "\t", file_ext %ilike% ".txt")) col_dlm <- ""
      file_head <- lapply(file_head, function(x) {
        x <- strsplit(x, split = col_dlm)
        x <- unlist(c(x, rep(NA, ncol(file) - length(x))))
        x <- lapply(x, function(z) gsub("\"", "", z))
        names(x) <- names(file)
        return(data.table::as.data.table(x))
      })
      file_head <- data.table::rbindlist(file_head)
      file <- rbind(file_head, file, use.names = TRUE)
      return(file)
    }
    file <- attempt::attempt(dyno_read(file_path))
  }
  if (attempt::is_try_error(file)) {
    message("Failed reading file check recognised file format")
    return(list(ipayipi_data_raw = file, err = TRUE))
  }
  data_setup_names <- c(
    "file_format", "station_title", "location", "logger_type",
    "logger_sn", "logger_os", "logger_program_name",
    "logger_program_sig", "logger_title", "table_name", # 1:10
    "date_time", # 11
    "phen_name", "phen_unit", "phen_var_type", "phen_measure",
    "phen_offset", "sensor_id", # 12:17
    "id_col", "data_row") # 18:19

  if (is.list(data_setup)) {
    # check that the defaults have been supplied
    if (!any(data_setup_names[c(1:2, 4:5, 11, 16)] %in% names(data_setup))) {
      message(paste0("The following names were not supplied: ",
      data_setup_names[!data_setup_names[c(1:2, 4:5, 11, 16)] %in%
        names(data_setup)], sep = "\n"))
      message("Supply all required information in the 'data_setup'!")
      return(list(ipayipi_data_raw = file, err = TRUE))
    }
    # check for unrecognised names
    if (any(!names(data_setup) %in% data_setup_names)) {
      message(paste0("The following names were not supported: ",
      names(data_setup)[!names(data_setup) %in% data_setup_names],
        sep = "\n"))
      message("Unsupported names in the 'data_setup'!")
      return(list(ipayipi_data_raw = file, err = TRUE))
    }
    # get header information
    head_names <- data_setup_names[1:10]
    head_info <- data_setup[names(data_setup) %in% head_names]
    head_info_i <- lapply(seq_along(head_info), function(i) {
      if ("ipayipi rng" %in% class(head_info[[i]])) {
        if (head_info[[i]][["c_rng"]][2] == "extract") {
            head_info[[i]]$c_rng <- head_info[[i]][["c_rng"]][1]:ncol(file)
            head_info[[i]]$r_rng <- rep(head_info[[i]]$r_rng,
              length(head_info[[i]]$c_rng))
        }
        if (!is.null(head_info[[i]]$string_extract)) {
          file_extract <- as.vector((unlist((file[head_info[[i]]$r_rng,
            head_info[[i]]$c_rng, with = FALSE]))))
          rng_pattern <- head_info[[i]]$string_extract$rng_pattern
          strxy <- file_extract %ilike% rng_pattern
          if (any(strxy)) {
            names(strxy) <- seq_along(strxy)
            ex_pos <- as.integer(names(strxy[which(strxy)])[1])
            rel_start <- head_info[[i]]$string_extract$rel_start
            rel_end <- head_info[[i]]$string_extract$rel_end
            head_info[[i]] <- substr(file_extract[ex_pos], # extract by pos
              unlist(gregexpr(rng_pattern, file_extract[ex_pos])) + rel_start,
              unlist(gregexec(rng_pattern, file_extract[ex_pos])) + rel_end)
          } else {
            head_info[[i]] <- NULL
          }
        } else {
          head_info[[i]] <- NULL
        }
      }
      if (!is.character(head_info[[i]]) && !is.null(head_info[[i]])) {
        head_info_l <- file[head_info[[i]][1], ][[head_info[[i]][2]]]
      }
      if (is.null(head_info[[i]]) || is.character(head_info[[i]])) {
        head_info_l <- head_info[[i]]
      }
      return(head_info_l)
    })
    names(head_info_i) <- names(head_info)
    # add null items to header list
    null_info <- head_names[!head_names %in% names(head_info)]
    null_info_i <- lapply(null_info, function(x) NULL)
    names(null_info_i) <- null_info
    if (length(null_info_i) > 0) head_info_i <- c(head_info_i, null_info_i)
    head_info_i <- lapply(head_info_i, function(x) {
      if (is.null(x)) x <- NA
      invisible(x)
    })
    # summarise phenomena info
    phen_names <- data_setup_names[12:17]
    phen_info <- data_setup[names(data_setup) %in% phen_names]
    phen_info_ij <- lapply(seq_along(phen_info), function(i) {
      if (any(class(phen_info[[i]]) %in% "ipayipi rng")) {
        if (phen_info[[i]][["c_rng"]][2] == "extract") {
          phen_info[[i]]$c_rng <- phen_info[[i]][["c_rng"]][1]:ncol(file)
          phen_info[[i]]$r_rng <- rep(phen_info[[i]]$r_rng,
            length(phen_info[[i]]$c_rng))
        }
        dta <- phen_info[[i]]
      } else {
        dta <- phen_info[[i]]
      }
      return(dta)
    })
    names(phen_info_ij) <- names(phen_info)
    phen_info_i <- lapply(seq_along(phen_info_ij), function(i) {
      if ("r_rng" %in% names(phen_info_ij[[i]])) {
        dta <- lapply(seq_along(phen_info_ij[[i]]$r_rng), function(z) {
          dtsb <- file[
            phen_info_ij[[i]]$r_rng[z], ][[phen_info_ij[[i]]$c_rng[z]]]
          return(dtsb)
        })
      } else {
        dta <- phen_info[[i]]
      }
      return(dta)
    })
    names(phen_info_i) <- names(phen_info)
    # check that the length of all phenomena info is the same
    if (any(unlist(sapply(phen_info_i,
      function(q) length(q) != length(phen_info_i[[1]]))))) {
      stop("Make sure all phenomena range info provided is equal.")
    }
    phen_null <- phen_names[!phen_names %in% names(phen_info)]
    phen_null_i <- lapply(phen_null, function(z) NA)
    names(phen_null_i) <- phen_null
    phen_info <- c(phen_info_i, phen_null_i)
    phen_info <- lapply(phen_info, function(px) {
      lapply(px, function(pxx) {
        if (is.na(pxx) | pxx == "") pxx <- "no_spec"
        return(pxx)
      })
    })
    # if (any(is.na(phen_info$phen_unit))) phen_info$phen_unit <- "no_spec"
    # if (any(is.na(phen_info$phen_measure))) phen_info$phen_measure <- "no_spec"
    # extract data
    dta <- file[data_setup$data_row:nrow(file),
      phen_info_ij$phen_name$c_rng, with = FALSE]
    names(dta) <- unlist(phen_info$phen_name)
    dta <- dta[, names(dta)[!names(dta) %in% ""], with = FALSE]
    date_time <- lubridate::parse_date_time(x = file[
      data_setup$data_row:nrow(file), ][[data_setup$date_time]],
      orders = dt_format, tz = dt_tz)
    if (any(is.na(date_time))) {
      message("Problem with reading date-time values")
      print(file[data_setup$data_row:nrow(file), ][[
        data_setup$date_time]][1:5])
      message("Read as:")
      print(date_time[1:5])
      return(list(ipayipi_data_raw = file, err = TRUE))
    }
    dta$date_time <- date_time
    if (is.null(data_setup$id_col)) {
      id <- seq_len(nrow(file)) - data_setup$data_row
    } else {
      id <- as.integer(file[
        data_setup$data_row:nrow(file), ][[data_setup$id_col]])
    }
    dta$id <- id

    # order the columns
    # phen names
    pn <- c("id", "date_time", unlist(phen_info$phen_name)[
        !unlist(phen_info$phen_name) %in% ""])
    pn <- pn[!is.na(pn)]
    dta <- subset(dta, select = pn)

    # finalize the phenomena table
    phens <- data.table::data.table(
      phid = seq_along(phen_info$phen_name),
      phen_name = unlist(phen_info$phen_name),
      units = unlist(phen_info$phen_unit),
      measure = unlist(phen_info$phen_measure),
      var_type = unlist(phen_info$phen_var_type),
      offset = unlist(phen_info$phen_offset),
      sensor_id = unlist(phen_info$sensor_id)
    )
    phens <- phens[phen_name %in% pn]
    phens[offset %in% "no_spec"]$offset <- NA

    # determine record interval - function run twice to account for
    # FALSE intervals at position one and two of the data
    dri <- ipayipi::record_interval_eval(
      dt = dta$date_time, dt_format = dt_format, dt_tz = dt_tz,
      dta_in = dta, remove_prompt = remove_prompt,
      record_interval_type = record_interval_type
    )
    dta <- dri$new_data
    dri <- ipayipi::record_interval_eval(
      dt = dta$date_time, dt_format = dt_format, dt_tz = dt_tz,
      dta_in = dta, remove_prompt = remove_prompt,
      record_interval_type = record_interval_type
    )

    dta <- dri$new_data
    # finalize the data_summary
    data_summary <- data.table::data.table(
      dsid = as.integer(1),
      file_format = as.character(head_info_i$file_format),
      uz_station = as.character(head_info_i$station_title),
      location = as.character(head_info_i$location),
      station = NA_character_,
      stnd_title = NA_character_,
      start_dttm = min(dta$date_time),
      end_dttm = max(dta$date_time),
      logger_type = as.character(head_info_i$logger_type),
      logger_title = as.character(head_info_i$logger_title),
      logger_sn = as.character(head_info_i$logger_sn),
      logger_os = as.character(head_info_i$logger_os),
      logger_program_name = as.character(head_info_i$logger_program_name),
      logger_program_sig = as.character(head_info_i$logger_program_sig),
      uz_record_interval_type = dri$record_interval_type,
      uz_record_interval = dri$record_interval,
      record_interval_type = NA_character_,
      record_interval = NA_character_,
      uz_table_name = as.character(head_info_i$table_name),
      table_name = NA_character_,
      nomvet_name = NA_character_,
      file_origin = file.path(pipe_house$source_dir, basename(file_path))
    )
    # generate a logger interference table
    logg_interfere <- data.table::data.table(
      id = as.integer(seq_len(2)),
      date_time = c(data_summary$start_dttm, data_summary$end_dttm),
      logg_interfere_type = as.character(rep(logg_interfere_type, 2))
    )
    # generate a phenomena by data summary table
    phen_data_summary <- data.table::data.table(
      phid = as.integer(phens$phid),
      # removed dsid as it takes up too many rows and slows joins when
      #  generating phenomena data summaries
      # dsid = as.integer(rep(as.integer(data_summary$dsid),
      #    nrow(phens))),
      start_dttm = rep(data_summary$start_dttm, nrow(phens)),
      end_dttm = rep(data_summary$end_dttm, nrow(phens)),
      table_name = NA_character_
    )
  }
  ipayipi_data_raw <- list(data_summary = data_summary, raw_data = dta,
    phens = phens, phen_data_summary = phen_data_summary,
    logg_interfere = logg_interfere)
  class(ipayipi_data_raw) <- "ipayipi_raw_data"
  return(list(ipayipi_data_raw = ipayipi_data_raw, err = FALSE))
}