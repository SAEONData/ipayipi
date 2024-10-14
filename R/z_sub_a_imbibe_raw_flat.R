#' @title Imbibes logger data exports
#' @md
#' @description Function to read in 'flat' loggers files into R. A first step towards processing data in `ipayipi`.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param file_path Path and name of file (excluding the file extension).
#' @param file_ext The file extension defaults of the raw logger data files. This can be left as `NULL` so 'all' files but those with extensions that cannot be imbibed (".ipr|.ipi|.iph|.xls|.rps|.rns|.ods|.doc").
#' @param col_dlm The column delimter which is fed to `data.table::fread()`. Defaults to NULL. When `NULL` the function uses `data.table::fread` ability to 'guess' the delimeter.
#' @param dt_format The function attempts to work out the date-time format from a vector of format types supplied to this argument. The testing is done via `lubridate::parse_date_time()`. `lubridate::parse_date_time()` prioritizes the tesing of date-time formats in the order vector of
#'  formats supplied. The default vector of date-time formats supplied should work well for most logger outputs.
#' @param dt_tz Recognized time-zone (character string) of the data locale. The default for the package is South African, i.e., "Africa/Johannesburg" which is equivalent to "SAST".
#' @param record_interval_type If there are is no discrete record interval set in the logger program, i.e., the sampling is event-based, then this parameter must be set to "event_based". By default this function has this parameter set to "continuous", but the record interval is scrutinized by 'ipayipi::record_interval_eval()' --- see the help files for this function for more information.
#'  The parameter supplied here is only used if there is only one data record and the record interval cannot be evaluated by `ipayipi::record_interval_eval()`.
#' @param data_setup List of options used to extract data and metadata from instrument data outputs. Mandatory fields are indicated with an '*'.
#'   File header options include\eqn{*^1}:
#'   1. *__file_format__ -- the native/raw file format.
#'   1. *__station_title__ -- the supplied instrument station title \eqn{*^2}.
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
#'   \eqn{*^1} These options must be supplied as a charater string or the row and column index provided as for example, rici(ri = 1, ci = 2). See `?ipayipi::rici()` for more details.
#'   \eqn{*^2} If this option is supplied as "!fp!" the `station_title` will extracted from the base name of the file path with digits, and leading/training white spaces and underscores removed. Note digits (integers) are only replaced when there are more than three in the string---to remove date-time information from the title.
#'
#'   1. *__date_time__ -- Only the column index must be provided here as an integer, e.g., 3. If the date and time are serperated in different columns, columns must be provided in a logical order as these are concatenated in the provided order.
#'   1. dttm_inc_exc -- Logical vector of length two. Defaults to `c(TRUE, FALSE)`. Used to define inclusive or exclusvie time intervals, and whether to change these (see the details section for more).
#'
#'  File phenomena information and data \eqn{*^3}:
#'   1. *__phen_name__ -- a list of row and column numbers corresponding to the names of phenomena (variables).
#'   1. phen_unit -- a list of row and column numbers corresponding to the names of phenomena units.
#'   1. phen_var_type -- a vector of character strings designating the type of variable for each phenomenon.
#'   1. phen_measure -- a list of row and column numbers corresponding to the type of measurement calculated by the logger for each phenomena, e.g., an 'average', 'sample', 'minimum', etc.
#'   1. phen_offset -- a list of offset values that have been pre-applied to the data, i.e, the offset is only noted and not used to transform the data.
#'   1. sensor_id -- a list of row and column numbers corresponding to sensor unique id values. Otherwise a vector of character strings designating the type of 'sensor_id' for each phenomenon.
#'
#'  \eqn{*^3} These options must be supplied as using `ipayipi::rng_rici()`, or input as a vector of character strings with the actual values. If using     `ipayipi::rng_rici()`, at least the row in which phenomena details are found and the columns wherein these lie are required. See ?ipayipi::rng_rici() for more details.
#'
#'   1. *__data_row__ -- a single integer value designating the row where phenomena data begin from.
#'   1. id_col -- a single integer value designating a data row unique identifier row.
#' 
#' NB! Note that for Solonist xle files there is a prebuilt 'data_setup' `ipayipi::solonist`. If the `file_path` is for an 'xle' file, and `data_setup` is null, then this default data_setup will be used.
#' 
#' @param remove_prompt Logical; passed to `ipayipi::record_interval_eval()`. Activate a readline prompt to choose whether or not filter our records from `dta_in` with inconsistent record intervals.
#' @param logg_interfere_type Two options here: "remote" or "on_site". Each time a logger is visited is counted as a logger interference event. Type _'remote'_ occurs when data is downloaded remotely. Type _'on_site'_ is when data was downloaded on site. _See details_ ...
#' @param verbose Logical passed to `attempt::attempt()` which reads the logger text file in with either `data.table::fread()` or base R. Also whether to print progress.
#' @details This function uses `data.table::fread` which is optimized for processing 'big data'. Apart from usual the usual options which can be parsed to `data.table::fread` this function generates some standardised metadata to complement the read from a logger data table (if `data.table::fread()` is unsuccessful `base::read.csv()` is used). This metadata may vary from one logger output to another. To cater for this variation this function requires a `data_setup` to be completed. Once setup this can be used as a standard for further imports.
#'  This function also attempts to check whether the recording interval in the data date-time stamp has been consistent. A prompt is called if there are inconsistent time intervals between record events, and data rows with inconsistent time intervals will be removed if approved.
#'  A basic check is performed to check the success of converting date-time values to a recognised format in R (i.e., POSIXct).
#'  Regarding the `logg_interfere_type` parameter. Owing to potential interference of sensors etc when downloading data 'on site' or logger related issues when data is sent/obtained remotely, the date-time stamps of these events must be preserved. A `logg_interfere` data table is generated for this purpose and stored with the data. This data cannot necessarily be extracted from the 'data_summary' once data has been appended as some of this data will be overwritten during the appending process. The purpose of the 'logg_interfere' table is to retain this information, which is used by `ipayipi` for further processing.
#' Regarding the `dttm_inc_exc`, vector of length two. The _first_ logical element indicates whether the date-time of raw-continuous data represents the starting or ending date-time of the recording interval. General convention (ISO 8601 standard) states that a time-interval measurements begins from point zero, and therefore, the starting timestamp captures all future recordings until the next timestamp. Therefore, all measurements on day one, include all measurements/scans until just before day two, i.e., from 1.000 to 1.999, as a simple example (limited to 4 significant figures). This can be written as [1.000,2.000), and is also known as the half-open approach. The _second_ logical element indicates whether the reverse of the first supplied argument must be applied, i.e., should the half-open be converted to (]---exclusive of the first teimstamp and exclusive of the last. Cambell Scientific loggers often use an the (] approach that can be converted to [) by supplying `c(FALSE, TRUE)` to the `dttm_inc_exc` parameter, Defaults to `c(TRUE, FALSE)`.
#' @return A list of class "ipayipi_raw_data" that contains a 'data_summary', 'phens' (phenomena), and 'raw_data' tables (data.table).
#' @export
#' @author Paul J. Gordijn
imbibe_raw_flat <- function(
  file_path = NULL,
  file_ext = NULL,
  col_dlm = NULL,
  dt_format = c(
    "Ymd HMOS", "Ymd HMS",
    "Ymd IMOSp", "Ymd IMSp",
    "ymd HMOS", "ymd HMS",
    "ymd IMOSp", "ymd IMSp",
    "mdY HMOS", "mdY HMS",
    "mdy HMOS",  "mdy HMS",
    "mdy IMOSp",  "mdy IMSp",
    "dmY HMOS", "dmY HMS",
    "dmy IMOSp", "dmy IMSp"
  ),
  dt_tz = "Africa/Johannesburg",
  data_setup = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  "%ilike%" <- ".N" <- NULL
  "phen_name" <- NULL

  # read flat
  file <- attempt::attempt(data.table::fread(file = file_path, header = FALSE,
      check.names = FALSE, blank.lines.skip = FALSE, sep = col_dlm,
      showProgress = xtra_v, strip.white = FALSE, fill = TRUE
    ), silent = TRUE
  )
  # if there was an error then we try and read the file using base r
  if (attempt::is_try_error(file) || ncol(file) == 1) {
    # use auto fread
    dyno_read <- function(file_path = NULL, ...) {
      file <- data.table::fread(file_path, header = FALSE, ...)
      l <- R.utils::countLines(file_path)[1]
      file_head <- readLines(file_path)[1:(l - nrow(file))]
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
    ipayipi::msg("Failed reading file check recognised file format", xtra_v)
    ipayipi::msg(paste0("Use \`imbibe_raw_batch()\` for processing ",
        "files in the \'wait_room\'.", collapse = ""
      ), xtra_v
    )
    return(list(ipayipi_data_raw = file, err = TRUE))
  }
  data_setup_names <- c(
    # *1           # *2             # 3         # *4
    "file_format", "station_title", "location", "logger_type",
    # *5         # 6          # 7
    "logger_sn", "logger_os", "logger_program_name",
    # 8                   # 9             # 10
    "logger_program_sig", "logger_title", "table_name",
    # 11         # 12
    "date_time", "dttm_inc_exc",
    # *13        # 14         # 15             # 16
    "phen_name", "phen_unit", "phen_var_type", "phen_measure",
    # 17           # 18
    "phen_offset", "sensor_id",
    # 19      # 20
    "id_col", "data_row"
  )
  if (is.list(data_setup)) {
    # check that the defaults have been supplied
    if (!any(data_setup_names[c(1:2, 4:5, 11, 20)] %in% names(data_setup))) {
      ipayipi::msg(paste0("The following names were not supplied: ",
          data_setup_names[!data_setup_names[c(1:2, 4:5, 11, 20)] %in%
              names(data_setup)
          ], sep = "\n"
        ), xtra_v
      )
      ipayipi::msg("Supply all required information in the 'data_setup'!",
        xtra_v
      )
      return(list(ipayipi_data_raw = file, err = TRUE))
    }
    # check for unrecognised names
    if (any(!names(data_setup) %in% data_setup_names)) {
      ipayipi::msg(paste0("The following names were not supported: ",
          names(data_setup)[!names(data_setup) %in% data_setup_names],
          sep = "\n"
        ), xtra_v
      )
      ipayipi::msg("Unsupported names in the 'data_setup'!", xtra_v)
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
            length(head_info[[i]]$c_rng)
          )
        }
        if (!is.null(head_info[[i]]$string_extract)) {
          file_extract <- as.vector(unlist(
            file[head_info[[i]]$r_rng, head_info[[i]]$c_rng, with = FALSE]
          ))
          rng_pattern <- head_info[[i]]$string_extract$rng_pattern
          strxy <- file_extract %ilike% rng_pattern
          if (any(strxy)) {
            names(strxy) <- seq_along(strxy)
            ex_pos <- as.integer(names(strxy[which(strxy)])[1])
            rel_start <- head_info[[i]]$string_extract$rel_start
            rel_end <- head_info[[i]]$string_extract$rel_end
            head_info[[i]] <- substr(file_extract[ex_pos], # extract by pos
              unlist(gregexpr(rng_pattern, file_extract[ex_pos])) + rel_start,
              unlist(gregexec(rng_pattern, file_extract[ex_pos])) + rel_end
            )
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
    if (head_info_i$station_title %in% "!fp!") {
      head_info_i$station_title <- gsub(
        paste0(file_ext, "|^_+|_+$|[.]"), "", basename(file_path)
      )
      head_info_i$station_title <- gsub(
        paste0(file_ext, "|^_+|_+$|[.]"),
        "", head_info_i$station_title
      )
      # replace numeric digits if there are more than three
      if (nchar(gsub("[^0-9]", "", head_info_i$station_title)) > 3) {
        head_info_i$station_title <- gsub(
          paste0(file_ext, "|^_+|_+$|[.]|[[:digit:]]+"),
          "", head_info_i$station_title
        )
      }
    }
    # remove special characters from station title
    head_info_i$station_title <- gsub(
      "[[:space:]]", "_", head_info_i$station_title
    )
    head_info_i$station_title <- gsub(
      "[[:space:]]", "_", head_info_i$station_title
    )
    head_info_i$station_title <- gsub(
      "[^[:alnum:]'_']", "", head_info_i$station_title
    )
    head_info_i$station_title <- gsub(
      "_+$|^_+", "", head_info_i$station_title
    )
    # summarise phenomena info
    phen_names <- data_setup_names[13:18]
    phen_info <- data_setup[names(data_setup) %in% phen_names]
    phen_info_ij <- lapply(seq_along(phen_info), function(i) {
      if (any(class(phen_info[[i]]) %in% "ipayipi rng")) {
        if (phen_info[[i]][["c_rng"]][2] == "extract") {
          phen_info[[i]]$c_rng <- phen_info[[i]][["c_rng"]][1]:ncol(file)
          phen_info[[i]]$r_rng <- rep(phen_info[[i]]$r_rng,
            length(phen_info[[i]]$c_rng)
          )
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
          dtsb <- file[phen_info_ij[[i]]$r_rng[z], ][[
            phen_info_ij[[i]]$c_rng[z]]
          ]
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
              function(q) length(q) != length(phen_info_i[[1]])
            )))
    ) {
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

    # extract data
    dta <- file[data_setup$data_row:nrow(file),
      phen_info_ij$phen_name$c_rng, with = FALSE
    ]
    names(dta) <- unlist(phen_info$phen_name)
    dta <- dta[, names(dta)[!names(dta) %in% ""], with = FALSE]
    # check dta names and return error if problematic
    nchk <- names(dta)[!names(dta) %in% "no_spec"]
    nchk <- sapply(nchk, function(x) {
      attempt::try_catch(expr = as.numeric(x), .w = ~TRUE, .e = ~TRUE)
    })
    if (length(nchk[nchk != FALSE]) == 0) {
      ipayipi::msg(
        x = "Failure to render phen names -- check phen name row in data_setup",
        verbose = xtra_v
      )
      return(list(ipayipi_data_raw = file, err = TRUE))
    }
    cat_cols <- names(file)[data_setup$date_time]
    file[, date_time := do.call(paste, .SD), .SDcols = cat_cols]
    date_time <- attempt::attempt(lubridate::parse_date_time(
      x = file[data_setup$data_row:nrow(file), ][,
        date_time := do.call(paste, .SD), .SDcols = cat_cols
      ]$date_time, orders = dt_format, tz = dt_tz
    ))
    if (attempt::is_try_error(date_time) && xtra_v) {
      ipayipi::msg("Problem with reading date-time values")
      print(file[data_setup$data_row:nrow(file), ][[
        data_setup$date_time
      ]][1:5])
      message("Read as:")
      print(date_time[1:5])
      return(list(ipayipi_data_raw = file, err = TRUE))
    }
    dta$date_time <- date_time
    if (is.null(data_setup$id_col)) {
      id <- seq_len(nrow(file[data_setup$data_row:.N]))
    } else {
      id <- as.numeric(file[data_setup$data_row:nrow(file), ][[
        data_setup$id_col
      ]])
    }
    dta$id <- id

    # order the columns
    # phen names
    pn <- c("id", "date_time", unlist(phen_info$phen_name)[
      !unlist(phen_info$phen_name) %in% ""
    ])
    pn <- pn[!is.na(pn)]
    dta <- dta[, pn, with = FALSE]

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

    # remove no spec phen names
    dta <- dta[, names(dta)[!names(dta) %in% "no_spec"], with = FALSE]
    phens <- phens[!phen_name %in% "no_spec"]

    # check the data half open statement
    if (is.null(data_setup$dttm_inc_exc)) {
      data_setup$dttm_inc_exc <- c(TRUE, FALSE)
    }
    if (length(data_setup$dttm_inc_exc) == 1) {
      ipayipi::msg("Error: Check \'dttm_inc_exc\' parameter!", xtra_v)
      return(list(ipayipi_data_raw = file, err = TRUE))
    }
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
      uz_record_interval_type = NA_character_,
      uz_record_interval = NA_character_,
      record_interval_type = NA_character_,
      record_interval = NA_character_,
      dttm_inc_exc = data_setup$dttm_inc_exc[1],
      dttm_ie_chng = data_setup$dttm_inc_exc[2],
      uz_table_name = as.character(head_info_i$table_name),
      table_name = NA_character_,
      nomvet_name = NA_character_,
      file_origin = NA_character_
    )
  }
  ipayipi_data_raw <- list(data_summary = data_summary, raw_data = dta,
    phens = phens
  )
  class(ipayipi_data_raw) <- "ipayipi_raw_data"
  return(list(ipayipi_data_raw = ipayipi_data_raw, err = FALSE))
}