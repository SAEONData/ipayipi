#' @title Open and prepare event metadata database.
#' @description Reads standardised metadata and appends records the matching station.
#' @param input_dir Directory in which to search for the metadata database.
#' @param meta_file File name of the standardised rainfall metadata database or a meta_file table object.
#' @param col_dlm The column delimter of the 'flat' logger file which is fed to `data.table::fread()` or `base::read.csv()`.`
#' @param col_names Names of the columns (as a vector of strings. If provided then this is used to check against the data being read. If a column is missing/misspelt then data will not be read.
#' @param col_types Inspired by `googlesheets4` --- string of data indicating the data tye of each column. The following formats are supported:
#'   - l: Logical,
#'   - i: Integer,
#'   - d or n: Numeric, or "double",
#'   - D*: Date (parsed as a date --- format as d_format,
#'   - t*: Time of day (time is parsed as a lubridate period --- format
#'        as per `t_format`),
#'   - T: POSIXct date-time (format as per `dt_format`), &
#'   - c: Character.
#'   - f: Factor.
#'  * not yet implemented.
#' @param dt_format The function attempts to work out the date-time format from a vector of format types supplied to this argument. The testing is done via `lubridate::parse_date_time()`. `lubridate::parse_date_time()` prioritizes the tesing of date-time formats in the order vector of formats supplied. The default vector of date-time formats supplied should work well for most logger outputs.
#' @param dt_tz Recognized time-zone (character string) of the data locale. The default for the package is South African, i.e., "Africa/Johannesburg" which is equivalent to "SAST".
#' @param d_format String indicating the order of the year month and date to be parsed by `lubridate`. Not yet implemented.
#' @param T_format String indicating time format, e.g., hms --- see lubridate. Not yet implemented.
#' @param output_dir Directory where meta file should be saved.
#' @param output_name Name of the file for saving as an RDS file. The default extension is ".rmds".
#' @param event_thresh The event threshold provided in seconds. Argument used to prepare event database for gap and pseudo event evaluation by populating NA values in the 'event_thresh_s' field by the argument value provided here. The defult is ten minutes, that is 10 * 60 seconds. _see_ `ipayipi::gap_eval()`.
#' @keywords logger data processing; field metadata; data pipeline; supplementary data; field notes
#' @author Paul J. Gordijn
#' @return Standardised data table of events.
#' @details Reads in an events database or sheet in 'csv' format. Checks that column names have been standardised. Transforms the date-time columns to a standardised format --- ** this format and timezone must match that used by the data pipeline **.
#'
#'  The metadata table rows must have an assicatiated time stamp with header "date_time". Additional date-time columns are permitted e.g., start and end date_times, as, 'start_dttm' and 'end_dttm', respectively.
#' @md
#' @export
meta_read <- function(
  input_dir = ".",
  meta_file = NULL,
  col_dlm = ",",
  col_names = NULL,
  col_types = NULL,
  dt_format = c(
    "Ymd HMS", "Ymd IMSp",
    "ymd HMS", "ymd IMSp",
    "mdY HMS", "mdy IMSp",
    "dmY HMS", "dmy IMSp",
    "Ymd HMOS", "Ymd IMOSp",
    "ymd HMOS", "ymd IMOSp",
    "mdY HMOS", "mdy IMOSp",
    "dmY HMOS", "dmy IMOSp"
  ),
  dt_tz = "Africa/Johannesburg",
  d_format = "ymd",
  T_format = "hms",
  sheet = NULL,
  output_dir = ".",
  output_name = NULL,
  event_thresh = 600,
  ...
) {
  # avoid no visible bind for data.table variables
  "%like%" <- ":=" <- ".SD" <- "event_thresh_s" <- "date_time" <-
    "start_dttm" <- "end_dttm" <- NULL
  # if we need to read in object
  if (is.character(meta_file)) {
    meta_file <- file.path(input_dir, meta_file)
    if (!file.exists(meta_file)) {
      stop("The events metadata database does not exist!")
    }
    edb <- attempt::attempt(data.table::fread(file = meta_file, header = TRUE,
      check.names = FALSE, blank.lines.skip = TRUE, sep = col_dlm,
      id_col = FALSE, strip.white = TRUE, fill = TRUE, ...
    ))
    # if there was an error then we try and read the file using base r
    if (attempt::is_try_error(edb)) {
      edb <- attempt::attempt(data.table::as.data.table(read.csv(
        meta_file, header = TRUE, colClasses = "character"
      )))
      if (attempt::is_try_error(edb)) {
        stop("Failed reading file using base R.")
      }
    }
  }
  if (any(class(meta_file) %in% c("tibble", "data.frame"))) {
    edb <- data.table::as.data.table(meta_file)
  }
  # check the column names
  if (!is.null(col_names) && !any(col_names %in% names(edb))) {
    stop("Column names don\'t match defined \`colnames\`!")
  } else {
    col_names <- names(edb)
  }
  # select columns
  edb <- edb[, col_names, with = FALSE]
  # organise column formats
  if (nchar(col_types) != length(names(edb))) {
    stop("Mismatch between the number of columns and column formats!")
  }
  if (!is.null(col_types)) {
    z <- sapply(seq_len(nchar(col_types)), function(i) {
      substr(x = col_types, i, i)
    })
    rfm <- c("l", "i", "d", "n", "T", "c", "f")
    if (!any(z %in% rfm)) {
      m <- paste0("Unrecognised col_types: ", paste(
        unlist(unlist(z[!z %in% rfm]), "!"), collapse = ", "
      ), collapse = ""
      )
      stop(m)
    }
    names(z) <- names(edb)
    vars <- names(z)[z %in% "i"]
    if (length(vars)) {
      edb[, (vars) := lapply(.SD, as.integer), .SDcols = vars]
    }
    vars <- names(z)[z %like% "n|d"]
    if (length(vars)) {
      edb[, (vars) := lapply(.SD, as.numeric), .SDcols = vars]
    }
    vars <- names(z)[z %like% "c|f|T"]
    if (length(vars)) {
      edb[, (vars) := lapply(.SD, as.character), .SDcols = vars]
    }
    vars <- names(z)[z %like% "f"]
    if (length(vars)) {
      edb[, (vars) := lapply(.SD, as.factor), .SDcols = vars]
    }
    vars <- names(z)[z %like% "l"]
    if (length(vars)) {
      edb[, (vars) := lapply(.SD, as.logical), .SDcols = vars]
    }
    vars <- names(z)[z %like% "T"]
    if (length(vars)) {
      edb[, (vars) := lapply(.SD, function(x) {
        lubridate::parse_date_time2(x = x, orders = dt_format, tz = dt_tz)
      }), .SDcols = vars]
    }
  }

  # standard event threshold information
  if (all(
    c("date_time", "start_dttm", "end_dttm", "event_thresh_s") %in% names(edb)
  )) {
    edb[is.na(event_thresh_s), "event_thresh_s"] <- event_thresh
    edb[!is.na(date_time) & is.na(start_dttm), "start_dttm"] <-
      edb[!is.na(date_time) & is.na(start_dttm), ]$date_time - event_thresh
    edb[!is.na(date_time) & is.na(end_dttm), "end_dttm"] <-
      edb[!is.na(date_time) & is.na(end_dttm), ]$date_time + event_thresh
  }

  if (!is.null(output_name)) {
    saveRDS(edb, paste0(file.path(output_dir, output_name), ".rmds"))
  }
  invisible(edb)
}