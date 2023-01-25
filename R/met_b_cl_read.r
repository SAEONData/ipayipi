#' @title Read cambell logger data exports
#' @description Function to read in Cambell logger '.dat' files in R.
#' @details 
#' @export
#' @author Paul J. Gordijn
#' 1. Read in file.
#' 1. Standardise units and generate file metadata in preparation for further
#'  data checks and station standardisation.
met_cl_read <- function(
  file_path = NULL,
  file_title_row = 1,
  phen_name_row = 2,
  phen_units_row = 3,
  phen_measure = 4,
  phen_first_col = 3,
  data_row_start = 5,
  dt_col = 1,
  dt_format = "%y-%m-%d %H:%M:%S",
  dt_tz = "Africa/Johannesburg",
  id_col = 2,
  file_ext = ".dat",
  file_sep_dlm = NULL,
  row_names = FALSE,
  ...
) {
  # check file delimiter
  if (file_ext == ".dat" && is.null(file_sep_dlm)) file_sep_dlm <- ","
  file <- read.table(file = file_path, header = FALSE,
    check.names = FALSE, blank.lines.skip = FALSE, sep = file_sep_dlm,
    row.names = row_names)
  # check the output format from the logger --- currently only the long header
  #  table export format supported. See the loggernet manual.
  if (file[1, 1] == "TOA5") {
    # get file header info
    file_title_row <- file[file_title_row, ]
    file_title_row <- file_title_row[!file_title_row[1, ] == ""]
    data_summary <- data.table::data.table(
      file_format = as.character(file_title_row[1]),
      uz_station = as.character(file_title_row[2]),
      stnd_title = NA_character_,
      location = NA_character_,
      station = NA_character_,
      start_dt = as.POSIXct(NA_character_, tz = dt_tz),
      end_dt = as.POSIXct(NA_character_, tz = dt_tz),
      logger = as.character(file_title_row[3]),
      logger_sn = as.character(file_title_row[4]),
      logger_prgm = as.character(file_title_row[5]),
      logger_title = as.character(file_title_row[6]),
      prog_sig = as.character(file_title_row[7]),
      record_interval = as.character(file_title_row[8]),
      file_origin = as.character(file_path)
    )
    # extract phenomena
    phens <- data.table::transpose(
      file[c(phen_name_row, phen_units_row, phen_measure),
        c(phen_first_col:ncol(file))])
    colnames(phens) <- c("phen_name", "units", "measure")
    met_data <- data.table::as.data.table(file[data_row_start:nrow(file), ])
    colnames(met_data)[c(phen_first_col:ncol(file))] <- phens$phen_name
    colnames(met_data)[c(dt_col, id_col)]  <- c("date_time", "id")
    met_data <- met_data[, c("id", "date_time", phens$phen_name),
      with = FALSE]
    met_data <- transform(met_data,
      id = as.integer(id),
      date_time = as.POSIXct(met_data$date_time, format = dt_format,
        tz = dt_tz))
    if (any(is.na(met_data$date_time))) stop("date_time conversion!")
  }
  met_data <- list(data_summary = data_summary, met_data = met_data,
    phens = phens)
  class(met_data) <- "SAEON_CS_prepare"
  invisible(met_data)
}
