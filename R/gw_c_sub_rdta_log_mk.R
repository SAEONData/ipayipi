#' @title Generate xle R data log file
#' @description Creates a log file of all RDS data files in a directory.
#' Designed for use within the package pipeline.
#' @param input_dir Directory for which to generate a log.
#' @param recurr Logical. Should the log be generated recursively i.e.,
#' for sub-directories.
#' @author Paul J. Gordijn
#' @keywords pipeline, log
#' @export
#' @return A metadata table of R data files.
#' @details The log this function creates extracts metadata from each
#' site where a logger is deployed. The metadata includes the site location
#' and borehole/piezometer name, serial numbers of the loggers that have been
#' deployed, the logger model, the start and end date times, the file name,
#' the location (directory) of the file, the barometer that should be used
#' to compensate the logger data (where relevant) and the class of the R
#' object, that is, either a 'baro_file' (barologger) or a 'level_file'
#' (water level and).
#' @examples
#' ## This function is a helper function not designed for isolated use.
#' ## Get list of solonist data files which have been converted to
#' # 'rds' format.
#' input_dir <- "./solR_dta" # set directory where processed data is maintained.
#' 
#' ## Generate log file
#' rdta_log_mk(input_dir = input_dir, recurr = FALSE)
  gw_rdta_log_mk <- function(
  input_dir = NULL,
  recurr = FALSE) {
  Rdtafiles <- gw_rdta_list(input_dir = input_dir, recurr = recurr)
  rd_log <- lapply(Rdtafiles, function(x) {
    f <- readRDS(x)
    if (length(f$log_baro_log$Baro_name) > 0) {
      baro <- f$log_baro_log$Baro_name[length(f$log_baro_log$Baro_name)]
    } else {
      baro <- NA
    }
    logdta <- data.table::data.table(
      Location = as.character(f$xle_LoggerHeader$Location[length(
        f$xle_LoggerHeader$Location
    )]),
      Borehole = as.character(f$xle_LoggerHeader$Project_ID[length(
        f$xle_LoggerHeader$Project_ID
    )]),
      SN = as.character(f$xle_LoggerInfo$Serial_number[length(
        f$xle_LoggerInfo$Serial_number
    )]),
      Model = as.character(f$xle_LoggerInfo$Model_number[length(
        f$xle_LoggerInfo$Model_number
    )]),
      Start = min(f$log_data$Date_time),
      End = max(f$log_data$Date_time),
      file.name = as.character(x),
      place = as.character("solr_room"),
      Baro_name = as.character(baro),
      Rfile_class = as.character(class(f))
    )

    logdta <- logdta[order(Location, Borehole, Start, End)]
    return(logdta)
  })
  rd_log <- data.table::rbindlist(rd_log)
  saveRDS(rd_log, file.path(input_dir = input_dir, "rdta_log.rds"))
  return(rd_log)
}
