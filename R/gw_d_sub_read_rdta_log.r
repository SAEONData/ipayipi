#' @title Reads in R data log csv file
#' @description This function just reads in the rdta_log csv file and converts
#'  to rds if all barologger file names were specified using an editor of user
#'  preferance.
#' @param input_dir Directory in which both the csv and R data files
#'  are located.
#' @param file File name of the csv to be read.
#' @author Paul J Gordijn
#' @keywords data log
#' @return A data.table of the logger files in a directory.
#' @export
#' @details The 'batch_baro' function uses this log file to search for
#' specified barologger files for the barometric compensation. When running
#' the 'batch_baro' function.
gw_read_rdta_log <- function(
  input_dir = NULL,
  file = NULL,
  ...) {

  fl_dir <- file.path(input_dir, basename(file))
  # Read in the csv file
  rdta_log <- read.csv(fl_dir, row.names = 1, header = TRUE)

  # Convert to table format
  rdta_log <- data.table(
    Location = as.character(rdta_log$Location),
    Borehole = as.character(rdta_log$Borehole),
    SN = as.character(rdta_log$SN),
    Model = as.character(rdta_log$Model),
    Start = as.POSIXct(as.character(rdta_log$Start)),
    End = as.POSIXct(as.character(rdta_log$End)),
    file.name = as.character(rdta_log$file.name),
    place = as.character(rdta_log$place),
    Baro_name = as.character(rdta_log$Baro_name),
    Rfile_class = as.character(rdta_log$Rfile_class)
  )

  # Check that there are no NA values in the baro name column
  cans <- nrow(rdta_log[which(rdta_log$Rfile_class == "level_file" &
    is.na(rdta_log$Baro_name)), ])

  if (cans > 0) {
    message("Missing barologger names in the rdta_log!")
    message("Please edit the csv file to remove NAs.")
  }
  if (cans == 0) {
    saveRDS(rdta_log, file.path(input_dir, "rdta_log.rds"))
  }
  return(rdta_log)
}