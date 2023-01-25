#' @title Extract rainfall data from 'master' files
#' @description Reads Hoboware csv outputs that have been collated in an
#'  excel file. The format of the excel file (esp. spacing of files and
#'  column headers need to be considered carefully---see example data).
#'  The excel data needs to be converted to 'csv' format before importing.
#' @param input_dir The directory in which the file is stored.
#' @param m_file The 'master' filename (excluding the directory).
#' @param ptitle The standardised title name of the rain gauge station. This
#'  value has to be supplied.
#' @keywords hoboware, tipping bucket rain gauge, data import, master file
#' @author Paul J. Gordijn
#' @return List of seperate hoboware rainfall exports in seperate data frames
#'  with no formatting applied.
#' @details This function returns the 'raw' hoboware file csv or text exports
#'  which have been appended in excel. It is critical that a blank line is
#'  placed between each csv export, the 'plot title' is placed below this
#'  blank line, then below the title the column headers from the export. The
#'  '#' needs to be retained (as the column header for the id row) as it
#'  signals the start of a new hobo download file.
#'  The exports that this function generates can be read by clean_rain_hobo().
#'  See the example data: "1C Raingauge_Masterfile_20211216.xlsx".
#' @export
rain_master <- function(
  input_dir = ".",
  m_file = NULL,
  ptitle = NA,
  ...
) {
  if (is.na(ptitle)) stop("ptitle not supplied!")
  # read excel file
  mc <- read.csv(file.path(input_dir, m_file))
  mc$tid <- seq_len(nrow(mc)) # create an id row

  # the hash in the file is reserved for new downloads/files
  mc$newhobo <- ifelse(mc[, 1] == "#", "new_file", NA)
  # count the number of 'new files'
  fileshere <- nrow(mc[which(mc$newhobo == "new_file"), ])
  # get the starting row number of each new file
  start_rows <- mc[which(mc$newhobo == "new_file"), ]$tid
  # get the end row: nb! assumes a certain excel file layout
  # --- see the linked file
  end_rows <- c(start_rows[2:length(start_rows)] - 3, nrow(mc))

  # add in a column wherein each download event is numbered
  for (i in seq_len(fileshere)) {
    mc[which(mc$tid %in% start_rows[i]:end_rows[i]), "dwn_event"] <- i
  }

  # split data.frame into a list containing a table for each download event
  dfs <- split.data.frame(mc, mc$dwn_event)
  dfs <- lapply(dfs, function(x) data.table::as.data.table(x))
  dfs <- list(
    dir = input_dir,
    m_file = m_file,
    ptitle = ptitle,
    dwn_list = dfs
  )
  names(dfs) <- c("dir", "m_file", "ptitle", "dwn_list")
  return(dfs)
}