#' @title xle log
#' @description A function to generate an __xle file log__ of all present files
#'  in a folder.
#' @param log_dir Path to the folder for which to generate a log. This must be
#'  the folder which contains the xle files.
#' @param xlefiles A vector of file names passed to this function.
#' @param place Directory in which this log function operates.
#' @keywords log
#' @return A data.table of all xle files in a directory with basic
#'  logger and data metadata.
#' @author Paul J Gordijn
#' @export
#' @details
#' In addition to the log, that is, an inventory of xle files listed in the
#'  directory the following metadata is extracted and appended in the log:
#' \itemize{
#'  \item{"Location name"}{That is the site name where the borehole/piezometer
#'  is deployed}
#'  \item{"Borehole name"}{In the nomenclature under the hierarchy of the
#'  'Location name'}
#'  \item{"SN"}{That is Serial Number of the solonist or other logger}
#'  \item{"Start"}{The date when the logger was programmed to begin collecting
#'  data}
#'  \item{"End"}{The date when the logger was programmed to stop collecting
#'    data}
#'  \item{"File name"}{The original name of the solonist xle file from which the
#'  respective data was extracted.}
#'  \item{"Place"}{The folder/directory from which the data was extracted/
#'  originally located}
#' }
#' @examples
#' # create an inventory of the xle files in the working directory
#' # first use internal gw_xle_list function to get vector of xle files in a
#' # directory.
#' xlefiles <- gw_xle_list(input_dir = dir, recurr = FALSE)
#' gw_xle_log(log_dir = ".", xlefiles = xlefiles, place = ".")
gw_xle_log <- function(
  log_dir = NULL,
  xlefiles = NULL,
  place = NULL,
  ...) {

  # check if there are files listed
  if (length(xlefiles) == 0) {
    cr_msg <- padr(core_message =
      " No xle files in the directory or list ",
      wdth = 80, pad_char = "-", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(0, 0))
    stop(cr_msg)
  }
  xlefiles <- basename(xlefiles)
  loglist <- lapply(xlefiles, function(x) {
    top <- attempt::try_catch(
        XML::xmlRoot(XML::xmlParse(file.path(log_dir, x))),
      .e = ~ stop(.x),
      .w = ~ warning("XML content inappropriate or does not exist.
        Try xle_import to standardise encoding to UTF-8. ", .x)
  )
  # Extract logger metadata
  loc_name <-
    XML::xmlValue(top[["Instrument_info_data_header"]][["Project_ID"]])
  bh_name <-
    XML::xmlValue(top[["Instrument_info_data_header"]][["Location"]])
  enddate <-
    XML::xmlValue(top[["File_info"]][["Date"]])
  endtime <-
    XML::xmlValue(top[["File_info"]][["Time"]])
  startdate <-
    XML::xmlValue(top[["Instrument_info_data_header"]][["Start_time"]])
  sn <- XML::xmlValue(top[["Instrument_info"]][["Serial_number"]])
  st_dt <- startdate
  ed_dt <- paste0(enddate, " ", endtime)

  logdta <- data.frame(
    Location = loc_name,
    Borehole = bh_name,
    SN = sn,
    Start = as.POSIXct(st_dt),
    End = as.POSIXct(ed_dt),
    file.name = x
  )
  })

  # Use data.table package to convert a list into a data frame -> data table
  logdta <- data.table::rbindlist(loglist, use.names = TRUE)
  logdta <- transform(logdta,
    Location = as.character(Location),
    Borehole = as.character(Borehole),
    SN = as.character(SN),
    Start = as.POSIXct(as.character(Start)),
    End = as.POSIXct(as.character(End)),
    file.name = as.character(file.name),
    place = as.character(place)
  )
  logdta <- logdta[order(Location, Borehole, SN, Start, End)]
  # Return the logdta data table
  return(logdta)
}