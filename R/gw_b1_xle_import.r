#' @title xle import function
#' @description Copy new data and prepare for preprocessing into the pipeline
#' @details The objectives of this function are:
#'  \enumerate{
#'    \item Copy raw file to a 'waiting room' folder where the raw data is
#'       temporarily stored.
#'       At his stage I have not enabled operation with .lev (old Solonist
#'       files). These can be exported as xle files from within Solonist.
#'       .lev files are a type of tab seperated value format.
#'       For more on this see the sub function to list .xle files.
#'    \item Standardise data encoding to UTF8 which is more usable.
#'      - raw files that have moved through here must not be re-processed
#'       to save time.
#'    \item NB! File renaming is based on the logger metadata NOT the .xle
#'      filename. Moreover, because the files are renamed with start and
#'      end dataes if the same logger is deployed twice on a given day with
#'      the same project ID and location it will be overwritten. It is
#'      therefore important to change these logger settings before
#'      deployment or after saving the file.
#' }
#' The name of the directory to be copied to must be customized to suite the
#'  user. Under my regime I will use a data drive on my network.
#'  The folder from which the raw files are being copied from is
#'  interactively selected by the user.
#' @param input_dir Directory where the files will be copied from.
#' @param wait_room Folder where ground water files can be temporarily stored
#'  and prepared for processing.
#' @param recurr Should the function recurrsively search in subdirectories
#'  for solonist files (.xle)?
#' @export
#' @author Paul J Gordijn
#' @return Screen printout of the xle files copied into the
#'  designated directory.
#' @keywords xle, xml, solonist, data pipe
#' @examples
#' ## import xle files into the groundwater level data pipeline. This is done
#' ## by bringing these into a temporary folder or "waiting room".
#' ## new_data_dir <- "./202104_sibhayi" # diectory from which to import data
#' ## wait_room <- "./temp" # temporary directory into which to introduce data
#'
#' ## use the import function. Note 'recurr' was set to TRUE so that the
#' ## function would import relevant xle files from subdirectories of the
#' ## 'input_dir'.
#' gw_xle_import(input_dir = new_data_dir, wait_room = wait_room, recurr = TRUE)
gw_xle_import <- function(
  input_dir = NULL,
  wait_room = NULL,
  recurr = TRUE,
  ...) {

  # Check defualts
  if (recurr == FALSE) {
    recurr    <- FALSE
  } else {
    if (recurr == TRUE) {
    recurr <- TRUE
    } else {
      stop("Recurr must be defined as a True|False variable")
    }
  }


  #####    - MAKE FILE LIST
  xlefiles_path <- gw_xle_list(input_dir = input_dir, recurr = recurr)
  if (length(xlefiles_path) == 0) {
    stop("There are noxle files in the input directory.")
    }

  cr_msg <- padr(core_message =
    paste0(" Importing ", length(xlefiles_path),
    " solonist files into the pipeline ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)

  sapply(xlefiles_path, function(z) {
    # Read in the xle file with appropriate encoding
    x <- xml2::read_xml(z, encoding = "ISO-8859-1")
    # Find and replace the temperature units into the correct format
    unit_nodes <- xml2::xml_find_all(x, "//Unit")
    new_units <- gsub("Deg C", replacement = "DegC",
      xml2::xml_text(xml2::xml_find_all(x, "//Unit")))
    xml2::xml_text(unit_nodes) <- new_units
    tfs <- grepl("C", xml2::xml_text(xml2::xml_find_all(x, "//Unit")),
      ignore.case = F)
    for (i in seq_len(length(tfs))) {
      if (tfs[i] == T) {
        new_units[i] <- "DegC"
        }
    }
    xml2::xml_text(unit_nodes) <- new_units

      # Find and replace the micro siemens per cm units in correct format
      tfs <- grepl("S/cm", xml2::xml_text(xml2::xml_find_all(x, "//Unit")),
        ignore.case = F)
      for (i in seq_len(length(tfs))) {
        if (tfs[i] == T) {
          new_units[i] <- "MicroS_per_cm"
          }
      }
      xml2::xml_text(unit_nodes) <- new_units

      file_name_node <- xml2::xml_find_first(x, "//FileName")
      xml2::xml_text(file_name_node) <- basename(z)

      enddate <- xml2::xml_text(xml2::xml_find_first(x,
        xpath = ".//File_info/Date"))
      enddate <- gsub(enddate, pattern = "/", replacement = "")
      startdate <-
        substr(xml2::xml_text(xml2::xml_find_first(x,
          ".//Instrument_info_data_header/Start_time")), 1, 10)
      startdate <- gsub(startdate, pattern = "/", replacement = "")
      bh_name <- xml2::xml_text(xml2::xml_find_first(x,
        ".//Instrument_info_data_header/Project_ID"))
      bh_name <- gsub(bh_name, pattern = "/", replacement = ".")
      loc_name <- xml2::xml_text(xml2::xml_find_first(x,
        xpath = ".//Instrument_info_data_header/Location"))
      loc_name <- gsub(loc_name, pattern = "/", replacement = ".")

      xml2::write_xml(x, file = file.path(
        wait_room, paste0(loc_name, "_", bh_name, "_",
        startdate, "-", enddate, ".xle")), encoding = "UTF-8")
      cr_msg <- padr(core_message = paste0(" ", basename(z),
        " --> ", loc_name, "_", bh_name, "_",
          startdate, "-", enddate, ".xle", collapse = ""),
        wdth = 80, pad_char = "-",  pad_extras = c("|-", "", "", "-|"),
        force_extras = TRUE, justf = c(-1, 0))
      message(cr_msg)
  }
    )
  cr_msg <- padr(core_message = " Import complete ",
    wdth = 80, pad_char = "=",  pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
}