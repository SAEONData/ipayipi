#' @title Check xle file nomenclature
#' @description A function to check standardisation of xle file names.
#' @details  Aims: Generate and/or update existing nomenclature tables
#'  merging the 'waiting room' data with vetted nomenclature data.
#'  \enumerate{
#'    \item Check data nomenclature.
#'    \item Compare nomenclature from (1) with an existing nomenclature table
#'       (if this exists).
#'    \item Output updated nomenclature table and highlight where manual
#'       edits are required.
#'    \item Save updated nomenclature table as usable if no manual edits are
#'       are required.
#' }
#'  Note that the double underscore "__" is treated as a special character to
#'  designate special logger types, i.e. salinity/conductivity loggers.
#' NB! If the embedded xml metadata nomenclature differs from the required
#'  standardisation, these will be changed accordingly. Encoding of
#'  the xle files is also standardised as UTF-8.
#' @param wait_room The working directory where the xle files are stored.
#' @param out_csv Logical. If TRUE a csv file is made in the working
#'  directory if there are files with unrecognised nomenclature.
#' @keywords nomenclature
#' @return returns a csv nomenclature file. Screen output notes whether
#'  the nomenclature table is complete.
#' @author Paul J. Gordijn
#' @export
#' @examples
#' # check the nomenclature of the files in the "waiting room" or any
#' # other folder with xle files against a standard list of names.
#' wait_room <- "./temp" # directory (temporary) with xle data
#' gw_xle_nomenclature(wait_room = wait_room, out_csv = TRUE)
#' # if there are unrecognized names then the function generates a csv
#' # in the directory specified. The updated csv can be used to update the
#' # "nomvet.rds" file (which in the pipeline is stored in the
#' # "waiting room" folder).
gw_xle_nomenclature <- function(
  wait_room = NULL,
  out_csv = TRUE,
  ...) {
  xlefiles_path <- gw_xle_list(input_dir = wait_room, recurr = FALSE)

  # The loop below runs off previously R processed .xle files

  # Check if a previous name standardisation table was saved in the
  #  working directory
  e_nomtab <- list.files(path = wait_room, pattern = "nomtab.rds",
    recursive = FALSE, full.names = TRUE)
  e_nomtab <- if (length(e_nomtab) > 0) {
    readRDS(file.path(wait_room, "nomtab.rds"))
    }
  data.table::setDT(e_nomtab)

  # Generates a lists of logger download events that include
  #  1 Location name
  #  2 Logger serial number
  #  3 Start Date time
  #  4 End Date time
  #  5 Logging interval
  #  6 Units of phenomena being measured
  # Then transformns lists into a data.table and csv in the
  #  data folder.

  # Make a table of all the data sets with their
  #  1. Location name -- this is saved in the 'PROJECT_ID' field.
  #  2. Borehole name -- this is saved in the 'Location' field.

  nom_list <- lapply(xlefiles_path, function(z) {
    x <- xml2::read_xml(z, encoding = "UTF-8")
    f_n <- xml2::xml_text(xml2::xml_find_first(x,
      xpath = ".//File_info/FileName"))
    loc_name <- xml2::xml_text(xml2::xml_find_first(x,
      xpath = ".//Instrument_info_data_header/Location"))
    loc_name <- gsub(loc_name, pattern = "/", replacement = ".")
    bh_name <-
      xml2::xml_text(xml2::xml_find_first(x,
        ".//Instrument_info_data_header/Project_ID"))
    bh_name <- gsub(bh_name, pattern = "/", replacement = ".")
    l_sn_log <- xml2::xml_integer(xml2::xml_find_first(x,
      xpath = ".//Instrument_info/Serial_number"))
    mod_sr <- xml2::xml_text(xml2::xml_find_first(x,
      xpath = ".//Instrument_info/Instrument_type"))
    mod_n <- gsub("[[:blank:]]", "",
      xml2::xml_text(xml2::xml_find_first(x,
        xpath = ".//Instrument_info/Model_number")))
    startdate <- substr(xml2::xml_text(xml2::xml_find_first(x,
      ".//Instrument_info_data_header/Start_time")), 1, 10)
    startdate <- gsub(startdate, pattern = "/", replacement = "")
    file_row <- data.table::data.table(
      Orig_FileName = as.character(f_n),
      Location_uz = as.character(loc_name),
      Borehole_uz = as.character(bh_name),
      SN = as.integer(l_sn_log),
      Instrument_type = as.character(mod_sr),
      Model = as.character(mod_n),
      SDT = as.character(startdate),
      Location = as.character(NA),
      Borehole = as.character(NA),
      b__special = as.character(""))
    return(file_row)
  }
  )
  nomtab <- data.table::rbindlist(nom_list, use.names = TRUE)
  nomtab <- nomtab[order(Location_uz, Borehole_uz)]
  nomtab <- unique(nomtab,
    by = c("Orig_FileName", "Location_uz", "Borehole_uz", "SN"))
  nomtab <- rbind(e_nomtab, nomtab)
  nomtab <- nomtab[order(Location_uz, Borehole_uz)]
  nomtab <- unique(nomtab, by = c("Location_uz", "Borehole_uz", "SN"))

  #If the generated/updated nomtab table has NA values the user will be notified
  cans <- sum(sapply(nomtab[, - c("b__special")], function(x) {
    sum(is.na(x))
    }))

  if (cans > 0) {
    out_csv <- TRUE
    message("There are unconfirmed identities in the nomenclature!")
    message("A csv file has been generated with updated nomenclature.")
    message("Please complete the csv file to remove NAs.")
  }
  if (cans == 0) {
    saveRDS(nomtab, file = file.path(wait_room, "/nomtab.rds"))
  }

  if (out_csv == FALSE) {
    out_csv <- FALSE
  } else if (out_csv == TRUE) {
    out_name <-
      paste0("nomtab_", format(as.POSIXlt(Sys.time()),
      format = "%Y%m%d_%Hh%M"), ".csv")
    write.csv(nomtab, file.path(wait_room, out_name))
  }else stop("out_csv must be defined as a True|False variable")
  return(nomtab)
}