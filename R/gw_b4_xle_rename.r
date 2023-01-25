#' @title A function to standardise xle file nomenclature
#' @description Using the xle nomenclature standarising table generated
#'  from the xle_nomenclature function, this function renames xle files.
#' @details \enumerate{
#'  \item Call the main nomenclature function, 'xle_nomenclature'
#'  to make sure that nomenclature is ready.
#'  \item Check for NA values in the table.
#'  \item If there are NA values:
#'  - stop the function and
#'  - produce warning.
#'  \item If the nomenclature table has no NA's:
#'   - rename files according to the nomenclature table.
#' }
#' @param wait_room Directory where the xle files are located with the
#'   nomenclature table.
#' @param out_csv Logical. Defaults to TRUE which initiates a nomenclature csv
#'   file output in the 'wait_room'.
#' @author Paul J Gordijn
#' @keywords rename files, nomenclature
#' @return Screen printout of re-named files.
#' @export
#' @examples
#' ## rename and standardize xle file names in the waiting room directory
#' wait_room <- "./temp" # directory (temporary) with xle data
#'
#' gw_xle_rename(wait_room = wait_room, out_csv = TRUE)
#'
#' ## Note that with 'out_csv' set to TRUE a csv will be generated with the
#' # current date-time of the running of this function.
#' # Once the csv has been updated the 'read_nomtab()' function can be used to
#' # read/import the csv.
#'
#' gw_read_nomtab_csv(wait_room = wait_room, file = csv_file)
gw_xle_rename <- function(
  wait_room,
  out_csv = TRUE,
  ...) {

  # Get the nomenclature table
  tnomtab <- gw_xle_nomenclature(wait_room = wait_room, out_csv = FALSE)

  # Check if there are NA values in tnomtab
  cans <- sum(
    sapply(tnomtab[, - c("b__special")], function(x) sum(is.na(x)))
    )
  if (cans > 0) {
      stop(paste0("The nomenclature table is incomplete.",
      " Try the 'xle_nomenclature' function"))
  }

# Generate a list of the files to be copied.
xlefiles_path <- gw_xle_list(input_dir = wait_room, recurr = FALSE)
xlefiles <- basename(xlefiles_path)

  #This is the main loop of this script.
  #Each file is copied and correctly encoded -
  #units not compatible with utf8 encoding are changed.
  #.xle files are in .xml fornat and can be read in R.
  #metadata is extracted from the files and used to
  #rename the files with start and end dates.

  # Read in the Borehole nomenclature standards.
  nomtab <- readRDS(file.path(wait_room, "nomtab.rds"))
  cr_msg <- padr(core_message = paste0(" correcting nomenclature in ",
    length(xlefiles), " files... "), wdth = 80, pad_char = "=",
    pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  vv <- sapply(xlefiles, function(z) {
    x <- xml2::read_xml(file.path(wait_room, z), encoding = "UTF-8")
    node_loc <- xml2::xml_find_first(x,
      xpath = ".//Instrument_info_data_header/Location")
    node_bh <- xml2::xml_find_first(x,
      ".//Instrument_info_data_header/Project_ID")
    loc_name <- xml2::xml_text(node_loc)
    loc_name <- gsub(loc_name, pattern = "/", replacement = "")
    bh_name <- xml2::xml_text(node_bh)
    bh_name <- gsub(bh_name, pattern = "/", replacement = "")
    node_sn <- xml2::xml_find_first(x,
      xpath = ".//Instrument_info/Serial_number")
      sn <- as.integer(xml2::xml_text(node_sn))

      rw <- subset(nomtab, subset = Location_uz == loc_name &
        Borehole_uz == bh_name & SN == sn)
      if (loc_name == rw$Location_uz &
        bh_name == rw$Borehole_uz & rw$SN == sn) {
        xml2::xml_text(node_loc) <- as.character(rw$Location)
        node_bh <- xml2::xml_find_first(x,
          ".//Instrument_info_data_header/Project_ID")
        under_s <- if (rw$b__special == "") {
          under_s <- ""
        } else {
          paste0("__", rw$b__special)
        }
        xml2::xml_text(node_bh) <-
          as.character(paste0(rw$Borehole, under_s))
      }

      startdate <- substr(xml2::xml_text(xml2::xml_find_first(x,
        ".//Instrument_info_data_header/Start_time")), 1, 10)
      startdate <- gsub(startdate, pattern = "/", replacement = "")
      enddate <- xml2::xml_text(xml2::xml_find_first(x,
        xpath = ".//File_info/Date"))
      enddate <- gsub(enddate, pattern = "/", replacement = "")

      if (rw$b__special == "") {
        new_fn <- paste0(rw$Location, "_", rw$Borehole, "_",
          startdate, "-", enddate, ".xle")
      } else {
        new_fn <- paste0(rw$Location, "_", rw$Borehole, "__", rw$b__special,
          "_", startdate, "-", enddate, ".xle")
        }

      old_fn <- z
      file.remove(file.path(wait_room, z))
      xml2::write_xml(x, file.path(wait_room, new_fn))
      cr_msg <- padr(core_message = paste0(old_fn, "  --->>  ", new_fn,
        collapse = ""), pad_char = "-", pad_extras = c("|-", "", "", "-|"),
        force_extras = TRUE, justf = c(-1, 2), wdth = 80)
      return(message(cr_msg))
  }
  )
  cr_msg <- padr(core_message = " nomenclature corrected ",
    wdth = 80, pad_char = "=",  pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
}
