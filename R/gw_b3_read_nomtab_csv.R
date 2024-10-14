#' @title Read xle nomenclature csv
#' @description Read in a user edited nomenclature file and save as a usable
#'  nomtab file in the 'waiting room' folder. Part of the xle to R solonist
#'  data pipeline.
#' @details
#'  \enumerate{
#'    \item Read in csv file from specified folder.
#'    \item Check for NA values in the table.
#'    \item If the nomenclature table has no NA's:
#'       - convert to tibble.
#'       - save as rds in the 'waiting room' folder.
#'    \item If there are NA values:
#'       - stop the function and.
#'       - produce warning, i.e. the user needs to fill in the NA values.
#' }
#' @return Saves the read nomenclature table in 'rds' format. Prints the
#'  nomenclature table.
#' @keywords Naming convention, nomenclature
#' @author Paul J Gordijn
#' @param wait_room Directory into which new data is copied into -- the start
#'  of the data processing pipeline.
#' @param file name of the csv file to be read as the nomenclature table.
#' @export
#' @examples
#' ## read in or import an updated 'nomtab' csv. 'nom' being nomenclature and
#' # 'tab' being table.
#' ## set variables
#' wait_room <- "./temp" # directory (temporary) with xle data
#' csv_file <- "./nomtab_20210415_09h42.csv"
#'
#' ## read in the nomenclature table
#' gw_read_nomtab_csv(wait_room = wait_room, file = csv_file)
gw_read_nomtab_csv <- function(
  wait_room = NULL,
  file = NULL,
  ...) {
  if (is.null(file)) {
    nomlist <- ipayipi::dta_list(input_dir = wait_room, file_ext = ".csv",
      wanted = "nomtab")
    if (length(nomlist) < 1) stop("There is no nomtab file in the wait_room!")
    nom_dts <- lapply(nomlist, function(x) {
      mtime <- file.info(file.path(wait_room, x))$mtime
      invisible(mtime)
    })
    names(nom_dts) <- nomlist
    nom_dts <- unlist(nom_dts)
    del_files <- names(nom_dts[-which(nom_dts == max(nom_dts))])
    lapply(del_files, function(x) {
      file.remove(file.path(wait_room, x))
      invisible(del_files)
    })
    file <- names(nom_dts[which(nom_dts == max(nom_dts))])
  }
  nomtab <- data.table::setDT(read.csv(
    file.path(wait_room, file), row.names = 1, header = TRUE))
  nomtab[is.na(nomtab)] <- ""
  cans <- sum(sapply(nomtab[, - c("b__special")],
      function(x) {
      sum(is.na(x))
      }))

  if (cans > 0) {
    message("There are unconfirmed identities in the nomenclature!")
    message("Please edit the csv file to remove NAs.")
  }
  if (cans == 0) {
      saveRDS(nomtab, file.path(wait_room, "nomtab.rds"))
    }
  return(nomtab)
}