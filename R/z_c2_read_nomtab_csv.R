#' @title Imbibe edited nomenclature csv
#' @description Reads a user edited nomenclature file and saves as a usable
#'  nomtab (nomenclature table) file in the 'waiting room' folder.
#' @param pipe_house List of pipeline directories. __See__
#'  `ipayipi::ipip_house()` __for details__.
#' @param file name of the csv file to be read as the nomenclature table.
#' @details
#'  - Searches for the most recently edited nomenclature table csv
#'   ('nomtab.csv') file in the `wait_room` directory.
#'  - Checks whether there are any `NA` values corresposing to unstandardised
#'   synonyms of the following fields: 'location', 'station', and a station's
#'   standard title, that is, 'stnd_title'.
#'  - The 'table_name' also requires standardisation. By default this field is
#'   given as concatenation of a preffix, that is, 'raw_', and the standardised
#'   record interval (standardised by `ipayipi::record_interval_eval()` during
#'   logger data 'imbibing' (`ipayipi::imbibe_raw_logger_dt()`)). This
#'   convention should be maintained for the pipelines structure. Note that for
#'   event based or discontinuous data the table name will default to
#'   'raw_discnt'.
#'
#'   __NB!__ Only edit these fields (above) of the nomenclature table during
#'    standardisation.
#'
#'  __NB!__ The 'ipayipi' data pipeline strongly suggests conforming with
#'  tidyverse data standards. Therefore, use lower case characters, no special
#'  characters, except for the underscore character which should be used for
#'  spacing.
#' @return Saves the read nomenclature table in 'rds' format. Prints the
#'  nomenclature table.
#' @md
#' @keywords Naming convention, nomenclature synonyms, pipeline standardisation.
#' @author Paul J. Gordijn
#' @export
read_nomtab_csv <- function(
  pipe_house = NULL,
  file = NULL,
  ...
) {
  if (is.null(file)) {
    nomlist <- ipayipi::dta_list(input_dir = pipe_house$wait_room, file_ext =
      ".csv", wanted = "nomtab"
    )
    if (length(nomlist) < 1) stop("There is no nomtab file in the wait_room!")
    nom_dts <- lapply(nomlist, function(x) {
      mtime <- file.info(file.path(pipe_house$wait_room, x))$mtime
      invisible(mtime)
    })
    names(nom_dts) <- nomlist
    nom_dts <- unlist(nom_dts)
    del_files <- names(nom_dts[-which(nom_dts == max(nom_dts))])
    lapply(del_files, function(x) {
      file.remove(file.path(pipe_house$wait_room, x))
      invisible(del_files)
    })
    file <- names(nom_dts[which(nom_dts == max(nom_dts))])
  }
  nomtab <- data.table::fread(file.path(pipe_house$wait_room, file),
    header = TRUE
  )
  ck_cols <- c("location", "station", "stnd_title")
  cans <- sum(
    sapply(nomtab[, ck_cols, with = FALSE], function(x) sum(is.na(x)))
  )
  if (cans > 0) {
    message("There are unconfirmed identities in the header nomenclature!")
    message("Please edit the csv file to remove NAs.")
    print(nomtab[rowSums(is.na(nomtab[, ck_cols, with = FALSE])) > 0])
  }

  nomtab <- transform(nomtab,
    uz_station = as.character(nomtab$uz_station),
    location = as.character(nomtab$location),
    station = as.character(nomtab$station),
    stnd_title = as.character(nomtab$stnd_title),
    logger_type = as.character(nomtab$logger_type),
    logger_title = as.character(nomtab$logger_title),
    uz_record_interval_type = as.character(nomtab$uz_record_interval_type),
    uz_record_interval = as.character(nomtab$uz_record_interval),
    record_interval_type = as.character(nomtab$record_interval_type),
    record_interval = as.character(nomtab$record_interval),
    uz_table_name = as.character(nomtab$uz_table_name),
    table_name = as.character(nomtab$table_name)
  )
  # standardise raw table name preffix
  nomtab$table_name <- data.table::fifelse(
    !grepl(pattern = "raw_", x = nomtab$table_name),
    paste0("raw_", nomtab$table_name), nomtab$table_name
  )
  saveRDS(nomtab, file.path(pipe_house$wait_room, "nomtab.rns"))
  invisible(nomtab)
}