#' @title Check ipayipi data file nomenclature
#' @description A function to check standardisation of meteorological data
#'  objects to be run before introducing data into the pipeline.
#' @details
#' @param wait_room The working directory where imported meteorological data
#'  files are stored.
#' @param check_nomenclature Should the file naming be check. This helps ensure
#'  that files are named consistently and that data from different stations are
#'  not appended erraneously.
#' @param out_csv Logical. If TRUE a csv file is made in the working
#'  directory if there are files with unrecognised nomenclature.
#' @param file_ext The extension of the file for which the nomenclature is being
#'  assessed.
#' @keywords nomenclature
#' @return returns a csv nomenclature file. Screen output notes whether
#'  the nomenclature table is complete.
#' @author Paul J. Gordijn
#' @export
nomenclature_sts <- function(
    wait_room = NA,
    check_nomenclature = TRUE,
    csv_out = TRUE,
    file_ext = "ipr",
    ...
  ) {
  "uz_station" <- "stnd_title" <- "station" <- "logger_type" <-
    "logger_title" <- "uz_table_name" <- "table_name" <- "location" <- NULL
  if (file.exists(file.path(wait_room, "nomtab.rns"))) {
    nomtab <- readRDS(file.path(wait_room, "nomtab.rns"))
  } else {
    nomtab <- data.table::data.table(
      uz_station = NA_character_, # 1
      location = NA_character_,
      station = NA_character_,
      stnd_title = NA_character_,
      logger_type = NA_character_, # 5
      logger_title = NA_character_,
      record_interval_type = NA_character_,
      record_interval = NA_character_,
      uz_table_name = NA_character_,
      table_name = NA_character_ # 10
    )
  }

  # extract nomenclature from files
  slist <- ipayipi::dta_list(input_dir = wait_room, file_ext = file_ext,
    prompt = FALSE, recurr = TRUE, unwanted = NULL, wanted = NULL)
  mfiles <- lapply(slist, function(x) {
    mfile <- readRDS(file.path(wait_room, x))
    invisible(mfile)
  })
  nomtab_import <- lapply(mfiles, function(x) {
    nomtabo <- data.table::data.table(
      uz_station = x$data_summary$uz_station, # 1
      location = NA_character_,
      station = NA_character_,
      stnd_title = NA_character_,
      logger_type = x$data_summary$logger_type, # 5
      logger_title = x$data_summary$logger_title,
      record_interval_type = x$data_summary$record_interval_type,
      record_interval = x$data_summary$record_interval,
      uz_table_name = x$data_summary$uz_table_name,
      table_name = NA_character_ # 10
    )
    invisible(nomtabo)
  })
  nomtab_import <- data.table::rbindlist(nomtab_import)

  nomtab <- rbind(nomtab, nomtab_import)
  nomtab <- nomtab[!is.na(uz_station), ]
  nomtab <- nomtab[order(stnd_title, uz_station, station, logger_type,
    logger_title, uz_table_name, table_name)]
  nomtab <- unique(nomtab,
    by = c("uz_station", "logger_type", "record_interval_type",
      "record_interval", "uz_table_name"))
  # standardise raw table name preffix
  nomtab$table_name <- data.table::fifelse(
    !grepl(pattern = "raw_", x = nomtab$table_name),
    paste0("raw_", nomtab$table_name), nomtab$table_name
  )
  saveRDS(nomtab, file.path(wait_room, "nomtab.rns"))

  if (check_nomenclature &&
    nrow(nomtab[is.na(location) | is.na(station) | is.na(stnd_title) |
        is.na(table_name)]) > 0) {
      message("There are unconfirmed identities in the nomenclature!")
      message("Check the nomenclature table.")
    if (csv_out) {
      message("A csv file has been generated with updated nomenclature.")
      message("Please complete the csv file to remove NAs.")
      out_name <-
        paste0("nomtab_", format(as.POSIXlt(Sys.time()),
          format = "%Y%m%d_%Hh%M"), ".csv")
      write.csv(nomtab, file.path(wait_room, out_name))
    }
  } else {
    message("Nomenclature has been standardised.")
    out_name <- NA
  }
  return(list(update_nomtab = nomtab, output_csv_name = out_name))
}