#' @title Check ipayipi logger data file/header nomenclature
#' @description Critical step in the data pipeline. A function to check standardisation of logger data header information. If an unrecognised
#'  synonym/data attribute appears the nomenclature database will have to be updated.
#' @param pipe_house List of pipeline directories. __See__
#'  `ipayipi::ipip_house()` __for details__.
#' @param check_nomenclature Should the file naming be checked. This helps ensure that files are named consistently and that data from different stations are not appended erraneously.
#' @param out_csv Logical. If TRUE a csv file is made in the working directory if there are files with unrecognised nomenclature.
#' @param file_ext The extension of the file for which the nomenclature is being assessed.
#' @param verbose Print some details on the files being processed? Logical.
#' @param cores Number of CPU's to use for processing in parallel. Only applies when working on Linux.
#' @keywords nomenclature, file metadata, file-header information, station synonyms, record interval
#' @return returns a csv nomenclature file when unrecognised synonyms/data attributes are detected. Screen output notes whether the nomenclature table is complete.
#' @author Paul J. Gordijn
#' @details When `ipayipi` imbibes logger data the data setup (e.g.,
#'  `ipayipi::hobo_rain`) allows for harvesting of logger file header
#'  information. This function assists with standardising this header
#'  information, storing this with header synonyms and other metadata in a
#'  table in the data pipelinese 'wait_room' directory. The file name of the
#'  table is set by default to 'nomtab.rns' (an rds file). If this file is
#'  deleted then the synonym database will need to be rebuilt using this
#'  function.
#'
#'  __Editing the nomenclature table__:
#'  When unrecognised synonyms are introduced (e.g., after a change in logger
#'  program setup, or simply a change in file name spelling) this function
#'  creates a csv table in the `wait_room` that should be edited to maintain
#'  the synonym database. _Unstandardised header information is preffixed
#'  in the nomenclature table (csv) by 'uz_'._ These unstandardised columns
#'  must not be edited by the user, but the standardised header information
#'  can be added by replacing corresponding `NA` values with appropriate
#'  standards.
#'
#'  The following fields in the nomenclature table may require editing/
#'  standardisation:
#'   - '__location__': The region name used to subset a group of stations by
#'      area.
#'   - '__station__': The name of the station collecting logger data.
#'   - '__stnd_title__': The standard station title. It is recommended that this
#'      title is given as the concatenation of the location and station field
#'      above, seperated by an underscore.
#'   - '__record_interval_type__': The record interval type; one of the following values: 'continuous', 'event_based', or 'mixed'.
#'   - '__record_interval__': `ipayipi::imbibe_raw_logger_dt()` uses the `ipayipi::record_interval_eval()` function to evaulate a date-time series to determine the record interval. Most times the record is evaluated correctly, but if there is insufficient data errors may occur. Therefore the record interval parameters need to be verified whilst checking nomenclature.
#'   - '__table_name__': The name of the table. By default the preffix to logger
#'     data is 'raw_'. Within the pipeline structure the name appended to this
#'     preffix should be the standardised record interval, e.g., 'raw_5_mins',
#'     for continuous 5 minute data, or 'raw_discnt' for raw event based data.
#'
#'   __NB!__ Only edit these fields (above) of the nomenclature table during
#'    standardisation.
#'
#'  Once this csv file has been edited and saved it can be pulled into the
#'  pipeline structure by running `ipayipi::read_nomtab_csv()`.
#'
#'  __What header information is used/standardised?__:
#'  Note that this function will only assess files for which the station name
#'  is known. If the station name is known, the "uz_station", "logger_type",
#'  "record_interval_type", "record_interval", and "uz_table_name" are used to
#'  define unique station and station table entries, that require
#'  standardisation.
#'  Note that `record_interval_type` and `record_interval` are evaluated during
#'  the `ipayipi::imbibe_raw_logger_dt()` function.
#'
#'  _The 'ipayipi' data pipeline strongly suggests conforming with
#'  'tidyverse' data standards. Therefore, use lower case characters, no special
#'  characters, except for the underscore character which should be used for
#'  spacing._
#'
#'  __NB!__ The header information and associated synonym database is used to
#'  standardise station metadata and is key for identifying like stations
#'  during the appending of data records. Phenomena or variables also need
#'  to be standardised using `ipayipi::phenomena_sts()`, but only after
#'  running `ipayipi::nomenclature_sts()`.
#' @export
nomenclature_chk <- function(
  pipe_house = NA,
  check_nomenclature = TRUE,
  csv_out = TRUE,
  file_ext = "ipr",
  verbose = FALSE,
  ...
) {
  "uz_station" <- "stnd_title" <- "station" <- "logger_type" <-
    "logger_title" <- "uz_record_interval_type" <- "uz_record_interval" <-
    "record_interval_type" <- "record_interval" <- "uz_table_name" <-
    "table_name" <- "location" <- NULL
  # if there is a more recent csv nomtab update the nomtab.rns
  # update nomtab.rns if csv is more recently modified
  # generate nomtab.rns if there is sa csv
  nomlist <- ipayipi::dta_list(input_dir = pipe_house$wait_room, file_ext =
      ".csv", wanted = "nomtab"
  )
  nom_dts <- lapply(nomlist, function(x) {
    mtime <- file.info(file.path(pipe_house$wait_room, x))$mtime
    invisible(mtime)
  })
  names(nom_dts) <- nomlist
  t1 <- attempt::attempt(max(unlist(nom_dts)), silent = !verbose)
  t2 <- as.numeric(attempt::attempt(silent = !verbose,
    max(file.info(file.path(pipe_house$wait_room, "nomtab.rns"))$mtime)
  ))
  c1 <- file.exists(file.path(pipe_house$wait_room, "nomtab.rns"))
  if (is.na(t2)) t2 <- attempt::attempt(0)
  if (all(!attempt::is_try_error(t1), !attempt::is_try_error(t2), t1 > t2)) {
    ipayipi::read_nomtab_csv(pipe_house = pipe_house)
  }
  if (all(!attempt::is_try_error(t1), attempt::is_try_error(t2), c1)) {
    ipayipi::read_nomtab_csv(pipe_house = pipe_house)
  }
  if (file.exists(file.path(pipe_house$wait_room, "nomtab.rns"))) {
    nomtab <- readRDS(file.path(pipe_house$wait_room, "nomtab.rns"))
    ns <- c("uz_station", "location", "station", "stnd_title", "logger_type",
      "logger_title", "uz_record_interval_type", "uz_record_interval",
      "record_interval_type", "record_interval", "uz_table_name", "table_name"
    )
    nomtab <- nomtab[, ns, with = FALSE]
  } else {
    nomtab <- data.table::data.table(
      uz_station = NA_character_, # 1
      location = NA_character_,
      station = NA_character_,
      stnd_title = NA_character_,
      logger_type = NA_character_, # 5
      logger_title = NA_character_,
      uz_record_interval_type = NA_character_,
      uz_record_interval = NA_character_,
      record_interval_type = NA_character_,
      record_interval = NA_character_, # 10
      uz_table_name = NA_character_,
      table_name = NA_character_ # 12
    )
  }

  # extract nomenclature from files
  slist <- ipayipi::dta_list(input_dir = pipe_house$wait_room, file_ext =
      file_ext, prompt = FALSE, recurr = TRUE, unwanted = NULL, wanted = NULL
  )
  nomtab_import <- future.apply::future_lapply(slist, function(x) {
    mfile <- readRDS(file.path(pipe_house$wait_room, x))
    nomtabo <- data.table::data.table(
      uz_station = mfile$data_summary$uz_station, # 1
      location = NA_character_,
      station = NA_character_,
      stnd_title = NA_character_,
      logger_type = mfile$data_summary$logger_type, # 5
      logger_title = mfile$data_summary$logger_title,
      uz_record_interval_type = mfile$data_summary$uz_record_interval_type,
      uz_record_interval = mfile$data_summary$uz_record_interval,
      record_interval_type = NA_character_,
      record_interval = NA_character_, # 10
      uz_table_name = mfile$data_summary$uz_table_name,
      table_name = NA_character_
    )
    invisible(nomtabo)
  })
  nomtab_import <- data.table::rbindlist(nomtab_import)

  nomtab <- rbind(nomtab, nomtab_import)
  nomtab <- nomtab[!is.na(uz_station), ]
  # trim white spaace around uz_station
  nomtab$uz_station <- gsub(" ^*|* $", "", nomtab$uz_station)
  nomtab <- nomtab[order(stnd_title, uz_station, station, logger_type,
    logger_title, uz_record_interval_type, uz_record_interval,
    record_interval_type, record_interval, uz_table_name, table_name
  )]
  nomtab <- unique(nomtab, by = c("uz_station", "logger_type",
    "uz_record_interval_type", "uz_record_interval", "uz_table_name"
  ))
  # standardise raw table name preffix
  nomtab$table_name <- data.table::fifelse(
    !grepl(pattern = "raw_", x = nomtab$table_name),
    paste0("raw_", nomtab$table_name), nomtab$table_name
  )
  saveRDS(nomtab, file.path(pipe_house$wait_room, "nomtab.rns"))
  # check critical nomenclature
  nomchk <- nomtab[is.na(location) | is.na(station) | is.na(stnd_title) |
      is.na(record_interval_type) | is.na(record_interval) | is.na(table_name)
  ]
  if (check_nomenclature && nrow(nomchk) > 0) {
    message("There are unconfirmed identities in the nomenclature!")
    message("Check the nomenclature table.")
    if (csv_out) {
      message("A csv file has been generated with updated nomenclature.")
      message("Please complete the csv file to remove NAs.")
      out_name <-
        paste0("nomtab_",
          format(as.POSIXlt(Sys.time()), format = "%Y%m%d_%Hh%M"), ".csv"
        )
      write.csv(nomtab, file.path(pipe_house$wait_room, out_name))
    }
  } else {
    out_name <- NA
  }
  return(list(update_nomtab = nomtab, output_csv_name = out_name))
}