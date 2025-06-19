#' @title Check ipayipi logger data file/header nomenclature
#' @description Critical step in the data pipeline. A function to check standardisation of logger data header information. If an unrecognised
#'  synonym/data attribute appears the nomenclature database will have to be updated.
#' @inheritParams header_sts
#' @param out_csv Logical. If TRUE a csv file is made in the working directory if there are files with unrecognised nomenclature.
#' @param file_ext The extension of the file for which the nomenclature is being assessed.
#' @keywords nomenclature, file metadata, file-header information, station synonyms, record interval
#' @return Returns a csv nomenclature file when unrecognised synonyms/data attributes are detected. Screen output notes whether the nomenclature table is complete.
#' @author Paul J. Gordijn
#' @inherit header_sts details
#' @export
nomenclature_chk <- function(
  pipe_house = NA,
  csv_out = TRUE,
  file_ext = "ipr",
  verbose = TRUE,
  xtra_v = FALSE,
  ...
) {
  "%ilike%" <- NULL
  "uz_station" <- "stnd_title" <- "station" <- "logger_type" <-
    "logger_title" <- "uz_record_interval_type" <- "uz_record_interval" <-
    "record_interval_type" <- "record_interval" <- "uz_table_name" <-
    "table_name" <- "location" <- NULL
  # if there is a more recent csv nomtab update the nomtab.rns
  # update nomtab.rns if csv is more recently modified
  # generate nomtab.rns if there is sa csv
  nomlist <- dta_list(input_dir = pipe_house$d2_wait_room, file_ext =
      ".csv", wanted = "aa_nomtab"
  )
  nom_dts <- lapply(nomlist, function(x) {
    mtime <- file.info(file.path(pipe_house$d2_wait_room, x))$mtime
    invisible(mtime)
  })
  names(nom_dts) <- nomlist
  t1 <- attempt::attempt(max(unlist(nom_dts)), silent = TRUE)
  t2 <- as.numeric(attempt::attempt(silent = TRUE,
    max(file.info(file.path(pipe_house$d2_wait_room, "aa_nomtab.rns"))$mtime)
  ))
  c1 <- file.exists(file.path(pipe_house$d2_wait_room, "aa_nomtab.rns"))
  if (is.na(t2)) t2 <- attempt::attempt(0)
  if (all(!attempt::is_try_error(t1), !attempt::is_try_error(t2), t1 > t2)) {
    ipayipi::read_nomtab_csv(pipe_house = pipe_house)
  }
  if (all(!attempt::is_try_error(t1), attempt::is_try_error(t2), c1)) {
    ipayipi::read_nomtab_csv(pipe_house = pipe_house)
  }
  if (file.exists(file.path(pipe_house$d2_wait_room, "aa_nomtab.rns"))) {
    nomtab <- readRDS(file.path(pipe_house$d2_wait_room, "aa_nomtab.rns"))
    ns <- c("uz_station", "location", "station", "stnd_title", "logger_type",
      "logger_title", "uz_record_interval_type", "uz_record_interval",
      "record_interval_type", "record_interval", "uz_table_name", "table_name"
    )
    # clean up nomtab file
    nomtab <- nomtab[, ns, with = FALSE
    ][!(is.na(stnd_title) | is.na(station) | is.na(location) |
          is.na(record_interval) | is.na(record_interval_type) |
          is.na(table_name) | table_name %ilike% "_NA"
      )
    ]
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
  slist <- ipayipi::dta_list(input_dir = pipe_house$d2_wait_room,
    file_ext = file_ext, prompt = FALSE, recurr = TRUE, unwanted = NULL,
    wanted = NULL
  )
  nomtab_import <- future.apply::future_lapply(slist, function(x) {
    mfile <- readRDS(file.path(pipe_house$d2_wait_room, x))
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
  saveRDS(nomtab, file.path(pipe_house$d2_wait_room, "aa_nomtab.rns"))
  # check critical nomenclature
  nomchk <- nomtab[is.na(location) | is.na(station) | is.na(stnd_title) |
      is.na(record_interval_type) | is.na(record_interval) | is.na(table_name) |
      table_name %in% "raw_NA"
  ]
  na_rows <- which(is.na(nomtab$location) | is.na(nomtab$station) |
      is.na(nomtab$stnd_title) | is.na(nomtab$record_interval_type) |
      is.na(nomtab$record_interval) | is.na(nomtab$table_name) |
      nomtab$table_name %in% "raw_NA"
  )
  if (nrow(nomchk) > 0) {
    if (verbose) {
      cli::cli_inform(c(
        "i" = "There are unconfirmed identities in the nomenclature!",
        "i" = "Remember all lower case characters and no spaces, use '_'.",
        ">" = "Check the following mandatory nomenclature fields:",
        "*" = "'location': two to four letter code for project area; ",
        "*" = "'station': name of the station WITH station type suffix.",
        " " = " Station-type suffices: '_aws' = automatic weather station;",
        " " = "'_bt' = atmospheric pressure - barometer; ",
        " " = "'_ct' = conductivity and temperature",
        " " = "'_cdt' = conductivity, depth, and temperature; ",
        " " = "'_ec' = eddy-covariance);",
        " " = "'_rn' = rain gauge;",
        " " = "'_sr' = surface renewal;",
        " " = "'_dt' = depth, and temperature sensor.",
        "*" = paste0("'stnd_title' (A concatenation of the location and ",
          "station (with suffix))"
        ),
        "*" = paste0("'record_interval_type'; one of the following: ",
          "'event_based', 'mixed', or 'continuous',",
          " see {.var ?record_interval_eval} for more details."
        ),
        "*" = paste0("'record_interval': this will habe been guessed by ",
          "{.var ?record_interval_eval}). NB! For 'event_based' data",
          " 'record_interval' == 'discnt'."
        ),
        "*" = paste0("'table_name': standardised table name for raw data.",
          " For 'event_based' rainfall data the pipeline standard is ",
          "'raw_rain', and for continuous data a concatenation of 'raw_' ",
          "and the {.var record_interval} is standard."
        ),
        "i" = paste0("An 'uz_' preffix indicates the 'unstandardised' ",
          "mandatory fields/columns captured from data import/imbibe header ",
          "info and imbibe 'data_setup' options."
        ),
        "i" = paste0("The following rows of the 'nomtab' have columns with ",
          "'NA' value(s) that requires populating: \n {na_rows}"
        )
      ))
    }
    print(nomchk)
    if (csv_out) {
      out_name <-
        paste0("aa_nomtab_",
          format(as.POSIXlt(Sys.time()), format = "%Y%m%d_%Hh%M"), ".csv"
        )
      cli::cli_inform(c("\n",
        "i" = "A csv version of the 'nom_tab has been generated here:",
        " " = "{file.path(pipe_house$d2_wait_room, out_name)}",
        "v" = "Fill out the NA fields and re-run {.var header_sts()}."
      ))
      write.csv(nomtab, file.path(pipe_house$d2_wait_room, out_name))
    }
  } else {
    out_name <- NA
  }
  return(list(update_nomtab = nomtab, output_csv_name = out_name))
}