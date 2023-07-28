#' @title Update meteorological file phenomena
#' @description A function to check standardisation of meteorological data
#'  phenomena to be run before introducing data into the pipeline.
#' @details
#' @param wait_room The working directory where imported meteorological data
#'  files are stored.
#' @param nomtab_import Is there an update to the extant (or non-existant)
#'  nomenclature table? This parameter can be used to input these updates.
#' @param check_phenomena Should the file naming be check. This helps ensure
#'  that files are named consistently and that data from different stations are
#'  not appended erraneously.
#' @param out_csv Logical. If TRUE a csv file is made in the working
#'  directory if there are files with unrecognised phenomena.
#' @keywords nomenclature
#' @return returns a csv phenomena file. Screen output notes whether
#'  the nomenclature table is complete.
#' @author Paul J. Gordijn
#' @export
phenomena_chk <- function(
    wait_room = NA,
    check_phenomena = TRUE,
    file_ext_in = ".iph",
    csv_out = TRUE,
    ...
  ) {
  if (file.exists(file.path(wait_room, "phentab.rps"))) {
    phentab <- readRDS(file.path(wait_room, "phentab.rps"))
  } else {
    phentab <- data.table::data.table(
      phid = NA_integer_,
      phen_name_full = NA_character_,
      phen_type = NA_character_,
      phen_name = NA_character_,
      units = NA_character_,
      measure = NA_character_,
      offset = NA_real_,
      var_type = NA_character_,
      uz_phen_name = NA_character_,
      uz_units = NA_character_,
      uz_measure = NA_character_,
      f_convert = NA_real_,
      sensor_id = NA_character_,
      notes = NA_character_
    )
  }

  # extract nomenclature from files
  slist <- ipayipi::dta_list(input_dir = wait_room, file_ext = file_ext_in,
    prompt = FALSE, recurr = TRUE, unwanted = NULL)
  mfiles <- lapply(slist, function(x) {
    mfile <- readRDS(file.path(wait_room, x))
    invisible(mfile)
  })
  phen_import <- lapply(mfiles, function(x) {
    phentabo <- data.table::data.table(
      phid = as.integer(seq_along(x$phens$phen_name)),
      phen_name_full = NA_character_,
      phen_type = NA_character_,
      phen_name = NA_character_,
      units = NA_character_,
      measure = NA_character_,
      offset = as.character(x$phens$offset),
      var_type = NA_character_,
      uz_phen_name = as.character(x$phens$phen_name),
      uz_units = as.character(x$phens$units),
      uz_measure = as.character(x$phens$measure),
      f_convert = NA_real_,
      sensor_id = NA_character_,
      notes = NA_character_
    )
    invisible(phentabo)
  })
  phen_import <- data.table::rbindlist(phen_import)

  phentab <- rbind(phentab, phen_import)
  phentab <- phentab[!is.na(uz_phen_name), ]
  phentab <- phentab[order(phen_name, units, measure, uz_phen_name,
    uz_units, uz_measure, offset, sensor_id)]
  phentab <- unique(phentab,
    by = c("uz_phen_name", "uz_units", "uz_measure", "f_convert",
      "sensor_id", "notes"))
  saveRDS(phentab, file.path(wait_room, "phentab.rps"))

  if (check_phenomena &
    nrow(phentab[is.na(phen_name_full) | is.na(phen_name) | is.na(units) |
        is.na(measure)]) > 0) {
      message("There are unconfirmed phenomena details!")
      message("Check the phenomena table.")
    if (csv_out) {
      message("A csv file has been generated with updated phenomena info.")
      message("Please complete the csv file to remove NAs.")
      out_name <-
        paste0("phentab_", format(as.POSIXlt(Sys.time()),
          format = "%Y%m%d_%Hh%M"), ".csv")
      write.csv(phentab, file.path(wait_room, out_name))
    }
  } else {
    message("Phenomena have been checked.")
    out_name <- NA
  }
  invisible(list(update_phenomena = phentab, output_csv_name = out_name))
}
