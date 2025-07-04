#' @title Update meteorological file phenomena
#' @description A function to check standardisation of meteorological data phenomena to be run before introducing data into the pipeline.
#' @inheritParams logger_data_import_batch
#' @param file The (base) file name of the csv import. If NULL then the function searches for the most recently modified csv file to read (and deletes older phenomena csv files).
#' @keywords phenomena; measurement standardisation;
#' @return returns a copy of the finalized phenomena table
#' @author Paul J. Gordijn
#' @export
phenomena_read_csv <- function(
  pipe_house = NULL,
  file = NULL,
  ...
) {
  if (is.null(file)) {
    phenlist <- ipayipi::dta_list(input_dir = pipe_house$d2_wait_room, file_ext =
        ".csv", wanted = "aa_phentab"
    )
    if (length(phenlist) < 1) {
      cli::cli_abort(c(
        "!" =
          "No phentab file ('aa_phentab_YYYYMMDD_HHMM.csv') in the d2_wait_room!",
        "i" = "Searching for 'aa_phentab_YYYYMMDD_HHMM.csv' here:",
        " " = "{pipe_house$d2_wait_room}"
      ))
    }
    phen_dts <- lapply(phenlist, function(x) {
      mtime <- file.info(file.path(pipe_house$d2_wait_room, x))$mtime
      invisible(mtime)
    })
    names(phen_dts) <- phenlist
    phen_dts <- unlist(phen_dts)
    del_files <- names(phen_dts[-which(phen_dts == max(phen_dts))])
    lapply(del_files, function(x) {
      file.remove(file.path(pipe_house$d2_wait_room, x))
      invisible(del_files)
    })
    file <- names(phen_dts[which(phen_dts == max(phen_dts))])
  }
  if (!file.exists(file.path(pipe_house$d2_wait_room, file))) {
    cli::cli_abort(c("File not found!"))
  }
  phentab <- read.csv(file.path(pipe_house$d2_wait_room, file))
  cans <- sum(
    sapply(phentab[, c("phen_name_full", "phen_name", "units", "measure")
      ], function(x) sum(is.na(x))
    )
  )
  if (cans == 0) {
    suppressMessages(phentab <- data.table::data.table(
      phid = as.integer(phentab$phid),
      phen_name_full = as.character(phentab$phen_name_full),
      phen_type = as.character(phentab$phen_type),
      phen_name = as.character(phentab$phen_name),
      units = as.character(phentab$units),
      measure = as.character(phentab$measure),
      offset = as.numeric(phentab$offset),
      var_type = as.character(phentab$var_type),
      uz_phen_name = as.character(phentab$uz_phen_name),
      uz_units = as.character(phentab$uz_units),
      uz_measure = as.character(phentab$uz_measure),
      f_convert = as.numeric(phentab$f_convert),
      sensor_id = as.character(phentab$sensor_id),
      notes = as.character(phentab$notes)
    ))
    saveRDS(phentab, file.path(pipe_house$d2_wait_room, "aa_phentab.rps"))
  }
  invisible(phentab)
}
