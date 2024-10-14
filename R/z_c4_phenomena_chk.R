#' @title Update logger data file phenomena/variable metadata
#' @description Critical standardisation for maintaining time-series data pipelines.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param nomtab_import Is there an update to the extant (or non-existant) phenomena table? This parameter can be used to input these updates.
#' @param check_phenomena Should the file naming be check. This helps ensure that files are named consistently and that data from different stations are not appended erraneously.
#' @param out_csv Logical. If `TRUE` a csv file is made in the working directory if there are files with unrecognised phenomena.
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on linux systems.
#' @param wanted A string of keywords to use to filter which stations are selected for processing. Multiple search kewords should be seperated with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param verbose Print some details on the files being processed? Logical.
#' @details Logger data phenomena must be standardised prior to being transferred to the nomenclature vetted room. This standardisation step must only be run once header information has been standardised using the `ipayipi::nomenclature_sts()` and `ipayipi::header_sts()` functionality.
#' Phenomena are standardised using the following unstandardised fields: "uz_phen_name", "uz_units", "uz_measure", "sensor_id".
#'
#'  A phenomena table or or database is kept in the pipelines 'wait_room' directory as an RDS file ('phentab.rps'). If this file is deleted the `ipayipi::phenomena_sts()` will initiate the recreation of this file. The csv file is only produced for user convienience for editing.
#' @keywords logger phenomena, logger variables, measures, units, synonyms, standardisation
#' @return returns a csv phenomena file. Screen output notes whether the phenomena table is complete.
#' @author Paul J. Gordijn
#' @export
phenomena_chk <- function(
  pipe_house = NULL,
  check_phenomena = TRUE,
  file_ext_in = ".iph",
  csv_out = TRUE,
  external_phentab = NULL,
  verbose = FALSE,
  wanted = NULL,
  unwanted = NULL,
  ...
) {
  "uz_phen_name" <- "phen_name" <- "measure" <- "uz_units" <-
    "uz_measure" <- "sensor_id" <- "phen_name_full" <- NULL
  # if there is a more recent csv phentab update the phentab.rps
  # update phentab.rps if csv is more recently modified
  # generate phentab.rps if there is a csv
  plist <- ipayipi::dta_list(input_dir = pipe_house$wait_room,
    file_ext = ".csv", unwanted = unwanted, wanted = "phentab"
  )
  p_dts <- lapply(plist, function(x) {
    mtime <- file.info(file.path(pipe_house$wait_room, x))$mtime
    invisible(mtime)
  })
  names(p_dts) <- plist

  t1 <- attempt::attempt(max(unlist(p_dts)), silent = !verbose)
  t2 <- attempt::attempt(silent = !verbose, as.numeric(max(
    file.info(file.path(pipe_house$wait_room, "phentab.rps"))$mtime
  )))
  c1 <- file.exists(file.path(pipe_house$wait_room, "phentab.rps"))
  if (is.na(t2)) t2 <- attempt::attempt(0)
  if (all(!attempt::is_try_error(t1), !attempt::is_try_error(t2), t1 > t2)) {
    ipayipi::read_phentab_csv(pipe_house = pipe_house)
  }
  if (all(!attempt::is_try_error(t1), attempt::is_try_error(t2), c1)) {
    ipayipi::read_phentab_csv(pipe_house = pipe_house)
  }
  if (file.exists(file.path(pipe_house$wait_room, "phentab.rps"))) {
    phentab <- readRDS(file.path(pipe_house$wait_room, "phentab.rps"))
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

  # extract phenomena from files
  slist <- ipayipi::dta_list(input_dir = pipe_house$wait_room, file_ext =
      file_ext_in, prompt = FALSE, recurr = TRUE, unwanted = NULL
  )
  phen_import <- future.apply::future_lapply(slist, function(x) {
    m <- readRDS(file.path(pipe_house$wait_room, x))
    phentabo <- data.table::data.table(
      phid = as.integer(seq_along(m$phens$phen_name)),
      phen_name_full = NA_character_,
      phen_type = NA_character_,
      phen_name = NA_character_,
      units = NA_character_,
      measure = NA_character_,
      offset = as.character(m$phens$offset),
      var_type = NA_character_,
      uz_phen_name = as.character(m$phens$phen_name),
      uz_units = as.character(m$phens$units),
      uz_measure = as.character(m$phens$measure),
      f_convert = NA_real_,
      sensor_id = NA_character_,
      notes = NA_character_
    )
    invisible(phentabo)
  })
  phen_import <- data.table::rbindlist(phen_import)
  if ("phens_sts" %in% class(external_phentab)) {
    phen_import <- external_phentab[phen_name %in% phen_import$uz_phen_name]
    phen_import <- phen_import[!phen_name %in% c("id", "date_time")]
    phentab <- external_phentab
  }
  phentab <- rbind(phentab, phen_import)

  # phens must have a column name
  phentab <- phentab[!is.na(uz_phen_name), ]
  phentab <- phentab[order(phen_name, units, measure, uz_phen_name,
    uz_units, uz_measure, offset, sensor_id
  )]

  phentab <- unique(phentab,
    by = c("uz_phen_name", "uz_units", "uz_measure", "sensor_id")
  )
  saveRDS(phentab, file.path(pipe_house$wait_room, "phentab.rps"))

  if (check_phenomena &&
    nrow(phentab[
      is.na(phen_name_full) | is.na(phen_name) |
        is.na(units) | is.na(measure)
    ]) > 0
  ) {
    message("There are unconfirmed phenomena details!")
    message("Check the phenomena table.")
    if (csv_out) {
      message("A csv file has been generated with updated phenomena info.")
      message("Please complete the csv file to remove NAs.")
      out_name <-
        paste0("phentab_", format(as.POSIXlt(Sys.time()),
            format = "%Y%m%d_%Hh%M"
          ), ".csv"
        )
      write.csv(phentab, file.path(pipe_house$wait_room, out_name))
    }
  } else {
    message("Phenomena have been checked.")
    out_name <- NA
  }
  invisible(list(update_phenomena = phentab, output_csv_name = out_name))
}