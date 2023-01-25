#' @title Batch append metadata to a station
#' @description Reads standardised metadata and appends records the matching
#'  station.
#' @param input_dir Directory where the standardised ipayipi rainfalll
#'  data sets are located (i.e., the `rainr_room`).
#' @param meta_file_name The name of the standardised rainfall metadata
#'  database.
#' @param recurr Search recursively through the input directory for
#'  standardised rainfall files.
#' @param prompt Logical parameter. If `TRUE` then the function will
#'  promt the user to interactively select files for which to append metadata
#'  to.
#' @param unwanted The function uses this vector of character strings used
#'  exclude files being processed which contain one of the search strings. The
#'  strings are passed to `ipayipi::dta_list()`.
#' @param wanted Character string containing search strongs/words to
#'  use to filter the station data. Multiple options/strings should be
#'  seperated by the bar symbol ("|").
#' @param dt_format The date-time format passed to
#'  `ipayipi::rain_meta_read()` --- the date-time format of the event
#'  metadata database.
#' @param tz  The time zone passed to `ipayipi::rain_meta_read()` ---
#'  the time zone of the event metadata database must match the data in
#'  question.
#' @keywords rainfall data processing; metadata; data pipeline; supplementary
#'  data; field notes; batch processing;
#' @author Paul J. Gordijn
#' @details This function is designed for batch processing of rainfall data
#'  which has been standardised in the **ipayipi** data pipeline.
#' 1. Opens and checks the events metadata database using
#'  `ipayipi::rain_meta_read()`.
#' 1. Extracts standardised rainfall data files from 'rainr_room' using
#'  search strings and/or an interactive selection method involving a prompt.
#' 1. Appends respective event metadata to each station.
#' 1. Saves the stations with updated event metadata.
#' @return  Saved data files and a list of data sets which have their
#'  respective metadata appended to them.
#' @export
rain_meta_batch <- function(
  input_dir = NULL,
  meta_file_name = "event_db.rmds",
  recurr = FALSE,
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  dt_format = "%Y-%m-%d %H:%M:%S",
  tz = "Africa/Johannesburg",
  ...
) {
  edb <- ipayipi::rain_meta_read(
    input_dir = input_dir, meta_file_name = meta_file_name,
    dt_format = dt_format, tz = tz)
  slist <- ipayipi::dta_list(input_dir = input_dir, file_ext = ".rds",
    prompt = prompt, recurr = recurr, unwanted = unwanted, wanted = wanted)
  cr_msg <- ipayipi::padr(core_message =
    paste0(" Appending event metadata to ", length(slist),
    " files ... ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)

  dta_sets <- lapply(slist, function(x) {
    dta <- readRDS(file.path(input_dir, x))
    invisible(dta)
  })
  names(dta_sets) <- slist

  dta_sets <- lapply(dta_sets, function(x) {
    dta <- ipayipi::rain_meta(input_file = x, meta_file = edb)
    invisible(dta)
  })
  names(dta_sets) <- slist
  # save the data
  dta_sets_saved <- lapply(seq_along(dta_sets), function(x) {
    saveRDS(dta_sets[[x]], file.path(input_dir, slist[x]))
    cr_msg <- ipayipi::padr(core_message =
        paste0("Done ", slist[x], " ... ", collapes = ""),
          wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
          force_extras = FALSE, justf = c(-1, 1))
    message(cr_msg)
  })
  cr_msg <- ipayipi::padr(core_message = "",
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  invisible(dta_sets)
}