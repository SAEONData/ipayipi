#' @title Bulk checking and summarising of data gaps
#' @description Processes selected or all starndardised files within a
#'  directory, and identifies manually declared and automatically
#'  detected data 'gaps'.
#' @param input_dir The path to the directory in which standardised 'ipayipi'
#'  rainfall data files are located.
#' @param recurr Search recursively through the input directory for
#'  standardised hobo rainfall station files.
#' @param prompt Logical parameter. If `TRUE` then the function will
#'  promt the user to interactively select files for which to append metadata
#'  to.
#' @param wanted Vector of strings listing files that should not be
#'  included in the import.
#' @param unwanted The function uses this vector of character strings used
#'  exclude files being processed which contain one of the search strings. The
#'  strings are passed to `ipayipi::dta_list()`.
#' @param search_patterns Character string containing search strongs/words to
#'  use to filter the stations bing processed. Multiple options/strings should
#'  be seperated by the bar symbol ("|").
#' @param gap_problem_thresh_s  The threshold duration of a gap beyond which
#'  the gap is considered problematic, and therefore, the data requires
#'  infilling or 'patching'. The default is six hours given in seconds, i.e.,
#'  6 * 60 * 60.
#' @param event_thresh_s A gap can also be specified in the event metadata by
#'  only providing a single date-time stamp and an 'event_threshold_s'. By
#'  taking the date-time stamp the function approximates the start and end-time
#'  of the data gap by adding and subtracting the given `event_thresh_s` to the
#'  date-time stamp, respectively. If the `event_thresh_s` is supplied in the
#'  event data then the value supplied therein is used in preference to the
#'  value provided in this function. The default event threshold given here
#'  in seconds is ten minutes, i.e., 10 * 60 seconds.
#' @keywords rainfall data processing; metadata; data pipeline; supplementary
#'  data; field notes; batch processing; missing data; data gaps;
#' @author Paul J. Gordijn
#' @details This function is designed for batch processing of rainfall data
#'  which has been standardised in the **ipayipi** data pipeline.
#' 1. Extracts standardised rainfall data files from 'rainr_room' using
#'  search strings and/or an interactive selection method involving a prompt.
#' 1. Checks and classifies data gaps using
#'  `ipayipi::rain_data_gaps()`.
#' 1. Appends table describing data gaps to each station.
#' 1. Saves the station data with updated gap table.
#' @return  Saved data files and a list of data sets which have their
#'  respective metadata appended to them.
#' @export
rain_gaps_batch <- function(
  input_dir = NULL,
  recurr = FALSE,
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  gap_problem_thresh_s = 6 * 60 * 60,
  event_thresh_s = 10 * 60,
  ...
) {
  slist <- ipayipi::dta_list(input_dir = input_dir, file_ext = ".rds",
    prompt = prompt, recurr = recurr, unwanted = unwanted, wanted = wanted)
  cr_msg <- ipayipi::padr(core_message =
    paste0(" Classifying missing data in ", length(slist),
    " files ... ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)

  dta_sets <- lapply(slist, function(x) {
    dta <- readRDS(file.path(input_dir, x))
    invisible(dta)
  })
  names(dta_sets) <- slist

  dta_sets <- lapply(seq_along(dta_sets), function(x) {
    message(slist[[x]])
    dta <- ipayipi::rain_gaps(input_file = dta_sets[[x]], gap_problem_thresh_s =
      gap_problem_thresh_s, event_thresh_s = event_thresh_s)
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