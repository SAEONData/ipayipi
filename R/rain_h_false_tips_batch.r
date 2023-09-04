#' @title Flag 'false tips' --- batch mode
#' @description Processes standardised hobo rainfall files in batch mode to
#'  create/update a rainfall data table, and a table of 'false tips'.
#' @param rainr_room Directory in which the standardised hobo rainfall files
#'  are located.
#' @param false_tip_thresh The time buffer in seconds around logger
#'  interferance events (e.g., logger connections/downloads) in which any
#'  tips (logs) should be considered false. The default used by SAEON is ten
#'  minutes, that is, 10x60=600 seconds.
#' @param prompt Logical. If `TRUE` the function will enable interactive
#'  selection of hobo rainfall files for processing in this function.
#' @param recurr Logical. Should the listing of standardised hobo rainfall
#'  files be done by searching recursively through the input directory, that
#'  is, the `rainr_room`.
#' @keywords hoboware, tipping bucket rain gauge, data cleaning, false tips
#' @author Paul J. Gordijn
#' @return List of class "SAEON_rainfall_data" with an updated 'fasle_tip'
#'  table. Updated files are saved (old files are overwritten) in the
#'  specificed input directory.
#' @details
#'  1. Generated list of available standardised rainfall stations in the
#'     `rainr_room` using `ipayipi::dta_list`.
#'  1. Uses `ipayipi::rain_false_tips` to generate rainfall data with
#'     false tips removed, and a 'false tip' table.
#'  1. Overwrites old files by saving over these.
#' @export
rain_false_tips_batch <- function(
  rainr_room = NULL,
  false_tip_thresh = 10 * 60,
  unwanted = NULL,
  wanted = NULL,
  prompt = TRUE,
  recurr = FALSE,
  ...
) {
  slist <- ipayipi::dta_list(input_dir = rainr_room, file_ext = ".rds",
    prompt = prompt, recurr = recurr, unwanted = unwanted, wanted)
  cr_msg <- ipayipi::padr(core_message =
    paste0(" Updating/or generating false tip tables for ", length(slist),
    " files ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)

  updated_files_all <- lapply(slist, function(x) {
    cr_msg <- ipayipi::padr(core_message =
      paste0("Working on ", x, collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(-1, 1))
    message(cr_msg)
    updated_file <- ipayipi::rain_false_tips(
      input_file = readRDS(file.path(rainr_room, x)),
      false_tip_thresh = false_tip_thresh
    )
    saveRDS(updated_file, file.path(rainr_room, x))
  })
  cr_msg <- ipayipi::padr(core_message =
    paste0(" Complete ", length(slist),
    " files.", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  invisible(updated_files_all)
}