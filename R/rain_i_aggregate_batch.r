#' @title Summarize rainfall data by multiple time periods
#' @description Reads rainfall data from the `clean_rain_hobo()` function
#'  then aggregates the data by specified time periods.
#' @param input_file Output from `clean_rain_hobo`
#' @param output_pref A string for the file name prefix. The time periods given
#'  in the `aggs` argument will form the suffex. The default preffix used in
#'  the data pipeline is 'agg_'. The 'agg_' preffix must be used for further
#'  processing.
#' @param aggs vector of strings giving the time periods to use for aggregating
#'  the data.
#' @param csv_out If `TRUE` the function will write a csv to the working
#'  for each set of aggregated data.
#' @param save_data Choose whether to append the new data aggregation to the
#'  standardised data saved in the pipeline or the updated objects.
#' @param ignore_nas Choose whether to ignore gaps and other `NA` values when
#'  aggregating the data by sum.
#' @param agg_offset A vector of two strings that can be coerced to a
#'  `lubridate` period. These can be used to offset the date-time from
#'  which aggregations are totalled. For example, for rainfall totals estimated
#'  from 8 am to 8pm the `agg_offset` should be set to c(8 hours, 8 hours).
#' @keywords hoboware, tipping bucket rain gauge, data aggregation
#' @author Paul J. Gordijn
#' @return See the 'save_data' parameter.
#' @details The function uses data.table to summarize the data by the time
#'  periods of interest.
#' @export
rain_agg_batch <- function(
  input_dir = NULL,
  aggs = c("5 mins", "day", "month"),
  output_pref = "agg_",
  recurr = FALSE,
  wanted = NULL,
  unwanted = NULL,
  prompt = FALSE,
  save_data = FALSE,
  ignore_nas = FALSE,
  agg_offset = c("0 secs", "0 secs"),
  ...) {

  slist <- ipayipi::dta_list(input_dir = input_dir, file_ext = ".rds",
    prompt = prompt, recurr = recurr, unwanted = unwanted, wanted = wanted)
  cr_msg <- ipayipi::padr(core_message =
    paste0(" Generating aggregated rainfall data for ", length(slist),
    " files ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)

  # read in the data
  dta <- lapply(slist, function(x) {
    dta <- readRDS(file.path(input_dir, x))
    invisible(dta)
  })
  dta <- lapply(dta, function(x) {
    ipayipi::rain_agg(
      input_file = x, output_pref = output_pref, aggs = aggs, csv_out = FALSE,
      ignore_nas = ignore_nas, agg_offset = agg_offset
    )
  })
  if (save_data) {
    saved_files <- lapply(seq_along(slist), function(x) {
      saveRDS(dta[[x]], file.path(input_dir, slist[x]))
      cr_msg <- ipayipi::padr(core_message =
        paste0(" Saving ", slist[x], collapes = ""),
        wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
        force_extras = FALSE, justf = c(-1, 1))
      return(message(cr_msg))
    })
  } else {
    saved_files <- dta
    names(dta) <- slist
    cr_msg <- ipayipi::padr(core_message =
      paste0(" Files processed ... ", collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(-1, 1))
    return(message(cr_msg))
  }
  cr_msg <- ipayipi::padr(core_message = "=",
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  invisible(saved_files)
}