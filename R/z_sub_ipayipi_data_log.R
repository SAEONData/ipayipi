#' @title Generate a log of data files within a directory.
#' @description Once the hobo rainfall file exports have been standardised,
#'  and have their nomenclature checked, this function saves the data from
#'  each import into the 'nomvet_room'.
#' @details  Aim: Archive each checked hobo rainfall file in a secure
#'  directory, that is, the '__nomvet_room__', which is part of the 'ipayipi'
#'  data pipeline. Once archived the data will be prepared for further
#'  processing. Here are some of the specifics:
#'  1. Generate inventory of standardised files that have been processed by
#'     `rain_hobo_conversion()` --- this object is 'fed' to the function via
#'     the `hobo_in` argument.
#'  1. Create (if none) or produce an inventory of files in the 'nomvet_room'.
#'  1. Compare the inventories to identify which new files should be
#'    transferred into the 'nomvet_room'.
#'  1. Transfer files---saving these in RDS format in the 'nomvet_room'.
#' @param hobo_in Converted hobo rainfall files as produced by
#'  `rain_hobo_conversion()`.
#' @param nomvet_room The directory where the standardised hobo rainfall files
#'  are archived.
#' @keywords data pipeline; archive data; save data
#' @return A table describing station file data within a directory, and the file extension of the station files.
#' @author Paul J. Gordijn
#' @export
ipayipi_data_log <- function(
  log_dir = NULL,
  file_ext = ".ipi",
  ...
) {
  # merge data sets into a station for given time periods
  slist <- ipayipi::dta_list(input_dir = log_dir, file_ext = file_ext,
    prompt = FALSE, recurr = FALSE, unwanted = NULL
  )
  # make table to guide merging by record interval, date_time, and station
  methdr <- future.apply::future_lapply(slist, function(x) {
    m <- readRDS(file.path(log_dir, x))
    methdr <- m$data_summary
    methdr <- update_ds(methdr)
    m$data_summary <- methdr
    saveRDS(m, file.path(log_dir, x))
    invisible(methdr)
  })
  methdr <- data.table::rbindlist(methdr)
  methdr$input_file <- slist
  if (nrow(methdr) == 0) {
    methdr <- data.table::data.table(
      dsid = NA_integer_,
      file_format = NA_character_,
      uz_station = NA_character_,
      location = NA_character_,
      station = NA_character_,
      stnd_title = NA_character_,
      start_dttm = as.POSIXct(NA_character_),
      end_dttm = as.POSIXct(NA_character_),
      logger_type = NA_character_,
      logger_title = NA_character_,
      logger_sn = NA_character_,
      logger_os = NA_character_,
      logger_program_name = NA_character_,
      logger_program_sig = NA_character_,
      uz_record_interval_type = NA_character_,
      uz_record_interval = NA_character_,
      record_interval_type = NA_character_,
      record_interval = NA_character_,
      dttm_inc_exc = NA,
      dttm_ie_chng = NA,
      uz_table_name = NA_character_,
      table_name = NA_character_,
      nomvet_name = NA_character_,
      file_origin = NA_character_,
      input_file = NA_character_
    )
    methdr <- methdr[0, ]
  }
  return(list(log = methdr, file_type = file_ext))
}
