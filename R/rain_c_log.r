#' @title Generate log of standardised hobo rain files
#' @description Creates an inventory of standardised hobo rain files using
#'  a list of files in the R environment, or reading archived RDS files
#'  in the 'nomvet_room'.
#' @details Extracts the data summary from each standardised hobo rainfall
#'  file and combines these into a table. A list of 'failed' hobo files (
#'  **see** rain_hobo_clean()) is also provided.
#' @param hobo_in Can be one of the following: a list of files in the R
#'  environment, or NULL for reading files in the 'nomvet_room'.
#' @param log_dir The directory for which to generate a log of all hobo
#'  raninfall file exports. If NULL then a list of standardised hobo rainfall
#' files must be supplied to the 'hobo_in' argument. Character string.
#' @keywords hobo rainfall data pipeline; data log; data inventory
#' @return List containing the inventory of standardised hobo rainfall files
#'  and a list of hobo rainfall file imports which failed to be standardised
#'  due to incorrect formatting of the file exported from Hoboware.
#' @author Paul J. Gordijn
#' @export
rain_log <- function(
  hobo_in = NULL,
  log_dir = NULL,
  ...) {
    # make inventory of files
    if (is.null(hobo_in)) {
      slist <- dta_list(input_dir = log_dir, file_ext = ".rds")
      hobo_in <- lapply(slist, function(x) {
        z <- readRDS(file.path(nomvet_room, basename(x)))
        return(z)
      })
    }
    if (class(hobo_in) == "converted_rain_hobo") {
      hobo_in <- hobo_in[["hobo_converts"]]
    }
    hblg_empty <- data.table::data.table(
      ptitle_standard = character(),
      location = character(),
      station = character(),
      start_dt = as.POSIXct(rep(NA, 0)),
      end_dt = as.POSIXct(rep(NA, 0)),
      instrument_type = character(),
      logger_sn = character(),
      sensor_sn = character(),
      tip_value_mm = numeric(),
      import_file_name = character(),
      ptitle_original = character(),
      nomvet_name = character()
    )

    # generate logs for standard and failed hobo rain files
    hobo_lg <- lapply(hobo_in, function(x) {
      if (class(x) == "SAEON_rainfall_data") {
        hblg <- x[["data_summary"]]
      } else hblg <- hblg_empty
      return(hblg)
    })
    hobo_lg <- data.table::rbindlist(hobo_lg)
    if (nrow(hobo_lg) < 1) hobo_lg <- hblg_empty
    hobo_fl <- lapply(hobo_in, function(x) {
      if (class(x) == "SAEON_rainfall_data_fail") {
        hbfl <- x[["f_parameters"]]["input_file"]
      } else hbfl <- hblg_empty
      return(hbfl)
    })
    hobo_fl <- data.table::rbindlist(hobo_fl)
    if (nrow(hobo_fl) < 1) hobo_fl <- hblg_empty
    return(list(hobo_wins = hobo_lg, hobo_fails = hobo_fl))
  }