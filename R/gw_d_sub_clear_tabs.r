#' @title Clear R data solonist tables
#' @description A function to clear all data tables in the standardised
#'  R groundwater data files except the actual untransformed water level
#'  data. Running in the pipeline will enable interactive selection of files
#'  for which tables will be cleared.
#' @author Paul J. Gordijn
#' @keywords clear rows; delete data
#' @param input_dir Directory where the function will search for
#'  R data solonist files.
#' @param clear_baro TRUE or FALSE. Defaults to FALSE. TRUE will clear
#'  the barometric data in the "log_baro_log", "log_baro_data" and all
#'  data in the "log_t" file. FALSE will clear the "log_t" table of all data.
#' @param recurr TRUE or FALSE. Defaults to FALSE. TRUE will enable a
#'  recursive search for R data solonist files in the "input_dir".
#' @param baros TRUE or FALSE. Defaults to FALSE. If TRUE, barometric
#'  data will be cleared as well.
#' @return Screen note that the tables were cleared.
#' @export
#' @details Clears all logger data in the R data barologger table (and
#'  associated baro logger tables) and/or the transformed logger data
#'  table named 'log_t'. Enables a fresh start to data processing.
gw_clear_tabs <- function(
  input_dir = ".",
  clear_baro = FALSE,
  recurr = FALSE,
  baros = TRUE,
  ...) {
  # Get a list of the files for table clearing
  slist <- gw_rsol_list(
    input_dir = input_dir, recurr = recurr,
    baros = baros, prompt = TRUE
  )

  if (clear_baro == TRUE) {
    t_names <- c("log_baro_log", "log_baro_data", "log_t")
  } else {
    t_names <- c("log_t")
  }

  lapply(slist, function(x) {
    f <- readRDS(file.path(input_dir, x))
    for (i in seq_along(t_names)) {
      f[[t_names[i]]] <- f[[t_names[i]]][
        which(is.na(f[[t_names[i]]][, 1])), ]
    }
      saveRDS(f, file.path(input_dir, x))
      return(message("Tables emptied."))
  })
}
