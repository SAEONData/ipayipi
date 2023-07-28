#' @title Append respective event metadata to a station
#' @description Reads standardised metadata and appends records the matching
#'  station.
#' @param input_file Standardised ipayipi rainfalll data set (stored in
#'  the `rainr_room`).
#' @param meta_file Standardised rainfall metadata database.
#' @keywords rainfall data processing; metadata; data pipeline; supplementary
#'  data; field notes
#' @author Paul J. Gordijn
#' @return Standardised ipayipi rainfall station object with updated event
#'  metadata.
#' @details The function takes event metadata which matches the standardised
#' @export
rain_meta <- function(
  input_file = NULL,
  meta_file = NULL,
  ...
) {
  if (class(input_file) != "SAEON_rainfall_data") {
    stop("Standardised ipayipi rainfall data ojbect required!")
  }
  input_file_name <- input_file$data_summary$ptitle_standard[1]
  edb <- meta_file[location_station == input_file_name]
  input_file$event_data <- edb

  invisible(input_file)
}