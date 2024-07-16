#' @title Opens a connection to an ipayipi station file.
#' @description Package subroutine for opening a station file for work.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param station_file Path to 'ipip' station file.
#' @keywords write station; edit station; add to station;
#' @export
#' @author Paul J Gordijn
#' @return Station file connection file paths. To be used in the pipeline.
#' @details This function is an internal function called by others in the
#'  pipeline.
close_sf_con <- function(
  pipe_house = NULL,
  station_file = NULL,
  overwrite = TRUE,
  append = FALSE,
  ...) {

  station_file <- basename(station_file)
  tmp_sf <- paste0(".", basename(station_file))
  tmp_sf <- file.path(pipe_house$ipip_room, tmp_sf)
  station_file <- file.path(pipe_house$ipip_room, station_file)

  # write station file
  ipayipi::write_station(pipe_house = pipe_house, station_file = station_file,
    overwrite = overwrite, append = append)

  return(station_file)
}