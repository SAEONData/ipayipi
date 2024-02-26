#' @title Opens a connection to an ipayipi station file.
#' @description Package subroutine for opening a station file for work.
#' @param pipe_house Optional. Used to generate filepath to station file name. List of pipeline directories. __See__ `ipayipi::ipip_init()` __for details__.
#' @param station_file Path (with file name and extension) to 'ipip' station file.
#' @keywords write station; edit station; add to station;
#' @export
#' @author Paul J Gordijn
#' @return Station file connection file paths. To be used in the pipeline.
#' @details In order to speed processing of data 'ipayipi' decompresses a station file list of tables/values into a 'hidden' temporary folder in the same working directory as a particular station file. Running this function will extract station file tables/values to a 'hidden' folder in the working directory. This function searches for_or_ creates a 'hidden' directory with station data and lists the file paths for each item in the directory. This list of directories for station data is termed a 'station file connection'. Functions using this connection will only update the station file on disk ('non-hidden' file) when updated information is written using `ipayipi::write_station()`.
open_sf_con <- function(
  pipe_house = NULL,
  station_file = NULL,
  ...) {

  if (!is.null(pipe_house)) {
    station_file <- basename(station_file)
    station_file <- file.path(pipe_house$ipip_room, station_file)
  }

  if (!file.exists(station_file)) {
    message("Error: no station file detected!")
    return(NULL)
  }

  # check if the sf_tmp alread exists
  sf_tmp <- file.path(tempdir(), "sf", basename(station_file))

  if (!file.exists(sf_tmp)) {
    dir.create(path = sf_tmp, recursive = TRUE)
    sf <- readRDS(station_file)
    sft <- sapply(seq_along(sf), function(i) {
      saveRDS(sf[[i]], file.path(sf_tmp, names(sf)[i]))
      invisible(file.path(sf_tmp, names(sf)[i]))
    })
    sfc <- sft
  } else {
    sfc <- list.files(sf_tmp, recursive = TRUE, full.names = TRUE)
  }
  names(sfc) <- list.files(sf_tmp, recursive = TRUE, full.names = FALSE)

  return(sfc)
}