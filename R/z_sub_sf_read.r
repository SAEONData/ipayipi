#' @title Opens a connection to an ipayipi station file.
#' @description Package subroutine for opening a station file for work.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_init()` __for details__.
#' @param sfc Station file connection gerenated using `ipayipi::open_sf_con()`. If `sfc` is provided both `pipe_house` and `station_file` arguments are optional.
#' @param station_file The file name of the station file (extension included).
#' @param tv String vector of the names of values or tables to read from the station file. The strings provided here are used to filter items in the `sfc` object. The filtered item is then read into memory.
#' @param tmp Logical. If TRUE then `sf_read()` reads from the sessions temporary file location for stations.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @keywords write station; edit station; add to station;
#' @export
#' @author Paul J Gordijn
#' @return Station file connection file paths. To be used in the pipeline.
#' @details This function is an internal function called by others in the
#'  pipeline.
sf_read <- function(
  sfc = NULL,
  tv = NULL,
  station_file = NULL,
  tmp = FALSE,
  pipe_house = NULL,
  verbose = TRUE,
  ...) {
  sf <- NULL
  if (is.null(station_file) && !is.null(sfc)) {
    ns <- basename(names(sfc)[1])
    n <- sub(paste0("\\/", ns), "", sfc[[1]])
    station_file <- gsub(pattern = paste0(dirname(n), "\\/"), replacement = "",
      x = n)
  }
  station_file <- basename(station_file)
  if (!tmp) {
    sfn <- file.path(pipe_house$ipip_room, station_file)
    if (file.exists(sfn)) {
      sf <- readRDS(file.path(pipe_house$ipip_room, station_file))
    } else {
      cr_msg <- "Station file not found!"
      msg(cr_msg, verbose)
      sf <- NULL
      return(sf)
    }
    sf_names <- names(sf)
    if (!is.null(tv)) {
      sf_names <- sf_names[sf_names %in% tv]
    }
    sf <- sf[sf_names]
  }
  sfn <- file.path(tempdir(), "sf", basename(station_file))
  if (tmp && dir.exists(sfn)) {
    sfns <- list.files(sfn, recursive = TRUE, full.names = TRUE)
    sfns <- sfns[basename(sfns) %in% tv]
    sfns <- sfns[file.exists(sfns)]
    sf <- lapply(sfns, function(x) {
      readRDS(x)
    })
    names(sf) <- basename(sfns)
  } else {
    cr_msg <- "Temporary station file not found!"
    msg(cr_msg, verbose)
    return(sf)
  }
  if (length(sf) == 0) {
    cr_msg <- "No matching table or value names in station connention ('sfc')"
    msg(cr_msg, verbose)
  }

  return(sf)
}