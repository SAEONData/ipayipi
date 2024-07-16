#' @title Simple data table reading function.
#' @description Package subroutine for opening a station file for work. NB! Will not work for chunked data tables.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param sfc Station file connection gerenated using `ipayipi::open_sf_con()`. If `sfc` is provided both `pipe_house` and `station_file` arguments are optional.
#' @param station_file The file name of the station file (extension included).
#' @param tv String vector of the names of values or tables to read from the station file. The strings provided here are used to filter items in the `sfc` object. The filtered item is then read into memory.
#' @param tmp Logical. If TRUE then `sf_read()` reads from the sessions temporary file location for stations.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @keywords read data; open data tables; open lists
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
    start_dttm = NULL,
    end_dttm = NULL,
    ...) {
  "sf" <- "%ilike%" <- NULL
  if (is.null(station_file) && !is.null(sfc)) {
    ns <- basename(names(sfc)[1])
    n <- sub(paste0("\\/", ns), "", sfc[[1]])
    station_file <- gsub(pattern = paste0(dirname(n), "\\/"), replacement = "",
      x = n
    )
  }
  station_file <- basename(station_file)
  if (!tmp) {
    sfn <- file.path(pipe_house$ipip_room, station_file)
    if (file.exists(sfn)) {
      sf <- readRDS(file.path(pipe_house$ipip_room, station_file))
    } else {
      cr_msg <- "Station file not found!"
      ipayipi::msg(cr_msg, verbose)
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
    sfns <- sfns[!basename(sfns) %ilike% "^i_*"]
    sfns <- sfns[!basename(sfns) %ilike% "aindxr"]
    sfns <- sfns[basename(sfns) %in% tv]
    sfns <- sfns[file.exists(sfns)]
    sf <- lapply(sfns, readRDS)
    names(sf) <- basename(sfns)
  } else {
    cr_msg <- "Temporary station file not found!"
    ipayipi::msg(cr_msg, verbose)
    return(sf)
  }
  return(sf)
}