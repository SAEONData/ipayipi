#' @title Writes a station file to disk
#' @description Takes input station values and tables and saves them to disk.
#' @param pipe_house Required. List of pipeline directories. __See__ `ipayipi::ipip_init()` __for details__.
#' @param sf Station file object.
#' @param station_file Path to station file. Required.
#' @param overwrite If `TRUE` then old data is overwritten. _Vice versa_ if `FALSE`.
#' @param append If `TRUE` then new values/tables will be written to an existing station file. If `FALSE` a new station will be created. When `append` and overwrite are `TRUE` then station tables/values will be overwritten by tables/values with similar names.
#' @param keep_open Maintains the station connection, i.e., the hidden station file is not removed (deleted).
#' @keywords write station; edit station; add to station;
#' @export
#' @author Paul J Gordijn
#' @return File path of station file.
#' @details In order to speed processing of data 'ipayipi' decompresses a station file list of tables/values into a 'hidden' temporary folder in the same working directory as a particular station file. This function assumes that if a temporary folder or station file is open, that is the most up-to-date station file information. This has implications for when saving whole station files or appending data to station files. A number of scenarios may arise when writing a station file when different items are available or privided.
#' 
#' #' __Scenario 1__: sf R object
#' 'sf R object' written as a station file on disk ('sf on disk').
#' 
#' __Scenario 2__: 'hidden' sf on disk + sf on disk + sf R object
#' __NB!__ In this scenario the station file on disk is _always_ assumed to be outdated/older than the 'hidden' station file on disk. Therefore, only the station file object, and 'hidden' station file are *merged under this scenario. The merged file is written as a station file on disk ('sf on disk').
#' 
#' __Scenario 3__: 'hidden' sf on disk + sf R object
#' Merges the 'sf on disk' with the provided 'sf R object'. The merged file is written as a station file on disk ('sf on disk').
#' 
#'  __Scenario 4__: 'hidden' sf on disk
#' Writes the 'hidden' station file to disk.
#'
#' Arguments `append` and `overwrite` control how different station files (i.e., hidden, R environment, or on disk) are merged.
#' 
write_station <- function(
  pipe_house = NULL,
  sf = NULL,
  station_file = NULL,
  overwrite = FALSE,
  append = TRUE,
  keep_open = FALSE,
  ...) {

  if (is.null(station_file) || is.null(pipe_house)) {
    return("No station file path (`pipe_house`) or filename ('station_file')!")
  }
  station_file <- basename(station_file)
  station_file <- file.path(pipe_house$ipip_room, station_file)
  sf_tmp <- file.path(tempdir(), "sf", basename(station_file))
  # check sf
  if (!is.null(sf) && !is.list(sf)) {
    return("Error: \'sf\' station file must be a list object!")
  }

  ex <- file.exists(station_file)
  extmp <- file.exists(sf_tmp)

  # if sf, sfc and station_file files exist delete station_file
  if (all(ex, extmp, !is.null(sf))) {
    unlink(station_file, recursive = TRUE)
    ex <- FALSE
  }
  if (all(!append, extmp, !is.null(sf))) {
    unlink(sf_tmp, recursive = TRUE)
    extmp <- FALSE
  }
  if (all(!append, ex, !is.null(sf))) {
    unlink(station_file, recursive = TRUE)
    ex <- FALSE
  }

  if (all(ex, !extmp, !is.null(sf))) {
    dir.create(path = sf_tmp, recursive = TRUE)
    stf <- readRDS(station_file)
    lapply(seq_along(stf), function(i) {
      saveRDS(object = stf[[i]],
        file = file.path(sf_tmp, paste0(names(stf)[i]))
      )
    })
    extmp <- TRUE
    unlink(station_file, recursive = TRUE)
  }

  if (all(!is.null(sf), extmp)) {
    # merge sf and extmp
    lapply(seq_along(sf), function(i) {
      if (file.exists(file.path(sf_tmp, names(sf)[i])) && !overwrite) {
        invisible(names(sf)[i])
      } else {
        saveRDS(
          object = sf[[i]],
          file = file.path(sf_tmp, paste0(names(sf)[i]))
        )
        invisible(names(sf)[i])
      }
    })
    sf <- NULL
  }

  if (all(extmp, is.null(sf))) {
    # write temp station to station file
    sf_names <- list.files(path = sf_tmp, full.names = TRUE)
    sf <- lapply(seq_along(sf_names), function(i) {
      sfi <- readRDS(sf_names[i])
      return(sfi)
    })
    names(sf) <- basename(sf_names)
  }

  # save station file
  if (all(!is.null(sf))) {
    sf <- sf[order(names(sf))]
    saveRDS(sf, file = station_file)
  }
  if (all(!keep_open, extmp)) unlink(sf_tmp, recursive = TRUE)
  if (all(keep_open, !extmp)) {
    sfc <- ipayipi::open_sf_con(pipe_house = pipe_house, station_file =
      station_file)
  } else {
    sfc <- NULL
  }

  return(list(station_file = station_file, sfc = sfc))
}