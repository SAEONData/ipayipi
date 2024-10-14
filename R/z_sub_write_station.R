#' @title Writes a station file to disk
#' @description Takes input station values and tables and saves them to disk.
#' @param pipe_house Required. List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param sf Station file object.
#' @param station_file Path to station file. Required.
#' @param overwrite If `TRUE`: old data overwritten. If `FALSE`: old data not overwritten.
#' @param append Logical:
#'  - `TRUE`: new values/tables will be written to an existing station file.
#'  - `FALSE`: Old station file deleted, and new station created.
#' NB! If `append` and `overwrite` == `TRUE`: Station tables/values will be overwritten by tables/values with similar names.
#' @param keep_open Maintains the station connection, i.e., the 'hidden' station file is not removed (deleted).
#' @keywords Internal
#' @export
#' @noRd
#' @author Paul J Gordijn
#' @return File path of station file.
#' @details
#'
#' #' __Scenario 1__: sf R object
#' 'sf R object' written as a station file on disk ('sf on disk').
#'
#' __Scenario 2__: 'hidden' sf on disk + sf on disk + sf R object
#' __NB!__ In this scenario the station file on disk is _always_ assumed to be outdated/older than the 'hidden' station file on disk. Therefore, only the station file object, and 'hidden' station file are *merged. The merged file is written as a station file on disk ('sf on disk').
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
  keep_open = TRUE,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  "%ilike%" <- NULL

  ipayipi::msg(paste0("Writing station: ", station_file), xtra_v)
  if (is.null(station_file) || is.null(pipe_house)) {
    return("No station file path (`pipe_house`) or filename ('station_file')!")
  }
  sf_dir <- dirname(station_file)
  if (!is.null(pipe_house)) {
    sf_dir <- dirname(station_file)
    sf_dir <- sub("^\\.", "", sf_dir)
    station_file <- basename(station_file)
    pipe_house$ipip_room <- gsub(
      paste0("*", sf_dir, "$"), "", pipe_house$ipip_room
    )
    station_file <- file.path(pipe_house$ipip_room, sf_dir, station_file)
    station_file <- gsub("^[/]|^[//]|^[\\]|[/]$|[\\]$", "", station_file)
  }
  sf_tmp <- file.path(getOption("chunk_dir"), "sf", basename(station_file))

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
    sfn <- list.files(path = sf_tmp, full.names = TRUE, recursive = TRUE)
    sfn <- sfn[!basename(sfn) %ilike% "^i_"]
    sfnn <- dirname(sfn[basename(sfn) %ilike% "aindxr"])
    names(sfnn) <- basename(sfnn)
    sfn <- sfn[!basename(sfn) %ilike% "aindxr"]
    names(sfn) <- basename(sfn)
    sfn <- sfn[order(sfn)]
    # need option here to read chunked data
    # filter out index files and aindxr from file
    # write temp station to station file paths
    sfd <- lapply(seq_along(sfnn), function(i) {
      fs <- list.files(file.path(sfnn[i]), full.names = TRUE,
        recursive = TRUE
      )
      fs <- fs[!fs %ilike% "*aindxr$"]
      dta <- data.table::rbindlist(lapply(fs, readRDS),
        fill = TRUE, use.names = TRUE
      )
      return(dta)
    })
    names(sfd) <- basename(sfnn)
    sfo <- lapply(seq_along(sfn), function(i) readRDS(sfn[i]))
    names(sfo) <- basename(sfn)
    sf <- c(sfd, sfo)
  }

  # save station file
  if (!is.null(sf)) {
    sf <- sf[order(names(sf))]
    saveRDS(sf, file = station_file)
  }
  if (all(!keep_open, extmp)) unlink(sf_tmp, recursive = TRUE)
  if (all(keep_open, !extmp)) {
    sfc <- ipayipi::open_sf_con(pipe_house = pipe_house, station_file =
        station_file, verbose = verbose, xtra_v = xtra_v
    )
  } else {
    sfc <- NULL
  }

  return(list(station_file = station_file, sfc = sfc))
}