#' @title Writes a station file to the d4_ipip_room
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
sf_write <- function(
  pipe_house = NULL,
  sf = NULL,
  station_file = NULL,
  overwrite = FALSE,
  chunk_v = FALSE,
  ...
) {
  "%ilike%" <- "%chin%" <- NULL
  "tbl_n" <- NULL

  # prep dirs ----
  if (chunk_v) cli::cli_inform("Writing station: {station_file}")
  if (is.null(station_file) && is.null(pipe_house)) {
    return("No station file path (`pipe_house`) or filename ('station_file')!")
  }
  sf_dir <- dirname(station_file)
  if (!is.null(pipe_house)) {
    sf_dir <- dirname(station_file)
    sf_dir <- sub("^\\.", "", sf_dir)
    station_file <- basename(station_file)
    pipe_house$d4_ipip_room <- gsub(
      paste0("*", sf_dir, "$"), "", pipe_house$d4_ipip_room
    )
    station_file <- file.path(pipe_house$d4_ipip_room, sf_dir, station_file)
    station_file <- gsub("^[/]|^[//]|^[\\]|[/]$|[\\]$", "", station_file)
  }
  sf_tmp <- file.path(getOption("chunk_dir"), "sf", basename(station_file))

  # check what exists
  ex <- file.exists(station_file)
  extmp <- file.exists(sf_tmp)

  # check here before overwriting non-tmp station file to see if
  # there is a data_summary and associated data tables, and phen tables
  #  - if these don't exist the the station should not be written!
  if (extmp) {
    sf_log <- readRDS(file.path(sf_tmp, "sf_log"))
    sfn <- sf_log$lg_tbl$tbl_n
    sfn_chk <- sfn %in% c("data_summary", "phens", "phen_data_summary",
      "phens_dt"
    )
    if (!sum(sfn_chk) >= 3) {
      cli::cli_abort(c("Missing {sfn[sfn_chk]} tables in {station_file}",
        "!" = "Cannot write station from tmp format to compressed RDS!"
      ))
    }
  }

  # merge station file in environment and the one on disk in ipip room
  if (!is.null(sf)) sf_env <- sf
  if (ex) sf_ipip <- readRDS(station_file)
  if (exists("sf_ipip") && exists("sf_env")) {
    sf_env_n <- names(sf_env)
    sf_ipip_n <- names(sf_ipip)
    sf_ipip <- sf_ipip[!sf_ipip_n %chin% sf_env_n]
    sf_ipip <- c(sf_ipip, sf_env)
    sf_ipip <- sf_ipip[order(names(sf_ipip))]
  }
  if (!ex && !is.null(sf_env)) sf_ipip <- sf
  sf_ipip_n <- names(sf_ipip)

  # overwrite and table/items in sf_ipip with open items from the sf_log
  if (exists("sf_log")) {
    # remove tmp files from the sf log
    sf_log$lg_tbl <- sf_log$lg_tbl[
      !tbl_n %ilike% "_tmp$|_hrv_tbl_|dt_working"
    ]
    sf_ipip <- sf_ipip[!names(sf_ipip) %chin% sf_log$lg_tbl[open == TRUE]$tbl_n]
    sf_ipip_n <- names(sf_ipip)

    # open files from the tmp dir location and add to sf_ipip
    sf_tmp_n <- sf_log$lg_tbl[open == TRUE]$tbl_n
    sfc <- file.path(sf_log$chnk_dir, sf_log$lg_tbl$tbl_n)
    names(sfc) <- sf_log$lg_tbl$tbl_n
    sf_tmp <- lapply(sf_tmp_n, function(x) {
      dta <- sf_dta_read(sfc = sfc, tv = x, return_dta = TRUE)
      dt_dta_open(dta_link = dta)
    })
    names(sf_tmp) <- sf_tmp_n
    sf_ipip <- c(sf_ipip, sf_tmp)
    sf_ipip <- sf_ipip[order(names(sf_ipip))]
  } else {
    sfc <- NULL
  }
  # save the station file to the ipip room
  if (exists("sf_ipip")) {
    saveRDS(sf_ipip, file = station_file)
  } else {
    cli::cli_abort("Failure to save station file: {station_file}")
  }
  return(list(station_file = station_file, sfc = sfc))
}