#' @title Append logger data files to 'station file'
#' @description Appends phenomena data and metadata into continuous or discontinuous data streams in the form of station files.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param overwrite_sf If `TRUE` then original data (station data) is disgarded in favour of new data. If TRUE both data sets will be evaluated
#'  and where there are NA values, a replacement, if available, will be used to replace the NA value. Defaults to FALSE.
#' @param by_station_table If TRUE then multiple station tables will not be kept in the same station file. Defaults to FALSE.
#' @param station_ext The extension of the station file. Defaults to 'ipip'.
#' @param sts_file_ext Extension of standardised files in the 'nomvet_room'.
#' @param prompt Prompt logical passed for selection of 'nomvet' files.
#' @param verbose Print some details on the files being processed? Logical.
#' @param xtra_v Logical. Whether or not to report xtra messages, progess, plus print data tables.
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on Linux.
#' @param keep_open Logical. If TRUE the station file connection is kept in the temporary directory after the function is finished processing.
#' @author Paul J. Gordijn
#' @keywords meteorological data; data pipeline; append data; logger data
#' @details
#'  This function searches for all standardised files in the `nomvet_room` and existing 'station files' in the `ipip_room`. Files from the
#'  `nomvet_room` that have not been appended to station files will be appended to appropriate stations. Appending is done via the
#'  `ipayipi::append_station()`, `ipayipi::append_phen_data()`, and `ipayipi::append_tables()` functions.
#'
#'  If the data is continuous the function ensures continuous date-time values between missing data chuncks. Missing values are fillled as NA. Event-based or discontinuous data are not treated as continuous data streams.
#'
#'  New phenomena are seemlessly included in the appending process. Moreover, each phenomena is evaluated in turn allowing for a choise of overwring old data with new data or _vice versa_.
#' @export
append_station_batch <- function(
  pipe_house = NULL,
  overwrite_sf = FALSE,
  by_station_table = FALSE,
  station_ext = ".ipip",
  sts_file_ext = ".ipi",
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  phen_id = TRUE,
  verbose = FALSE,
  xtra_v = FALSE,
  keep_open = FALSE,
  cores = getOption("mc.cores", 2L),
  big_data = TRUE,
  ...
) {
  # overwrite_sf = FALSE
  # by_station_table = FALSE
  # station_ext = ".ipip"
  # sts_file_ext = ".ipi"
  # prompt = FALSE
  # wanted = NULL
  # unwanted = NULL
  # phen_id = TRUE
  # verbose = TRUE
  # keep_open = FALSE
  # cores = getOption("mc.cores", 2L)
  # con_f = "open_sf_con2"
  # big_data <- TRUE
  # get list of station names in the ipip directory
  station_files <- ipayipi::dta_list(
    input_dir = pipe_house$ipip_room, file_ext = station_ext, prompt = FALSE,
    recurr = FALSE, baros = FALSE, unwanted = NULL, wanted = NULL
  )
  # list of standardised files in the nomvet_room
  nom_files <- ipayipi::dta_list(
    input_dir = pipe_house$nomvet_room, file_ext = sts_file_ext,
    prompt = prompt, recurr = FALSE, baros = FALSE, unwanted = unwanted,
    wanted = wanted
  )
  nom_stations <- sapply(nom_files, function(x) {
    z <- readRDS(file.path(pipe_house$nomvet_room, x))
    if (by_station_table) {
      nomvet_station <- paste(
        z$data_summary$stnd_title[1], z$data_summary$table_name[1],
        sep = "_"
      )
    } else {
      nomvet_station <- z$data_summary$stnd_title[1]
    }
    return(nomvet_station)
  })
  station_files <- gsub(station_ext, "", station_files)
  ## statread
  all_station_files <- unlist(lapply(station_files, function(x) {
    sfc <- ipayipi::open_sf_con2(pipe_house = pipe_house,
      station_file = paste0(x, station_ext), tmp = TRUE, cores = cores,
      xtra_v = xtra_v, verbose = verbose
    )
    all_station_files <- readRDS(sfc[names(sfc) %in% "data_summary"])[[
      "nomvet_name"
    ]]
    names(all_station_files) <- rep(x, length(all_station_files))
    return(all_station_files)
  }))
  new_station_files <- nom_stations[
    !names(nom_stations) %in% all_station_files
  ]
  stations_to_update <- unique(new_station_files)
  cr_msg <- padr(
    core_message = paste0(" Updating ", length(stations_to_update),
      " stations with ", length(new_station_files),
      " standarised files ... ", collape = ""
    ), wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0)
  )
  ipayipi::msg(cr_msg, verbose)

  # update and/or create new stations
  # upgraded_stations <- lapply(seq_along(new_station_files), function(i) {
  upgraded_stations <- lapply(seq_along(new_station_files), function(i) {
    # write the table name onto the 'new_data'
    new_data <- readRDS(file.path(pipe_house$nomvet_room,
      names(new_station_files[i])
    ))
    new_data$phens$table_name <- new_data$data_summary$table_name[1]
    cr_msg <- padr(
      core_message = paste0(" +> ", i, ": ", names(new_station_files[i]),
        collapes = ""
      ), wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(1, 1)
    )
    ipayipi::msg(cr_msg, verbose)
    # get/make station file
    fp <- file.path(
      pipe_house$ipip_room, paste0(new_station_files[i], station_ext)
    )
    if (!file.exists(fp)) {
      # create new station file
      ipayipi::write_station(pipe_house = pipe_house, sf = new_data,
        station_file = paste0(new_station_files[i], station_ext),
        overwrite = TRUE, append = FALSE, keep_open = TRUE
      )
    } else {
      # append data
      station_file <- paste0(new_station_files[i], station_ext)
      # append function, then save output as new station
      args <- list(pipe_house = pipe_house,
        station_file = station_file, new_data = new_data,
        overwrite_sf = overwrite_sf, by_station_table = by_station_table,
        phen_id = phen_id, verbose = verbose, xtra_v = xtra_v, cores = cores
      )
      if (big_data) f <- "append_station3"
      do.call(f, args)
    }
    # close station file connection if finished with the station
    j <- i + 1
    if (j > length(new_station_files)) j <- length(new_station_files)
    sfl <- new_station_files[j:length(new_station_files)]
    sfl <- sfl[sfl %in% new_station_files[i]]
    sfl <- sfl[!names(sfl) %in% names(new_station_files[i])]
    if (any(length(sfl) == 0, i == j)) {
      ipayipi::write_station(pipe_house = pipe_house, station_file =
          paste0(new_station_files[i], station_ext), append = TRUE,
        overwrite = TRUE, keep_open = keep_open
      )
    }
    invisible(new_station_files[i])
  })
  upgraded_stations <- unique(upgraded_stations)
  cr_msg <- padr(core_message = paste0("  stations appended  ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0)
  )
  ipayipi::msg(cr_msg, verbose)
  invisible(upgraded_stations)
}