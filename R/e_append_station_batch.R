#' @title Append logger data files to 'station file'
#' @description Appends phenomena data and metadata into continuous or discontinuous data streams in the form of station files.
#' @inheritParams logger_data_import_batch
#' @param overwrite_sf If `TRUE` then original data (station data) is disgarded in favour of new data. If TRUE both data sets will be evaluated
#'  and where there are NA values, a replacement, if available, will be used to replace the NA value. Defaults to FALSE.
#' @param phen_id Logical. If TRUE then the station 'phens' table will be used to update the 'phens_data_summary'. If FALSE this will be ignored.
#' @param chunk_v Set to TRUE to view messaging regarding the progress of data chunking.
#' @author Paul J. Gordijn
#' @keywords time-series data; data pipeline; append data; logger data
#' @details
#'  This function searches for all standardised files in the `nomvet_room` and existing 'station files' in the `ipip_room`. Files from the `nomvet_room` that have not been appended to station files will be appended to appropriate stations. Appending is done via the `ipayipi::append_station()`, `ipayipi::append_phen_data()`, and `ipayipi::append_tables()` functions.
#'
#'  If the data is continuous the function ensures continuous date-time values between missing data chuncks. Missing values are fillled as NA. Event-based or discontinuous data are not treated as continuous data streams.
#'
#'  New phenomena are seemlessly included in the appending process. Moreover, each phenomena is evaluated in turn allowing for a choise of overwring old data with new data or _vice versa_.
#' @export
append_station_batch <- function(
  pipe_house = NULL,
  overwrite_sf = FALSE,
  wanted = NULL,
  unwanted = NULL,
  phen_id = TRUE,
  prompt = FALSE,
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  ...
) {

  # assignments
  sf_ext <- ".ipip"
  sts_file_ext <- ".ipi"

  # get list of station names in the ipip directory
  station_files <- ipayipi::dta_list(
    input_dir = pipe_house$ipip_room, file_ext = sf_ext, prompt = FALSE,
    recurr = FALSE, baros = FALSE, unwanted = NULL, wanted = NULL
  )
  # list of standardised files in the nomvet_room
  nom_files <- ipayipi::dta_list(
    input_dir = pipe_house$nomvet_room, file_ext = sts_file_ext,
    prompt = prompt, recurr = FALSE, baros = FALSE, unwanted = unwanted,
    wanted = wanted
  )
  nom_stations <- future.apply::future_sapply(nom_files, function(x) {
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
  station_files <- gsub(sf_ext, "", station_files)
  ## statread
  if (xtra_v || chunk_v && length(station_files)) cli::cli_inform(c(
    ">" = "Decompressing extant station files for rapid querying.",
    " " = "Query used to determine which files require append operations."
  ))
  all_station_files <- unlist(lapply(
    station_files, function(x) {
      sfc <- sf_open_con(pipe_house = pipe_house,
        station_file = paste0(x, sf_ext), chunk_v = chunk_v
      )
      all_station_files <- sf_dta_read(sfc = sfc, tv = "data_summary"
      )[["data_summary"]][["nomvet_name"]]
      names(all_station_files) <- rep(x, length(all_station_files))
      return(all_station_files)
    }
  ))
  new_station_files <- nom_stations[
    !names(nom_stations) %in% all_station_files
  ]
  if (length(length(new_station_files)) == 0) {
    cli::cli_inform("v" = "Station files up to date.")
    invisible(NULL)
  }
  if (verbose || xtra_v || chunk_v) {
    cli::cli_h1(paste0("Updating {length(unique(new_station_files))} ",
      "station{?s} with {length(new_station_files)} standardised file{?s}"
    ))
  }
  if (fcoff()) {
    xtra_v <- FALSE
    chunk_v <- FALSE
  }
  # update and/or create new stations
  # upgraded_stations <- lapply(seq_along(new_station_files), function(i) {
  sf_dt <- data.table::data.table(sf = unlist(new_station_files),
    sf_file = names(new_station_files)
  )
  sf_dtl <- split.data.frame(sf_dt, f = factor(sf_dt$sf))
  ups <- future.apply::future_lapply(sf_dtl, function(x) {
    upgraded_stations <- lapply(seq_len(nrow(x)), function(i) {
      # write the table name onto the 'new_data'
      new_data <- readRDS(file.path(pipe_house$nomvet_room,
        x[i][["sf_file"]]
      ))
      new_data$phens$table_name <- new_data$data_summary$table_name[1]
      # get/make station file
      fp <- file.path(pipe_house$ipip_room, paste0(x[i][["sf"]], sf_ext))
      if (!file.exists(fp)) {
        # create new station file
        if (verbose || chunk_v || xtra_v) {
          cli::cli_h2("{paste0(x[i][[\'sf\']], sf_ext)} created:")
          cli::cli_inform(c(" " = "{i}: working on {x[i][[\'sf_file\']]};"))
        }
        sf_write(pipe_house = pipe_house, sf = new_data,
          station_file = paste0(x[i][["sf"]], sf_ext),
          chunk_v = chunk_v
        )
      } else {
        # append data
        station_file <- paste0(x[i][["sf"]], sf_ext)
        if (i == 1) {
          cli::cli_h2("Started with: {paste0(x[i][[\'sf\']], sf_ext)}:")
        }
        cli::cli_inform(c(" " = "{i}: working on {x[i][[\'sf_file\']]};"))
        # append function, then save output as new station
        sfc <- sf_open_con(pipe_house = pipe_house, station_file = station_file)
        append_station(pipe_house = pipe_house, station_file = station_file,
          new_data = new_data, overwrite_sf = overwrite_sf, sfc = sfc,
          phen_id = phen_id, verbose = verbose, chunk_v = chunk_v,
          xtra_v = xtra_v
        )
      }
      # close station file connection if finished with the station
      j <- i + 1
      if (j > nrow(x)) j <- nrow(x)
      if (i == j) {
        sf_write(pipe_house = pipe_house, station_file =
            paste0(x[i][["sf"]], sf_ext), chunk_v = chunk_v
        )
        if (verbose || chunk_v || xtra_v) {
          cli::cli_inform(c(
            "v" = "writing {paste0(x[i][[\'sf\']], sf_ext)}",
            after = " "
          ))
        }
      }
      return(x[i][["sf"]])
    })
    return(unique(upgraded_stations))
  })
  upgraded_stations <- ups
  if (verbose || xtra_v || chunk_v) {
    cli::cli_h1("")
    cli::cli_inform(c(
      "What next?",
      "v" = "Station files created/updated.",
      "i" = paste0("Station files are now decompressed for rapid read/write ",
        "access in further processing."
      ),
      ">" = "Process, visualise, export and query data with:",
      "*" = "{.var gap_eval_batch()},",
      "*" = "{.var dta_availability()},",
      "*" = "{.var meta_to_station()},",
      "*" = "{.var dt_process_batch()},",
      "*" = "{.var dta_flat_pull()}, and more..."
    ))
  }
  invisible(upgraded_stations)
}