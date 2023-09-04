#' @title Append meteorological station data
#' @description Appends two standardised 'ipayipi' meteorological' station
#'  files.
#' @param station_file The main data set to which data will be appended.
#' @param new_data The data that will be appended to the main 'hobo station'.
#' @param overwrite_old If `TRUE` then original data (station data) is
#'  disgarded in favour of new data. If TRUE both data sets will be evaluated
#'  and where there are NA values, a replacement, if available, will be used
#'  to replace the NA value. Defaults to FALSE.
#' @author Paul J Gordijn
#' @keywords meteorological data; data pipeline; append data;
#' @details
#'  This function searches for all standardised files in the `nomvet_room`
#'  and existing 'station files' in the `ipip_room`. Files from the
#'  `nomvet_room` that have not been appended to station files will be
#'   appended to appropriate stations. Appending is done via the
#'   `ipayipi::append_station()`, `ipayipi::append_phen_data()`, and
#'   `ipayipi::append_tables()` functions.
#'  If the data is continuous the function ensures continuous date-time values
#'  between missing data chuncks. Missing values are fillled as NA.
#' @export
append_station_batch <- function(
  ipip_room = NULL,
  nomvet_room = NULL,
  overwrite_sf = FALSE,
  by_station_table = FALSE,
  station_ext = ".ipip",
  sts_file_ext = ".ipi",
  ...
) {
  overwrite_sf = FALSE
  by_station_table = FALSE
  station_ext = ".ipip"
  sts_file_ext = ".ipi"
  # get list of station names in the ipip directory
  station_files <- ipayipi::dta_list(
    input_dir = ipip_room, file_ext = station_ext, prompt = FALSE,
    recurr = FALSE, baros = FALSE, unwanted = NULL, wanted = NULL)
  # list of standardised files in the nomvet_room
  nom_files <- ipayipi::dta_list(
    input_dir = nomvet_room, file_ext = sts_file_ext, prompt = FALSE,
    recurr = FALSE, baros = FALSE, unwanted = NULL)
  nom_stations <- sapply(nom_files, function(x) {
    z <- readRDS(file.path(nomvet_room, x))
    if (by_station_table) {
      nomvet_station <- paste(
        z$data_summary$stnd_title[1], z$data_summary$table_name[1],
        sep = "_")
    } else {
      nomvet_station <- z$data_summary$stnd_title[1]
    }
    return(nomvet_station)
  })
  station_files <- gsub(station_ext, "", station_files)
  all_station_files <- unlist(sapply(station_files, function(x) {
    all_station_files <- readRDS(file.path(ipip_room, paste0(x, station_ext)))$
      data_summary$nomvet_name
    return(all_station_files)
  }))
  new_station_files <- nom_stations[
    !names(nom_stations) %in% all_station_files]
  stations_to_update <- unique(new_station_files)
  cr_msg <- padr(core_message =
    paste0(" Updating ", length(stations_to_update), " stations with ",
      length(new_station_files), " standarised files ... ", collape = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)

  # update and/or create new stations
  # upgraded_stations <- lapply(seq_along(new_station_files), function(i) {
  upgraded_stations <- lapply(seq_along(new_station_files), function(i) {
    message(i)
    # write the table name onto the 'new_data'
    new_data <- readRDS(file.path(nomvet_room, names(new_station_files[i])))
    new_data$phens$table_name <- new_data$data_summary$table_name[1]
    cr_msg <- padr(core_message = paste0(
        " +> ", names(new_station_files[i]), collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(1, 1))
    message(cr_msg)
    # get/make station file
    if (!file.exists(
        file.path(ipip_room, paste0(new_station_files[i], station_ext)))) {
      # create station file
      saveRDS(
        new_data,
        file.path(ipip_room, paste0(new_station_files[i], station_ext))
      )
    } else {
      # append data
      station_file <- file.path(
        ipip_room, paste0(new_station_files[i], station_ext))
      # append function, then save output as new station
      station_file <- ipayipi::append_station(station_file = station_file,
        new_data = new_data, overwrite_sf = overwrite_sf,
        by_station_table = by_station_table)
      saveRDS(station_file,
        file = file.path(ipip_room, paste0(new_station_files[i], station_ext)))
    }
    invisible(new_station_files[i])
  })
  upgraded_stations <- unique(upgraded_stations)
  cr_msg <- padr(core_message = paste0("", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  invisible(upgraded_stations)
}