#' @title Pushes flattened station data into station files
#' @description Effectively the reverse of `ipayipi::dta_flat_pull()`.
#' @param pipe_house The directory in which to search for stations form which to extract time-series data. If working across different 'pipe_house' directories a 'pseudo' pipehouse object can be created, e.g., pipe_house <- list(ipip_room = "."); only the 'ipip_room' is listed here as this is the standard folder where stations are kept.
#' @param tab_names Vector of table names in a station files from which to extract data. Add items to the vector such that only one table per station is selected. Table names are selected via the `%in%` argument.
#' @param phen_name The field/column/name of the phenomena which to join into a single data table.
#' @param ri Record-interval string. This function will guess the record interval used to generate a flat table. However, the guess may not be possible if there is insufficient data.
#' @param output_dir The output directory where an output csv file is saved.
#' @param wanted A strong containing keywords to use to filter which stations are selected for processing. Multiple search kewords should be seperated with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param out_csv Logical. If TRUE a csv file is exported to the output directory.
#' @param out_csv_preffix Preffix for the output csv file. The phenomena name and then time interval by which the data are summarised are used as a suffix.
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on Linux systems.
#' @param file_ext The extension of the stations from where data will be extracted. Defaults to ".ipip".
#' @param prompt Logical. If `TRUE` a prompt will be called so that the user can interactively select station files.
#' @details Notethis function only extracts data from compressed station files. Development is required to modify this function to extract from 'temporary station files' to spped up data extraction from large files.
#' @keywords data pipeline; summarise time-series data; long-format data.
#' @return A list containing 1) the summarised data in a single data.table, 2) a character string representing the time interval by which the data has been summarised, 3) the list of stations used for the summary, 4) the name of the table and the name of the field for which data was summarised.
#' @author Paul J. Gordijn
#' @export
dta_flat_push <- function(
  pipe_house = NULL,
  flat_dta = NULL,
  tab_names = NULL,
  phen_name = NULL,
  ri = NULL,
  stations = NULL,
  recurr = FALSE,
  file_ext = ".ipip",
  ...
) {
  "date_time" <- NULL
  if (length(stations) == 0) return(NULL)

  slist <- file.path(pipe_house$ipip_room, paste0(stations))
  names(slist) <- gsub(paste0(file_ext, "$"), "", basename(slist))

  # check uniqueness of station names
  if (length(unique(names(slist))) != length(slist)) {
    warning("Non-unique station names---can't process!")
    return(NULL)
  }
  # check all stations exist
  if (any(!file.exists(gsub("^\\./", "", slist), recursive = TRUE))) {
    warning("Missing station in set directory!")
    return(NULL)
  }

  # write data to stations--tables
  t <- future.apply::future_lapply(seq_along(slist), function(i) {
    m <- readRDS(slist[i])
    tab_name <- names(m)[names(m) %in% tab_names][1]
    t <- m[[tab_name]]
    if (phen_name %in% names(t)) {
      imp <- flat_dta[date_time %in% t$date_time][[names(slist[i])]]
      t[[phen_name]] <- imp
      m[[tab_name]] <- t
      saveRDS(m, slist[i])
    }
    return(slist[i])
  })
  return(t)
}