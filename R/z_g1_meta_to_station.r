#' @title Open and check rainfall event metadata database.
#' @description Reads standardised metadata and appends records the matching
#'  station.
#' @param pipe_house List of pipeline directories. __See__
#'  `ipayipi::ipip_init()` __for details__.
#' @param input_dir Directory from which to retreieve a metadata file
#'  read and saved by `ipayipi::meta_read()`.
#' @param meta_file File name of the standardised rainfall metadata
#'  database.
#' @param file_ext Extension of the metadata file. Defaults to ".rmds".
#' @param station_ext The extension of station files. Used by
#'  `ipayipi::dta_list()` to search for station files. Defaults to ".ipip".
#' @param in_station_meta_name The file name (without extension) of the
#'  metadata file that will be filtered and appended to a station file.
#' @param wanted A strong containing keywords to use to filter which stations
#'  are selected for processing. Multiple search kewords should be seperated
#'  with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted
#'  stations.
#' @param ... Additional arguments passed to `ipayipi::dta_list()`.
#' @keywords logger data processing; field metadata; data pipeline;
#'  supplementary data; field notes
#' @author Paul J. Gordijn
#' @return Standardised data table of events.
#' @details Reads in an events database or sheet in 'csv' format. Checks that
#'  column names have been standardised. Transforms the date-time columns to
#'  a standardised format --- ** this format and timezone must match that used
#'  by the data pipeline **.
#' @md
#' @export
meta_to_station <- function(
  pipe_house = NULL,
  input_dir = NULL,
  meta_file = NULL,
  file_ext = ".rmds",
  station_ext = ".ipip",
  in_station_meta_name = "meta_events",
  wanted = NULL,
  unwanted = NULL,
  ...
) {
  # avoid no visible bind for data.table variables
  "%ilike%" <- NULL
  if (is.null(input_dir)) input_dir <- pipe_house$ipip_room
  # if we need to read in object
  if (is.character(meta_file)) {
    meta_file <- file.path(input_dir, paste0(meta_file,
      file_ext))
    if (!file.exists(meta_file)) {
      stop("The events metadata database does not exist!")
    }
    edb <- readRDS(paste0(meta_file))
  }
  # get list of stations
  slist <- ipayipi::dta_list(input_dir = input_dir,
    file_ext = station_ext, ...)
  sl <- gsub(pattern = station_ext, replacement = "", x = slist)
  s <- names(edb)[names(edb) %ilike% "station|site"]
  edb <- edb[eval(parse(text = s)) %in% unique(sl)]
  slist <- slist[slist %ilike% unique(edb[[s]])]
  cr_msg <- padr(core_message =
    paste0(" Adding metadata to ", length(slist),
      " stations", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  r <- lapply(slist, function(x) {
    sf <- readRDS(file.path(input_dir, x))
    sfn <- gsub(pattern = station_ext, replacement = "", x = x)
    sf[[in_station_meta_name]] <- edb[
      eval(parse(text = paste0(s, " == \"", sfn, "\"", collapse = "")))]
    saveRDS(sf, file = file.path(input_dir, x))
    cr_msg <- padr(core_message =
      paste0(basename(x), " done ...", collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(-1, 2))
    message(cr_msg)
  })
  rm(r)
  cr_msg <- padr(core_message =
    paste0(" Metadata added  ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  return(message(cr_msg))
}