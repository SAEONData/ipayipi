#' @title Open and check rainfall event metadata database.
#' @description Reads standardised metadata and appends records the matching station standard titles.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param input_dir Directory from which to retreieve a metadata file read and saved by `ipayipi::meta_read()`.
#' @param meta_file File name of the standardised rainfall metadata database.
#' @param file_ext Extension of the metadata file. Defaults to ".rmds".
#' @param station_ext The extension of station files. Used by `ipayipi::dta_list()` to search for station files. Defaults to ".ipip".
#' @param in_station_meta_name The file name (without extension) of the metadata file that will be filtered and appended to a station file.
#' @param wanted A strong containing keywords to use to filter which stations are selected for processing. Multiple search kewords should be seperated with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param keep_open Logical. Keep _hidden_ 'station_file' open for ease of access. Defaults to `TRUE`.
#' @param ... Additional arguments passed to `ipayipi::dta_list()`.
#' @keywords logger data processing; field metadata; data pipeline; supplementary data; field notes
#' @author Paul J. Gordijn
#' @return Standardised data table of events.
#' @details Reads in an events database or sheet in 'csv' format. Checks that column names have been standardised. Transforms the date-time columns to a standardised format --- ** this format and timezone must match that used by the data pipeline **.
#' @md
#' @export
meta_to_station <- function(
  pipe_house = NULL,
  input_dir = NULL,
  meta_file = "event_db",
  file_ext = ".rmds",
  station_ext = ".ipip",
  in_station_meta_name = "meta_events",
  stnd_title_col_name = "stnd_title",
  wanted = NULL,
  unwanted = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  keep_open = TRUE,
  ...
) {
  if (is.null(input_dir)) input_dir <- pipe_house$ipip_room
  # if we need to read in object
  if (is.character(meta_file)) {
    meta_file <- file.path(input_dir, paste0(meta_file, file_ext))
    if (!file.exists(meta_file)) {
      stop("The events metadata database does not exist!")
    }
    edb <- readRDS(paste0(meta_file))
  }
  # get list of stations
  slist <- ipayipi::dta_list(input_dir = input_dir,
    file_ext = station_ext, wanted = wanted, unwanted = unwanted,
    prompt = FALSE, recurr = FALSE
  )
  cr_msg <- padr(core_message = paste0(
    " Adding metadata to ", length(slist), " stations", collapes = ""
  ), wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
  force_extras = FALSE, justf = c(0, 0)
  )
  ipayipi::msg(cr_msg, verbose)
  r <- lapply(slist, function(x) {
    sfc <- ipayipi::open_sf_con(pipe_house = pipe_house, station_file = x,
      verbose = verbose, xtra_v = xtra_v
    )
    sfn <- gsub(pattern = station_ext, replacement = "", x = x)
    sfn <- basename(sfn)
    mdta <- edb[which(edb[[stnd_title_col_name]] %in% sfn[1])]
    # write event metadata to temporary station file
    unlink(sfc[in_station_meta_name], recursive = TRUE)
    ipayipi::msg("Chunking logger event data", xtra_v)
    ipayipi::sf_dta_wr(
      dta_room = file.path(dirname((sfc[1])), in_station_meta_name),
      dta = mdta, overwrite = TRUE, tn = in_station_meta_name,
      verbose = verbose, xtra_v = xtra_v
    )
    write_station(pipe_house = pipe_house, station_file = x,
      overwrite = TRUE, append = FALSE
    )
    cr_msg <- padr(core_message = paste0(
      basename(x), " done ...", collapes = ""
    ), wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(-1, 2)
    )
    ipayipi::msg(cr_msg, verbose)
  })
  rm(r)
  cr_msg <- padr(core_message = paste0(" Metadata added  ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0)
  )
  return(ipayipi::msg(cr_msg, verbose))
}