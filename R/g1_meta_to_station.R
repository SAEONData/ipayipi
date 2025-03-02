#' @title Open and check rainfall event metadata database.
#' @description Reads standardised metadata and appends records the matching station standard titles.
#' @inheritParams logger_data_import_batch
#' @param input_dir Directory from which to retreieve a metadata file read and saved by `ipayipi::meta_read()`.
#' @param meta_file File name of the standardised rainfall metadata database.
#' @param file_ext Extension of the metadata file. Defaults to ".rmds".
#' @param station_ext The extension of station files. Used by `ipayipi::dta_list()` to search for station files. Defaults to ".ipip".
#' @param in_station_meta_name The file name (without extension) of the metadata file that will be filtered and appended to a station file.
#' @keywords logger data processing; field metadata; data pipeline; supplementary data; field notes
#' @author Paul J. Gordijn
#' @return Standardised data table of events.
#' @details Reads in an events database or sheet in 'csv' format. Checks that column names have been standardised. Transforms the date-time columns to a standardised format --- ** this format and timezone must match that used by the data pipeline **.
#' @md
#' @export
meta_to_station <- function(
  pipe_house = NULL,
  input_dir = NULL,
  meta_file = "aa_event_db",
  file_ext = ".rmds",
  in_station_meta_name = "meta_events",
  stnd_title_col_name = "stnd_title",
  wanted = NULL,
  unwanted = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  ...
) {
  # assignments
  station_ext <- ".ipip"

  if (is.null(input_dir)) input_dir <- pipe_house$ipip_room
  # if we need to read in object
  if (is.character(meta_file)) {
    meta_file <- file.path(input_dir, paste0(meta_file, file_ext))
    if (!file.exists(meta_file)) {
      cli::cli_abort("The events metadata database does not exist!")
    }
    edb <- readRDS(meta_file)
  }
  # get list of stations
  slist <- ipayipi::dta_list(input_dir = input_dir,
    file_ext = station_ext, wanted = wanted, unwanted = unwanted,
    prompt = FALSE, recurr = FALSE
  )
  if (length(slist) == 0) cli::cli_abort(
    "No station files detected in the {.var ipip_room}: {pipe_house$ipip_room}"
  )
  if (verbose || xtra_v || chunk_v) cli::cli_h1(
    "Adding metadata to {length(slist)} station{?s}"
  )
  r <- lapply(slist, function(x) {
    sfc <- ipayipi::sf_open_con(pipe_house = pipe_house, station_file = x,
      verbose = verbose, chunk_v = chunk_v
    )
    sfn <- gsub(pattern = station_ext, replacement = "", x = x)
    sfn <- basename(sfn)
    mdta <- edb[which(edb[[stnd_title_col_name]] %in% sfn[1])]
    class(mdta) <- c(class(mdta), "ipip-sf_rds")
    # write event metadata to temporary station file
    sf_dta_rm(sfc = sfc, rm = in_station_meta_name)
    if (chunk_v) cli::cli_inform(c(" " = "Chunking logger event data"))
    sl <- sf_dta_wr(
      dta_room = file.path(dirname((sfc[1])), in_station_meta_name),
      dta = mdta, tn = in_station_meta_name, rit = "event_based",
      ri = "discnt", chunk_v = chunk_v
    )
    sf_log_update(sfc = sfc, log_update = sl)
    sf_write(pipe_house = pipe_house, station_file = x,
      overwrite = TRUE, append = FALSE, chunk_v = chunk_v
    )
    if (verbose || xtra_v || chunk_v) cli::cli_inform(c(
      "v" = "{basename(x)}, done ..."
    ))
  })
  rm(r)
  if (verbose || xtra_v || chunk_v) cli::cli_h1("")
  invisible()
}