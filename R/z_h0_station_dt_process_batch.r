#' @title Append logger data files to 'station file'
#' @description Appends phenomena data and metadata into continuous or
#'  discontinuous data streams in the form of station files.
#' @param pipe_house List of pipeline directories. __See__
#'  `ipayipi::ipip_init()` __for details__.
#' @param overwrite_sf If `TRUE` then original data (station data) is
#'  disgarded in favour of new data. If TRUE both data sets will be evaluated
#'  and where there are NA values, a replacement, if available, will be used
#'  to replace the NA value. Defaults to FALSE.
#' @param by_station_table If TRUE then multiple station tables will not be
#'  kept in the same station file. Defaults to FALSE.
#' @param station_ext The extension of the station file. Defaults to 'ipip'.
#' @param sts_file_ext Extension of standardised files in the 'nomvet_room'.
#' @param prompt Prompt logical passed for selection of 'nomvet' files.
#' @param verbose Print some details on the files being processed? Logical.
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on Linux.
#' @author Paul J. Gordijn
#' @keywords meteorological data; data pipeline; append data; logger data
#' @details
#'  This function searches for all standardised files in the `nomvet_room`
#'  and existing 'station files' in the `ipip_room`. Files from the
#'  `nomvet_room` that have not been appended to station files will be
#'  appended to appropriate stations. Appending is done via the
#'  `ipayipi::append_station()`, `ipayipi::append_phen_data()`, and
#'  `ipayipi::append_tables()` functions.
#'
#'  If the data is continuous the function ensures continuous date-time values
#'  between missing data chuncks. Missing values are fillled as NA. Event-based
#'  or discontinuous data are not treated as continuous data streams.
#'
#'  New phenomena are seemlessly included in the appending process. Moreover,
#'  each phenomena is evaluated in turn allowing for a choise of overwring old
#'  data with new data or _vice versa_.
#' @export
dt_process_batch <- function(
  pipe_house = NULL,
  pipe_seq = NULL,
  unwanted_tbls = "_tmp",
  output_dt_preffix = "dt_",
  output_dt_suffix = NULL,
  overwrite_pipe_memory = FALSE,
  station_ext = ".ipip",
  wanted = NULL,
  unwanted = NULL,
  prompt = FALSE,
  verbose = FALSE,
  cores = getOption("mc.cores", 2L),
  ...
) {

  # get list of station names in the ipip directory
  station_files <- ipayipi::dta_list(
    input_dir = pipe_house$ipip_room, file_ext = station_ext, prompt = prompt,
    recurr = FALSE, baros = FALSE, unwanted = unwanted, wanted = wanted)

  cr_msg <- padr(core_message =
    paste0(" Data processing: ", length(station_files), " station files ",
    collape = ""), wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  msg(cr_msg, verbose)

  # update and/or create new stations
  # upgraded_stations <- lapply(seq_along(new_station_files), function(i) {
  station_files <- parallel::mclapply(seq_along(station_files), function(i) {
    cr_msg <- padr(core_message = paste0(
        " +> ", station_files[i], collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(1, 1))
    msg(cr_msg, verbose)
    # process data
    dtp <- attempt::attempt(
      ipayipi::dt_process(station_file = station_files[i],
        pipe_house = pipe_house, pipe_seq = pipe_seq,
        output_dt_preffix = output_dt_preffix,
        output_dt_suffix = output_dt_suffix,
        overwrite_pipe_memory = overwrite_pipe_memory,
        verbose = verbose)
    )
    invisible(station_files[i])
  }, mc.cores = cores)
  # sapply(station_files, function(x) {
  #   ipayipi::write_station(pipe_house = pipe_house,
  #     station_file = paste0(x, station_ext), append = FALSE, overwrite =
  #     FALSE, keep_open = keep_open)
  #   return(x)
  # })
  cr_msg <- padr(core_message = paste0("  data processed  ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  msg(cr_msg, verbose)
  invisible(station_files)
}