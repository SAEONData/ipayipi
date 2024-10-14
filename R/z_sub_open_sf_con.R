#' @title Establishes a connection to an ipayipi station file.
#' @description Package subroutine for opening a station file for work. Chuncks station file data with 'date_time' column and generates index. Reduces data read into memory when processing by accessing chunks filtered by date and time.
#' @param pipe_house Optional. Used to generate filepath to station file name. List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param station_file Path (with file name and extension) to 'ipip' station file.
#' @param tmp Logical that defaults to FALSE..If `FALSE` then the temporary station is updated with new tables from the station file (old tables in the temporary station file are not overwritten). If `TRUE` then the temporary station file is not updated with extra/new tables in station file is ignored.
#' @param chunk_i Time interval, as a string, used to chunk time series data. Only tables selected using this regex, "^raw_*|^dt_*", are chunked. A chunk index is creater to facilitate rapid read/writing of data.
#' @param i_zeros Used to name chunked files; the number of leading zeros to include before a chunk number index.
#' @param buff_period Time periods represented by a character string used to buffer the min and max date-times around index creation. Should ideally be at least 10 years. If new data extends before or beyond the index dates then data will be rechunked, and a new index generated.
#' @param tv String vector of the names of values or tables to read from the station file. The strings provided here are used to filter items in the `sfc` object. If left NULL (default) all tables will be opened into the 'tmp' folder. Table names are matched with data.tables %ilike% operator.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param xtra_v Extra verbose messages. Logical.
#' @keywords Internal
#' @export
#' @noRd
#' @author Paul J Gordijn
#' @return Station file connection file paths. To be used in the pipeline.
#' @details This function will not overwrite temporary station tables/data. If there is no chunked versino of the station file available, the station file ("ipip" file) will be chunked, and the connection established.
open_sf_con <- function(
  pipe_house = NULL,
  station_file = NULL,
  tmp = TRUE,
  chunk_i = NULL,
  i_zeros = 5,
  rit = NULL,
  ri = NULL,
  verbose = FALSE,
  tv = NULL,
  xtra_v = FALSE,
  ...
) {
  "%ilike%" <- "table_name" <- NULL
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

  if (!file.exists(station_file) && !is.null(pipe_house)) {
    ipayipi::msg("Error: no station file detected!", verbose = xtra_v)
    return(NULL)
  }

  # tmp dir setup ----
  # check if the sf_tmp alread exists
  sf_tmp <- file.path(getOption("chunk_dir"), "sf", basename(station_file))
  sf_tmp_ex <- file.exists(sf_tmp)
  if (!sf_tmp_ex) dir.create(path = sf_tmp, recursive = TRUE)

  # write station file data to tmp file/dirs ----
  # will append tmp data
  if (!tmp || !sf_tmp_ex) {
    sfn <- names(readRDS(station_file))
    if (!is.null(tv)) sfn <- sfn[sfn %ilike% tv]
    lapply(sfn, function(x) {
      ipayipi::msg(paste0("Extracting: ", station_file, ": ", x), xtra_v)
      sfx <- readRDS(station_file)[[x]]
      # chunk data ----
      ds <- NULL
      if (x %ilike% "^raw_*") {
        ds <- readRDS(station_file)[["data_summary"]]
        rit <- ds[table_name %in% x]$record_interval_type[1]
        ds <- ds[table_name %in% x]$record_interval[1]
      }
      if (x %ilike% "^dt_*") {
        ds <- readRDS(station_file)[["phens_dt"]]
        rit <- ds[table_name %in% x]$record_interval_type[1]
        ds <- ds[table_name %in% x]$dt_record_interval[1]
      }
      if (!is.null(ds)) {
        ds <- ipayipi::sts_interval_name(ds)
        ri <- ds[["sts_intv"]]
      }
      s <- ipayipi::sf_dta_wr(dta_room = file.path(sf_tmp, x),
        dta = sfx, tn = x, rit = rit, ri = ri, verbose = verbose,
        overwrite = TRUE, xtra_v = xtra_v
      )
      invisible(file.path(sf_tmp, x))
    })
  }
  # list files and dirs ----
  sfc <- list.files(sf_tmp, recursive = TRUE, full.names = TRUE)
  sfc <- sfc[!basename(sfc) %ilike% "^i_"]
  sfcc <- dirname(sfc[basename(sfc) %ilike% "aindxr"])
  names(sfcc) <- basename(sfcc)
  sfc <- sfc[!basename(sfc) %ilike% "aindxr"]
  names(sfc) <- basename(sfc)
  sfc <- c(sfc, sfcc)
  return(sfc)
}