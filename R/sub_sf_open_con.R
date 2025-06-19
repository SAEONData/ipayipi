#' @title Establishes a connection to an ipayipi station file.
#' @description Package subroutine for opening a station file for work. Chuncks station file data with 'date_time' column and generates index. Reduces data read into memory when processing by accessing chunks filtered by date and time.
#' @param pipe_house Optional. Used to generate filepath to station file name. List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param station_file Path (with file name and extension) to 'ipip' station file.
#' @param tmp Logical that defaults to FALSE..If `FALSE` then the temporary station is updated with new tables from the station file (old tables in the temporary station file are not overwritten). If `TRUE` then the temporary station file is not updated with extra/new tables in station file is ignored.
#' @param chunk_i Time interval, as a string, used to chunk time series data. Only tables selected using this regex, "^raw_*|^dt_*", are chunked. A chunk index is creater to facilitate rapid read/writing of data.
#' @param i_zeros Used to name chunked files; the number of leading zeros to include before a chunk number index.
#' @param buff_period Time periods represented by a character string used to buffer the min and max date-times around index creation. Should ideally be at least 10 years. If new data extends before or beyond the index dates then data will be rechunked, and a new index generated.
#' @param tv String vector of the names of values or tables to read from the station file. The strings provided here are used to filter items in the `sfc` object. If left NULL (default) all tables will be opened into the 'tmp' folder. Table names are matched with data.tables %ilike% operator.
#' @param chunk_v Logical. Whether or not to report messages and progress.
#' @keywords Internal
#' @export
#' @noRd
#' @author Paul J Gordijn
#' @return Station file connection file paths. To be used in the pipeline.
#' @details This function will not overwrite temporary station tables/data. If there is no chunked versino of the station file available, the station file ("ipip" file) will be chunked, and the connection established.
sf_open_con <- function(
  pipe_house = NULL,
  station_file = NULL,
  sfc = NULL,
  tmp = TRUE,
  chunk_i = NULL,
  i_zeros = 5,
  rit = NULL,
  ri = NULL,
  tv = NULL,
  chunk_v = FALSE,
  ...
) {
  "%ilike%" <- "%chin%" <- NULL
  "table_name" <- "tbl_n" <- NULL

  if (!is.null(pipe_house) && !is.null(station_file)) {
    sf_dir <- dirname(station_file)
    sf_dir <- sub("^\\.", "", sf_dir)
    station_file <- basename(station_file)
    pipe_house$d4_ipip_room <- gsub(
      paste0("*", sf_dir, "$"), "", pipe_house$d4_ipip_room
    )
    station_file <- file.path(pipe_house$d4_ipip_room, sf_dir, station_file)
    station_file <- gsub("^[/]|^[//]|^[\\]|[/]$|[\\]$", "", station_file)
    if (!file.exists(station_file) && !is.null(pipe_house)) {
      cli::cli_warn(c("This station file {station_file} was not detected!"))
      return(NULL)
    }
    # tmp dir setup ----
    sf_tmp <- file.path(getOption("chunk_dir"), "sf", basename(station_file))
  }

  # use only station file - path must be provided in full
  if (is.null(pipe_house) && !is.null(station_file)) {
    sf_dir <- dirname(station_file)
    sf_dir <- sub("^\\.", "", sf_dir)
    sf_dir <- gsub("^[/]|^[//]|^[\\]|[/]$|[\\]$", "", sf_dir)
    station_file <- basename(station_file)
    station_file <- gsub("^[/]|^[//]|^[\\]|[/]$|[\\]$", "", station_file)
    if (!file.exists(file.path(sf_dir, station_file))) {
      cli::cli_warn(c(
        "Station file {station_file} was not detected here: {sf_dir}",
        " " = "Provide the full path to the station_file arg IF no pipe_house"
      ))
      return(NULL)
    }
    sf_tmp <- file.path(getOption("chunk_dir"), "sf", station_file)
    station_file <- file.path(sf_dir, station_file)
  }
  if (is.null(pipe_house) && is.null(station_file) && !is.null(sfc)) {
    # use sfc
    sf_tmp <- dirname(sfc[1])
  }


  # check if the sf_tmp alread exists
  sf_tmp_ex <- file.exists(sf_tmp)

  # if tmp sf doesn't exist creat a tmp file log that will be updated with
  #  records of open data tables
  if (!sf_tmp_ex) {
    dir.create(sf_tmp, recursive = TRUE)
    sfn <- names(readRDS(station_file))
    # generate empty station file tmp log
    sf_log <- list(
      lg_tbl = data.table::data.table(
        tbl_n = sfn,
        rit = NA_character_,
        ri = NA_character_,
        open = FALSE
      ),
      sf_file = station_file,
      chnk_dir = sf_tmp
    )
    saveRDS(sf_log, file.path(sf_tmp, "sf_log"))
  } else {
    sf_log <- readRDS(file.path(sf_tmp, "sf_log"))
    station_file <- sf_log$sf_file
    sfn <- NULL
  }
  # remove any NA tbl names
  sf_log$lg_tbl <- sf_log$lg_tbl[!is.na(tbl_n)]
  unlink(file.path(sf_log$chnk_dir, "NA"))

  # force opening of listed tables ('tv')
  # set default tv's
  dft_tv <- c("data_summary", "phens", "phen_data_summary")
  tv <- unique(c(tv, dft_tv))
  # abort if the table name tv is not in the station file tmp log
  null_tvs <- tv[!tv %chin% sf_log$lg_tbl$tbl]
  if (length(null_tvs > 0) && chunk_v) {
    cli::cli_inform(
      c("i" = "In {station_file} no tables called: {null_tvs}.",
        ">" = "table may be created ...")
    )
  }

  # refine tv depending on sf_log open files
  tv <- tv[!tv %chin% sf_log$lg_tbl[open == TRUE]$tbl_n]
  tv <- tv[!tv %chin% null_tvs]
  # write station file data to tmp file/dirs ----
  # will append tmp data
  sf_tmp_ex <- any(sapply(file.path(
    sf_log$chnk_dir,
    sf_log$lg_tbl[tbl_n %chin% tv]$tbl_n
  ), file.exists))
  if (!tmp || !sf_tmp_ex && length(tv) > 0) {
    if (is.null(sfn)) sfn <- names(readRDS(station_file))
    if (!isTRUE(is.null(tv))) sfn <- sfn[sfn %chin% tv]
    if (chunk_v) {
      cli::cli_inform(c(" " = "Chunking data ...",
        " " = "Extracting station file data for rapid read/write:",
        "i" = "Station file: {station_file}", " " = "Extracting to: {sf_tmp}"
      ))
    }
    log_update <- lapply(sfn, function(x) {
      if (chunk_v) cli::cli_inform(c("*" = "table/data: {x}"))
      sfx <- readRDS(station_file)[[x]]
      # chunk data ----
      ds <- NULL
      rit <- "event_based"
      ri <- "discnt"
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
      if (x %ilike% paste0("^dt_*.*_fltr_vals$|phens|data_summary|",
          "f_params|phens_dt|gaps"
        )
      ) {
        ds <- "discnt"
        rit <- "event_based"
      }
      if (!is.null(ds)) {
        ds <- sts_interval_name(ds)
        ri <- ds[["sts_intv"]]
      }
      stlog <- sf_dta_wr(dta_room = file.path(sf_tmp, x),
        dta = sfx, tn = x, rit = rit, ri = ri, overwrite = TRUE,
        chunk_v = chunk_v
      )
      stlog
    })
    log_update <- data.table::rbindlist(
      log_update, fill = TRUE, use.names = TRUE
    )
    log_update$open <- TRUE
    sf_log$lg_tbl <- sf_log$lg_tbl[!tbl_n %in% log_update$tbl_n]
    sf_log$lg_tbl <- rbind(sf_log$lg_tbl, log_update, fill = TRUE)[order(tbl_n)]
    saveRDS(sf_log, file.path(sf_tmp, "sf_log"))
  }
  # summarise sfc ----
  sfc <- file.path(sf_log$chnk_dir, sf_log$lg_tbl$tbl_n)
  names(sfc) <- sf_log$lg_tbl$tbl_n
  return(sfc)
}