#' @title Writes data to file
#' @description Chunks data if the data.table has a 'date_time' column. Data without a 'date_time' column are saved in RDS format.
#' @param dta_room File path where the data will be saved and/or is chunked and indexed.
#' @param dta Data to be saved to the `dta_room`. Must be a 'data.table' object. Not a list.
#' @param tn Name of the data table to be saved. The tables data time-series qualities must be consistent with ipayipi's 'continuous', 'event_based', or 'mixed' series qualities.
#' _Time-series properties:
#' Time-series properties are not required. If not provided this will be extracted from data summary information, or via `ipayipi::record_interval_eval()`.
#' @param rit The record interval type: 'continuous', 'mixed', or 'event_based'.
#' @param ri The record interval of the data. A string that will be parsed to `ipayipi::sts_interval_name()`. This is used to evaluate the integrity of data chunks time-series consistency; missing date-time values will be filled in as NA values. This applies to mixed and continuous data streams, not event-based where there is no consistent record interval. If the record interval is not provided the function will attempt to define this. If this fails the function will use a default chunking index of two years.
#' @param chunk_i The chunking interval. Must be a string representing a time period. The string is standardised by `ipayipi::sts_interval_name()`. If no chunk interval is provided the function will estimate an appropriate chunking interval based on the record interval.
#' @param rechunk Logical. If TRUE chunked data will be 'rechunked' if necessary. Rechunking is done when `chunk_i` is forced to change.
#' @param buff_period See `ipayipi::sf_dta_chunkr()`.
#' @param i_zeros Used to name chunked files; the number of leading zeros to include before a chunk number index.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @keywords Internal
#' @noRd
#' @export
#' @author Paul J Gordijn
#' @return Logical indicating whether data has been saved.
#' @details This function is an internal function called by others in the pipeline. Its main funciton is to standardise how data is chunked and maintain time-series data integrity. Chunking is done per a 'chunking index' that seperates data chunks by a date floor and ceiling. The index number serves as the identifier for a particular chunk. In addition to the chunk table index the overall min and max date-time of the data series is provided in an index list.
sf_dta_wr <- function(
    dta_room = NULL,
    dta = NULL,
    tn = NULL,
    sfc = NULL,
    overwrite = TRUE,
    rit = NULL,
    ri = NULL,
    chunk_i = NULL,
    rechunk = FALSE,
    buff_period = "50 years",
    i_zeros = 5,
    verbose = TRUE,
    xtra_v = FALSE,
    ...) {
  "%ilike%" <- "table_name" <- NULL

  ipayipi::msg("sf_dta_wr()", xtra_v)
  # check args ----
  # required: tn, dta, dta_room
  if (is.null(dta) || "function" %in% class(dta)) return(TRUE)
  a_args <- list(dta_room = dta_room, dta = dta, tn = tn)
  a_args <- a_args[sapply(a_args, is.null)]
  if (length(a_args) > 0) {
    msg <- paste0("Null args provided to sf_dta_wr(): ",
      paste(names(a_args), collapse = ", "), "."
    )
    ipayipi::msg(msg, verbose)
  }
  if ("f_params" %in% class(dta)) {
    saveRDS(dta, dta_room)
    return(TRUE)
  }
  if (!data.table::is.data.table(dta)) {
    msg <- paste0("sf_dta_wr() requires data.table input dta")
    ipayipi::msg(msg, verbose)
  }

  # organise data columns ----
  dno <- names(dta)
  special_tbls <- c("data_summary", "phen_data_summary", "phens", "gaps",
    "pipe_seq"
  )
  if (!tn %in% special_tbls) {
    dno <- dno[order(dno)]
    dno <- c(dno[dno %ilike% "*id$"], dno[dno %ilike% "date_time"],
      dno[!dno %ilike% "*id$|date_time"]
    )
  }
  dta <- dta[, dno, with = FALSE]

  # write data as ----
  s <- FALSE
  if (all(
    data.table::is.data.table(dta),
    "date_time" %in% names(dta)
  )) {
    # chunks ----
    # attempt to get record interval for raw and 'dt' tables
    ds <- NULL
    if (tn %ilike% "^raw_*" && !is.null(sfc) && !is.null(ri)) {
      ds <- readRDS(sfc["data_summary"])
      rit <- ds[table_name %in% tn]$record_interval_type[1]
      ds <- ds[table_name %in% tn]$record_interval[1]
    }
    if (tn %ilike% "^dt_*" && !is.null(sfc) && is.null(ri)) {
      ds <- readRDS(sfc["phens_dt"])
      rit <- ds[table_name %in% tn]$record_interval_type[1]
      ds <- ds[table_name %in% tn]$dt_record_interval[1]
    }
    if (!is.null(ds) && !is.null(sfc) && !is.null(ri)) {
      ds <- ipayipi::sts_interval_name(ds)
      ri <- ds[["sts_intv"]]
    }
    # chunk data into file
    ipayipi::sf_dta_chunkr(dta_room = dta_room, chunk_i = NULL,
      rechunk = FALSE, i_zeros = i_zeros, dta_sets = list(dta), tn = tn,
      rit = rit, ri = ri, overwrite = overwrite, verbose =
        verbose, xtra_v = xtra_v
    )
    m <- paste0(tn, ": Data chunked")
    s <- TRUE
  } else {
    # single RDS ----
    d <- list(attempt::try_catch(expr = readRDS(dta_room),
      .e = ~dta[0],
      .w = ~dta[0]
    ))
    names(d) <- tn
    dta <- list(dta)
    names(dta) <- tn
    dta <- ipayipi::append_tables(original_tbl = d, new_tbl = dta,
      overwrite_old = overwrite
    )[[tn]]
    # speccial operation for logg_interfere tables
    if ("logg_interfere" %in% tn) {
      dta <- unique(dta, by = c("date_time", "logg_interfere_type"))
      dta$id <- seq_len(nrow(dta))
    }
    saveRDS(dta, dta_room)
    m <- paste0(tn, ": Data saved as single RDS.")
    s <- TRUE
  }
  if (s) ipayipi::msg(m, xtra_v)
  return(s)
}