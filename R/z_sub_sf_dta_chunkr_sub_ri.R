#' @title Chunks data by time period
#' @description Chunks in indexes a list of data sets belonging to the same time-series or table into a specified location. Data must have a 'date_time' column---used for chunking.
#' @param dta_room File path to the directory where the data will be and/or is chunked and indexed.
#' @param dta_sets List of data sets, belonging to the same data series to chunk in the `dta_room`.
#' @param rit The record interval type: 'continuous', 'mixed', or 'event_based'.
#' @param ri The record interval of the data. A string that will be parsed to `ipayipi::sts_interval_name()`. This is used to evaluate the integrity of data chunks time-series consistency; missing date-time values will be filled in as NA values. This applies to mixed and continuous data streams, not event-based where there is no consistent record interval. If the record interval is not provided the function will attempt to define this. If this fails the function will use a default chunking index of two years.
#' @param chunk_i The chunking interval. Must be a string representing a time period. The string is standardised by `ipayipi::sts_interval_name()`. If no chunk interval is provided the function will estimate an appropriate chunking interval based on the record interval.
#' @param i_zeros Used to name chunked files; the number of leading zeros to include before a chunk number index.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @keywords Internal
#' @export
#' @noRd
#' @author Paul J Gordijn
#' @details This function is an internal function called by others in the pipeline. Its main funciton is to standardise how data is chunked and maintain time-series data integrity. Chunking is done per a 'chunking index' that seperates data chunks by a date floor and ceiling. The index number serves as the identifier for a particular chunk. In addition to the chunk table index the overall min and max date-time of the data series is provided in an index list.
sf_dta_chunkr_sub_ri <- function(
    dta_room = NULL,
    indx = NULL,
    dta_sets = NULL,
    rit = NULL,
    ri = NULL,
    chunk_i = NULL,
    spchr = NULL,
    rechunk = FALSE,
    verbose = TRUE,
    xtra_v = FALSE,
    ...) {
  "cumn" <- "dta" <- NULL
  ipayipi::msg("Evaluating data record interval", xtra_v)
  if (all(
    !is.null(dta_sets),
    is.null(indx) && is.null(ri) || !is.null(indx) && rechunk
  )) {
    s <- sapply(cumsum(sapply(dta_sets, function(x) nrow(x))), function(x) {
      x <= 1000
    })
    if (length(s) == 1) s  <- TRUE
    dt <- data.table::rbindlist(dta_sets[s], fill = TRUE, use.names = TRUE
    )[["date_time"]]
    if (length(dt) < 2) rie  <- list(
      record_interval_type = "event_based", record_interval = "discnt"
    )
    rie <- ipayipi::record_interval_eval(dt)[
      c("record_interval_type", "record_interval")
    ]
    dts_ri <- rie$record_interval
    dts_rit <- rie$record_interval_type
    ddtlength <- length(dt)
  }

  # evaluate extant data to estimate ri
  if (all(!is.null(indx), rechunk)) {
    ix <- indx$indx_tbl[dta == TRUE]
    ix$cumn <- cumsum(ix$n)
    ix <- ix[cumn <= 1000]
    if (nrow(ix) == 0) ix <- indx$indx_tbl[dta == TRUE][1]
    dt <- data.table::rbindlist(
      lapply(
        file.path(dta_room, paste0("i_", sprintf(spchr, ix$indx))), readRDS
      ), fill = TRUE, use.names = TRUE
    )[["date_time"]]
    if (length(dt) > 2) {
      rie <- ipayipi::record_interval_eval(dt)
      erit <- rie$record_interval_type
      eri <- gsub("_", " ", rie$record_interval)
    } else {
      erit <- "event_based"
      eri <- "discnt"
    }
    edtlength <- length(dt)
  }
  if (all(exists("edtlength"), exists("ddtlength"))) {
    # get record interval eval from dta_sets or extant data
    # extract where dt rows were greatest
    v <- c(edtlength, ddtlength)
    l <- sapply(v, function(x) x == max(v))
    names(l) <- seq_along(l)
    l <- l[sapply(l, function(x) x == TRUE)][1]
    ri <- gsub("_", " ", c(dts_ri, eri)[as.integer(names(l))])
    rit <- c(dts_rit, erit)[as.integer(names(l))]
  }
  # get chunk interval
  # standards for chunking interval based on ri
  if (!is.null(ri) && !ri %in% "discnt") {
    ds <- ipayipi::sts_interval_name(ri)
    if (ds[["dfft_units"]] %in% "years") chunk_ii <- "10 years"
    if (ds[["dfft_units"]] %in% "months") chunk_ii <- "10 years"
    if (ds[["dfft_units"]] %in% "weeks") chunk_ii <- "10 years"
    if (ds[["dfft_units"]] %in% "days") chunk_ii <- "10 years"
    if (ds[["dfft_units"]] %in% "hours") chunk_ii <- "2 months"
    if (ds[["dfft_units"]] %in% "mins") chunk_ii <- "1 months"
    if (ds[["dfft_units"]] %in% "secs") chunk_ii <- "1 weeks"
  } else {
    ri <- "discnt"
  }

  if (!is.null(indx) && !rechunk) {
    chunk_i <- attempt::try_catch(
      expr = ipayipi::sts_interval_name(indx$chunk_i)[["sts_intv"]],
      .w = ~"2 years", .e = ~"2 years"
    )
  } else if (exists("chunk_ii") && !rechunk) {
    chunk_i <- chunk_ii
  }
  if (rechunk && exists("chunk_ii")) {
    if (all(
      ipayipi::sts_interval_name(chunk_ii)[["dfft_secs"]] >
        ipayipi::sts_interval_name(ri)[["dfft_secs"]],
      is.null(chunk_i)
    )) {
      chunk_i <- chunk_ii
    }
  }
  if (is.null(chunk_i)) chunk_i <- "2 years"

  return(list(chunk_i = chunk_i, rit = rit, ri = ri))
}