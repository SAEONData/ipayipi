#' @title Chunks data by time period
#' @description Chunks in indexes a list of data sets belonging to the same time-series or table into a specified location. Data must have a 'date_time' column---used for chunking.
#' @param dta_room File path to the directory where the data will be and/or is chunked and indexed.
#' @param dta_sets List of data sets, belonging to the same data series to chunk in the `dta_room`.
#' @param tn Name of the data table to be chunked. The tables data time-series qualities must be consistent with ipayipi's 'continuous', 'event_based', or 'mixed' series qualities.
#' _Time-series properties:
#' Time-series properties are not required. If not provided this will be extracted from data summary information, or via `ipayipi::record_interval_eval()`.
#' @param rit The record interval type: 'continuous', 'mixed', or 'event_based'.
#' @param ri The record interval of the data. A string that will be parsed to `ipayipi::sts_interval_name()`. This is used to evaluate the integrity of data chunks time-series consistency; missing date-time values will be filled in as NA values. This applies to mixed and continuous data streams, not event-based where there is no consistent record interval. If the record interval is not provided the function will attempt to define this. If this fails the function will use a default chunking index of two years.
#' @param chunk_i The chunking interval. Must be a string representing a time period. The string is standardised by `ipayipi::sts_interval_name()`. If no chunk interval is provided the function will estimate an appropriate chunking interval based on the record interval.
#' @param buff_period Time periods represented by a character string used to buffer the min and max date-times around index creation. Should ideally be at least 10 years. If new data extends before or beyond the index dates then data will be rechunked, and a new index generated.
#' @param i_zeros Used to name chunked files; the number of leading zeros to include before a chunk number index.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @keywords Internal
#' @noRd
#' @export
#' @author Paul J Gordijn
#' @details This function is an internal function called by others in the pipeline. Its main funciton is to standardise how data is chunked and maintain time-series data integrity. Chunking is done per a 'chunking index' that seperates data chunks by a date floor and ceiling. The index number serves as the identifier for a particular chunk. In addition to the chunk table index the overall min and max date-time of the data series is provided in an index list.
sf_dta_chunkr <- function(
    dta_room = NULL,
    dta_sets = NULL,
    tn = NULL,
    rit = NULL,
    ri = NULL,
    chunk_i = NULL,
    rechunk = FALSE,
    buff_period = "50 years",
    i_zeros = 5,
    verbose = FALSE,
    xtra_v = FALSE,
    ...) {
  ":=" <- "." <- "%ilike%" <- "chnk_fl" <- "chnk_cl" <- "date_time" <-
    "nchnk_fl" <- "nchnk_cl" <- "d1" <- "d2" <- "dta_new" <- NULL
  ipayipi::msg("sf_dta_chunkr()", xtra_v)
  # open file indx and setup ----
  if (file.exists(file.path(dta_room, "aindxr"))) {
    indx <- readRDS(file.path(dta_room, "aindxr"))
    fs <- basename(list.files(dta_room, "^i_*")[1])
    fs <- gsub("^i_", "", fs)
    spchr <- paste0("%0", nchar(fs), "d")
  } else {
    indx <- NULL
    spchr <- paste0("%0", i_zeros, "d")
  }

  null_indx_tbl <- data.table::data.table(chnk_fl = as.POSIXct(NA), chnk_cl  =
      as.POSIXct(NA), indx = NA_integer_, dta = NA, dta_mn = as.POSIXct(NA),
    dta_mx = as.POSIXct(NA), n = NA_integer_, n_pot = NA_integer_
  )[0]
  # prep data sets eval ----
  if (!is.null(dta_sets)) {
    # generate dta_sets names
    dta_sets[["dta_n"]] <- indx$dta_n
    dn <- data.table::rbindlist(lapply(dta_sets, function(x) x[0]),
      use.names = TRUE, fill = TRUE
    )
    dno <- names(dn)[order(names(dn))]
    dno <- c(dno[dno %ilike% "*id$"], dno[dno %ilike% "date_time"],
      dno[!dno %ilike% "*id$|date_time"]
    )
    dn <- dn[, dno, with = FALSE]
    # verify a date_time column
    # extract na date_time rows
    dta_nas <- data.table::rbindlist(
      lapply(dta_sets, function(x) x[is.na(date_time)]),
      use.names = TRUE, fill = TRUE
    )
    if (!is.null(indx)) {
      dta_nas <- rbind(dta_nas,
        readRDS(file.path(dta_room, paste0("i_", sprintf(spchr, 0)))),
        fill = TRUE
      )
    }
    dta_nas <- unique(rbind(dta_nas, dn, fill = TRUE))
    dir.create(dta_room, recursive = TRUE, showWarnings = FALSE)
    saveRDS(dta_nas, file.path(dta_room, paste0("i_", sprintf(spchr, 0))))
    rm(dta_nas)
    dta_sets <- lapply(dta_sets, function(x) x[!is.na(date_time)])
    dta_sets <- dta_sets[sapply(dta_sets, function(x) nrow(x) != 0)]
    if (length(dta_sets) == 0 && !rechunk) {
      # save null data table
      indx <- list(mn = indx$mn, mx = indx$mx,
        indx_tbl = if (is.null(indx$indx_tbl)) null_indx_tbl else indx$indx_tbl,
        chunk_i = indx$chunk_i, table_name = tn, rit = indx$rit, ri = indx$ri,
        dta_n = dn
      )
      dir.create(dta_room, showWarnings = FALSE)
      saveRDS(indx, file.path(dta_room, "aindxr"))
      return(indx)
    }
    dttm <- data.table::rbindlist(dta_sets, fill = TRUE, use.names = TRUE
    )[["date_time"]]
    dts_min <- min(dttm)
    dts_max <- max(dttm)
    rm(dttm)
    ipayipi::msg("Data sets evaluated ...", xtra_v)
  }

  # get ri ----
  # evaluate dta_sets record interval if:
  #  - there is no indx file and ri is null, or
  #  - there is a indx file and we want to 'rechunk' data
  ci <- ipayipi::sf_dta_chunkr_sub_ri(dta_room = dta_room, indx = indx,
    dta_sets = dta_sets, rit = rit, ri = ri, chunk_i = chunk_i, spchr = spchr,
    rechunk = rechunk, verbose = verbose, buff_period = buff_period,
    xtra_v = xtra_v
  )
  chunk_i <- ci$chunk_i
  rit <- ci$rit
  ri <- ci$ri

  # rechunk data ----
  # function will only rechunk if:
  # rechunk is TRUE; !is.null(indx) & chunk_ != indx$chunk_i
  indx <- ipayipi::sf_dta_chunkr_sub_rechnk(
    dta_room = dta_room, dts_min = dts_min, dts_max = dts_max, indx = indx,
    chunk_i = chunk_i, rechunk = rechunk, buff_period = buff_period, i_zeros =
      i_zeros, verbose = verbose, xtra_v = xtra_v
  )

  # add input data sets to chunk files ----
  # setup
  if (file.exists(file.path(dta_room, "aindxr"))) {
    indx <- readRDS(file.path(dta_room, "aindxr"))
    fs <- basename(list.files(dta_room, "^i_*")[1])
    fs <- gsub("^i_", "", fs)
    spchr <- paste0("%0", nchar(fs), "d")
  }
  if (is.null(dta_sets)) return(indx)

  # make input datasets chunk index
  rtb <- ipayipi::chunkr_sub_it(dta_indx = TRUE, mn = dts_min, mx = dts_max,
    buff_period = 0, chunk_i = chunk_i
  )

  # get or make main chunk index table
  it <- indx$indx_tbl
  if (is.null(it)) {
    it <- ipayipi::chunkr_sub_it(dta_indx = FALSE, mn = dts_min, mx = dts_max,
      buff_period = buff_period, chunk_i = chunk_i
    )
  }
  # if the old index table date_time coverage is limited rename files
  ipayipi::msg("Checking old index table ...", xtra_v)
  imn <- min(c(dts_min, indx$mn))
  imx <- max(c(dts_max, indx$mx))
  c1 <- all(!is.null(indx), nrow(it) > 0)
  if (c1) {
    c2 <- any(min(rtb$nchnk_fl) < min(it$chnk_fl),
      max(rtb$nchnk_cl) > max(it$chnk_cl)
    )
  } else {
    c2 <- FALSE
  }
  if (c2 || nrow(it) == 0) {
    nit <- ipayipi::chunkr_sub_it(dta_indx = FALSE, mn = imn, mx = imx,
      chunk_i = chunk_i, buff_period = buff_period
    )
    if (nrow(it) > 0) {
      names(nit) <- c("nchnk_fl", "nchnk_cl", "nindx", "dta_new", "d1", "d2")
      it <- it[, ":="(d1 = chnk_fl, d2 = chnk_cl)]
      # rename old index files with new indices
      it <- it[nit, on = .(d2 > d1, d1 < d2, d2 <= d2)]
      it <- it[, dta_new := data.table::fifelse(
        imn >= nchnk_fl & imx < nchnk_cl, TRUE, FALSE
      )]
      # rename files dev ----
      # assign new indx tbl 'it' ----
    } else {
      it <- nit
    }
  }
  # chunk data sets
  ipayipi::msg("Chunking data ...", xtra_v)
  wd <- it[dts_min < chnk_cl & dts_max > chnk_fl]
  if (ri %in% "discnt" && is.null(rit)) rit <- "event_based"
  if (is.null(rit)) rit <- "continuous"
  w <- ipayipi::chunkr_sub_wr(dta_room = dta_room, write_tbl = wd, dta_sets =
      dta_sets, i_zeros = i_zeros, ri = ri, rit = rit, overwrite = TRUE,
    verbose = verbose, xtra_v = xtra_v
  )

  it <- it[!indx %in% w$indx]
  it <- rbind(it, w, fill = TRUE)
  it <- it[, names(w), with = FALSE][order(indx)]
  # make indx
  indx <- list(mn = imn, mx = imx, indx_tbl = it,
    chunk_i = chunk_i, table_name = tn, rit = rit, ri = gsub("_", " ", ri),
    dta_n = dn
  )
  # inter-chunk time series check/fill ----
  ipayipi::msg("Checking inter chunk ts integrity ...", xtra_v)
  w <- ipayipi::chunkr_inter_chk(dta_room = dta_room, indx = indx, i_zeros =
      i_zeros, verbose = verbose, xtra_v
  )
  it <- it[!indx %in% w$indx]
  it <- rbind(it, w, fill = TRUE)
  it <- it[,
    c("chnk_fl", "chnk_cl", "indx", "dta", "dta_mn", "dta_mx", "n", "n_pot"),
    with = FALSE
  ][order(indx)]
  indx$indx_tbl <- it
  saveRDS(indx, file.path(dta_room, "aindxr"))
  return(indx)
}