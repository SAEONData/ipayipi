#' @title Rechunks data
#' @description If the chunk interval of data needs to be changed. This is the function that handles it. Date are chunked
#' @param dta_room File path to the directory where the data will be and/or is chunked and indexed.
#' @param dta_sets List of data sets, belonging to the same data series to chunk in the `dta_room`.
#' @param tn Name of the data table to be chunked. The tables data time-series qualities must be consistent with ipayipi's 'continuous', 'event_based', or 'mixed' series qualities.
#' @param rit The record interval type: 'continuous', 'mixed', or 'event_based'.
#' @param ri The record interval of the data. A string that will be parsed to `ipayipi::sts_interval_name()`. This is used to evaluate the integrity of data chunks time-series consistency; missing date-time values will be filled in as NA values. This applies to mixed and continuous data streams, not event-based where there is no consistent record interval. If the record interval is not provided the function will attempt to define this. If this fails the function will use a default chunking index of two years.
#' @param chunk_i The chunking interval. Must be a string representing a time period. The string is standardised by `ipayipi::sts_interval_name()`. If no chunk interval is provided the function will estimate an appropriate chunking interval based on the record interval.
#' @param i_zeros Used to name chunked files; the number of leading zeros to include before a chunk number index.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @keywords Internal
#' @noRd
#' @export
#' @author Paul J Gordijn
sf_dta_chunkr_sub_rechnk <- function(
    dta_room = NULL,
    dts_min = NULL,
    dts_max = NULL,
    indx = NULL,
    chunk_i = NULL,
    rechunk = FALSE,
    buff_period = "50 years",
    i_zeros = 5,
    verbose = TRUE,
    xtra_v = FALSE,
    ...) {
  "chnk_fl" <- "chnk_cl" <- "dta" <- NULL
  ipayipi::msg("Rechuning: sf_dta_chunkr_sub_rechnk()", xtra_v)
  # rechunk data ----
  if (rechunk && !is.null(indx) && chunk_i != indx$chunk_i) {
    imn <- min(c(dts_min, indx$mn))
    imx <- max(c(dts_max, indx$mx))
    it <- ipayipi::chunkr_sub_it(dta_indx = FALSE, mn = imn, mx = imx,
      buff_period = buff_period, chunk_i = chunk_i
    )
    wd <- it[imn < chnk_cl & imx > chnk_fl]
    rechunk_room <- tempfile(pattern = "rechunk")
    dir.create(rechunk_room, showWarnings = FALSE, recursive = TRUE)
    nindx <- list(mn = imn, mx = imx, indx_tbl = it, chunk_i = chunk_i,
      table_name = indx$table_name, rit = indx$rit, ri = indx$ri,
      dta_n = indx$dta_n
    )
    saveRDS(nindx, file.path(rechunk_room, "aindxr"))
    di <- indx$indx_tbl[dta == TRUE]
    lapply(di$indx, function(x) {
      fs <- basename(list.files(dta_room, "^i_*")[1])
      fs <- gsub("^i_", "", fs)
      spchr <- paste0("%0", nchar(fs), "d")
      d <- list(readRDS(
        file.path(dta_room, paste0("i_", sprintf(spchr, x)))
      ))
      w <- ipayipi::chunkr_sub_wr(dta_room = rechunk_room, write_tbl = wd,
        dta_sets = d, i_zeros = i_zeros, rit = indx$rit, ri = indx$ri,
        overwrite = TRUE, xtra_v = xtra_v
      )
      itx <- readRDS(file.path(rechunk_room, "aindxr"))
      itx$indx_tbl <- itx$indx_tbl[!indx %in% w$indx]
      itx$indx_tbl <- rbind(itx$indx_tbl, w, fill = TRUE)
      itx$indx_tbl <- itx$indx_tbl[, names(w), with = FALSE][
        order(itx$indx_tbl)
      ]
      saveRDS(itx, file.path(rechunk_room, "aindxr"))
    })
    indx <- readRDS(file.path(rechunk_room, "aindxr"))
    fs <- basename(list.files(rechunk_room, "^i_*")[1])
    fs <- gsub("^i_", "", fs)
    fmf <- paste0("%0", nchar(fs), "d")
    fs <- basename(list.files(dta_room, "^i_*")[1])
    fs <- gsub("^i_", "", fs)
    spchr <- paste0("%0", nchar(fs), "d")
    file.rename(from = file.path(dta_room, paste0("i_", sprintf(spchr, 0))),
      to = file.path(dta_room, paste0("i_", sprintf(fmf, 0)))
    )
    # copy rechunked dir to old data room
    rfs <- list.files(rechunk_room, full.names = TRUE)
    fs <- list.files(dta_room, full.names = TRUE)
    fs <- fs[!fs %in% file.path(dta_room, paste0("i_", sprintf(fmf, 0)))]
    unlink(fs, recursive = TRUE)
    file.copy(from = rfs, to = dta_room, overwrite = TRUE)
    unlink(rfs, recursive = TRUE)
  }
  return(indx)
}