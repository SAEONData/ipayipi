#' @title Writes data to chunk files
#' @description Internal function. Checks integrity of continuous data whilst writing data chunks. Does not check between chunks (inter chunk) integrity, only intra chunk integrity.
#' @param dta_room Directory where data chunks are housed.
#' @param write_tbl The segment of the index table used to write data chunks. Contains date-time floor and ceiling for filtering data chunks.
#' @param dta_sets List of data sets to be written as chunks per the write_tbl.
#' @param i_zeros The number of leading zeros used to name files.
#' @param ri Not implemented.
#' @param rit Not yet implemented.
#' @param overwrite Must date time records with the same chunk index be overwritten by matching date-time records in the input data sets? Logical: overwrites if `TRUE`.
#' @param verbose Whether to report back on function progress. Useful for debugging.
#' @keywords Internal
#' @noRd
#' @export
#' @author Paul J Gordijn
#' @return The `write_tbl` with updated minimum and maximum date times for each row/chunk index.
chunkr_sub_wr <- function(
  dta_room = NULL,
  write_tbl = FALSE,
  dta_sets = NULL,
  i_zeros = 5,
  ri = NULL,
  rit = NULL,
  overwrite = TRUE,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  "%ilike%" <- "date_time" <- "indx" <- NULL
  ipayipi::msg("chunkr_sub_wr()", xtra_v)
  dir.create(dta_room, showWarnings = FALSE, recursive = TRUE)
  # temp dir for chunking data before writing to the dta_room
  tmp_cdir <- tempfile(pattern = "cwrite")
  dir.create(tmp_cdir, recursive = TRUE)
  fmf <- paste0("%0", i_zeros, "d")
  ri <- gsub("_", " ", ri)
  # writes data in chunks to a temp folder
  # this enables parallel reading and writing to the dta_room
  lapply(dta_sets, function(dta) {
    lapply(write_tbl$indx, function(x) {
      wi <- write_tbl[indx %in% x]
      if (nrow(wi) != 1) warning("Corrupt chunk index file!")
      d <- dta[date_time >= min(wi$chnk_fl)][date_time < max(wi$chnk_cl)]
      tmp_cdiri <- file.path(tmp_cdir, basename(tempfile()))
      dir.create(tmp_cdiri)
      saveRDS(d, file.path(tmp_cdiri, paste0("i_", sprintf(fmf, x))))
    })
  })
  # write chunked data to directory
  wri <- lapply(write_tbl$indx, function(x) {
    wi <- write_tbl[indx %in% x]
    if (nrow(wi) != 1) warning("Corrupt chunk index file!")
    # read + merge all files with same chunk index
    fs <- list.files(path = tmp_cdir, pattern = paste0("i_", sprintf(fmf, x)),
      recursive = TRUE, full.names = TRUE
    )
    d <- data.table::rbindlist(lapply(fs, readRDS),
      fill = TRUE, use.names = TRUE
    )
    fs <- list.files(path = dta_room, pattern = paste0("i_", sprintf(fmf, x)),
      recursive = TRUE, full.names = TRUE
    )
    if (length(fs) > 0) {
      de <- data.table::rbindlist(lapply(fs, readRDS),
        fill = TRUE, use.names = TRUE
      )
      if (overwrite) {
        de <- de[!date_time %in% d$date_time]
      } else {
        d <- d[!date_time %in% de$date_time]
      }
      d <- rbind(d, de, fill = TRUE)
    }
    d <- unique(d, by = "date_time")
    n <- nrow(d)
    if (!n > 0) return(NULL)
    wi$dta <- TRUE
    wi$dta_mn <- min(d$date_time)
    wi$dta_mx <- max(d$date_time)
    if (!rit %in% "event_based" && !is.null(ri)) {
      aoffset <- wi$dta_mn - lubridate::round_date(wi$dta_mn, unit = ri)
      dtsq <- data.table::data.table(
        date_time = seq(
          lubridate::round_date(wi$dta_mn, unit = ri) +
            lubridate::as.duration(aoffset),
          lubridate::round_date(wi$dta_mx, unit = ri) +
            lubridate::as.duration(aoffset), by = ri
        )
      )
      dsq <- dtsq[!date_time %in% d$date_time]
      d <- rbind(d, dsq, fill = TRUE)
      if (rit %in% "continuous" && n != nrow(dtsq)) {
        ipayipi::msg(paste0(
          " Missing chunk data --- time-series integrity questioned!"
        ), verbose)
        ipayipi::msg(paste0(
          " Results from gaps in continuous data ... filled with NA\'s"
        ), verbose)
        ipayipi::msg(dta_room, xtra_v)
      }
      wi$n_pot <- nrow(dtsq)
    } else {
      wi$n_pot <- n
    }
    d <- d[order(date_time)]
    wi$n <- nrow(d)
    saveRDS(d, file.path(dta_room, paste0("i_", sprintf(fmf, x))))
    return(wi)
  })
  wri <- data.table::rbindlist(wri, fill = TRUE, use.names = TRUE)
  wri <- wri[, names(wri)[!names(wri) %ilike% "2$|1$"], with = FALSE]
  # clean up temporary directory
  unlink(tmp_cdir, recursive = TRUE)
  return(wri)
}
