#' @title Purges all data in chunks a time interval
#' @description Accesses a chunks index and data and purges data between a specified min and max date-time, and updates the chunk index and other index metadata.
#' @param dta_room File path to the directory where the data will be and/or is chunked and indexed.
#' @param dta_in Link to the data to be purged. Link created by `sf_dtta_read()`.
#' @param tn The name of the table to be purged.
#' @param mn The `mn` and `mx` parameters set the minimum and maximum date-time bounds within which to purge data. All data within and at (i.e, greater/less \bold{and equal} to) the supplied `mn` and `mx` date-time's will be purged.
#' @param mx See `mn`.
#' @param chlck List parsed as data.table environment to remove part of data depending on stage and step.
#' @param chunk_v Logical. Whether or not to report messages and progress.
#' @keywords Internal function for purging data. Whilst the 'overwrite' capactiy of `ipayipi::chunkr_sub_wr()` is useful for continuous data, this function only overwrites data with matching date-time stamps. Therefore, for discontinuous data in an overwrite, if date-time stamps are different to existing chunk data date-time stamps these will not be overwritten. This function on the other hand removes all data between and at a specified minimum and maximum date, ensuring the removal of data.
#' @noRd
#' @export
#' @author Paul J Gordijn
#' @details This function is an internal function called by others in the pipeline. Its main funciton is to standardise how data is chunked and maintain time-series data integrity. Chunking is done per a 'chunking index' that seperates data chunks by a date floor and ceiling. The index number serves as the identifier for a particular chunk. In addition to the chunk table index the overall min and max date-time of the data series is provided in an index list.
sf_dta_chunkr_purge <- function(
  dta_room = NULL,
  dta_in = NULL,
  tn = NULL,
  mn = NULL,
  mx = NULL,
  chlck = NULL,
  chunk_v = FALSE,
  ...
) {
  ":=" <- "." <- NULL
  "chnk_fl" <- "chnk_cl" <- "se2" <- "se1" <- "c2" <- "c1" <- "indx" <-
    "date_time" <- "n_pot" <- "stage" <- "phen" <- NULL
  ei <- dta_in[[tn]]$eindx
  iindx <- readRDS(file.path(dta_room, "aindxr"))
  i_zeros <- nchar(basename(dta_in[[tn]]$fs[1])) - 2
  spchr <- paste0("%0", i_zeros, "d")
  ei[, ":="(c1 = chnk_fl, c2 = chnk_cl)]
  ij <- data.table::data.table(se1 = c(mn, mx), se2 = c(mn, mx),
    ij = TRUE
  )
  ij <- ij[ei, on = .(se2 < c2, se1 >= c1)]
  ij <- lapply(seq_len(nrow(ij)), function(i) {
    # gen file name
    fn <- file.path(dta_room, paste0("i_", sprintf(spchr, ij[i]$indx)))
    # extract the indx info
    iji <- iindx$indx_tbl[indx == ij[i]$indx]
    # purge file
    fi <- readRDS(fn)
    # select stage and step and filter by max min dates
    fif <- fi[stage %in% stage & step %in% step,
      env = list(stage = chlck$stage, step = chlck$step)
    ][!date_time > mn][!date_time < mx]

    # select all fi except stage and step of interest
    fi <- fi[!stage %in% stage & !step %in% step,
      env = list(stage = chlck$stage, step = chlck$step)
    ]

    # add together
    fi <- rbind(fi, fif)[order(date_time, stage, step, phen)]
    if (nrow(fi) == 0) {
      # if there is no data delete the indx file
      unlink(fn)
      dta_mn <- as.POSIXct(NA_character_)
      dta_mx <- as.POSIXct(NA_character_)
      dta <- FALSE
    } else {
      # else write file
      saveRDS(fi, fn)
      dta_mn <- min(fi$date_time)
      dta_mx <- max(fi$date_time)
      dta <- TRUE
    }
    # update file indx
    n <- if (dta) nrow(fi) else NA_integer_
    iji <- iji[, ":="(dta_mn = dta_mn, dta_mx = dta_mx, dta = dta, n = n,
        n_pot = if (dta) n_pot else n
      )
    ]
    return(iji)
  })
  ij <- data.table::rbindlist(ij)
  iindx$indx_tbl <- iindx$indx_tbl[!indx %in% ij$indx]
  iindx$indx_tbl <- rbind(iindx$indx_tbl, ij)[order(indx)]
  iindx$mn <- min(iindx$indx_tbl$dta_mn, na.rm = TRUE)
  iindx$mx <- max(iindx$indx_tbl$dta_mx, na.rm = TRUE)
  saveRDS(iindx, file.path(dta_room, "aindxr"))
}