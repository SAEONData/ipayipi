#' @title Generates chunking index table templates
#' @description Internal function. Performs checks on intra chunk integrity. Writes missing date-time values to chunks where missing.
#' @param dta_room Directory where data chunks are to be stored.
#' @param indx The indx list from `ipayipi::sf_dta_chunkr()`.
#' @param dta_sets List of data sets to be written as chunks per the write_tbl.
#' @param i_zeros The number of leading zeros used to name files.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @keywords Internal.
#' @export
#' @noRd
#' @author Paul J Gordijn
#' @return Updated chunk index table. This must be used to update a chunked folder's chunk index table.
chunkr_inter_chk <- function(
    dta_room = NULL,
    indx = FALSE,
    dta_sets = NULL,
    i_zeros = NULL,
    verbose = FALSE,
    xtra_v = FALSE,
    ...) {
  ".N" <- "dta" <- NULL
  if (is.null(indx$indx_tbl)) return("no chunk index table")
  if (!indx$rit %in% "event_based" && !is.null(indx$ri)) {
    it <- indx$indx_tbl[dta == TRUE][!.N]
    inter_fill <- lapply(it$indx, function(j) {
      ix <- it[indx %in% j]
      aoffset <- indx$mn - lubridate::round_date(indx$mn, unit = indx$ri)
      s <- seq(from = lubridate::round_date(ix$dta_mx, unit = indx$ri) +
          lubridate::as.duration(aoffset),
        to = lubridate::round_date(ix$chnk_cl, unit = indx$ri)  +
          lubridate::as.duration(aoffset), by = indx$ri
      )
      s <- s[!s %in% c(it[indx %in% j]$chnk_cl, it[indx %in% j]$dta_mx)]
      s <- s[!s > it[indx %in% j]$chnk_cl]
      s <- data.table::data.table(date_time = s)
      return(list(wd = ix, s = s))
    })
    if (length(inter_fill) > 0) {
      inter_fill <- inter_fill[
        sapply(inter_fill, function(x) nrow(x[["s"]]) > 0)
      ]
    }
  } else {
    inter_fill <- list()
  }
  wd <- data.table::rbindlist(lapply(inter_fill, function(x) x$wd))
  w <- ipayipi::chunkr_sub_wr(dta_room = dta_room, write_tbl = wd, dta_sets =
      lapply(inter_fill, function(x) x$s), ri = indx$ri, rit = indx$rit,
    i_zeros = i_zeros, overwrite = FALSE, xtra_v = xtra_v
  )
  return(w)
}
