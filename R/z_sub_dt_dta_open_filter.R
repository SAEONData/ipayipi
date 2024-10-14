#' @title dt: Opens data table
#' @description Internal function that filters data by start and end date-times provided by the processing pipeline ppsij.
#' @param dta_link Link from `ipayipi::sf_dta_read()`.
#' @param ppsij Pipeline processing table step info.
#' @author Paul J. Gordijn
#' @return Filtered data link.
#' @keywords Internal
#' @export
#' @noRd
dt_dta_filter <- function(
  dta_link = NULL,
  ppsij = NULL,
  ...
) {
  "chnk_fl" <- "dta" <- NULL
  if ("ipip-sf_chnk" %in% class(dta_link[[1]])) {
    # eindx filter
    fchk <- all(!sapply(c(ppsij$start_dttm[1], ppsij$end_dttm[1]), is.na),
      !is.null(dta_link[[1]]$eindx)
    )
    if (fchk) {
      if (ppsij$end_dttm[1] < dta_link[[1]]$indx$mx) {
        dta_link[[1]]$eindx <- dta_link[[1]]$eindx[
          chnk_fl >= ppsij$end_dttm[1]
        ]
      }
      fzeros <- nchar(basename(dta_link[[1]]$fs[1])) - 2
      dirn <- dirname(dta_link[[1]]$fs[1])
      dta_link[[1]]$fs <- paste0("i_", sprintf(
        paste0("%0", fzeros, "d"), c(0, dta_link[[1]]$eindx[dta == TRUE]$indx)
      ))
      dta_link[[1]]$fs <- file.path(dirn, dta_link[[1]]$fs)
    }
    dta_link[[1]] <- dta_link
  }
  return(dta_link)
}