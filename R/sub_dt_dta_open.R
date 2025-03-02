#' @title dt: Opens data table
#' @description Internal function that handles the variety of ways data are harvested; returning data from either chunked or unchunked sources.
#' @param dta_link Link from `ipayipi::sf_dta_read()`.
#' @param start_dttm Earliest data filter date-time.
#' @param end_dttm Latest data filter date-time.
#' @author Paul J. Gordijn
#' @return Data in date.table format.
#' @export
#' @noRd
#' @keywords Internal
dt_dta_open <- function(
  dta_link = NULL,
  start_dttm = NULL,
  end_dttm = NULL,
  ...
) {
  "%chin%" <- NULL
  "date_time" <- NULL
  dta <- NULL
  if (length(dta_link) == 0) dta <- NULL
  if ("ipip-sf_chnk" %in% class(dta_link[[1]][[1]])) dta_link <- dta_link[[1]]
  if ("ipip-sf_chnk" %in% class(dta_link[[1]])) {
    # funny start and end names in case these are reserved names in working data
    sdtmx_xz_r <- start_dttm
    edtmx_xz_r <- end_dttm
    xn <- names(dta_link)[1]
    if (!is.null(dta_link[[xn]]$fs)) {
      if (is.null(sdtmx_xz_r)) sdtmx_xz_r <- dta_link[[xn]]$indx$mn
      if (is.null(edtmx_xz_r)) edtmx_xz_r <- dta_link[[xn]]$indx$mx
      if (is.null(dta_link[[xn]]$hsf_phens)) {
        dta_phens <- names(dta_link[[xn]]$indx$dta_n)
      } else {
        dta_phens <- dta_link[[xn]]$hsf_phens[
          dta_link[[xn]]$hsf_phens %in% names(dta_link[[xn]]$indx$dta_n)
        ]
      }
      dta_phens <- unique(dta_phens)

      # bind together data chunks
      dta_link[[xn]]$dta <- lapply(dta_link[[xn]]$fs, function(x) {
        dx <- readRDS(x)
        if ("date_time" %in% names(dx)) {
          dx <- dx[date_time >= sdtmx_xz_r] #[date_time <= end_dttm]
        }
        dx <- dx[, names(dx)[names(dx) %in% c("date_time", dta_phens)],
          with = FALSE
        ]
        return(dx)
      })
      ri <- dta_link[[xn]]$indx$ri
      # if gaps in chunks then join into continuous series
      if (any(!isTRUE(dta_link[[xn]]$eindx$dta)) &&
          isTRUE(!ri %chin% "discnt")
      ) {
        dta_link[[xn]]$dta <- dttm_extend_long(data_sets = dta_link[[xn]]$dta,
          ri = ri, intra_check = FALSE
        )
      } else {
        dta_link[[xn]]$dta <- data.table::rbindlist(
          dta_link[[xn]]$dta, fill = TRUE, use.names = TRUE
        )
      }
    }
    if (is.null(dta_link[[xn]]$dta) && !is.null(dta_link[[xn]]$indx$dta_n)) {
      dta_link[[xn]]$dta <- dta_link[[xn]]$indx$dta_n
      if ("date_time" %in% names(dta_link[[xn]]$dta)) {
        dta_link[[xn]]$dta <- dta_link[[xn]]$dta[
          date_time >= sdtmx_xz_r
        ][date_time <= edtmx_xz_r]
      }
    }
    dta <- dta_link[[xn]]$dta
  }

  # if the data is not chunked
  if ("ipip-sf_rds" %chin% class(dta_link[[1]][[1]])) {
    dta_link[[1]] <- dta_link[[1]][[1]]
  }
  if (all(
    "ipip-sf_rds" %chin% class(dta_link[[1]]),
    !is.null(dta_link[[1]]$dta)
  )) {
    dta <- dta_link[[1]]$dta
  }
  if (all(
    "ipip-sf_rds" %chin% class(dta_link[[1]]),
    is.null(dta_link$dta),
    data.table::is.data.table(dta_link[[1]])
  )) {
    dta <- dta_link[[1]]
  }
  return(dta)
}