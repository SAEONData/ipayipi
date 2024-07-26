#' @title  'dt' processing pipeline: harvest data
#' @description Used to extract data from other table/s, stations, or source/s.
#' @param station_file File path of the station being processed.
#' @param hsf_table The path of the directory in which to search for the external data and/or data table.
#' @param ppsij The desirsed time_interval associated with the extracted data. If this is `NULL` then the table is extracted as is.
#' @param f_params Function parameters parsed by `ipayipi::dt_process()`.
#' @param sfc Station file connection; see `ipayipi::open_sf_con()`.
#' @author Paul J. Gordijn
#' details
#' @export
dt_harvest <- function(
    station_file = NULL,
    f_params = NULL,
    ppsij = NULL,
    sfc = NULL,
    verbose = FALSE,
    xtra_v = FALSE,
    cores = cores,
    ...) {
  "%ilike%" <- NULL
  # harvest data from tables
  if (station_file == unique(f_params$hsf_station)[1]) {
    hsfc <- sfc
  } else {
    hsfc <- attempt::try_catch(
      expr = ipayipi::open_sf_con(
        station_file = unique(f_params$hsf_station)[1], verbose = verbose,
        xtra_v = xtra_v, cores = cores
      ), .w = ~stop
    )
  }
  ppsid <- paste0(ppsij$dt_n, "_", ppsij$dtp_n)
  hsf_params <- f_params[ppsid %in% ppsid]
  hsf_names <- unique(hsf_params$hsf_table)
  # extract and save dataset or dt/raw chunked data index
  # need to standardised how data is returned by this harvest function
  lapply(hsf_names, function(x) {
    h <- ipayipi::sf_dta_read(sfc = hsfc, tv = x, tmp = TRUE,
      start_dttm = NULL, end_dttm = NULL, return_dta = FALSE
    )
    if (is.null(h[[x]]$indx) && data.table::is.data.table(h[[x]])) {
      h <- list(list(dta = h[[x]]))
      names(h) <- x
      n <- c(names(h[[x]]$dta)[names(h[[x]]$dta) %in% hsf_params$phen_name])
      n <- c(n[n %in% "date_time"], n[!n %in% "date_time"])
      n <- c(n[n %ilike% "*id$"], n[!n %ilike% "*id$"])
      h[[x]]$dta <- h[[x]]$dta[, n, with = FALSE]
    }
    if ("ipip-sf_chnk" %in% class(h[[x]])) {
      n1 <- names(h[[x]]$indx$dta_n)
      n <- c(n1[n1 %ilike% "*id$|date_time"], n1[n1 %in% hsf_params$phen_name
        ]
      )
      h[[x]]$hsf_phens <- n
    }
    # save the data/index
    saveRDS(h[[x]], file.path(dirname(sfc)[1], paste0(ppsid, "_hsf_table_", x)))
    return(x)
  })
  return(list(hsf_dts = hsf_names, ppsij = ppsij))
}