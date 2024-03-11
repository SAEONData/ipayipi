#' @title  'dt' processing pipeline: harvest data
#' @description Used to extract data from other table/s, stations, or source/s.
#' @param station_file File path of the station being processed.
#' @param hsf_table The path of the directory in which to search for the external data and/or data table.
#' @param time_interval The desirsed time_tinterval associated with the
#'  extracted data. If this is `NULL` then the table is extracted as is.
#' @param f_params A vector of phenomena name to be extracted from the harvested data table. If NULL all column names are extracted.
#' @author Paul J. Gordijn
#' details
#' @export
dt_harvest <- function(
  station_file = NULL,
  f_params = NULL,
  time_interval = NULL,
  ppsij = NULL,
  sfc = NULL,
  ...) {
  "hsf_table" <- NULL
  # harvest data from tables
  if (station_file == unique(f_params$hsf_station)[1]) {
    hsfc <- sfc
  } else {
    hsfc <- attempt::try_catch(
      expr = ipayipi::open_sf_con(
        station_file = unique(f_params$hsf_station)[1]
      ), .w = ~stop)
  }
  ppsid <- paste0(ppsij$dt_n, "_", ppsij$dtp_n)
  hsf_params <- f_params[ppsid %in% ppsid]
  hsf_names <- unique(hsf_params$hsf_table)
  lapply(hsf_names, function(x) {
    h <- ipayipi::sf_read(sfc = hsfc, tv = x, tmp = TRUE)
    hn <- names(h)[1]
    n <- c(names(h[[x]])[names(h[[x]]) %in% hsf_params$phen_name])
    if ("date_time" %in% names(h[[x]])) {
      n <- c("date_time", n[!n %in% "date_time"])
    }
    if (any(names(h[[x]]) %ilike% "*id$")) {
      id <- names(h[[x]])[names(h[[x]]) %ilike% "*id$"]
      n <- c(id, n)
      n <- n[!duplicated(n, fromLast = FALSE)]
    }
    h[[x]] <- h[[x]][, n, with = FALSE]
    # save the data
    saveRDS(h[[x]], file.path(dirname(sfc)[1], paste0(ppsid, "_hsf_table_", x)))
    return(hn)
  })
  return(list(hsf_dts = hsf_names))
}