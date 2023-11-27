#' @title Appends data tables by phenomena
#' @description This function is for internal use. The function is designed to
#'  append phenomena data and associated phenomena standards efficiently, but
#'  retaining metadata records for each phenomena. Moreover, the function
#'  evaluates missing data and compares new and older data records so avoid
#'  loosing data.
#' @param station_file Path and filename of the station file.
#' @param sf_phen_ds Phenomena summary from the station file.
#' @param ndt Data from the new data file.
#' @param new_phen_ds Phenomena summary for the new data.
#' @param phen_dt Phenomena table of from both the station and new data files
#'  combined. These are combined and phids standarised in
#'  ipayipi::station_append().
#' @param overwrite_sf If `TRUE` then original data is disgarded
#'  in favour of new data. If TRUE both data sets will be evaluated and where
#'  there are NA values, a replacement, if available, will be used to replace
#'  the NA value. Defaults to FALSE.
#' @param tn The name of the phenomena data tables.
#' @param ri The record interval associated with the data sets. As a
#'  standardised string.
#' @keywords append phenomena data, overwrite data, join tables,
#' @author Paul J Gordijn
#' @return A phenomena table that contains the start and end dates of each
#'  phenomena and the respective data source identification numbers of the
#'  data. Also, the appended data.
#' @export
#' @details This function is an internal function called by others in the
#'  pipeline.
append_phen_data <- function(
  station_file = NULL,
  sf_phen_ds = NULL,
  ndt = NULL,
  new_phen_ds = NULL,
  phen_dt = NULL,
  overwrite_sf = FALSE,
  tn = NULL,
  ri = NULL,
  ...) {
  ":=" <- "phid" <- "start_dttm" <- "table_name" <- "date_time" <-
    "end_dttm" <- NULL
  phens <- unique(phen_dt[table_name == tn]$phen_name)

  nd_min <- min(ndt$date_time)
  nd_max <- max(ndt$date_time)
  sfo <- readRDS(station_file)[[tn]][date_time >= nd_min][date_time <= nd_max]
  if (overwrite_sf) {
    sfo <- sfo[date_time > nd_max][date_time < nd_min]
  }
  sfo <- sfo[!duplicated(sfo$date_time)]

  ovlap <- ipayipi::append_phen_overlap_data(
    nd_min = nd_min, nd_max = nd_max, sfo = sfo, ndt = ndt, phen_dt = phen_dt,
    phens = phens, sf_phen_ds = sf_phen_ds, new_phen_ds = new_phen_ds,
    ri = ri, tn = tn, overwrite_sf = overwrite_sf)
  phen_dtos <- ovlap$phen_dtos
  raw_dto <- ovlap$raw_dto
  sfo_max <- ovlap$sfo_max
  sfo_min <- ovlap$sfo_min
  rm(ovlap)

  ### deal with non-overlap data
  sfno1 <- readRDS(station_file)[[tn]][date_time < nd_min]
  sfno2 <- readRDS(station_file)[[tn]][date_time > nd_max]
  if (is.null(sfo_min)) {
    sfo_min <- min(readRDS(station_file)[[tn]]$date_time)
    sfo_max <- max(readRDS(station_file)[[tn]]$date_time)
  }
  ndno1 <- ndt[date_time < sfo_min]
  ndno2 <- ndt[date_time > sfo_max]

  if (nrow(sfno1) < 1 && nrow(sfno2) < 1) {
    sfno_pdt <- sf_phen_ds[0, ]
  } else {
    sfno_dttm_min <- min(rbind(sfno1, sfno2)$date_time)
    sfno_dttm_max <- max(rbind(sfno1, sfno2)$date_time)
    sfno_pdt <- sf_phen_ds
    sfno_pdt$start_dttm <- data.table::fifelse(
      sfno_pdt$start_dttm < sfno_dttm_min, sfno_dttm_min, sfno_pdt$start_dttm)
    sfno_pdt$end_dttm <- data.table::fifelse(
      sfno_pdt$end_dttm > sfno_dttm_max, sfno_dttm_max, sfno_pdt$end_dttm)
  }
  if (nrow(ndno1) < 1 && nrow(ndno2) < 1) {
    ndno_pdt <- new_phen_ds[0, ]
  } else {
    ndno_dttm_min <- min(rbind(ndno1, ndno2)$date_time)
    ndno_dttm_max <- max(rbind(ndno1, ndno2)$date_time)
    ndno_pdt <- new_phen_ds[table_name == tn]
    ndno_pdt$start_dttm <- data.table::fifelse(
      ndno_pdt$start_dttm < ndno_dttm_min, ndno_dttm_min, ndno_pdt$start_dttm)
    ndno_pdt$end_dttm <- data.table::fifelse(
      ndno_pdt$end_dttm > ndno_dttm_max, ndno_dttm_max, ndno_pdt$end_dttm)
  }
  #sfno <- rbind(sfno, ndno, fill = TRUE)
  sfno_pdt <- rbind(sfno_pdt, ndno_pdt, fill = TRUE)

  # join overlap data and non-overlap data
  if (!is.null(phen_dtos)) {
    phen_ds <- rbind(phen_dtos, sfno_pdt, fill = TRUE)
    phen_ds$table_name <- tn
  } else {
    phen_ds <- sfno_pdt
  }
  phen_ds <- phen_ds[!is.na(phid)][order(phid, start_dttm, end_dttm)]
  split_phen_ds <- split.data.frame(phen_ds,
    f = factor(paste(phen_ds$phid)))
  split_phen_ds <- lapply(split_phen_ds, function(z) {
    z <- data.table::data.table(start_dttm = min(z$start_dttm),
      end_dttm = max(z$end_dttm), phid = z$phid[1])
    return(z)
  })
  phen_ds <- data.table::rbindlist(split_phen_ds)[order(phid, start_dttm)][,
    table_name := tn]
  phen_ds <- unique(phen_ds)

  # join data and ensure continuous time series
  data_sets <- list(sfno1, sfno2, ndno1, ndno2)
  data_sets <- lapply(data_sets, function(x) {
    data.table::setcolorder(x, c("id", "date_time",
      names(x)[!names(x) %in% c("id", "date_time")]))
  })
  dtsnd <- ipayipi::dttm_extend_long(data_sets = data_sets, ri = ri)
  data_sets <- list(
    data.table::rbindlist(data_sets, fill = TRUE), raw_dto, dtsnd)
  app_dta <- data.table::rbindlist(data_sets, fill = TRUE)[order(date_time)]
  #message(paste0(min(app_dta$date_time), "--", max(app_dta$date_time)))
  return(list(app_dta = app_dta, phen_ds = phen_ds))
}
