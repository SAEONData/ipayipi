#' @title Appends data table phenomena by phenomena
#' @description This function is for internal use. The function is designed to append phenomena data and associated phenomena standards efficiently, but retaining phenomena associated metadata records. Moreover, the function evaluates missing data and compares new and older data records so avoid   loosing data.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param station_file Path and filename of the station file or station file object.
#' @param sf_phen_ds Optional argument. Phenomena summary from the station file (developed by the data pipeline).
#' @param ndt Data from the new-data file.
#' @param new_phen_ds Phenomena summary for the new data.
#' @param phen_id Set this to TRUE for generating updated station file phenomena data summaries. When set to FALSE, a new phenomena data summary will not be executed. Set to FALSE if no phenomena data summaries are available.
#' @param phen_dt Phenomena table of from both the station and new data files combined. These are combined and phids standarised in ipayipi::station_append().
#' @param overwrite_sf If `TRUE` then original data is disgarded in favour of new data. If TRUE both data sets will be evaluated and where there are NA values, a replacement, if available, will be used to replace the NA value. Defaults to FALSE.
#' @param tn The name of the phenomena data tables.
#' @param ri The record interval associated with the data sets. String standardised using `ipayipi::sts_interval_name()`.
#' @param rit Record interval type. One of the following options for time-series data: 'continuous', 'event_based', or 'mixed'.
#' @keywords append phenomena data, overwrite data, join tables,
#' @author Paul J Gordijn
#' @return A phenomena table that contains the start and end dates of each
#'  phenomena and the respective data source identification numbers of the
#'  data. Also, the appended data.
#' @export
#' @details This function is an internal function called by others in the
#'  pipeline.
append_phen_data <- function(
  pipe_house = NULL,
  station_file = NULL,
  sf_phen_ds = NULL,
  ndt = NULL,
  new_phen_ds = NULL,
  phen_id = FALSE,
  phen_dt = NULL,
  overwrite_sf = FALSE,
  tn = NULL,
  ri = NULL,
  rit = NULL,
  verbose = verbose,
  xtra_v = xtra_v,
  ...
) {
  ":=" <- "phid" <- "start_dttm" <- "table_name" <- "date_time" <-
    "end_dttm" <- NULL
  # prep data tables date-time for chunk extraction
  nd_min <- min(ndt$date_time)
  nd_max <- max(ndt$date_time)
  # get station/main data table ----
  if (is.character(station_file)) {
    sfc <- ipayipi::open_sf_con(pipe_house = pipe_house, station_file =
        station_file, verbose = verbose, xtra_v = xtra_v
    )
    # read_dta
    sf_eindx <- ipayipi::sf_dta_read(sfc = sfc, station_file = station_file,
      tv = tn, pipe_house = pipe_house, start_dttm = nd_min,
      end_dttm = nd_max, return_dta = FALSE
    )[[tn]]
  } else if (data.table::is.data.table(station_file)) {
    sf <- station_file
  }
  if (is.null(phen_dt)) {
    phens <- unique(c(names(sf), names(ndt)))[order(phens)]
  } else {
    phens <- unique(phen_dt[table_name == tn]$phen_name)
  }

  ovlap <- ipayipi::append_phen_overlap_data(
    ndt = ndt, nd_min = nd_min, nd_max = nd_max,
    phen_id = phen_id, phens = phens, phen_dt = phen_dt,
    sf_phen_ds = sf_phen_ds, new_phen_ds = new_phen_ds,
    ri = ri, rit = rit, tn = tn, overwrite_sf = overwrite_sf,
    sf_eindx = sf_eindx,
    verbose = verbose, xtra_v = xtra_v
  )
  phen_dtos <- ovlap$phen_dtos
  phen_dtos <- unique(data.table::rbindlist(phen_dtos))
  sfo_max <- ovlap$sfo_max
  sfo_min <- ovlap$sfo_min

  # get station file table aindxr
  ai <- readRDS(file.path(sfc[tn], "aindxr"))
  # add in min and max for each chunk in the aindxr file for use below
  # so that the actual min and max dttm of data can be queried easily

  ### deal with non-overlap data
  if (is.null(sfo_min)) {
    sfo_min <- min(ai$mn)
    sfo_max <- max(ai$mx)
  }
  ndno1 <- ndt[date_time < sfo_min]
  ndno2 <- ndt[date_time > sfo_max]

  ## only do this section if phen_id is TRUE ---------------------
  if (phen_id) {
    si <- sf_eindx$indx$indx_tbl
    #if (nrow(sfno1) < 1 && nrow(sfno2) < 1) {
    simn <- min(si$dta_mn, na.rm = TRUE)
    simx <- max(si$dta_mx, na.rm = TRUE)
    if (all(min(simn > nd_min, simx < nd_max))) {
      sfno_pdt <- sf_phen_ds[0, ]
    } else {
      sfno_dttm_min <- simn
      sfno_dttm_max <- simx
      sfno_pdt <- sf_phen_ds
      sfno_pdt$start_dttm <- data.table::fifelse(
        sfno_pdt$start_dttm < sfno_dttm_min,
        sfno_dttm_min, sfno_pdt$start_dttm
      )
      sfno_pdt$end_dttm <- data.table::fifelse(
        sfno_pdt$end_dttm > sfno_dttm_max,
        sfno_dttm_max, sfno_pdt$end_dttm
      )
    }
    if (nrow(ndno1) < 1 && nrow(ndno2) < 1) {
      ndno_pdt <- new_phen_ds[0, ]
    } else {
      ndno_dttm_min <- min(rbind(ndno1, ndno2)$date_time)
      ndno_dttm_max <- max(rbind(ndno1, ndno2)$date_time)
      ndno_pdt <- new_phen_ds[table_name == tn]
      ndno_pdt$start_dttm <- data.table::fifelse(
        ndno_pdt$start_dttm < ndno_dttm_min,
        ndno_dttm_min, ndno_pdt$start_dttm
      )
      ndno_pdt$end_dttm <- data.table::fifelse(
        ndno_pdt$end_dttm > ndno_dttm_max,
        ndno_dttm_max, ndno_pdt$end_dttm
      )
    }
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
      f = factor(paste(phen_ds$phid))
    )
    split_phen_ds <- lapply(split_phen_ds, function(z) {
      z <- data.table::data.table(start_dttm = min(z$start_dttm),
        end_dttm = max(z$end_dttm), phid = z$phid[1]
      )
      return(z)
    })
    phen_ds <- data.table::rbindlist(split_phen_ds)[order(phid, start_dttm)][,
      table_name := tn
    ]
    phen_ds <- unique(phen_ds)
  }

  # write in new data no overlap
  ipayipi::sf_dta_chunkr(dta_room = sfc[tn], dta_sets = list(ndno1, ndno2),
    tn = tn, ri = ri, rit = rit, rechunk = FALSE, verbose = verbose,
    xtra_v = xtra_v
  ) # chunkr performs intra time series integrity check
  # add in overlap data
  fs <- dta_list(input_dir = ovlap$dta_room, unwanted = "aindxr")
  lapply(fs, function(x) {
    dta_sets <- list(readRDS(file.path(ovlap$dta_room, x)))
    ipayipi::sf_dta_chunkr(dta_room = sfc[tn], dta_sets = dta_sets,
      tn = tn, ri = ri, rit = rit, rechunk = FALSE, verbose = verbose,
      xtra_v = xtra_v
    )
  })
  unlink(ovlap$dta_room, recursive = TRUE)
  return(list(phen_ds = phen_ds))
}
