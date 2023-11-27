#' @title Append meteorological station data
#' @description Appends two standardised 'ipayipi' meteorological' station
#'  files.
#' @param station_file The main data set to which data will be appended.
#' @param new_data The data that will be appended to the main 'hobo station'.
#' @param overwrite_sf Logical. Defaults to FALSE so that data from the
#'  `station_file` is not over written by the `new_data`.
#' @param by_station_table If TRUE then multiple station tables will not be
#'  kept in the same station file. Defaults to FALSE.
#' @author Paul J. Gordijn
#' @keywords meteorological data; data pipeline; append data;
#' @details
#'  Special attention is given to the `data summary` table of
#'  `SAEON_rain_data` objects. The data summary contains information
#'  which is used in the pipeline to determine which files are appended to
#'  another. Specifically, the standardised ptitle in the data summary is the
#'  identifier for stations, and records in the `nomvet_name` field are used to
#'  determine which files have been included in any given `SAEON_rain_data`
#'  object.
#' @export
append_station <- function(
  station_file = NULL,
  new_data = NULL,
  overwrite_sf = TRUE,
  by_station_table = FALSE,
  phen_id = TRUE,
  ...
) {
  "phen_name" <- "table_name" <- "dups" <- "phid_new" <-
    "phen_name_full" <- "phen_type" <- "measure" <- "uz_phen_name" <-
      "var_type" <- "uz_units" <- "uz_measure" <- "phid" <- "origin" <-
        NULL
  ## read in data ----
  sf_ds <- readRDS(station_file)$data_summary
  sf_phen_ds <- unique(readRDS(station_file)$phen_data_summary)
  sf_phens <- readRDS(station_file)$phens
  sf_names <- names(readRDS(station_file))
  # check station names
  if (sf_ds$stnd_title[1] != new_data$data_summary$stnd_title[1]) {
      stop("Station name mismatch")
  }
  # check table names
  tn <- new_data$data_summary$table_name[1]

  ## data summary integration ----
  # generate data summary unique id
  new_data$data_summary$dsid <- seq_len(nrow(new_data$data_summary)) +
    max(sf_ds$dsid)
  # update the dsid in the phen data summary of the new data
  if ("phen_name" %in% names(new_data$phen_data_summary)) {
    new_data$phen_data_summary <- subset(new_data$phen_data_summary,
      select = -phen_name)
  }
  if ("phen_name" %in% names(sf_phen_ds)) {
    sf_phen_ds <- subset(sf_phen_ds, select = -phen_name)
  }

  ## organise phenomena ----
  # warn user if phenomena standards are different
  # need to set up ids for phenomena
  # first need to deal with only phens of the same input table name
  new_data$phens$phid <- seq_len(nrow(new_data$phens)) +
    max(sf_phens$phid)

  # update new data phids to match existant station phen table
  sf_phens$origin <- "s_f"
  new_data$phens$origin <- "n_d"
  phb <- rbind(sf_phens, new_data$phens)
  # the uphen vector can be updated with more fields to retain greater detail
  uphen <- c("phen_name", "units", "measure", "var_type")
  # standardise non-unique phens with lowest phid
  f <- paste(phb$phen_name, phb$units, phb$measure, phb$var_type, sep = "-")
  phb <- split.data.frame(phb, f = factor(f))
  phb <- lapply(phb, function(x) {
    x$phid_update <- min(x$phid)
    invisible(x)
  })
  ucols <- c("phid", "phid_update")
  phb_ids <- data.table::rbindlist(phb)[origin == "s_f"][, ucols, with = FALSE]
  phb_ids <- unique(phb_ids)
  phb <- lapply(phb, function(x) {
    x <- unique(x, by = c(uphen, "table_name", "origin"))
    invisible(x)
  })
  phb <- data.table::rbindlist(phb)
  phb$phid <- phb$phid_update
  phb <- phb[, -c("origin", "phid_update"), with = FALSE]
  phb <- unique(phb, by = c("phen_name", "phid", "table_name"))
  phen_dt <- phb[order(table_name, phen_name_full, phen_name)][
    table_name == tn]
  new_data$phen_data_summary <- data.table::data.table(
    phid = phb[table_name == tn]$phid,
    start_dttm = min(new_data[[tn]]$date_time),
    end_dttm = max(new_data[[tn]]$date_time),
    table_name = tn
  )
  non_tab_phens <- sf_phens[table_name != tn]
  non_tab_phen_ds <- sf_phen_ds[table_name != tn]
  sf_phen_ds <- sf_phen_ds[table_name == tn]

  # append tables
  # if the raw data is new to the station then just add to station
  if (tn %in% sf_names) {
    # check the record intervals
    stint <- sf_ds[table_name == tn]$record_interval
    ndint <- new_data$data_summary[table_name == tn]$record_interval
    if (!ndint %in% stint) {
      message("Warning difference in record intervals detected!")
      # if data is continuous and only one record in new data: inherit
      # record_interval from station data
      if (nrow(new_data[[tn]]) < 2) {
        message(paste0("nrow(", tn, ") data < 2.",
          " Inheriting record interval from station.", collapse = ""))
        new_data$data_summary[table_name == tn]$record_interval <- stint[1]
        ndint <- stint[1]
      }
    }

    # make a date-time sequence to join the phen data to
    if (!"discnt" %in% ndint[1]) {
      start_dttm <- min(new_data[[tn]]$date_time)
      end_dttm <- max(new_data[[tn]]$date_time)
      ri <- gsub(pattern = "_", replacement = " ", ndint)
      start_dttm <- lubridate::round_date(start_dttm, unit = ri)
      end_dttm <- lubridate::round_date(end_dttm, unit = ri)
      dt_seq <- seq(from = start_dttm, to = end_dttm, by = ri)
      dt_seq <- data.table::data.table(date_time = dt_seq)
      # now use dt_seq to ensure continuity of time series data
      new_data[[tn]]$date_time <- lubridate::round_date(
        new_data[[tn]]$date_time, ri
      )
      new_data[[tn]] <- new_data[[tn]][!duplicated(new_data[[tn]]$date_time)]
      data.table::setkey(dt_seq, "date_time")
      data.table::setkey(new_data[[tn]], "date_time")
      new_data[[tn]] <-
        merge(x = new_data[[tn]], y = dt_seq, all.y = TRUE,
          all.x = FALSE)
      new_data[[tn]] <- new_data[[tn]][!duplicated(new_data[[tn]])]
    } else {
      ri <- "discnt"
    }

    phd <- ipayipi::append_phen_data2(station_file = station_file,
      sf_phen_ds = sf_phen_ds, ndt = new_data[[tn]],
      new_phen_ds = new_data$phen_data_summary, tn = tn,
      overwrite_sf = overwrite_sf, phen_dt = phen_dt, ri = ri,
      phen_id = phen_id)
  } else {
    phd <- list(
      app_dta = new_data[[tn]],
      phen_ds = new_data$phen_data_summary)
  }

  # finalise the data summary
  ds <- rbind(
    sf_ds[table_name == tn],
    new_data$data_summary[table_name == tn]
  )[order(start_dttm, end_dttm)]
  ds <- unique(ds)

  # finalise all other tables
  # special tables are appended outside the 'append_tables()' function
  special_tbls <- c("data_summary", "phen_data_summary", "phens", tn)
  noriginal_tbl <- unique(sf_names[!sf_names %in% special_tbls])
  original_tbl <- readRDS(station_file)[noriginal_tbl]
  names(original_tbl) <- noriginal_tbl
  nnew_tbl <- unique(names(new_data)[!names(new_data) %in% special_tbls])
  new_tbl <- new_data[nnew_tbl]
  names(new_tbl) <- nnew_tbl
  other_tbls <- ipayipi::append_tables(
    original_tbl = original_tbl, new_tbl = new_tbl)

  # add in the data_summary table
  station_file <- readRDS(station_file)
  station_file$data_summary <- station_file$data_summary[table_name != tn]
  station_file$data_summary <- rbind(station_file$data_summary, ds)
  # add in the raw data
  station_file[[tn]] <- phd$app_dta
  station_file$phens <- unique(rbind(phen_dt, non_tab_phens, fill = TRUE))[
    order(table_name, phen_name_full, uz_phen_name)]
  # add in the phen data summary
  phends <- rbind(non_tab_phen_ds, phd$phen_ds, fill = TRUE)[
    , names(phd$phen_ds)[!names(phd$phen_ds) %in% c("phen_name")],
    with = FALSE]
  phends <- unique(phends)
  phends <- merge(x = unique(station_file$phens[,
    c("phid", "phen_name"), with = FALSE]),
    y = phends, by = "phid", all.y = TRUE,
    allow.cartesian = TRUE)[
    order(table_name, phen_name, start_dttm, end_dttm)]
  phends <- unique(phends)
  station_file$phen_data_summary <- phends
  station_file <- station_file[!sf_names %in% names(other_tbls)]
  station_file <- c(station_file, other_tbls)
  station_file$logg_interfere <- unique(station_file$logg_interfere,
    by = c("date_time", "logg_interfere_type"))
  station_file$logg_interfere$id <- seq_len(nrow(station_file$logg_interfere))
  class(station_file) <- c(class(station_file), "ipayipi station file")
  return(station_file)
}
