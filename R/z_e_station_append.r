#' @title Append meteorological station data
#' @description Appends two standardised 'ipayipi' meteorological' station
#'  files.
#' @param station_file The main data set to which data will be appended.
#' @param new_data The data that will be appended to the main 'hobo station'.
#' @param overwrite_sf Logical. Defaults to FALSE so that data from the
#'  `station_file` is not over written by the `new_data`.
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
  ...
) {
  sf_ds <- readRDS(station_file)$data_summary
  sf_phen_ds <- readRDS(station_file)$phen_data_summary
  sf_phens <- readRDS(station_file)$phens
  sf_names <- names(readRDS(station_file))
  # check station names
  if (sf_ds$stnd_title[1] != new_data$data_summary$stnd_title[1]) {
      stop("Station name mismatch")
  }
  # check table names
  tn <- new_data$data_summary$table_name[1]
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
  # warn user if phenomena standards are different
  # need to set up ids for phenomena
  # first need to deal with only phens of the same input table name
  new_data$phens$phid <- seq_len(nrow(new_data$phens)) +
    max(sf_phens$phid)
  # update the new_data phenomena data summary
  new_data$phen_data_summary <- data.table::data.table(
    phid = as.integer(new_data$phens$phid),
    # dsid = as.integer(rep(as.integer(new_data$data_summary$dsid),
    #  nrow(new_data$phens))),
    start_dttm = rep(new_data$data_summary$start_dttm,
      nrow(new_data$phens)),
    end_dttm = rep(new_data$data_summary$end_dttm, nrow(new_data$phens)),
    table_name = rep(new_data$data_summary$table_name,
      nrow(new_data$phens))
  )
  non_tab_phens <- sf_phens[table_name != tn]
  non_tab_phen_ds <- sf_phen_ds[table_name != tn]
  st_phens <- sf_phens[table_name == tn]
  nd_phens <- new_data$phens[table_name == tn]
  st_phens$origin <- "station"
  nd_phens$origin <- "new_data"
  phen_dt <- rbind(st_phens, nd_phens)
  # check that the phenomena have been standardised
  uphen <- c("phen_name_full", "phen_name", "units", "measure", "var_type")
  phen_dt_dups <- unique(phen_dt[, uphen, with = FALSE])
  if (any(duplicated(phen_dt_dups$phen_name))) {
    print(phen_dt[duplicated(phen_dt_dups$phen_name), ])
    message("Standardise phenomena before appending data")
    message("Phenomena must have the same: ")
    message(paste(uphen, " ", sep = " "), " details.")
    stop("Difference in the above phenomena detected")
  }
  phen_dt$dups <- duplicated(phen_dt, by = names(phen_dt)[
    !names(phen_dt) %in% c("origin", "phid")])
  phen_dt$phid_new <- 0
  # need to update phid's of duplicates
  for (j in seq_len(nrow(phen_dt[dups == TRUE]))) {
    z_name_full <- phen_dt[dups == TRUE]$phen_name_full[j]
    z_type <- phen_dt[dups == TRUE]$phen_type[j]
    z_name <- phen_dt[dups == TRUE]$phen_name[j]
    zunits <- phen_dt[dups == TRUE]$units[j]
    zmeasure <- phen_dt[dups == TRUE]$measure[j]
    zoffset <- phen_dt[dups == TRUE]$offset[j]
    zvar_type <- phen_dt[dups == TRUE]$var_type[j]
    zuz_phen_name <- phen_dt[dups == TRUE]$uz_phen_name[j]
    zuz_units <- phen_dt[dups == TRUE]$uz_units[j]
    zuz_measure <- phen_dt[dups == TRUE]$uz_measure[j]
    zf_convert <- phen_dt[dups == TRUE]$f_convert[j]
    phen_dt[dups == TRUE]$phid_new[j] <-
      phen_dt[dups == FALSE][phen_name_full %in% z_name_full][
        phen_type %in% z_type][phen_name %in% z_name][
        units %in% zunits][measure %in% zmeasure][
        offset %in% zoffset][var_type %in% zvar_type][
        uz_phen_name %in% zuz_phen_name][uz_units %in% zuz_units][
        uz_measure %in% zuz_measure]$phid[1]
  }
  # update the phen data summary of the new data
  for (j in seq_len(nrow(phen_dt[dups == TRUE]))) {
    phid_n <- phen_dt[dups == TRUE]$phid[j]
    new_data$phen_data_summary[
      phid == phid_n & table_name == tn]$phid <-
        phen_dt[dups == TRUE & phid == phid_n]$phid_new
  }
  phen_dt[dups == TRUE & origin == "new_data"]$phid <-
    phen_dt[dups == TRUE & origin == "new_data"]$phid_new
  phen_dt <- unique(subset(phen_dt, select = -c(origin, dups, phid_new)))
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
      data.table::setkey(dt_seq, "date_time")
      data.table::setkey(new_data[[tn]], "date_time")
      new_data[[tn]] <-
        merge(x = new_data[[tn]], y = dt_seq, all.y = TRUE)
    } else {
      ri <- "discnt"
    }

    phd <- ipayipi::append_phen_data(station_file = station_file,
      sf_phen_ds = sf_phen_ds, ndt = new_data[[tn]],
      new_phen_ds = new_data$phen_data_summary, tn = tn,
      overwrite_sf = overwrite_sf, phen_dt = phen_dt, ri = ri)
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
