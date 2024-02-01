#' @title Append meteorological station data
#' @description Appends two standardised 'ipayipi' meteorological' station
#'  files.
#' @param station_file The main data set to which data will be appended.
#' @param new_data The data that will be appended to the main 'hobo station'.
#' @param overwrite_sf Logical. Defaults to FALSE so that data from the
#'  `station_file` is not over written by the `new_data`.
#' @param by_station_table If TRUE then multiple station tables will not be
#'  kept in the same station file. Defaults to FALSE.
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on Linux.
#' @author Paul J. Gordijn
#' @keywords logger data; data pipeline; append data;
#' @export
append_station <- function(
  pipe_house = NULL,
  station_file = NULL,
  new_data = NULL,
  overwrite_sf = TRUE,
  by_station_table = FALSE,
  phen_id = TRUE,
  cores = getOption("mc.cores", 2L),
  ...
) {
  "phen_name" <- "table_name" <- "dups" <- "phid_new" <-
    "phen_name_full" <- "phen_type" <- "measure" <- "uz_phen_name" <-
      "var_type" <- "uz_units" <- "uz_measure" <- "phid" <- "origin" <-
        "date_time" <- NULL

  sfc <- ipayipi::open_sf_con(pipe_house = pipe_house, station_file =
    station_file)

  ## read in data ----
  sfi <- ipayipi::sf_read(sfc = sfc, station_file = station_file,
    pipe_house = pipe_house, tmp = TRUE, tv = c("data_summary",
      "phen_data_summary", "phens"))
  sf_ds <- sfi[["data_summary"]]
  sf_phen_ds <- unique(sfi[["phen_data_summary"]])
  sf_phens <- sfi[["phens"]]
  rm(sfi)

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
  if (tn %in% names(sfc)) {
    # check the record intervals
    stint <- sf_ds[table_name == tn]$record_interval
    ndint <- new_data$data_summary[table_name == tn]$record_interval
    rit <- new_data$data_summary[table_name == tn]$record_interval_type
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
      # special handing of "mixed" record intervals
      if (any(rit %in% "mixed")) {
          dmix <- new_data[[tn]][!date_time %in% dt_seq$date_time]
          new_data[[tn]][date_time %in% dt_seq$date_time]
      } else {
        dmix <- new_data[[tn]][0, ]
        # now use dt_seq to ensure continuity of time series data
        new_data[[tn]]$date_time <- lubridate::round_date(
          new_data[[tn]]$date_time, ri
        )
      }

      new_data[[tn]] <- new_data[[tn]][!duplicated(new_data[[tn]]$date_time)]
      data.table::setkey(dt_seq, "date_time")
      data.table::setkey(new_data[[tn]], "date_time")
      new_data[[tn]] <-
        merge(x = new_data[[tn]], y = dt_seq, all.y = TRUE, all.x = FALSE)
      new_data[[tn]] <- rbind(dmix, new_data[[tn]])[order(date_time)]
      new_data[[tn]] <- new_data[[tn]][!duplicated(new_data[[tn]])]
    } else {
      ri <- "discnt"
    }

    ndt <- new_data[[tn]]
    new_phen_ds <- new_data$phen_data_summary
    phd <- ipayipi::append_phen_data2(pipe_house = pipe_house,
      station_file = station_file, sf_phen_ds = sf_phen_ds, ndt = ndt,
      new_phen_ds = new_phen_ds, tn = tn, overwrite_sf = overwrite_sf,
      phen_dt = phen_dt, rit = rit, ri = ri, phen_id = phen_id, cores =
      cores)
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
  original_tbl <- unique(names(sfc)[!names(sfc) %in% special_tbls])
  names(original_tbl) <- original_tbl
  nnew_tbl <- unique(names(new_data)[!names(new_data) %in% special_tbls])
  new_tbl <- new_data[nnew_tbl]
  names(new_tbl) <- nnew_tbl
  utbls <- unique(c(names(original_tbl), names(new_tbl)))
  parallel::mclapply(seq_along(utbls), function(i) {
    if (!is.null(original_tbl[names(original_tbl) %in% utbls[i]])) {
      o <- ipayipi::sf_read(sfc = sfc, tv = utbls[i], station_file =
        station_file, pipe_house = pipe_house, tmp = TRUE)
      names(o) <- utbls[i]
      if (is.null(o[[1]])) o <- NULL
    } else {
      o <- NULL
    }
    if (!is.null(new_tbl[names(new_tbl) %in% utbls[i]])) {
      n <- new_tbl[utbls[i]]
      if (is.null(n[[1]])) n <- NULL
    } else {
      n <- NULL
    }
    a <- ipayipi::append_tables(original_tbl = o, new_tbl = n)

    # special op for logg_interfere
    if ("logg_interfere" %in% names(a)) {
      a[[1]] <- unique(a[[1]], by = c("date_time", "logg_interfere_type"))
      a[[1]]$id <- seq_len(nrow(a[[1]]))
    }
    # save table to temp dir
    saveRDS(a[[1]], file = file.path(dirname(sfc[1]), names(a)))
  }, mc.cores = cores)

  # add in the data_summary table
  sfds <- sf_ds[table_name != tn]
  sfds <- rbind(sfds, ds)
  saveRDS(sfds, sfc[names(sfc) %in% "data_summary"])

  # add in the raw data
  saveRDS(phd$app_dta, file.path(dirname(sfc[1]), tn))
  sf_phens <- unique(rbind(phen_dt, non_tab_phens, fill = TRUE))[
    order(table_name, phen_name_full, uz_phen_name)]
  saveRDS(sf_phens, sfc[names(sfc) %in% "phens"])

  # add in the phen data summary
  phends <- rbind(non_tab_phen_ds, phd$phen_ds, fill = TRUE)[
    , names(phd$phen_ds)[!names(phd$phen_ds) %in% c("phen_name")],
    with = FALSE]
  phends <- unique(phends)
  phends <- merge(x = unique(sf_phens[,
    c("phid", "phen_name"), with = FALSE]),
    y = phends, by = "phid", all.y = TRUE,
    allow.cartesian = TRUE)[
    order(table_name, phen_name, start_dttm, end_dttm)]
  phends <- unique(phends)
  saveRDS(phends, sfc[names(sfc) %in% "phen_data_summary"])

  return(station_file)
}
