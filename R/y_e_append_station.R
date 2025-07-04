#' @title Append station data using chunck reads
#' @description Appends two standardised 'ipayipi' station files.
#' @inheritParams append_station_batch
#' @param station_file The name main data set to which data will be appended.
#' @param new_data The data that will be appended to the main 'hobo station'.
#' @param sfc List of file paths to the temporary station-file directory. Generated using [sf_open_con()].
#' @author Paul J. Gordijn
#' @keywords logger data; data pipeline; append data;
#' @export
append_station <- function(
  pipe_house = NULL,
  station_file = NULL,
  new_data = NULL,
  overwrite_sf = TRUE,
  phen_id = TRUE,
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  sfc = NULL,
  ...
) {
  "%ilike%" <- "%chin%" <- NULL
  "phen_name" <- "table_name" <- "phen_name_full" <-
    "uz_phen_name" <- "origin" <- "date_time" <- NULL

  sfc <- sf_open_con(sfc = sfc)

  ## read in data ----
  sf_ds <- readRDS(sfc["data_summary"])
  sf_phen_ds <- readRDS(sfc["phen_data_summary"])
  sf_phens <- readRDS(sfc["phens"])

  # check station names
  if (sf_ds$stnd_title[1] != new_data$data_summary$stnd_title[1]) {
    cli::cli_alert("Station name mismatch")
  }
  # check table names
  tn <- new_data$data_summary$table_name[1]

  ## data summary integration ----
  # generate data summary unique id
  new_data$data_summary$dsid <- seq_len(nrow(new_data$data_summary)) +
    max(sf_ds$dsid)
  # update the dsid in the phen data summary of the new data
  if ("phen_name" %chin% names(new_data$phen_data_summary)) {
    new_data$phen_data_summary <- subset(new_data$phen_data_summary,
      select = -phen_name
    )
  }
  if ("phen_name" %chin% names(sf_phen_ds)) {
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
    table_name == tn
  ]
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
  ndt <- new_data[[tn]]
  if (tn %chin% names(sfc)) {
    # check the record intervals
    stint <- sf_ds[table_name == tn]$record_interval
    ndint <- new_data$data_summary[table_name == tn]$record_interval
    rit <- new_data$data_summary[table_name == tn]$record_interval_type
    if (!ndint %chin% stint) {
      cli::cli_inform(c("!" = "Difference in record intervals detected!"))
      # if data is continuous and only one record in new data: inherit
      # record_interval from station data
      if (nrow(ndt) < 2) {
        cli::cli_warn(c("i" = "In continuous data from {station_file}:",
          "nrow({tn}) data < 2.",
          ">" = paste0("Inheriting 'record interval' from station, such that:",
            " the interval of {tn} is {stint[1]}"
          )
        ))
        new_data$data_summary[table_name == tn]$record_interval <- stint[1]
        ndint <- stint[1]
      }
    }

    # make a date-time sequence to join the phen data to
    if (!"discnt" %chin% ndint[1]) {
      start_dttm <- min(ndt$date_time)
      end_dttm <- max(ndt$date_time)
      ri <- gsub(pattern = "_", replacement = " ", ndint)
      start_dttm <- lubridate::round_date(start_dttm, unit = ri)
      end_dttm <- lubridate::round_date(end_dttm, unit = ri)
      dt_seq <- seq(from = start_dttm, to = end_dttm, by = ri)
      dt_seq <- data.table::data.table(date_time = dt_seq)
      # special handing of "mixed" record intervals
      if (any(rit %chin% "mixed")) {
        dmix <- ndt[!date_time %in% dt_seq$date_time]
        ndt[date_time %in% dt_seq$date_time]
      } else {
        dmix <- ndt[0, ]
        # now use dt_seq to ensure continuity of time series data
        ndt$date_time <- lubridate::round_date(ndt$date_time, ri)
      }

      ndt <- ndt[!duplicated(ndt$date_time)]
      data.table::setkey(dt_seq, "date_time")
      data.table::setkey(ndt, "date_time")
      ndt <-
        merge(x = ndt, y = dt_seq, all.y = TRUE, all.x = FALSE)
      ndt <- rbind(dmix, ndt)[order(date_time)]
      ndt <- ndt[!duplicated(ndt)]
    } else {
      ri <- "discnt"
    }
    new_phen_ds <- new_data$phen_data_summary
    phd <- append_phen_data(pipe_house = pipe_house,
      station_file = station_file, sf_phen_ds = sf_phen_ds, ndt = ndt,
      new_phen_ds = new_phen_ds, tn = tn, overwrite_sf = overwrite_sf,
      phen_dt = phen_dt, rit = rit, ri = ri, phen_id = phen_id,
      verbose = verbose, xtra_v = xtra_v, chunk_v = chunk_v, sfc = sfc
    )
  } else {
    phd <- list(
      phen_ds = new_data$phen_data_summary
    )
    rit <- new_data$data_summary[table_name == tn]$record_interval_type
    ri <- new_data$data_summary[table_name == tn]$record_interval[1]
    ri <- gsub("_", " ", ri)
    sl <- sf_dta_wr(dta_room = file.path(dirname(sfc[1]), tn),
      tn = tn, ri = ri, rit = rit, dta = ndt, chunk_v = chunk_v
    )
    sf_log_update(sfc = sfc, log_update = sl)
  }

  # finalise the data summary
  ds <- rbind(
    sf_ds[table_name == tn],
    new_data$data_summary[table_name == tn]
  )[order(start_dttm, end_dttm)]
  ds <- unique(ds)

  # update the data summary with the logg_interfere type
  if (!"logg_interfere" %chin% names(ds)) ds$logg_interfere <- "on_site"

  # finalise all other tables
  # special tables are appended outside the 'append_tbls()' function
  special_tbls <- c("data_summary", "phen_data_summary", "phens", tn)
  nnew_tbl <- unique(names(new_data)[!names(new_data) %chin% special_tbls])
  nnew_tbl <- nnew_tbl[!nnew_tbl %ilike% "^raw_|^dt_"]
  new_tbl <- new_data[nnew_tbl]
  names(new_tbl) <- nnew_tbl
  # write data and update tmp sf log
  log_update <- lapply(seq_along(nnew_tbl), function(i) {
    # save table to temp dir
    stlog <- sf_dta_wr(dta_room = file.path(dirname(sfc[1]), nnew_tbl[i]),
      dta = new_tbl[[i]], overwrite = TRUE, verbose = verbose,
      tn = nnew_tbl[i], chunk_v = chunk_v
    )
    return(stlog)
  })
  sf_log_update(sfc = sfc, log_update = log_update)

  # add in the data_summary table
  sfds <- sf_ds[table_name != tn]
  sfds <- rbind(sfds, ds)
  sf_dta_rm(sfc = sfc, rm = "data_summary")
  stlog <- sf_dta_wr(dta_room = file.path(dirname(sfc[1]), "data_summary"),
    dta = sfds, tn = "data_summary", chunk_v = chunk_v
  )
  sf_log_update(sfc = sfc, log_update = stlog)

  sf_phens <- unique(rbind(phen_dt, non_tab_phens, fill = TRUE))[
    order(table_name, phen_name_full, uz_phen_name)
  ][, -c("origin")]
  sf_dta_rm(sfc = sfc, rm = "phens")
  stlog <- sf_dta_wr(dta_room = file.path(dirname(sfc[1]), "phens"),
    dta = sf_phens, overwrite = TRUE, verbose = verbose,
    tn = "phens", chunk_v = chunk_v
  )
  sf_log_update(sfc = sfc, log_update = stlog)
  #saveRDS(sf_phens, sfc[names(sfc) %chin% "phens"])

  # add in the phen data summary
  phends <- rbind(non_tab_phen_ds, phd$phen_ds, fill = TRUE)[
    , names(phd$phen_ds)[!names(phd$phen_ds) %chin% c("phen_name")],
    with = FALSE
  ]
  phends <- unique(phends)
  phends <- merge(x = unique(
    sf_phens[, c("phid", "phen_name"), with = FALSE]
  ), y = phends, by = "phid", all.y = TRUE, allow.cartesian = TRUE
  )[order(table_name, phen_name, start_dttm, end_dttm)]
  phends <- unique(phends)
  sf_dta_rm(sfc = sfc, rm = "phen_data_summary")
  stlog <- sf_dta_wr(dta_room = file.path(dirname(sfc[1]), "phen_data_summary"),
    dta = phends, tn = "phen_data_summary", chunk_v = chunk_v
  )
  sf_log_update(sfc = sfc, log_update = stlog)
  return(station_file)
}
