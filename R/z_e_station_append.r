#' @title Append meteorological station data
#' @description Appends two standardised 'ipayipi' meteorological' station
#'  files.
#' @param station_file The main data set to which data will be appended.
#' @param new_data The data that will be appended to the main 'hobo station'.
#' @param overwrite_old Logical. Defaults to FALSE so that data from the
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
  overwrite_old = TRUE,
  by_station_table = FALSE,
  ...
) {
  # check station names
  if (station_file$data_summary$stnd_title[1] !=
    new_data$data_summary$stnd_title[1]) {
      stop("Station name mismatch")
  }
  # check table names
  tn <- new_data$data_summary$table_name[1]
  # generate data summary unique id
  new_data$data_summary$dsid <- seq_len(nrow(new_data$data_summary)) +
    max(station_file$data_summary$dsid)
  # update the dsid in the phen data summary of the new data
  new_data$phen_data_summary[
    table_name == tn]$dsid <- new_data$data_summary$dsid[1]
  if ("phen_name" %in% names(new_data$phen_data_summary)) {
    new_data$phen_data_summary <- subset(new_data$phen_data_summary,
    select = -phen_name)
  }
  if ("phen_name" %in% names(station_file$phen_data_summary)) {
    station_file$phen_data_summary <- subset(station_file$phen_data_summary,
    select = -phen_name)
  }
  # warn user if phenomena standards are different
  # need to set up ids for phenomena
  # first need to deal with only phens of the same input table name
  new_data$phens$phid <- seq_len(nrow(new_data$phens)) +
    max(station_file$phens$phid)
  # update the new_data phenomena data summary
  new_data$phen_data_summary <- data.table::data.table(
    phid = as.integer(new_data$phens$phid),
    dsid = as.integer(rep(as.integer(new_data$data_summary$dsid),
      nrow(new_data$phens))),
    start_dt = rep(new_data$data_summary$start_dt,
      nrow(new_data$phens)),
    end_dt = rep(new_data$data_summary$end_dt, nrow(new_data$phens)),
    table_name = rep(new_data$data_summary$table_name,
      nrow(new_data$phens))
  )
  non_tab_phens <- station_file$phens[table_name != tn]
  non_tab_phen_summ <- station_file$phen_data_summary[
    table_name != tn]
  st_phens <- station_file$phens[table_name == tn]
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
  for (i in seq_len(nrow(phen_dt[dups == TRUE]))) {
    z_name_full <- phen_dt[dups == TRUE]$phen_name_full[i]
    z_type <- phen_dt[dups == TRUE]$phen_type[i]
    z_name <- phen_dt[dups == TRUE]$phen_name[i]
    zunits <- phen_dt[dups == TRUE]$units[i]
    zmeasure <- phen_dt[dups == TRUE]$measure[i]
    zoffset <- phen_dt[dups == TRUE]$offset[i]
    zvar_type <- phen_dt[dups == TRUE]$var_type[i]
    zuz_phen_name <- phen_dt[dups == TRUE]$uz_phen_name[i]
    zuz_units <- phen_dt[dups == TRUE]$uz_units[i]
    zuz_measure <- phen_dt[dups == TRUE]$uz_measure[i]
    zf_convert <- phen_dt[dups == TRUE]$f_convert[i]
    phen_dt[dups == TRUE]$phid_new[i] <-
      phen_dt[dups == FALSE][phen_name_full %in% z_name_full][
        phen_type %in% z_type][phen_name %in% z_name][
        units %in% zunits][measure %in% zmeasure][
        offset %in% zoffset][var_type %in% zvar_type][
        uz_phen_name %in% zuz_phen_name][uz_units %in% zuz_units][
        uz_measure %in% zuz_measure]$phid[1]
  }
  # update the phen data summary of the new data
  for (i in seq_len(nrow(phen_dt[dups == TRUE]))) {
    phid_n <- phen_dt[dups == TRUE]$phid[i]
    new_data$phen_data_summary[
      phid == phid_n & table_name == tn]$phid <-
        phen_dt[dups == TRUE & phid == phid_n]$phid_new
  }
  phen_dt[dups == TRUE & origin == "new_data"]$phid <-
    phen_dt[dups == TRUE & origin == "new_data"]$phid_new
  phen_dt <- unique(subset(phen_dt, select = -c(origin, dups, phid_new)))
  # append tables
  # if the raw data is new to the station then just add to station
  if (!paste0("raw_", tn) %in% names(station_file)) {
    station_file[[paste0("raw_", tn)]] <-
      new_data[[paste0("raw_", tn)]]
    station_file$data_summary <- rbind(station_file$data_summary,
      new_data$data_summary)[order(table_name, start_dt, end_dt)]
  }
  # check the record intervals
  stint <- station_file$data_summary[table_name == tn]$record_interval
  ndint <- new_data$data_summary[table_name == tn]$record_interval
  if (!ndint %in% stint) {
    message("Warning difference in record intervals detected!")
    # if data is continuous and only one record in new data: inherit
    # record_interval from station data
    if (nrow(new_data[[paste0("raw_", tn)]]) < 2) {
      message(paste0("nrow(", paste0("raw_", tn), ") data < 2.",
        " Inheriting record interval from station.", collapse = ""))
      new_data$data_summary[table_name == tn]$record_interval <- stint
      ndint <- stint
    }
  }

  # get the right tables for appending
  new_data_cm <- names(station_file)[names(station_file) %in% names(new_data)]
  new_data_cm <- new_data_cm[new_data_cm %ilike% "raw_"]

  # # make a date-time sequence to join the phen data to
  # if (append_mode == "continuous") {
  #   start_dt <- min(c(station_file[[new_data_cm]]$date_time,
  #     new_data[[new_data_cm]]$date_time))
  #   end_dt <- max(c(station_file[[new_data_cm]]$date_time,
  #     new_data[[new_data_cm]]$date_time))
  #   seq_int <- mondate::as.difftime(
  #     as.numeric(substr(stint[1], 1, unlist(gregexpr("_", stint[1]))[1] - 1)),
  #     units = substr(stint[1], unlist(gregexpr("_", stint[1]))[1] + 1,
  #       nchar(stint[1])))
  #   tchar <- ipayipi::sts_interval_name(seq_int)
  #   tchar <- paste(tchar$dfft[1], tchar$dfft_units[1])
  #   start_dt <- lubridate::floor_date(start_dt, unit = attr(seq_int, "units"))
  #   dt_seq <- seq(from = start_dt, to = end_dt, by = tchar)
  #   # set up table for fuzzy date-time join
  #   dt_seq <- data.table::data.table(
  #     dt_seq = dt_seq,
  #     dt_fuzz_1 = mondate::subtract(dt_seq, (seq_int / 2.0000000001)),
  #     dt_fuzz_2 = dt_seq + (seq_int / 2.0000000001)
  #   )
  # } else {
  #   dt_seq <- unique(c(station_file[[new_data_cm]]$date_time,
  #     new_data[[new_data_cm]]$date_time))
  #   dt_seq <- dt_seq[order(dt_seq)]
  #   dt_seq <- data.table::data.table(
  #     dt_seq = dt_seq, dt_fuzz_1 = dt_seq, dt_fuzz_2 = dt_seq
  #   )
  # }

  phd <- ipayipi::append_phen_data(old_dta = station_file[[new_data_cm]],
    old_phen_ds = station_file$phen_data_summary[table_name == tn],
    new_dta = new_data[[new_data_cm]],
    new_phen_ds = new_data$phen_data_summary[table_name == tn], tn = tn,
    overwrite_old = overwrite_old, phen_dt = phen_dt)

  # finalise the data summary
  ds <- rbind(
    station_file$data_summary[table_name == tn],
    new_data$data_summary[table_name == tn]
  )[order(start_dt, end_dt)]

  # finalise all other tables
  # special tables are appended outside the 'append_tables()' function
  special_tbls <- c("data_summary", "phen_data_summary", "phens", new_data_cm)
  noriginal_tbl <- names(station_file)[!names(station_file) %in% special_tbls]
  original_tbl <- station_file[noriginal_tbl]
  names(original_tbl) <- noriginal_tbl
  nnew_tbl <- names(new_data)[!names(new_data) %in% special_tbls]
  new_tbl <- new_data[nnew_tbl]
  names(new_tbl) <- nnew_tbl
  other_tbls <- ipayipi::append_tables(
    original_tbl = original_tbl, new_tbl = new_tbl)

  # add in the data_summary table
  station_file$data_summary <- station_file$data_summary[table_name != tn]
  station_file$data_summary <- rbind(station_file$data_summary, ds)
  # add in the raw data
  station_file[[new_data_cm]] <- phd$app_dta
  station_file$phens <- unique(rbind(phen_dt, non_tab_phens, fill = TRUE))[
    order(table_name, phen_name_full, uz_phen_name)]
  # add in the phen data summary
  phends <- rbind(non_tab_phen_summ, phd$phen_ds, fill = TRUE)[
    , names(phd$phen_ds)[!names(phd$phen_ds) %in% c("phen_name")],
    with = FALSE]
  phends <- merge(x = unique(station_file$phens[,
    c("phid", "phen_name"), with = FALSE]),
    y = phends, by = "phid", all.y = TRUE)[
    order(table_name, phen_name, start_dt, end_dt)]
  station_file$phen_data_summary <- phends
  station_file <- station_file[!names(station_file) %in% names(other_tbls)]
  station_file <- c(station_file, other_tbls)
  return(station_file)
}