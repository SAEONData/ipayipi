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
  by_station_table = TRUE,
  ...
) {
  # check station names
  if (station_file$data_summary$stnd_title[1] !=
    new_data$data_summary$stnd_title[1]) {
      stop("Station name mismatch")
  }
  # check table names
  ds_tabn_in <- new_data$data_summary$table_name[1]
  ds_tabn_station <- station_file$data_summary$table_name[1]
  if (ds_tabn_in != ds_tabn_station && by_station_table) {
    stop("Station table name mismatch")
  }
  # warn user if phenomena standards are different
  phens_old <- station_file$phens$phen_name
  phens_new <- new_data$phens$phen_name
  phens_common <- phens_old[phens_new %in% phens_old]
  uphen <- c("phen_name_full", "phen_name", "units", "measure", "var_type")
  phen_diffs <- data.table::fintersect(
    station_file$phens[phen_name %in% phens_common][, uphen, with = FALSE],
    new_data$phens[phen_name %in% phens_common][, uphen, with = FALSE],
  )
  if (nrow(phen_diffs) <
    nrow(station_file$phens[phen_name %in% phens_common])) {
    print(phen_diffs)
    message("Standardise phenomena before appending data")
    message("Phenomena must have the same: ")
    message(paste0(uphen, " details."))
    stop("Difference in the above phenomena detected")
  }

  # append tables
  # first work with raw data which needs to be linked to
  # data summaries & phenomena
  # if the raw data is new to the station then just add to station
  if (!paste0("raw_", ds_tabn_in) %in% names(station_file)) {
    station_file[[paste0("raw_", ds_tabn_in)]] <-
      new_data[[paste0("raw_", ds_tabn_in)]]
    station_file$data_summary <- rbind(station_file$data_summary,
      new_data$data_summary)[order(table_name, start_dt, end_dt)]
  }
  new_data_cm <- names(station_file)[names(station_file) %in% names(new_data)]
  new_data_cm <- new_data_cm[new_data_cm %ilike% "raw_"]
  tn <- substr(new_data_cm, nchar("raw_") + 1, nchar(new_data_cm))
  station_file$data_summary$dsid <- seq_len(nrow(station_file$data_summary))
  new_data$data_summary$dsid <- seq_len(nrow(new_data$data_summary))
  dss <- station_file$data_summary[table_name == tn]
  dss <- dss[, c("dsid", "start_dt", "end_dt"), with = FALSE]
  dsn <- new_data$data_summary[table_name == tn]
  dsn <- dsn[, c("dsid", "start_dt", "end_dt"), with = FALSE]
  data.table::setkey(dss, start_dt, end_dt)
  data.table::setkey(dsn, start_dt, end_dt)
  station_file[[new_data_cm]]$d1 <- station_file[[new_data_cm]]$date_time
  station_file[[new_data_cm]]$d2 <- station_file[[new_data_cm]]$date_time
  new_data[[new_data_cm]]$d1 <- new_data[[new_data_cm]]$date_time
  new_data[[new_data_cm]]$d2 <- new_data[[new_data_cm]]$date_time
  data.table::setkey(station_file[[new_data_cm]], d1, d2)
  data.table::setkey(new_data[[new_data_cm]], d1, d2)
  station_file[[new_data_cm]] <- data.table::foverlaps(dss,
    station_file[[new_data_cm]], mult = "all", type = "any")
  station_file[[new_data_cm]] <- station_file[[new_data_cm]][,
    -c("start_dt", "end_dt"), with = FALSE]
  new_data[[new_data_cm]] <- data.table::foverlaps(dsn,
    new_data[[new_data_cm]], mult = "all", type = "any")
  new_data[[new_data_cm]] <- new_data[[new_data_cm]][,
    -c("start_dt", "end_dt"), with = FALSE]
  # make a list of data for each old and new phen and join phen info
  # this allows independent phenomena assessments
  dold_l <- lapply(seq_along(phens_old), function(i) {
    dold <- station_file[[new_data_cm]][
      , c("id", "dsid", "date_time", phens_old[i], "d1", "d2"), with = FALSE]
    data.table::setkey(dold, d1, d2)
    station_file$phens$phid <- seq_len(nrow(station_file$phens))
    st_phens <- station_file$phens[,
      c("phid", "phen_name", "start_dt", "end_dt"), with = FALSE]
    data.table::setkey(st_phens, start_dt, end_dt)
    dtp <- data.table::foverlaps(st_phens[phen_name == phens_old[i]], dold,
      mult = "all", type = "any")
    dtp <- dtp[, -c("start_dt", "end_dt", "phen_name")]
    return(dtp)
  })
  dnew_l <- lapply(seq_along(phens_new), function(i) {
    dnew <- new_data[[new_data_cm]][
      , c("id", "dsid", "date_time", phens_new[i], "d1", "d2"), with = FALSE]
    data.table::setkey(dnew, d1, d2)
    new_data$phens$phid <- seq_len(nrow(new_data$phens))
    nt_phens <- new_data$phens[,
      c("phid", "phen_name", "start_dt", "end_dt"), with = FALSE]
    data.table::setkey(nt_phens, start_dt, end_dt)
    dtp <- data.table::foverlaps(nt_phens[phen_name == phens_new[i]], dnew,
      mult = "all", type = "any")
    dtp <- dtp[, -c("start_dt", "end_dt", "phen_name")]
    return(dtp)
  })
  # trim the data by dates determined by overwrite_old
  s <- station_file[[new_data_cm]][, "date_time"]
  n <- new_data[[new_data_cm]][, "date_time"]
  s_min <- min(s$date_time)
  s_max <- max(s$date_time)
  n_min <- min(n$date_time)
  n_max <- min(n$date_time)
  s$new <- FALSE
  n$new <- TRUE
  if (overwrite_old) {
    s <- s[date_time < n_min | date_time > n_max]
  } else {
    n <- n[date_time > s_max | date_time < s_min]
  }
  sn <- rbind(s, n)
  s <- station_file[[new_data_cm]][["date_time"]]
  n <- new_data[[new_data_cm]][["date_time"]]
  # check if there have been any dates excluded for some reason
  if (any(!s %in% sn$date_time) || any(!n %in% sn$date_time)) {
    warning("There were some unique dates excluded")
    message("Check the appending process and overwrite logic")
  }

  # use sn to get all phenomena back into a table whilst updating
  # the start and end dates of the phens and data_summary
  dold_l <- lapply(dold_l, function(z) {
    pdt <- data.table::merge.data.table(
      x = sn[new == FALSE], y = z, by = "date_time"
    )
    return(pdt)
  })
  dnew_l <- lapply(dnew_l, function(z) {
    pdt <- data.table::merge.data.table(
      x = sn[new == TRUE], y = z, by = "date_time"
    )
    return(pdt)
  })
  # update the old and new data summaries, only need one of
  # dold_l and dnew_l to do this
  dold_l[[1]]
  
  # trim the data as per the overwrite_old param
    # and join to data summaries
    data.table::setkey(dss, start_dt, end_dt)
    data.table::setkey(dsn, start_dt, end_dt)
    s <- station_file[[x]]
    n <- station_file[[x]]
    s_min <- min(s$date_time)
    s_max <- max(s$date_time)
    n_min <- min(n$date_time)
    n_max <- max(n$date_time)
    if (overwrite_old) {
      so <- s[date_time >= n_min | date_time <= n_max]
      s <- s[date_time < n_min | date_time > n_max]
    } else {
      no <- n[date_time <= s_max | date_time >= s_min]
      n <- n[date_time > s_max | date_time < s_min]
    }
    # what about unique values that may be overwritten ...
    

    s$d1 <- s$date_time
    s$d2 <- s$date_time
    o$d1 <- s$date_time
    o$d2 <- s$date_time
    data.table::setkey(s, d1, d2)
    data.table::setkey(o, d1, d2)



  # })
  # if (paste0("raw_data_", ds_tabn_in) %in% names(station_file)) {
    
  # }


  # join import filenames to tip data --- will be used to generate a new
  # data summary
  station_file$data_summary$dsid <- seq_len(nrow(station_file$data_summary))
  new_data$data_summary$dsid <- seq_len(nrow(new_data$data_summary)) +
    max(station_file$data_summary$dsid)

  # need to add the logger interference table in here so that the
  # start and end dates are reflected accurately
  station_file$met_data$dumm_end <- station_file$met_data$date_time
  new_data$met_data$dumm_end <- new_data$met_data$date_time
  data.table::setkey(station_file$met_data, date_time, dumm_end)
  data.table::setkey(new_data$met_data, date_time, dumm_end)
  data.table::setkey(station_file$data_summary, start_dt, end_dt)
  data.table::setkey(new_data$data_summary, start_dt, end_dt)

  station_file$met_data <- data.table::foverlaps(
    y = station_file$data_summary[, c("start_dt", "end_dt", "dsid")],
    x = station_file$met_data,
    mult = "all", nomatch = NA
  )
  new_data$met_data <- data.table::foverlaps(
    y = new_data$data_summary[, c("start_dt", "end_dt", "dsid")],
    x = new_data$met_data,
    mult = "all", nomatch = NA
  )

  # # determine the start and end dates for which to append then append tables
  # ## need to also do the case for data being appended from a time before the
  # # station data!
  # catch all for appending other tables, new and old
  common_tables <- names(station_file)[names(station_file)
    %in% names(new_data)]
  common_tables <- common_tables[!common_tables %in%
    c("data_summary", "logg_interfere")]
  common_tables_ndt <- sapply(common_tables, function(x) !"date_time" %in%
    names(station_file[[x]]))
  common_tables_ndt <- common_tables[unlist(common_tables_ndt)]
  common_tables <- common_tables[!common_tables %in%
    common_tables_ndt]
  for (i in seq_along(common_tables)) {
    if (!any(nrow(station_file[[common_tables[i]]]) == 0,
      nrow(new_data[[common_tables[i]]]) == 0)) {
        st_org <- min(station_file[[common_tables[i]]]$date_time)
        ed_org <- max(station_file[[common_tables[i]]]$date_time)
        st_new <- min(new_data[[common_tables[i]]]$date_time)
        ed_new <- max(new_data[[common_tables[i]]]$date_time)
        # 
        if (!overwrite_old) { # don't overwrite old data
          dta0 <- station_file[[common_tables[i]]]
          dta1 <- new_data[[common_tables[i]]][
            date_time > ed_org |
            date_time < st_org]
          # account for unique data not included in old data
          unique_dt <- new_data[[common_tables[i]]][
            date_time < ed_org |
            date_time > st_org][order(date_time)]
          unique_dt <- unique_dt[!date_time %in% dta0$date_time]
          dta0 <- rbind(dta0, unique_dt)
          dta0 <- dta0[order(date_time)]
        } else { # can overwrite old data
          dta0 <- station_file[[common_tables[i]]][
            date_time < st_new | date_time > ed_new]
          dta1 <- new_data[[common_tables[i]]]
          # account for unique data not included in new data
          unique_dt <- station_file[[common_tables[i]]][
            date_time > st_new | date_time < ed_new]
          unique_dt <- unique_dt[!date_time %in% dta1$date_time]
          dta0 <- rbind(dta0, unique_dt)
          dta0 <- dta0[order(date_time)]
        }
      }
    station_file[[common_tables[i]]] <- rbind(dta0, dta1, fill = TRUE)
    station_file[[common_tables[i]]] <-
      unique(station_file[[common_tables[i]]])
    station_file[[common_tables[i]]] <- station_file[[common_tables[i]]][
      order(date_time), ]
  }
  for (i in seq_along(common_tables_ndt)) {
    if (!any(nrow(station_file[[common_tables_ndt[i]]]) == 0,
      nrow(new_data[[common_tables_ndt[i]]]) == 0)) {
        station_file[[common_tables_ndt[i]]] <- rbind(
          common_tables_ndt[i], common_tables_ndt[i], fill = TRUE)
        station_file[[common_tables_ndt[i]]] <-
          unique(station_file[[common_tables_ndt[i]]])
        if ("start_dt" %in% names(station_file[[common_tables_ndt[i]]]) &
          "end_dt" %in% names(station_file[[common_tables_ndt[i]]])) {
            station_file[[common_tables_ndt[i]]][order(start_dt, end_dt)]
        } else {
          station_file[[common_tables_ndt[i]]] <-
            setorder(station_file[[common_tables_ndt[i]]],
              cols = names(station_file[[common_tables_ndt[i]]])[1])
        }
    }
  }

  # data_summary
  summr <- rbind(station_file$data_summary, new_data$data_summary)
  ds <- merge(x = summr,
    y = station_file$met_data[, c("date_time", "dsid")], by = "dsid")
  ds <- ds[order(date_time), ]
  if (nrow(ds) > 1) {
    ds$intv <- c(NA,
      ifelse(ds[2:nrow(ds), ]$dsid != ds[1:(nrow(ds) - 1), ]$dsid, 1, NA))
  } else ds$intv <- NA
  ds$intv <- findInterval(ds$date_time, ds[intv == 1]$date_time)
  ds <- split(ds, by = "intv")
  ds <- lapply(ds, function(x) {
    x$start_dt <- min(x$date_time)
    x$end_dt <- max(x$date_time)
    return(x)
  })
  ds <- data.table::rbindlist(ds)
  ds <- ds[order(date_time), ][, -c("dsid", "date_time", "intv")]
  ds <- unique(ds)
  ds <- ds[!start_dt == end_dt]
  station_file$data_summary <- ds

  # logger interference table
  station_file$logg_interfere <- station_file$met_data[is.na(cumm_rain),
    c("id", "date_time", "detached", "attached", "host", "file_end")][
      order(date_time), ]
  station_file$logg_interfere$nas <- data.table::fifelse(
    is.na(station_file$logg_interfere$detached) &
    is.na(station_file$logg_interfere$attached) &
    is.na(station_file$logg_interfere$host) &
    is.na(station_file$logg_interfere$file_end), TRUE, FALSE
  )
  station_file$logg_interfere <- unique(station_file$logg_interfere)
  station_file$logg_interfere$dup1 <-
    duplicated(station_file$logg_interfere, by = c("date_time"))
  station_file$logg_interfere$dup2 <-
    duplicated(station_file$logg_interfere, by = c("date_time"),
      fromLast = TRUE)
  station_file$logg_interfere$dups <- data.table::fifelse(
    station_file$logg_interfere$dup1 == TRUE |
      station_file$logg_interfere$dup2 == TRUE, TRUE, FALSE
  )
  station_file$logg_interfere <- station_file$logg_interfere[
    !which(station_file$logg_interfere$nas == TRUE &
      station_file$logg_interfere$dups == TRUE), ][order(date_time)][
        , -c("nas", "dup1", "dup2", "dups")]

  #logger tip data
  not_met_data_names <- names(station_file$met_data)[
    !names(station_file$met_data) %in% c(
    "start_dt", "end_dt", "dsid", "detached", "attached", "host",
    "file_end", "dumm_end")]
  station_file$met_data <- station_file$met_data[!is.na(cumm_rain),
    ..not_met_data_names][order(date_time)]
  # remove consequtive same value cumm_rain rows
  station_file$met_data$test <- c(FALSE,
    station_file$met_data$cumm_rain[2:nrow(station_file$met_data)] ==
      station_file$met_data$cumm_rain[1:(nrow(station_file$met_data) - 1)])
  station_file$met_data <-
    station_file$met_data[test == FALSE][, -c("test"), with = FALSE]

  new_tables <- names(new_data)[!names(new_data) %in% names(station_file)]
  for (i in seq_along(new_tables)) {
    station_file[[new_tables[i]]] <- new_data[[new_tables[i]]]
  }
  return(station_file)
}
