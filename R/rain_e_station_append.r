#' @title Append hobo rainfall station data
#' @description Appends two standardised hobo rainfall files.
#' @param hobo_station The main data set to which data will be appended.
#' @param new_data The data that will be appended to the main 'hobo station'.
#' @param overwrite_old Logical. Defaults to FALSE so that data from the
#'  `hobo_station` is not over writed by the `new_data`.
#' @author Paul J. Gordijn
#' @keywords hobo rainfall; data pipeline; append data;
#' @details
#'  Special attention is given to the `data summary` table of
#'  `SAEON_rain_data` objects. The data summary contains information
#'  which is used in the pipeline to determine which files are appended to
#'  another. Specifically, the standardised ptitle in the data summary is the
#'  identifier for stations, and records in the `nomvet_name` field are used to
#'  determine which files have been included in any given `SAEON_rain_data`
#'  object.
#' @export
rain_station_append <- function(
  rain_station = NULL,
  new_data = NULL,
  overwrite_old = FALSE,
  ...
) {
  # if data has been standardised continue
  if (class(rain_station) == "SAEON_rainfall_data" &
    class(new_data) == "SAEON_rainfall_data") {

    # join import filenames to tip data --- will be used to generate a new
    # data summary
    rain_station$data_summary$dsid <- seq_len(nrow(rain_station$data_summary))
    new_data$data_summary$dsid <- seq_len(nrow(new_data$data_summary)) +
      max(rain_station$data_summary$dsid)

    # need to add the logger interference table in here so that the
    # start and end dates are reflected accurately
    rain_station$tip_data <- rbind(
      rain_station$tip_data, rain_station$logg_interfere, fill = TRUE)
    new_data$tip_data <- rbind(
      new_data$tip_data, new_data$logg_interfere, fill = TRUE)
    rain_station$tip_data$dumm_end <- rain_station$tip_data$date_time
    new_data$tip_data$dumm_end <- new_data$tip_data$date_time
    data.table::setkey(rain_station$tip_data, date_time, dumm_end)
    data.table::setkey(new_data$tip_data, date_time, dumm_end)
    data.table::setkey(rain_station$data_summary, start_dt, end_dt)
    data.table::setkey(new_data$data_summary, start_dt, end_dt)

    rain_station$tip_data <- data.table::foverlaps(
      y = rain_station$data_summary[, c("start_dt", "end_dt", "dsid")],
      x = rain_station$tip_data,
      mult = "all", nomatch = NA
    )
    new_data$tip_data <- data.table::foverlaps(
      y = new_data$data_summary[, c("start_dt", "end_dt", "dsid")],
      x = new_data$tip_data,
      mult = "all", nomatch = NA
    )

    # # determine the start and end dates for which to append then append tables
    # ## need to also do the case for data being appended from a time before the
    # # station data!
    # catch all for appending other tables, new and old
    common_tables <- names(rain_station)[names(rain_station)
      %in% names(new_data)]
    common_tables <- common_tables[!common_tables %in%
      c("data_summary", "logg_interfere")]
    common_tables_ndt <- sapply(common_tables, function(x) !"date_time" %in%
      names(rain_station[[x]]))
    common_tables_ndt <- common_tables[unlist(common_tables_ndt)]
    common_tables <- common_tables[!common_tables %in%
      common_tables_ndt]
    for (i in seq_along(common_tables)) {
      if (!any(nrow(rain_station[[common_tables[i]]]) == 0,
        nrow(new_data[[common_tables[i]]]) == 0)) {
          st_org <- min(rain_station[[common_tables[i]]]$date_time)
          ed_org <- max(rain_station[[common_tables[i]]]$date_time)
          st_new <- min(new_data[[common_tables[i]]]$date_time)
          ed_new <- max(new_data[[common_tables[i]]]$date_time)
          if (!overwrite_old) { # don't overwrite old data
            dta0 <- rain_station[[common_tables[i]]]
            dta1 <- new_data[[common_tables[i]]][
              date_time > ed_org |
              date_time < st_org]
            # account for unique data not included in old data
            unique_dt <- new_data[[common_tables[i]]][
              date_time < ed_org |
              date_time > st_new][order(date_time)]
            unique_dt <- unique_dt[!as.character(unique_dt$date_time) %in%
              as.character(dta0$date_time)]
            unique_dt <- unique_dt[
              !as.character(unique_dt$date_time) %in%
                as.character(dta1$date_time)]
            dta0 <- rbind(dta0, unique_dt, fill = TRUE)
            dta0 <- dta0[order(date_time)]
            if (common_tables[i] %in% c("tip_data")) {
              # sort out tip data
              # need to remove logg_interfere duplicates with NA values
              dta0$dup <- ifelse(
                duplicated(dta0$date_time, fromLast = FALSE) == TRUE |
                  duplicated(dta0$date_time, fromLast = TRUE) == TRUE,
                    TRUE, FALSE)
              dta0$nas <- ifelse(is.na(dta0$cumm_rain) &
                is.na(dta0$attached) & is.na(dta0$host) &
                is.na(dta0$file_end) &
                  is.na(dta0$cumm_rain), TRUE, FALSE)
              dta0 <- dta0[!(dup == TRUE & nas == TRUE)]
              dta0$dup <- ifelse(duplicated(as.character(dta0$date_time),
                fromLast = FALSE) == TRUE |
                  duplicated(as.character(dta0$date_time),
                    fromLast = TRUE) == TRUE, TRUE, FALSE)
              dta0_dups <- dta0[dup == TRUE]
              dta0_dups <- split.data.frame(dta0_dups, f = dta0_dups$date_time)
              dts <- lapply(seq_along(dta0_dups), function(d) {
                dts <- dta0_dups[[d]]
                while (nrow(dts) > 1) {
                  dts$min_dsid <- min(dts$dsid)
                  lgg <- c("detached", "attached", "host", "file_end")
                  dts[, lgg := paste(detached, attached, host, file_end)]
                  dts[, lgg := lgg %ilike% "logged"]
                  if (nrow(dts) > 1 & any(dts$lgg)) {
                    dts <- dts[lgg == TRUE]
                  }
                  if (nrow(dts) > 1) {
                    dts <- dts[min_dsid == dsid]
                  }
                  if (nrow(dts) > 1) {
                    dts <- dts[1, ]
                  }
                }
                invisible(dts)
              })
              dta0 <- rbind(dta0, data.table::rbindlist(dts, fill = TRUE),
                fill = TRUE)
              dta0 <- dta0[, !(names(dta0) %in%
                  c("min_dsid", "lgg", "dup", "nas")), with = FALSE][
                    order(date_time)]
            }
          } else { # can overwrite old data
            dta0 <- rain_station[[common_tables[i]]][
              date_time < st_new | date_time > ed_new]
            dta1 <- new_data[[common_tables[i]]]
            # account for unique data not included in new data
            unique_dt <- rain_station[[common_tables[i]]][
              date_time > st_new | date_time < ed_new]
            unique_dt <- unique_dt[!date_time %in% dta1$date_time]
            dta0 <- rbind(dta0, unique_dt)
            dta0 <- dta0[order(date_time)]
          }
        }
      rain_station[[common_tables[i]]] <- rbind(dta0, dta1, fill = TRUE)
      rain_station[[common_tables[i]]] <-
        unique(rain_station[[common_tables[i]]])
      rain_station[[common_tables[i]]] <- rain_station[[common_tables[i]]][
        order(date_time), ]
    }
    for (i in seq_along(common_tables_ndt)) {
      if (!any(nrow(rain_station[[common_tables_ndt[i]]]) == 0,
        nrow(new_data[[common_tables_ndt[i]]]) == 0)) {
          rain_station[[common_tables_ndt[i]]] <- rbind(
            common_tables_ndt[i], common_tables_ndt[i], fill = TRUE)
          rain_station[[common_tables_ndt[i]]] <-
            unique(rain_station[[common_tables_ndt[i]]])
          if ("start_dt" %in% names(rain_station[[common_tables_ndt[i]]]) &
            "end_dt" %in% names(rain_station[[common_tables_ndt[i]]])) {
              rain_station[[common_tables_ndt[i]]][order(start_dt, end_dt)]
          } else {
            rain_station[[common_tables_ndt[i]]] <-
              setorder(rain_station[[common_tables_ndt[i]]],
                cols = names(rain_station[[common_tables_ndt[i]]])[1])
          }
      }
    }

    # data_summary
    summr <- rbind(rain_station$data_summary, new_data$data_summary)
    ds <- merge(x = summr,
      y = rain_station$tip_data[, c("date_time", "dsid")], by = "dsid")
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
    rain_station$data_summary <- ds

    # logger interference table
    rain_station$logg_interfere <- rain_station$tip_data[is.na(cumm_rain),
      c("id", "date_time", "detached", "attached", "host", "file_end")][
        order(date_time), ]
    rain_station$logg_interfere$nas <- data.table::fifelse(
      is.na(rain_station$logg_interfere$detached) &
      is.na(rain_station$logg_interfere$attached) &
      is.na(rain_station$logg_interfere$host) &
      is.na(rain_station$logg_interfere$file_end), TRUE, FALSE
    )
    rain_station$logg_interfere <- unique(rain_station$logg_interfere)
    rain_station$logg_interfere$dup1 <-
      duplicated(rain_station$logg_interfere, by = c("date_time"))
    rain_station$logg_interfere$dup2 <-
      duplicated(rain_station$logg_interfere, by = c("date_time"),
        fromLast = TRUE)
    rain_station$logg_interfere$dups <- data.table::fifelse(
      rain_station$logg_interfere$dup1 == TRUE |
        rain_station$logg_interfere$dup2 == TRUE, TRUE, FALSE
    )
    rain_station$logg_interfere <- rain_station$logg_interfere[
      !which(rain_station$logg_interfere$nas == TRUE &
        rain_station$logg_interfere$dups == TRUE), ][order(date_time)][
          , -c("nas", "dup1", "dup2", "dups")]

    #logger tip data
    not_tip_data_names <- names(rain_station$tip_data)[
      !names(rain_station$tip_data) %in% c(
      "start_dt", "end_dt", "dsid", "detached", "attached", "host",
      "file_end", "dumm_end")]
    rain_station$tip_data <- rain_station$tip_data[!is.na(cumm_rain),
      ..not_tip_data_names][order(date_time)]
    # remove consequtive same value cumm_rain rows
    rain_station$tip_data$test <- c(FALSE,
      rain_station$tip_data$cumm_rain[2:nrow(rain_station$tip_data)] ==
        rain_station$tip_data$cumm_rain[1:(nrow(rain_station$tip_data) - 1)])
    rain_station$tip_data <-
      rain_station$tip_data[test == FALSE][, -c("test"), with = FALSE]

    new_tables <- names(new_data)[!names(new_data) %in% names(rain_station)]
    for (i in seq_along(new_tables)) {
      rain_station[[new_tables[i]]] <- new_data[[new_tables[i]]]
    }
    return(rain_station)
  } else {
    return(message("Error: the input object class is incorrect!"))
  }
}
