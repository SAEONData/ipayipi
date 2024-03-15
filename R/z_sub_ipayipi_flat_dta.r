#' @title Pulls multiple station data into a single file
#' @description Summarises data by the longest time interval for a particular
#'  phenomena wihtin the specified data table in an `ipayipi` station object.
#'  The function can be used to summarise time-series data from a number of
#'  stations and the output saved to a csv file.
#' @param input_dir The directory in which to search for stations form which
#'  to extract weather data.
#' @param file_ext The extension of the stations from where data will be
#'  extracted.
#' @param tab_name Vector of table names in a station files (list object) from which to extract data. Add items to the vector such that only one table per station is selected. Table names are selected via the `%in%` argument.
#' @param phen_name The field/column/name of the phenomena which to join into
#'  a single data table.
#' @param ri Record interval string. This function will guess the record interval used to generate a flat table. However, the guess may not be possible if there is insufficient data.
#' @param output_dir The output directory where an output csv file is saved.
#' @param prompt Logical. If `TRUE` a prompt will be called so that the user
#'  can interactively select station files.
#' @param wanted A strong containing keywords to use to filter which stations
#'  are selected for processing. Multiple search kewords should be seperated
#'  with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted
#'  stations.
#' @param out_csv Logical. If TRUE a csv file is exported to the output
#'  directory.
#' @param out_csv_preffix Preffix for the output csv file. The phenomena name
#'  and then time interval by which the data are summarised are used as a
#'  suffix.
#' @keywords data pipeline; summarise time-series data;
#' @return A list containing 1) the summarised data in a single data.table, 2)
#'  a character string representing the time interval by which the data has
#'  been summarised, 3) the list of stations used for the summary, 4) the name
#'  of the table and the name of the field for which data was summarised.
#' @author Paul J. Gordijn
#' @export
ipayipi_flat_dta <- function(
  pipe_house = NULL,
  file_ext = ".ipi",
  tab_names = NULL,
  phen_name = NULL,
  ri = NULL,
  output_dir = NULL,
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  out_csv = FALSE,
  out_tab_name = NULL,
  out_csv_preffix = "",
  recurr = FALSE,
  ...) {
    "dt1" <- "dt2" <- "dt3" <- "dt4" <- "..cols_inc" <- "%ilike%" <- NULL
  # merge data sets into a station for given time periods
  slist <- ipayipi::dta_list(input_dir = pipe_house$ipip_room,
    file_ext = file_ext, prompt = prompt, recurr = recurr,
    unwanted = unwanted, wanted = wanted)
  # extract all relevant tables from the data
  t <- lapply(slist, function(x) {
    m <- readRDS(file.path(pipe_house$ipip_room, x))
    tab_name <- names(m)[names(m) %in% tab_names][1]
    t <- m[[tab_name]]
    cols_inc <- names(t)[names(t) %like% "date_time|Date_time"]
    cols_inc <- c(cols_inc, names(t)[names(t) %in% phen_name])
    t <- t[, c(cols_inc), with = FALSE]
    invisible(t)
  })
  names(t) <- gsub(pattern = file_ext, replacement = "", x = slist)
  # check the time intervals of each station
  ti <- lapply(t, function(x) {
    dt_name <- names(x)[names(x) %like% "date_time|Date_time"]
    ti <- ipayipi::record_interval_eval(dt = x[][[dt_name]],
      dta_in = x, remove_prompt = FALSE)
    if (!is.null(ri)) ti$record_interval <- ri
    dt_x <- data.table::data.table(
      ti = ti$record_interval_difftime,
      ti_chr = ti$record_interval,
      dt_min = min(x[][[dt_name]], na.rm = TRUE),
      dt_max = max(x[][[dt_name]], na.rm = TRUE)
    )
    if (dt_x$ti %in% "discnt") dt_x$ti <- lubridate::as.difftime(ri)
    invisible(dt_x)
  })
  ti <- data.table::rbindlist(ti)
  tii <- max(ti$ti, na.rm = TRUE)
  ti_chr <- ti[ti == tii]$ti_chr[1]
  tii <- lubridate::as.period(ti_chr)
  dt_min <- min(ti$dt_min, na.rm = TRUE)
  dt_max <- max(ti$dt_max, na.rm = TRUE)
  seq_dt <- seq(from = dt_min, to = dt_max, by = gsub("_", " ", ti_chr))
  dt_seq <- data.table::data.table(
    dt = seq_dt,
    dt1 = seq_dt - 0.499 * lubridate::as.duration(tii),
    dt2 = seq_dt + 0.499 * lubridate::as.duration(tii))
  # join the data to the date sequence
  tx <- lapply(seq_along(t), function(i) {
    data.table::setkey(dt_seq, dt1, dt2)
    dt_name <- names(t[[i]])[names(t[[i]]) %like% "date_time|Date_time"]
    data.table::setnames(t[[i]], old = dt_name, new = "dt3")
    t[[i]][, "dt4"] <- t[[i]][, "dt3"]
    data.table::setkey(t[[i]], dt3, dt4)
    tseq <- data.table::foverlaps(x = dt_seq, y = t[[i]], mult = "first",
      type = "any")
    tseq <- subset(tseq, select = phen_name)
    data.table::setnames(tseq, old = phen_name, new = names(t[i]))
    return(tseq)
  })
  # make a 'flat file'
  tflat <- do.call("cbind", tx)
  dta <- cbind(dt_seq[, c("dt")], tflat)
  data.table::setnames(dta, old = "dt", new = "date_time")
  tii <- ipayipi::record_interval_eval(dt = dta$date_time,
    dta_in = dta)
  tii <- tii$record_interval
  if (is.null(out_tab_name)) out_tab_name <- ti_chr
  dta <- list(dta = dta, time_interval = ti_chr, stations = slist,
    tab_names = tab_names, phen_name = phen_name)
  if (out_csv) {# save the csv file
    data.table::fwrite(dta$dta, file = file.path(output_dir, paste0(
        out_csv_preffix, "_", out_tab_name, "_", phen_name, ".csv")),
      na = NA, dateTimeAs = "write.csv")
  }
  return(dta)
}