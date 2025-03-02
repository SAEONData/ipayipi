#' @title plot hampel filter run results in ggplot
#' @export
plotgg_chks <- function(
  input_dir = ".",
  output_dir = NULL,
  wanted = NULL,
  unwanted = NULL,
  tbl_names = NULL,
  start_dttm = NULL,
  end_dttm = NULL,
  slide = 1,
  dflt_nobs = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  station_file = NULL,
  pipe_house = NULL,
  ...
) {
  "%chin%" <- "%ilike%" <- "." <- ":=" <- NULL
  "date_time" <- "stnd_title" <- "original_v" <- "replace_v" <- "col1" <-
    "col1" <- "col2" <- NULL


  # get and organise data from single station
  
  dta <- dta_flat_pull(input_dir = input_dir, phen_name = phen_name,
    wanted = wanted, unwanted = unwanted, tbl_names = tbl_names,
    gaps = gaps, pipe_house = pipe_house, xtra_v = xtra_v,
    start_dttm = start_dttm, end_dttm = end_dttm
  )

  # join on filter values to multiple series
  dtf <- dta$cleanr_vals
  # remove duplicates
  dtf <- unique(dtf,
    by = c("date_time", "stnd_title", "table_name"), fromLast = TRUE
  )

  # split by station
  dtf <- split.data.frame(dtf, f = dtf$stnd_title)
  # join outlier info onto the main data
  dt <- dta$dta
  dtf <- lapply(dtf, function(x) {
    sn <- x$stnd_title[1]
    x <- x[, .(date_time, original_v, replace_v)]
    data.table::setnames(x, old = c("original_v", "replace_v"),
      new = c(paste0(sn, "_flt_ogr"), paste0(sn, "_flt_rep"))
    )
    x <- x[dt, on = "date_time", mult = "first"]
    x <- x[, names(x)[names(x) %ilike% "_flt_rep$|_flt_ogr$"], with = FALSE]
    return(x)
  })
  sn <- names(dta$dta)[!names(dta$dta) %chin% "date_time"]
  dtf <- do.call(cbind, dtf)
  if (!is.null(dtf)) names(dtf) <- sub(".*['.']", "", names(dtf))

  dt <- cbind(dta$dta, dtf)
  # generate dummy columns for filter values if not present
  dtdum <- lapply(sn, function(x) {
    r <- NULL
    if (!paste0(x, "_flt_ogr") %chin% names(dt)) {
      dt[, col := NA, env = list(col = paste0(x, "_flt_ogr"))]
      dt[, col := NA, env = list(col = paste0(x, "_flt_rep"))]
      r <- dt[, .(col1, col2), env = list(
        col1 = paste0(x, "_flt_ogr"), col2 = paste0(x, "_flt_rep")
      )]
    }
    return(r)
  })
  dtdum <- do.call(cbind, dtdum)
  dt <- cbind(dt, dtdum)
  if (!outliers) dt <- dt[, names(dt)[!names(dt) %ilike% "_flt_"], with = FALSE]

  # convert data to long format for ggplot
  mv <- names(dt)[!names(dt) %ilike% c("date_time|^id|_flt_")]
  dtd <- data.table::melt(dt, id.vars = "date_time", measure.vars = mv,
    value.name = phen_name, variable.name = "stnd_title"
  )

  p <- ggplot2::ggplot(dtd, ggplot2::aes(
    x = date_time, y = !!as.name(phen_name), colour = stnd_title
  )) +
    ggplot2::geom_line() +
    khroma::scale_colour_sunset(scale_name = "station", discrete = TRUE) +
    ggplot2::labs(x = "Date") +
    egg::theme_article()
  return(p)
}