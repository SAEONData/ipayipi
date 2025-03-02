#' @title plot hampel filter run results
#' @export
plotdy_cleanr <- function(
  input_dir = ".",
  phen_name = NULL,
  phen_units = "",
  outliers = TRUE,
  output_dir = NULL,
  wanted = NULL,
  unwanted = NULL,
  tbl_names = NULL,
  gaps = TRUE,
  mn = NULL,
  mx = NULL,
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
  "date_time" <- "original_v" <- "replace_v" <- "col1" <- "col2" <- NULL

  # get and organise data
  dta <- dta_flat_pull(input_dir = input_dir, phen_name = phen_name,
    wanted = wanted, unwanted = unwanted, tbl_names = tbl_names,
    gaps = gaps, pipe_house = pipe_house, xtra_v = xtra_v
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
  sn <- names(dtf)
  dtf <- do.call(cbind, dtf)
  names(dtf) <- sub(".*['.']", "", names(dtf))

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
  # now some plotting
  x <- xts::as.xts(
    dt, order.by = "date_time", xts_check_TZ = FALSE
  )
  p <- dygraphs::dyRangeSelector(
    dygraphs::dygraph(x, xlab = "Time", ylab = phen_name)
  )
  p <- dygraphs::dyGroup(p, name = sn,
    color = viridisLite::mako(length(sn), begin = 0.2, end = 0.8)
  )
  if (outliers) {
    p <- dygraphs::dyGroup(p, name = paste0(sn, "_flt_ogr"),
      color = viridisLite::mako(length(sn), begin = 0.2, end = 0.8),
      drawPoints = TRUE, strokeWidth = 0,
      pointSize = 3
    )
    # p <- dygraphs::dyGroup(p, name = paste0(sn, "_flt_rep"),
    #   color = viridisLite::mako(length(sn), begin = 0.2, end = 0.8),
    #   drawPoints = TRUE, strokeWidth = 0, fillGraph = TRUE,
    #   pointSize = 3
    # )
    p <- dygraphs::dyOptions(p, connectSeparatedPoints = FALSE,
      drawGapEdgePoints = FALSE, pointShape = "ex"
    )
  }
  return(p)
}