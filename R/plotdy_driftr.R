#' @title plot hampel filter run results
#' @export
plotdy_driftr <- function(
  station_file = NULL,
  pipe_house = NULL,
  tbl_name = NULL,
  phen_key = NULL,
  mn = NULL,
  mx = NULL,
  slide = 1,
  dflt_nobs = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  ...
) {
  "%chin%" <- "%ilike%" <- "." <- ":=" <- NULL
  "date_time" <- "n" <- NULL
  # open station file connection
  sfc <- sf_open_con(pipe_house = pipe_house, station_file = station_file)
  sn <- names(sfc)
  if (!tbl_name %in% sn) {
    cli::cli_abort(c(
      "No station file with table named ({.var tbl_name}):",
      "\'{tbl_name}\' detected!"
    ))
  }

  dta_in <- sf_dta_read(sfc = sfc, tv = tbl_name)
  dto_in <- sf_dta_read(sfc = sfc, tv = paste0(tbl_name, "_fltr_vals"))

  # open data
  ppsij <- data.table::data.table(
    start_dttm = as.POSIXct(mn), end_dttm = as.POSIXct(mx)
  )
  dta_inf <- dt_dta_filter(dta_link = dta_in, ppsij = ppsij)
  dta <- dt_dta_open(dta_link = dta_inf[[1]])

  # open filter table
  dto_inf <- dt_dta_filter(dta_link = dto_in, ppsij = ppsij)
  dto <- dt_dta_open(dta_link = dto_inf[[1]])

  # setup data for dygraphs
  dto <- split.data.frame(dto, f = factor(dto$phen))
  dto <- lapply(seq_along(dto), function(i) {
    pn <- names(dto[i])
    data.table::setnames(dto[[i]], c("original_v", "replace_v"),
      c(paste0(pn, "_orgl"), paste0(pn, "_fltr"))
    )
    return(dto[[i]][, -c("phen"), with = FALSE])
  })

  # join dta and dto
  dtaj <- lapply(seq_along(dto), function(i) {
    dtoj <- dto[[i]][dta, on = .(date_time)]
    dtoj <- dtoj[, names(dtoj)[!names(dtoj) %in% names(dta)], with = FALSE]
    return(dtoj)
  })
  dtaj <- do.call(cbind, c(list(dta), dtaj))

  # convert data to xts
  # plot points with line and outliers as points
  if (is.null(dflt_nobs)) dflt_nobs <- nrow(dtaj)
  nobs <- ceiling(nrow(dtaj) / dflt_nobs)

  dtaj[, n := rep(seq_len(nobs), each = dflt_nobs)[seq_len(nrow(dtaj))]]
  data.table::setcolorder(dtaj,
    neworder = c("date_time", names(dtaj)[!names(dtaj) %chin% "date_time"])
  )

  dtaj[, dum_drift := as.numeric(dum_drift)][, dum_drift := data.table::fifelse(
    !is.na(dum_drift), drift_cal, dum_drift
  )]
  s <- c(
    "date_time", "drift_cal", "dum_drift", "cal_offset", phen_key,
    paste0(phen_key, "_raw"), paste0(phen_key, "_fltr"),
    paste0(phen_key, "_orgl")
  )
  s <- s[s %in% names(dtaj)]
  dtaj <- dtaj[, s, with = FALSE]
  # only retain numeric and posix columns for conversion to xts
  dtaj <- dtaj[, names(dtaj)[sapply(dtaj, function(x) {
    any(class(x) %ilike% "numeric|posix|integ|double")
  })], with = FALSE]
  if (all(names("cal_offset") %in% names(dtaj), phen_key %in% names(dtaj))) {
    dtaj[, xphen := phen_key - cal_offset,
      env = list(xphen = paste0(phen_key, "_uncorrected"), phen_key = phen_key)
    ]
  }
  x <- xts::as.xts(
    dtaj, order.by = "date_time", xts_check_TZ = FALSE
  )
  s <- s[!s %in% "date_time"]
  sf <- gsub("['.']ipip$", "", basename(station_file))
  # general plot
  p <- dygraphs::dyRangeSelector(
    dygraphs::dygraph(x[, s], xlab = "Time", ylab = phen_key, main = sf)
  )
  # main series -phen_key
  p <- dygraphs::dySeries(p, name = phen_key, color = "#1874CD")
  # drift calibration points
  if ("drift_cal" %in% names(x)) {
    p <- dygraphs::dySeries(p, name = "drift_cal", color = "#29060d",
      drawPoints = TRUE, strokeWidth = 0, pointShape = "ex",
      pointSize = 3
    )
  }
  # dummy drift indicators
  if ("dum_drift" %in% names(x)) {
    p <- dygraphs::dySeries(p, name = "dum_drift", color = "#155e1a",
      drawPoints = TRUE, strokeWidth = 0, pointShape = "ex",
      pointSize = 3
    )
  }
  # original raw data
  if (paste0(phen_key, "_uncorrected") %in% names(x)) {
    p <- dygraphs::dySeries(p, name = paste0(phen_key, "_raw"),
      color = "#755d08"
    )
  }
  # outliers
  if (paste0(phen_key, "_fltr") %in% names(x)) {
    p <- dygraphs::dySeries(p, name = paste0(phen_key, "_fltr"),
      color = "#07909a",
      drawPoints = TRUE, strokeWidth = 0, pointShape = "ex",
      pointSize = 3
    )
  }
  if (paste0(phen_key, "_orgl") %in% names(x)) {
    p <- dygraphs::dySeries(p, name = paste0(phen_key, "_orgl"),
      color = "#9a076e",
      drawPoints = TRUE, strokeWidth = 0, pointShape = "ex",
      pointSize = 3
    )
  }
  p <- dygraphs::dyOptions(p, connectSeparatedPoints = FALSE,
    drawGapEdgePoints = FALSE
  )
  return(p)
}