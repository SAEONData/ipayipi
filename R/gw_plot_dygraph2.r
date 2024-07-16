#' @title Visualisation of ground water corrections with interactive widgets.
#' @description This function uses the html based `dygraphs' package to
#'  generate interactive graphs that can be included in markdown documents.
#'  There are several options provided for plotting transformed groundwater
#'  data.
#' @details Plotting is optimized for evaluating transformed groundwater data
#'  processed using `ipayipi`. The transformed data should be compared to the
#'  `drift-neutral` data to visually assess the effectiveness of data
#'  processing. Drift-netral data has not been drift corrected, and therefore,
#'  indicates the effectiveness of barometric compensation.
#' @param file A level file object read into R.
#' @param vis_type Integer options 1, 2, 3, or 4. 1 - plots only the drift
#'  corrected water level; 2 - plots only drift neutral time series with
#'  outliers; 3 - combination of 1 and 2. 4 - plots barologger data alongside
#'  drift neutral series; cannot include rainfall (`rain_data` must be set to
#'  NULL, and conduct to FALSE).
#' @param dippr If TRUE this add dipper readings to the graphs.
#' @param conduct Logical. If `TRUE` conductivity data (if present) will be
#'  plotted on the secondary axis.
#' @param group use a common string to syncronize the a-axes of separate graphs.
#' @param show_events Show when logger data was downloaded.
#' @param rain_data A list with three elements. If provided the
#'  value will be passed to `ipayipi::rain_aggs()` --- see documentation
#'  --- and the rainfall data  will be included on the graphs secondary axis.
#'  If this argument is provided it will override display of elecroconductivity
#'  on the secondary axis. The first element must contain the aggregation period
#'  and the second a search keyword that will be used to select a rainfall
#'  station.
#' @author Paul J. Gordijn
#' @keywords Ground water visualisation, graph,
#' @export
gw_vis_dy2 <- function(
  input_dir = ".",
  wanted = NULL,
  unwanted = NULL,
  rain_data = NULL,
  recurr = FALSE,
  prompt = FALSE,
  ...
  ) {

  # summarise station data
  dta <- ipayipi::dta_flat_pull(input_dir = input_dir, file_ext = ".rds",
    tab_name = "log_t", phen_name = "t_level_m", prompt = prompt,
    recurr = recurr, wanted = wanted, unwanted = unwanted)

  if (!is.null(rain_data)) {
    rn  <- ipayipi::rain_plot_aggs(agg = rain_data$agg,
      input_dir = rain_data$rain_dir, wanted =
      rain_data$search_patterns, ignore_nas = TRUE)
    rn <- rn$plot_data
    rn$date_time <- as.POSIXct(rn$date_time, tz =
      attr(dta$dta$date_time, "tzone"))
    rn <- rn[date_time >= max(dta$dta$date_time) |
      date_time <= max(dta$dta$date_time)]
    rn$dt2 <- rn$date_time
    data.table::setkey(rn, date_time, dt2)
    dta$dta$dtt2 <- dta$dta$date_time
    data.table::setkey(dta$dta, date_time, dtt2)
    dta1 <- data.table::foverlaps(dta$dta, rn, mult = "first",
      nomatch = NA)
    dta1$int <- ifelse(!is.na(dta1$rain_mm), 1, NA)
    l <- nrow(dta1[int == 1])
    dta1[int == 1]$int <- seq_len(l)
    dta1[nrow(dta1), "int"] <- l + 1
    dta1[1, "int"] <- 0
    int_seq <- dta1[!is.na(int)]$int
    rs <- lapply(seq_along(int_seq), function(x) {
      if (x > 1) {
        r1 <- which(dta1$int == (int_seq[x] - 1))
        r2 <- which(dta1$int == (int_seq[x])) - 1
        rs <- dta1[r1:r2, ]
        rs$rain_mm <- sum(rs$rain_mm, na.rm = TRUE)
      } else {
        rs <- dta1[0, ]
      }
      invisible(rs)
    })
    dta1 <- data.table::rbindlist(rs)

    dta$dta <- dta1[, c("dtt2", "rain_mm", gsub(".rds", "", dta$stations)),
      with = FALSE]
    data.table::setnames(dta$dta, old = "dtt2", new = "date_time")
  } else {
    dta$dta$rain_mm  <- NA
  }

  ## convert d_main to xts format
  x <- xts::xts(dta$dta[, -"date_time", with = FALSE],
    order.by = dta$dta$date_time)

  p <- dygraphs::dyRangeSelector(dygraphs::dygraph(x,
      xlab = "Time", ylab = "Water level (m a.s.l.)"))
  p <- dygraphs::dySeries(p, name = names(x)[!names(x) %in% "rain_mm"])
  p <- dygraphs::dySeries(p, name = "rain_mm",
    color = "#5d5a50ab", axis = "y2", stepPlot = TRUE,
      fillGraph = TRUE)
  p <- dygraphs::dyAxis(p, "y2", label = paste0("Rainfall (mm)"))
  p <- dygraphs::dyOptions(p, fillAlpha = 0.4)
  p <- dygraphs::dyOptions(p, connectSeparatedPoints = FALSE,
    drawGapEdgePoints = FALSE)
  cols <- viridisLite::mako((ncol(x) - 1))
  p <- dygraphs::dyOptions(p, colors = cols)

  return(list(p = p, dta = dta$dta, cols = cols))
}
