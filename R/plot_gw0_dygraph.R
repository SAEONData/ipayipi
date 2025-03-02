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
gw_vis_dy <- function(
  file = NULL,
  vis_type = 1,
  dippr = FALSE,
  group = NULL,
  show_events = TRUE,
  conduct = FALSE,
  rain_data = NULL,
  ...
) {

  ## data need to be converted to xts format
  ## prepare data
  d_main <- file$log_t
  d_main$outliers <- ifelse(d_main$bt_Outlier == TRUE,
    d_main$bt_level_m + mean(d_main$Drift_offset_m, na.rm = TRUE), NA
  )
  d_main$drift_neutral <- d_main$t_bt_level_m +
    mean(d_main$Drift_offset_m, na.rm = TRUE)
  d_main$drift_neutral <- ifelse(is.na(d_main$t_bt_level_m),
    NA, d_main$drift_neutral
  )
  d_main$ut1_level_m <- d_main$ut1_level_m +
    (mean(d_main$ut1_level_m, na.rm = TRUE) -
        mean(d_main$t_level_m, na.rm = TRUE)
    )
  d_main  <- d_main[, c("Date_time", "ut1_level_m", "level_kpa", "outliers",
    "drift_neutral", "t_level_m"
  )]

  ## add dipper readings to d_main
  dips <- file$log_retrieve
  dtum <- file$log_datum[, c("Start", "End", "H_masl")]
  dips$dips_s <- dips$Date_time
  dips$dips_e <- dips$Date_time
  data.table::setkey(dips, dips_s, dips_e)
  data.table::setkey(dtum, Start, End)
  dips_j <- data.table::foverlaps(dips, dtum, mult = "first", nomatch = NA)
  dips_j$dip_read_m <- dips_j$Dipper_reading_m - dips_j$Casing_ht_m
  dips_j$manual_level_m <- dips_j$H_masl - dips_j$dip_read_m
  dips_j <- dips_j[, c("Date_time", "id", "manual_level_m")]
  d_main <- dips_j[d_main, on = "Date_time"]

  ## extract events based on when logger files were downloaded
  intv <- d_main$Date_time[2] - d_main$Date_time[1]
  dwd <- file$xle_FileInfo[, "Date_time"]
  colnames(dwd) <- "pull_t"
  dwd$s1 <- dwd$pull_t - intv / 2
  dwd$e1 <- dwd$pull_t + intv / 2
  d_main$s2 <- d_main$Date_time
  d_main$e2 <- d_main$Date_time
  data.table::setkey(dwd, s1, e1)
  data.table::setkey(d_main, s2, e2)
  ev <- data.table::foverlaps(dwd, d_main, mult = "first", nomatch = NA)
  ev <- ev[which(!is.na(ev$Date_time)), "Date_time"]

  ## extract and join additional logger env data to d_main
  ## uses a 'fuzzy' join based on the interval between data logging
  ec <- file$log_data[,
    c("Date_time", "temperature_degc", "conduct_mS_per_cm")
  ]
  ec$s3 <- ec$Date_time - intv / 2
  ec$e3 <- ec$Date_time + intv / 2
  data.table::setkey(ec, s3, e3)
  ec_tab <- data.table::foverlaps(d_main, ec,
    mult = "first", nomatch = NA
  )
  d_main1 <- ec_tab[, c("id", "Date_time", "ut1_level_m", "level_kpa",
    "temperature_degc", "conduct_mS_per_cm",
    "manual_level_m", "outliers", "drift_neutral", "t_level_m"
  )]
  d_main1$Date_time <- d_main$Date_time
  d_main <- d_main1

  if (!is.null(rain_data)) {
    rn <- rain_data[, c("date_time", "rain_tot"), with = FALSE]
    rn$date_time <- as.POSIXct(rn$date_time, tz =
        attr(d_main$Date_time, "tzone")
    )
    rn <- rn[date_time >= max(d_main$Date_time) |
        date_time <= max(d_main$Date_time)
    ]
    rn$dt2 <- rn$date_time
    data.table::setkey(rn, date_time, dt2)
    d_main$dtt2 <- d_main$Date_time
    data.table::setkey(d_main, Date_time, dtt2)
    d_main1 <- data.table::foverlaps(d_main, rn, mult = "first",
      nomatch = NA
    )
    d_main1$int <- ifelse(!is.na(d_main1$rain_tot), 1, NA)
    l <- nrow(d_main1[int == 1])
    d_main1[int == 1]$int <- seq_len(l)
    d_main1[nrow(d_main1), "int"] <- l + 1
    d_main1[1, "int"] <- 0
    int_seq <- d_main1[!is.na(int)]$int
    rs <- lapply(seq_along(int_seq), function(x) {
      if (x > 1) {
        r1 <- which(d_main1$int == (int_seq[x] - 1))
        r2 <- which(d_main1$int == (int_seq[x])) - 1
        rs <- d_main1[r1:r2, ]
        rs$rain_tot <- sum(rs$rain_tot, na.rm = TRUE)
      } else {
        rs <- d_main1[0, ]
      }
      invisible(rs)
    })
    d_main1 <- data.table::rbindlist(rs)

    d_main <- d_main1[, c("id", "Date_time", "ut1_level_m", "level_kpa",
      "temperature_degc", "conduct_mS_per_cm",
      "manual_level_m", "outliers", "drift_neutral", "t_level_m", "rain_tot"
    )]
  } else {
    d_main$rain_tot  <- NA
  }

  ## convert d_main to xts format
  x <- xts::xts(d_main[, c("id", "ut1_level_m", "level_kpa",
      "temperature_degc", "conduct_mS_per_cm",
      "manual_level_m", "outliers", "drift_neutral", "t_level_m", "rain_tot"
    )], order.by = d_main$Date_time
  )

  ## create graphs
  ## graph with only drift corrected data
  if (vis_type == 1) {
    seriez <- c("t_level_m")
    if (dippr == TRUE) seriez <- c(seriez, "manual_level_m")
    if (conduct == TRUE && is.null(rain_data)) {
      seriez <- c(seriez, "conduct_mS_per_cm")
    }
    if (!is.null(rain_data)) seriez  <- c(seriez, "rain_tot")
    p <- dygraphs::dyRangeSelector(dygraphs::dygraph(x[, seriez],
      xlab = "Time", ylab = "Water level (m a.s.l.)"
    ))
    p <- dygraphs::dySeries(p, name = "t_level_m", color = "#1874CD")
    if (dippr == TRUE) {
      p <- dygraphs::dySeries(p, name = "manual_level_m",
        color = "#352007", drawPoints = TRUE,
        strokeWidth = 0, pointShape = "ex",
        pointSize = 3
      )
    }
  }
  # drift neutral graph
  if (vis_type == 2) {
    p <- dygraphs::dyRangeSelector(dygraphs::dygraph(
      x[, c("outliers", "drift_neutral")]
    ))
    p <- dygraphs::dySeries(p, name = "outliers", drawPoints = TRUE,
      strokeWidth = 0, color = "#E93E3E", pointSize = 2
    )
    p <- dygraphs::dySeries(p, name = "drift_neutral", color = "#8B7355")
  }
  if (vis_type == 3) {
    seriez <- c("outliers", "t_level_m", "drift_neutral")
    if (dippr == TRUE) seriez <- c(seriez, "manual_level_m")
    if (conduct == TRUE && is.null(rain_data)) {
      seriez <- c(seriez, "conduct_mS_per_cm")
    }
    if (!is.null(rain_data)) seriez  <- c(seriez, "rain_tot")
    p <- dygraphs::dyRangeSelector(
      dygraphs::dygraph(x[, seriez], xlab = "Time",
        ylab = "Water level (m a.s.l.)"
      )
    )
    p <- dygraphs::dySeries(p, name = "outliers", drawPoints = TRUE,
      strokeWidth = 0, color = "#E93E3E", pointSize = 2
    )
    p <- dygraphs::dySeries(p, name = "drift_neutral", color = "#8B7355")
    p <- dygraphs::dySeries(p, name = "t_level_m", color = "#1874CD")
    if (dippr == TRUE) {
      p <- dygraphs::dySeries(p, name = "manual_level_m",
        color = "#352007", drawPoints = TRUE,
        strokeWidth = 0, pointShape = "ex",
        pointSize = 2
      )
    }
  }
  if (conduct == TRUE && is.null(rain_data)) {
    p <- dygraphs::dySeries(p, name = "conduct_mS_per_cm",
      color = "#DBB633", axis = "y2"
    )
    p <- dygraphs::dyAxis(p, "y2", label = "EC (&mu;S<sup>-cm</sup>)")
    p <- dygraphs::dyOptions(p, axisLabelWidth = 80)
  }
  if (!is.null(rain_data)) {
    p <- dygraphs::dySeries(p, name = "rain_tot",
      color = "#5d5a50ab", axis = "y2", stepPlot = TRUE,
      fillGraph = TRUE
    )
    p <- dygraphs::dyAxis(p, "y2", label = paste0("Rainfall (mm)"))
    p <- dygraphs::dyOptions(p, fillAlpha = 0.4)
  }
  if (vis_type == 4 && is.null(rain_data)) {
    seriez <- c("outliers", "ut1_level_m", "level_kpa", "t_level_m",
      "drift_neutral"
    )
    if (dippr == TRUE) seriez <- c(seriez, "manual_level_m")
    p <- dygraphs::dyRangeSelector(dygraphs::dygraph(x[, seriez],
      xlab = "Time", ylab = "Water level (m a.s.l.)"
    ))
    p <- dygraphs::dySeries(p, name = "outliers", drawPoints = TRUE,
      strokeWidth = 0, color = "#E93E3E", pointSize = 2
    )
    p <- dygraphs::dySeries(p, name = "level_kpa",
      color = "#eb97ea", axis = "y2"
    )
    p <- dygraphs::dyAxis(p, "y2", label = "Atm pressure (kPa)")
    p <- dygraphs::dySeries(p, name = "ut1_level_m", color = "#285f38")
    p <- dygraphs::dySeries(p, name = "drift_neutral", color = "#8B7355")
    p <- dygraphs::dySeries(p, name = "t_level_m", color = "#1874CD")
    p <- dygraphs::dyOptions(p, axisLabelWidth = 80)
    if (dippr == TRUE) {
      p <- dygraphs::dySeries(p, name = "manual_level_m",
        color = "#352007", drawPoints = TRUE,
        strokeWidth = 0, pointShape = "ex",
        pointSize = 2
      )
    }
  }
  if (show_events == TRUE) {
    p <- dygraphs::dyEvent(p, x = ev$Date_time,
      strokePattern = "dashed", color = "#ABC7C7"
    )
  }
  p <- dygraphs::dyOptions(p, connectSeparatedPoints = FALSE,
    drawGapEdgePoints = FALSE
  )
  return(p)
}
