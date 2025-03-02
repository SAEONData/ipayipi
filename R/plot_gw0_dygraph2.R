#' @title Visualisation of multiple groundwater level series.
#' @description Plots interactive graph for visualising trends in water level and daily rainfall patterns.
#' @details Plotting is optimized for evaluating transformed groundwater data processed using `ipayipi`. Daily rainfall data must have no missing values within the data set (i.e., leading and training NA values are fine, but NAs between the start and end date-times will be problematic). Plotting is done via `dygraphs`.
#' @param input_dir Defaults to the current working directory. Only used if the `pipe_house` object is `NULL`.
#' @param ri Record interval string used to generate time sequence for merging data by date-time. Parsed to `dta_flat_pull0()`.
#' @param wanted Regex string used to filter stations when searching for groundwater data to plot.
#' @param unwanted Regex string to filter off unwanted stations when searching for groundwater stations from which to extract data.
#' @param rain_data A data.table with daily rainfall data. The rainfall column must be headed 'rain_tot', and the 'date-time' column must be named 'date_time'.
#' @param recurr Whether to search recursively within the `input_dir` or `pipe_house$ipip_room`. Logical. Defaults to FALSE.
#' @param prompt Logical. Use a prompt to select stations for plotting? Defaults to FALSE.
#' @param pipe_house If the `pipe_house` is provided stationse will be searched for in the `pipe_house$ipip_room` directory.
#' @author Paul J. Gordijn
#' @keywords Ground water visualisation, graph, grouped series
#' @export
gw_vis_dy2 <- function(
  input_dir = ".",
  ri = NULL,
  wanted = NULL,
  unwanted = NULL,
  rain_data = NULL,
  recurr = TRUE,
  prompt = FALSE,
  pipe_house = NULL,
  ...
) {
  ":=" <- NULL
  "date_time" <- NULL

  if (!is.null(pipe_house)) input_dir <- pipe_house$ipip_room
  if (is.null(ri)) stop("Please specifify record interval (ri)!")

  # summarise station data
  dta <- ipayipi::dta_flat_pull0(input_dir = input_dir, file_ext = ".rds",
    tab_name = "log_t", phen_name = "t_level_m", prompt = prompt,
    recurr = recurr, wanted = wanted, unwanted = unwanted, ri = ri
  )
  names(dta$dta)[names(dta$dta) %in% "Date_time"] <- "date_time"

  if (!is.null(rain_data)) {
    rn <- rain_data[, c("date_time", "rain_tot"), with = FALSE]
    rn$date_time <- as.POSIXct(rn$date_time,
      tz = attr(dta$dta$date_time, "tzone")
    )
    rn <- rn[date_time >= max(dta$dta$date_time) |
        date_time <= max(dta$dta$date_time)
    ]
    rn <- rn[, ":="(dtr1 = date_time, dtr2 = date_time)]
    dta1 <- dta$dta
    dta1 <- dta1[, ":="(dt1 = date_time, dt2 = date_time)]
    dta1 <- rn[dta1, on = "date_time", roll = TRUE]

    dta$dta <- dta1[,
      c("date_time", "rain_tot", gsub(".rds", "", dta$stations)),
      with = FALSE
    ]
  } else {
    dta$dta$rain_tot  <- NA
  }

  ## convert d_main to xts format
  x <- xts::xts(dta$dta[, -"date_time", with = FALSE],
    order.by = dta$dta$date_time
  )

  p <- dygraphs::dyRangeSelector(dygraphs::dygraph(x,
    xlab = "Time", ylab = "Water level (m a.s.l.)"
  ))
  p <- dygraphs::dyGroup(p, name = names(x)[!names(x) %in% "rain_tot"],
    color = viridisLite::mako((ncol(x) - 1), begin = 0.2, end = 0.8)
  )
  p <- dygraphs::dySeries(p, name = "rain_tot", color = "#5d5a50ab",
    axis = "y2", stepPlot = TRUE, fillGraph = TRUE
  )
  p <- dygraphs::dyAxis(p, "y2", label = paste0("Rainfall (mm)"))
  p <- dygraphs::dyOptions(p, fillAlpha = 0.4)
  p <- dygraphs::dyOptions(p, connectSeparatedPoints = FALSE,
    drawGapEdgePoints = FALSE
  )
  return(list(p = p, dta = dta$dta))
}
