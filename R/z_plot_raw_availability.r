#' @title Plot the coverage of standardised hobo rainfall files
#' @description Uses dygraphs to plot the availability of data at a group
#'  of hobo rainfall stations.
#' @author Paul J. Gordijn
#' @keywords graphs; data management; missing data; hobo files;
#' @param input_dir Folder in which to search for standardised SAEON hobo
#'  rainfall files.
#' @param recurr Should the function search recursively? I.e., thorugh
#'  sub-folders as well --- `TRUE`/`FALSE`.
#' @param wanted Character string of the station keyword. Use this to
#'  filter out stations that should not be plotted alongside eachother. If more
#'  than one search key is included, these should be separated by the bar
#'  character, e.g., `"mcp|manz"`, for use with data.table's `%ilike%` operator.
#' @param unwanted Vector of strings listing files that should not be included
#'  in the import.
#' @param prompt Logical. If `TRUE` the function will prompt the user to
#'  identify stations for plotting. The wanted and unwanted filters
#'  still apply when in interactive mode (`TRUE`).
#' @details This function will plot the continuity of data from a number of
#'  stations (rain gauges). The potential continuity of data, from start to end,
#'  is plotted then overlaid with 'problem' data gaps. Data gaps are extracted
#'  from the 'gaps' table in a standardised `ipayipi` data object. See the
#'  `ipayipi::rain_gaps()` and `ipayipi::rain_gaps_batch()` functions.
#' @return A list containing a plot, which shows the availability of data, the
#'  plot data and a seperate data table showing the mid-point, in time, of each
#'  problem gap, plus associated gap details. This data could be used to
#'  annotated the plot. The plot is produced using `ggplot2::ggplot()`.
#' @keywords plotting; missing data; data over view; data reporting;
#' @author Paul J. Gordijn
#' @export
plot_raw_availability <- function(
  pipe_house = NULL,
  gap_problem_thresh_s = 6 * 60 * 60,
  event_thresh_s = 10 * 60,
  station_ext = ".ipip",
  wanted = NULL,
  unwanted = NULL,
  recurr = FALSE,
  prompt = FALSE,
  cores = getOption("mc.cores", 2L),
  keep_open = FALSE,
  ...
) {
  "station_n" <- "stnd_title" <- "dtt0" <- "dtt1" <- "station" <-
    "table_name" <- "data_yes" <- "date_time" <- "gap_yes" <-
    "station_wrd" <- "i" <- NULL
  slist <- ipayipi::dta_list(input_dir = pipe_house$ipip_room,
    file_ext = ".ipip", prompt = prompt, recurr = recurr,
    unwanted = unwanted, wanted = wanted)

  # extract data for gaps
  dta <- parallel::mclapply(slist, function(x) {
    dta <- readRDS(file.path(pipe_house$ipip_room, x))[
      c("data_summary", "gaps")]
    return(dta)
  }, mc.cores = cores)

  gaps <- lapply(dta, function(x) x$gaps)
  names(gaps) <- slist

  # produce gap tables where they are missing
  run_gaps <- names(gaps[sapply(gaps, is.null)])
  run_gap_gaps <- lapply(run_gaps, function(x) {
    g <- ipayipi::gap_eval(pipe_house = pipe_house, station_file = x,
      gap_problem_thresh_s = gap_problem_thresh_s, event_thresh_s =
      event_thresh_s, keep_open = keep_open)
    ipayipi::write_station(pipe_house = pipe_house, sf = g, station_file = x,
      overwrite = TRUE, append = TRUE, keep_open = keep_open)
    invisible(g$gaps)
  })
  names(run_gap_gaps) <- run_gaps

  gaps <- c(gaps[!sapply(gaps, is.null)], run_gap_gaps)
  gs <- lapply(seq_along(gaps), function(i) {
    gaps[[i]]$station <- sub("\\.ipip", "", names(gaps[i]))
    g <- split.data.frame(gaps[[i]], f = as.factor(gaps[[i]]$table_name))
    return(g)
  })
  gs <- unlist(gs, recursive = FALSE)
  gs <- lapply(seq_along(gs), function(i) {
    gs[[i]]$gap_yes <- i
    data.table::setkey(gs[[i]], "gap_start", "gap_end")
    gs[[i]]
  })

  sedta <- lapply(seq_along(dta), function(i) {
    ds <- dta[[i]]$data_summary[, c("start_dttm", "end_dttm", "stnd_title")]
    ds$start_dttm <- min(ds$start_dttm)
    ds$end_dttm <- max(ds$end_dttm)
    ds <- ds[c(1, nrow(ds)), ]
    ds <- unique(ds)
    ds$data_yes <- i
    ds$station_n <- rev(seq_along(dta))[i]
    return(ds)
  })
  sedta <- data.table::rbindlist(sedta)
  data.table::setkey(sedta, "start_dttm", "end_dttm")

  # get all time recordings
  dts <- lapply(seq_along(gs), function(i) {
    dts1 <- sedta[stnd_title == gs[[i]]$station[1], "start_dttm"]
    names(dts1) <- "dtt0"
    dts2 <- sedta[stnd_title == gs[[i]]$station[1], "end_dttm"]
    names(dts2) <- "dtt0"
    dts <- rbind(dts1, dts2)
    # add in the gaps
    dts1 <- gs[[i]][, "gap_start"]
    names(dts1) <- "dtt0"
    dts2 <- gs[[i]][, "gap_end"]
    names(dts2) <- "dtt0"
    dts3 <- gs[[i]][, "gap_end"] + 1
    names(dts3) <- "dtt0"
    dts <- rbind(dts, dts1, dts2, dts3)
    invisible(dts)
  })
  dts <- data.table::rbindlist(dts)[order(dtt0), ]
  dts <- unique(dts)
  dts$dtt1 <- dts$dtt0
  data.table::setkey(dts, dtt0, dtt1)

  dty <- lapply(seq_along(gs), function(i) {
    dtj <- data.table::foverlaps(
      y = sedta[stnd_title == gs[[i]]$station[1]],
        x = dts, mult = "all", nomatch = NA)
    data.table::setkey(dtj, dtt0, dtt1)
    dtj <- data.table::foverlaps(
      y = gs[[i]], x = dtj, mult = "all", nomatch = NA)
    dtj$station <- dtj[!is.na(station)]$station[1]
    dtj$table_name <- dtj[!is.na(table_name)]$table_name[1]
    dtj <- dtj[, c("gid", "data_yes", "gap_yes", "dtt0",
      "station", "station_n", "table_name")]
    data.table::setnames(dtj, "dtt0", "date_time")
    dtj[!is.na(data_yes)]$data_yes <- i
    dtj$station_n <- dtj[!is.na(station_n)]$station_n[1]
    dtj$table_wrd <- paste0(dtj$station_n[1], ": ", dtj$table_name[1])
    dtj$i <- i
    return(dtj)
  })

  # gap_labs <- lapply(seq_along(gaps), function(i) {
  #   g <- gaps[[i]]
  #   g$mid_time <- g$gap_start + (g$dt_diff_s / 2)
  #   g$station <- sub("\\.ipip", "", names(gaps[i]))
  #   g <- g[, c("gid", "mid_time", "station")]
  #   g$n <- i
  #   return(g)
  # })

  # gap_labs <- data.table::rbindlist(gap_labs)
  dts <- data.table::rbindlist(dty)
  dts$station_wrd <- paste0(dts$station_n, ": ", dts$station)

  p <- ggplot2::ggplot(dts, ggplot2::aes(
      y = data_yes, x = date_time, group = i, colour = station_wrd)) +
    ggplot2::geom_line(ggplot2::aes(y = gap_yes), linewidth = 8,
      colour = "#801b1b", na.rm = TRUE) +
    ggplot2::geom_line(na.rm = TRUE, linewidth = 2, alpha = 0.8) +
    ggplot2::scale_y_continuous(
      breaks = unique(dts[!is.na(i), ]$i),
      labels = unique(dts[i %in% unique(dts[!is.na(i), ]$i), ]$table_wrd)) +
    ggplot2::labs(x = "Date",
      colour = "Station") +
    egg::theme_article() +
    ggplot2::theme(axis.title.y = ggplot2::element_blank())
  data_availability <- list(p, dts)
  names(data_availability) <- c("plot_availability", "plot_data")
  return(data_availability)
}