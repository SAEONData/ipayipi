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

  sedta <- lapply(seq_along(dta), function(i) {
    ds <- dta[[i]]$data_summary[,
      c("start_dttm", "end_dttm", "stnd_title", "table_name")]
    ds$station_n <- rev(seq_along(dta))[i]
    ds <- split.data.frame(
      ds, f = factor(paste(ds$station_n, ds$table_name, sep = "; ")))
    return(ds)
  })
  sedta <- unlist(sedta, recursive = FALSE)
  sedta <- lapply(seq_along(sedta), function(i) {
    ds <- sedta[[i]]
    ds$start_dttm <- min(ds$start_dttm)
    ds$end_dttm <- max(ds$end_dttm)
    ds <- ds[c(1, nrow(ds)), ]
    ds <- unique(ds)
    ds$data_yes <- rev(seq_along(sedta))[i]
    return(ds)
  })
  sedta <- data.table::rbindlist(sedta)
  data.table::setkey(sedta, "start_dttm", "end_dttm")

  no_gaps <- sapply(gs, function(x) x$station[1])
  no_gaps <- sedta$stnd_title[!sedta$stnd_title %in% no_gaps]
  #no_gaps <- sedta$stnd_title[!sedta$stnd_title %in% unique((dty$station))]
  no_gaps <- sedta[stnd_title %in% no_gaps]
  no_gaps <- data.table::data.table(
    dtt0 = c(no_gaps$start_dttm, no_gaps$end_dttm)
  )

  gs <- lapply(gs, function(x) {
    tn <- x$table_name[1]
    s <- x$station[1]
    x$station_n <- sedta[stnd_title == s]$station_n[1]
    data_yes <- sedta[stnd_title == s & table_name == tn]$data_yes[1]
    x$data_yes <- data_yes
    x$gap_yes <- data_yes
    return(x)
  })

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
  dts <- c(dts, list(no_gaps))
  dts <- data.table::rbindlist(dts)[order(dtt0), ]
  dts <- unique(dts)
  dts$dtt1 <- dts$dtt0
  data.table::setkey(dts, dtt0, dtt1)
  gs <- data.table::rbindlist(gs)
  names(gs) <- paste0("gs_", names(gs))
  data.table::setkey(gs, "gs_gap_start", "gs_gap_end")

  dty <- lapply(unique(sedta$data_yes), function(i) {
    dtj <- data.table::foverlaps(y = sedta[data_yes == i],
        x = dts, mult = "all", nomatch = NA)
    data.table::setkey(dtj, dtt0, dtt1)
    dtj <- data.table::foverlaps(
      y = gs[gs_data_yes == i], x = dtj, mult = "all", nomatch = NA)
    dtj$station <- dtj[!is.na(stnd_title)]$stnd_title[1]
    dtj$station_n <- dtj[!is.na(station_n)]$station_n[1]
    dtj$table_name <- dtj[!is.na(table_name)]$table_name[1]
    dtj <- dtj[, c("gs_gid", "data_yes", "gs_gap_yes", "dtt0",
      "station", "station_n", "table_name")]
    dtj$table_wrd <- paste0(dtj$station_n[1], ": ", dtj$table_name[1])
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
  data.table::setnames(dts, old = c("dtt0", "gs_gap_yes"),
    new = c("date_time", "gap_yes"))

  dts$station_wrd <- paste0(dts$station_n, ": ", dts$station)
  q <- dts[!is.na(data_yes),
    c("data_yes", "station", "station_n", "table_name", "station_wrd")][
      order(data_yes)
    ]
  q <- unique(q)
  p <- ggplot2::ggplot(dts, ggplot2::aes(
      y = data_yes, x = date_time, group = data_yes, colour = station)) +
    ggplot2::geom_line(ggplot2::aes(x = date_time, y = gap_yes), linewidth = 8,
      colour = "#801b1b", na.rm = TRUE) +
    ggplot2::geom_line(na.rm = TRUE, linewidth = 2, alpha = 0.8) +
    ggplot2::scale_y_continuous(
      breaks = q$data_yes, labels = q$station,
      sec.axis = ggplot2::sec_axis(~.,
        breaks = q$data_yes, labels = q$table_name)) +
    ggplot2::labs(x = "Date", colour = "Station") +
    egg::theme_article() +
    ggplot2::theme(axis.title.y = ggplot2::element_blank(),
      legend.position = "top")
  data_availability <- list(p, dts)
  names(data_availability) <- c("plot_availability", "plot_data")
  return(data_availability)
}