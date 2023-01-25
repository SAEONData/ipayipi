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
rain_plot_availability <- function(
  input_dir = ".",
  recurr = FALSE,
  wanted = NULL,
  unwanted = NULL,
  prompt = FALSE,
  ...
) {
  slist <- ipayipi::dta_list(input_dir = input_dir, file_ext = ".rds",
    prompt = FALSE, recurr = recurr, unwanted = unwanted, wanted = wanted)
  rains_list <- lapply(slist, function(x) {
    dta <- readRDS(file.path(input_dir, x))
    invisible(dta)
  })
  rains_names <- lapply(rains_list, function(x) {
    h_name <- x$data_summary$ptitle_standard[1]
    invisible(h_name)
  })
  names(rains_list) <- rains_names

  # get the gaps
  gaps <- lapply(seq_along(rains_list), function(x) {
    if (!"gaps" %in% names(rains_list[[x]])) {
      stop("All standardised data require a gaps table")
    }
    gaps <- rains_list[[x]]$gaps
    gaps$gap_yes <- x
    return(gaps)
  })
  names(gaps) <- rains_names

  sedta <- lapply(seq_along(rains_list), function(x) {
    dta <- rains_list[[x]]$data_summary[, c("start_dt", "end_dt", "station")]
    dta <- dta[c(1, nrow(dta)), ]
    dta$start_dt <- min(dta$start_dt)
    dta$end_dt <- max(dta$end_dt)
    dta <- unique(dta)
    dta$data_yes <- x
    data.table::setkey(dta, "start_dt", "end_dt")
    return(dta)
  })

  # get all time recordings
  dts <- lapply(seq_along(rains_list), function(x) {
    dts1 <- rains_list[[x]]$data_summary[, "start_dt"]
    names(dts1) <- "dtt0"
    dts2 <- rains_list[[x]]$data_summary[, "end_dt"]
    names(dts2) <- "dtt0"
    dts <- rbind(dts1, dts2)
    # add in the gaps
    dts1 <- gaps[[x]][, "gap_start"]
    names(dts1) <- "dtt0"
    dts2 <- gaps[[x]][, "gap_end"]
    names(dts2) <- "dtt0"
    dts <- rbind(dts, dts1, dts2)
    invisible(dts)
  })
  dts <- data.table::rbindlist(dts)[order(dtt0), ]
  dts <- unique(dts)
  dts$dtt1 <- dts$dtt0
  data.table::setkey(dts, dtt0, dtt1)

  dty <- lapply(seq_along(sedta), function(x) {
    dtj <- data.table::foverlaps(
      y = sedta[[x]], x = dts, mult = "all", nomatch = NA)
    data.table::setkey(dtj, dtt0, dtt1)
    dtj <- data.table::foverlaps(
      y = gaps[[x]][!problem_gap == FALSE], x = dtj, mult = "all", nomatch = NA)
    dtj$station <- dtj[!is.na(station)]$station[1]
    dtj <- dtj[, c("gid", "data_yes", "gap_yes", "dtt0", "station")]
    data.table::setnames(dtj, "dtt0", "date_time")
    return(dtj)
  })

  gap_labs <- lapply(seq_along(gaps), function(x) {
    g <- gaps[[x]]
    g$mid_time <- g$gap_start + (g$dt_diff_s / 2)
    g$station <- rains_names[x]
    g <- g[problem_gap == TRUE]
    g <- g[, c("gid", "mid_time", "station")]
    g$n <- x
    return(g)
  })

  gap_labs <- data.table::rbindlist(gap_labs)
  dts <- data.table::rbindlist(dty)

  p <- ggplot2::ggplot(dts, ggplot2::aes(
      y = data_yes, x = date_time, group = station,
        colour = station)) +
    ggplot2::geom_line(ggplot2::aes(y = gap_yes), size = 8,
      colour = "#801b1b", na.rm = TRUE) +
    ggplot2::geom_line(na.rm = TRUE, size = 2, alpha = 0.8) +
    ggplot2::scale_y_continuous(
      breaks = seq_along(unique(dts[!is.na(station), ]$station)),
      labels = unique(dts[!is.na(station), ]$station)) +
    ggplot2::labs(x = "Date") +
    egg::theme_article() +
    ggplot2::theme(axis.title.y = ggplot2::element_blank())
  data_availability <- list(p, dts, gap_labs)
  names(data_availability) <- c("plot_availability", "plot_data", "gap_labels")
  return(data_availability)
}