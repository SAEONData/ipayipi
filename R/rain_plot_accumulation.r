#' @title Plot rainfall accumulation from a batch of rain gauges
#' @description Uses ggplot to generate a plot of rainfall tip accumulation
#'  using standardised rainfall data.
#' @author Paul J. Gordijn
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
#' @details This function will plots rainfall accumulation of a number of
#'  stations (rain gauges). Data is extracted from the selected stations'
#' 'rain_data' data table. The plot can be made into an interactive chart using
#'  the `plotly` and `ggplot2` plotting interoperability.
#' @keywords plotting; cummulative rainfall; data over view; data reporting;
#' @author Paul J. Gordijn
#' @export
rain_plot_accumulation <- function(
  input_dir = ".",
  recurr = FALSE,
  wanted = NULL,
  unwanted = NULL,
  prompt = FALSE,
  ...
) {
  slist <- ipayipi::dta_list(input_dir = input_dir, file_ext = ".rds",
    prompt = FALSE, recurr = recurr, unwanted = unwanted, wanted = wanted)
  hobos_list <- lapply(slist, function(x) {
    dta <- readRDS(file.path(input_dir, x))
    invisible(dta)
  })
  hobo_names <- lapply(hobos_list, function(x) {
    h_name <- x$data_summary$ptitle_standard[1]
    invisible(h_name)
  })
  names(hobos_list) <- hobo_names
  # append all rain data and prep for ggplot
  dts <- lapply(seq_along(hobos_list), function(x) {
    dts_start <- data.table::data.table(
      id = 0, date_time = hobos_list[[x]]$data_summary$start_dt[1],
      rain_mm = 0, station = names(hobos_list)[x]
    )
    dts <- hobos_list[[x]]$rain_data
    dts$station <- names(hobos_list)[x]
    dts <- rbind(dts_start, dts)
    dts$cumm_rain_mm <- cumsum(dts$rain_mm)
    invisible(dts)
  })
  dts <- data.table::rbindlist(dts)

  p <- ggplot2::ggplot(dts, ggplot2::aes(
      x = date_time, y = cumm_rain_mm, colour = station)) +
    ggplot2::geom_line(lwd = 1) +
      ggplot2::labs(x = "Date-time", y = "Rainfall accumulation (mm)")
  rain_accumulation <- list(dts, p)
  names(rain_accumulation) <- c("plot_data", "plot_acc")
  return(p)
}