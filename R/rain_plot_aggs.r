#' @title Plot rainfall accumulation from a batch of rain gauges
#' @description Uses ggplot to generate a plot
#'  of rainfall tip accumulation using standardised rainfall data.
#' @author Paul J. Gordijn
#' @param agg The time period of rainfall aggregation to be plotted given
#'  as a character string. The time period for the aggregation must match one
#'  of those processed for the stations in question by the
#'  `ipayipi::rain_agg` or the equivalent for batch processing, that is,
#'  `ipayipi::rain_agg_batch`.
#' @param input_dir Folder in which to search for standardised SAEON ipayipi
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
#' @param ignore_nas Default is FALSE. When set to TRUE the function aggregating
#'  data will use na.rm = TRUE to avoid return NA values.
#' @details Extracts available data from the selected stations and prepares it
#'  for plotting ggplot.
#' @return A list containing the ggplot plot, the data used for plotting, and
#'  the aggregation period (`agg`) used for data extraction or summary.
#' @keywords plotting; rainfall aggregates; data over view; data reporting;
#' @author Paul J. Gordijn
#' @export
rain_plot_aggs <- function(
  agg = "month",
  input_dir = ".",
  recurr = FALSE,
  wanted = NULL,
  unwanted = NULL,
  prompt = FALSE,
  ignore_nas = FALSE,
  ...
) {
  slist <- ipayipi::dta_list(input_dir = input_dir, file_ext = ".rds",
    prompt = prompt, recurr = recurr, unwanted = unwanted, wanted = wanted)

  # read in the data
  rains_list <- lapply(slist, function(x) {
    dta <- readRDS(file.path(input_dir, x))
    invisible(dta)
  })

  # name data by station
  rain_names <- lapply(rains_list, function(x) {
    h_name <- x$data_summary$ptitle_standard[1]
    invisible(h_name)
  })
  names(rains_list) <- rain_names

  # genearte aggregate data
  ag <- paste0("agg_", agg)
  dts <- lapply(seq_along(rains_list), function(x) {
    r1 <- ipayipi::rain_agg(
      input_file = rains_list[[x]], aggs = agg, ignore_nas = FALSE)
    r1 <- r1[[names(r1)[names(r1) %ilike% ag]]]
    r2 <- ipayipi::rain_agg(
      input_file = rains_list[[x]], aggs = agg, ignore_nas = TRUE)
    r2 <- r2[[names(r2)[names(r2) %ilike% ag]]]
    data.table::setnames(r2, "rain_mm", "rain_mm_nas")
    dts <- cbind(r1, r2[, "rain_mm_nas"])
    dts$station <- names(rains_list[x])
    return(dts)
  })
  # agg_checks <- lapply(rains_list, function(x) {
  #   agg_check <- names(x) %ilike% agg
  #   if (!any(agg_check)) {
  #     stop(paste0("Run the rainfall aggregation function for the",
  #         " specified aggregation period.", collapse = ""))
  #   }
  #   invisible(agg_check)
  # })
  # # extract data
  # dts <- lapply(seq_along(rains_list), function(x) {
  #   dts_n <- seq_along(names(rains_list[[x]]))[which(agg_checks[[x]] == TRUE)]
  #   dts <- rains_list[[x]][[dts_n]]
  #   dts$station <- names(rains_list[x])
  #   return(dts)
  # })
  dts_all <- data.table::rbindlist(dts)
  dt_seq <- seq(from = min(dts_all$date_time), to = max(dts_all$date_time),
    by = agg)
  dts <- lapply(seq_along(dts), function(x) {
    dts <- merge(data.table::data.table(date_time = dt_seq),
      dts[[x]], by = "date_time", all.x = TRUE)
    dts$station <- dts$station[which(!is.na(dts$station))[1]]
    return(dts)
  })
  dts <- data.table::rbindlist(dts)
  # generate plot
  p <- ggplot2::ggplot(dts, ggplot2::aes(
      x = date_time, y = rain_mm, fill = station)) +
    ggplot2::geom_bar(stat = "identity", position = "dodge") +
    ggplot2::labs(x = "Date-time", y = "Rainfall (mm)")
  agg_obj <- list(p, dts, agg)
  names(agg_obj) <- c("plot", "plot_data", "agg_period")
  return(agg_obj)
}