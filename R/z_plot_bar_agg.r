#' @title Plot data by period aggregate
#' @description Plots data that has been processed by 'ipayipi' into a defined aggregation period.
#' @author Paul J. Gordijn
#' @param agg The time period of data aggregation to be plotted given as a character string. The character string is evaluated by `ipayipi::sts_interval_name()` to ensure compatibility.
#' @param phen_name The standardised phenomena name. This needs to be an exact match to the desired phenomena.
#' @param tbl_search_key A character string used to filter station tables within which the `phen_name` is found. This is a search string filters station tables using the `%ilike%` operator. Multiple search strings should be separated using the '|' symbol (regex apply).
#' @param wanted Character string of the station keyword. Use this to filter out stations that should not be plotted alongside eachother. If more than one search key is included, these should be separated by the bar character, e.g., `"mcp|manz"`, for use with data.table's `%ilike%` operator.
#' @param unwanted Vector of strings listing files that should not be included in the import.
#' @param x_lab The x-axis label to be rendered in the 'ggplot'.
#' @param y_lab The y-axis label.
#' @param input_dir Folder in which to search for standardised station data. Defaults to the current directory.
#' @param file_ext The station file extension. Inherits the 'ipayipi' defult of '.ipip'.
#' @param prompt Logical. If `TRUE` the function will prompt the user to identify stations for plotting. The wanted and unwanted filters still apply when in interactive mode (`TRUE`).
#' @param recurr Should the function search recursively? I.e., thorugh sub-folders as well --- `TRUE`/`FALSE`. Defaults to `FALSE`.
#' @param pipe_house If provided the 'ipip_room' will be used as the input directory. _See_ `ipip_house()`.
#' @return A list containing the plot data, a ggplot object, and the aggregation period used to summarise data.
#' @details When searching for station data only the first station table that matches tbl_search_key is extracted. Extracted tables will not be used if they don't have either a "date-time" column, or the specified 'phen_name'.
#' _NA_ values in data aggretation periods: if the 'gid' field (for gap identification) was created using `ipayipi::dt_agg()` a column in the returned dataa with the suffix '_nas' will be calculated whereby NA values are returned if a gap id ('gid') was present (_see also_ `ipayipi::gap_eval()`).
#' @keywords plotting; phenomena aggregates; data over view; data reporting;
#' @author Paul J. Gordijn
#' @export
plot_bar_agg <- function(
  input_dir = ".",
  agg = "1 month",
  phen_name = NULL,
  tbl_search_key = "dt_1_months_agg",
  show_gaps = FALSE,
  wanted = NULL,
  unwanted = NULL,
  x_lab = "Date-time",
  y_lab = NULL,
  file_ext = ".ipip",
  prompt = FALSE,
  recurr = TRUE,
  pipe_house = NULL,
  ...
) {

  "date_time" <- "stnd_title" <- NULL

  # input dir
  if (!is.null(pipe_house)) input_dir <- pipe_house$ipip_room

  # sts agg period
  agg <- ipayipi::sts_interval_name(agg)[["sts_intv"]]
  agg <- gsub(" ", "_", agg)

  # get station data
  unwanted <- paste("*.ipr$|*.ipi$|*.iph$|*.xls$|*.rps|*.rns$|*.ods$|*.doc$",
    "wait_room|nomvet_room|*.r$|*.rmds$", unwanted, sep = "|"
  )
  unwanted <- gsub(pattern = "\\|$", replacement = "", x = unwanted)
  slist <- ipayipi::dta_list(input_dir = input_dir, file_ext = file_ext,
    prompt = prompt, recurr = recurr, unwanted = unwanted, wanted = wanted
  )
  tbl_read <- function(x) {
    "stnd_title" <- NULL
    dta <- readRDS(file.path(input_dir, x))
    stnd_title <- dta$data_summary$stnd_title[1]
    dta <- dta[names(dta) %in% tbl_search_key]
    if (!any(names(dta[[1]]) %in% phen_name)) dta <- NULL
    if (!any(names(dta[[1]]) %in% "date_time")) dta <- NULL
    return(list(stnd_title = stnd_title, dta = dta))
  }
  dts <- lapply(slist, function(x) {
    dta <- attempt::attempt(tbl_read(x))
    if (attempt::is_try_error(dta)) {
      dta <- NULL
      message(paste0("Could not read or extract data from: ", x))
    }
    return(dta)
  })
  dts <- dts[!sapply(dts, is.null)]
  dts_names <- sapply(dts, function(x) x[["stnd_title"]])
  dts <- lapply(dts, function(x) x[["dta"]][[1]])
  names(dts) <- dts_names
  dts <- lapply(seq_along(dts), function(i) {
    dts[[i]][["stnd_title"]] <- dts_names[i]
    return(dts[[i]])
  })
  # join to common date_time series
  dts <- data.table::rbindlist(dts)
  if ("gid" %in% names(dts)) {
    dts[[paste0(phen_name, "_nas")]] <- data.table::fifelse(
      is.na(dts[["gid"]]), dts[[phen_name]], NA_real_
    )
  }

  # generate basic plot
  if (is.null(y_lab)) y_lab <- phen_name
  dts$year <- lubridate::year(dts$date_time)
  dts$month <- lubridate::month(dts$date_time, abbr = TRUE,
    label = TRUE
  )
  if (show_gaps) pgap <- paste0(phen_name, "_nas") else pgap <- phen_name
  p <- ggplot2::ggplot(dts, ggplot2::aes(
    x = date_time, y = !!as.name(pgap), colour = stnd_title,
    fill = stnd_title
  )) +
    ggplot2::geom_point(
      aes(y = !!as.name(phen_name)),
      position = ggplot2::position_dodge(width = 1),
      na.rm = TRUE, shape = "-", size = 15, alpha = 0.4
    ) +
    ggplot2::geom_bar(stat = "identity", position = "dodge") +
    ggplot2::labs(x = x_lab, y = y_lab) +
    khroma::scale_colour_sunset(discrete = TRUE) +
    khroma::scale_fill_sunset(discrete = TRUE) +
    egg::theme_article() +
    ggplot2::guides(
      color = ggplot2::guide_legend(override.aes = list(size = 5))
    )
  agg_obj <- list(plt_data = dts, plt = p, agg = agg)
  names(agg_obj) <- c("plt_data", "plt", "agg_period")
  return(agg_obj)
}