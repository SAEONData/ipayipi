#' @title Plot cummulative sum
#' @description Calculates the cummulative sum of a selected phenomena for one or more stations. Filter stations by using the `wanted` and `unwwanted` search keys (below).
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
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on Linux systems.
#' @return A list containing the plot data and a ggplot object.
#' @details This function will plot the cummulative sum of similar phenomena from a number of stations.The plot can be made into an interactive chart using the `plotly` and `ggplot2` plotting interoperability.
#' __Filtering station files__
#' `unwanted`: By default the following files (when searching for stations) are filtered out:
#'  - Unstandarised `ipayipi` files: '*.ipr$|*.ipi$|*.iph$|wait_room|nomvet_room',
#'  - Other 'ipayipi' files: '*.rps|*.rns$|*.rmds$', and
#'  - Miscellaneous: '*.ods$|*.doc$|*.xls$|*.r$'.
#'
#' @keywords plotting; cummulative phenomena; data over view; data reporting;
#' @author Paul J. Gordijn
#' @export
plot_cumm <- function(
  phen_name = NULL,
  tbl_search_key = NULL,
  wanted = NULL,
  unwanted = NULL,
  x_lab = "Date-time",
  y_lab = NULL,
  input_dir = ".",
  file_ext = ".ipip",
  prompt = FALSE,
  recurr = TRUE,
  ...
) {

  "date_time" <- "stnd_title" <- NULL
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
    if (!any(names(dta[[1]]) %in% phen_name)) {
      dta <- NULL
    } else {
      dta <- dta[[1]][,
        names(dta[[1]])[names(dta[[1]]) %in% c("date_time", phen_name, "id$")],
        with = FALSE
      ]
    }
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
  if (length(dts) == 0) {
    stop("Refine search keys---no station files with matching data!")
  }
  dts_names <- sapply(dts, function(x) x[["stnd_title"]])
  dts <- lapply(dts, function(x) x[["dta"]])
  names(dts) <- dts_names

  dts <- lapply(seq_along(dts), function(i) {
    dts[[i]][[paste0(phen_name, "_cumm")]] <- cumsum(dts[[i]][[phen_name]])
    dts[[i]][["stnd_title"]] <- dts_names[i]
    return(dts[[i]])
  })
  # append all data and prep for ggplot
  dts <- data.table::rbindlist(dts)
  y_var <- paste0(phen_name, "_cumm")
  p <- ggplot2::ggplot(dts, ggplot2::aes(x = date_time,
    y = !!as.name(y_var), color = stnd_title
  )) +
    ggplot2::geom_line(lwd = 1) +
    ggplot2::labs(x = x_lab, y = y_lab)
  phen_cummulative <- list(plt_data = dts, plt = p)
  return(phen_cummulative)
}