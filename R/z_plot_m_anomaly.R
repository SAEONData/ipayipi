#' @title Pulls multiple station data into a 'flat' file
#' @description Queries continuous data of common record intervals.
#' @param input_dir The directory in which to search for stations form which to extract time-series data.
#' @param pipe_house If the `pipe_house` argument is provided the `pipe_house$ipip_room` will be used instead of the `input_dir`.
#' @param tab_names Vector of table names in a station files from which to extract data. Add items to the vector such that only the first matching table from a station is selected. Table names are selected via the `%in%` argument.
#' @param phen_name The field/column/name of the phenomena which to join into a single data table.
#' @param wide Logical. If FALSE output data will be pivoted to wide format. Defaults to TRUE.
#' @param ri Record-interval string. This function will guess the record interval used to generate a flat table. However, the guess may not be possible if there is insufficient data.
#' @param output_dir The output directory where an output csv file is saved.
#' @param wanted A strong containing keywords to use to filter which stations are selected for processing. Multiple search kewords should be seperated with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param out_csv Logical. If TRUE a csv file is exported to the output directory.
#' @param out_csv_preffix Preffix for the output csv file. The phenomena name and then time interval by which the data are summarised are used as a suffix.
#' @param recurr Whether to search recursively through folders. Defaults to TRUE.
#' @param file_ext The extension of the stations from where data will be extracted. Defaults to ".ipip".
#' @param prompt Logical. If `TRUE` a prompt will be called so that the user can interactively select station files.
#' @param verbose Whether to report on progress.
#' @param xtra_v Extra verbose. Logical.
#' @details Note this function only extracts data from decompressed station files. If a station has not be decompressed the function will take time decompressing.
#' @keywords data pipeline; summarise time-series data; long-format data.
#' @return A list containing 1) the summarised data in a single data.table, 2) a character string representing the time interval by which the data has been summarised, 3) the list of stations used for the summary, 4) the name of the table and the name of the field for which data was summarised.
#' @author Paul J. Gordijn
#' @export
plot_m_anomaly <- function(
  input_dir = ".",
  phen_name = NULL,
  phen_units = "",
  output_dir = NULL,
  wanted = NULL,
  unwanted = NULL,
  out_csv = FALSE,
  out_tab_name = NULL,
  out_csv_preffix = "",
  recurr = TRUE,
  file_ext = ".ipip",
  prompt = FALSE,
  verbose = FALSE,
  xtra_v = FALSE,
  pipe_house = NULL,
  ...
) {
  ":=" <- ".N" <- "." <- NULL
  "station" <- "ypoly" <- "id" <- "xlab" <- "y" <- "ana" <-
    "month" <- "month_u" <- "date_time" <- NULL
  if (!is.null(pipe_house)) input_dir <- pipe_house$ipip_room
  # extract data ----
  d <- dta_flat_pull(input_dir = input_dir, phen_name = phen_name,
    tab_names = "dt_1_months_agg", pipe_house = NULL, wanted = wanted,
    unwanted = unwanted, file_ext = file_ext, prompt = prompt, recurr = recurr
  )

  # prep data for plotting
  d$dta <- d$dta[!c(1, .N)]
  dta <- d$dta
  dta$id <- seq_len(nrow(dta))
  dta <- data.table::melt(dta, id.vars = c("id", "date_time"),
    value.name = "ana", variable.name = "station"
  )

  dta$month <- data.table::month(dta$date_time)
  dta[, month_u := mean(ana, na.rm = TRUE), by = .(station, month)]
  dta[, ana := ifelse(!is.na(ana), ana - month_u, ana)]
  dta[, xlab := ifelse(month %in% c(12, 6, 3, 9),
      paste0(data.table::year(date_time), "-", sprintf("%02d", month)), ""
    )
  ]
  dtas <- split.data.frame(dta, f = factor(dta$station))
  vals <- lapply(seq_len(nrow(dta)), function(i) {
    d <- data.table::data.table(
      id = dta[i]$id, date_time = dta[i]$date_time,
      station = dta[i]$station,
      y = if (!is.na(dta[i]$ana)) {
        seq(0, dta[i]$ana,
          by = 1 * if (dta[i]$ana < 0) -1 else 1
        )
      } else {
        NA
      }
    )
    d[, ypoly := d[.N]$y]
  })

  dta <- data.table::rbindlist(vals)
  dta$xlabs <- ifelse(data.table::month(dta$date_time) %in% c(3, 6, 9, 12),
    paste0(data.table::year(dta$date_time), "-",
      sprintf("%02d", data.table::month(dta$date_time))
    ), NA_character_
  )
  yl <- paste0("Monthly ", phen_name, " anomaly")
  ul <- ""
  if (phen_units != "") {
    yl <- paste0(yl, " (", phen_units, ")")
    ul <- paste0("\n  (", phen_units, ")")
  }

  ana_plot <- function(x) {
    ggplot2::ggplot(dta[station %in% x],
      ggplot2::aes(x = id - 0.45, xend = id + 0.45,
        y = y, yend = y, colour = y
      )
    ) +
      ggplot2::geom_vline(xintercept = dtas[[x]][!is.na(xlab)]$id,
        colour = "#a0b1ba67"
      ) +
      ggplot2::geom_segment(linewidth = 2) +
      ggplot2::scale_color_gradientn(
        colours = c("#a42a21", "orange", "#ffcc00", "#dfdbc9",
          "lightblue", "#7777ad", "#22227f"
        ), limits = c(-400, 400), name = paste0("Monthly\nanomaly", ul)
      ) +
      ggplot2::geom_hline(yintercept = 0, colour = "#5f5555") +
      ggplot2::labs(x = "Date-time", y = yl,
        title = x
      ) +
      ggplot2::scale_x_continuous(
        breaks = dtas[[x]][!is.na(xlab)]$id,
        minor_breaks = dtas[[x]][is.na(xlab)]$id,
        labels = dtas[[x]][!is.na(xlab)]$xlab
      ) +
      ggplot2::guides(x = ggplot2::guide_axis(minor.ticks = TRUE),
        y = ggplot2::guide_axis(minor.ticks = TRUE)
      ) +
      ggplot2::theme(
        axis.text.x = ggplot2::element_text(angle = 90, vjust = 0.5, hjust = 1),
        axis.minor.ticks.x.bottom = ggplot2::element_line(colour = '#a09d9d'),
        panel.grid.major.y = ggplot2::element_line(colour = "#5f555530")
      )
  }
  plt <- lapply(unique(dta$station), ana_plot)
  names(plt) <- unique(dta$station)
  return(list(dta = dta, plt = plt, stations = d$stations, ri = d$time_interval,
    phen_name = d$phen_name, phen_units = phen_units
  ))
}