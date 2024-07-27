#' @title Plot the coverage of ipayipi station files
#' @description Uses ggplot2 to show the availability of data for a select group of station's and their raw data tables. _Data availability is based on overall data logging time not NA values_.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param station_ext Required argument. File extension of the station files in the pipe_house directory 'ipip_room'.
#' @param gap_problem_thresh_s _Parsed to_ `ipayipi::gap_eval()`.
#' @param event_thresh_s _Parsed to_ `ipayipi::gap_eval()`.
#' @param start_dttm For plotting data.
#' @param end_dttm For plotting data.
#' @param plot_tbls Vector of table names to include when plotting data availability.
#' @param meta_events _Parsed to_ `ipayipi::gap_eval()`.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param wanted A string of keywords to use to filter which stations are selected for processing. Multiple search kewords should be seperated with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param prompt Should the function use an interactive file selection function otherwise all files are returned. TRUE or FALSE.
#' @param recurr Should the function search recursively into sub directories for hobo rainfall csv export files? TRUE or FALSE.
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on Linux.
#' @param keep_open Logical. Keep _hidden_ 'station_file' open for ease of access. Defaults to `FALSE`.
#' @author Paul J. Gordijn
#' @details Gap data for each station and respective tables are extracted from the station, using `ipayipi::gap_eval()`. Gap tables are combined with the full record of data in a data summary table and plotted using ggplot2. Note that 'gaps' can be edited by imbibing metadata into a stations record, _see_ `ipayipi::gap_eval()` for details.
#' @return A list containing a plot, which shows the availability of data, the gap data as formatted for plotting. The plot is produced using `ggplot2::ggplot()`.
#' @keywords plotting; missing data; data over view; data reporting;
#' @author Paul J. Gordijn
#' @export
dta_availability2 <- function(
  pipe_house = NULL,
  station_ext = ".ipip",
  gap_problem_thresh_s = 6 * 60 * 60,
  event_thresh_s = 10 * 60,
  start_dttm = NULL,
  end_dttm = NULL,
  plot_tbls = NULL,
  meta_events = "meta_events",
  verbose = FALSE,
  wanted = NULL,
  unwanted = NULL,
  recurr = FALSE,
  prompt = FALSE,
  keep_open = TRUE,
  xtra_v = FALSE,
  ...
) {

  # station_ext <- ".ipip"
  # gap_problem_thresh_s <- 6 * 60 * 60
  # event_thresh_s <- 10 * 60
  # start_dttm <- NULL
  # end_dttm <- NULL
  # plot_tbls <- NULL
  # meta_events <- "meta_events"
  # verbose <- TRUE
  # wanted <- NULL
  # unwanted <- NULL
  # recurr <- FALSE
  # prompt <- FALSE
  # keep_open <- FALSE
  # xtra_v <- TRUE

  ":=" <- NULL
  "stnd_title" <- "station" <- "table_name" <- "data_yes" <- "gid" <-
    "problem_gap" <- "gap_start" <- "gap_end" <- "phen" <-  NULL
  slist <- ipayipi::dta_list(input_dir = pipe_house$ipip_room,
    file_ext = ".ipip", prompt = prompt, recurr = recurr,
    unwanted = unwanted, wanted = wanted
  )
  slist <- slist[order(basename(slist))]
  names(slist) <- basename(slist)
  if (length(slist) == 0) {
    stop("Refine search parameters---no station files detected.")
  }
  # extract data for gaps
  gaps <- future.apply::future_lapply(slist, function(x) {
    dta <- ipayipi::sf_dta_read(station_file = x, pipe_house = pipe_house,
      tv = "gaps", verbose = verbose, xtra_v = xtra_v, tmp = TRUE
    )
    return(dta$gaps)
  })

  # produce gap tables where they are missing
  run_gaps <- names(gaps[sapply(gaps, is.null)])
  lapply(run_gaps, function(x) {
    ipayipi::open_sf_con(pipe_house = pipe_house, station_file = x,
      tmp = TRUE, keep_open = TRUE
    )
  })
  run_gap_gaps <- lapply(run_gaps, function(x) {
    g <- ipayipi::gap_eval(pipe_house = pipe_house, station_file = x,
      gap_problem_thresh_s = gap_problem_thresh_s, event_thresh_s =
        event_thresh_s, keep_open = keep_open, meta_events = meta_events,
      verbose = verbose, xtra_v = xtra_v
    )
    ipayipi::write_station(pipe_house = pipe_house, sf = g, station_file = x,
      overwrite = TRUE, append = TRUE, keep_open = keep_open
    )
    g$gaps <- g$gaps[problem_gap == TRUE]
    invisible(g$gaps)
  })
  names(run_gap_gaps) <- run_gaps

  gaps <- c(gaps[!sapply(gaps, is.null)], run_gap_gaps)
  gaps_null <- gaps[[length(gaps)]][0]
  gs <- lapply(seq_along(gaps), function(i) {
    gaps[[i]]$station <- sub("\\.ipip", "", names(gaps[i]))
    g <- split.data.frame(gaps[[i]][problem_gap == TRUE],
      f = as.factor(gaps[[i]][problem_gap == TRUE]$table_name)
    )
    return(g)
  })
  gs <- unlist(gs, recursive = FALSE)
  if (!is.null(plot_tbls)) gs <- gs[names(gs) %in% plot_tbls]

  sedta <- lapply(seq_along(slist), function(i) {
    ds <- sf_dta_read(pipe_house = pipe_house, tv = "data_summary",
      station_file = slist[i], tmp = TRUE
    )[["data_summary"]]
    ds <- ds[, c("start_dttm", "end_dttm", "stnd_title", "table_name")]
    if (!is.null(plot_tbls)) ds <- ds[table_name %in% plot_tbls]
    ds$station_n <- rev(seq_along(slist))[i]
    ds <- split.data.frame(
      ds, f = factor(paste(ds$station_n, ds$table_name, sep = "; "))
    )
    return(ds)
  })
  sedta <- unlist(sedta, recursive = FALSE)
  # get max and min dttm's for table data
  sedta <- lapply(seq_along(sedta), function(i) {
    ds <- sedta[[i]]
    ds$start_dttm <- min(ds$start_dttm)
    ds$end_dttm <- max(ds$end_dttm)
    ds <- ds[c(1, nrow(ds)), ]
    ds <- unique(ds)
    ds$data_yes <- rev(seq_along(sedta))[i] # data_yes = plt y-axis
    return(ds)
  })
  sedta <- data.table::rbindlist(sedta)

  no_gaps <- sapply(gs, function(x) x$station[1])
  no_gaps <- sedta$stnd_title[!sedta$stnd_title %in% no_gaps]
  no_gaps <- sedta[stnd_title %in% no_gaps]
  no_gaps <- data.table::data.table(
    dtt0 = c(no_gaps$start_dttm, no_gaps$end_dttm)
  )

  gs <- lapply(gs, function(x) {
    tn <- x$table_name[1]
    s <- basename(x$station[1])
    x$station_n <- sedta[stnd_title == s]$station_n[1]
    data_yes <- sedta[stnd_title == s & table_name == tn]$data_yes[1]
    x$data_yes <- data_yes
    x$gap_yes <- data_yes
    return(x)
  })

  gs <- data.table::rbindlist(c(gs, list(gaps_null)), fill = TRUE,
    use.names = TRUE
  )
  gs <- gs[, ":="(start_dttm = gap_start, end_dttm = gap_end)]
  gs$table_wrd <- paste0(gs$station, ": ", gs$data_yes)
  sedta <- sedta[, station := stnd_title][, gid := ""]
  gs <- rbind(gs, sedta, use.names = TRUE, fill = TRUE)
  p <- suppressWarnings(ggplot2::ggplot(gs[phen %in% "logger"], ggplot2::aes(
    y = data_yes, yend = data_yes, x = start_dttm, xend = end_dttm,
    group = data_yes, colour = station,
    text = paste("<b>Start--end: </b>", start_dttm, "--", end_dttm,
      "<br>", "<b>Station: </b>", station, "<br>", "<b>Table name: </b>",
      table_name, "<br>", "<b>Gap ID: </b>", gid
    )
  )) +
    ggplot2::geom_segment(data = sedta, linewidth = 2, ggplot2::aes(
      text = paste("<b>Start--end: </b>", start_dttm, "--", end_dttm,
        "<br>", "<b>Station: </b>", station, "<br>", "<b>Table name: </b>",
        table_name
      )
    )) +
    ggplot2::geom_segment(colour = "#801b1b", linewidth = 6) +
    ggplot2::geom_segment(data = gs[!phen %in% "logger" & !is.na(phen)],
      colour = "#271a1195", linewidth = 4, ggplot2::aes(
        text = paste("<b>Start--end: </b>", start_dttm, "--", end_dttm,
          "<br>", "<b>Station: </b>", station, "<br>", "<b>Table name: </b>",
          table_name, "<br>", "<b>Phenomena: </b>", phen
        )
      )
    ) +
    ggplot2::scale_y_continuous(
      breaks = sedta$data_yes, labels = paste0(
        sedta$station, "\n", sedta$table_name
      )
    ) + khroma::scale_colour_sunset(discrete = TRUE) +
    ggplot2::labs(x = "Date", colour = "Station") +
    egg::theme_article() +
    ggplot2::theme(axis.title.y = ggplot2::element_blank(),
      legend.position = "none",
      axis.text.y = ggplot2::element_text(angle = 15)
    )
  )

  data_availability <- list(plt_availability = p, plt_data = gs)
  invisible(data_availability)
}