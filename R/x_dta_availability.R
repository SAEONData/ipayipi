#' @title Plot the coverage of ipayipi station file data
#' @description Uses ggplot2 to show the availability of data for a select group of station's and their raw data tables. _Data availability is based on overall data logging time not NA values_.
#' @param input_dir Defaults to the working diretory. If the `pipe_house` arument is provided then the working directory defaults to `pipe_house$ipip_room`.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param phen_names Vector of phenomena names. Defaults to `NULL` whereby all phenomena gaps are included. Logger gaps are always included --- see the 'gap' table.
#' @param station_ext Required argument. File extension of the station files in the pipe_house directory 'ipip_room'.
#' @param gap_problem_thresh_s _Parsed to_ `ipayipi::gap_eval()`.
#' @param event_thresh_s _Parsed to_ `ipayipi::gap_eval()`.
#' @param start_dttm For plotting data.
#' @param end_dttm For plotting data.
#' @param tbl_names Vector of table names to include when plotting data availability.
#' @param meta_events _Parsed to_ `ipayipi::gap_eval()`.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param wanted A string of keywords to use to filter which stations are selected for processing. Multiple search kewords should be seperated with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param prompt Should the function use an interactive file selection function otherwise all files are returned. TRUE or FALSE.
#' @param recurr Should the function search recursively into sub directories for hobo rainfall csv export files? TRUE or FALSE.
#' @author Paul J. Gordijn
#' @details Gap data for each station and respective tables are extracted from the station, using `ipayipi::gap_eval()`. Gap tables are combined with the full record of data in a data summary table and plotted using ggplot2. Note that 'gaps' can be edited by imbibing metadata into a stations record, _see_ `ipayipi::gap_eval()` for details.
#' 
#' Gap retrival for processed data (not begining with 'raw_') may not work as expected.
#' @return A list containing a plot, which shows the availability of data, the gap data as formatted for plotting. The plot is produced using `ggplot2::ggplot()`.
#' @keywords plotting; missing data; data over view; data reporting;
#' @author Paul J. Gordijn
#' @export
dta_availability <- function(
  pipe_house = NULL,
  input_dir = ".",
  phen_names = NULL,
  phen_eval = TRUE,
  station_ext = ".ipip",
  gap_problem_thresh_s = 6 * 60 * 60,
  event_thresh_s = 10 * 60,
  start_dttm = NULL,
  end_dttm = NULL,
  tbl_names = "^raw_",
  meta_events = "meta_events",
  wanted = NULL,
  unwanted = NULL,
  recurr = TRUE,
  prompt = FALSE,
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  ...
) {
  "." <- ":=" <- "%chin%" <- "%ilike%" <- NULL
  "stnd_title" <- "station" <- "table_name" <- "data_yes" <- "gid" <-
    "problem_gap" <- "gap_start" <- "gap_end" <- "phen" <-  NULL

  if (is.null(pipe_house)) pipe_house <- list(ipip_room = input_dir)

  slist <- ipayipi::dta_list(input_dir = pipe_house$ipip_room,
    file_ext = station_ext, prompt = prompt, recurr = recurr,
    unwanted = unwanted, wanted = wanted
  )
  slist <- slist[order(basename(slist))]
  names(slist) <- basename(slist)
  sn <- gsub(paste0(station_ext, "$"), "", basename(slist))
  if (anyDuplicated(sn) > 0) {
    cli::cli_inform(c(
      "!" = "Reading duplicated stations ({.var stnd_title}) not allowed!",
      "i" = "Available station file list: \n {slist[order(sn)]}",
      ">" =
        "Refine station search using args: {.var wanted} and {.var unwanted}"
    ))
    return(NULL)
  }
  if (length(slist) == 0) {
    cli::cli_abort(c("Refine search parameters---no station files detected."))
  }

  if (all(tbl_names %ilike% "raw_")) {
    plt_dta <- get_raw_gaps(
      pipe_house = pipe_house, slist = slist, input_dir = input_dir,
      phen_names = phen_names, phen_eval = phen_eval, station_ext = station_ext,
      gap_problem_thresh_s = gap_problem_thresh_s, event_thresh_s =
        event_thresh_s, start_dttm = start_dttm, end_dttm = end_dttm,
      tbl_names = tbl_names, meta_events = meta_events,
      verbose = verbose, xtra_v = xtra_v, chunk_v = chunk_v
    )
  }

  if (all(tbl_names %ilike% "dt_")) {
    if (length(phen_names) > 1) {
      cli::cli_inform(c("i" = "Only using first phen: {phen_names[1]}"))
    }
    # query data and extract gap info
    h <- dta_flat_pull(input_dir = input_dir, pipe_house = pipe_house,
      tbl_names = tbl_names, phen_name = phen_names[1], wanted = wanted,
      unwanted = unwanted, recurr = recurr, prompt = prompt, verbose =
        verbose, xtra_v = xtra_v, chunk_v = chunk_v
    )
    if (length(h$tbl_names) > 1) {
      cli::cli_inform(c("i" = "Tables summarised under: {h$tbl_names[1]}"))
      tbl_names <- h$tbl_names[1]
    }
    # summarise general info per station then gap info
    dsl <- lapply(seq_len(ncol(h$dtgaps) - 1), function(i) {
      # get data start and end times
      statn <- names(h$dtgaps)[i + 1]
      mn <- min(h$dtgaps[s == TRUE, env = list(s = statn)]$date_time)
      mx <- max(h$dtgaps[s == TRUE, env = list(s = statn)]$date_time)
      ds <- data.table::data.table(
        start_dttm = mn, end_dttm = mx,
        stnd_title = statn,
        station = statn,
        station_n = i,
        table_name = tbl_names,
        phen = NA_character_,
        data_yes = i
      )
      # now get gaps summarised
      gi <- h$dta[, .(date_time, s), env = list(s = statn)][date_time >= mn][
        date_time <= mx
      ]
      gi[, na_int := na_intervals(s), env = list(s = statn)]
      gi <- gi[is.na(s), env = list(s = statn)]
      gi <- gi[, ":="(start_dttm = min(date_time), end_dttm = max(date_time)),
        by = "na_int"
      ][, ":="(station = statn, station_n = i, table_name = tbl_names,
          phen = phen_names[1], gap_type = "auto", data_yes = i
        )
      ][, .(station, station_n, table_name, phen, start_dttm, end_dttm,
          gap_type, data_yes
        )
      ]
      gi <- unique(gi)
      gi[, gid := seq_len(.N)]
      ds <- rbind(ds, gi, fill = TRUE)
      return(ds)
    })
    gs <- data.table::rbindlist(dsl)
    plt_dta <- list(gs = gs, sedta = gs[is.na(gid)])
  }
  plt_dta1 <- plt_dta
  gs <- plt_dta$gs
  sedta <- plt_dta$sedta

  p <- suppressWarnings(ggplot2::ggplot(
    gs[],
    ggplot2::aes(
      y = data_yes, x = start_dttm,
      group = data_yes, colour = station,
      text = paste("<b>Start--end: </b>", start_dttm, "--", end_dttm,
        "<br>", "<b>Station: </b>", station, "<br>", "<b>Table name: </b>",
        table_name, "<br>", "<b>Gap ID: </b>", gid
      )
    )
  ) +
    ggplot2::geom_segment(
      data = sedta,
      inherit.aes = FALSE,
      ggplot2::aes(
        y = data_yes, x = start_dttm,
        xend = end_dttm, yend = data_yes,
        text = paste("<b>Start--end: </b>", start_dttm, "--", end_dttm,
          "<br>", "<b>Station: </b>", station, "<br>", "<b>Table name: </b>",
          table_name
        ),
        colour = station
      ), linewidth = 2
    )
  )
  if (nrow(gs[phen %in% "logger"]) > 0) {
    p <- p + suppressWarnings(ggplot2::geom_segment(
      data = gs[phen %in% "logger"],
      inherit.aes = FALSE,
      ggplot2::aes(
        y = data_yes, x = start_dttm,
        xend = end_dttm, yend = data_yes,
        text = paste("<b>Start--end: </b>", start_dttm, "--", end_dttm,
          "<br>", "<b>Station: </b>", station, "<br>", "<b>Table name: </b>",
          table_name
        )
      ),
      colour = "#801b1b", linewidth = 6
    ))
  }

  if (nrow(gs[!phen %in% "logger" & !is.na(phen)]) > 0) {
    p <- suppressWarnings(p + ggplot2::geom_segment(
      data = gs[!phen %in% "logger" & !is.na(phen)],
      inherit.aes = FALSE,
      ggplot2::aes(
        y = data_yes, x = start_dttm,
        xend = end_dttm, yend = data_yes,
        text = paste("<b>Start--end: </b>", start_dttm, "--", end_dttm,
          "<br>", "<b>Station: </b>", station, "<br>", "<b>Table name: </b>",
          table_name, "<br>", "<b>Phenomena: </b>", phen
        )
      ), colour = "#271a1195", linewidth = 4
    ))
  }
  p <- suppressWarnings(p +
    ggplot2::scale_y_continuous(
      breaks = sedta$data_yes, labels = paste0(
        sedta$station, "\n", sedta$table_name
      ), limits = c(0.5, max(sedta$data_yes) + 0.5)
    ) + khroma::scale_colour_sunset(scale_name = "station", discrete = TRUE) +
    ggplot2::labs(x = "Date") +
    egg::theme_article() +
    ggplot2::theme(axis.title.y = ggplot2::element_blank(),
      legend.position = "none",
      axis.text.y = ggplot2::element_text(angle = 15)
    )
  )

  data_availability <- list(plt = p, plt_data = gs)
  invisible(data_availability)
}