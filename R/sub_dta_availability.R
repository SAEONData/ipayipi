#' @title Plot the coverage of ipayipi station files
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
#' @return A list containing a plot, which shows the availability of data, the gap data as formatted for plotting. The plot is produced using `ggplot2::ggplot()`.
#' @keywords plotting; missing data; data over view; data reporting;
#' @author Paul J. Gordijn
#' @export
dta_availability <- function(
  input_dir = ".",
  pipe_house = NULL,
  phen_names = NULL,
  station_ext = ".ipip",
  gap_problem_thresh_s = 6 * 60 * 60,
  event_thresh_s = 10 * 60,
  start_dttm = NULL,
  end_dttm = NULL,
  tbl_names = NULL,
  meta_events = "meta_events",
  verbose = FALSE,
  wanted = NULL,
  unwanted = NULL,
  recurr = TRUE,
  prompt = FALSE,
  xtra_v = FALSE,
  ...
) {
  ":=" <- "%chin%" <- NULL
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
    message("Reading duplicated stations ('stnd_title') not allowed!")
    message("Refine search keywords using the 'un\\wanted arguments")
    print(slist[order(sn)])
    return(NULL)
  }
  if (length(slist) == 0) {
    stop("Refine search parameters---no station files detected.")
  }

  # check if the plot tbls are present
  sedta <- lapply(seq_along(slist), function(i) {
    ds <- sf_dta_read(pipe_house = pipe_house, tv = "data_summary",
      station_file = slist[i], tmp = TRUE
    )[["data_summary"]]
    ds <- ds[, c("start_dttm", "end_dttm", "stnd_title", "table_name")]
    tbl <- unique(ds$table_name)
    # select tbl based on ordering of tbl_names
    if (!is.null(tbl_names)) {
      tbl <- tbl[tbl %chin% tbl_names]
      tbln <- tbl_names[tbl_names %chin% tbl]
      tbl <- tbl[order(tbln)][1]
    }
    ds <- ds[table_name %chin% tbl]
    if (nrow(ds) == 0) return(NULL)
    return(ds)
  })

  slist_null <- slist[sapply(sedta, is.null)]
  if (length(slist_null) > 0) {
    ipayipi::msg("The following stations have no table called: ", verbose)
    ipayipi::msg(paste0("\r", paste0(tbl_names, collapse = " or ")), verbose)
    m <- paste0(slist, collapse = " | ")
    ipayipi::msg(m, verbose)
  }
  # remove stations with no appropriate tables
  slist <- slist[!sapply(sedta, is.null)]
  sedta <- sedta[!sapply(sedta, is.null)]

  # generate station numbers ----
  sedta <- lapply(seq_along(sedta), function(i) {
    sedta[[i]]$station_n <- rev(seq_along(slist))[i]
    sedta[[i]] <- split.data.frame(sedta[[i]],
      f = factor(paste(sedta[[i]]$station_n, sedta[[i]]$table_name, sep = "; "))
    )
    return(sedta[[i]])
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

  # extract data for gaps ----
  gaps <- lapply(slist, function(x) {
    dta <- sf_dta_read(station_file = x, pipe_house = pipe_house,
      tv = "gaps", verbose = verbose, xtra_v = xtra_v
    )
    return(dta$gaps)
  })

  # produce gap tables where they are missing
  run_gaps <- slist[sapply(gaps, is.null)]
  lapply(run_gaps, function(x) {
    open_sf_con(pipe_house = pipe_house, station_file = x)
  })
  run_gap_gaps <- lapply(run_gaps, function(x) {
    g <- ipayipi::gap_eval(pipe_house = pipe_house, station_file = x,
      gap_problem_thresh_s = gap_problem_thresh_s, event_thresh_s =
        event_thresh_s, meta_events = meta_events,
      verbose = verbose, xtra_v = xtra_v
    )
    ipayipi::write_station(pipe_house = pipe_house, sf = g, station_file = x,
      overwrite = TRUE, append = TRUE
    )
    g$gaps <- g$gaps[problem_gap == TRUE]
    invisible(g$gaps)
  })
  names(run_gap_gaps) <- run_gaps

  gaps <- c(gaps[!sapply(gaps, is.null)], run_gap_gaps)
  gaps_null <- gaps[[length(gaps)]][0]
  gs <- lapply(seq_along(gaps), function(i) {
    s <- sub("\\.ipip", "", names(gaps[i]))
    gaps[[i]]$station <- s
    # select tbl based on seddta tbls
    tbl <- unique(sedta[stnd_title %chin% s]$table_name)
    gaps[[i]] <- gaps[[i]][table_name %chin% tbl]
    if (!is.null(phen_names)) {
      gaps[[i]] <- gaps[[i]][phen %chin% unique(c("logger", phen_names))]
    }
    g <- split.data.frame(gaps[[i]][problem_gap == TRUE],
      f = as.factor(gaps[[i]][problem_gap == TRUE]$table_name)
    )
    return(g)
  })
  gs <- unlist(gs, recursive = FALSE)

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
  sedta <- sedta[, station := stnd_title][, gid := ""]
  gs <- rbind(gs, sedta, use.names = TRUE, fill = TRUE)
  gs$table_wrd <- paste0(gs$station, ": ", gs$data_yes)

  p <- suppressWarnings(ggplot2::ggplot(
    gs[data_yes %in% 1],
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
      )
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