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
#' @param keep_open Logical. Keep _hidden_ 'station_file' open for ease of access. Defaults to `TRUE`.
#' @author Paul J. Gordijn
#' @details Gap data for each station and respective tables are extracted from the station, using `ipayipi::gap_eval()`. Gap tables are combined with the full record of data in a data summary table and plotted using ggplot2. Note that 'gaps' can be edited by imbibing metadata into a stations record, _see_ `ipayipi::gap_eval()` for details.
#' @return A list containing a plot, which shows the availability of data, the gap data as formatted for plotting. The plot is produced using `ggplot2::ggplot()`.
#' @keywords plotting; missing data; data over view; data reporting;
#' @author Paul J. Gordijn
#' @export
dta_availability0 <- function(
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
  ...
) {
  ".SD" <- ":=" <- "." <- NULL
  "station_n" <- "stnd_title" <- "dtt0" <- "dtt1" <- "station" <-
    "table_name" <- "data_yes" <- "date_time" <- "gap_yes" <-
    "gs_data_yes" <- "problem_gap" <- "gs_gid" <- "station_tbl_n" <-
    "gs_gap_start" <- "gs_gap_end" <- "gdttm1" <- "gdttm0" <- "dttm0" <-
    "dttm1" <- "table_wrd" <- "n" <- NULL
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
    dta <- readRDS(file.path(pipe_house$ipip_room, x))["gaps"]
    return(dta$gaps)
  })
  names(gaps) <- slist

  # produce gap tables where they are missing
  run_gaps <- names(gaps[sapply(gaps, is.null)])
  lapply(run_gaps, function(x) {
    ipayipi::open_sf_con(pipe_house = pipe_house, station_file = x)
  })
  run_gap_gaps <- lapply(run_gaps, function(x) {
    g <- ipayipi::gap_eval(pipe_house = pipe_house, station_file = x,
      gap_problem_thresh_s = gap_problem_thresh_s, event_thresh_s =
        event_thresh_s, keep_open = keep_open, meta_events = meta_events,
      verbose = verbose
    )
    ipayipi::write_station(pipe_house = pipe_house, sf = g, station_file = x,
      overwrite = TRUE, append = TRUE, keep_open = keep_open
    )
    g$gaps <- g$gaps[problem_gap == TRUE]
    invisible(g$gaps)
  })
  names(run_gap_gaps) <- run_gaps

  gaps <- c(gaps[!sapply(gaps, is.null)], run_gap_gaps)
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
    ds <- readRDS(file.path(pipe_house$ipip_room, slist[i]))[["data_summary"]]
    ds <- ds[, c("start_dttm", "end_dttm", "stnd_title", "table_name")]
    if (!is.null(plot_tbls)) ds <- ds[table_name %in% plot_tbls]
    ds$station_n <- rev(seq_along(slist))[i]
    ds <- split.data.frame(
      ds, f = factor(paste(ds$station_n, ds$table_name, sep = "; "))
    )
    return(ds)
  })
  sedta <- unlist(sedta, recursive = FALSE)
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
  data.table::setkey(sedta, "start_dttm", "end_dttm")

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
    dts3 <- gs[[i]][, "gap_end"] + 0.01
    names(dts3) <- "dtt0"
    dts <- rbind(dts, dts1, dts2, dts3)
    invisible(dts)
  })
  dts <- c(dts, list(no_gaps))
  dts <- data.table::rbindlist(dts)[order(dtt0), ]
  dts <- unique(dts)
  dts$dtt1 <- dts$dtt0
  data.table::setkey(dts, dtt0, dtt1)
  gs <- data.table::rbindlist(gs, use.names = TRUE)
  names(gs) <- paste0("gs_", names(gs))
  data.table::setkey(gs, "gs_gap_start", "gs_gap_end")
  gs <- gs[, ":="(gdttm0 = gs_gap_start, gdttm1 = gs_gap_end)]

  dty <- lapply(unique(sedta$data_yes), function(i) {
    dtj <- data.table::foverlaps(y = sedta[data_yes == i],
      x = dts, mult = "all", nomatch = NA
    )
    dtj <- dtj[, ":="(dttm0 = dtt0, dttm1 = dtt1)]
    data.table::setkey(dtj, dtt0, dtt1)
    dtj <- gs[gs_data_yes == i][dtj, on = .(gdttm1 >= dttm0, gdttm0 <= dttm1)]
    dtj$station <- dtj[!is.na(stnd_title)]$stnd_title[1]
    dtj$station_n <- dtj[!is.na(station_n)]$station_n[1]
    dtj$table_name <- dtj[!is.na(table_name)]$table_name[1]
    dtj <- dtj[, names(dtj)[
      names(dtj) %in% c("gs_gid", "data_yes", "gs_gap_yes", "dtt0",
        "station", "station_n", "table_name"
      )
    ], with = FALSE]
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
    new = c("date_time", "gap_yes")
  )

  dts$station_wrd <- paste0(dts$station_n, ": ", dts$station)
  q <- dts[!is.na(data_yes),
    c("data_yes", "station", "station_n", "table_name", "station_wrd")
  ][order(data_yes)]
  if (is.null(start_dttm)) start_dttm <- min(dts$date_time)
  if (is.null(end_dttm)) end_dttm <- max(dts$date_time)
  dts <- dts[date_time >= start_dttm]
  dts <- dts[date_time <= end_dttm]

  dt_label <- dts
  dt_label <- dt_label[!is.na(gs_gid)]
  if (nrow(dt_label) > 0) {
    dt_label <- dt_label[, date_time := min(date_time),
      by = .(station, gs_gid)
    ]
  }
  dt_label <- unique(dt_label)
  q <- unique(q)
  p <- ggplot2::ggplot(dts, ggplot2::aes(
    y = data_yes, x = date_time, group = data_yes, colour = station,
    text = paste("<b>Date-time: </b>", date_time, "<br>", "<b>Station: </b>",
      station, "<br>", "<b>Table name: </b>", table_name, "<br>",
      "<b>Gap ID: </b>", gs_gid
    )
  )) +
    ggplot2::geom_line(ggplot2::aes(x = date_time, y = gap_yes), linewidth = 8,
      colour = "#801b1b", na.rm = TRUE
    ) +
    ggplot2::geom_line(na.rm = TRUE, linewidth = 2, alpha = 0.8) +
    ggplot2::scale_y_continuous(
      breaks = q$data_yes, labels = q$station,
      sec.axis = ggplot2::sec_axis(~.,
        breaks = q$data_yes, labels = q$table_name
      )
    ) +
    ggplot2::labs(x = "Date", colour = "Station") +
    egg::theme_article() +
    ggplot2::theme(axis.title.y = ggplot2::element_blank(),
      legend.position = "top"
    )
  p <- p + ggplot2::geom_text(data = dt_label, inherit.aes = FALSE,
    mapping = ggplot2::aes(y = data_yes, x = date_time, group = data_yes,
      label = gs_gid
    ), nudge_y = 0.35, size = 3
  )

  # build table of dependancies for full time period and table limited time
  # period
  # unique nomenclature table
  dts <- dts[, station_tbl_n := as.numeric(as.factor(table_wrd))][
    order(station_tbl_n)
  ]
  ntbl <- unique(dts[
    , -c("gs_gid", "gap_yes", "table_name", "data_yes", "date_time")
  ])[order(station_tbl_n)]
  dtl <- split.data.frame(dts, f = as.factor(dts$station_tbl_n))
  dtg <- do.call(cbind, lapply(dtl, function(x) {
    s <- x[!is.na(data_yes)]$table_wrd[1]
    x[!is.na(data_yes) & is.na(gap_yes), "data"] <- 1 # data present
    sb <- which(x$data == 1)
    if (sb[1] > 0) x[c(1:(sb[1] - 1)), "data"] <- 0 # an NA lead
    if (sb[length(sb)] < nrow(x)) {
      x[c(sb[length(sb)]:nrow(x)), "data"] <- 2 # NA tail
    }
    x <- x[, "data"]
    names(x) <- s
    return(x)
  }))
  names(dtg) <- ntbl$table_wrd
  # generate dependancy matrices for imputation
  dep <- future.apply::future_lapply(ntbl$station_tbl_n, function(i) {
    tbln <- ntbl$table_wrd[i]
    dtgx <- dtg[,
      ntbl$station_tbl_n[!ntbl$station_tbl_n %in% i],
      with = FALSE
    ]
    dtgx <- dtgx[, n := rowSums(.SD == 1, na.rm = TRUE)]
    l_dep <- dtgx[which(dtg[[tbln]] == 0 & dtgx$n >= 1)]
    d <- colSums(l_dep[, -"n"] == 1, na.rm = TRUE)
    g_dep <- dtgx[which(is.na(dtg[[tbln]]))]
    g <- colSums(g_dep[, -"n"] == 1, na.rm = TRUE)
    t_dep <- dtgx[which(dtg[[tbln]] == 2 & dtgx$n >= 1)]
    t <- colSums(t_dep[, -"n"] == 1, na.rm = TRUE)
    data.table::data.table(depn = rep(tbln, length(dtl)),
      predn = c(tbln, names(d)),
      lead_pred = c(0, data.table::fifelse(d > 0, 1, 0)),
      gap_pred = c(0, data.table::fifelse(g > 0, 1, 0)),
      tail_pred = c(0, data.table::fifelse(t > 0, 1, 0))
    )
  })
  dep <- data.table:::rbindlist(dep)
  depl <- data.table::dcast(dep, depn ~ predn, value.var = "lead_pred")
  nord <- names(depl)[!names(depl) %in% "depn"]
  depl <- depl[, c("depn", nord[order(as.numeric(sub(": .*", "", nord)))]),
    with = FALSE
  ][order(as.numeric(sub(": .*", "", depl$depn)))]
  ml <- as.matrix(depl[, -"depn"])
  rownames(ml) <- ntbl$table_wrd
  depl <- data.table::dcast(dep, depn ~ predn, value.var = "gap_pred")
  depl <- depl[, c("depn", nord[order(as.numeric(sub(": .*", "", nord)))]),
    with = FALSE
  ][order(as.numeric(sub(": .*", "", depl$depn)))]
  mg <- as.matrix(depl[, -"depn"])
  rownames(mg) <- ntbl$table_wrd
  depl <- data.table::dcast(dep, depn ~ predn, value.var = "tail_pred")
  depl <- depl[, c("depn", nord[order(as.numeric(sub(": .*", "", nord)))]),
    with = FALSE
  ][order(as.numeric(sub(": .*", "", depl$depn)))]
  mt <- as.matrix(depl[, -"depn"])
  rownames(mt) <- ntbl$table_wrd

  data_availability <- list(plt_availability = p, plt_data = dts,
    lead_m = ml, gap_m = mg, tail_m = mt, nomtbl = ntbl
  )
  return(data_availability)
}