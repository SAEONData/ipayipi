#' @title Generates 'gap' (missing data) table
#' @description The period between the time a logger/sensor is stopped, removed, or discommissioned, and when a logger/sensor beings recording data again, counts as missing data. However, for event-based data, where the frequency of recordings is temporarily erratic, identifying gaps is more tricky. This function sets some rules when identifying gaps in event-based time-series data. Furthermore, gaps can be manually declared in event metadata and integrated into the gap table.
#' @inherit gap_eval_batch details
#' @keywords data gaps; data pipeline; missing data; event data;
#' @author Paul J. Gordijn
#' @return A table describing data 'gaps'
#' @export
gap_eval <- function(
  pipe_house = NULL,
  station_file = NULL,
  gap_problem_thresh_s = 6 * 60 * 60,
  event_thresh_s = 10 * 60,
  phen_eval = TRUE,
  phens = NULL,
  meta_events = "meta_events",
  tbl_n = "^raw_*",
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  sfc = NULL,
  ...
) {

  ":=" <- "%ilike%" <- ".N" <- NULL
  "dt_diff_s" <- "table_name" <- "start_dttm" <- "end_dttm" <- "dta_start" <-
    "dta_end" <- "event_type" <- "gap_start" <- "gap_end" <- "problem_gap" <-
    "qa" <- "ev_start_dttm" <- "ev_end_dttm" <- "ev_phen" <- "ev_event_type" <-
    "gap_type" <- "ev_table_name" <- "ev_eid" <- "phen" <- "ev_notes" <-
    "gid" <- "odd" <- NULL

  # open station connection
  sfc <- sf_open_con(sfc = sfc)

  # generate gap table for each raw data table from data_summary ----
  ds <- ipayipi::sf_dta_read(sfc = sfc, tv = "data_summary", tmp = TRUE
  )[["data_summary"]]
  dta <- data.table::data.table(dta_start = min(ds$start_dttm),
    dta_end =  max(ds$end_dttm)
  )
  data.table::setkey(dta, dta_start, dta_end)
  ds$risec <- sapply(ds$record_interval, function(x) {
    sts_interval_name(x)[["dfft_secs"]]
  })

  tn <- unique(ds$table_name)

  seq_dt <- lapply(tn, function(x) {
    dx <- ds[table_name == x][order(start_dttm, end_dttm)]
    dx <- dx[, names(dx)[!names(dx) %ilike%
          "^uz_|logger|nomvet|origin|format"
      ], with = FALSE
    ]
    dx <- lapply(split.data.frame(dx, factor(dx$start_dttm)), function(x) {
      x$end_dttm <- max(x$end_dttm)
      return(x[1])
    })
    dx <- data.table::rbindlist(dx)[order(end_dttm)]
    l <- nrow(dx)
    if (l > 1) {
      dx$odd <- c(dx$start_dttm[1:(l - 1)] > dx$start_dttm[(2:(l))], FALSE)
    }
    while (l > 1 && any(dx$odd)) {
      dx <- dx[odd != TRUE]
      l <- nrow(dx)
      if (nrow(dx) > 1) {
        dx$odd <- c(dx$start_dttm[1:(l - 1)] > dx$start_dttm[(2:(l))], FALSE)
      }
      l <- nrow(dx)
    }
    # get rit and ri
    rit <- dx$record_interval_type
    if (!any("mixed" %in% rit, "event_based" %in% rit)) {
      ri <- dx$record_interval[1]
      ri <- sts_interval_name(ri)[["dfft_secs"]]
      gap_problem_thresh_s <- ri
    }
    l <- nrow(dx)
    # account for inclusive exclusive date_time intervals
    dttm_inc_exc <- dx$dttm_inc_exc[1]
    dttm_ie_chng <- dx$dttm_ie_chng[1]
    dttm_inc_exc <- if (dttm_ie_chng) !dttm_inc_exc else dttm_inc_exc
    dttm_inc_exc <- if (dttm_inc_exc && rit[1] %in% "continuous") {
      lubridate::as.duration(dx$record_interval[1])
    } else {
      lubridate::as.duration(0)
    }
    if (l == 1) {
      seq_dt <- data.table::data.table(
        gid = seq_len(1),
        eid = NA,
        gap_type = "auto",
        phen = "logger",
        table_name = x,
        gap_start = dx$end_dttm[1] + dttm_inc_exc,
        gap_end = dx$start_dttm[1] - dttm_inc_exc,
        dt_diff_s = difftime(
          dx$start_dttm[1], dx$end_dttm[1], units = "secs"
        ),
        gap_problem_thresh_s = gap_problem_thresh_s,
        problem_gap = TRUE,
        notes = as.character(NA)
      )
      seq_dt <- seq_dt[0]
    } else {
      seq_dt <- data.table::data.table(
        gid = seq_len(l - 1),
        eid = NA,
        gap_type = "auto",
        phen = "logger",
        table_name = x,
        gap_start = dx$end_dttm[1:(l - 1)] + dttm_inc_exc,
        gap_end = dx$start_dttm[2:l] - dttm_inc_exc,
        dt_diff_s = difftime(
          dx$start_dttm[2:l], dx$end_dttm[1:(l - 1)], units = "secs"
        ),
        gap_problem_thresh_s = gap_problem_thresh_s,
        problem_gap = TRUE,
        notes = NA_character_
      )
    }
    seq_dt <- seq_dt[dt_diff_s >= 0]
    seq_dt[dt_diff_s <= gap_problem_thresh_s, "problem_gap"] <- FALSE
    seq_dt <- seq_dt[problem_gap == TRUE]
    return(seq_dt)
  })
  names(seq_dt) <- sapply(seq_dt, function(x) x$table_name[1])
  gaps <- data.table::rbindlist(seq_dt)

  # if there is no table name apply to all
  tns <- unique(names(seq_dt))
  if (any(is.na(tns))) {
    gaps_ntns <- gaps[is.na(table_name)]
    gaps_ntns <- lapply(tns, function(x) {
      gaps_ntns$table_name <- x
      return(gaps_ntns)
    })
    gaps_ntns <- data.table::rbindlist(gaps_ntns)
    gaps <- rbind(gaps[!is.na(table_name)], gaps_ntns)
    gaps <- gaps[order(table_name, phen, gap_start)]
  }
  gaps <- gaps[, gid := seq_len(.N), by = table_name]

  # write gaps to temporary station file
  if (chunk_v) cli::cli_inform(c(" " = "Chunking logger gap data"))
  sf_dta_rm(sfc = sfc, rm = "gaps")
  #file.remove(sfc["gaps"], recursive = TRUE)
  sl <- sf_dta_wr(dta_room = file.path(dirname((sfc[1])), "gaps"),
    dta = gaps, overwrite = TRUE, tn = "gaps", chunk_v = chunk_v,
    rit = "event_based", ri = "discnt"
  )
  sf_log_update(sfc = sfc, log_update = sl)
  # refresh station connection
  sfc <- sf_open_con(sfc = sfc)

  # generate phen gaps summary ----
  # this gap summary should not overlap with the logger gap summary
  phen_gaps <- phen_gaps(sfc = sfc, tbl_n = tbl_n, verbose = verbose,
    xtra_v = xtra_v, chunk_v = chunk_v, phen_eval = phen_eval,
    gap_problem_thresh_s = gap_problem_thresh_s
  )
  gaps <- rbind(gaps, phen_gaps, use.names = TRUE, fill = TRUE)
  gaps <- gaps[, gid := seq_len(.N), by = "table_name"]

  # write gaps to temporary station file
  sfc <- sf_open_con(sfc = sfc)
  sf_dta_rm(sfc = sfc, rm = "gaps")
  if (chunk_v) cli::cli_inform(c(" " = "Chunking logger phen gap data"))
  sl <- sf_dta_wr(dta_room = file.path(dirname((sfc[1])), "gaps"),
    dta = gaps, overwrite = TRUE, tn = "gaps", chunk_v = chunk_v
  )
  sf_log_update(sfc = sfc, log_update = sl)
  # event data ----
  e <- ipayipi::sf_dta_read(sfc = sfc, tv = meta_events[1])
  e <- ipayipi::dt_dta_open(dta_link = e)
  e <- attempt::try_catch(
    .e = ~NULL,
    expr = e[qa == TRUE & event_type %in% c("gap_notes", "manual_gap")]
  )
  # check meta data gap info ----
  ## first check problem gaps in metadata
  if (!is.null(e) && nrow(e) > 0) {
    # handling of events
    # - gap_notes: used to make notes on a gap, or if
    # the notes are associated with 'probelm_gap' == FALSE then
    # the gap is declared non-problematic in discontinuous data.
    # - manual_gap: used to decare a manual gap in data.
    # these are only applied if the event 'qa' (quality assessment)
    # is TRUE.

    # prep man gap table
    e[is.na(event_thresh_s)][
      is.na(start_dttm) | is.na(end_dttm), "event_thresh_s"
    ] <- event_thresh_s
    e[is.na(start_dttm), "start_dttm"] <- e[is.na(start_dttm)
    ]$date_time - e[is.na(start_dttm)]$event_thresh_s
    e[is.na(end_dttm), "end_dttm"] <- e[is.na(end_dttm)
    ]$date_time + e[is.na(end_dttm)]$event_thresh_s
    data.table::setkey(e, start_dttm, end_dttm)
    # only use gaps within the data range
    e <- data.table::foverlaps(e, dta, nomatch = NA, mult = "all")
    e <- e[!is.na(dta_start) & !is.na(dta_end)]
    e$start_dttm <- data.table::fifelse(
      e$start_dttm < e$dta_start, e$dta_start, e$start_dttm
    )
    e$end_dttm <- data.table::fifelse(
      e$end_dttm > e$dta_end, e$dta_end, e$end_dttm
    )
    e <- subset(e, select = -c(dta_start, dta_end))

    # join man_gaps to auto gaps and operate
    seq_dt <- sf_dta_read(sfc = sfc, tv = "gaps")[["gaps"]]
    seq_dt <- split.data.frame(seq_dt, f = as.factor(seq_dt$table_name))
    iw <- lapply(seq_along(seq_dt), function(i) {
      if (any(!"table_name" %in% names(e))) {
        mgi <- e
      } else {
        mgi <- e[table_name == names(seq_dt)[i] | is.na(table_name)]
      }
      names(mgi) <- paste0("ev_", names(mgi))
      mgi[is.na(ev_phen), "ev_phen"] <- "logger"
      mgii <- mgi
      mgi <- mgii[ev_event_type == "manual_gap"]
      sq <- seq_dt[[i]]
      # first deal with gap notes
      data.table::setkey(mgi, ev_start_dttm, ev_end_dttm)
      data.table::setkey(sq, gap_start, gap_end)
      man_gaps_r <- data.table::foverlaps(mgi, sq, nomatch = NA, mult = "all")
      # define gap type
      man_gaps_seeprate <- man_gaps_r[is.na(gap_start)]
      man_gaps_seeprate <- man_gaps_seeprate[, ":=" (
        gap_type = "seperate", gap_start = ev_start_dttm,
        gap_end = ev_end_dttm
      )]
      man_gaps_seeprate$dt_diff_s <- man_gaps_seeprate$gap_end -
        man_gaps_seeprate$gap_start
      man_gaps_s <- man_gaps_r
      man_gaps_before <- man_gaps_s[
        ev_start_dttm < gap_start, gap_type := "before"
      ][!is.na(gap_start)][gap_type == "before"]

      # next need to account for overlap of start dates split the dt by start_dt
      # and assign start dates to gap_start accordingly.
      split_mmgb <- split.data.frame(man_gaps_before, man_gaps_before$ev_eid)
      mgl <- lapply(seq_along(split_mmgb), function(x) {
        mg <- split_mmgb[[x]]
        l <- nrow(mg)
        dt_i <- lapply(seq_len(l), function(z) {
          if (z == 1) {
            mg$gap_start[z] <- split_mmgb[[x]]$ev_start_dttm[z]
            mg$gap_end[z] <- split_mmgb[[x]]$gap_start[z] - 1
          }
          if (z > 1) {
            mg$gap_start[z] <- data.table::fifelse(
              split_mmgb[[x]]$ev_start_dttm[z] > split_mmgb[[x]]$gap_end[z - 1],
              split_mmgb[[x]]$ev_start_dttm[z],
              split_mmgb[[x]]$gap_end[z - 1] + 1
            )
            mg$gap_end[z] <- split_mmgb[[x]]$gap_start[z] - 1
          }
          return(mg[z])
        })
        dt_i <- data.table::rbindlist(dt_i)
        return(dt_i)
      })
      split_mmgb <- data.table::rbindlist(mgl)

      man_gaps_after <- man_gaps_s[ev_end_dttm > gap_end, gap_type := "after"][
        !is.na(gap_start)
      ][gap_type == "after"]
      split_mga <- split.data.frame(man_gaps_after, man_gaps_after$ev_eid)
      mgl <- lapply(seq_along(split_mga), function(x) {
        mg <- split_mga[[x]]
        l <- nrow(mg)
        dt_i <- lapply(seq_len(l), function(z) {
          if (z < l & l != 1) {
            mg$gap_start[z] <- split_mga[[x]]$gap_end[z] + 1
            mg$gap_end[z] <- data.table::fifelse(
              split_mga[[x]]$ev_end_dttm[z] <= split_mga[[x]]$gap_start[z + 1],
              split_mga[[x]]$ev_end_dttm[z],
              split_mga[[x]]$gap_start[z + 1] - 1
            )
          }
          if (z == l & l != 1) {
            mg$gap_start[z] <- split_mga[[x]]$gap_end[z] + 1
            mg$gap_end[z] <- split_mga[[x]]$ev_end_dttm[z]
          }
          if (l == 1) {
            mg$gap_start[z] <- split_mga[[x]]$gap_end[z] + 1
            mg$gap_end[z] <- split_mga[[x]]$ev_end_dttm[z]
          }
          return(mg[z])
        })
        dt_i <- data.table::rbindlist(dt_i)
        return(dt_i)
      })
      split_mga <- data.table::rbindlist(mgl)

      # combine gaps
      mg <- rbind(man_gaps_seeprate, split_mmgb, split_mga)
      mg <- transform(mg, eid = ev_eid)
      mg[!is.na(ev_notes), "notes"] <- mg[!is.na(ev_notes)]$ev_notes
      mg[!is.na(ev_phen), "phen"] <- mg[!is.na(ev_phen)]$ev_phen
      mg[!is.na(ev_table_name), "table_name"] <-
        mg[!is.na(ev_table_name)]$ev_table_name
      mg[is.na(phen)]$phen <- "logger"
      mg$problem_gap <- data.table::fifelse(
        mg$ev_problem_gap, TRUE, mg$ev_problem_gap
      )
      mg <- mg[, c(
        "gid", "eid", "table_name", "phen", "gap_type", "gap_start",
        "gap_end", "dt_diff_s", "gap_problem_thresh_s", "problem_gap", "notes"
      )]
      mg <- rbind(mg, sq)[order(gap_start)]
      # a gremlin may creep in here if the end time of the manual gap falls
      # within the next gap---the "before" type captures this gap type fine
      mg <- unique(mg, by = c("gap_start"))
      mg[, dt_diff_s := gap_end - gap_start]
      mg[gap_type != "auto", "gap_type"] <- "manual"
      mg[, "gap_problem_thresh_s"] <- gap_problem_thresh_s
      # mg <- transform(mg, problem_gap = data.table::fifelse(
      #   dt_diff_s > gap_problem_thresh_s, TRUE, FALSE
      # ))
      mg$gid <- seq_len(nrow(mg))
      mgn <- mgii[ev_event_type == "gap_notes"]
      # 1. check for problem gaps surrounded by problem gaps
      # 2. append logger gap notes from event data
      data.table::setkey(mgn, ev_start_dttm, ev_end_dttm)
      data.table::setkey(mg, gap_start, gap_end)
      gaps_o <- data.table::foverlaps(mgn, mg, nomatch = NULL,
        mult = "all"
      )
      gaps_o <- split.data.frame(gaps_o, gaps_o$gid)
      go <- lapply(gaps_o, function(x) {
        p <- any(x$ev_problem_gap)
        x$problem_gap <- p
        x$eid <- data.table::fifelse(is.na(x$eid), x$ev_eid, x$eid)
        x$notes <- paste0("Gap notes: ", x$ev_notes, ",", x$notes)
        x <- x[1, ]
        invisible(x)
      })
      go <- data.table::rbindlist(go)
      if (nrow(go) == 0) return(gaps <- seq_dt[[i]])
      go <- go[, c(
        "gid", "eid", "table_name", "phen", "gap_type", "gap_start",
        "gap_end", "dt_diff_s", "gap_problem_thresh_s", "problem_gap", "notes"
      )]
      go$notes <- gsub(",NA", "", go$notes)
      mg <- mg[!gid %in% go$gid]
      gaps <- rbind(mg, go)
      gaps <- gaps[order(gap_start)]
    })
    gaps <- unique(data.table::rbindlist(iw))
    gaps <- gaps[, gid := seq_len(.N), by = "table_name"]
    # write gaps to temporary station file
    sfc <- sf_open_con(sfc = sfc)
    sf_dta_rm(sfc = sfc, rm = "gaps")
    if (chunk_v) cli::cli_inform(c(" " = "Chunking event gap data"))
    sl <- sf_dta_wr(dta_room = file.path(dirname((sfc[1])), "gaps"),
      dta = gaps, overwrite = TRUE, tn = "gaps", chunk_v = chunk_v
    )
    sf_log_update(sfc = sfc, log_update = sl)
  }
  sf_write(pipe_house = pipe_house, station_file = station_file,
    overwrite = TRUE, append = FALSE, chunk_v = chunk_v
  )
  invisible(list(gaps = gaps))
}