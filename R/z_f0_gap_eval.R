#' @title Generates 'gap' (missing data) table
#' @description The period between the time a logger/sensor is stopped, removed, or discommissioned, and when a logger/sensor beings recording data again, counts as missing data. However, for event-based data, where the frequency of recordings is temporarily erratic, identifying gaps is more tricky. This function sets some rules when identifying gaps in event-based time-series data. Furthermore, gaps can be manually declared in event metadata and integrated into the gap table.
#' @param pipe_house Required. List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param station_file Required. Standardised __ipayipi__ station file.
#' @param gap_problem_thresh_s A duration threshold (in seconds) beyond which a gap, in event-based time series records, is considered problematic --- a 'true' gap. These gaps require infilling or 'patching'. The default for event-based data is six hours (i.e., 6 * 60 * 60 seconds). If the 'record_interval_type' is not 'mixed' or 'event_based', i.e. it is only continuous, then the 'record_interval' parameter is used as the `gap_problem_thresh_s`.
#' @param event_thresh_s A gap can also be specified in the event metadata by providing only a single date-time stamp and an 'event_threshold_s' (in seconds). By taking this date-time stamp the function approximates the start and end-time of the data gap by adding and subtracting the given `event_thresh_s` to the date-time stamp, respectively. If the `event_thresh_s` is supplied in the 'meta_events' data (_see_ `meta_to_station()`) then the value supplied therein is used in preference to the `event_thresh_s` argument in this function. The default event threshold here is ten minutes, i.e., 10 * 60 seconds.
#' @param phens Vector of phenomena names used for the 'phen' gap evaluation. Phenomena gap evaluation checks for 'NA' values in each phenomena column of a data table. Phenomena gaps result from sensor errors, whilst logger downtime result in 'logger' data gaps.
#' @param phen_eval Logical. If TRUE phenomena will be evaluated for 'NA' values. Note this is not appropriate from some loggers where gaps have to be determined from logger data input summaries, e.g., hobo logger event data---rainfall.
#' @param meta_events Optional. The name of the data.table in the station file with event metadata. Defaults to "meta_events".
#' @param keep_open Logical. Keep _hidden_ 'station_file' open for ease of access. Defaults to `TRUE`.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @details There are a number of different data problems that arise in time-series data. One of the simplest issues are data gaps, that is, periods where no data were recorded by a logger, or an individual sensor attached to a logger. Moreover, data can be declared 'missing' if it is erraneous. This function helps users identify gaps in continuous and discontinuous data types. Below is an overview of this function.
#'
#'  1. Derive gap table from raw data data summary information.
#'    If the gap period is longer than the record interval in continuous data the gap is declared problematic, i.e., `problem_gap == TRUE`. For discontinuous data, where the gap period extends beyond the `gap_problem_thresh_s` the gap is _'automatically'_ declared to be problematic.
#'  1. Integrate event metadata into the gap table (`meta_events` table).
#'    If a 'problem gap' has been declared in event metadata then this information is transferrred into the gap table. Also we can add notes to gaps declared automatically in the step above. Note that any gap declared in the metadata requires appropriate metadata describing the gap. These include:
#'    - 'eid': a unique event identification number (integer),
#'    - 'qa': Quality assessment. Logical. Only evaluates `qa == TRUE`,
#'    - 'station/location station': character string identifying the staiton name,
#'    - 'date_time': The date and time of the event (POSIX date time with format to match the raw data date_time),
#'    - 'event_type': Stanardised character string. For declaring a gap period use 'manual_gap'. For adding notes to a gap period use 'gap_notes' --- _see notes on this below_,
#'    - 'phen': If not provided this will default to 'logger', or else a specific phenomena name can be provided (standardised from the phens table). If this value is 'logger' we will assume the logger (with all sensors attached) was not functioning over the declared period,
#'    - 'table_name': A specific table the gap period is applied to. If no value is provided this will default to all raw data,
#'    - 'gap_problem_thresh_s': Can be provided to overwrite the `gap_problem_thresh_s` provided in the function arguments,
#'    - 'event_thresh_s': The event threshhold in seconds (_see start_dttm and end_dttm fields below),
#'    - 'problem_gap': Logical. If `TRUE` then the gap period declared is considered to be problematic. If `FALSE` a gap period will not be considered problematic. This is useful for discontinuous data. For example, if it was not raining during the period our logger had been stopped --- the 'automatically' identified gap, between the stop and start of the logger recording rainfall events (discontinuous record interval) --- the gap is not problematic. Therefore, we can declare `problem_gap` to be `FALSE` in the event metadata. Appropriate notes will be appended from the event metadata to the gap table in all cases.
#'    - 'notes': Character field for general note taking,
#'    - 'start_dttm': *Start date time of the manual gap period, and
#'    - 'end_dttm': *End date time of the manual gap period.
#'
#' * note that if start and end date times are not provided the 'date_time' field must be provided. Using adding and substracting the `event_thresh_s` to `date_time` will be used to generate `start_dttm` and `end_dttm` fields in this case.
#'
#' The final gap table summarises gap data merged with the above event metadata.
#'
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
  keep_open = TRUE,
  meta_events = "meta_events",
  verbose = FALSE,
  xtra_v = FALSE,
  tbl_n = "^raw_*",
  ...
) {
  # station_file <- "pipe_data_rainfall/mcp2/ipip_room/mcp_manz_office_rn.ipip"
  # gap_problem_thresh_s = 6 * 60 * 60
  # event_thresh_s = 10 * 60
  # keep_open = TRUE
  # meta_events = "meta_events"
  # verbose = FALSE
  # xtra_v = TRUE
  # cores = 4
  # tbl_n = "^raw_*"
  # phen_eval = TRUE

  ":=" <- "%ilike%" <- ".N" <- NULL
  "dt_diff_s" <- "table_name" <- "start_dttm" <- "end_dttm" <- "dta_start" <-
    "dta_end" <- "event_type" <- "gap_start" <- "gap_end" <- "problem_gap" <-
    "qa" <- "ev_start_dttm" <- "ev_end_dttm" <- "ev_phen" <- "ev_event_type" <-
    "gap_type" <- "ev_table_name" <- "ev_eid" <- "phen" <- "ev_notes" <-
    "gid" <- "odd" <- NULL

  # open station connection
  sfc <- ipayipi::open_sf_con(pipe_house = pipe_house,
    station_file = station_file, verbose = verbose, xtra_v = xtra_v
  )

  # generate gap table for each raw data table from data_summary ----
  ds <- ipayipi::sf_dta_read(sfc = sfc, tv = "data_summary", tmp = TRUE)[[
    "data_summary"
  ]]
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
  ipayipi::msg("Chunking logger gap data", xtra_v)
  file.remove(sfc["gaps"], recursive = TRUE)
  ipayipi::sf_dta_wr(dta_room = file.path(dirname((sfc[1])), "gaps"),
    dta = gaps, overwrite = TRUE, tn = "gaps",
    verbose = verbose, xtra_v = xtra_v
  )
  # refresh station connection
  sfc <- ipayipi::open_sf_con(pipe_house = pipe_house, station_file =
      station_file, verbose = verbose,
    xtra_v = xtra_v
  )

  # generate phen gaps summary ----
  # this gap summary should not overlap with the logger gap summary
  phen_gaps <- phen_gaps(pipe_house = pipe_house, station_file = station_file,
    tbl_n = tbl_n, verbose = verbose, xtra_v = xtra_v,
    gap_problem_thresh_s = gap_problem_thresh_s, phen_eval = phen_eval
  )
  gaps <- rbind(gaps, phen_gaps, use.names = TRUE, fill = TRUE)
  gaps <- gaps[, gid := seq_len(.N), by = "table_name"]

  # write gaps to temporary station file
  file.remove(sfc["gaps"], recursive = TRUE)
  ipayipi::msg("Chunking logger phen gap data", xtra_v)
  ipayipi::sf_dta_wr(dta_room = file.path(dirname((sfc[1])), "gaps"),
    dta = gaps, overwrite = TRUE, tn = "gaps",
    verbose = verbose, xtra_v = xtra_v
  )
  # event data ----
  e <- ipayipi::sf_dta_read(sfc = sfc, tv = meta_events[1], tmp = TRUE,
    verbose = verbose
  )
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
    seq_dt <- sf_dta_read(sfc = sfc, tv = "gaps", tmp = TRUE)[["gaps"]]
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
    file.remove(sfc["gaps"], recursive = TRUE)
    ipayipi::msg("Chunking event gap data", xtra_v)
    ipayipi::sf_dta_wr(dta_room = file.path(dirname((sfc[1])), "gaps"),
      dta = gaps, overwrite = TRUE, tn = "gaps",
      verbose = verbose, xtra_v = xtra_v
    )
  }
  if (!keep_open) {
    write_station(pipe_house = pipe_house, station_file = station_file,
      overwrite = TRUE, append = FALSE
    )
  }
  invisible(list(gaps = gaps))
}