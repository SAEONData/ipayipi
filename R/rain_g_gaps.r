#' @title Highlight missing data at a station
#' @description The period between each logger download, stopping, removal,
#'  or discommissioning and the next time a logger beings recording data again
#'  counts as missing data if it rained over this period.
#' @param input_file Standardised **ipayipi** rainfall file.
#' @param gap_problem_thresh_s The threshold duration of a gap beyond which
#'  the gap is considered problematic, and therefore, the data requires
#'  infilling or 'patching'. The default is six hours given in seconds, i.e.,
#'  6 * 60 * 60.
#' @param event_thresh_s A gap can also be specified in the event metadata by
#'  only providing a single date-time stamp and an 'event_threshold_s'. By
#'  taking the date-time stamp the function approximates the start and end-time
#'  of the data gap by adding and subtracting the given `event_thresh_s` to the
#'  date-time stamp, respectively. If the `event_thresh_s` is supplied in the
#'  event data then the value supplied therein is used in preference to the
#'  value provided in this function. The default event threshold given here
#'  in seconds is ten minutes, i.e., 10 * 60 seconds.
#' @details There are a number of different types of data problems that may be
#'  encountered whilst collecting rainfall data. Confusion has to be avoided
#'  between declaring that no rainfall occurred or there were no recordings of
#'  rainfall over a period of time. This function helps users identify these
#'  different types problems with the data taking the following steps to
#'  identify and classify data 'gaps' (i.e., missing data).
#'  1. Trim any manually declared data gaps by the start and end dates of the
#'   extant rainfall data (start and end dates taken from the data summary).
#'  1. Generate a table of data 'gaps' between each download and deploy event.
#'   This type of data gap, i.e., the 'gap_type' is categorized as 'auto' in
#'   the final output. The rest of the gap types dealt with are manually
#'   declared data gaps.
#'  1. If there has been event data appended to the station data the function
#'   will search therein for any manually declared gaps, that is, the
#'   'event_type' 'logger_declare_error_segment'. If these segments extend
#'   'over' any 'auto' gaps they are trimmed to preserve the record of the
#'   'auto' gap. If any 'auto' gaps fall within an manually declared error
#'   segment then, they too, are considered a problem gap, even if the duration
#'   of the 'auto' gap is short enough to not be considered a problem gap.
#'  1. Within the pipeline notes can be appended to a gap using the
#'   'logger_gap_notes' 'event_type'. To do this a date-time stamp must be
#'   provided which falls within the gap of interest. When appending the notes
#'   ***the function will check whether the 'problem_gap' field was marked***.
#'   *If this field was FALSE then the gap is not considered a problematic gap*
#'   , i.e., the user is condident that no rainfall occurred during the 'gap'
#'   period. If the ***raining*** field was TRUE in the 'event_data' then the
#'   gap is considered problematic.
#'  1. A final 'gap' table is provided in the R data object. This table will
#'   be used to highlight no data when aggregating the data. Furthermore, in
#'   infilling exercises, the gaps provide a means for highlighting where
#'   'patching' is required. Below is a description of the gap table fields:
#'
#'   |Column  |Description |
#'   |--------|------------|
#'   |gid     |Unique identifier for each gap.  |
#'   |euid    |Event ID number imported from manually declared error segments. |
#'   |gap_start|The start date-time of a gap. |
#'   |gap_end |The end date-time of the gap. |
#'   |dt_diff_s |The time duration in seconds of the gap. |
#'   |gap_problem_thresh_s|The threshold duration beyond which a gap is
#'     considered problematic.|
#'   |problem_gap    |Logical field indicating whether a gap is considered to be
#'    problematic.|
#'   |Notes    | Notes describing the gap. The function will append any notes
#'    imported from the event data, if existent. |
#'
#' @keywords data gaps; data pipeline; missing data; event data;
#' @author Paul J. Gordijn
#' @return The input file (standardised R rainfall station) with a table
#'  describing data 'gaps'
#' @export
rain_gaps <- function(
  input_file = NULL,
  gap_problem_thresh_s = 6 * 60 * 60,
  event_thresh_s = 10 * 60 * 60,
  ...
) {
  if (class(input_file) != "SAEON_rainfall_data") {
    stop("Standardised ipayipi hobo rainfall data ojbect required!")
  }
  l <- nrow(input_file$data_summary)
  dta <- data.table::data.table(
    dta_start = input_file$data_summary$start_dt[1],
    dta_end =  input_file$data_summary$end_dt[l]
  )
  data.table::setkey(dta, dta_start, dta_end)
  if (l == 1) {
    seq_dt <- data.table::data.table(
      gid = seq_len(1),
      euid = NA,
      gap_type = "auto",
      gap_start = input_file$data_summary$end_dt[1],
      gap_end = input_file$data_summary$start_dt[1],
      dt_diff_s = difftime(input_file$data_summary$start_dt[1],
        input_file$data_summary$end_dt[1], units = "secs"),
      gap_problem_thresh_s = gap_problem_thresh_s,
      problem_gap = TRUE,
      Notes = as.character(NA)
    )
    seq_dt <- seq_dt[0, ]
  } else {
    seq_dt <- data.table::data.table(
      gid = seq_len(l - 1),
      euid = NA,
      gap_type = "auto",
      gap_start = input_file$data_summary$end_dt[1:(l - 1)],
      gap_end = input_file$data_summary$start_dt[2:l],
      dt_diff_s = difftime(input_file$data_summary$start_dt[2:l],
        input_file$data_summary$end_dt[1:(l - 1)], units = "secs"),
      gap_problem_thresh_s = gap_problem_thresh_s,
      problem_gap = TRUE,
      Notes = as.character(NA)
    )
  }
  seq_dt[dt_diff_s < gap_problem_thresh_s, "problem_gap"] <- FALSE
  data.table::setkey(seq_dt, gap_start, gap_end)

  # check if there is gap related metadata
  if ("event_data" %in% names(input_file)) {
    # populate event thresh data
    input_file$event_data[
      event_type %in% c("logger_gap_notes", "logger_missing",
        "logger_declare_error_segment")][is.na(event_thresh_s)][
          is.na(start_dt) | is.na(end_dt), "event_thresh_s"] <- event_thresh_s
    # extract manually declared gaps
    man_gaps <- input_file$event_data[event_type ==
      "logger_declare_error_segment" & qa == TRUE]
    man_gaps[is.na(start_dt), "start_dt"] <- man_gaps[is.na(start_dt)
      ]$date_time - man_gaps[is.na(start_dt)]$event_thresh_s
    man_gaps[is.na(end_dt), "end_dt"] <- man_gaps[is.na(end_dt)
      ]$date_time + man_gaps[is.na(end_dt)]$event_thresh_s
    data.table::setkey(man_gaps, start_dt, end_dt)
    man_gaps <- data.table::foverlaps(man_gaps, dta, nomatch = NA,
      mult = "all")
    man_gaps <- man_gaps[!is.na(dta_start) & !is.na(dta_end)]
    man_gaps$start_dt <- data.table::fifelse(
      man_gaps$start_dt < man_gaps$dta_start,
      man_gaps$dta_start, man_gaps$start_dt)
    man_gaps$end_dt <- data.table::fifelse(man_gaps$end_dt > man_gaps$dta_end,
      man_gaps$dta_end, man_gaps$end_dt)
    man_gaps <- subset(man_gaps, select = -c(dta_start, dta_end))
    data.table::setkey(man_gaps, start_dt, end_dt)

    man_gaps_r <- data.table::foverlaps(man_gaps, seq_dt, nomatch = NA,
      mult = "all")
    # define gap type
    man_gaps_seeprate <- man_gaps_r[is.na(gap_start)]
    man_gaps_seeprate <- man_gaps_seeprate[, ":=" (
      gap_type = "seperate", gap_start = start_dt, gap_end = end_dt)]
    man_gaps_seeprate$dt_diff_s <- man_gaps_seeprate$gap_end -
      man_gaps_seeprate$gap_start
    man_gaps_s <- man_gaps_r
    man_gaps_before <- man_gaps_s[start_dt < gap_start,
      gap_type := "before"][!is.na(gap_start)][gap_type == "before"]
    # next need to account for overlap of start dates split the dt by start_dt
    # and assign start dates to gap_start accordingly.
    split_mmgb <- split.data.frame(man_gaps_before, man_gaps_before$uid)
    mgl <- lapply(seq_along(split_mmgb), function(x) {
      mg <- split_mmgb[[x]]
      l <- nrow(mg)
      dt_i <- lapply(seq_len(l), function(z) {
        if (z == 1) {
          mg$gap_start[z] <- split_mmgb[[x]]$start_dt[z]
          mg$gap_end[z] <- split_mmgb[[x]]$gap_start[z] - 1
        }
        if (z > 1) {
          mg$gap_start[z] <- data.table::fifelse(
            split_mmgb[[x]]$start_dt[z] > split_mmgb[[x]]$gap_end[z - 1],
            split_mmgb[[x]]$start_dt[z], split_mmgb[[x]]$gap_end[z - 1] + 1)
          mg$gap_end[z] <- split_mmgb[[x]]$gap_start[z] - 1
        }
        return(mg[z])
      })
      dt_i <- data.table::rbindlist(dt_i)
      return(dt_i)
    })
    split_mmgb <- data.table::rbindlist(mgl)

    man_gaps_after <- man_gaps_s[end_dt > gap_end,
      gap_type := "after"][!is.na(gap_start)][gap_type == "after"]
    split_mga <- split.data.frame(man_gaps_after, man_gaps_after$uid)
    mgl <- lapply(seq_along(split_mga), function(x) {
      mg <- split_mga[[x]]
      l <- nrow(mg)
      dt_i <- lapply(seq_len(l), function(z) {
        if (z < l & l != 1) {
          mg$gap_start[z] <- split_mga[[x]]$gap_end[z] + 1
          mg$gap_end[z] <- data.table::fifelse(
            split_mga[[x]]$end_dt[z] <= split_mga[[x]]$gap_start[z + 1],
            split_mga[[x]]$end_dt[z], split_mga[[x]]$gap_start[z + 1] - 1)
        }
        if (z == l & l != 1) {
          mg$gap_start[z] <- split_mga[[x]]$gap_end[z] + 1
          mg$gap_end[z] <- split_mga[[x]]$end_dt[z]
        }
        if (l == 1) {
          mg$gap_start[z] <- split_mga[[x]]$gap_end[z] + 1
          mg$gap_end[z] <- split_mga[[x]]$end_dt[z]
        }
        return(mg[z])
      })
      dt_i <- data.table::rbindlist(dt_i)
      return(dt_i)
    })
    split_mga <- data.table::rbindlist(mgl)

    # combine gaps
    mg <- rbind(man_gaps_seeprate, split_mmgb, split_mga)
    mg <- transform(mg, euid = uid, Notes = paste0(mg$event_type, ": ",
      mg$notes))
    mg <- mg[, c("gid", "euid", "gap_type", "gap_start", "gap_end", "dt_diff_s",
      "gap_problem_thresh_s", "problem_gap", "Notes")]
    mg <- rbind(mg, seq_dt)[order(gap_start)]
    # a gremlin may creep in here if the end time of the manual gap falls
    # within the next gap---the "before" type captures this gap type fine though
    mg <- unique(mg, by = c("gap_start"))
    mg[, dt_diff_s := gap_end - gap_start]
    mg[gap_type != "auto", "gap_type"] <- "manual"
    mg[, "gap_problem_thresh_s"] <- gap_problem_thresh_s
    mg <- transform(mg, problem_gap = data.table::fifelse(
      dt_diff_s > gap_problem_thresh_s, TRUE, FALSE
    ))
    mg <- mg[gap_type != "auto"]
  } else {
    mg <- seq_dt[0, ]
  }
  gaps <- rbind(seq_dt, mg)
  gaps <- gaps[order(gap_start)]

  # 1. check for problem gaps surrounded by problem gaps
  # 2. append logger gap notes from event data
  if ("event_data" %in% names(input_file)) {
    data.table::setkey(man_gaps, start_dt, end_dt)
    data.table::setkey(gaps, gap_start, gap_end)
    gaps_o <- data.table::foverlaps(gaps, man_gaps, nomatch = NA,
      mult = "all")
    gaps_o_na <- gaps_o[is.na(uid)]
    gaps_o <- split.data.frame(gaps_o[!is.na(uid)], gaps_o[!is.na(uid)]$uid)
    go <- lapply(gaps_o, function(x) {
      x$problem_gap <- data.table::fifelse(
        x$raining == TRUE | x$flag_problem == TRUE, TRUE, FALSE)
      x$euid <- data.table::fifelse(is.na(x$euid), x$uid, x$euid)
      invisible(x)
    })
    go <- data.table::rbindlist(go)
    go <- rbind(go, gaps_o_na)
    go <- go[, c("gid", "euid", "gap_type", "gap_start", "gap_end", "dt_diff_s",
      "gap_problem_thresh_s", "problem_gap", "Notes")][order(gap_start)]
    gaps <- go

    gn <- input_file$event_data[event_type == "logger_gap_notes"]
    gn$dt2 <- gn$date_time
    data.table::setkey(gn, date_time, dt2)
    data.table::setkey(gaps, gap_start, gap_end)
    gno <- data.table::foverlaps(gaps, gn, nomatch = NA,
      mult = "all")
    gno[!is.na(uid), "problem_gap"]  <- data.table::fifelse(
      gno[!is.na(uid), "raining"] != TRUE &
      gno[!is.na(uid), "flag_problem"] != TRUE, FALSE, TRUE
    )
    gno[!is.na(uid), Notes := paste0(event_type, ": ", notes, ". ", Notes)]
    gno[!is.na(uid), ]$Notes  <- gsub(pattern = ". NA", replacement = "",
      x = gno[!is.na(uid), ]$Notes)
    gno <- gno[, names(gaps), with = FALSE]
    gaps <- gno
  }
  gaps$gid <- seq_len(nrow(gaps))

  input_file$gaps <- gaps
  invisible(input_file)
}
