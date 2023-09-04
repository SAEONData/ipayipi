#' @title Create rainfall data table and flag 'false tips'
#' @description Identifies likely erraneous rainfall recordings. The tips
#'  of a tipping bucket raingauge are classed as `FALSE` if they occur
#'  within a specified time period, called the `event threshold', around an
#'  interference event, e.g., when the data from the logger was downloaded.
#'  In the process of identifying false tips the 'interference event' table
#'  is updated or created and a rainfall data table created after removing
#'  'false tips'.
#' @param input_file A data object standardised as a SAEON 'ipayipi' hobo
#'  rainfall file.
#' @param false_tip_thresh The time buffer in seconds around logger
#'  interferance events (e.g., logger connections/downloads) in which any
#'  tips (logs) should be considered false. The default used by SAEON is ten
#'  minutes, that is, 10x60=600 seconds. *Event thresholds for other types
#'  of `false tips` are specified during metadata collection.
#' @keywords hoboware, tipping bucket rain gauge, data cleaning, false tips
#' @author Paul J. Gordijn
#' @return Object of class "SAEON_rainfall_data" with an updated 'fasle_tip'
#'  table.
#' @details
#'  1. Creates table of false tips
#'    - "auto" false tips include tips within the threshold of a "logged"
#'       event where there was 'interference with the logger'.
#'    - "auto_double_tip".
#'    - "manual" false tips imported from
#'  1. Checks for an event table---check for rain events and manually declared
#'   false tips.
#'  1. Generates rainfall table
#'  1. Warning printout
#' @export
rain_false_tips <- function(
  input_file = NULL,
  false_tip_thresh = 10 * 60,
  ...
) {
  if (class(input_file) != "SAEON_rainfall_data") {
    stop("Standardised ipayipi hobo rainfall data ojbect required!")
  }

  hb <- input_file$tip_data[order(date_time)]
  hb$tid <- seq_len(nrow(hb))

  # update the logg_interfere table
  # auto extract start and end events
  hbs <- hb
  hbs$lev <- data.table::fifelse(hbs$id == 1, "start", as.character(NA))
  hbs$lev <- data.table::fifelse(c(hbs$id[2:nrow(hbs)], 0) == 1,
    "end", hbs$lev)
  hbs[!is.na(lev)]
  hbs[lev == "end"]
  li <- input_file$logg_interfere
  li$lid <- seq_len(nrow(li))
  li <- transform(li,
    li_id = id,
    ev_id = rep(NA, nrow(li)),
    false_tip_type = rep("auto", nrow(li)),
    raining = rep(FALSE, nrow(li)),
    event_thresh = false_tip_thresh)
  li <- li[, -c("id")]

  # read in event table for manually declared false tips
  if ("event_data" %in% names(input_file)) {
    # query events and check false tips
    # - manually declared false tips --- "manual"
    # - if "raining" overwrite auto false tips if raining durning
    #    interference event
    evts <- c("logger_program", "logger_download")
    evs <- input_file$event_data
    evs <- evs[event_type %in% evts & qa == TRUE]
    evs$event_thresh_s <- data.table::fifelse(is.na(evs$event_thresh_s),
      false_tip_thresh, evs$event_thresh_s)
    evs$date_time_mid <- data.table::fifelse(is.na(evs$date_time),
      evs$start_dt + evs$event_thresh_s, evs$date_time)
    evs$start_dt <- data.table::fifelse(is.na(evs$start_dt),
      evs$date_time - false_tip_thresh, evs$start_dt)
    evs$end_dt <- data.table::fifelse(is.na(evs$end_dt),
      evs$date_time + false_tip_thresh, evs$end_dt)
    evs <- evs[, c("uid", "date_time_mid", "event_type", "event_thresh_s",
      "start_dt", "end_dt", "raining", "flag_problem")]
    data.table::setkey(evs, start_dt, end_dt)
    lii <- li
    lii$dtt1 <- lii$date_time - false_tip_thresh
    lii$dtt2 <- lii$date_time + false_tip_thresh
    lii <- lii[, -c("raining")]
    data.table::setkey(lii, dtt1, dtt2)
    evsj <- data.table::foverlaps(evs, lii, nomatch = NA, mult = "all",
      type = "any")
    # missed/missaligned logger interference events
    warns <- evsj[is.na(dtt1)][, c("uid", "event_type", "date_time_mid",
      "event_thresh_s", "start_dt", "end_dt", "raining", "flag_problem")]
    if (nrow(warns) > 0) {
      message(paste0("Warning! There are missing or misaligned logger ",
        "interference events!", collapse = ""))
      message(paste0("Check that the event date-time values are",
        " correct!", collapse = ""))
      message(paste0("Cross-check using the event id column (uid)",
        collapse = ""))
      print(warns)
    }
    # remove li rows repeated in evsj to avoid replication
    li <- li[!lid %in% evsj[!is.na(dtt1)]$lid]
    evsj <- transform(evsj[!is.na(dtt1)],
      event_thresh = evsj[!is.na(dtt1)]$event_thresh_s,
      ev_id = evsj[!is.na(dtt1)]$uid
    )
    evsj <- evsj[, names(evsj)[names(evsj) %in% names(li)], with = FALSE]
    li <- rbind(li, evsj)
    li <- li[order(lid)]

    evts <- c("logger_false_tip")
    evs <- input_file$event_data
    evs <- evs[event_type %in% evts & qa == TRUE & flag_problem == TRUE]
    evs[is.na(start_dt) | is.na(end_dt), ":=" (
      start_dt = date_time - false_tip_thresh,
      end_dt = date_time + false_tip_thresh)][, -c("location_station",
        "battery", "memory_used", "data_recorder", "data_capturer")]
    data.table::setnames(evs, c("date_time", "raining"),
      c("daaate_time", "pouring"))
    data.table::setkey(evs, start_dt, end_dt)
  }

  li <- transform(li,
    dtt = date_time,
    t0 = date_time - event_thresh,
    t1 = date_time + event_thresh)
  li <- li[, -c("date_time")]
  data.table::setkey(li, t0, t1)

  # not rainfall tip, i.e., is a 'false tip' if ... :
  #  - within threshold time of a logged event (and not raining)
  #  - a "logged" event
  #  - cumm_rain is zero
  #  - cumm_rain equals cumm_rain in previous row

  # join on logger interference events (li)
  hb <- transform(hb, t3 = hb$date_time, t4 = hb$date_time)
  data.table::setkey(hb, t3, t4)
  hb <- data.table::foverlaps(hb, li, mult = "all", nomatch = NA)
  # join data summary info to hb
  ds <- input_file$data_summary[, c("start_dt", "end_dt", "tip_value_mm")]
  data.table::setkey(ds, start_dt, end_dt)
  hb <- data.table::foverlaps(hb, ds, mult = "all", nomatch = NA)

  # identify true tips caused by rainfall by checking if cumm_rain changed
  # in next value, if so, then real acummulation is TRUE
  hb[which(!is.na(hb$cumm_rain)), "tip_event"] <- c(NA, ifelse(
    hb[!is.na(cumm_rain)]$cumm_rain[2:nrow(hb[!is.na(cumm_rain)])] !=
      hb[!is.na(cumm_rain)]$cumm_rain[1:(nrow(hb[!is.na(cumm_rain)]) - 1)],
        TRUE, FALSE))
  # where cumm rain is zero there has been no 'tip'
  hb[which(hb$cumm_rain == 0), "tip_event"] <- FALSE
  # # check if it is a "logged" event (i.e., not a tip)
  # hb[which(hb$detached == "logged" | hb$attached == "logged" |
  #   hb$host == "logged" | hb$file_end == "logged"), "tip_event"]  <- FALSE
  # allocate false tips
  hb$false_tip <- data.table::fifelse(!is.na(hb$false_tip_type) &
    hb$tip_event == TRUE & hb$raining != TRUE, TRUE, FALSE)

  # join on manually declared false tip values and remove these
  # then detect and remove double tips ---> add to false tip table
  if ("event_data" %in% names(input_file)) {
    hb <- hb[, -c("start_dt", "end_dt")]
    hb_names <- names(hb)
    hb <- data.table::foverlaps(hb, evs, mult = "all", nomatch = NA)
    # declare type of false tip
    hb[is.na(pouring), "pouring"] <- FALSE
    hb$false_tip_type <- data.table::fifelse(
      hb$event_type == "logger_false_tip" & is.na(hb$false_tip_type) &
      hb$tip_event == TRUE, "manual", hb$false_tip_type)
    hb$raining <- data.table::fifelse(hb$pouring == TRUE, TRUE, hb$raining)
    hb[is.na(raining), "raining"] <- FALSE
    hb[which(hb$false_tip_type == "manual" & hb$raining == FALSE),
      "false_tip"] <- TRUE
    hb$ev_id <- data.table::fifelse(hb$false_tip_type == "manual",
      hb$uid, hb$ev_id)
    hb <- subset(hb, select = hb_names)
  }
  hb$false_tip <- data.table::fifelse(is.na(hb$false_tip) &
    hb$tip_event == TRUE, FALSE, hb$false_tip)

  # continue from here make false tip table and new rain data
  hb1 <- hb[order(date_time)]
  # remove double tips
  hb1$double_tips <- c(FALSE, data.table::fifelse(
    abs(c(hb1$date_time[2:nrow(hb1)]) -
      c(hb1$date_time[1:(nrow(hb1) - 1)])) == 1, TRUE, FALSE))
  hb1$false_tip_type <- data.table::fifelse(
    hb1$tip_event == TRUE & hb1$double_tips == TRUE,
    "auto_double_tip", hb1$false_tip_type)
  # hb1$false_tip_type <- data.table::fifelse(
  #   is.na(hb1$false_tip_type) & hb1$tip_event == TRUE &
  #   hb1$double_tips == TRUE, "auto_double_tip", hb1$false_tip_type)
  hb1[which(hb1$false_tip_type == "auto_double_tip"), "false_tip"] <- TRUE
  hb0 <- hb1[false_tip == FALSE & tip_event == TRUE,
    c("id", "date_time", "tip_value_mm")]
  data.table::setnames(hb0, "tip_value_mm", "rain_mm")
  input_file$rain_data <- hb0[order(date_time)][!is.na(rain_mm)]

  fttb_names <- c("id", "date_time", "false_tip_type", "raining",
    "event_thresh")
  if ("notes" %in% names(hb1)) {
    fttb_names <- c(fttb_names, "notes")
  }
  false_tip_table <- hb1[tip_event == TRUE & false_tip == TRUE,
    ..fttb_names][order(date_time)]
  input_file$false_tip_table <- false_tip_table
  invisible(input_file)
}