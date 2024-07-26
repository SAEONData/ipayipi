#' @title Update manual calibration measurements
#' @description This function updates the relevant boreholes with
#'  all 'physical' or manual readings used to correct water level
#'  height and drift. This is done by reading in the datum and
#'  log retrieve logs. Syncing with the logs is only done in a
#'  one-way direction, that is, if the R data ground water files are
#'  updated internally -- this will not be transferred to the csv log.
#' @param input_dir The working directory where the R data ground water
#'  files are stored with the retrieve and datum logs.
#' @param recurr Should the function work recurrsively in the parent
#'  directory.
#' @param overwrite_logs Defaults to TRUE which will overwrite all datum
#'  and retrieve log information in the R data ground water files.
#' @param first_dip If this option is TRUE then the dipper readings taken
#'  before the start of the barometrically corrected data are included if
#'  they are within 12 hours of the start of the barometrically corrected data.
#' @param prompted If this is set to TRUE then an on screen prompt will be used
#'  to interactively select files to process in this function.
#' @keywords dipper readings, datum, altitude, calibration, collar height,
#'  casing height
#' @author Paul J. Gordijn
#' @return Screen print out of the R data files that have had their manual
#'  calibration readings and datum atrributes updated.
#' @export
gw_physiCally <- function(
  input_dir = ".",
  recurr = FALSE,
  overwrite_logs = TRUE,
  first_dip = TRUE,
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  ...) {

  dum <- gw_read_datum_log(input_dir = input_dir, recurr = recurr)
  if (is.null(dum) == TRUE) {
    stop("There is no datum_log file")
  }
  rum <- gw_read_retrieve_log(input_dir = input_dir, recurr = recurr)
  if (is.null(dum) == TRUE || is.null(rum) == TRUE) {
    stop("There is no retrieve_log file")
  }
  slist <- ipayipi::dta_list(input_dir = input_dir, recurr = recurr,
    prompt = prompt, wanted = wanted, unwanted = unwanted,
    file_ext = ".rds"
  )
  if (length(slist) == 0) {
    stop("There are no such standardised R water level files in the directory!")
  }

  # Update the logger retrieve and datum info
  cr_msg <- padr(core_message = " updating datum and retrieve logs ",
    pad_char = "=", pad_extras = c("|", "", "", "|"), force_extras = FALSE,
    justf = c(0, 0), wdth = 80
  )
  message(cr_msg)
  dum$zum <- paste0(dum$Location, "_", dum$Borehole)
  dum <- dum[which(dum$Lock == TRUE & dum$QA == TRUE), ]
  rum$zum <- paste0(rum$Location, "_", rum$Borehole_name)
  rum$xum <- gsub("__[^__]+$", "", rum$zum)
  rum <- rum[which(rum$Lock == TRUE & rum$QA == TRUE), ]

  sapply(slist, FUN = function(emm) {
    emma <- sub(x = emm, pattern = ".rds", replacement = "")
    cr_msg <- padr(core_message = paste0(">~ ", emma, " ~<", collapse = ""),
      pad_char = c("-"), pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(-1, 3), wdth = 80
    )
    message(cr_msg)
    emmat <- gsub("__[^__]+$", "", emma)
    dum_temp <- dum[which(dum$zum == emmat), ]
    dum_temp[order(Date_time)][, c("Date_time", "dID")]
    sol_temp <- readRDS(file = file.path(input_dir, emm))

    # Datum update - rolling join to spread datum measurements over
    #  the log_t date_times appropriately
    #  This allows multiple datum measurements per site.
    #  This function only proceeds if there is data in the log_t file
    #  created by barometric compensation.
    if (nrow(sol_temp$log_t) > 0) {
      if (nrow(dum_temp) > 0) {
        log_t_temp <- sol_temp$log_t[, c("Date_time")]
        colnames(log_t_temp) <- "SEtimes"
        log_t_temp$join_time <- log_t_temp$SEtimes
        dum_temp$join_time <- dum_temp$Date_time
        data.table::setkey(log_t_temp, "join_time")
        data.table::setkey(dum_temp, "join_time")
        # the rollends argument determines the behaivour
        #  of the rolling join ends
        j_t <- dum_temp[log_t_temp, roll = TRUE, rollends = c(TRUE, TRUE)]
        # Next get min and max times by dID grouping
        j_t <- j_t[, ":=" (Start = min(SEtimes), End = max(SEtimes)),
          by = "dID"]
        j_t <- j_t[, c("dID", "Start", "End")]
        j_t <- unique(j_t)
        data.table::setkey(j_t, "dID")
        data.table::setkey(dum, "dID")
        j_t <- dum[j_t]
        j_t <- j_t[, c(
          "dID", "Start", "End", "Datum_ESPG", "X", "Y", "H_masl")]
        colnames(j_t)[c(1, 4)] <- c("id", "Datum")
        ddum <- TRUE
      } else {
      ddum <- FALSE
      }
    } else {
      ddum <- FALSE
    }

    #sol_temp$log_retreive
    rum_temp <- rum[which(rum$Event_type == "Borehole readings"), ]
    rum_temp <- rum_temp[order(Date_time)]
    rum_lg_temp <- rum_temp[which(!is.na(rum_temp$Depth_to_logger_m) &
      rum_temp$zum == emma), ]
    rum_lg_temp <- rum_lg_temp[, c("EvID", "Date_time")]
    rum_bh_temp <- rum_temp[xum == emmat][!is.na(Date_time)]
    rum_bh_temp_l <- rum_bh_temp
    rum_bh_temp <- rum_bh_temp[
      which(!is.na(rum_bh_temp$Dipper_reading_m) |
        !is.na(rum_bh_temp$Casing_ht_m)), c("EvID", "Date_time")]
    rum_bh_temp <- rum_bh_temp[!is.na(Date_time)]

    if (nrow(sol_temp$log_t) > 0) {
      if (nrow(rum_bh_temp) > 0) {
        log_t_temp <- sol_temp$log_t[, c("Date_time")]
        colnames(log_t_temp) <- "dttm"
        s_rate <- max(sol_temp$xle_LoggerHeader$Sample_rate_s)
        log_t_temp$dummy_ltt_s <- as.POSIXct(log_t_temp$dttm - 0.5 * s_rate)
        log_t_temp$dummy_ltt_e <- as.POSIXct(log_t_temp$dttm + 0.5 * s_rate)
        rum_bh_temp$dummy_rbt_s <- rum_bh_temp$Date_time
        rum_bh_temp$dummy_rbt_e <- rum_bh_temp$Date_time
        data.table::setkey(log_t_temp, dummy_ltt_s, dummy_ltt_e)
        data.table::setkey(rum_bh_temp, dummy_rbt_s, dummy_rbt_e)
        rum_bh <- # overlap join
          data.table::foverlaps(
            rum_bh_temp, log_t_temp, mult = "first", nomatch = NA)
        rum_bh <- rum_bh[which(!is.na(rum_bh$dttm)), c("dttm", "EvID")]
        colnames(rum_bh) <- c("dttm", "id")
        if (nrow(rum_bh) > 0) {
          data.table::setkey(rum_temp, EvID)
          data.table::setkey(rum_bh, id)
          rum_bh <- rum_temp[rum_bh, ][,
            c("EvID", "dttm", "QA", "Dipper_reading_m",
              "Casing_ht_m", "Depth_to_logger_m", "Notes")]

          # Before we continue here we test the first_dip option
          # If the first (or last) dipper reading is within 12 hrs of
          #  the first baro corrected data in log_t then these
          #  borehole readings are incorporated
          if (first_dip == TRUE) {
            colnames(rum_bh_temp_l)[5] <- "dttm"
            rum_bh_temp_l <- rum_bh_temp_l[,
              c("EvID", "dttm", "QA", "Dipper_reading_m", "Casing_ht_m",
                  "Depth_to_logger_m", "Notes")]
            diffs <- setdiff(rum_bh_temp_l$EvID, rum_bh$EvID)
            rum_bh_temp_l <-
              rum_bh_temp_l[which(rum_bh_temp_l$EvID %in% diffs), ]
              slog_t <- min(sol_temp$log_t$Date_time)
              elog_t <- max(sol_temp$log_t$Date_time)
            for (i in seq_len(nrow(rum_bh_temp_l))) {
              if (difftime(slog_t, rum_bh_temp_l$dttm[i], units = "hours") <=
                12 & difftime(slog_t, rum_bh_temp_l$dttm[i], units = "hours") >
                  0) {
                cr_msg <- padr(core_message = " First_dip applied to first ",
                  pad_char = " ", pad_extras = c("|", "", "", "|"),
                  force_extras = TRUE, justf = c(-1, 3), wdth = 80)
                message(cr_msg)
                rum_bh_temp_l$dttm[i] <- slog_t
                rum_bh <- rbind(rum_bh_temp_l[i, ], rum_bh)
              }
              if (difftime(elog_t, rum_bh_temp_l$dttm[i], units = "hours") >=
                -12 & difftime(slog_t, rum_bh_temp_l$dttm[i], units = "hours")
                < 0) {
                  cr_msg <- padr(core_message = " First_dip applied to last ",
                    pad_char = " ", pad_extras = c("|", "", "", "|"),
                    force_extras = TRUE, justf = c(-1, 3), wdth = 80)
                    message(cr_msg)
                    rum_bh_temp_l$dttm[i] <- elog_t
                    rum_bh <- rbind(rum_bh_temp_l[i, ], rum_bh)
              }
            }
            rum_bh <- rum_bh[order(dttm)]
            rum_bh <- unique(rum_bh, by = "EvID")
          }
        } else {
          rum_bh <- NULL
        }
      } else {
        rum_bh <- NULL
      }
      if (nrow(rum_lg_temp) > 0) {
        # A rolling join will be used for this exercise
        rum_lg_temp$join_time <- rum_lg_temp$Date_time
        log_t_temp <- sol_temp$log_t[, c("Date_time")]
        colnames(log_t_temp) <- "dttm"
        log_t_temp$join_time <- log_t_temp$dttm
        data.table::setkey(rum_lg_temp, join_time)
        data.table::setkey(log_t_temp, join_time)
        # the rollends argument determines the behaivour
        #  of the rolling join ends
        rum_lg_temp <-
          rum_lg_temp[log_t_temp, roll = TRUE, rollends = c(TRUE, TRUE)]
        rum_lg_temp <- rum_lg_temp[, ":=" (Start = min(dttm), End = max(dttm)),
          by = "EvID"]
        rum_lg_temp <- rum_lg_temp[, c("EvID", "Start", "End")]
        rum_lg_temp <- unique(rum_lg_temp)
        data.table::setkey(rum_lg_temp, EvID)
        data.table::setkey(rum_temp, EvID)
        rum_lg <- rum_temp[rum_lg_temp, ][,
          c("EvID", "Start", "QA", "Dipper_reading_m", "Casing_ht_m",
            "Depth_to_logger_m", "Notes")]
        colnames(rum_lg) <- c("EvID", "dttm", "QA", "Dipper_reading_m",
          "Casing_ht_m", "Depth_to_logger_m", "Notes")
      } else {
        rum_lg <- NULL
      }
      if (!is.null(rum_bh) & !is.null(rum_lg)) {
        rumms <- rbind(rum_bh, rum_lg)
        colnames(rumms) <- colnames(sol_temp$log_retrieve)
      }
      if (is.null(rum_bh) & is.null(rum_lg)) {
        rumms <- sol_temp$log_retrieve
        colnames(rumms) <- colnames(sol_temp$log_retrieve)
      }
      if (is.null(rum_bh) & !is.null(rum_lg)) {
        rumms <- rum_lg
        colnames(rumms) <- colnames(sol_temp$log_retrieve)
      }
      if (is.null(rum_lg) & !is.null(rum_bh)) {
        rumms <- rum_bh
        colnames(rumms) <- colnames(sol_temp$log_retrieve)
      }

      rumms <- rumms[order(Date_time)]
      rumms <- unique(rumms, by = c("Date_time", "QA", "Dipper_reading_m",
                                    "Casing_ht_m", "Depth_to_logger_m"))
      rrum <- TRUE
    } else {
      rrum <- FALSE
    }

    if (ddum == TRUE) {
      sol_temp$log_datum <- j_t
      cr_msg <- padr(core_message = "Datum updated", pad_char = " ",
        pad_extras = c("|", "", "", "|"), force_extras = TRUE,
        justf = c(-1, 3), wdth = 80)
      message(cr_msg)
    } else {
      cr_msg <- padr(core_message = "Datum NOT updated", pad_char = " ",
        pad_extras = c("!", "", "", "!"), force_extras = TRUE,
        justf = c(-1, 3), wdth = 80)
      message(cr_msg)
    }

    if (rrum == TRUE) {
      sol_temp$log_retrieve <- rumms
      cr_msg <- padr(core_message = "Log retrieve updated",
        pad_char = " ", pad_extras = c("|", "", "", "|"),
        force_extras = TRUE, justf = c(-1, 3), wdth = 80)
        message(cr_msg)
    } else {
      cr_msg <- padr(core_message = "Log retrieve NOT updated",
        pad_char = " ", pad_extras = c("!", "", "", "!"),
        force_extras = TRUE, justf = c(-1, 3), wdth = 80)
      message(cr_msg)
    }
    saveRDS(sol_temp, file = file.path(input_dir, emm))

  }
  )
  cr_msg <- padr(core_message = " datum and retrieve logs updated ",
    pad_char = "=", pad_extras = c("|", "", "", "|"), force_extras = FALSE,
    justf = c(0, 0), wdth = 80)
  return(message(cr_msg))
}
