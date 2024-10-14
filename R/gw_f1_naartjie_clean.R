#' @title Interpolate and flag outliers plus missing values.
#' @description Function to detect outliers and remove them from the
#'  data plus predict replacement values. Cleaning the
#'  data like this is done sequentially through each
#'  segment of the data as divided by logger download events and missing
#'  data.
#' @param input_dir Working directory where the R data solonist
#'  files are stored.
#' @param recurr Logical. Whether to search for R data files in
#'  recursively.
#' @param seg_na_t Fractional tolerace of the amount of NA values
#'  in a segment for linear interpolation of missing values.
#' @param w_size Window size for the hempel filter. Defaults to ten.
#' @param mad_dev Scalar factor of MAD. Higher values relax oulier detection.
#'  Defaults to the standard of three.
#' @param last_rule Whether or not to reapply stored rules in the outlier
#'  rule column. TRUE will apply old rules
#' @param tighten Scaling fraction between zero and one that sensitizes the
#'  detection of outliers near the head and tail ends of segments. The fraction
#'  is multiplied by the mad_dev factor.
# #' @param check_segs Defaulted to FALSE. TRUE will display
# #'  graphs showing the corrected versus raw data per segment.
#' @param wanted Vector of strings listing files that should not be
#'  included in the import.
#' @param unwanted Vector of strings listing files that should not be included
#'  in the import.
#' @param prompt Logical. Invoke prompt to select which data files in the
#'  working directory are processed.
#' @keywords outlier detection, value interpolation.
#' @export
#' @author Paul J. Gordijn
#' @return A vector of xle file paths.
gw_naartjie_clean <- function(
  input_dir = ".",
  recurr = FALSE,
  seg_na_t = 0.75,
  w_size = 21,
  mad_dev = 3,
  last_rule = FALSE,
  tighten = 0.65,
  # check_segs = TRUE,
  wanted = NULL,
  unwanted = NULL,
  prompt = TRUE,
  verbose = FALSE,
  ...
) {

  slist <- ipayipi::dta_list(input_dir = input_dir, recurr = recurr,
    baros = FALSE, prompt = prompt, wanted = wanted, unwanted = unwanted,
    file_ext = ".rds"
  )

  if (length(slist) == 0) {
    stop("No R data solonist files in the working directory")
  }
  cr_msg <- padr(core_message = paste0(" non-linear anomaly detection ",
      "and interpolation ", collapse = ""
    ), pad_char = "=", wdth = 80, pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0)
  )
  message(cr_msg)
  emmasf <- sapply(slist, FUN = function(emm) {
    ### Function inputs
    emma <- sub(x = emm, pattern = ".rds", replacement = "")
    cr_msg <- padr(
      core_message = paste0(" segmenting ", emma, "... ", collapse = ""),
      pad_char = "-", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(0, 0), wdth = 80
    )
    message(cr_msg)
    sol_temp <- readRDS(file = file.path(input_dir, emm))
    rlog <- sol_temp$log_retrieve[which(sol_temp$log_retrieve$QA == TRUE), ]
    drifting <- sol_temp$log_t

    # Continue with f if there is data in all the tables
    if (nrow(rlog) > 0 & nrow(sol_temp$log_t) > 0) {
      # rlog - Interval join rlog
      rlog_reads_t <- rlog[, c("Date_time")]
      rlog_reads_t$event <- "dips"
      dt <- data.table::data.table(
        Date_time = as.POSIXct(c(min(drifting$Date_time),
          max(drifting$Date_time)
        ))
      )
      dt$event <- "se"
      rlog_reads_t <- rbind(rlog_reads_t, dt)
      # Define intervals of NA values as breakpoints in the data
      drifting_ttt <- drifting[, c("Date_time", "bt_level_m")]
      drifting_ttt$id <- seq_len(nrow(drifting_ttt))
      drifting_ttt$j <- ifelse(is.na(drifting_ttt$bt_level_m),
        drifting_ttt$id, NA
      )
      x <- which(!is.na(drifting_ttt$j))
      if (length(x) > 0) {
        drifting_ttt <- drifting_ttt[x, ]
        drifting_ttt$k <- c(1, drifting_ttt$j[c(2:(nrow(drifting_ttt)))]
          - drifting_ttt$j[c(1:(nrow(drifting_ttt) - 1))]
        )
        drifting_ttt$l <- c(c(drifting_ttt$j[c(1:(nrow(drifting_ttt) - 1))]
            - drifting_ttt$j[c(2:(nrow(drifting_ttt)))]
          ) * -1, 1
        )
        drifting_ttt$l[1] <- 2
        drifting_ttt$l[nrow(drifting_ttt)] <- 2
        dt <- drifting_ttt[which(drifting_ttt$k > 1 | drifting_ttt$l > 1),
          "Date_time"
        ]
        dt$event <- "nd"
        rlog_reads_t <- rbind(rlog_reads_t, dt)
      }
      dt <- data.table::data.table(
        Date_time = as.POSIXct(c(as.POSIXct(sol_temp$xle_FileInfo$Start),
          as.POSIXct(sol_temp$xle_FileInfo$End)
        ))
      )
      dt$event <- "dwn"
      rlog_reads_t <- rbind(rlog_reads_t, dt)
      rlog_reads_t <- rlog_reads_t[order(Date_time), ]
      rlog_reads_t <- unique(rlog_reads_t, by = "Date_time")
      rlog_reads_t$i <- TRUE
      data.table::setkey(drifting, Date_time)
      data.table::setkey(rlog_reads_t, Date_time)
      drifting_tt <- merge(y = rlog_reads_t, x = drifting, all.x = TRUE)

      # Assign each interference event with reading a consecutive number
      segs <- as.numeric(
        seq_len(nrow(drifting_tt[which(drifting_tt$i == TRUE), ])) - 1
      )
      drifting_tt[which(drifting_tt$i == TRUE), "iN"] <- segs
      segs_n <- max(drifting_tt$iN, na.rm = TRUE)

      # If the segment length is one, the segments need to be adjusted
      for (i in seq_len(segs_n)) {
        if (i == 1) {
          r1 <- which(drifting_tt$iN == 0)
          r2 <- which(drifting_tt$iN == 1)
        }
        if (i > 1) {
          r1 <- which(drifting_tt$iN == (i - 1))
          r2 <- which(drifting_tt$iN == i)
        }
        if ((r2 - r1) == 1) {
          if (drifting_tt$event[r1] == "rdu") { # rdu means redundant
            drifting_tt$i[r2] <- NA
            drifting_tt$event[r2] <- "rdu"
          }
          if (drifting_tt$event[r1] == "dwn" | # dwn means download event
                drifting_tt$event[r2] == "dwn") {
            if (drifting_tt$event[r1] == "dwn") {
              drifting_tt$i[r2] <- NA
              drifting_tt$event[r2] <- "rdu"
            } else {
              drifting_tt$i[r1] <- NA
              drifting_tt$event[r1] <- "rdu"
            }
          } else {
            if (is.na(drifting_tt$bt_level_m[r1]) |
                is.na(drifting_tt$bt_level_m[r2])
            ) {
              if (is.na(drifting_tt$bt_level_m[r1])) {
                drifting_tt$i[r1] <- NA
                drifting_tt$event[r1] <- "rdu"
              } else {
                drifting_tt$i[r2] <- NA
                drifting_tt$event[r2] <- "rdu"
              }
            }
          }
        }
      }

      # Assign modified interference event with reading a consecutive number
      segs <- as.numeric(
        seq_len(nrow(drifting_tt[which(drifting_tt$i == TRUE), ])) - 1
      )
      drifting_tt[which(!is.na(drifting_tt$iN)), "iN"] <- NA
      drifting_tt[which(drifting_tt$i == TRUE), "iN"] <- segs
      segs_n <- max(drifting_tt$iN, na.rm = TRUE)

      # Work through each of the naartjie segments and check for outliers
      naarbs <- future.apply::future_lapply(seq_len(segs_n), function(z) {
        if (z == 1) {
          r1 <- which(drifting_tt$iN == 0)
          r2 <- which(drifting_tt$iN == 1)
        }
        if (z > 1) {
          r1 <- which(drifting_tt$iN == (z - 1)) + 1
          r2 <- which(drifting_tt$iN == z)
        }

        tab <- drifting_tt[r1:r2, ]
        cr_msg <- padr(core_message = paste0("seg ", z, ": ",
            tab$Date_time[1], " --> ", tab$Date_time[nrow(tab)], collapse = ""
          ), pad_char = c(" "), pad_extras = c("|", "", "", "|"),
          force_extras = TRUE, justf = c(-1, 2), wdth = 59
        )
        message("\r", appendLF = FALSE)
        message("\r", cr_msg, appendLF = FALSE)
        nasc <- sum(is.na(tab$t_bt_level_m))
        nnasc <- sum(!is.na(tab$t_bt_level_m))
        if (nasc / nnasc <= seg_na_t & !is.infinite(nasc / nnasc)) {
          should_i <- TRUE
        } else {
          should_i <- FALSE
        }
        tab_ts <- tab
        if (nrow(tab_ts) > 3 & should_i == TRUE) {
          # hampel rule import
          if (last_rule == TRUE) {
            rule <- tab_ts$bt_Outlier_rule[1]
          } else {
            rule <- NA
          }
          if (is.factor(rule)) rule <- as.character(rule)
          if (is.na(rule)) {
            msg <- paste0(" Dflt rule: ", "hf_", w_size, "_", mad_dev, " |")
            message(msg, appendLF = TRUE)
          } else {
            msg <- paste0(" Last rule: ", rule, " |")
            message(msg, appendLF = TRUE)
            rule <- strsplit(rule, "_")
            w_size <- as.integer(rule[[1]][2])
            x_devs <- as.integer(rule[[1]][3])
          }

          tab_ts_cleen_f <- function() {
            cleen_seg <- hampel_f(
              srs = tab_ts[, c("Date_time", "bt_level_m")],
              w_width = w_size,
              x_devs = mad_dev,
              tighten = tighten
            )
            return(cleen_seg)
          }
          cleen_seg <- tab_ts_cleen_f()

          # need to change this return to a cleaned segment
          tab_ts_cleen <- data.table::data.table(
            Date_time = as.POSIXct(cleen_seg$hamper$Date_time),
            t_bt_level_m = as.numeric(cleen_seg$hamper$hf),
            bt_Outlier = as.logical(cleen_seg$hamper$bt_Outlier),
            bt_Outlier_rule = as.factor(
              rep(paste0("hf_", w_size, "_", mad_dev),
                nrow(cleen_seg$hamper)
              )
            )
          )
        } else {
          tab_ts_cleen <- data.table::data.table(
            Date_time = as.POSIXct(tab_ts$Date_time),
            t_bt_level_m = as.numeric(rep(NA, nrow(tab_ts))),
            bt_Outlier = as.logical(rep(NA, nrow(tab_ts))),
            bt_Outlier_rule = as.factor(rep(NA, nrow(tab_ts)))
          )
          message(" Too many NAs to clean segment.", appendLF = TRUE)
        }
        if (nrow(tab_ts_cleen) != nrow(tab_ts)) {
          message("------------------------------FUNKY")
        }
        return(tab_ts_cleen)
      })

      naarbs <- data.table::rbindlist(naarbs)

      ## Prepare to save the output
      sol_temp$log_t$t_bt_level_m <- naarbs$t_bt_level_m
      sol_temp$log_t$bt_Outlier <- naarbs$bt_Outlier
      sol_temp$log_t$bt_Outlier_rule  <- naarbs$bt_Outlier_rule
      ## Flag manually detected outliers
      for (i in seq_len(nrow(sol_temp$log_t_man_out))) {
        start <- as.POSIXct(as.character(sol_temp$log_t_man_out$Start[i]))
        end <- as.POSIXct(as.character(sol_temp$log_t_man_out$End[i]))
        sol_temp$log_t[
          which(sol_temp$log_t$Date_time >= start &
              sol_temp$log_t$Date_time <= end
          ), "bt_Outlier"
        ] <- TRUE
        sol_temp$log_t[
          which(sol_temp$log_t$Date_time >= start &
              sol_temp$log_t$Date_time <= end
          ), "t_bt_level_m"
        ] <- NA
      }
      saveRDS(sol_temp, file.path(input_dir, emm))
    }
    cr_msg <- padr(core_message = paste0(" naartjies consumed ", collapse = ""),
      pad_char = "=", wdth = 80, pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(0, 0)
    )
    message(cr_msg)
  }
  )
}