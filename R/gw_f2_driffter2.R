#' @title A function to correct for __instrument drift__.
#' @description The function requires that manual water level, that is,
#'  dipper readings to calculate drift. Drift is assumued to be linear.
#'  The function also requires that datum information has been
#'  appended to the level logger file before drift correction.
#' @param input_dir The working directory were where the R data
#'  solonist files are stored.
#' @param recurr Setting this to TRUE will enable searching
#'  for R data files in sub directories i.e., a recursive search
#' @param rng_side The window side (half window) for calculation
#'  drift offsets. The size of this range represents the size of
#'  the number of point measurements around a central value which
#'  is used to estimate drift correction.
#' @param rng_max If there are NA values in the window determined by
#'  __rng_side__ the window side size will expand until reaching this
#'  maximum value.
#' @param method driffter2() differs from gw_driffter() in that it includes
#'  a method for simple drift correction. Ironically, the simple method
#'  was designed to be more effective for data series with high frequency
#'  large amplitude patterns. The methods are: "mean", "median", and
#'  "fv". "fv" stands for face value.
#' @param prompted If TRUE, a command line prompt will be used to
#'  enable selection of which files in the working directory require
#'  drift correction.
#' @details Linear drift correction may not be as straight forward as
#'  may be anticipated. This function works in stages at each point in
#'  the water level data series where there are corresponding
#'  calibration measurements.
#'  Firstly, at each dipper reading, 'windows' around the central water
#'  level data point, which corresponds to the (nearest) time when the
#'  dipper reading was taken, are manufactured. The size of a window size
#'  is specified using the 'rng_side' paramenter. The amount by which the
#'  central water level data point is offset is estimated by calculating
#'  the difference in the calibration value and the median or mean (see
#'  'robust' parameter) water level of the specified window. If there is
#'  'leading' and 'tailing' data around the central water level value,
#'  the window size is equal to two times the 'rng_side' parameter. If
#'  there is only leading or trailing data, the window size is equal to
#'  'rng_side' (plus one for the central window value).
#'  Note that if there is missing data in the 'leading' or 'training' data
#'  windows, the window can be set to expand, until sufficient non-null data
#'  is found. The expansion of the window in search of non-null data is set by
#'  the 'rng_max' parameter.
#'  Once the offset values are estimated for each central data point,
#'  graph theory, that is, linear algebra is used to esimtate offsets for
#'  each data point in the full data series. The gradient and y-intercept for
#'  this is estimated using 'half-windows' in segments of the data series
#'  between each central data point (which correspond to when calibration
#'  measures were taken). Determination of window sizes and indicies (i.e.,
#'  index number in the time series) is done using the 'win_rows' function.
#'  For wiggly data (high variability) it is better to used smaller windows
#'  for drift correstion. More needs to be done using this function to explore
#'  the effectiveness of this drift correction method. It would be good to
#'  include a dynamic window size based on the rate of change in water level,
#'  that is, the first derivative.
#' @export
#' @keywords drift correction; linear drift;
#' @return R solonist object with corrected outlier values and flags indicating
#'  outlier values plus the ourlier rule used.

driffter2 <- function(
  input_dir = ".",
  recurr = FALSE,
  rng_side = 3,
  rng_max = 5,
  robust = TRUE,
  prompted = TRUE,
  ...) {

  slist <- gw_rsol_list(
    input_dir = input_dir,
    recurr = recurr,
    baros = FALSE,
    prompt = prompted)
  if (length(slist) == 0) {
      stop("There are no R data solonist files in the working directory")
  }
  cr_msg <- padr(core_message = paste0(" linear drift correction ",
      collapse = ""), pad_char = "=", pad_extras = c("|", "", "", "|"),
      force_extras = TRUE, justf = c(0, 0), wdth = 80)
  message(cr_msg)
  sapply(slist, FUN = function(emm) {
    ### Function inputs
    emma <- sub(x = emm, pattern = ".rds", replacement = "")
    cr_msg <- padr(core_message = paste0(" ", emma, " ", collapse = ""),
      pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(-1, 3), wdth = 80)
    message(cr_msg)
    sol_temp <- readRDS(file = file.path(input_dir, emm))
    dtum <- sol_temp$log_datum
    rlog <-
      sol_temp$log_retrieve[
        which(sol_temp$log_retrieve$QA == TRUE
          & !is.na(sol_temp$log_retrieve$Dipper_reading_m)
          & !is.na(sol_temp$log_retrieve$Casing_ht_m)), ]
    drifting <- sol_temp$log_t

    # Continue with f if there is data in all the tables
    if (nrow(dtum) > 0 & nrow(rlog) > 0 & nrow(sol_temp$log_t) > 0) {
      # Datum - Interval join rlog
      drifting$dummy_s <- drifting$Date_time
      drifting$dummy_e <- drifting$Date_time
      data.table::setkey(drifting, dummy_s, dummy_e)
      data.table::setkey(dtum, Start, End)
      drifting_t <- data.table::foverlaps(drifting, dtum,
        mult = "first", nomatch = NA)
      drifting_tt <- drifting_t[,
        c("Date_time", "ut1_level_m", "level_kpa", "bt_level_m", "bt_Outlier",
          "bt_Outlier_rule", "t_bt_level_m", "Drift_offset_m", "t_Outlier",
          "t_Outlier_rule", "t_level_m", "H_masl")]

      # Casing heights and Dipper readings offsets
      rlog_reads <- rlog[
        which(!is.na(rlog$Dipper_reading_m) & !is.na(rlog$Casing_ht_m)), ]
      rlog_reads$dips <- rlog_reads$Dipper_reading_m - rlog_reads$Casing_ht_m
      rlog_reads_t <- rlog_reads[, c("Date_time", "dips")]
      rlog_reads_t <- unique(rlog_reads_t, by = "Date_time")
      drifting_ttt <- merge(y = rlog_reads_t, x = drifting_tt,
                              all.x = TRUE, by = "Date_time")
      drifting_t <- drifting_ttt[, c("Date_time", "ut1_level_m", "level_kpa",
          "bt_level_m", "bt_Outlier", "bt_Outlier_rule", "t_bt_level_m",
          "Drift_offset_m", "t_Outlier", "t_Outlier_rule", "t_level_m",
          "H_masl", "dips"), with = TRUE]

      # Combine dipper reading, casing hts and H_asl
      #  to get a calibration estimate
      drifting_t[which(!is.na(drifting_t$dips)), "m_level_m"] <-
        drifting_t[which(!is.na(drifting_t$dips)), ]$H_masl -
          drifting_t[which(!is.na(drifting_t$dips)), ]$dips

      # Assign each dipper reading a consecutive number
      drifting_t[which(!is.na(drifting_t$dips)), "dip_n"] <-
        seq_len(nrow(drifting_t[which(!is.na(drifting_t$dips)), ]))

      # Estimate the difference between the sensor measured water level
      #  and the manual dip measurement at each dip_d

      # The number of dipper readings to be used as calibrations
      dippers <- max(drifting_t$dip_n, na.rm = TRUE)

      drifting_t <- transform(drifting_t,
        dip_offset = as.numeric(rep(NA, nrow(drifting_t))))

      # Work through each of the dipper readings and generate calibration
      #  values for the rows above and below the calibration values
      #  Cushions for the possibility of no rows above or below
      callbs <-
        lapply(seq_len(dippers),
          function(z, d_tab=drifting_t, rng_side_in=rng_side,
            rng_max_in=rng_max) {
            c_row <- which(d_tab$dip_n == z)
            dip <- d_tab[which(d_tab$dip_n == z), ]$m_level_m

            # Get the centre callibration value
            if (c_row >= 1) {
              win_row_v <-
                win_rows(dta = d_tab$t_bt_level_m, rng_side = rng_side_in,
                  rng_max = rng_max_in, c_row = c_row, pos = "c",
                  include_center = TRUE)
            } else {
              win_row_v <- NULL
            }

            if (!is.null(win_row_v)) {
              if (robust == TRUE) {
                medi <- stats::median(
                  d_tab[c(win_row_v[1]:win_row_v[2]),
                    ]$t_bt_level_m, na.rm = TRUE)
                if (is.na(medi)) medi <- NaN
              } else {
                medi <- mean(d_tab[c(win_row_v[1]:win_row_v[2]),
                  ]$t_bt_level_m, na.rm = TRUE)
              }
              cal_val_c <- dip - medi
            } else {
              cal_val_c <- NA
            }

            # Get calibration value for the leading row
            if ((c_row - 1) >= 1) {
              l_row <- c_row - 1
            } else {
              l_row <- NA
            }

            if (!is.na(l_row)) {
              win_row_v <-
                win_rows(dta = d_tab$t_bt_level_m, rng_side = rng_side_in,
                  rng_max = rng_max_in, c_row = l_row, pos = "l",
                  include_center = FALSE)
            } else {
              win_row_v <- NULL
            }

            if (!is.null(win_row_v)) {
              if (robust == TRUE) {
                medi <- stats::median(
                  d_tab[c(win_row_v[1]:win_row_v[2]),
                    ]$t_bt_level_m, na.rm = TRUE)
                if (is.na(medi)) medi <- NaN
              } else {
                medi <- mean(d_tab[c(win_row_v[1]:win_row_v[2]),
                  ]$t_bt_level_m, na.rm = TRUE)
              }
              cal_val_l <- dip - medi
            } else {
              cal_val_l <- cal_val_c
            }

            # Get the calibration value for the trailing row
            if ((c_row + 1) <= nrow(d_tab)) {
              t_row <- c_row + 1
            } else {
              t_row <- NA
            }

            if (!is.na(t_row)) {
              win_row_v <-
                win_rows(dta = d_tab$t_bt_level_m, rng_side = rng_side_in,
                  rng_max = rng_max_in, c_row = t_row, pos = "t",
                  include_center = FALSE)
            } else {
              win_row_v <- NULL
            }

            if (!is.null(win_row_v)) {
              if (robust == TRUE) {
                medi <- stats::median(d_tab[c(win_row_v[1]:win_row_v[2]),
                    ]$t_bt_level_m, na.rm = TRUE)
                if (is.na(medi)) medi <- NaN
              } else {
                medi <- mean(d_tab[c(win_row_v[1]:win_row_v[2]),
                    ]$t_bt_level_m, na.rm = TRUE)
              }
              cal_val_t <- dip - medi
            } else {
              cal_val_t <- cal_val_c
            }

            row_ns <- c(l_row, c_row, t_row)
            possi <- c("l", "c", "t")
            dip_nz <- c(z, z, z)
            # If the leading or trailing calibration value is missing
            #  fill it in with the center cal value
            if (!is.na(cal_val_c)) {
              if (is.na(cal_val_l)) {
                cal_val_l <- cal_val_c
              }
              if (is.na(cal_val_t)) {
                cal_val_t <- cal_val_c
              }
            }
            row_ccs <- c(cal_val_l, cal_val_c, cal_val_t)
            cal_val_tab <- data.table::as.data.table(
              cbind(row_ns, possi, dip_nz, row_ccs))

          return(cal_val_tab)
      })

      callbs <- data.table::rbindlist(callbs)
      callbs <- transform(callbs,
        row_ns = as.integer(callbs$row_ns),
        possi = as.character(callbs$possi),
        dip_nz = as.integer(callbs$dip_nz),
        row_ccs = as.double(callbs$row_ccs))
      callbs <- callbs[which(!is.na(callbs$row_ns)), ]
      callbs <- callbs[which(!is.nan(callbs$row_ccs)), ]

      # Transfer callibration value to the drifting_t table
      drifting_t[c(callbs$row_ns), "dip_offset"] <- callbs$row_ccs
      drifting_t[c(callbs$row_ns), "possi"] <- as.character(callbs$possi)

      # Set dip_n to NA if the dip_offset is.na
      drifting_t[c(callbs$row_ns), "dip_n"] <-
        ifelse(is.na(drifting_t[c(callbs$row_ns), "dip_offset"]),
          NA, drifting_t[c(callbs$row_ns), ]$dip_n)
      # Set possi to NA if there is no dip_offset
      drifting_t[c(callbs$row_ns), "possi"] <-
        ifelse(is.na(drifting_t[c(callbs$row_ns), "dip_offset"]),
          NA, drifting_t[c(callbs$row_ns), ]$possi)
      possi_cat <- callbs$possi
      possi_cat <- capture.output(cat(possi_cat, sep = ""))
      possi_cat <- unlist(gregexpr(possi_cat, pattern = "tl"))

      # Calculate the drift off sets between each tail and lead
      if (possi_cat[1] != -1) {
        for (dof in seq_along(possi_cat)) {
        pc_s <- possi_cat[dof]
        pc_e <- pc_s + 1
        ac_s <- callbs$row_ns[pc_s]
        ac_e <- callbs$row_ns[pc_e]
        ht_diff <- drifting_t$dip_offset[ac_e] - drifting_t$dip_offset[ac_s]
        t_diff <-
          as.numeric(drifting_t$Date_time[ac_e]) -
          as.numeric(drifting_t$Date_time[ac_s])
        mu <- ht_diff / t_diff # gradient
        # y intercept
        c <- drifting_t$dip_offset[ac_s] -
          mu * as.numeric(drifting_t$Date_time[ac_s])
        t_seri <- as.numeric(drifting_t$Date_time[ac_s:ac_e])
        drifting_t$dip_offset[ac_s:ac_e] <- mu * t_seri + c
        }
      }

      # Now perform drift offsets for the head lead and
      #  head tail of they exist
      if (callbs$possi[1] == "l") {
        ac_s <- callbs$row_ns[1]
        df_s <- 1
        drifting_t$dip_offset[df_s:ac_s] <- drifting_t$dip_offset[ac_s]
      }
      if (callbs$possi[length(callbs$possi)] == "t") {
        ac_e <- callbs$row_ns[length(callbs$row_ns)]
        df_e <- nrow(drifting_t)
        drifting_t$dip_offset[ac_e:df_e] <- drifting_t$dip_offset[ac_e]
      }

      # Save the output
      sol_temp$log_t$Drift_offset_m <- drifting_t$dip_offset
      sol_temp$log_t$t_level_m <-
        sol_temp$log_t$t_bt_level_m + sol_temp$log_t$Drift_offset_m
      saveRDS(sol_temp, file.path(input_dir, emm))
    }
  }
  )
    cr_msg <- padr(core_message = paste0(" drift corrected ",
      collapse = ""), pad_char = "=", pad_extras = c("|", "", "", "|"),
      force_extras = TRUE, justf = c(0, 0), wdth = 80)
    message(cr_msg)
}
