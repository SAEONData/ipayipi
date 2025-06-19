#' @title Dev function: Linear drift corrections based on calibration readings.
#' @description Function under development. Uses the medians of defined window sizes, around calibration points to perform linear dirift correction for a variable .
#' @inheritParams logger_data_import_batch
#' @param sfc  List of file paths to the temporary station file directory. Generated using `ipayipi::sf_open_con()`.
#' @param station_file Name of the station being processed.

#' @param ppsij Data processing `pipe_seq` table from which function parameters and data are extracted/evaluated. This is parsed to this function automatically by `ipayipi::dt_process()`.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param xtra_v Logical. Whether or not to report xtra messages, progess, plus print data tables.
#' @param chunk_v Logical. Print data chunking messages. Useful for debugging/digging into chunking methods.
#' @keywords outlier detection, value imputation, univariate data,
#' @details This function employs a user-customised hampel filter to check data for outliers and impute missing values with the median of the filter window. Function under development.
#' - Need to add generated table details to the phen_ds summary table!
#' @export
#' @author Paul J. Gordijn
dt_drift <- function(
  phen = NULL,
  wside = 3,
  wside_max = 12,
  robust = TRUE,
  edge_adj = NULL,
  pipe_house = NULL,
  station_file = NULL,
  ppsij = NULL,
  f_params = NULL,
  sfc = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  ...
) {
  "%ilike%" <- "%chin%" <- ":=" <- ".N" <- "." <- NULL
  "date_time" <- "drift_cal" <- "table_name" <- "dttm_orig" <- "dttm_move" <-
    "cal_val_orig" <- "cal_val_move" <- "row_ns" <- "dip_nz" <- "row_ccs" <-
    "possi" <- "cal_offset" <- "t_p" <- "p" <- "dum_drift" <- "q2" <- "q3" <-
    "q4" <- NULL

  # set default args (f_params) from ppsij
  #  - generate a table for this purpose
  # determine how far back to reach when opening data
  # open data
  #  - filter bt start and end dttm
  #  - check for phens
  #  - subset data by phens
  # loop through runs with froll
  #  - generate outlier dt tbl for each sequential loop
  #    this needs to be sequential where the next run depends on
  #    a cleaned version of the last [do while]
  # perform the tighten arg
  # save data and outlier dt tbl

  # prep data read ----
  sfcn <- names(sfc)
  dta_in <- NULL
  hrv_dta <- NULL
  if ("dt_working" %in% sfcn) {
    dta_in <- sf_dta_read(sfc = sfc, tv = "dt_working")
    ng <- sf_dta_read(sfc = sfc, tv = "gaps")[["gaps"]][
      table_name %in% ppsij$output_dt[1]
    ]
  }

  if (is.null(dta_in) && any(sfcn %ilike% "_hrv_tbl_")) {
    pstep <- paste0("^", ppsij$dt_n[1], "_.+_hrv_tbl_")
    hrv_dta <- sfcn[sfcn %ilike% pstep]
    hrv_dta <- hrv_dta[length(hrv_dta)]
    hrv_dta <- hrv_dta[order(as.integer(
      sapply(strsplit(hrv_dta, "_"), function(x) x[2])
    ))]
    dta_in <- sf_dta_read(sfc = sfc, tv = hrv_dta)
    ng <- dta_in[[1]]$gaps
    ng$table_name <- ppsij$output_dt[1]
  }

  # eindx filter
  # filter from last two drift cal vals
  dcal <- sf_dta_read(sfc = sfc, tv = "dt_dcal")[["dt_dcal"]]
  if (is.null(dcal)) {
    ppsij$start_dttm <- NA
  } else {
    ppsij$start_dttm <- dcal[(.N - 1):.N]$dttm_orig[1]
  }
  dta_in_o <- dt_dta_filter(dta_link = dta_in, ppsij = ppsij)
  dta_in <- dta_in_o

  # prep drift args ----
  z_default <- data.table::data.table(phen = NA_character_,
    wside = NA_integer_, wside_max = NA_integer_, robust = NA
  )[0]
  f_params <- ppsij$f_params
  z <- lapply(f_params, function(x) {
    x <- eval(parse(text = sub("^~", "data.table::data.table", x)))
    x
  })
  z <- rbind(z_default, data.table::rbindlist(z, fill = TRUE), fill = TRUE)
  z$table_name <- ppsij$output_dt
  z$robust <- data.table::fifelse(is.na(z$robust), robust, z$robust)
  ri <- dta_in_o[[1]][[1]]$indx$ri

  # to account for receiving batches of data --- not whole data sets...
  # generate drift cal table (dcal_tbl)
  # filter to last drift_cal value OR drift cal on all
  # *zipper start - if present
  # zipper end - if present
  # dummy drift corrections
  #  generate auto dummy drift readings if is.na phen vals

  # *zipper = st and end buffers for joining drift cal values
  dcal_tbl <- data.table::data.table(
    dttm_orig = as.POSIXct(NA_character_),
    dttm_move = as.POSIXct(NA_character_),
    cal_val_orig = NA_real_,
    cal_val_move = NA_real_
  )[0]
  dcal <- rbind(dcal_tbl, dcal)
  # code here to filter data based on what has been processed and
  # the latest drift recordings, then open data.
  # code code code
  # open data ----
  dt <- dt_dta_open(dta_link = dta_in[[1]])
  if (xtra_v || chunk_v) {
    cli::cli_inform(c("i" = "Pre-clean data -- head"))
    print(head(dt))
  }

  # check for calibration column
  if (!"drift_cal" %chin% names(dt)) {
    cli::cli_abort(paste0("Drift correction requires calibration ",
      "measurements in column \'drift_cal\'"
    ))
  }
  # gen tmp phen col 'p' for syntax
  dt[, p := phen, env = list(phen = z$phen[1])]

  if (nrow(dt[!is.na(p)]) == 0) {
    cli::cli_inform(c(
      "!" = "No non-NA values for drift correction of {.var z$phen[1]}",
      " " = "Errors may follow in further calculations ..."
    ))
    return(list(ppsij = ppsij))
  }

  # update and/or make new drift cal table
  dcal_new <- dt[!is.na(drift_cal), .(date_time, drift_cal)][,
    ":="(dttm_orig = date_time, dttm_move = date_time, cal_val_orig = drift_cal,
      cal_val_move = drift_cal
    )
  ][, .(dttm_orig, dttm_move, cal_val_orig, cal_val_move)]

  # get params for drift correction
  dr <- lubridate::as.duration(z$edge_adj[1])

  # cheak head and tail cal vals and see if there is data for calibration
  # seq through drift cal vals
  #  - where they fall on na phen values mark dum_drift as TRUE
  #   - adj drift cal vals with linear geom if necessary
  #     else just move if with edje_adj
  #     BUT must not move drift cal val within edge adj period of next
  #     drift cal val.
  drws <- dt[!is.na(drift_cal)]
  # confirm if there are drift cal values before filtered dt data
  dcal$old <- TRUE   #  combine dcal and dcal_new
  dcal_new$old <- FALSE
  dcalw <- rbind(dcal, dcal_new)
  # check if there are any dcal values before the current ones
  dcv <- drws$date_time
  q0 <- any(dcalw[dttm_orig < dcv[1]]$old)

  dps <- lapply(seq_along(dcv), function(i) {
    drr <- dt[date_time >= dcv[i] - dr & date_time <= dcv[i] + dr]
    # check if phen val for dcv is na. If it is need to:
    #  a. generate dumdrift val, or
    #  b. move the drift cal val
    # questions
    # is the phen where cal val is na?
    q1 <- is.na(drr[date_time == dcv[i]]$p)
    # are there more than one cal vals in drr?
    q2 <- nrow(drr[!is.na(drift_cal)]) > 1
    # determine the closest !is.na p val to dcv[i] = 'drw'
    if (all(is.na(drr$p))) return(NULL)
    drrv <- abs(drr[!is.na(p)]$date_time - dcv[i])
    drw <- drr[!is.na(p)][sapply(drrv, function(x) x == min(drrv))]
    # check if there is no cal vals in drw
    q3 <- is.na(drw$drift_cal)
    # check that there are no dum drift indicators in drw
    q4 <- is.na(drw$dum_drift)

    # if:
    # !q0 - there are no preceeding cal vals,
    #  q1 - p (phen val) is na, &
    #   i == 1, this is the first cal val
    #  then b. --- move it
    if (all(!q0, q1, i == 1)) {
      drw$drift_cal <- drr[date_time == dcv[i]]$drift_cal
      dcw <- dcalw[dttm_orig == dcv[i]][dttm_move == drw$date_time]
      drm <- drr[date_time == dcv[i]]
      drm$drift_cal <- NA_real_
      drw <- rbind(drm, drw)
      return(list(i = i, drw = drw, dcw = dcw, drr = drr))
    }
    # if:
    # q0 - there are preceeding drift cal vals,
    # q1 - p (phen val) is na, &
    # !i %in% length(dcv) - this is not the last cal val
    #  then a. --- make dum drift == TRUE
    if (all(!q0, q1, !i %in% length(dcv))) {
      drw$dum_drift <- TRUE
      list(i = i, drw = drw, dcw = dcalw[dttm_orig == dcv[i]], drr = drr)
    }
  })
  dps <- dps[!sapply(dps, is.null)]
  # replace old dt rows with new dummy drift ones and shifted cal vals
  new_rws <- data.table::rbindlist(lapply(dps, function(x) x$drw))
  dt <- dt[!date_time %in% new_rws$date_time]
  dt <- rbind(dt, new_rws)
  dt <- dt[order(date_time)]

  # estimate dummy drift values using euclidean geom
  dtdum <- dt[!is.na(drift_cal) | dum_drift == TRUE]
  dtdum <- dtdum[!date_time < min(dtdum[!is.na(drift_cal)]$date_time)][
    !date_time > max(dtdum[!is.na(drift_cal)]$date_time)
  ]
  dum_i <- which(dtdum$dum_drift == TRUE & is.na(dtdum$drift_cal))
  dpd <- lapply(seq_along(dum_i), function(q) {
    y1_qndx <- dum_i[q] - 1
    y2_qndx <- dum_i[q] + 1
    y1 <- dtdum$drift_cal[y1_qndx]
    y2 <- dtdum$drift_cal[y2_qndx]
    # shift row indices for x and y until non-na drift cal values are
    # detected
    while (is.na(y1) & y1_qndx > 1) {
      y1_qndx <- y1_qndx - 1
      y1 <- dtdum$drift_cal[y1_qndx]
    }
    while (is.na(y2) & y2_qndx <= nrow(dtdum)) {
      y2_qndx <- y2_qndx + 1
      y2 <- dtdum$drift_cal[y2_qndx]
    }
    x1 <- as.numeric(dtdum$date_time[y1_qndx])
    x2 <- as.numeric(dtdum$date_time[y2_qndx])
    # Euc geometry to estimate dumm drift cal point
    B <- (y2 - y1) / (x2 - x1)
    C <- y1 - (B * x1)
    y <- B * as.numeric(dtdum$date_time[dum_i[q]]) + C
    dtdum[dum_i[q]]$drift_cal <- y
    dtdum[dum_i[q]]
  })
  dpd <- data.table::rbindlist(dpd)

  # replace old dumdrift rows with ones with dummy drift cal vals in
  dt <- dt[!date_time %in% dpd$date_time]
  dt <- rbind(dt, dpd)[order(date_time)]

  # drift correction ----
  # assign each dipper reading a consecutive number
  cal_n_tot <- nrow(dt[!is.na(drift_cal) & !is.na(p)])
  dt[, ":="(cal_offset = NA_real_, t_p = NA_real_, cal_n = NA_integer_)]
  dt[!is.na(drift_cal) & !is.na(p), "cal_n"] <- seq_len(cal_n_tot)

  # estimate cal val offsets
  callbs <- lapply(seq_len(cal_n_tot),
    function(w, d_tab = dt, rng_side_in = z$wside[1],
      rng_max_in = z$wside_max[1]
    ) {
      c_row <- which(d_tab$cal_n == w)
      cval <- d_tab[which(d_tab$cal_n == w), ]$drift_cal

      # Get the centre callibration value
      if (c_row >= 1) {
        win_row_v <- win_rows(
          dta = d_tab$p, rng_side = rng_side_in,
          rng_max = rng_max_in, c_row = c_row, pos = "c",
          include_center = TRUE
        )
      } else {
        win_row_v <- NULL
      }

      if (!is.null(win_row_v)) {
        if (robust == TRUE) {
          medi <- stats::median(d_tab[win_row_v]$p, na.rm = TRUE)
          if (is.na(medi)) medi <- NaN
        } else {
          medi <- mean(d_tab[win_row_v]$p, na.rm = TRUE)
        }
        cal_val_c <- cval - medi
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
          win_rows(dta = d_tab$p, rng_side = rng_side_in,
            rng_max = rng_max_in, c_row = l_row, pos = "l",
            include_center = FALSE
          )
      } else {
        win_row_v <- NULL
      }

      if (!is.null(win_row_v)) {
        if (robust == TRUE) {
          medi <- stats::median(d_tab[win_row_v]$p, na.rm = TRUE)
          if (is.na(medi)) medi <- NaN
        } else {
          medi <- mean(d_tab[win_row_v]$p, na.rm = TRUE)
        }
        cal_val_l <- cval - medi
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
          win_rows(dta = d_tab$p, rng_side = rng_side_in,
            rng_max = rng_max_in, c_row = t_row, pos = "t",
            include_center = FALSE
          )
      } else {
        win_row_v <- NULL
      }

      if (!is.null(win_row_v)) {
        if (robust == TRUE) {
          medi <- stats::median(d_tab[win_row_v]$p, na.rm = TRUE)
          if (is.na(medi)) medi <- NaN
        } else {
          medi <- mean(d_tab[win_row_v]$p, na.rm = TRUE)
        }
        cal_val_t <- cval - medi
      } else {
        cal_val_t <- cal_val_c
      }

      row_ns <- c(l_row, c_row, t_row)
      possi <- c("l", "c", "t")
      dip_nz <- c(w, w, w)
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
        cbind(row_ns, possi, dip_nz, row_ccs)
      )

      return(cal_val_tab)
    }
  )
  callbs <- data.table::rbindlist(callbs)
  callbs[, ":="(row_ns = as.integer(row_ns), possi = as.character(possi),
      dip_nz = as.integer(dip_nz), row_ccs = as.double(row_ccs)
    )
  ][!is.na(row_ns)][!is.na(row_ccs)]

  # push cal values to dt
  dt[callbs$row_ns, "cal_offset"] <- callbs$row_ccs
  dt[callbs$row_ns, "possi"] <- callbs$possi

  # set drift cal to na if there is no cal offset, then
  # set possi to na if no cal offset
  dt[, ":="(drift_cal = data.table::fifelse(
    !is.na(drift_cal) & is.na(cal_offset), NA_real_, drift_cal
  ), possi = data.table::fifelse(
    !is.na(drift_cal) & is.na(cal_offset), NA_character_, possi
  ))]

  # get sequence of drift correction position patterns
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
      ht_diff <- dt$cal_offset[ac_e] - dt$cal_offset[ac_s]
      t_diff <- as.numeric(dt$date_time[ac_e]) -
        as.numeric(dt$date_time[ac_s])
      mu <- ht_diff / t_diff # gradient
      # y intercept
      c <- dt$cal_offset[ac_s] -
        mu * as.numeric(dt$date_time[ac_s])
      t_seri <- as.numeric(dt$date_time[ac_s:ac_e])
      dt$cal_offset[ac_s:ac_e] <- mu * t_seri + c
    }
  }

  # now perform drift offsets for the head lead and
  #   head tail of they exist
  if (callbs$possi[1] == "l") {
    ac_s <- callbs$row_ns[1]
    df_s <- 1
    dt$cal_offset[df_s:ac_s] <- dt$cal_offset[ac_s]
  }
  if (callbs$possi[length(callbs$possi)] == "t") {
    ac_e <- callbs$row_ns[length(callbs$row_ns)]
    df_e <- nrow(dt)
    dt$cal_offset[ac_e:df_e] <- dt$cal_offset[ac_e]
  }
  dt[, t_p := p + cal_offset]
  dt[, phen := t_p, env = list(phen = z$phen[1])]
  dt <- dt[, names(dt)[!names(dt) %in% c("t_p", "p")], with = FALSE]

  # save working data ----
  sf_dta_rm(sfc = sfc, rm = "dt_working")
  sl <- sf_dta_wr(dta_room = file.path(dirname(sfc)[1], "dt_working"),
    dta = dt, tn = "dt_working", ri = ppsij$time_interval[1],
    chunk_v = chunk_v
  )
  sf_log_update(sfc = sfc, log_update = sl)
  return(list(ppsij = ppsij))
}