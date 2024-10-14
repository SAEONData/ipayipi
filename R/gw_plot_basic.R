#' @title Visualisation of GW corrections
#' @description This function uses ggplot functionality to plot and visualize corrected and uncorrected water level data.
#' @details In addiion to the basic plot of corrected water level data, using ggplot this function adds basic information that may be used to interpret whether the corrections performed on the data are valid.
#' @param file A level file object read into R.
#' @param log_dwd If TRUE this adds interference events, that is, the start and stop plus logger logger download annotations.
#' @param dips If TRUE this will annotate dip level readings.
#' @param drift_neutral If TRUE the barometrically corrected data with a constant drift correction factor calculated with the mean of the drift offset.
#' @param ly If TRUE the transformed water level is plotted in the interactive plotly interface.
#' @param time_labs_y The position on the y-axis on which the time labels are drawn for the interference events.
#' @param zoom_x Vector of start and end date times to limit x axis e.g. c("2018-03-12", "2018-04-11").
#' @param focus_y If TRUE, the y axis is limited scaled to focus on the corrected data.
#' @param date_labels Default format is %Y-%m-%d.
#' @param y_break_inc Optional specification of increment between tick marks.
#' @param date_breaks Optional increment of date time axis tick mark increments,
#' @author Paul J. Gordijn
#' @keywords Ground water visualisation; graph
gw_vis <- function(
  file = NULL,
  log_dwd = TRUE,
  dips = TRUE,
  drift_neutral = TRUE,
  ly = FALSE,
  time_labs_y = NULL,
  zoom_x = NULL,
  focus_y = TRUE,
  date_labels = "%Y-%m-%d",
  y_break_inc = NULL,
  date_breaks = NULL,
  ...
  ) {

  ymn <- min(file$log_t$t_level_m, na.rm = TRUE)
  ymn <- ymn - (0.0025 * ymn)
  ymx <- max(file$log_t$t_level_m, na.rm = TRUE)
  ymx <- ymx + (0.0025 * ymn)
  if (!is.null(y_break_inc)) {
    ymn <- round(ymn / y_break_inc) * y_break_inc
    ymx <- round(ymx / y_break_inc) * y_break_inc
  }
  if (is.null(time_labs_y)) {
    time_labs_y <- ymn
  }

  p1 <- ggplot(file$log_t) +
    aes(x = Date_time, y = t_level_m) +
    xlab("Date-time") + ylab("Water level (m a.s.l.)")
  p2 <- geom_point(color = "lightblue", alpha = 0.6, na.rm = TRUE)
  p3 <- geom_line(color = "black", na.rm = TRUE)
  p4 <- ggtitle(
          label = "Ground water time series",
          subtitle = paste0("Borehole: ",
          file$xle_LoggerHeader$Location[1], " ",
          file$xle_LoggerHeader$Project_ID[1],
          ". Outlier rule: ",
          as.character(file$log_t$bt_Outlier_rule)))
  gg0 <- p1 + p2 + p3 + p4


  if (log_dwd == T) {
    # Add the lines indicating when the data was downloaded
    d <- data.frame(dt = as.POSIXct(unique((file$xle_FileInfo$Date_time))))
    d$lab <- as.character(d$dt)
    dwd_dt <- geom_vline(data = d,
      mapping = aes(xintercept = dt),
      color = "navyblue", size = 1.2, na.rm = TRUE)
    dwd_dt_l <- geom_text(data = d,
      mapping = aes(x = dt, y = time_labs_y, label = lab),
      size = 2, angle = 90, vjust = -0.4, hjust = 0,
      check_overlap = TRUE, na.rm = TRUE)

    # Add lines indicating when the logger was set to start
    d <- data.frame(dt = as.POSIXct(unique((file$xle_FileInfo$Start))),
      lab = as.character(as.POSIXct(unique((file$xle_FileInfo$Start)))))
    dwd_dtS <- geom_vline(data = d,
      mapping = aes(xintercept = dt),
      color = "darkgreen", linetype = "twodash",
      size = 2, na.rm = TRUE)
    dwd_dtS_l <- geom_text(data = d,
      mapping = aes(x = dt, y = time_labs_y, label = lab),
      size = 2, angle = 90, vjust = -0.4, hjust = 0,
      check_overlap = T, na.rm = TRUE)
    d <- data.frame(dt = as.POSIXct(unique((file$xle_FileInfo$End))),
      lab = as.character(as.POSIXct(unique((file$xle_FileInfo$End)))))
    dwd_dtE <- geom_vline(data = d,
      mapping = aes(xintercept = dt),
      color = "darkred", size = 1, na.rm = TRUE)
    dwd_dtE_l <- geom_text(data = d,
      mapping = aes(x = dt, y = time_labs_y, label = lab),
      size = 2, angle = 90, vjust = -0.4, hjust = 0,
      check_overlap = T, na.rm = TRUE)
    gg0 <- p1 + dwd_dt + dwd_dt_l + dwd_dtE + dwd_dtE_l +
      dwd_dtS + dwd_dtS_l + p2 + p3 + p4
  }
  if (dips == TRUE & log_dwd == TRUE) {
    d <- data.frame(dt =
      as.POSIXct(unique((subset(file$log_retrieve$Date_time,
      file$log_retrieve$QA == TRUE &
      !is.na(file$log_retrieve$Dipper_reading_m))))))
    d$lab <- as.character(d$dt)
    if (nrow(d) > 0) {
      dip <- geom_vline(data = d,
        mapping = aes(xintercept = dt),
        color = "yellow", linetype = "dotted",
        na.rm = TRUE, size = 2)
      dip_l <- geom_text(data = d,
        mapping = aes(x = dt, y = time_labs_y,
        label = lab), size = 2, angle = 90, vjust = -0.4,
        hjust = 0, check_overlap = TRUE, na.rm = TRUE)
      gg0 <- p1 + dwd_dt + dwd_dt_l + dwd_dtE + dwd_dtE_l +
        dwd_dtS + dwd_dtS_l + dip + dip_l + p2 + p3 + p4
      dip_l_p <- TRUE
    } else dip_l_p <- FALSE
  }
  if (drift_neutral == TRUE) {
    p5 <- geom_point(mapping = aes(x = Date_time,
      y = ifelse(file$log_t$bt_Outlier == TRUE,
        mean(file$log_t$Drift_offset_m, na.rm = TRUE) +
        file$log_t$bt_level_m,
        rep(NA, length(file$log_t$Drift_offset_m)))),
      color = "red", alpha = 0.6, na.rm = TRUE)
    p6 <- geom_point(mapping = aes(x = Date_time,
      y = mean(Drift_offset_m, na.rm = TRUE) + t_bt_level_m),
      color = "darkgray", alpha = 1, na.rm = TRUE)
    gg0 <- p1 + p6 + p5 + p2 + p3 + p4
  }
  if (dips == TRUE & log_dwd == TRUE &
    drift_neutral == TRUE & dip_l_p == TRUE) {
    dta <- unique(file$log_retrieve[which(file$log_retrieve$QA == T &
      !is.na(file$log_retrieve$Dipper_reading_m)), ])
    lvs <- file$log_t[which(file$log_t$Date_time %in% dta$Date_time), ]
    dts <- dta[which(lvs$Date_time %in% dta$Date_time), ]
    ids <- dts$id
    dip <- geom_vline(xintercept = lvs$Date_time,
      color = "yellow", linetype = "dotted",
      na.rm = TRUE, size = 2)
    dip_id_bk <- geom_label(data = lvs,
      mapping = aes(x = lvs$Date_time, y = lvs$t_level_m, label = ids),
      color = "darkgrey", na.rm = T, angle = 45, size = 2, alpha = 0.8,
      label.size = 0.2, fontface = "bold",
      label.padding = unit(0.1, "lines"))
    gg0 <- p1 + dwd_dt + dwd_dt_l + dwd_dtE + dwd_dtE_l +
      dwd_dtS + dwd_dtS_l + dip + dip_l +
      p6 + p5 + p2 + p3 + p4 + dip_id_bk
  }
  if (ly == TRUE) gg0 <- plotly::ggplotly(gg0)

  # Theming gg0

  if (is.null(date_breaks)) {
    gg0 <- gg0 + scale_x_datetime(date_labels = date_labels)
    if (!is.null(zoom_x)) {
      gg0  <- gg0 + scale_x_datetime(date_labels = date_labels,
        limits = c(as.POSIXct(as.character(zoom_x[1])),
          as.POSIXct(as.character(zoom_x[2]))))
    }
  } else {
    gg0 <- gg0 +
      scale_x_datetime(date_labels = date_labels, date_breaks = date_breaks)
  }

  if (focus_y == TRUE) {
    gg0 <- gg0 + ylim(ymn, ymx)
    y_breaks <- seq(ymn, ymx, y_break_inc)
    gg0  <- gg0 + scale_y_continuous(breaks = y_breaks, limits = c(ymn, ymx))
  } else {
    if (!is.null(y_break_inc)) {
      y_breaks <- seq(ymn, ymx, y_break_inc)
      gg0  <- gg0 + scale_y_continuous(breaks = y_breaks, limits = c(ymn, ymx))
      }
  }
  gg0  <- gg0 + theme(axis.text.x = element_text(angle = 90, hjust = 0)) +
    theme(plot.background = element_rect(colour = "grey50"))
  return(gg0)
}