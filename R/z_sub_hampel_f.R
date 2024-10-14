#' @title Hampel outlier detection and filter
#' @description Applies the non-linear hampel filter to detect ourliers and
#'  produce estimates for interpolation of missing or outlier values.
#'  The window size used to detect outliers and perform interpolation is
#'  customizable. The window formation or relative position of the window
#'  to a center value is adjustable.
#' @param srs Input data in the form of a data table or data frame with one
#'  heading as the "bt_level_m" and the other date-time series as POSIXT date
#'  time (not time series).
#' @param w_width The desired width of the window used to evaluate a central
#'  value.
#' @param x_devs The number of deviations from the central tendacy of the
#'  window used to detect outlier values.
#' @param tighten multiplier of the _x_devs_ parameter only applied at
#'  vector sides or begining and end. Designed to increase the sensitivity of
#'  outlier detection at the vector sides.
#' @keywords outlier detection; interpolation; missing values; patching;
#' @export
#' @author Paul J Gordijn
#' @return list containing tables including the interpolated values and outlier
#'  flags.
#' @details This function is key to avoiding correcting drift based on outlier
#'  values.
hampel_f <- function(
  srs = NULL,
  w_width = 21,
  x_devs = 3,
  tighten = 1,
  ...
) {
  # organise input data
  dttm <- srs$Date_time
  srs <- srs$bt_level_m

  # functions to be used in rolling function
  median_f <- function(x) stats::median(x, na.rm = TRUE)
  mad_f <- function(x) stats::mad(x, na.rm = TRUE)

  # adjust w_width if necessary
  if (length(srs) <= 5) w_width <- 3
  if (w_width > length(srs)) w_width <- as.integer(length(srs) / 2)
  # make w_width odd
  if (w_width %% 2 == 0) w_width <- w_width - 1

  # for beinging of vector use left aligned
  if ((2 * w_width) > length(srs)) {
    w_factor <- length(srs)
  } else {
    w_factor <- (2 * w_width)
  }
  slice_s <- c(1:w_factor)
  srs_mad <- data.table::frollapply(
    srs[slice_s], n = w_width, fill = NA, FUN = mad_f, align = "left"
  )
  srs_median <- data.table::frollapply(
    srs[slice_s], n = w_width, fill = NA, FUN = median_f, align = "left"
  )
  srs_return <- ifelse(
    srs[slice_s] > srs_median + (x_devs * tighten * srs_mad) |
      srs[slice_s] < srs_median - (x_devs * tighten * srs_mad),
    srs_median, srs[slice_s]
  )
  srs_return <- ifelse(is.na(srs_return), srs_median, srs_return)
  srs_dt_start <- data.table::data.table(
    hf = as.numeric(srs_return),
    org = as.numeric(srs[slice_s]),
    n_dev = as.numeric(srs[slice_s] - x_devs * tighten * srs_mad),
    p_dev = as.numeric(srs[slice_s] + x_devs * tighten * srs_mad)
  )

  # for end of vector use right aligned
  slice_e <- c((length(srs) - w_factor + 1): length(srs))
  srs_mad <- data.table::frollapply(
    srs[slice_e], n = w_width, fill = NA, FUN = mad_f, align = "right"
  )
  srs_median <- data.table::frollapply(
    srs[slice_e], n = w_width, fill = NA, FUN = median_f, align = "right"
  )
  srs_return <- ifelse(
    srs[slice_e] > srs_median + (x_devs * tighten * srs_mad) |
      srs[slice_e] < srs_median - (x_devs * tighten * srs_mad),
    srs_median, srs[slice_e]
  )
  srs_return <- ifelse(is.na(srs_return), srs_median, srs_return)
  srs_dt_end <- data.table::data.table(
    hf = as.numeric(srs_return),
    org = as.numeric(srs[slice_e]),
    n_dev = as.numeric(srs[slice_e] - x_devs * tighten * srs_mad),
    p_dev = as.numeric(srs[slice_e] + x_devs * tighten * srs_mad)
  )

  # for middle of vector use left aligned
  slice_m <- c((((w_width - 1) / 2) + 1): (length(srs) - ((w_width - 1) / 2)))
  srs_mad <- data.table::frollapply(
    srs, n = w_width, fill = NA, FUN = mad_f, align = "center"
  )
  srs_median <- data.table::frollapply(
    srs, n = w_width, fill = NA, FUN = median_f, align = "center"
  )
  srs_return <- ifelse(
    srs > srs_median + (x_devs * srs_mad) |
      srs < srs_median - (x_devs * srs_mad),
    srs_median, srs
  )
  srs_return <- ifelse(is.na(srs_return), srs_median, srs_return)
  srs_dt_mid <- data.table::data.table(
    hf = as.numeric(srs_return[slice_m]),
    org = as.numeric(srs[slice_m]),
    n_dev = as.numeric(srs[slice_m] - x_devs * srs_mad[slice_m]),
    p_dev = as.numeric(srs[slice_m] + x_devs * srs_mad[slice_m])
  )
  srs_hf <- rbind(srs_dt_start[c(1:((w_width - 1) / 2))], srs_dt_mid,
    srs_dt_end[
      c((nrow(srs_dt_end) + 1 - ((w_width - 1) / 2)):(nrow(srs_dt_end)))
    ]
  )
  srs_hf <- data.table::data.table(
    Date_time = as.POSIXct(dttm),
    hf = as.numeric(srs_hf$hf),
    org = as.numeric(srs_hf$org),
    n_dev = as.numeric(srs_hf$n_dev),
    p_dev = as.numeric(srs_hf$p_dev)
  )
  srs_hf$bt_Outlier <- as.logical(ifelse(srs_hf$hf != srs_hf$org, TRUE, FALSE))

  your_hamper <- list(srs_hf)
  names(your_hamper) <- c("hamper")

  return(your_hamper)
}