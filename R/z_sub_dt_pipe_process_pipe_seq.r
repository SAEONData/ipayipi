#' @title Presentation of pipe processes in sequence
#' @description Accepts a list of parameters corresponding to pipeline
#'  processing steps, each compiled by `ipayipi::p_dt`.
#' @param pipe_eval Logical. If TRUE then basic checks are done on the
#'  processing steps to ensure all parameters are filled and correct.
#' @author Paul J. Gordijn
#' @export
#' @return A standardised data table of class "pipe_seq".
pipe_seq <- function(
  p = NULL,
  pipe_eval = TRUE,
  output_dt_preffix = "dt_",
  output_dt_suffix = NULL,
  ...
  ) {

  # generate a pipe_seq table from pipe steps
  # parameters
  p <- lapply(p, function(x) {
    x <- data.table::as.data.table(x)
    x$n <- seq_len(nrow(x))
    data.table::setcolorder(x, neworder = c(
      "dt_n", "dtp_n", "n", "f", "f_params", "input_dt",
      "output_dt", "time_interval")
    )
    return(x)
  })
  p <- data.table::rbindlist(p, fill = TRUE)
  p$start_dttm <- as.POSIXct(NA_character_)
  p$end_dttm <- as.POSIXct(NA_character_)
  p <- split.data.frame(p, f = factor(p$dt_n))

  # evaluate and standardise pipeline options
  p <- lapply(seq_along(p), function(i) {
    pii <- p[[i]]
    # harvesting must be first step
    pij <- pii[dt_n == 1 & dtp_n == 1 & !f %in% "dt_harvest"]
    pij <- sapply(seq_along(nrow(pij)[!nrow(pij) %in% 0]), function(ki) {
      message(paste0("The first step (\"dtp_n\" = 1) of each table ",
        "processing stage (\"dt_n\") must begin with harvesting data.",
        collapse = ""))
      message("Use \"dt_harvest()\" function.")
      message(paste0("When \"dt_n\" > 1 the pipeline absorbs data ",
        "from the end of stage one processing (\"dt_n\" == 1).",
        collapse = ""))
      stop("Harvest error", call. = FALSE)
    })

    # dt_agg specifics
    # only one 'dt_agg' per stage
    pij <- pii[f == "dt_agg" & n == 1]
    pij <- sapply(seq_along(nrow(pij)[!nrow(pij) %in% 0]), function(ki) {
      warning(
        "Set only one data aggregation per processeing stage (\"dt_n\")")
      stop(, call. = FALSE)
    })

    # check time_intervals
    # only one time_interval per stage
    pij <- pii[!is.na(time_interval)]$time_interval
    pij <- sapply(
      seq_along(unique(pij))[!seq_along(unique(pij)) %in% 1], function(x) {
        m <- paste0("More than one \'time_interval\' in stage (n) ", i,
          ": ", paste0(unique(pij), collapse = ", "), collapse = "")
        stop(m, call. = FALSE)
    })

    # check for duplicate output_dt table names
    pij <- pii[!is.na(output_dt)]$output_dt
    pijt <- sapply(
      seq_along(unique(pij))[!seq_along(unique(pij)) %in% 1], function(x) {
        m <- paste0("More than one \'output_dt\' in stage (n) ", i,
          ": ", paste0(unique(pij), collapse = ", "), collapse = "")
        warning(m, call. = FALSE)
        return(pij[1])
    })

    invisible(pii)
  })
  p <- data.table::rbindlist(p, fill = TRUE)
  
      # fill in blank time intervals
    ti <- unique(pii[!is.na(time_interval)]$time_interval)
    fsf <- c("dt_calc_chain", "dt_agg", "dt_join")
    pii[is.na(time_interval) & f %in% fsf]$time_interval <- ti
    # inherit output_dt from first instance
    pii$output_dt <- pii[!is.na(pii$output_dt)]$output_dt[1]

  q <- subset(p, select = c(dt_n, output_dt, time_interval))
  q$dt_n <- q$dt_n + 1
  names(q) <- c("dt_n", "new_name", "new_ti")
  q[p, on = "dt_n", roll = TRUE, mult = "first"]
  q$time_interval
  # last checks
  for (i in seq_along(p)) {
    # if there is no input data in a stage set this to the last stage output
    if (is.na(p[[i]]$input_dt[1])) {
      p[[i]]$input_dt <- p[[(i - 1)]]$output_dt[1]
    }
    # as for above but with the time interval
    if (is.na(p[[i]]$time_interval[1])) {
      p[[i]]$time_interval <- p[[(i - 1)]]$time_interval[1]
    }
  }
  # check that na values input and output tables & the time interval
  for (i in seq_along(p)) {
    for (j in seq_along(unique(p[[i]]$dtp_n))) {
      if (is.na(p[[i]][dtp_n == j]$input_dt[1])) {
        p[[i]][dtp_n == j]$input_dt <-
          p[[i]][dtp_n == (j - 1)]$output_dt[1]
      }
    }
  }
  p <- data.table::rbindlist(p, fill = TRUE)

  if (is.null(p) && is.null(pipe_seq_dt)) p <- NULL
  class(p) <- c(class(p), "pipe_seq")
  return(p)
}