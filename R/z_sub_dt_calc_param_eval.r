#' @title Organise sequencial processing of phenomena aggregations.
#' @description An alias for list in ipayipi::dt_calc. Used for sequence of
#'  `ipayipi::chainer()` calculations.
#' @author Paul J. Gordijn
#' @export
calc_param_eval <- function(
  full_eval = FALSE,
  f_params = NULL,
  f_summary = NULL,
  ppsij = NULL,
  sf = sf,
  ...
  ) {
  "phen_name" <- NULL
  x <- list(...)
  # x <- list(
  #   t_lag = chainer(
  #     dt_syn_ac = 'date_time + lubridate::as.period(1, unit = \"secs\")'),
  #   false_tip = chainer(
  #     dt_syn_ac = "fifelse(date_time == t_lag, TRUE, FALSE)",
  #     temp_var = FALSE, measure = "smp", units = "false_tip",
  #     var_type = "lg"),
  #   false_fork = chainer(dt_syn_bc = "false_tip == FALSE",
  #     fork_table = "false_tips")
  # )
  if (!any(class(f_params) %in% "dt_calc_string") && full_eval == FALSE) {
    # evaluate chain and parse text
    v_names <- names(x)
    dt_parse <- sapply(seq_along(v_names), function(ii) {
      xii <- paste0("[",
        # before 1st comma
        x[[v_names[ii]]]$dt_syn_bc, ", ",
        # after 1st comma
        if (!is.null(x[[v_names[ii]]]$dt_syn_ac)) {
          paste0(v_names[ii], " := ", collapse = "")
        } else {
          ""
        },
        # after 2nd comma
        x[[v_names[ii]]]$dt_syn_ac,
        if (!is.null(x[[v_names[ii]]]$dt_syn_exc)) ", " else "",
        x[[v_names[ii]]]$dt_syn_exc, "]")

        # add in additional arguments between chains
        x_names <- names(x[[ii]])
        # check the basics are there
        essnt_params <- c("measure", "units", "var_type")
        m <- essnt_params[!essnt_params %in% x_names]
        if (length(m) > 0 && length(m) < 3) {
          stop(paste("Missing parameters:", paste(m, collapse = ", ")))
        }
        essnt_params <- c(
          "measure", "units", "var_type", "notes", "fork_table")
        # assign null values for those not there
        ms_params <- essnt_params[!essnt_params %in% x_names]
        xsyn <- x[[ii]][x_names %in% essnt_params]
        for (q in seq_along(ms_params)) xsyn[ms_params[q]] <- NA
        xsyn <- xsyn[essnt_params]
        xsyn <- xsyn[!is.na(xsyn)]
        if (length(xsyn) > 0) {
          z <- paste0(names(xsyn), " = \"", xsyn, "\"", collapse = ", ")
          z <- paste("ipip(", z, ")", collapse = "", sep = "")
          if (length(unlist(grep(pattern = ", ]", x = xii))) > 0) {
            xii <- gsub(pattern = ", ]", replacement = paste0(", ", z, "]"),
              x = xii)
          } else {
            xii <- gsub(pattern = "]", replacement = paste0(", ", z, "]"),
              x = xii)
          }
        }
      if (ii == 1) {
        xii <- paste0("dt", xii)
      }
      return(xii)
    })
    class(dt_parse) <- c(class(dt_parse), "dt_calc_string")
  } else {
    # full evaluation of chainer with providing phenmoena
    # extract chains from ppsij
    if (is.null(f_params)) {
      idt <- as.list(ppsij$f_params)
    } else {
      s <- unlist(gregexpr(pattern = "\\[", text = f_params))
      e <- unlist(gregexpr(pattern = "\\]", text = f_params))
      idt <- lapply(seq_along(s), function(si) {
        si <- substr(x = f_params, start = s[si], stop = e[si])
        return(si)
      })
    }
    xtras <- lapply(idt, function(si) {
      if (grepl(pattern = "ipip\\(", x = si)) {
        s <- unlist(gregexpr(pattern = "ipip\\(", text = si))
        e <- unlist(gregexpr(pattern = "\\]", text = si))
        si <- substr(x = si, start = s, stop = e)
        si <- gsub(pattern = "\\]", replacement = "", x = si)
        si <- gsub(pattern = "ipip", replacement = "list", x = si)
        si <- eval(parse(text = si))
      } else {
        si <- NULL
      }
      return(si)
    })
    new_phens <- lapply(idt, function(si) {
      if (grepl(pattern = ":=", x = si)) {
        s <- unlist(gregexpr(pattern = ",", text = si))[1]
        e <- unlist(gregexpr(pattern = ":=", text = si))[1]
        si <- substr(x = si, start = s, stop = e)
        si <- gsub(pattern = " |:=|:|,", replacement = "", x = si)
        si <- gsub(pattern = "ipip", replacement = "list", x = si)
      } else {
        si <- NA
      }
      return(si)
    })
    # generate a phen table
    record_interval_type <- ipayipi::sts_interval_name(ppsij$time_interval[1])
    fna <- function(x, y) if (any(is.null(x[[y]]))) NA else x[[y]][1]
    phens_dt <- data.table::data.table(
      ppsid = paste(ppsij$dt_n, ppsij$dtp_n, sep = "_"),
      phid = NA,
      phen_name = unlist(new_phens),
      units = sapply(xtras, fna, y = "units"),
      measure = sapply(xtras, fna, y = "measure"),
      var_type = sapply(xtras, fna, y = "var_type"),
      record_interval_type = data.table::fifelse(
        record_interval_type$sts_intv %in% "discnt",
        "event_based", "continuous"),
      orig_record_interval = record_interval_type$sts_intv,
      dt_record_interval = gsub(pattern = " ", replacement = "_",
        record_interval_type$sts_intv),
      orig_table_name = ppsij$output_dt,
      table_name = ppsij$output_dt
    )
    phens_dt <- phens_dt[!is.na(phen_name)]
    dt_parse <- list(f_params = list(calc_params = f_params),
      phens_dt = phens_dt)
  }
  return(dt_parse)
}
