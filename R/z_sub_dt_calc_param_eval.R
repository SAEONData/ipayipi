#' @title Organise sequencial processing of phenomena aggregations.
#' @description An alias for list in ipayipi::dt_calc. Used for sequence of
#'  `ipayipi::chainer()` calculations.
#'  - := indicates new phenomena for which units, measure and var_types are necessary for further processing.
#' @param f_params If `full_eval` is FALSE: a list of `ipayipi::chainer()` operations.
#' @author Paul J. Gordijn
#' @export
calc_param_eval <- function(
  f_params = NULL,
  full_eval = FALSE,
  ppsij = NULL,
  sfc = NULL,
  ...
) {
  ".N" <- NULL
  "phen_name" <- "nas" <- NULL

  ## partial evaluation ----
  if (!any(class(f_params) %in% "dt_calc_string") && full_eval == FALSE) {
    # evaluate chain and parse text
    x <- f_params
    v_names <- names(x)
    dt_parse <- sapply(seq_along(v_names), function(ii) {
      xii <- paste0("[",
        # before 1st comma
        x[[v_names[ii]]]$dt_syn_bc, ", ",
        # after 2nd comma
        x[[v_names[ii]]]$dt_syn_ac,
        if (!is.null(x[[v_names[ii]]]$dt_syn_exc)) ", " else "",
        x[[v_names[ii]]]$dt_syn_exc, "]"
      )

      # add in additional arguments between chains
      x_names <- names(x[[ii]])
      # check the basics are there
      essnt_params <- c("measure", "units", "var_type")
      m <- essnt_params[!essnt_params %in% x_names]
      if (length(m) > 0 && length(m) < 3) {
        stop(paste("Missing parameters:", paste(m, collapse = ", ")))
      }
      essnt_params <- c(
        "measure", "units", "var_type", "notes", "fork_table"
      )
      # assign null values for those not there
      ms_params <- essnt_params[!essnt_params %in% x_names]
      xsyn <- x[[ii]][x_names %in% essnt_params]
      for (q in seq_along(ms_params)) xsyn[ms_params[q]] <- NA
      xsyn <- xsyn[essnt_params]
      xsyn <- xsyn[!is.na(xsyn)]
      if (length(xsyn) > 0) {
        z <- paste0(names(xsyn), " = \"", xsyn, "\"", collapse = ", ")
        z <- paste("ipip(", z, ")", collapse = "", sep = "")
        if (length(unlist(grep(pattern = ", ]$", x = xii))) > 0) {
          xii <- gsub(pattern = ", ]$", replacement = paste0(", ", z, "]"),
            x = xii
          )
        } else {
          xii <- gsub(pattern = "]$", replacement = paste0(", ", z, "]"),
            x = xii
          )
        }
      }
      if (ii == 1) {
        xii <- paste0("dt", xii)
      }
      return(xii)
    })
    class(dt_parse) <- c(class(dt_parse), "dt_calc_string")
  } else {
    ## full evaluation of chainer with providing phenmoena ----
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
    xtras <- lapply(seq_along(xtras), function(i) {
      x <- data.table::as.data.table(xtras[[i]])
      x$phen_name <- new_phens[[i]]
      return(x)
    })

    xtras <- data.table::rbindlist(xtras, fill = TRUE)
    xtras <- xtras[!is.na(phen_name)]
    xtras$nas <- rowSums(is.na(xtras))
    xtras <- split.data.frame(xtras, f = factor(xtras$phen_name))
    xtras <- lapply(xtras, function(x) {
      x[nas == min(nas)][.N]
    })
    xtras <- data.table::rbindlist(xtras)
    xtras[, names(xtras)[!names(xtras) %in% "nas"], with = FALSE]

    # generate a phen table
    record_interval_type <- ipayipi::sts_interval_name(ppsij$time_interval[1])

    old_phens_dt <- sf_dta_read(sfc = sfc, tv = "phens_dt")[["phens_dt"]][
      phen_name %in% xtras$phen_name
    ]
    old_phens_dt$nas <- rowSums(
      is.na(old_phens_dt[, c("units", "measure", "var_type"), with = FALSE])
    )
    old_phens_dt <- split.data.frame(old_phens_dt,
      f = factor(old_phens_dt$phen_name)
    )
    old_phens_dt_l <- lapply(old_phens_dt, function(x) {
      x <- x[!nas == max(nas)][.N]
      return(x)
    })
    old_phens_dt <- data.table::rbindlist(old_phens_dt_l)
    if (nrow(old_phens_dt) > 0) {
      old_phens_dt <- old_phens_dt[,
        c("phen_name", "units", "measure", "var_type"), with = FALSE
      ]
      names(old_phens_dt) <- c("phen_name", "c_units", "c_measure",
        "c_var_type"
      )
    }
    phens_dt <- data.table::data.table(
      ppsid = paste(ppsij$dt_n[1], ppsij$dtp_n[1], sep = "_"),
      phid = NA,
      phen_name = xtras$phen_name,
      units = xtras$units,
      measure = xtras$measure,
      var_type = xtras$var_type,
      record_interval_type = data.table::fifelse(
        record_interval_type$sts_intv %in% "discnt",
        "event_based", "continuous"
      ),
      orig_record_interval = record_interval_type$sts_intv,
      dt_record_interval = gsub(pattern = " ", replacement = "_",
        record_interval_type$sts_intv
      ),
      orig_table_name = ppsij[.N]$input_dt,
      table_name = ppsij[.N]$output_dt
    )
    # overwirte na var details with old phen details
    phens_dt_n <- names(phens_dt)
    if (nrow(old_phens_dt) > 0) {
      phens_dt <- old_phens_dt[phens_dt, on = list(phen_name)]
      phens_dt$units <- data.table::fifelse(is.na(phens_dt$units),
        phens_dt$c_units, phens_dt$units
      )
      phens_dt$measure <- data.table::fifelse(is.na(phens_dt$measure),
        phens_dt$c_measure, phens_dt$measure
      )
      phens_dt$var_type <- data.table::fifelse(is.na(phens_dt$var_type),
        phens_dt$c_var_type, phens_dt$var_type
      )
      phens_dt <- phens_dt[, phens_dt_n, with = FALSE]
    }
    if (!"phen_name" %in% names(phens_dt)) phens_dt <- NULL
    dt_parse <- list(f_params = list(calc_params = f_params),
      phens_dt = phens_dt
    )
  }
  return(dt_parse)
}