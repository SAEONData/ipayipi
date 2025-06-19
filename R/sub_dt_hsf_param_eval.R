#' @title 'dt' processing pipeline: generate data harvest criteria
#' @description Sets up a data harvest from a station file.
#' @param full_eval Defaults to `FALSE`. Evaluation is run 'fully' in `ipayipi::dt_process()` where full phenomena descriptions are developed.
#' @param station_file *The path and name of the main station to which data is being harvested.
#' @param hrv_station *The path and name of the station, or a keyword, to search for the station from which data will be harvested. _If `NA` the `hrv_station` is set to `station_file`, i.e., data will be harvested from the `station_file`._ If a string is provided the function will search for a matching station during full evaluation (`eval_full` set to TRUE`).
#' @param harvest_dir *The directory in which to search for the `hrv_station`.
#' @param hrv_tbl *The name of the table from which to harvest data. If the keyword "raw" is used data will extracted from stations standardised 'raw' data. The linked "phen" table will be used for describing the extracted "raw" data. If there are no phenomena descriptions the function will use the default format of the fields/columns of the harvested table. This will be added to the processing phenomena information ('phens_dt').
#' @param time_interval *The desired time interval of the `output_dt`. The full evaluation will extract phenomena from 'raw' data tables accordingly.
#' @param phen_names *Names of phenomena to extract from tables. If `NULL` all phenomena possible will be harvested. If the desired `time_interval` is shorter than the time interval or frequency of recordings of the data to be harvested, phenomena will not be harvested.
#' @param recurr Logical. Whether to search recursively in directories for the `hrv_station`. Parsed to `ipayipi::dta_list()`.
#' @param harvest_station_ext Parsed to `ipayipi::dta_list()`. Defaults to ".ipip". Must include the period (".").
#' @param prompt Parsed to `ipayipi::dta_list()`. Set to `TRUE` to use interactive harvest station selection.
#' @param single_out Forces through an interactive process the singling out of a harvest station. Useful for example where `ipayipi::dta_list()` returns from than one option.
#' @param f_params For the partial evaluation multiple phenomena can be described using `ipayipi::agg_params()`. If left blank defaults will be used on all phenomena harvested.
#' @param ppsij Summary pipe process table for the function. If provided this data will be used to overwrite similar arguments provided in the function. This must be provided for the full function evaluation.
#' @details _Parameters indicated by '*' are used in the partial evaluation.
#'  Results of partial evaluations are fed to the full evaluation.
#'
#' _Time_interval_: In order to minimise the memory requried to aggregate harvested data, if there are more than one 'raw' tables with different record intervals, phenomena will be exrtacted from the 'raw' table whose record interval most closely matches the specified `time_interval`.
#' The same functionality needs to be tested for phenomena from processed data where phens are stored in the 'phens_dt'. Currently this is not supported.
#' @export
hrv_param_eval <- function(
  station_file = NULL,
  hrv_station = NULL,
  harvest_dir = ".",
  hrv_tbl = "raw",
  time_interval = "discnt",
  phen_names = NULL,
  phen_gaps = NULL,
  recurr = TRUE,
  prompt = TRUE,
  single_out = TRUE,
  full_eval = FALSE,
  f_params = NULL,
  ppsij = NULL,
  sfc = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  ...
) {
  "%chin%" <- "%ilike%" <- ":=" <- "." <- NULL
  "phen_name" <- "table_name" <- "input_dt" <- "phen_syn" <- "dt_n" <-
    "dtp_n" <- "output_dt" <- "orig_tbl_name" <- "dt_record_interval" <-
    "phen_gap" <- NULL

  # assignments ----
  harvest_station_ext <- ".ipip"


  ## partial function evaluation ----------------------------------------------
  #  returns shorter hrv_param list
  #  full evaluation returns a list of available phenomena and
  #  their phenomena details.
  if (full_eval != TRUE) {
    if (harvest_dir == ".") harvest_dir <- NULL
    hrv_params <- list(
      station_file = station_file,
      hrv_station = hrv_station,
      harvest_dir = harvest_dir,
      hrv_tbl = hrv_tbl,
      phen_names = phen_names,
      phen_gaps = phen_gaps
    )
    f_params <- hrv_params[!sapply(hrv_params, is.null)]
    class(f_params) <- c(class(f_params), "dt_harvest_params")
  } else {
    # full function evaluation -------------------------------------------------
    # extract parts of ppsij and assign values in env
    if (!is.null(ppsij)) {
      ppsi_names <- names(ppsij)[!names(ppsij) %in% c("n", "f", "f_params")]
      for (k in seq_along(ppsi_names)) {
        assign(ppsi_names[k], ppsij[[ppsi_names[k]]][1])
      }
    }

    # extract parts of f_params and assign values in environment
    hrv_param_names <- names(f_params)
    for (k in seq_along(hrv_param_names)) {
      assign(hrv_param_names[k], f_params[[hrv_param_names[k]]])
    }

    # single out the harvest (hrv) station
    if (!is.null(hrv_station)) {
      hrvn <- if (!file.exists(hrv_station)) {
        dta_list(input_dir = harvest_dir, recurr = recurr,
          file_ext = harvest_station_ext, wanted = hrv_station,
          unwanted = NULL, prompt = prompt, single_out = single_out
        )
      } else {
        hrv_station
      }
      hrvn <- gsub(pattern = "^./", replacement = "", hrvn)
    } else {
      station_file <- gsub(pattern = "^./", replacement = "", station_file)
      hrvn <- station_file
    }
    # open hrv file if different from sf
    if (basename(hrvn) != basename(station_file)) {
      hrvc <- attempt::try_catch(
        sf_open_con(station_file = hrvn, tv = hrv_tbl, chunk_v = chunk_v)
      )
    } else {
      hrvc <- sfc
    }
    hrv_summary <- sf_dta_read(
      sfc = hrvc, tv = c("data_summary", "phens", "phens_dt")
    )
    ## prepare hrv phen metadata ----
    # - depends on time series data type (discnt or continuous)
    # - if extracting from standardised pipe data we can extract phenomena
    # details...

    # initial phenomena name organisation
    if (exists("input_dt")) hrv_tbl <- input_dt

    # check hrv_tbl -- name of the input table
    # identify & extract correct phen table
    phen_tabs <- hrv_summary[names(hrv_summary) %ilike% "phens"]
    if (!is.null(phen_names)) {
      phen_tabs <- lapply(phen_tabs, function(x) x[phen_name %in% phen_names])
    }

    p <- NULL
    # extract from raw data phen table ('phens')
    if (hrv_tbl %ilike% "raw") {
      p <- phen_tabs$phens[table_name %ilike% "raw",
        c("phid", "phen_name", "units", "measure", "var_type", "table_name")
      ]
      if (hrv_tbl != "raw") p <- p[table_name == hrv_tbl]
      # join on data summary info
      s <- unique(
        hrv_summary$data_summary[
          table_name %in% unique(p$table_name)
        ], by = "table_name"
      )
    }

    # extract from processed data phen table ('phens_dt')
    # if (all(!hrv_tbl %ilike% "raw",
    #   hrv_tbl %in% phen_tabs$phens_dt$table_name
    # )) {
    #   p <- phen_tabs$phens_dt[table_name %ilike% hrv_tbl,
    #     c("phid", "phen_name", "units", "measure", "var_type", "table_name")
    #   ]
    #   # get data summary info
    #   s <- unique(
    #     hrv_summary$data_summary[
    #       table_name %in% unique(p$table_name)
    #     ], by = "table_name"
    #   )
    # }

    # generate own phen table
    if (!any(sapply(phen_tabs, function(x) any(x$table_name %in% hrv_tbl))) &&
        !hrv_tbl %ilike% "^raw"
    ) {
      # open table
      ht <- ipayipi::sf_dta_read(sfc = hrvc, tv = hrv_tbl)
      ht <- ipayipi::dt_dta_open(dta_link = ht)
      # get date time info
      dttm <- names(ht)[names(ht) %ilike% "date_time|date-time"][1]
      if (is.na(dttm)) {
        dttm <- names(ht)[names(ht) %ilike% "start|end"][1]
      }
      # evaluate other data record interval
      # dtti <- ipayipi::record_interval_eval(dt = ht[[dttm]], dta_in = ht,
      #   record_interval_type = "event_based"
      # )
      # set record interval as defined by hrv
      dtti <- list(record_interval = time_interval)
      if (dtti$record_interval %chin% "discnt") {
        dtti$record_interval_type <- "event_based"
      } else {
        dtti$record_interval_type <- "continuous"
      }
      p <- data.table::data.table(
        phid = NA_integer_, phen_name = names(ht), units = NA_character_,
        measure = NA_character_, var_type = sapply(ht, function(x) class(x)[1]),
        table_name = hrv_tbl
      )
      # standardise var_type nomenclature
      suppressWarnings(
        if (max(ht[[dttm]], na.rm = TRUE) == -Inf) {
          sd <- as.POSIXct(NA, tz = attr(ht[[dttm]], "tz"))
          se <- as.POSIXct(NA, tz = attr(ht[[dttm]], "tz"))
        } else {
          sd <- max(ht[[dttm]], na.rm = TRUE)
          se <- min(ht[[dttm]], na.rm = TRUE)
        }
      )

      s <- data.table::data.table(record_interval_type =
          dtti$record_interval_type, record_interval = dtti$record_interval,
        start_dttm = sd, end_dttm = se, table_name = hrv_tbl
      )
      p$var_type <- as.vector(sapply(p$var_type, function(x) {
        ipayipi::sts_phen_var_type[phen_syn %ilike% x][["phen_prop"]][1]
      }))
    }
    if (!is.null(p)) {
      ## phen aggregation interval options ----
      # add record interval to p to organise aggregation intervals and phenomena
      # selection
      s <- s[, names(s)[names(s) %in% c(
        "record_interval_type", "record_interval", "table_name", "start_dttm",
        "end_dttm"
      )], with = FALSE]
      sx <- lapply(s$record_interval, ipayipi::sts_interval_name)
      sx <- data.table::rbindlist(sx)
      s <- cbind(s, sx)
      p <- merge(x = p, y = s, all.y = TRUE, by = "table_name")
    }
    # extract from processed data phen table
    if ("phens_dt" %in% names(sfc) && is.null(p)) {
      p <- phen_tabs$phens_dt
      p <- p[table_name == hrv_tbl]
      # check aggregation interval
      p <- p[, ":="(record_interval = dt_record_interval)]
    }

    if (is.null(p)) {
      cli::cli_inform(c("i" = "Phenomena metadata:",
        "!" = "Harvested phenomena details not developed!",
        "i" = "Phenomena details are extracted from:",
        "*" = "the \'phens'\ table if the harvest table name is == \'raw\',",
        "*" = paste0("the \'phens_dt\' table if the harvested table has",
          " the preffix \'dt_\', or"
        ), "*" = "are constructed through evaluation."
      ))
    }
    ## phen aggregation interval options ----
    # add record interval to p to organise aggregation intervals and phenomena
    # selection
    ri <- ipayipi::sts_interval_name(time_interval)$sts_intv
    ri <- ri[!sapply(ri, is.na)]
    ri <- lapply(unique(p$record_interval), ipayipi::sts_interval_name)
    ri <- sapply(ri, function(x) x[["sts_intv"]])
    # if there is more than one time interval then set as the provided arg
    if (length(ri) > 1) ri <- time_interval

    # ### discontinuous data option ----
    # if ("event_based" %in% p$record_interval_type) {
    #   p$dfft_secs <- 0
    # } else {
    #   p$dfft_secs <- wanted_ri
    # }
    # if (is.na(agg_time_interval$dfft_secs)) agg_time_interval$dfft_secs <- 0
    # p$agg_intv <- agg_time_interval$dfft_secs
    # p$dfft_diff <- p$agg_intv - p$dfft_secs

    # # can only aggregate if time intervals are more lengthy than raw data
    # if (all(p$dfft_diff < 0)) {
    #   warning(paste0(time_interval, " has a shorter duration than ",
    #     "available raw data!", collapse = ""))
    # }
    # p <- p[dfft_diff >= 0]
    # data.table::setorderv(p, cols = c("phen_name", "dfft_diff"))
    # p <- lapply(split.data.frame(p, f = factor(p$phen_name)), function(x) {
    #   x$decis <- ifelse(x$dfft_diff == min(x$dfft_diff), TRUE, FALSE)
    #   x <- x[decis == TRUE][1, ]
    #   invisible(x)
    # })
    phens_dt <- data.table::data.table(
      ppsid = paste(dt_n, dtp_n, sep = "_"),
      phid = p$phid,
      phen_name = p$phen_name,
      units = p$units,
      measure = p$measure,
      var_type = p$var_type,
      record_interval_type = p$record_interval_type,
      orig_record_interval = p$record_interval,
      dt_record_interval = gsub(" ", "_", ri),
      orig_tbl_name = p$table_name,
      table_name = output_dt
    )

    hrv_params <- phens_dt[,
      c("ppsid", "orig_tbl_name", "phen_name"), with = FALSE
    ][, ":="(station_file = station_file, hrv_station = hrvn)][,
      hrv_tbl := orig_tbl_name
    ][, c("ppsid", "station_file", "hrv_station", "hrv_tbl", "phen_name"),
      with = FALSE
    ][, phen_gap := phen_name]
    pg_n <- names(hrv_params)
    # join on phen gap names
    pg <- data.table::rbindlist(lapply(seq_along(phen_gaps), function(i) {
      data.table::data.table(p = names(phen_gaps)[i], pg = phen_gaps[[i]])
    }))
    if (!is.null(phen_gaps)) {
      hrv_params <- pg[hrv_params, on = .(p == phen_name)]
      data.table::setnames(hrv_params, "p", "phen_name")
      if (nrow(hrv_params[is.na(pg)]) == nrow(hrv_params)) {
        cli::cli_inform(c(
          "!" = "While evaluating phenomena and associated gaps:",
          " " = "Failed to match up phen_gap names!",
          ">" = "Check phen names are spelt correctly ..."
        ))
      }
      hrv_params <- hrv_params[, phen_gap := data.table::fifelse(
        !is.na(pg), pg, phen_gap
      )][, pg_n, with = FALSE]
    }

    if (is.null(phen_names[1]) || is.na(phen_names[1])) {
      phen_names <- unique(p$phen_name)
    }
    phen_names <- unique(p$phen_name)
    if (!is.null(phen_names)) phen_names <- phen_names[order(phen_names)]

    f_params <- list(f_params = list(hrv_params = hrv_params),
      phens_dt = phens_dt
    )
  }
  return(f_params)
}