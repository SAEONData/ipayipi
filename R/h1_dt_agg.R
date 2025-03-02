#' @title Aggregate data by a time interval
#' @description This function aggregates 'ipayipi' formatted data by time periods.
#' @param sfc List of file paths to the temporary station-file directory. Generated using `ipayipi::sf_open_con()`.
#' @param pipe_house Pipeline directory structure generated using `ipayipi::ipip_house()`.
#' @param station_file The file path of the station being assessed.
#' @param f_params Function parameters evaluated by `ipayipi::agg_param_eval()`. These are internally parsed to `dt_agg()` by `dt_process()`.
#' @param ppsij Internally parsed by `dt_process()`. Table derived from `ipayipi::pipe_seq()` showing the summarised function parameters for a data processing step.
#' @param gaps Logical. It `TRUE` then gaps with their respective unique gap identifiers from the 'gap table' of a station are joined to the aggregated series. These identifiers could be used for further processing.
#' @param agg_offset String describing period of offset from the rounded time interval used to aggregate data. For example, if aggregating rainfall data from five minute to daily records, but staggered so that daily rainfall is totalled from 8 am to 8 pm the next day the `agg_offset` must be set to `"8 hours"`.  When `full_eval == TRUE` the function overwrites this parameter in favor of `agg_offset` extracted from the partial evaluation.
#' @param ignore_nas Logical. If `TRUE` then NAs will not be produced in the resulting aggregation. To reflect gaps in data set to `FALSE`.
#' @param gap_tbl String. Provided to search in the 'gaps' table for a table_name used to described gaps.
#' @param gaps_phens Whether to extract gaps pertaining to specific phens. Not fully implemented. Testing functionality.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param xtra_v Logical. Whether or not to report xtra messages, progess, plus print data tables.
#' @param chunk_v Logical. Extra messaging around data chunking.
#' @details Function has to work within the `ipayipi` pipeline as parameters for aggregation are determined during the pipeline evaluation process. The data has to have a column named 'date_time' for aggregation to work.
#'
#' All recognised variable types, that is, those for which aggregation functions are by default defined in `ipayipi::sts_agg_functions`, will be aggregated unless otherwise specified in the aggregation parameters
#'
#'  Processing is done whilst keeping a minimum amount of data in memory. The station file being worked on is saved in a temporary location so if any errors occur in the processing the original file will not be overwritten.
#'
#'  If an offset on the aggregation is used then the secon value of the offset is added to the 'date_time' stamp for the data.tables 'date_time' value. Aggregation defaults are determined by `ipayipi::agg_param_eval()`. Default aggregation functions are determined by the phenomena measure parameter --- see the function for more details.
#'
#' @author Paul J. Gordijn
#' @export
#'
dt_agg <- function(
  sfc = NULL,
  pipe_house = NULL,
  station_file = NULL,
  f_params = NULL,
  ppsij = NULL,
  gaps = FALSE,
  agg_offset = "0 sec",
  ignore_nas = FALSE,
  gap_tbl = "raw_",
  gaps_phens = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  ...
) {
  "%ilike%" <- ".N" <- ":=" <- ".SD" <- "." <- NULL
  "dttm_fl" <- "phen_name" <-  "agg_f" <- "dt_record_interval" <-
    "date_time" <- "phen_out_name" <- "agg_intervals" <- "var_type" <-
    "ignore_nas" <- "gap_start" <- "gap_end" <- "phen" <-
    "orig_table_name" <- "ppsid" <- "p" <- "table_name" <- "tn" <- NULL
  # read and prep data 1 ----
  sfcn <- names(sfc)
  if ("dt_working" %in% sfcn) {
    dta_in <- sf_dta_read(sfc = sfc, tv = "dt_working")
  } else {
    dta_in <- NULL
  }
  hsf_dta <- NULL
  if (is.null(dta_in) && any(sfcn %ilike% "_hsf_table_")) {
    hsf_dta <- sfcn[sfcn %ilike% "_hsf_table_"]
    hsf_dta <- hsf_dta[hsf_dta %ilike% paste0(
      unique(f_params$orig_table_name
      ), collapse = "|"
    )]
    hsf_dta <- hsf_dta[hsf_dta %ilike% paste0("^", ppsij$dt_n[1], "_.")]
    dta_in <- sf_dta_read(sfc = sfc, tv = hsf_dta)
    names(dta_in) <- sub(".*_hsf_table_", "", hsf_dta, fixed = FALSE)
  }
  # filter out unwanted tables by name
  dta_in <- dta_in[
    names(dta_in) %in% c("dt_working", unique(f_params$orig_table_name))
  ]
  # get agg parameters ----
  # extract agg_options from ppsij
  if (any(ppsij$f_params %ilike% "agg_options")) {
    agg_options <- gsub(pattern = "agg_options", replacement = "list",
      x = ppsij$f_params[ppsij$f_params %ilike% "agg_options"]
    )
    agg_options <- lapply(agg_options, function(x) eval(parse(text = x)))
    agg_options <- unlist(agg_options, recursive = FALSE)
    for (i in seq_along(agg_options)) {
      assign(names(agg_options)[[i]], agg_options[[names(agg_options)[[i]]]])
    }
  }
  f_params <- f_params[ppsid %in% ppsid]

  # prepare data 2 ----
  # establish record interval info & start and end dttm
  filt_t <- do.call("c", list(
    min(do.call("c", lapply(dta_in, function(x) min(x$indx$mn)))),
    max(do.call("c", lapply(dta_in, function(x) max(x$indx$mx))))
  ))
  # agg data ----
  # by interval ----
  agg_intv <- agg_intervals[order(unique(agg_intervals))]
  names(agg_intv) <- unique(f_params, by = "dt_record_interval")[
    order(dt_record_interval)
  ]$table_name
  lapply(seq_along(agg_intv), function(ri) {
    ti <- gsub("_", " ", agg_intv[ri])
    # dttm prep ----
    fpi <- f_params[dt_record_interval %in% agg_intv[ri]]
    fpi <- unique(fpi, by = c("phen_name", "phen_out_name"))
    # harvest data ----
    dta <- dta_in[
      names(dta_in)[
        names(dta_in) %in% c("dt_working", unique(fpi$orig_table_name))
      ]
    ]
    #dta_ex <- sf_dta_read(sfc = sfc, tv = names(agg_intv)[ri])
    # filter filt_t for data exrtaction
    # create dttm sequence for merging aggregate data sets
    start_dttm <- lubridate::floor_date(filt_t[1], unit = ti) +
      lubridate::as.duration(agg_offset)
    end_dttm <- lubridate::ceiling_date(filt_t[2], unit = ti,
      change_on_boundary = FALSE
    ) + lubridate::as.duration(agg_offset)
    main_agg <- data.table::data.table(
      date_time = seq(from = start_dttm, to = end_dttm, by = ti)
    )
    data.table::setkey(main_agg, "date_time")

    # by source table ----
    dta <- lapply(names(dta), function(x) {
      if (!x %in% "dt_working") {
        fpii <- fpi[orig_table_name %in% x & dt_record_interval == agg_intv[ri]]
      } else {
        fpii <- fpi[dt_record_interval == agg_intv[ri]]
      }
      # harvest data ----
      d <- ipayipi::dt_dta_open(dta_link = dta[x])
      vn <- names(d)[names(d) %in% unique(fpi$phen_name)]
      d <- d[, names(d)[names(d) %in% c("date_time", vn)], with = FALSE]
      px <- fpii[phen_name %in% unique(vn)]
      agg_fs <- unique(px$agg_f)
      d <- d[, dttm_fl := lubridate::floor_date(date_time, unit = ti) +
          lubridate::as.duration(agg_offset)
      ]
      # gaps nas WIP ----
      gr <- sf_dta_read(sfc = sfc, tv = "gaps")[["gaps"]]
      if (!is.null(dta[[x]]$gaps)) {
        g <- dta[[x]]$gaps[phen %in% fpii$phen_name][, p := phen]
        gf <- fpii[, c("phen_name", "phen_out_name", "table_name"),
          with = FALSE
        ]
        names(gf)[3] <- "tn"
        g <- gf[g, on = .(phen_name == p)][,
          phen := data.table::fifelse(
            !is.na(phen_out_name), phen_out_name, phen
          )
        ][, table_name := tn][, names(dta[[x]]$gaps), with = FALSE]
        # save to gap table
        gr <- sf_dta_read(sfc = sfc, tv = "gaps")[["gaps"]]
        gr <- gr[!(table_name %in% g$table_name & phen %in% g$phen)]
        gr <- rbind(gr, g)[order(table_name, phen, gap_start, gap_end)]
        # delete old gaps file
        sf_dta_rm(sfc = sfc, rm = "gaps")
        # write updated gap files
        sl <- sf_dta_wr(dta_room = file.path(dirname(sfc[1]), "gaps"),
          dta = gr, tn = "gaps"
        )
        sf_log_update(sfc = sfc, log_update = sl)
      }
      #saveRDS(gr, file = sfc["gaps"])
      # generate na table for building gid list-column
      # sdcols <- names(d)[!names(d) %in% c("date_time", "dttm_fl")]
      # dna <- d[, (sdcols) := lapply(.SD, is.na),
      #   .SDcols = sdcols
      # ]
      # # dna$rain_mm[3] <- TRUE
      # # dna$rain_mm[500:1550] <- TRUE
      # dna <- dna[, (sdcols) := lapply(.SD, any),
      #   .SDcols = sdcols, by = "dttm_fl"
      # ]
      # dna <- unique(dna, by = "dttm_fl")
      # dna[, nas := rowSums(.SD), .SDcols = sdcols]
      # nac <- colSums(dna[, (sdcols), with = FALSE])
      # dd <- lapply(seq_along(nac), function(ni) {
      #   if (nac[ni] == 0) return(NULL)
      #   dnai <- dna[, c("dttm_fl", names(nac[ni])[1]), with = FALSE]
      #   dnai <- dnai[, int := data.table::fifelse(colv == TRUE, 1, 0),
      #     env = list(colv = names(nac[ni])[1])
      #   ]
      #   dnai$int <- change_intervals(dnai$int)
      #   dnai <- dnai[colv == TRUE, env = list(colv = names(nac[ni])[1])]
      #   dnai <- dnai[, ":="(gap_start = min(dttm_fl), gap_end = max(dttm_fl)),
      #     by = "int"
      #   ]
      #   dnai <- unique(dnai, by = "int")
      #   dnai <- dnai[, ":="(table_name = names(agg_intv)[ri],
      #     phen = names(nac[ni])[1], gap_type = "auto", dt_diff_s = difftime(
      #       gap_end + lubridate::as.duration(agg_intv[ri]), gap_start,
      #       units = "secs"
      #     ), gap_problem_thresh_s = as.numeric(
      #       lubridate::as.duration(agg_intv[ri]), "sec"
      #     ), problem_gap = TRUE, notes = NA_character_
      #   )][, names(dnai)[
      #     !names(dnai) %in% c("dttm_fl", "int", names(nac[ni])[1])
      #   ], with = FALSE]
      #   # need fuzzy merge phen gaps with gaps already in the gap
      #   # table

      #   return(dnai)
      # })
      # dd <- data.table::rbindlist(dd)

      # agg data by agg f ----
      if (xtra_v) {
        cli::cli_inform(c(">" = "Aggregating data in groups by function:"))
      }
      dtx <- lapply(agg_fs, function(z) {
        cols <- c("date_time", "dttm_fl", px[agg_f %in% z]$phen_name)
        xz <- subset(d, select = cols)
        cols <- cols[!cols %ilike% "date_time|dttm_fl"]
        if (xtra_v) {
          cli::cli_inform(c(" " = "processing {cols} by {z} ..."))
        }
        xzi <- suppressWarnings(attempt::try_catch(
          expr = xz[, lapply(.SD, function(x) eval(parse(text = z))),
            by = dttm_fl, .SDcols = cols
          ][, lapply(.SD, function(x) {
            data.table::fifelse(is.infinite(x) | is.nan(x), NA, x)
          }), by = dttm_fl, .SDcols = cols],
          .w = ~xz[, lapply(.SD, function(x) {
            x <- eval(parse(text = z))
            return(x)
          }), by = dttm_fl, .SDcols = cols
          ][, lapply(.SD, function(x) {
            data.table::fifelse(is.infinite(x) | is.nan(x), NA, x)
          }), by = dttm_fl, .SDcols = cols]
        ))
        data.table::setnames(xzi, old = "dttm_fl", new = "date_time")
        data.table::setkey(xzi, "date_time")
        return(xzi)
      })
      # merge agg f ----
      dtx <- Reduce(function(...) merge(..., all = TRUE), dtx)
      return(dtx)
    })
    # merge source table aggs ----
    ndttm <- lapply(dta, function(x) main_agg[!date_time %in% x$date_time])
    ndttm <- unique(data.table::rbindlist(ndttm))
    dta[["main_agg"]] <- main_agg
    dta <- Reduce(function(...) merge(..., all = TRUE), dta) # merge data tables
    dta <- unique(dta, by = "date_time")
    data.table::setcolorder(dta, c("date_time",
      fpi$phen_name[fpi$phen_name %in% names(dta)]
    ))
    data.table::setnames(dta, old = names(dta), new = c("date_time",
      fpi$phen_out_name[fpi$phen_name %in% names(dta)]
    ))

    # trim rows with NA values at begining and end
    if (anyNA(dta[.N, ])) dta <- dta[!.N, ]
    if (anyNA(dta[1, ])) dta <- dta[!1, ]
    fpii <- fpi[phen_out_name %in% names(dta) & var_type == "num"][
      ignore_nas == TRUE
    ]$phen_out_name

    if (length(fpii) > 0) {
      dta[date_time %in% ndttm$date_time, (fpii) := lapply(.SD, function(x) {
        data.table::fifelse(is.na(x) | is.nan(x), 0, x)
      }), .SDcols = fpii]
    }
    dta <- dta[, unique(names(dta)), with = FALSE]

    # add in gap info ----
    # if (any(is.logical(gaps) || all(sapply(gaps, is.character))) &&
    #       "gaps" %in% names(sfc)) {
    #   if (!is.logical(gaps) && !is.null(gap_tbl)) {
    #     gap_tbl <- gap_tbl[phen %in% gaps]
    #   }
    #   n <- c(names(dta), "gid")
    #   dta$xdt1 <- dta$date_time
    #   dta$xdt2 <- lubridate::ceiling_date(dta$date_time, unit = ti,
    #     change_on_boundary = TRUE
    #   )
    #   gap_tbl <- gap_tbl[, ":="(d1 = gap_start, d2 = gap_end)]
    #   dta <- gap_tbl[dta, on = .(d2 >= xdt1, d1 <= xdt2), mult = "first"][
    #     , n, with = FALSE
    #   ]
    #   dta <- dta[, names(dta)[!names(dta) %in% "dttm_cl"], with = FALSE]
    # }
    # write agg data to files
    sl <- sf_dta_wr(dta_room = file.path(dirname(sfc[1]), names(agg_intv[ri])),
      dta = dta, tn = names(agg_intv[ri]), rit = "continuous", ri = agg_intv[ri]
    )
    sf_log_update(sfc = sfc, log_update = sl)
    return(agg_intv[ri])
  })

  # clean harvest data ----
  if (!is.null(hsf_dta)) {
    ppsid_hsf <- unique(gsub("_hsf_table_?.+", "", hsf_dta))
    hsf_rm <- sfc[
      names(sfc)[names(sfc) %ilike% paste0(ppsid_hsf, "_hsf_table_*.")]
    ]
    sf_dta_rm(sfc = sfc, rm = hsf_rm)
  }
  return(list(agg_intervals = names(agg_intv), ppsij = ppsij))
}