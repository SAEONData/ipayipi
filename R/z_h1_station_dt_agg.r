#' @title Aggregate time series data by a time interval
#' @description This function aggregates 'ipayipi' formatted data by
#'  time periods. The requries an evaluated pipeline.
#' @param station_file The file path of the station being assessed.
#' @param dta_in A list of data tables to be aggregated. The tables
#'  must come from the same station, but may have different record intervals.
#' @param f_params Function parameters evaluated by
#'  `ipayipi::agg_param_eval()`.
#' @param phens_dt The phenomena table describing each of the phenomena in the
#'  `dta_in` parameter. This information is used in the aggregation process.
#' @param ppsij Table derived from `ipayipi::pipe_seq()` showing the summarised
#'  function parameters.
#' @param f_summary List of tables with data summaries.
#' @param agg_offset Character vector of length two. Strings describe period
#'  of offset from the rounded time interval used to aggregate data. For
#'  example, if aggregating rainfall data from five minute to daily records,
#'  but staggered so that daily rainfall is totalled from 8 am to 8 pm the
#'  next day the `agg_offset` must be set to `c("8 hours", "8 hours")`.
#' @details Function has to work within the `ipayipi` pipeline as parameters
#'  for aggregation are determined during the pipeline evaluation process.
#'
#'  Processing is done whilst keeping a minimum amount of data in memory. The
#'   station file being worked on is saved in a temporary location so if any
#'   errors occur in the processing the original file will not be overwritten.
#'
#'  If an offset on the aggregation is used then the secon value of the offset
#'   is added to the 'date_time' stamp for the data.tables 'date_time' value.
#'  Aggregation defaults are determined by `ipayipi::agg_param_eval()`. Default
#'  aggregation functions are determined by the phenomena measure parameter ---
#'  see the function for more details.
#' 
#'  - requires date_time col
#'
#' @author Paul J. Gordijn
#' @export
#'
dt_agg <- function(
  station_file = NULL,
  dta_in = NULL,
  f_params = NULL,
  ppsij = NULL,
  gaps = FALSE,
  agg_offset = c("0 sec", "0 sec"),
  sfc = NULL,
  verbose = FALSE,
  ...) {
  "%ilike%" <- ".N" <- "date_time_floor" <- "phen_name" <- "table_name" <-
    "agg_f" <- ".SD" <- "dt_record_interval" <- "date_time" <-
    "phen_out_name" <- "agg_intervals" <- "var_type" <- "ignore_nas" <-
    "." <- "gap_start" <- "gap_end" <- "xdt" <- "phen" <- "xdt1" <-
    "xdt2" <- "orig_table_name" <- "ppsid" <- NULL
  sfcn <- names(sfc)
  if ("dt_working" %in% sfcn) {
    dta_in <- ipayipi::sf_read(sfc = sfc, tv = "dt_working", tmp = TRUE)
  }
  hsf_dta <- NULL
  if (is.null(dta_in) && any(sfcn %ilike% "_hsf_table_")) {
    hsf_dta <- sfcn[sfcn %ilike% "_hsf_table_"]
    hsf_dta <- hsf_dta[hsf_dta %ilike% paste0(unique(f_params$orig_table_name),
      collapse = "|")]
    dta_in <- ipayipi::sf_read(sfc = sfc, tv = hsf_dta, tmp = TRUE)
    names(dta_in) <- sub(".*_hsf_table_", "", hsf_dta, fixed = FALSE)
  }
  # filter out unwanted tables by name
  dta_in <- dta_in[
    names(dta_in) %in% c("dt_working", unique(f_params$orig_table_name))]
  # extract agg_options from ppsij
  if (any(ppsij$f_params %ilike% "agg_options")) {
    agg_options <- gsub(pattern = "agg_options", replacement = "list",
      x = ppsij$f_params[ppsij$f_params %ilike% "agg_options"])
    agg_options <- lapply(agg_options, function(x) eval(parse(text = x)))
    agg_options <- unlist(agg_options, recursive = FALSE)
    for (i in seq_along(agg_options)) {
      assign(names(agg_options)[[i]], agg_options[[names(agg_options)[[i]]]])
    }
  }
  f_params <- f_params[ppsid %in% ppsid]

  # filter out phens not included in the aggregation
  dta_in_n <- names(dta_in)
  dta_in <- lapply(names(dta_in), function(x) {
    if (!x %in% "dt_working") {
      ftn <- f_params[orig_table_name %in% x]$phen_name
    } else {
      ftn <- f_params$phen_name
    }
    ftn <- c("date_time", unique(ftn))
    d <- dta_in[[x]]
    d <- d[, names(d)[names(d) %in% ftn], with = FALSE]
    return(d)
  })
  names(dta_in) <- dta_in_n
  # establish record interval info & start and end dttm
  filt_t <- do.call("c", list(
    min(do.call("c", lapply(dta_in, function(x) min(x$date_time)))),
    max(do.call("c", lapply(dta_in, function(x) max(x$date_time))))))
  vnames <- sapply(dta_in, function(x) names(x)[!names(x) %in% "date_time"])
  f_params <- f_params[phen_name %in% unique(unlist(vnames))]
  agg_dts <- lapply(agg_intervals[order(unique(agg_intervals))], function(ri) {
    #print(ri)
    ti <- gsub("_", " ", ri)
    #create sequence for merging aggregate data sets
    start_dttm <- lubridate::floor_date(filt_t[1], unit = ti) +
      lubridate::as.period(agg_offset[1])
    end_dttm <- lubridate::ceiling_date(filt_t[2], unit = ti,
      change_on_boundary = FALSE) + lubridate::as.period(agg_offset[2])
    main_agg <- data.table::data.table(
      date_time = seq(from = start_dttm, to = end_dttm, by = ti)
    )
    data.table::setkey(main_agg, "date_time")
    fpi <- f_params[dt_record_interval %in% ri]
    fpi <- unique(fpi, by = c("phen_name", "phen_out_name"))
    dta <- dta_in[names(dta_in)[names(dta_in) %in% c("dt_working",
      unique(fpi$orig_table_name))]]
    # filter out unqante phens
    dta <- lapply(names(dta), function(x) {
      if (!x %in% "dt_working") {
        fpii <- fpi[orig_table_name %in% x & dt_record_interval == ri]
        d <- dta[[x]][, c("date_time", fpii$phen_name), with = FALSE]
      } else {
        fpii <- fpi[dt_record_interval == ri]
        d <- dta[[1]][, c("date_time", fpii$phen_name), with = FALSE]
      }
      # ensure time series is contiuous if orig == continuous
      if (!any(fpii$orig_record_interval %in% "discnt")) {
        ori <- unique(fpii$orig_record_interval)
        oti <- gsub("_", " ", ori)
        start_dttmi <- lubridate::floor_date(
          min(d$date_time, na.rm = TRUE), unit = oti)
        end_dttmi <- lubridate::ceiling_date(
          max(d$date_time, na.rm = TRUE), unit = oti)
        minor_agg <- data.table::data.table(
          date_time = seq(from = start_dttmi, to = end_dttmi, by = oti)
        )
        d <- merge(d, minor_agg, by = "date_time", all = TRUE)
      }
      return(d)
    })
    # add ceiling and floor date_times for aggregation periods
    dta <- lapply(dta, function(x) {
      x$date_time_floor <- lubridate::floor_date(x$date_time,
        unit = ti) + lubridate::as.period(agg_offset[1])
      x$date_time_ceiling <- lubridate::ceiling_date(x$date_time,
        unit = ti, change_on_boundary = TRUE) - 0.1 +
        lubridate::as.period(agg_offset[2])
      data.table::setkey(x, "date_time_floor")
      invisible(x)
    })
    main_agg$date_time_ceiling <- lubridate::ceiling_date(main_agg$date_time,
      unit = ti, change_on_boundary = TRUE) - 0.1 +
      lubridate::as.period(agg_offset[2])

    # run through the raw data and generate aggregations
    dta <- lapply(dta, function(dti) {
      vn <- names(dti)[names(dti) %in% unique(fpi$phen_name)]
      px <- fpi[phen_name %in% unique(vn)]
      agg_fs <- unique(px$agg_f)
      dtx <- lapply(agg_fs, function(z) {
        # print(z)
        cols <- c("date_time", "date_time_floor", px[agg_f %in% z]$phen_name)
        xz <- subset(dti, select = cols)
        if (verbose) {
          xz <- xz[, lapply(.SD, function(x) eval(parse(text = z))),
            by = date_time_floor, .SDcols = cols[!cols %ilike% "date_time"]]
        } else {
          xz <- suppressWarnings(
            xz[, lapply(.SD, function(x) eval(parse(text = z))),
              by = date_time_floor, .SDcols = cols[!cols %ilike% "date_time"]]
          )
        }
        data.table::setnames(xz, old = "date_time_floor", new = "date_time")
        data.table::setkey(xz, "date_time")
        return(xz)
      })
      # merge data tables
      dtx <- Reduce(function(...) merge(..., all = TRUE), dtx)
      return(dtx)
    })
    # if missing date_time values merge to continuous sequence
    ndttm <- lapply(dta, function(x) main_agg[!date_time %in% x$date_time])
    ndttm <- unique(data.table::rbindlist(ndttm))
    dta[["main_agg"]] <- main_agg
    dta <- Reduce(function(...) merge(..., all = TRUE), dta)# merge data tables
    dta <- unique(dta, by = "date_time")
    data.table::setcolorder(dta, c("date_time", "date_time_ceiling",
      fpi$phen_name))
    data.table::setnames(dta, old = names(dta), new = c("date_time",
      "date_time_ceiling", fpi$phen_out_name))

    # trim rows with NA values at begining and end
    if (anyNA(dta[.N, ])) dta <- dta[!.N, ]
    if (anyNA(dta[1, ])) dta <- dta[!1, ]
    fpii <- fpi[
      phen_out_name %in% names(dta) & var_type == "num"][
        ignore_nas == TRUE]$phen_out_name

    if (length(fpii) > 0) {
      dta[date_time %in% ndttm$date_time, (fpii) := lapply(.SD, function(x) {
          data.table::fifelse(is.na(x), 0, x)
          }), .SDcols = fpii]
    }
    dta <- dta[, unique(names(dta)), with = FALSE]
    return(dta)
  })
  names(agg_dts) <- unique(f_params, by = "dt_record_interval")[
    order(dt_record_interval)]$table_name

  if (is.logical(gaps) || all(sapply(gaps, is.character))) {
    gap_tbl <- ipayipi::sf_read(sfc = sfc, tv = "gaps", tmp = TRUE)[[
      "gaps"
    ]]
    if (!is.logical(gaps) && !is.null(gap_tbl)) {
      gap_tbl <- gap_tbl[phen %in% gaps]
    }
    agg_dts <- lapply(agg_dts, function(x) {
      n <- c(names(x), "gid")
      x$xdt1 <- x$date_time
      x$xdt2 <- x$date_time_ceiling
      data.table::setkey(x, xdt1, xdt2)
      data.table::setkey(gap_tbl, gap_start, gap_end)
      x <- data.table::foverlaps(x = x, y = gap_tbl, mult = "first",
        nomatch = NA, type = "any")
      x <- x[, n, with = FALSE]
      x <- x[, names(x)[!names(x) %in% "date_time_ceiling"], with = FALSE]
      return(x)
    })
  }

  # remove harvest data from this step
  if (!is.null(hsf_dta)) {
    ppsid_hsf <- unique(gsub("_hsf_table_?.+", "", hsf_dta))
    hsf_rm <- sfc[
      names(sfc)[names(sfc) %ilike% paste0(ppsid_hsf, "_hsf_table_*.")]]
    lapply(hsf_rm, function(x) {
      unlink(file.path(dirname(sfc[1]), x), recursive = TRUE)
    })
  }

  # save aggregated data
  lapply(names(agg_dts), function(x) {
    saveRDS(agg_dts[[x]], file.path(dirname(sfc)[1], x))
  })
  return(names(agg_dts))
}