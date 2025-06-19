#' @title Pulls continuous data for a phenomena
#' @description Queries **continuous** data of a single variable/phenomena; common record interval, from multiple stations; query in 'long' or 'wide' formats. Use [dta_flat_pull_discnt()] or [ipayipi2csv()] to query and/or export discontinuous data.
#' @param input_dir The directory in which to search for stations form which to extract time-series data.
#' @param pipe_house If the `pipe_house` argument is provided the `pipe_house$d4_ipip_room` will be used instead of the `input_dir`.
#' @param tbl_names Vector of table names in a station files from which to extract data. Add items to the vector such that only the first matching table from a station is selected. Table names are selected via the `%in%` argument.
#' @param phen_name The field/column/name of the phenomena which to join into a single data table.
#' @param gaps If `TRUE` then gap info is printed if out_csv functionality is enabled.
#' @param wide Logical. If FALSE output data will be pivoted to wide format. Defaults to TRUE.
#' @param ri Record-interval string. This function will guess the record interval used to generate a flat table. However, the guess may not be possible if there is insufficient data.
#' @param output_dir The output directory where an output csv file is saved.
#' @param wanted A strong containing keywords to use to filter which stations are selected for processing. Multiple search kewords should be seperated with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param out_tab_name If left `NULL` (default), the output table name will be `paste0(out_csv_preffix, '_', ri)`, where ri is determined using the `ipayipi::sts_interval_name()` or provided by the `ri` argument.
#' @param out_csv Logical. If TRUE a csv file is exported to the output directory.
#' @param out_csv_preffix Preffix for the output csv file. The phenomena name and then time interval by which the data are summarised are used as a suffix.
#' @param recurr Whether to search recursively through folders. Defaults to TRUE.
#' @param file_ext The extension of the stations from where data will be extracted. Defaults to ".ipip".
#' @param prompt Logical. If `TRUE` a prompt will be called so that the user can interactively select station files.
#' @param verbose Whether to report on progress. Logical.
#' @param xtra_v Extra verbose. Logical.
#' @param chunk_v More messages when chunking data? Logical.
#' @param dt_format The function attempts to work out the date-time format from a vector of format types supplied to this argument. The testing is done via `lubridate::parse_date_time()`. `lubridate::parse_date_time()` prioritizes the tesing of date-time formats in the order vector of
#'  formats supplied. The default vector of date-time formats supplied should work well for most logger outputs.
#' @param dt_tz Recognized time-zone (character string) of the data locale. The default for the package is South African, i.e., "Africa/Johannesburg" which is equivalent to "SAST".
#' @keywords data pipeline; summarise time-series data; long-format data; wide-format data; data query.
#' @return A list containing 1) the summarised data in a single data.table, 2) a logical table indicating data gaps, 3) a character string representing the time interval by which the data has been summarised, 4) the list of stations used for the summary, 5) the name of the table and the name of the field for which data was summarised, 6) Filter table results from data processing using `dt_clean`, & 7) the station file extension uesd for querying data.
#' @author Paul J. Gordijn
#' @export
dta_flat_pull <- function(
  input_dir = ".",
  pipe_house = NULL,
  tbl_names = NULL,
  phen_name = NULL,
  start_dttm = NULL,
  end_dttm = NULL,
  wide = TRUE,
  ri = NULL,
  output_dir = NULL,
  wanted = NULL,
  unwanted = NULL,
  out_csv = FALSE,
  out_tab_name = NULL,
  out_csv_preffix = "",
  recurr = TRUE,
  prompt = FALSE,
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  gaps = FALSE,
  dt_format = c(
    "Ymd HMOS", "Ymd HMS",
    "Ymd IMOSp", "Ymd IMSp",
    "ymd HMOS", "ymd HMS",
    "ymd IMOSp", "ymd IMSp",
    "mdY HMOS", "mdY HMS",
    "mdy HMOS", "mdy HMS",
    "mdy IMOSp", "mdy IMSp",
    "dmY HMOS", "dmY HMS",
    "dmy HMOS", "dmy HMS",
    "dmy IMOSp", "dmy IMSp"
  ),
  dt_tz = "Africa/Johannesburg",
  ...
) {
  ":=" <- "%chin%" <- "." <- NULL
  "stnd_title" <- "date_time" <- "gap_start" <- "gap_end" <- "dttm1" <-
    "dttm2" <- "problem_gap" <- "pn" <- "pg" <- "table_name" <-
    "phen" <- "gid" <- NULL
  file_ext <- ".ipip"

  # orgainise directories
  if (!is.null(pipe_house)) input_dir <- pipe_house$d4_ipip_room
  if (!is.null(pipe_house) && is.null(output_dir)) {
    output_dir <- pipe_house$d5_dta_out
  }

  # merge data sets into a station for given time periods
  slist <- ipayipi::dta_list(input_dir = input_dir, file_ext = file_ext,
    prompt = prompt, recurr = recurr, unwanted = unwanted, wanted = wanted,
    xtra_v = xtra_v
  )
  sn <- gsub(paste0(file_ext, "$"), "", basename(slist))
  if (anyDuplicated(sn) > 0) {
    cli::cli_inform(c("While querying stations",
      "!" = "Reading duplicated stations {.var stnd_title} not allowed!",
      ">" =
        "Refine search keywords using {.var wanted} and {.var unwanted} args."
    ))
    print(slist[order(sn)])
    return(NULL)
  }
  if (length(slist) == 0) return(NULL)
  # extract all relevant tables from the data
  t <- future.apply::future_lapply(slist, function(x) {
    # open station file connections
    sfc <- ipayipi::sf_open_con(station_file = file.path(input_dir, x),
      chunk_v = chunk_v
    )
    tab_name <- names(sfc)[names(sfc) %in% tbl_names]
    if (length(tab_name) > 0) {
      tn <- basename(sfc[[tab_name[1]]])
    } else {
      tn <- NULL
    }
    t <- sf_dta_read(sfc = sfc, tv = tn)
    invisible(t)
  })
  names(t) <- slist
  t <- t[!sapply(t, is.null)]
  if (length(t) == 0) {
    cli::cli_abort(c(
      "No matching table names in stations!",
      " " = "Available stations: {slist}",
      "v" = "Check the station tables or table name spelling!"
    ))
  }
  t <- t[sapply(t, function(x) {
    if (phen_name %in% names(x[[1]]$indx$dta_n)) {
      return(TRUE)
    } else {
      sf <- gsub(paste0(".*/sf/|", paste0(file_ext, ".*")), "", x[[1]]$fs[1])
      cli::cli_warn(c("i" = paste0("No match for {phen_name} in station",
          " \'{sf}\', table \'{x[[1]]$indx$table_name}\'!"
        ), x = "This station/table cannot be queried!"
      ))
      return(FALSE)
    }
  })]
  if (length(t) == 0) {
    cli::cli_abort(c("No matching phenomena in all station tables!"))
  }

  # prep to join datasets together
  # check dataset ri's
  if (!is.null(ri)) ri <- ipayipi::sts_interval_name(ri)[["sts_intv"]]
  if (is.null(ri)) ri <- t[[1]][[1]]$indx$ri
  if (ri %chin% "discnt") {
    cli::cli_abort(c(
      "i" = "{.var dta_flat_pull() only processes continuous data.}",
      " " = "Use {.var ipayipi2csv} to export discontinuous data.",
      " " = "The record interval{?s} of the data you are querying: \'{ri}\'"
    ))
  }
  ri_chk <- sapply(t, function(x) x[[1]]$indx$ri) %in% ri
  if (any(!ri_chk)) {
    cli::cli_inform(c(
      "i" = "Record-interval mismatch.",
      " " = "The record interval{?s}: sapply(t, function(x) x[[1]]$indx$ri).",
      ">" = "Refine the search keys for the data you are querying."
    ))
  }
  t <- t[ri_chk]
  mn <- data.table::rbindlist(
    lapply(t, function(x) data.table::data.table(mn = x[[1]]$indx$mn))
  )
  # check that all starting points are equal
  mn$dur <- lubridate::as.duration(mn$mn - lubridate::round_date(mn$mn))
  d <- mn$dur[1]
  mn$dur_chk <- mn$dur == mn$dur[1]
  mn[, stnd_title := gsub(paste0(file_ext, "$"), "", basename(names(t)))]
  if (!all(mn$dur_chk)) {
    cli::cli_inform(c(
      "!" = "Unequal date-time starting points. Removing station tables:"
    ))
    names(mn)[1] <- "Start_dttm"
    print(mn)
    t <- t[mn$dur_chk]
  }
  mn <- data.table::rbindlist(
    lapply(t, function(x) data.table::data.table(mn = x[[1]]$indx$mn))
  )
  mx <- data.table::rbindlist(
    lapply(t, function(x) data.table::data.table(mx = x[[1]]$indx$mx))
  )
  if (is.null(start_dttm)) {
    start_dttm <- min(mn$mn)
  } else {
    start_dttm <- lubridate::round_date(
      lubridate::parse_date_time(start_dttm, orders = dt_format, tz = dt_tz), ri
    )
  }
  if (is.null(end_dttm)) {
    end_dttm <- max(mx$mx)
  } else {
    end_dttm <- lubridate::round_date(
      lubridate::parse_date_time(end_dttm, orders = dt_format, tz = dt_tz), ri
    )
  }
  if (start_dttm >= min(mn$mn) && start_dttm <= max(mx$mx)) mn$mn <- start_dttm
  if (end_dttm >= min(mn$mn) && end_dttm <= max(mx$mx)) mx$mx <- end_dttm
  start_dttm <- min(mn$mn)
  end_dttm <- max(mx$mx)
  dt <- data.table::data.table(
    date_time = seq(start_dttm, end_dttm, by = ri) + d
  )

  # check for sufficient date_coverage
  tu <- sapply(seq_along(t), function(i) {
    if (any(t[[i]][[1]]$indx$mx < start_dttm,
      t[[i]][[1]]$indx$mn > end_dttm
    )) {
      return(NULL)
    } else {
      return(t[[i]])
    }
  })
  t <- t[!sapply(tu, is.null)]

  # harvest gap info for series
  g <- lapply(seq_along(t), function(i) {
    sfc <- ipayipi::sf_open_con(
      station_file = file.path(input_dir, names(t)[i]), chunk_v = chunk_v
    )
    if (!"gaps" %chin% names(sfc)) stop("Please run \`gap_eval\` for stations.")
    g <- sf_dta_read(sfc = sfc, tv = "gaps", tmp = TRUE)[["gaps"]]
    g <- g[phen %chin% c("logger", phen_name)][
      table_name %chin% t[[i]][[1]]$indx$table_name
    ]
    invisible(g)
  })
  # make gap TRUE FALSE table
  dt[, ":="(dttm1 = date_time, dttm2 = date_time)]
  gtbls <- lapply(seq_along(g), function(i) {
    gdt <- g[[i]][dt, on = .(gap_end >= dttm1, gap_start <= dttm2)]
    gdt[!is.na(gid)][problem_gap == TRUE][, .(date_time, problem_gap)]
  })
  names(gtbls) <- gsub(paste0(file_ext, "$"), "", basename(names(t)))
  # open data series
  dti <- future.apply::future_lapply(seq_along(t), function(i) {
    # add hrv_phens to the dta_link
    t[[i]][[names(t[[i]])[1]]]$hrv_phens <- phen_name
    dti <- dt_dta_open(t[[i]], start_dttm = start_dttm, end_dttm = end_dttm)
    dti <- gtbls[[i]][dti, on = "date_time"]
    dti <- dti[dt, on = "date_time"][, c("date_time", phen_name, "problem_gap"),
      with = FALSE
    ]
    dti <- dti[, pg := FALSE]
    mn <- min(dti[
      !is.na(pn) | !is.na(problem_gap), env = list(pn = phen_name)
    ]$date_time)
    mx <- max(dti[
      !is.na(pn) | !is.na(problem_gap), env = list(pn = phen_name)
    ]$date_time)
    dti <- dti[date_time >= mn & date_time <= mx,
      problem_gap := data.table::fifelse(is.na(pn), TRUE, problem_gap),
      env = list(pn = phen_name)
    ]
    dti <- dti[date_time >= mn & date_time <= mx,
      problem_gap := data.table::fifelse(is.na(problem_gap), pg, problem_gap)
    ]
    dti <- dti[, c("date_time", phen_name, "problem_gap"), with = FALSE]
    data.table::setnames(dti, phen_name,
      gsub(paste0(file_ext, "$"), "", basename(names(t[i])))
    )
    return(dti)
  })
  dtdta <- lapply(
    dti, function(q) subset(q, select = -c(date_time, problem_gap))
  )
  names(dti) <- gsub(paste0(file_ext, "$"), "", basename(names(t)))
  dtgap <- lapply(seq_along(dti), function(qi) {
    q <- subset(dti[[qi]], select = c(problem_gap))
    data.table::setnames(q, "problem_gap", names(dti)[qi])
    return(q)
  })
  dtd <- do.call(cbind, args = c(list(dt[, .(date_time)]), dtdta))
  dtg <- do.call(cbind, args = c(list(dt[, .(date_time)]), dtgap))
  data.table::setcolorder(
    dtd, c("date_time", names(dtd)[!names(dtd) %in% "date_time"])
  )
  data.table::setcolorder(
    dtg, c("date_time", names(dtg)[!names(dtg) %in% "date_time"])
  )
  # check if there are filter value tables to harvest and compile for
  # attaching
  ftabs <- paste0(tbl_names, "_fltr_vals")
  ftb <- lapply(names(t), function(x) {
    sfc <- sf_open_con(station_file = file.path(input_dir, x))
    if (!any(ftabs %chin% names(sfc))) return(NULL)
    ftn <- sfc <- sfc[names(sfc) %chin% ftabs][1]
    dtf <- sf_dta_read(sfc = sfc, tv = names(ftn))
    dtf <- dt_dta_open(dta_link = dtf)
    sn <- gsub(paste0("['.']ipip", "$"), "", basename(x))
    dtf[, stnd_title := sn][, table_name := names(ftn)]
    return(dtf)
  })
  ftb <- data.table::rbindlist(ftb)
  if (nrow(ftb) > 0) {
    data.table::setcolorder(
      ftb, c("stnd_title", "table_name"), before = "date_time"
    )
  }
  slist <- names(t)
  # convert to long
  if (!wide) {
    mv <- names(dtd)[!names(dtd) %in% c("date_time", "gid")]
    dtd <- data.table::melt(dtd, id.vars = "date_time", measure.vars = mv,
      value.name = phen_name, variable.name = "stnd_title"
    )
    mv <- names(dtg)[!names(dtg) %in% c("date_time", "gid")]
    dtg <- data.table::melt(dtg, id.vars = "date_time", measure.vars = mv,
      value.name = phen_name, variable.name = "stnd_title"
    )
  }
  # remove preceeding and anteceeding na rows in data
  dtd[, nasum := rowSums(is.na(.SD))]
  dtd[, cng_int := change_intervals(nasum)]
  dtd <- dtd[!(cng_int == 1 & nasum == length(slist))]
  dtd <- dtd[!(cng_int == max(cng_int) & nasum == length(slist))]
  dtd <- dtd[, names(dtd)[!names(dtd) %chin% c("nasum", "cng_int")],
    with = FALSE
  ]

  if (is.null(out_tab_name)) out_tab_name <- gsub(" ", "_", ri)
  dta <- list(dta = dtd, dtgaps = dtg, time_interval = gsub(" ", "_", ri),
    stations = slist, tbl_names = tbl_names, phen_name = phen_name,
    cleanr_vals = ftb, file_ext = file_ext
  )
  if (out_csv) {# save the csv file
    out_tab_name <- paste0(
      out_csv_preffix, "_", out_tab_name, "_", phen_name, ".csv"
    )
    out_tab_name <- gsub("^_", "", out_tab_name)
    data.table::fwrite(dta$dta, file = file.path(output_dir, out_tab_name),
      na = NA, dateTimeAs = "write.csv"
    )
  }
  if (gaps && out_csv) {
    data.table::fwrite(dta$dtgaps, file = file.path(
      output_dir, gsub(".csv$", "_gaps.csv", out_tab_name)
    ), na = NA, dateTimeAs = "write.csv")
  }
  return(dta)
}