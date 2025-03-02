#' @title Pulls discontinuous data
#' @description Queries **discontinuous** data from multiple stations and converts to 'long' format.
#' @param input_dir The directory in which to search for stations form which to extract time-series data.
#' @param pipe_house If the `pipe_house` argument is provided the `pipe_house$ipip_room` will be used instead of the `input_dir`.
#' @param tbl_names Vector of table names in a station files from which to extract data. Add items to the vector such that only the first matching table from a station is selected. Table names are selected via the `%in%` argument.
#' @param phen_name The field/column/name of the phenomena which to join into a single data table.
#' @param gaps If `TRUE` then a logical table of the same dimensions as the extracted flat data is saved to disk The same file name as for the data but with a '_gaps' suffix is used. Where gaps in the data have been detected in the pipeline process respective data rows are set to `TRUE`; `FALSE` indicates no data gap. Gap data is produced by running `ipayipi::gap_eval_batch()`.
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
#' @details Note this function only extracts data from decompressed station files. If a station has not be decompressed the function will take time decompressing.
#' @keywords data pipeline; summarise time-series data; long-format data; data query.
#' @return A list containing 1) the summarised data in a single data.table, 2) a character string representing the time interval by which the data has been summarised, 3) the list of stations used for the summary, 4) the name of the table and the name of the field for which data was summarised, & 5) the station file extension uesd for querying data.
#' @author Paul J. Gordijn
#' @export
dta_flat_pull_discnt <- function(
  input_dir = ".",
  pipe_house = NULL,
  tbl_names = NULL,
  gaps = TRUE,
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
  ...
) {
  ":=" <- "%chin%" <- "." <- NULL
  "stnd_title" <- "date_time" <- "gap_start" <- "gap_end" <- "dttm1" <-
    "dttm2" <- "problem_gap" <- "pn" <- "pg" <- "table_name" <-
    "phen" <- "gid" <- NULL

  # orgainise directories
  if (!is.null(pipe_house)) input_dir <- pipe_house$ipip_room
  if (!is.null(pipe_house) && is.null(output_dir)) {
    output_dir <- pipe_house$dta_out
  }

  # merge data sets into a station for given time periods
  slist <- ipayipi::dta_list(input_dir = input_dir, file_ext = ".ipip",
    prompt = prompt, recurr = recurr, unwanted = unwanted, wanted = wanted
  )
  sn <- gsub(paste0("['.']ipip", "$"), "", basename(slist))
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
  t <- lapply(slist, function(x) {
    # open station file connections
    sfc <- sf_open_con(station_file = file.path(input_dir, x))
    tab_name <- names(sfc)[names(sfc) %in% tbl_names]
    if (length(tab_name) > 0) {
      tn <- basename(sfc[[tab_name[1]]])
    } else {
      tn <- NULL
    }
    t <- sf_dta_read(sfc = sfc, tv = tn, tmp = TRUE)
    invisible(t)
  })
  names(t) <- slist
  t <- t[!sapply(t, is.null)]
  slist <- names(t)
  if (length(t) == 0) {
    cli::cli_abort(c(
      "No matching table names in stations!",
      " " = "Available stations: {slist}",
      "v" = "Check the station tables or table name spelling!"
    ))
  }
  # open data sets then bind together
  t <- lapply(seq_along(t), function(i) {
    dt <- dt_dta_open(t[i])
    sn <- gsub(paste0("['.']ipip", "$"), "", basename(names(t[i])))
    dt[, stnd_title := sn]
    no <- names(dt)[!names(no) %chin% "stnd_title"]
    no <- c("stnd_title", no[order(no)])
    data.table::setcolorder(dt, no)
    return(dt)
  })
  names(t) <- slist
  dt <- data.table::rbindlist(t, use.names = TRUE, fill = TRUE)

  # harvest gap info for series
  g <- lapply(seq_along(t), function(i) {
    sfc <- sf_open_con(station_file = file.path(input_dir, names(t)[i]),
      tv = "gaps"
    )
    tab_name <- names(sfc)[names(sfc) %in% tbl_names][1]
    g <- sf_dta_read(sfc = sfc, tv = "gaps", tmp = TRUE)[["gaps"]]
    sn <- gsub(paste0("['.']ipip", "$"), "", basename(names(t)[i]))
    g <- g[phen %chin% c("logger", names(dt))][
      table_name %chin% tab_name
    ][, stnd_title := sn]
    invisible(g)
  })
  g <- data.table::rbindlist(g, use.names = TRUE, fill = TRUE)
  data.table::setcolorder(g, "stnd_title", after = "gid")
  slist <- names(t)

  if (is.null(out_tab_name)) out_tab_name <- gsub(" ", "_", "discnt")
  dta <- list(dta = dt, dtgaps = g, time_interval = gsub(" ", "_", "discnt"),
    stations = slist, tbl_names = tbl_names, phen_name = names(dt),
    file_ext = ".ipip"
  )
  if (out_csv) {# save the csv file
    data.table::fwrite(dta$dta, file = file.path(output_dir, paste0(
      out_csv_preffix, "_", out_tab_name, ".csv"
    )), na = NA, dateTimeAs = "write.csv")
  }
  if (gaps && out_csv) {
    data.table::fwrite(dta$dtgaps, file = file.path(output_dir, paste0(
      out_csv_preffix, "_", out_tab_name, "_gaps.csv"
    )), na = NA, dateTimeAs = "write.csv")
  }
  return(dta)
}