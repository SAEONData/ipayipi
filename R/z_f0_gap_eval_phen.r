#' @title Plot the coverage of ipayipi station files
#' @description Uses ggplot2 to show the availability of data for a select group of station's and their raw data tables. _Data availability is based on overall data logging time not NA values_.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param station_ext Required argument. File extension of the station files in the pipe_house directory 'ipip_room'.
#' @param gap_problem_thresh_s _Parsed to_ `ipayipi::gap_eval()`.
#' @param event_thresh_s _Parsed to_ `ipayipi::gap_eval()`.
#' @param start_dttm For plotting data.
#' @param end_dttm For plotting data.
#' @param plot_tbls Vector of table names to include when plotting data availability.
#' @param meta_events _Parsed to_ `ipayipi::gap_eval()`.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param wanted A string of keywords to use to filter which stations are selected for processing. Multiple search kewords should be seperated with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param prompt Should the function use an interactive file selection function otherwise all files are returned. TRUE or FALSE.
#' @param recurr Should the function search recursively into sub directories for hobo rainfall csv export files? TRUE or FALSE.
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on Linux.
#' @author Paul J. Gordijn
#' @details 
#'  - function assumes date_time column
#' Gap data for each station and respective tables are extracted from the station, using `ipayipi::gap_eval()`. Gap tables are combined with the full record of data in a data summary table and plotted using ggplot2. Note that 'gaps' can be edited by imbibing metadata into a stations record, _see_ `ipayipi::gap_eval()` for details.
#' @return A list containing a plot, which shows the availability of data, the gap data as formatted for plotting. The plot is produced using `ggplot2::ggplot()`.
#' @keywords plotting; missing data; data over view; data reporting;
#' @author Paul J. Gordijn
#' @export
phen_gaps <- function(
  pipe_house = NULL,
  station_file = NULL,
  phens = NULL,
  phen_eval = TRUE,
  start_dttm = NULL,
  end_dttm = NULL,
  gap_problem_thresh_s = 6 * 60 * 60,
  tbl_n = "^raw_*",
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  # pipe_house <- pipe_house
  # station_file <- station_file
  # verbose <- TRUE
  # xtra_v <- T
  # tbl_n = "^raw_*"
  # phens <- NULL

  "%ilike%" <- ".SD" <- ":=" <- "." <- NULL
  "table_name" <- "problem_gap" <- "phen" <- "int" <- "gsdttm" <- "dttm1" <-
    "gedttm" <- "dttm0" <- "gap_end" <- "gap_start" <- "date_time" <-
    "phen_name" <- NULL
  # read station data of relevance
  # filter out unwanted phens, focus on wanted phens
  # read station phen summary
  # check if relevant phen summary table is present
  # generate NA gap summary
  # match gap summary to gap table format and return table

  if (!phen_eval) return(NULL)

  # read station data of relevance
  sfc <- open_sf_con(pipe_house = pipe_house, station_file = station_file,
    verbose = verbose, xtra_v = xtra_v, tmp = TRUE
  )
  tbl_n <- sfc[names(sfc) %ilike% tbl_n]
  if (length(sfc) == 0) {
    message(paste0("No match for ", tbl_n, " in ", station_file, "."))
    return(NULL)
  }
  dts <- sf_dta_read(sfc = sfc, verbose = verbose, xtra_v = xtra_v,
    tv = names(tbl_n), tmp = TRUE
  )
  # filter out unwanted phens, focus on wanted phens
  # remove links with no wanted phens
  dts <- lapply(dts, function(x) {
    if (is.null(phens)) phens <- names(x$indx$dta_n)
    if (!any(names(x$indx$dta_n) %in% c("id", "date_time", phens))) {
      return(NULL)
    } else {
      x$indx$dta_n <- x$indx$dta_n[, names(x$indx$dta_n)[
        names(x$indx$dta_n) %in% phens
      ], with = FALSE]
      return(x)
    }
  })
  phen_ds <- sf_dta_read(sfc = sfc, verbose = verbose, xtra_v = xtra_v,
    tv = "phen_data_summary", tmp = TRUE
  )[["phen_data_summary"]]
  # read station data
  phen_gaps <- future.apply::future_lapply(seq_along(names(dts)), function(i) {
    dta <- ipayipi::dt_dta_open(dta_link = dts[names(dts)[i]])
    sdcols <- names(dts[[names(dts)[i]]]$indx$dta_n)
    dta <- dta[, sdcols, with = FALSE]
    sdcols <- sdcols[!sdcols %ilike% c("id$|date_time")]
    dta <- dta[, (sdcols) := lapply(.SD, is.na), .SDcols = sdcols]
    # filter out logger gaps here
    gaps <- sf_dta_read(sfc = sfc, verbose = verbose, xtra_v = xtra_v,
      tv = "gaps", tmp = TRUE
    )[["gaps"]][table_name %in% names(dts)[i]][problem_gap == TRUE]
    gaps_lg <- gaps[phen %in% "logger"]
    gaps_lg <- gaps_lg[, ":="(gsdttm = gap_start, gedttm = gap_end)]
    dta <- dta[, ":="(dttm0 = date_time, dttm1 = date_time)]
    dta <- gaps_lg[dta, on = .(gsdttm <= dttm1, gedttm >= dttm0),
      mult = "first"
    ]
    dta <- dta[, names(dta)[
      names(dta) %ilike% paste0(
        "*id$|date_time|phen|", paste0(sdcols, collapse = "|")
      )
    ], with = FALSE]
    pds <- phen_ds[table_name %in% names(dts)[i]]
    ri <- dts[[names(dts)[i]]]$indx$ri
    if (!ri %in% "discnt") {
      dft <- lubridate::as.duration(ri)
      gap_problem_thresh_s <- dft
    } else {
      dft <- lubridate::as.duration(0)
    }
    phen_gaps <- lapply(seq_along(sdcols), function(j) {
      pdsj <- pds[phen_name %in% sdcols[j]]
      x <- dta[, c("date_time", "phen", sdcols[j]), with = FALSE]
      if (nrow(pdsj) > 0) {
        x <- x[date_time >= min(pdsj$start_dttm)][
          date_time <= max(pdsj$end_dttm)
        ]
      }
      x[which(x[[sdcols[j]]] == TRUE), "int"] <- 1
      x[phen %in% "logger", "int"] <- 0
      x[is.na(int), "int"] <- 2
      x$int <- ipayipi::change_intervals(x$int)
      x <- x[!phen %in% "logger"]
      x <- x[, ":="(gap_start = min(date_time), gap_end = max(date_time)),
        by = "int"
      ]
      x <- x[which(x[[sdcols[j]]] == TRUE)]
      x <- unique(x, by = c("int"))
      x <- x[, phen := sdcols[j]][, -"date_time"]
      x <- x[, ":="(table_name = names(dts)[i], gap_type = "auto",
        gid = NA_real_, problem_gap = TRUE,
        dt_diff_s = difftime(gap_end, gap_start, unit = "secs") + dft,
        gap_problem_thresh_s = as.numeric(
          gap_problem_thresh_s, units = "secs"
        ), notes = NA_character_
      )][, names(x)[!names(x) %in% c(sdcols[j], "int")], with = FALSE]
      return(x)
    }, future.packages = "ipayipi")
    phen_gaps <- phen_gaps[sapply(phen_gaps, function(gx) nrow(gx) > 0)]
    phen_gaps <- data.table::rbindlist(phen_gaps)
    return(phen_gaps)
  })
  phen_gaps <- data.table::rbindlist(phen_gaps)
  return(phen_gaps)
}