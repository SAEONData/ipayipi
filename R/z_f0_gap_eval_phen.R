#' @title Extracts and evaluates NA values in raw data
#' @description Works through each phenomena in a raw data table and generates gap start and end times based on NA values. The start and end date-time for each phenomena is sliced by its begining and end data-time from the phenomena data summary for a respective table.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param station_file Station file name being evaluated. File path should match up with the pipe_house$ipip_room.
#' @param phens Vector. Phenomena names to focus on for evaluation. If NULL all phenomena are processed.
#' @param phen_eval Logical. Decides whether to evaluate phenomena gaps. If FALSE this function will return NULL.
#' @param gap_problem_thresh_s _Parsed to_ `ipayipi::gap_eval()`.
#' @param tbl_n Defaults to all 'raw tables'. Functionality not included to evaluate other tables owing to the import of the phenomena data summary.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param xtra Logical. If TRUE extra messages are displayed.
#' @author Paul J. Gordijn
#' @details Function called by `ipayipi::gap_eval()`.
#' @return A list containing a plot, which shows the availability of data, the gap data as formatted for plotting. The plot is produced using `ggplot2::ggplot()`.
#' @keywords Internal; plotting; missing data; data over view; data reporting;
#' @author Paul J. Gordijn
#' @export
#' @noRd
phen_gaps <- function(
  pipe_house = NULL,
  station_file = NULL,
  phens = NULL,
  phen_eval = TRUE,
  gap_problem_thresh_s = 6 * 60 * 60,
  tbl_n = "^raw_*",
  verbose = FALSE,
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
  sfc <- ipayipi::open_sf_con(
    pipe_house = pipe_house, station_file = station_file
  )
  tbl_n <- sfc[names(sfc) %ilike% tbl_n]
  if (length(sfc) == 0) {
    message(paste0("No match for ", tbl_n, " in ", station_file, "."))
    return(NULL)
  }
  dts <- sf_dta_read(sfc = sfc, verbose = verbose, tv = names(tbl_n))
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
  phen_ds <- sf_dta_read(sfc = sfc, verbose = verbose,
    tv = "phen_data_summary"
  )[["phen_data_summary"]]
  # read station data
  phen_gaps <- future.apply::future_lapply(seq_along(names(dts)), function(i) {
    dta <- ipayipi::dt_dta_open(dta_link = dts[names(dts)[i]])
    sdcols <- names(dts[[names(dts)[i]]]$indx$dta_n)
    dta <- dta[, sdcols, with = FALSE]
    sdcols <- sdcols[!sdcols %ilike% c("id$|date_time")]
    dta <- dta[, (sdcols) := lapply(.SD, is.na), .SDcols = sdcols]
    # filter out logger gaps here
    gaps <- sf_dta_read(sfc = sfc, verbose = verbose, tv = "gaps")[[
      "gaps"
    ]][table_name %in% names(dts)[i]][problem_gap == TRUE]
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
    })
    phen_gaps <- phen_gaps[sapply(phen_gaps, function(gx) nrow(gx) > 0)]
    phen_gaps <- data.table::rbindlist(phen_gaps)
    return(phen_gaps)
  })
  phen_gaps <- data.table::rbindlist(phen_gaps)
  return(phen_gaps)
}