#' @title Read station data
#' @description Reads station data from chunked, or other station data files.
#' @param sfc Station file connection gerenated using `ipayipi::open_sf_con()`. If `sfc` is provided both `pipe_house` and `station_file` arguments are optional.
#' @param tv String vector of the names of values or tables to read from the station file. The strings provided here are used to filter items in the `sfc` object. The filtered item is then read into memory.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param station_file The file name of the station file (extension included).
#' @param tmp Logical. If TRUE then `sf_dta_read()` reads from the sessions temporary file location for stations.
#' @param return_dta Set to `TRUE` to return data files in addition to the data index; applies to chunked data tables.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param start_dttm Date-time filter. Applies to data tables with a 'date-time' column.
#' @param end_dttm Date-time filter. Applies to data tables with a 'date-time' column.
#' @keywords read station tables and chunked data;
#' @export
#' @author Paul J Gordijn
#' @return Station data tables corresponding to the input table names(`tv`), and chunked data indices with data (if `return_dta` is `TRUE`). If no matching table is found then `NULL` is returned.
#' @details Used to handle reading ipayipi station data from a variety of station file formats.
sf_dta_read <- function(
    sfc = NULL,
    tv = NULL,
    pipe_house = NULL,
    station_file = NULL,
    tmp = TRUE,
    return_dta = FALSE,
    verbose = TRUE,
    start_dttm = NULL,
    end_dttm = NULL,
    xtra_v = FALSE,
    ...) {
  ":=" <- "." <- "d1" <- "d2" <- "chnk_fl" <- "chnk_cl" <- "date_time" <- NULL
  if (is.null(station_file) && !is.null(sfc)) {
    ns <- basename(names(sfc)[1])
    n <- sub(paste0("\\/", ns), "", sfc[[1]])
    station_file <- gsub(pattern = paste0(dirname(n), "\\/"), replacement = "",
      x = n
    )
  }
  sf_dir <- dirname(station_file)
  sf_dir <- sub("^\\.", "", sf_dir)
  station_file <- basename(station_file)
  dta <- NULL
  # get data ----
  if (!tmp) {
    # from station file ----
    sfn <- file.path(pipe_house$ipip_room, sf_dir, station_file)
    cr_msg <- paste0(sfn, ": station data not found!")
    if (!file.exists(sfn)) ipayipi::msg(cr_msg, verbose)
    dta <- attempt::try_catch(expr = readRDS(sfn)[tv], .e = ~NULL, .w = ~NULL)
    dta <- dta[!sapply(dta, function(x) any(is.null(x)))]
    class(dta) <- c(class(dta), "ipip-sf_rds")
  } else {
    # from tmp station file ----
    sfc <- ipayipi::open_sf_con(sfc = sfc, pipe_house = pipe_house,
      station_file = file.path(sf_dir, station_file), tmp = TRUE,
      xtra_v = xtra_v
    )
    sfc <- sfc[names(sfc) %in% tv]
    dta <- lapply(sfc, function(x) {
      indx <- attempt::try_catch(
        expr = readRDS(file.path(x, "aindxr")),
        .e = ~NULL,
        .w = ~NULL,
      )
      if (is.null(indx)) return(readRDS(x))
      eindx <- NULL
      fs <- list.files(x, recursive = TRUE)
      fs <- fs[!fs %in% "aindxr"]
      names(fs) <- as.integer(gsub("^i_", "", fs))
      dtax <- NULL
      if (nrow(indx$indx_tbl) > 0) {
        if (is.null(start_dttm)) start_dttm <- indx$mn
        if (is.null(end_dttm)) end_dttm <- indx$mx
        eindx <- data.table::data.table(start_dttm = start_dttm,
          end_dttm = end_dttm, d1 = start_dttm, d2 = end_dttm
        )
        aindx <- indx$indx_tbl[, ":="(d1 = chnk_fl, d2 = chnk_cl)]
        eindx <- aindx[eindx, on = .(d1 <= d2, d2 > d1), mult = "all"]
        eindx <- eindx[, c("chnk_fl", "chnk_cl", "indx", "dta"),
          with = FALSE
        ]
        fs <- fs[names(fs) %in% c(0, eindx$indx)]
      } else {
        fs <- fs[names(fs) %in% 0]
      }
      if (return_dta) {
        dtax <- lapply(file.path(x, fs), function(dx) {
          d <- unique(readRDS(dx))
          d <- d[order(date_time)]
          return(d)
        })
        dtax <- data.table::rbindlist(dtax)
      }
      if (length(fs) == 0) fs <- NULL else fs <- file.path(x, fs)
      return(list(eindx = eindx, indx = indx, fs = fs, dta = dtax))
    })
    dta <- lapply(dta, function(x) {
      if (is.null(x$indx)) {
        class(x) <- c(class(x), "ipip-sf_rds")
      } else {
        class(x) <- c(class(x), "ipip-sf_chnk")
      }
      return(x)
    })
    names(dta) <- names(sfc)
  }
  if (length(dta) == 0) {
    m <- paste(tv, " --- data not read or found by sf_dta_read()")
    ipayipi::msg(m, xtra_v)
    dta <- NULL
  }
  return(dta)
}