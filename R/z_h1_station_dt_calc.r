#' @title Perform data processing calculations
#' @description Calculations are parsed to data.table.
#' @param sfc List of file paths to the temporary station file directory. Generated using `ipayipi::open_sf_con()`.
#' @param station_file Name of the station being processed.
#' @param f_params Function parameters evaluated by `ipayipi::calc_param_eval()`. These are parsed to `dt_calc()` from `dt_process()`.
#' @param ppsij Data processing `pipe_seq` table from which function parameters and data are extracted/evaluated. This is parsed to this function automatically by `ipayipi::dt_process()`.
#' @param station_file_ext The station file extension (period included '.'). Defaults to '.ipip'.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param xtra_v Logical. Whether or not to report xtra messages, progess, plus print data tables.
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on Linux.
#' @return A list containing the processed data sets 'dts_dt'.
#' @author Paul J. Gordijn
#' details
#'
#' @export
dt_calc <- function(
  sfc = NULL,
  station_file = NULL,
  f_params = NULL,
  station_file_ext = ".ipip",
  ppsij = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  "%ilike%" <- NULL
  # read in the available data
  sfcn <- names(sfc)
  dta_in <- NULL
  hsf_dta <- NULL
  if ("dt_working" %in% sfcn) {
    dta_in <- ipayipi::sf_dta_read(sfc = sfc, tv = "dt_working", tmp = TRUE)
  }

  if (is.null(dta_in) && any(sfcn %ilike% "_hsf_table_")) {
    hsf_dta <- sfcn[sfcn %ilike% "_hsf_table_"]
    hsf_dta <- hsf_dta[length(hsf_dta)]
    dta_in <- ipayipi::sf_dta_read(sfc = sfc, tv = hsf_dta, tmp = TRUE)
  }
  # create station object for calc parsing
  station <- gsub(
    paste(dirname(station_file), station_file_ext, "/", sep = "|"),
    "", station_file
  )
  ipayipi::msg(paste0("Calc station: ", station), xtra_v)

  # eindx filter
  dta_in <- ipayipi::dt_dta_filter(dta_link = dta_in, ppsij = ppsij)
  # open data ----
  dt <- ipayipi::dt_dta_open(dta_link = dta_in[[1]])
  ipayipi::msg("Pre-calc data", xtra_v)
  if (xtra_v) print(head(dt))

  # organise f_params
  # extract ipip arguments from the seperate changes
  f_ipips <- lapply(f_params, function(x) {
    xpos <- regexpr(", ipip\\(", x)[1]
    xr <- substr(x, xpos, nchar(x) - 1)
    if (xpos > 0) {
      x <- eval(parse(text = gsub(", ipip\\(", "list\\(", xr)))
      if (!"fork_table" %in% names(x)) x <- NULL
      x <- x[sapply(names(x), function(x) x %in% "fork_table")]
      x <- unlist(x, recursive = TRUE)
    } else {
      x <- NULL
    }
  })
  # remove special ipip aruments for the evaluation
  f_params <- lapply(f_params, function(x) sub(", ipip\\(.*]$", "]", x))
  # chain together fork tables
  fk_params <- lapply(seq_along(f_params), function(i) {
    ic <- f_params
    if (!is.null(f_ipips[[i]])) {
      ic[[i]] <- paste0("[!", substr(ic[[i]], 2, nchar(ic[[i]])), collapse = "")
      r <- paste0(ic[1:i], collapse = "")
    } else {
      r <- NULL
    }
    return(r)
  })
  names(fk_params) <- f_ipips
  fk_params <- fk_params[sapply(fk_params, function(x) !is.null(x))]
  dt_fk_params <- lapply(fk_params, function(x) {
    attempt::try_catch(expr = eval(parse(text = x)), .e = ~NULL)
  })
  dt_working <- attempt::try_catch(
    expr = list(eval(parse(text = paste0(f_params, collapse = "")))),
    .e = ~list(NULL)
  )
  names(dt_working) <- "dt_working"
  ipayipi::msg("Post-calc data", xtra_v)
  if (xtra_v) print(head(dt_working))

  # dttm foor ppsij
  if (all(
    "date_time" %in% names(dt_working[["dt_working"]]),
    nrow(dt_working[["dt_working"]]) > 0
  )) {
    ppsij$start_dttm <- min(dt_working[["dt_working"]]$date_time, na.rm = TRUE)
    ppsij$end_dttm <- max(dt_working[["dt_working"]]$date_time, na.rm = TRUE)
  }
  # save working data
  unlink(sfc["dt_working"], recursive = TRUE)
  # work through data to be saved
  dta_sets <- c(dt_fk_params, dt_working)
  lapply(seq_along(dta_sets), function(dsi) {
    d <- dta_sets[[dsi]]
    if (!data.table::is.data.table(d)) d <- unlist(d, recursive = TRUE)
    n <- names(dta_sets)[dsi]
    ipayipi::msg("Chunking data", xtra_v)
    ipayipi::sf_dta_wr(dta_room = file.path(dirname((sfc[1])), n[1]),
      dta = d, overwrite = TRUE, tn = n[1], ri = ppsij[1]$time_interval,
      verbose = verbose, xtra_v = xtra_v
    )
  })
  # remove harvest data from this step
  if (!is.null(hsf_dta)) {
    ppsid_hsf <- unique(gsub("_hsf_table_?.+", "", hsf_dta))
    hsf_rm <- sfc[
      names(sfc)[names(sfc) %ilike% paste0(ppsid_hsf, "_hsf_table_*.")]
    ]
    lapply(hsf_rm, function(x) {
      unlink(file.path(dirname(sfc[1]), x), recursive = TRUE)
    })
  }
  return(list(ppsij = ppsij))
}