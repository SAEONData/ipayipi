#' @title Perform data processing calculations
#' @description Calculations are parsed to data.table.
#' @param station_file Name of the station being processed.
#' @param dta_in A list of data to be processed.
#' param input_dt 
#' param output_dt 
#' @param f_params Function parameters evaluated by `ipayipi::calc_param_eval()`. These are parsed to `dt_calc()` from `dt_process()`.
#' @param f_summary A summary table of function parameters. This
#'  summary table is stored in the station file object. If there
#'  is a summary table for the function, differences between the
#'  `f_params` and table are checked and the 'f_summary' table
#'  is updated---triggering a re-calculation of the whole time series instead
#'  of for a slice of the series.
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
  ...) {
  "dt" <- "station" <- NULL
  # read in the available data
  sfcn <- names(sfc)
  dta_in <- NULL
  hsf_dta <- NULL
  if ("dt_working" %in% sfcn) {
    dta_in <- ipayipi::sf_read(sfc = sfc, tv = "dt_working", tmp = TRUE)[[
      "dt_working"
    ]]
  }

  if (is.null(dta_in) && any(sfcn %ilike% "_hsf_table_")) {
    hsf_dta <- sfcn[sfcn %ilike% "_hsf_table_"]
    hsf_dta <- hsf_dta[length(hsf_dta)]
    dta_in <- ipayipi::sf_read(sfc = sfc, tv = hsf_dta, tmp = TRUE)[[1]]
  }
  # create station object for calc parsing
  station <- gsub(paste(dirname(station_file), station_file_ext,
    "/", sep = "|"), "", station_file)
  dt <- data.table::as.data.table(dta_in)

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
  dt_fk_params <- lapply(fk_params, function(x) eval(parse(text = x)))
  dt_working <- eval(parse(text = paste0(f_params, collapse = "")))
  # save working data
  saveRDS(dt_working, file.path(dirname(sfc[1]), "dt_working"))
  # remove harvest data from this step
  if (!is.null(hsf_dta)) {
    ppsid_hsf <- unique(gsub("_hsf_table_?.+", "", hsf_dta))
    hsf_rm <- sfc[
      names(sfc)[names(sfc) %ilike% paste0(ppsid_hsf, "_hsf_table_*.")]]
    lapply(hsf_rm, function(x) {
      unlink(file.path(dirname(sfc[1]), x), recursive = TRUE)
    })
  }
  return(dt_fk_params)
}