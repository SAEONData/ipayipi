#' @title Evaluate and prepare tables for joins
#' @description Flexible and fast joining of `ipayipi` data.
#' @param clean_f Algorithm name. Currently only the non-linear "hampel" filter supported.
#' @param phen_names List of vectors of phenomena names. Each list item must correspond to a filter 'run'. This allows multiple runs to be performed whilst data is in memory with different parameters being passed to the filter algorithm. If NULL the filter will run on all numeric phenomena. To 'deselect' phenomena from the run include a minus directly before the phenomena name.
#' @param w_size Window size for the filter algorithm. Defaults to ten. Must be supplied as a vector or list with subsequent values corresponding to each filter 'run'.
#' @param mad_dev Scalar factor of MAD (median absolute deviation). Higher values relax oulier detection. Defaults to the standard of three.  Must be supplied as a vector or list with subsequent values corresponding to each filter 'run'.
#' @param segs Vector of station data table names that contain timestamps used to slice data series into segments for independent outlier detection runs. If left `NULL` (default) the series is treated as one segment. A good option here is the 'logg_interfere' table. Must be supplied as a vector or list with subsequent values corresponding to each filter 'run'.
#' @param seg_fuzz String representing the threshold time interval between the list of segment date-time values. If two or more date-time values occur within this threshold time there corresponding values will be merged to the earliest date-time stamp. If left `NULL` `seg_fuzz` defaults to the record interval duration. Must be supplied as a vector or list with subsequent values corresponding to each filter 'run'.
#' @param seg_na_t Fractional tolerace of the count of NA values within a window for interpolation of missing values. If the number of NAs exceeds the threshold, filtering will not be performed. Must be supplied as a vector or list with subsequent values corresponding to each filter 'run'.
#' @param tighten Scaling fraction between zero and one that sensitizes the detection of outliers near the head and tail ends of segments. The fraction is multiplied by the mad_dev factor. Must be supplied as a vector or list with subsequent values corresponding to each filter 'run'.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param xtra_v Logical. Whether or not to report xtra messages, progess, plus print data tables.
#' @param full_eval
#' param f_params
#' @param ppsij Data processing `pipe_seq` table from which function parameters and data are extracted/evaluated. This is parsed to this function automatically by `ipayipi::dt_process()`.
#' param sf
#' @param sfc  List of file paths to the temporary station file directory. Generated using `ipayipi::open_sf_con()`.
#' @param station_file Name of the station being processed.
#' details
#' @author Paul J. Gordijn
#' @export
clean_param_eval <- function(
  clean_f = "hampel",
  phen_names = NULL,
  segs = NULL,
  seg_fuzz = NULL,
  seg_na_t = 0.75,
  w_size = 21,
  mad_dev = 3,
  tighten = 0.65,
  full_eval = FALSE,
  station_file = NULL,
  ppsij = NULL,
  f_params = NULL,
  sfc = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {

  # default data.tableish arguments
  d_args <- list(clean_f = "hampel", phen_names = "NULL", segs = NULL,
    seg_fuzz = NULL, seg_na_t = 0.75, w_size = 21, mad_dev = 3,
    last_rule = FALSE, tighten = 0.65
  )
  p_args <- list(station_file = "NULL", f_params = NULL, ppsij = "NULL",
    sfc = "NULL"
  )
  #pda <- append(p_args, d_args)

  ## partial evaluation -----------------------------------------------------
  # check filter types
  cf <- c("hampel")
  clean_f <- c(clean_f)
  m <- clean_f[!clean_f %in% cf]
  m <- lapply(m, function(x) {
    stop(paste0("Mismatch in \'clean_f\' arg: ", paste0(m, collapse = ", ")),
      call. = FALSE
    )
  })

  # convert input args for clean to list
  d_args <- d_args[!names(d_args) %in% c("phen_names", "clean_f")]
  args <- lapply(seq_along(d_args), function(i) {
    x <- get(names(d_args)[[i]])
    if (!is.vector(x)) x <- as.vector(x)
    if (all(x %in% d_args[[i]])) x <- NULL
    return(x)
  })
  names(args) <- names(d_args)
  if (is.vector(phen_names)) phen_names <- list(phen_names)
  if (is.vector(clean_f)) clean_f <- list(clean_f)
  args <- data.table::as.data.table(args)
  if (!is.null(phen_names)) args$phen_names <- phen_names
  args$clean_f <- clean_f
  args <- lapply(seq_len(nrow(args)), function(i) as.expression(args[i]))
  args <- lapply(args, function(x) {
    x$phen_names <- unlist(x$phen_names)
    x$clean_f <- unlist(x$clean_f)
    return(x)
  })
  class(args) <- c(class(args), "dt_clean_params")
  if (!full_eval) return(args)

  ## full evaluation --------------------------------------------------------
  # get harvested table phens to check if the listed phens are recognised
  
}
