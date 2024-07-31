#' @title Evaluate and prepare tables for joins
#' @description Flexible and fast joining of `ipayipi` data.
#' @param sfc  List of file paths to the temporary station file directory. Generated using `ipayipi::open_sf_con()`.
#' @param station_file Name of the station being processed.
#' @param clean_f Algorithm name. Only "hampel" supported.
#' @param phen_names Vector of the phenomena names that will be evaluated by the hampel filter. If NULL the function will not run.
#' @param w_size Window size for the hempel filter. Defaults to ten.
#' @param mad_dev Scalar factor of MAD (median absolute deviation). Higher values relax oulier detection. Defaults to the standard of three.
#' @param segs Vector of station data table names which contain timestamps used to slice data series into segments with independent outlier detection runs. If left `NULL` (default) the run will be from the start to end of data being evaluated.
#' @param seg_fuzz String representing the threshold time interval between the list of segment date-time 
#' @param seg_na_t Fractional tolerace of the amount of NA values in a segment for linear interpolation of missing values.
#' @param last_rule Whether or not to reapply stored rules in the outlier rule column. TRUE will apply old rules.
#' @param tighten Scaling fraction between zero and one that sensitizes the detection of outliers near the head and tail ends of segments. The fraction is multiplied by the mad_dev factor.
#' @param ppsij Data processing `pipe_seq` table from which function parameters and data are extracted/evaluated. This is parsed to this function automatically by `ipayipi::dt_process()`.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param xtra_v Logical. Whether or not to report xtra messages, progess, plus print data tables.
#' @param full_eval
#' param f_params
#' @param ppsij Data processing `pipe_seq` table from which function parameters and data are extracted/evaluated. This is parsed to this function automatically by `ipayipi::dt_process()`.
#' param sf
#' @details
#' __Join types__
#' - `full_join`: Merges matching `x_tbl` and `y_tbl` rows, and retains all unmatching records.
#' - `left_join`: Matching `x_tbl` and `y_tbl` rows, plus all unmatched `x_tbl` rows.
#' - `right_join`: Matching `x_tbl` and `y_tbl` rows, plus all unmatched `y_tbl` rows.
#' - `inner_join`: Only retains matching `x_tbl` and `y_tbl` rows.
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
  last_rule = FALSE,
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
  pda <- append(p_args, d_args)

  if (!full_eval) {
    ## partial evaluation -----------------------------------------------------
    # check join types
    cf <- list("hampel")
    clean_f <- list(clean_f)
    m <- clean_f[!clean_f %in% cf]
    m <- lapply(m, function(x) {
      stop(paste0("Mismatch in \'clean_f\' arg: ", paste0(m, collapse = ", ")),
        call. = FALSE
      )
    })
    clean_f <- unlist(clean_f)

    if (!is.list(w_size)) w_size <- list(w_size)
    if (!is.list(mad_dev)) mad_dev <- list(mad_dev)
    # each run will be represented by a row in the pipe_seq table
    fp <- data.table::data.table(w_size = list(1, 6), z = 1)
    # time seq arg
    if (!any(time_seq == FALSE)) time_seq <- NULL

    # extract non-default args for f_params
    if (join == "full_join") join <- "fj"
    f_params <- lapply(seq_along(pda), function(i) {
      c_arg <- get(names(pda)[i])
      if (is.null(c_arg)) c_arg <- "NULL"
      if (pda[[i]] %in% c_arg) {
        r <- NULL
      } else {
        r <- get(names(pda)[i])
      }
      return(r)
    })
    names(f_params) <- names(pda)
    if (f_params$join == "fj") f_params$join <- "full_join"
    dt_parse <- f_params[!sapply(f_params, function(x) is.null(x))]
    class(dt_parse) <- c(class(dt_parse), "dt_join_params")
  } else {
    ## full function evaluation -----------------------------------------------
    if (!is.null(ppsij)) {
      # extract parts of ppsij and assign values in env
      ppsi_names <- names(ppsij)[!names(ppsij) %in% c("n", "f", "f_params")]
      for (k in seq_along(ppsi_names)) {
        assign(ppsi_names[k], ppsij[[ppsi_names[k]]][1])
      }
    }
    # extract parts of f_params and assign values in environment
    join_param_names <- names(f_params)
    for (k in seq_along(join_param_names)) {
      assign(join_param_names[k], f_params[[join_param_names[k]]])
    }
    # check args
    m <- list("join", "x_tbl", "y_tbl", "f_summary")
    m <- m[sapply(m, function(x) is.null(x) || is.na(x))]
    m <- lapply(m, function(x) {
      stop(paste0("NULL/NA args: ", paste0(m, collapse = ", ")), call. = FALSE)
    })
    # read in phen_dt
    #  and check key phens
    #  generate new phen_dt
    phens_dt <- eval_seq$phens_dt

    dt_parse <- list(
      f_params = list(join_params = f_params),
      phens_dt = phens_dt
    )
  }
  return(dt_parse)
}
