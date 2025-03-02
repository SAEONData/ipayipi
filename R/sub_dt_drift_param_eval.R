#' @title Evaluate arguments for linear drift correction.
#' @description Performs linear drift correction of a single phenomena using calibration measurements.
#' @param phen String name of phenomena name ('phen_name') to undergo drift correction.
#' @param wside The window side (half window) for calculation of drift offsets. The size of this range represents the size of the number of point measurements around a central value which is used to estimate drift correction.
#' @param wside_max If there are `NA` values in the window given by `wside` the window side size will expand until reaching this maximum value.
#' @param robust Use the median instead of the mean to calculate the central tendancy of the window of interest. Reduces the influence of outliers on drift parametization.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param full_eval Logical that indicates the depth of evaluation. This argument is provided parsed internally. Full evaluation can only be completed within `ipayipi::dt_process()` sequence evaluation.
#' @param f_params Argument used for parsing `pipe_seq` function parameters to `drift_param_eval()` when `full_eval == TRUE`.
#' @param ppsij Data processing `pipe_seq` table from which function parameters and data are extracted/evaluated. This is parsed to this function automatically by `ipayipi::dt_process()`.
#' @param sfc  List of file paths to the temporary station file directory. Generated using `ipayipi::sf_open_con()`.
#' @param station_file Name of the station being processed.
#' details
#' @author Paul J. Gordijn
#' @export
drift_param_eval <- function(
  phen = NULL,
  wside = 3,
  wside_max = 12,
  robust = TRUE,
  edge_adj = NULL,
  pipe_house = NULL,
  station_file = NULL,
  full_eval = FALSE,
  f_params = NULL,
  ppsij = NULL,
  sfc = NULL,
  verbose = FALSE,
  ...
) {
  "%ilike%" <- NULL
  "ppsid" <- "phen_name" <- "var_type" <- NULL

  # default data.tableish arguments
  d_args <- list(phen = "NULL", wside = 3, wside_max = 12, robust = TRUE,
    edge_adj = "NULL"
  )
  # full eval default args
  p_args <- list(station_file = "NULL", f_params = NULL, ppsij = "NULL",
    sfc = "NULL"
  )

  ## partial evaluation -----------------------------------------------------
  # convert input args for clean to list
  d_args <- d_args[
    !names(d_args) %in% c("phen", "wside", "wside_max", "robust", "edge_adj")
  ]
  args <- lapply(seq_along(d_args), function(i) {
    x <- get(names(d_args)[[i]])
    if (!is.vector(x)) x <- as.vector(x)
    if (all(x %in% d_args[[i]])) x <- NULL
    return(x)
  })
  names(args) <- names(d_args)
  if (is.vector(phen)) phen <- list(phen)
  if (is.vector(wside)) wside <- list(wside)
  if (is.vector(wside_max)) wside_max <- list(wside_max)
  if (is.vector(robust)) robust <- list(robust)
  if (is.vector(edge_adj)) edge_adj <- list(edge_adj)
  args <- data.table::as.data.table(args)
  if (!is.null(phen)) args$phen <- phen[1]
  if (!is.null(wside)) args$wside <- wside[1]
  if (!is.null(wside_max)) args$wside_max <- wside_max[1]
  if (!is.null(robust)) args$robust <- robust[1]
  if (!is.null(edge_adj)) args$edge_adj <- edge_adj[1]

  args <- lapply(seq_len(nrow(args)), function(i) as.expression(args[i]))
  class(args) <- c(class(args), "dt_drift_params")
  if (!full_eval) return(args)

  ## full evaluation --------------------------------------------------------
  # get harvested table phens to check if the listed phens are recognised
  # phens
  # rows to reach back based on windows
  # function verify
  # replace values
  # highlight outliers

  # flow
  # get f params expressions from ppsij
  # open station hsf f params

  # f_params
  # parse f_params to expressions -- each expression is a seperate run
  # tht may have 'subruns' if multiple phen names are selected
  z <- lapply(ppsij$f_params, function(x) {
    eval(parse(text = sub("^~", "expression", x)))
  })

  # read phens_dt ----
  phens_dt <- sf_dta_read(sfc = sfc, tv = "phens_dt")[["phens_dt"]]
  # filter oiut phens from other stages
  phens_dt <- phens_dt[ppsid %ilike% paste0("^", ppsij$dt_n[1], "_")]

  # match up phen names with those in existence
  # only use phens that have variable type 'numeric'
  z <- lapply(z, function(x) {
    pn <- phens_dt[phen_name %ilike% x$phen][var_type %in% "num"]
    if (nrow(pn) > 1) {
      cli::cli_inform(c("i" = "While assessing phenomena for 'cleaning'",
        "!" = "Multiple phen name matches. Only one required!",
        "i" = "Phen matches:",
        "*" = "{phens_dt[phen_name %ilike% x$phen]$phen_name}",
        ">" = "Please refine the phen name search keys if necessary ...",
        ">" = paste0("Perhap units/measures/var_types, or ",
          "\'record_interval_type', of the same phen were different?"
        )
      ))
      print(x)
      print(phens_dt[phen_name %ilike% x$phen])
    }
    x$phens <- pn$phen_name
    return(x)
  })

  # test duration
  if (is.null(z[[1]]$edge_adj)) {
    t <- NULL
  } else {
    t <- lubridate::as.duration(z[[1]]$edge_adj)
  }
  if (is.na(t)) {
    cli::cli_abort("Unrecognised duration: \'edge_adj\' = {z[[1]]$edge_adj}")
  }
  # make phen dt
  return(NULL)
}
