#' @title Evaluate arguments for pipeline univariate filter.
#' @description The default filter used is the 'hampel' non-linear, non-parametric filter, i.e., it application is suited to time series data. The 'hampel' is the only filter currently implemented.
#' @param phens List of vectors of phenomena names. Each list item must correspond to a filter 'run'. This allows multiple runs to be performed whilst data is in memory with different parameters being passed to the filter algorithm. If NULL the filter will run on all numeric phenomena. To 'deselect' phenomena from the run include a minus directly before the phenomena name.
#' @param w Total window size for the filter algorithm. Defaults to ten. Must be supplied as a vector or list with subsequent values corresponding to each filter 'run'.
#' @param madf Scalar factor of MAD (median absolute deviation). Higher values relax oulier detection. Defaults to the standard of three.  Must be supplied as a vector or list with subsequent values corresponding to each filter 'run'.
#' @param align One of the following character options following 'data.table' lateral syntax framework: 1) \bold{'right'} (the default) evaluates the current value, on the right, based on preceeding values, falling to the left; option \bold{'left'} does the opposite evaluting the values to the right; option \bold{centre} places the evauated value in middle of the total window size provided by \bold{w} (option 'centre' is 'left' biased for odd window sizes). This argument is parsed to 'data.table' `frollapply`.
#' @param cush Logical indicating whether to prevent NAs at data start and end dates owing to window/filter alignment. `TRUE` will \bold{cush}ion data start and ends where necessary by adjust window alignment properties (within segments). `FALSE` does not prevent NA values.
#' @param seg Vector of station data table names that contain values that will be used to slice data series into segments for independent outlier detection runs. Values don't have to correspond to segment numbers---segments are orgaised within this function. If `NULL` (default) the series is treated as one segment. An option here is the 'logg_interfere' table. Must be supplied as a vector or list with subsequent values corresponding to each filter 'run'.
#' @param clean_f Algorithm name. Only the non-linear "hampel" filter currently supported.
#' @param na_t Threshold (as a proportion of window length) of NA values tolerated. If the number of NAs exceeds the threshold, filtering will not be performed. Must be supplied as a vector or list with subsequent values corresponding to each filter 'run'.
#' @param tighten Scaling fraction between zero and one that sensitizes the detection of outliers near the head and tail ends of segments. The fraction is multiplied by the mad_dev factor. Must be supplied as a vector or list with subsequent values corresponding to each filter 'run'.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param xtra_v Logical. Whether or not to report xtra messages, progess, plus print data tables.
#' @param full_eval Logical that indicates the depth of evaluation. This argument is provided parsed internally. Full evaluation can only be completed within `ipayipi::dt_process()` sequence evaluation.
#' param f_params
#' @param ppsij Data processing `pipe_seq` table from which function parameters and data are extracted/evaluated. This is parsed to this function automatically by `ipayipi::dt_process()`.
#' @param sfc  List of file paths to the temporary station file directory. Generated using `ipayipi::sf_open_con()`.
#' @param station_file Name of the station being processed.
#' @details
#'  - `cush`: When `align` is set to `left` at the 'end' of a data series values cannot be evaluated and therefore set to NA, and vice versa when `align` is `right`. NAs at data start and end extremities are avoided when `cush` is set to `TRUE` by switching `align` properties in these cases. When `align` is set to `center` both 'left' and 'right' alignment are used to fill NA values.
#' 
#' @author Paul J. Gordijn
#' @export
clean_param_eval <- function(
  phens = NULL,
  w = 21,
  madf = 3,
  align = "left",
  seg = NA_character_,
  cush = TRUE,
  clean_f = "hampel",
  na_t = 0.25,
  tighten = 0.65,
  owrite = TRUE,
  full_eval = FALSE,
  station_file = NULL,
  ppsij = NULL,
  f_params = NULL,
  sfc = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  "%chin%" <- ":=" <- "." <- ".N" <- ".SD" <- "%ilike%" <- NULL
  "ppsid" <- "phen_name" <- "dt_n_tmp" <- "var_type" <- NULL

  # default data.tableish arguments
  d_args <- list(clean_f = "hampel", phens = "NULL", seg = "NULL",
    cush = TRUE, na_t = 0.75, w = 21, madf = 3, tighten = 0.65, align = "left",
    owrite = TRUE
  )
  p_args <- list(station_file = "NULL", f_params = NULL, ppsij = "NULL",
    sfc = "NULL"
  )

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
  d_args <- d_args[!names(d_args) %in% c("phens", "seg", "clean_f")]
  args <- lapply(seq_along(d_args), function(i) {
    x <- get(names(d_args)[[i]])
    if (!is.vector(x)) x <- as.vector(x)
    if (all(x %in% d_args[[i]])) x <- NULL
    x
  })
  names(args) <- names(d_args)
  if (is.vector(phens)) phens <- list(phens)
  if (is.vector(seg)) seg <- list(seg)
  if (is.vector(madf)) madf <- list(madf)
  if (is.vector(align)) align <- list(align)
  args <- data.table::as.data.table(args)
  if (!is.null(phens)) args$phens <- phens
  if (!is.null(seg)) args$seg <- seg
  if (!is.null(madf)) args$madf <- madf
  if (!is.null(align)) args$align <- align

  args <- lapply(seq_len(nrow(args)), function(i) as.expression(args[i]))
  args <- lapply(args, function(x) {
    x$phens <- unlist(x$phens)
    if ("seg" %in% names(x)) {
      if (is.na(x$seg)) x$seg <- NULL
    }
    x
  })

  class(args) <- c(class(args), "dt_clean_params")
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
  # open station hrv f params

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
    pn <- phens_dt[phen_name %chin% x$phens][var_type %in% "num"]
    pn <- pn[, dt_n_tmp := as.numeric(gsub("_.", "", ppsid))]
    pn <- pn[, .SD[.N], by = .(phen_name, dt_n_tmp)]
    if (length(unique(pn$phen_name)) > 1) {
      cli::cli_inform(c("i" = "While assessing phenomena for 'cleaning'",
        "!" = "Multiple phen name matches. All will be \'cleaned\'!",
        "i" = "Phen matches:", "*" = "{x$phens}",
        ">" = "Please refine the phen name search keys if necessary ..."
      ))
      print(x)
    }
    x$phens <- unique(pn$phen_name)
    x
  })
  # make phen dt forthe fltr val table
  phens_dt <- data.table::data.table(
    ppsid = paste0(ppsij$dt_n[1], "_", ppsij$dtp_n[1]),
    phid = NA_integer_,
    phen_name = c("original_v", "phen", "replace_v", "stage", "step"),
    units = NA_character_,
    measure = NA_character_,
    var_type = c("num", "chr", "num", "int", "int"),
    record_interval_type = "event_based",
    orig_record_interval = "discnt",
    dt_record_interval = "discnt",
    orig_tbl_name = paste0(ppsij$input_dt[1], "_fltr_vals"),
    table_name = paste0(ppsij$input_dt[1], "_fltr_vals")
  )

  return(list(phens_dt = phens_dt))
}
