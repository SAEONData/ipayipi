#' @title Organise sequencial processing of phenomena aggregations.
#' @md
#' @description An alias for list in ipayipi::dt_calc.
#' @param dt_syn_bc String to be placed before the first comma in data.table
#'  syntax.
#' @param dt_syn_ac String to be placed after the first comma in data.table
#'  syntax.
#' @param dt_syn_exc String to be placed after the third comma in data.table
#'  syntax.
#' @param temp_var designates the calculated variable as temporary. Temporary
#'  variables don't need to have their measure, units, and variable type
#'  described. `temp_vars` will be removed at the end of a chain sequence.
#' @param measure One of the following to describe the type of measure:
#'  - "smp" for a sample (instantaneous),
#'  - "tot" for total,
#'  - "avg" for mean,
#'  - "min" for a minimum,
#'  - "max" for a maximum,
#'  - "sd" for standard deviation, and
#'  - "cumm" for a cummulative measure.
#' @param units Standard shorthand for the units of the phenomena being
#'  calculated.
#' @param var_type The type of variable, a string description using one of the
#'  following accepted options:
#'  - "num" for numeric,
#'  - "int" for integer,
#'  - "chr" for string or character,
#'  - "fac" for factor, and
#'  - "posix" for POSIX date-time.
#' @param notes Description of the calculation step. Character string.
#' @param fork_table Character string which if supplied will fork the reverse
#'  of the logic provided by `dt_syn_bc`.
#' @author Paul J. Gordijn
#' @export
chainer <- function(
  dt_syn_bc = NULL,
  dt_syn_ac = NULL,
  dt_syn_exc = NULL,
  temp_var = TRUE,
  measure = NULL,
  units = NULL,
  var_type = NULL,
  notes = NULL,
  fork_table = NULL,
  ...) {
  "%ilike%" <- NULL
  # some evaluation of the chain segment
  if (!temp_var) { # if the variable is permanent
    essnt_params <- c("measure", "units", "var_type")
    m <- essnt_params[sapply(essnt_params, function(x) is.null(get(x)))]
    if (length(m) > 0) {
      stop(paste("Missing parameters:", paste(essnt_params, collapse = ", ")))
    }
    # standardise types of measures, units and var_types ...
    ipayipi::sts_phen_var_type
    ivar_type <- sts_phen_var_type[phen_syn %ilike% var_type]$phen_prop
    if (length(ivar_type) > 1 || length(ivar_type) < 1) {
      stop(paste("Phenomena var_type mismatch: ",
        paste(var_type, collapse = ", "), collapse = ""))
    }
    var_type <- ivar_type
    sts_phen_measure <- ipayipi::sts_phen_measure
    imeasure <- sts_phen_measure[phen_syn %ilike% measure]$phen_prop
    if (length(imeasure) > 1 || length(imeasure) < 1) {
      stop(paste("Phenomena measure mismatch:",
        paste(measure, collapse = ", "), collapse = ""))
    }
    measure <- imeasure
  }
  if (!is.null(fork_table) && is.null(dt_syn_bc)) {
    stop(paste("Provide logical syntax before comma (`dt_syn_bc`)",
      "when forking a table.", collapse = ""))
  }
  # set extraneous null args and defaults --- keeps parse short
  if (temp_var) temp_var <- NULL

  x <- list(
    dt_syn_bc = dt_syn_bc,
    dt_syn_ac = dt_syn_ac,
    dt_syn_exc = dt_syn_exc,
    temp_var = temp_var,
    measure = measure,
    units = units,
    var_type = var_type,
    notes = notes,
    fork_table = fork_table,
    ...)
  # remove extraneous args
  x <- x[!sapply(x, function(z) is.null(z))]

  # convert to custom
  return(x)
}
