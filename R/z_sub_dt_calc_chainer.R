#' @title Develop a 'data.table' chain link for processing data
#' @description Link together multiple data.table operations into a single chain.
#' @param dt_syn_bc `data.table` before first-comma syntax. Before the first comma syntax operates on a data.tables rows.
#' @param dt_syn_ac `data.table` after first-comma syntax. After the first comma syntax operates on columns. _See examples_.
#' @param measure If a new vatiable is described it's measure should be provided. Measure's are standardised using the `ipayipi::sts_phen_measure` standard table. New variables can be assigned using the ":=" operator.
#' @param units The unit of measure of a new variable, e.g., "mm" for rainfall, or "deg_c" for degrees Celcius. _Notice conformity to __snake case__ descriptions --- this must be applied_.
#' @param var_type The type (or class) of variable, e.g., "num" for numeric. These are standardised using the `ipayipi::sts_phen_var_type` table.
#' @param notes An additional description of the calculation/operation can be provided.
#' @param fork_table If not `NULL` (default) a duplicate table of the reverse `dt_syn_bc` (before comma logic) will be produced and kept in the data pipeline.
#' @param temp_var Indicates whether an introduced variable is temporary. If temporary it will be removed during the processing stage.
#' @param link Integer denoting the numeric order of a chain link in a sequence put together in a list.
#' @examples
#' # Set up chain link to filter out zero mm rainfall events
#' x <- list(remove0 = chainer(dt_syn_bc = "rain_cumm != 0"))
#' x
#' # Two stage chaining to filter out zero mm rainfall events then calculate
#' # sequential time lag (assign a variable called 't_lag').
#' # Note use of data.table's .N (number of rows).
#' x <- list(
#'  remove0 = chainer(dt_syn_bc = "rain_cumm != 0"),
#'  t_lag = chainer(
#'    dt_syn_ac = "t_lag := c(0, date_time[2:.N] - date_time[1:(.N - 1)])")
#' )
#' print(x)
#'
#' # Transform chain links in to data.table syntax
#' calc_param_eval(x)
#'
#' @author Paul J. Gordijn
#' @export
chainer <- function(
  dt_syn_bc = NULL,
  dt_syn_ac = NULL,
  measure = NULL,
  units = NULL,
  var_type = NULL,
  notes = NULL,
  fork_table = NULL,
  temp_var = FALSE,
  link = NULL,
  ...
) {
  "phen_syn" <- "%ilike%" <- NULL
  x <- list(dt_syn_bc = dt_syn_bc, dt_syn_ac = dt_syn_ac, measure = measure,
    units = units, var_type = var_type, notes = notes, fork_table = fork_table,
    link = link
  )
  u <- list(...)
  m <- lapply(u, function(z) {
    message("Unrecognised arguments:")
    print(names(z))
  })
  rm(m)
  x <- x[!sapply(x, is.null)]
  if (temp_var) x$temp_var <- temp_var

  # standardise var_type and measure
  if ("var_type" %in% names(x)) {
    x$var_type <- ipayipi::sts_phen_var_type[
      phen_syn %ilike% x$var_type[1]
    ]$phen_prop
  }
  if ("var_type" %in% names(x)) {
    x$measure <- ipayipi::sts_phen_measure[
      phen_syn %ilike% x$measure[1]
    ]$phen_prop
  }
  return(x)
}
