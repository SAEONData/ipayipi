#' @title Develop a 'data.table' chain link for processing data
#' @description Link together multiple data.table operations into a single chain.
#' @param i `data.table` before first-comma syntax. Before the first comma syntax operates on a data.tables rows. Can be provided as a multiple length vector that will be pasted with no seperator into one string. Same for `j` below.
#' @param j `data.table` after first-comma syntax. After the first comma syntax operates on columns. _See examples_.
#' @param measure If a new vatiable is described it's measure should be provided. Measure's are standardised using the `ipayipi::sts_phen_measure` standard table. New variables can be assigned using the ":=" operator.
#' @param units The unit of measure of a new variable, e.g., "mm" for rainfall, or "deg_c" for degrees Celcius. _Notice conformity to __snake case__ descriptions --- this must be applied_.
#' @param var_type The type (or class) of variable, e.g., "num" for numeric. These are standardised using the `ipayipi::sts_phen_var_type` table.
#' @param notes An additional description of the calculation/operation can be provided.
#' @param fork_tbl If not `NULL` (default) a duplicate table of the reverse `dt_syn_bc` (before comma logic) will be produced and kept in the data pipeline.
#' @param temp_var Indicates whether an introduced variable is temporary. If temporary it will be removed during the processing stage.
#' @param link Integer denoting the numeric order of a chain link in a sequence put together in a list.
#' @details
#' * New phenomena (variables):
#'  In order to describe phenomena details for further processing it is important to provide metadata as they are introduced. Each time the data.table operator ':=' is used this serves as a que that a new vairable has been added to the processing pipeline. For this, respective 'measure', 'units', and 'var_type' properties must be described.
#'
#' NB! This parameter `dt_syn_ac` and `dt_syn_bc` are being phased out in favor of `i` and `j`.
#' @examples
#' # Set up chain link to filter out zero mm rainfall events
#' x <- list(remove0 = chainer(i = "rain_cumm != 0"))
#' x
#' # Two stage chaining to filter out zero mm rainfall events then calculate
#' # sequential time lag (assign a variable called 't_lag').
#' # Note use of data.table's .N (number of rows).
#' x <- list(
#'  remove0 = chainer(i = "rain_cumm != 0"),
#'  t_lag = chainer(
#'    j = "t_lag := c(0, date_time[2:.N] - date_time[1:(.N - 1)])",
#'    measure = "smp", units = "time", var_type = "num"
#'  )
#' )
#' print(x)
#'
#' # Transform chain links in to data.table syntax
#' calc_param_eval(x)
#'
#' @author Paul J. Gordijn
#' @export
chainer <- function(
  i = NULL,
  j = NULL,
  measure = NULL,
  units = NULL,
  var_type = NULL,
  notes = NULL,
  fork_tbl = NULL,
  temp_var = FALSE,
  link = NULL,
  dt_syn_bc = NULL,
  dt_syn_ac = NULL,
  ...
) {
  "phen_syn" <- "%ilike%" <- NULL
  # below helps transition frm calling dt_syn_bc, i, and dt_syn_ac, j.
  # for now both will work until a future version is pblished.
  if (!is.null(dt_syn_bc)) i <- dt_syn_bc
  if (!is.null(dt_syn_ac)) j <- dt_syn_ac
  # convert i and j to strings, if vectors
  if (is.vector(i)) i <- paste0(i, collapse = "")
  if (is.vector(j)) j <- paste0(j, collapse = "")
  x <- list(i = i, j = j, measure = measure, units = units,
    var_type = var_type, notes = notes, fork_tbl = fork_tbl,
    link = link
  )
  u <- list(...)
  m <- lapply(u, function(z) {
    cli::cli_inform(c(
      "i" = "In chaining:",
      "!" = "Unrecognised arguments: {names(z)}"
    ))
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
