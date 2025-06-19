#' @title Set custom aggregation parameters
#' @description Used to generate a list of cusstom aggregation parameters, that is, `agg_params` using ipayipi::agg_params() for particular phenomena.
#' @details `agg()` is an `ipayipi` alias for `list()` but is structured to modify aggregation parameters for specific phenomena. As such, list names must equal the 'phen_name' of interest. List objects, for associated 'phen_names' must be generated using `ipayipi::agg_params()`. Aggregation parameters provided here will override default aggregation parameters. The `ipayipi::sts_agg_functions` table stores default aggregation functions for different phenomena types.
#' Set `all_phens` to `FALSE` (in the `pipe_seq` generation---_see_ `ipayipi::agg_param_eval()`) to limit the phenomena being aggregated in a table to those named and listed in `aggs()`.
#' @return List custom aggregation parameters for each phenomena listed.
#' @author Paul J. Gordijn
#' @examples
#'
#' # use of aggs() within a pipe_seq stage, step.
#' # rain_mm is the phenomena of interest in the data being
#' # aggregated.
#'  p_step(dt_n = 6, dtp_n = 3, f = "dt_agg",
#'   agg_param_eval(
#'     agg_offset = c("8 hours", "8 hours"),
#'     agg_intervals = c("daily"),
#'     ignore_nas = TRUE,
#'     all_phens = FALSE,
#'     agg_parameters = aggs(
#'       rain_mm = agg_params(units = "mm", phen_out_name = "rain_tot")
#'     ),
#'     agg_dt_suffix = "_agg_saws"
#'   )
#' )
#' @export
aggs <- function(
  ...
) {
  x <- list(...)
  # x <- aggs(rain_mm = agg_params(phen_out_name = "rain",
  #   units = "mm"), dew_drop = agg_params(phen_out_name =
  #   "dew", measure = "smp"))

  # ensure phen names are provided
  xn <- names(x)
  if (is.null(xn) || "" %in% xn) {
    cli::cli_inform(c("!" = "Missing aggregation parameters:",
      "i" = "Missing {.var agg_params} names in {.var aggs}!"
    ))
    x <- x[!sapply(xn, function(x) is.null(x) || x == "")]
  }
  class(x) <- c("agg_params", "list")
  return(x)
}