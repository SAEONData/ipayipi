#' @title Set custom aggregation parameters
#' @description Used to generate a list of cusstom aggregation parameters, that is, `agg_params` using ipayipi::agg_params().
#' @details `agg()` is an `ipayipi` alias for `list()`, that will only include elements generated using `ipayipi::agg_params()`. Aggregation parameters provided here will override default aggregation parameters. The `ipayipi::sts_agg_functions` table stores default aggregation functions for different phenomena types.
#' @return List custom aggregation parameters for each phenomena listed.
#' @author Paul J. Gordijn
#' @export
aggs <- function(
  ...) {
  x <- list(...)
  # x <- aggs(rain_mm = agg_params(phen_out_name = "rain",
  #   units = "mm"), dew_drop = agg_params(phen_out_name =
  #   "dew", measure = "smp"))

  # ensure phen names are provided
  xn <- names(x)
  if (is.null(xn) || "" %in% xn) {
    message("Missing \'agg_params\' names in \'aggs\'!")
    x <- x[!sapply(xn, function(x) is.null(x) || x == "")]
  }
  class(x) <- c("agg_params", "list")
  return(x)
}