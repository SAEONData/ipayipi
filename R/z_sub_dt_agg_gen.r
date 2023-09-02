#' @title Organise sequencial processing of phenomena aggregations.
#' @description An alias for list in pipe processing.
#' @param agg_offset A vector of two strings that can be coerced to a
#'  `lubridate` period. These can be used to offset the date-time from
#'  which aggregations are totalled. For example, for rainfall totals estimated
#'  from 8 am to 8pm the `agg_offset` should be set to c(8 hours, 8 hours).
#' @author Paul J. Gordijn
#' @export
agg_gen <- function(
  ...) {
  x <- list(...)
  class(x) <- c("agg_params", "list")
  return(x)
}