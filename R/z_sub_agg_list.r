#' @title Organise sequencial processing of phenomena aggregations.
#' @description An alias for list in pipe processing.
#' @param agg_offset A vector of two strings that can be coerced to a
#'  `lubridate` period. These can be used to offset the date-time from
#'  which aggregations are totalled. For example, for rainfall totals estimated
#'  from 8 am to 8pm the `agg_offset` should be set to c(8 hours, 8 hours).
#' @author Paul J. Gordijn
#' @export
aggs <- function(
  agg_offset = c("0 secs", "0 secs"),
  ...) {
    x <- list(...)
    x <- list(agg_offset = agg_offset, f_params = x)
    class(x) <- c("f_params", "list")
    return(x)
  }
