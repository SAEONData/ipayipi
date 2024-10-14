#' @title Aggregate organisation for phenomena
#' @description Function to organise a list of aggregation parameters. Can be used to override default aggregation parameters, e.g., in the case of aggregating a mean by a maximum to estimate the maximum-mean. Provide as much detail as possible. _The phenomena name is not provided as a parameter to this function --- see `ipayipi::agg_param_eval()`.
#' @param n Optional parameter for assigning an integer to the phenomena being aggregated. Duplicated 'n's will be filtered out in partial evaluation.
#' @param phen_out_name Final aggregated phenomena name (can be different from the phenomena name in the harvested or other data source).
#' @param agg_f Custom aggregation function. Must be supplied with `x` as it's argument, e.g., `mean(x, na.rm = TRUE)`. String variable.
#' @param units Standardised unit of the phenomena.
#' @param measure Measure describing the original phenomena. "__smp__" for sample; "__avg__" for mean or average; "__tot__" for total; "__min__", "__max__" for maximum; or "__sd__" for standard deviation.
#' @param var_type The specified type or class of a variable/phenomenon. "__num__" for numeric or double; "__int__" for integer; "__fac__" for factor; "posix" for dates or date-time stamps; "__chr__" for character or string.
#' @param ignore_nas If not a custom function must `NA` values be ignored in aggregation functions. If `NA` values are ignored then aggregate periods with `NA` values will not be calculated? Logical `TRUE` or `FALSE`.
#' @author Paul J. Gordijn
#' @export
agg_params <- function(
  phen_out_name = NULL,
  agg_f = NULL,
  units = NULL,
  measure = NULL,
  var_type = NULL,
  ignore_nas = TRUE,
  n = NULL,
  ...
) {
  # standard agg parameters
  # agg parameters provided here will be used to overwrite feaults
  d_args <- list(phen_out_name = NULL, agg_f == NULL,
    units = NULL, measure = NULL, var_type = NULL, ignore_nas = TRUE
  )
  a_args <- list(phen_out_name = phen_out_name, agg_f = agg_f, units = units,
    measure = measure, var_type = var_type, ignore_nas = ignore_nas
  )
  x <- lapply(seq_along(a_args), function(i) {
    if (all(sapply(a_args[[i]], function(x) x %in% d_args[[i]]))) {
      return(NULL)
    } else {
      return(a_args[[i]])
    }
  })
  names(x) <- names(a_args)
  x <- x[!sapply(x, is.null)]
  class(x) <- c("agg_params", "list")
  return(x)
}