#' @title Data extractor by row and column indicies
#' @description A predefined function to return the row and column indicies in a standardised way, that is, providing the row, then column indicies, in that order, respectively, in a list of length two.
#' @param r_rng The row indicies provided as a vector.
#' @param c_rng The column indicies provided as a vector.
#' @param r_fx Single integer value corresponding to the row number wherein lie all row indicies of interest.
#' @param c_fx Single integer value corresponding to the column number wherein lie all column indicies of interest.
#' @param c_fx_start If the `c_rng` or `c_fx` parameters are not supplied the function needs the start (`c_fx_start`) and end (`c_fx_end`) of the column indicies where phenomena are stored.
#' @param c_fx_end If the `c_rng` or `c_fx` parameters are not supplied the function needs the start (`c_fx_start`) and end (`c_fx_end`) of the column indicies where phenomena are stored. `c_fx_end` can be extracted from the supplied file if this option is set to "extract".
#' @param string_extract If these options are specified the function will store parameters for the extraction of a single character string from the defined rici range when reading logger data.
#'   A list made up of the following objects (all requried):
#'    1. `rng_pattern` A string used to search the range used to identify the position of the substring of interest to be extracted.
#'    1. `rel_start` The starting position (integer) of the string relative to the begining of the `rng_pattern`.
#'    1. `rel_end` The end position (integer) of the string relative to the begining of the `rng_pattern`.
#' @param setup_name The name of the item/data found at the defined index. A character string.
#' @details This is an internal function not meant to be used as stand alone feature.
#' @return A list of length two containing interger indicies for a row and column, respectively.
#' @export
#' @author Paul J. Gordijn
rng_rici <- function(
  r_rng = NULL,
  c_rng = NULL,
  r_fx = NULL,
  c_fx = NULL,
  c_fx_start = NULL,
  c_fx_end = NULL,
  setup_name = NA,
  string_extract = NULL,
  ...) {
  # make sure there is a complete set of row and column indicies
  unknown <- c_fx_end
  if (c_fx_end == "extract") c_fx_end <- c_fx_start
  if (!is.null(c_fx_start) || !is.null(c_fx_end)) {
    c_rng <- c_fx_start:c_fx_end
  }
  if (any(is.null(r_rng) && is.null(r_fx), is.null(c_rng) && is.null(c_fx))) {
    message(paste0("If the row or column range are not supplied, ",
      "a fixed row or column, respectively, must be supplied."
    ))
    stop("Missing row or column index value(s)")
  }
  if (is.null(r_rng)) r_rng <- rep(r_fx, times = length(c_rng))
  if (is.null(c_rng)) c_rng <- rep(c_fx, times = length(r_rng))
  if (length(r_rng) != length(c_rng)) {
    message(paste0("For ", setup_name))
    stop("Lengths of row and column indicies differ.")
  }
  if (unknown == "extract") c_rng <- c(paste0(c_fx_start), unknown)
  rng <- list(r_rng = r_rng, c_rng = c_rng, string_extract = string_extract)
  class(rng) <- c("ipayipi rng", "list")
  return(rng)
}
