#' @title Data extractor by row and column index
#' @description A predefined function to return the row and column index in a standardised way, that is, providing the row, then column index, in that order, respectively, in a vector of length two.
#' @param ri The row index provided as an integer.
#' @param ci The column index provided as an integer.
#' @param setup_name The name of the item/data found at the defined index. A   character string.
#' @details This is an internal function not meant to be used as stand alone   feature.
#' @return A vector of length two containing interger indicies for a row and   column, respectively.
#' @author Paul J. Gordijn
#' @export
rici <- function(ri = NULL, ci = NULL, setup_name = NA, ...) {
  if (any(is.null(ri), is.null(ci))) {
    message(paste0("Both row index and column index must be supplied for ",
      setup_name
    ))
    message(paste0("E.g., ", setup_name, " = rici(ri = 1, ci = 3)"))
    stop("Supply ri ci values!")
  }
  return(as.integer(c(ri, ci)))
}
