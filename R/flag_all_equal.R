#' @title flag all equal
#' @description Function to flag uniform windows
#' @param x Vector of numeric values to evaluate.
#' @param ignore Numeric vector with values to ignore from all equal test.
#' @details Vectors are ignored if NA values are detected. One can choose to ignore certain numeric values provided to the `ignore` argument. These are removed from `x` if present and the test is only performed if the length of the vector after ignoring values is 95% of the original `x`'s length.
#' @return Logical TRUE for all values tested being identical and FALSE when there are multiple unique values in `x`.
#' @author Paul J. Gordijn
#' @export
flag_all_equal <- function(x, ignore = NULL, ...) {
  # ignore if NA values present
  if (length(x) != length(x[!is.na(x)])) return(as.logical(NA))
  # remove ignore values
  if (!is.null(ignore)) {
    y <- x[!x %in% ignore]
    # only perform test if 95% of values are not ignored
    if (length(y) < as.integer(0.95 * length(x))) return(as.logical(0))
    y <- unique(x)
  }
  y <- unique(x)
  # if there is only one unique value return a positive (TRUE) else FALSE
  d <- if (length(y) == 1) 1 else 0
  return(as.logical(d))
}
