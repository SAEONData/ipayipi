#' @noRd
flag_all_equal_dt <- function(x, ignore = NULL, ...) {
  # ignore if NA values present
  if (length(x[[1]]) != length(x[!is.na(x[[1]])])) return(as.logical(NA))
  # remove ignore values
  if (!is.null(ignore)) {
    y <- x[!x[[1]] %in% ignore]
    # only perform test if 95% of values are not ignored
    if (length(y) < as.integer(0.95 * length(x[[1]]))) return(as.logical(0))
    y <- unique(x)
  }
  y <- unique(x[[1]])
  # if there is only one unique value return a positive (TRUE) else FALSE
  d <- if (length(y) == 1) 1 else 0

  # check for variation in temperature
  d <- if (isTRUE(sd(x[[2]]) > 0.5)) 1 else 0

  return(as.logical(d))
}