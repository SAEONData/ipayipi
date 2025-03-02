#' @title flag4.5
#' @description Small function to flag range issues in 5 min wind speed data
flag4.5 <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA_complex_)
  mn <- min(x)
  mx <- max(x)
  d <- data.table::fifelse(abs(mx - mn) < 3, TRUE, FALSE)
  return(d)
}