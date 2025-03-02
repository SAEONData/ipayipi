#' @title flag4.7
#' @description Small function to flag range issues in 5 min wind speed data
flag4.7 <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA_complex_)
  m <- mean(x)
  mx <- max(x)
  d <- data.table::fcase(m < 20 & abs(m - mx) > 20, TRUE,
    m > 20 & abs(m - mx) < 20, TRUE, default = FALSE
  )
  return(d)
}