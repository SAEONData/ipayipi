#' @title Finds sequential intervals of change
#' @description Defines consecutive intervals of change based on the presence of NA values in a data vector.
#' @param dta Data to assess. In vector format
#' @author Paul J Gordijn
#' @return Vector with a consecutive integers indicating changes between consecutive NA and non-NA values of the input. The length of the output matches that of the input `dta`.
#' @details This function is an internal function being test for release.
#' @export
#' @noRd
na_intervals <- function(
  dta = NULL,
  ...
) {
  ":=" <- NULL
  "cint" <- "int" <- "nat" <- NULL
  dt <- data.table::data.table(dta = dta)
  dt[, nat := is.na(dta)]
  dt[nat == TRUE, "int"] <- 1
  dt[is.na(int), "int"] <- 2
  dt[, cint := change_intervals(int)]
  return(dt$cint)
}