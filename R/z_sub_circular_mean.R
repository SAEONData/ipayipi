#' @title A wrapper for the circular.mean function
#' @description Find the mean angle/azimuth
#' @param x Vector of angles.
#' @param u Angle units. Either degrees or radians.
#' @param verbose Logical indicating whether to print error messages etc.
#' @return The mean angle.
#' @author Paul J. Gordijn
#' @export
circular_mean <- function(x, u = "degrees", ignore_nas = FALSE,
  verbose = FALSE, ...
) {
  x <- circular::circular(x, units = u, type = "angles",
    template = "geographics", rotation = "clock", modulo = "2pi",
    names = NULL, zero = 0
  )
  if (verbose) {
    x <- as.numeric(mean(x, na.rm = ignore_nas))
  } else {
    x <- as.numeric(suppressWarnings(mean(x, na.rm = ignore_nas)))
  }
  return(x)
}