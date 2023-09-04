#' @title A wrapper for the circular.mean function
#' @description Find the mean angle/azimuth
#' @param x Vector of angles.
#' @param u Angle units. Either degrees or radians.
#' @return The mean angle.
#' @author Paul J. Gordijn
#' @export
circular_mean <- function(x, u = "degrees", ignore_nas = FALSE) {
  x <- circular::circular(x, units = u, type = "angles",
    template = "geographics", rotation = "clock", modulo = "2pi",
      names = NULL, zero = 0)
  x <- mean(x, na.rm = ignore_nas)
  return(x)
}