#' @title Window width determination
#' @description returns a customized window for a given vector.
#' @param dta The vector data series of interest.
#' @param rng_side The desired size of the window side---this is
#'  adjusted
#' @param rng_max If there are no data values in the input vectors
#'  then the window side size increases to a maximum size of
#'  _rng_max_.
#' @param c_row Centre position in the vector around which to construct the
#'  window.
#' @param pos Optional string describing the position of the window relative
#'  to the centre row. "centre" attempts to put the _c_row_ in the centre.
#' "lead" attempts to put the window before (and including the c_row) and
#' "trail" attempts to do this after the _c_row_.
#' @param include_center If the center point is not included the range is
#' effectively reduced by one.
#' @keywords Internal
#' @noRd
#' @export
#' @author Paul J Gordijn
#' @return Indicies relating to a window centre and sizes.
#' @details This function is an internal function called by others in the
#'  pipeline.
win_rows <- function(
  dta = NULL,
  rng_side = 1,
  rng_max = 3,
  c_row = NULL,
  pos = "center",
  include_center = TRUE,
  ...
) {
  if (pos %in% c(c("center", "centre", "c"), c("lead", "l"),
      c("trail", "t")
    )
  ) {
    if (length(dta) <= rng_side) {
      message("Insufficient data for drift correction")
    }
    if (include_center == TRUE) {
      win_size <- 1 + 2 * rng_side
    } else {
      win_size <- 2 * rng_side
    }
    i <- rng_side
    if (pos %in% c("center", "centre", "c")) {
      while (i <= rng_side && i <= rng_max) {
        if ((c_row - rng_side) <= 1) ss <- 1
        if ((c_row - rng_side) > 1) ss <- c_row - rng_side
        if ((c_row + rng_side) >= length(dta)) se <- length(dta)
        if ((c_row + rng_side) < length(dta)) se <- c_row + rng_side
        if (length(!is.na(dta[ss:se])) < win_size) {
          i <- i + 1
          rng_side <- rng_side + 1
        } else {
          i <- i + 1
        }
      }
    }
    if (pos %in% c("lead", "l")) {
      while (i <= rng_side & i <= rng_max) {
        if ((c_row - rng_side) <= 1) ss <- 1
        if ((c_row - rng_side) > 1) ss <- c_row - rng_side
        se <- c_row
        if (length(!is.na(dta[ss:se])) < win_size) {
          i <- i + 1
          rng_side <- rng_side + 1
        } else {
          i <- i + 1
        }
      }
    }
    if (pos %in% c("trail", "t")) {
      while (i <= rng_side && i <= rng_max) {
        if ((c_row + rng_side) >= length(dta)) se <- length(dta)
        if ((c_row + rng_side) < length(dta)) se <- c_row + rng_side
        ss <- c_row
        if (length(!is.na(dta[ss:se])) < win_size) {
          i <- i + 1
          rng_side <- rng_side + 1
        } else {
          i <- i + 1
        }
      }
    }
    v <- c(ss:se)
    if (include_center == FALSE) {
      v <- setdiff(v, c_row)
      if (length(!is.na(dta[ss:se])) < win_size) {
        v <- NULL
      }
    }
  } else {
    v <- NULL
  }
  return(v)
}
