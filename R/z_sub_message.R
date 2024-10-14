#' @title Internal function for returning message or not
#' @description An alias to return a message.
#' @param x A message string.
#' @param verbose If TRUE the message will print, and _vice versa_ for FALSE.
#' @author Paul J. Gordijn
#' @export
#' @noRd
#' @keywords Internal
msg <- function(x = NULL, verbose = TRUE) {
  if (verbose) message(x)
}