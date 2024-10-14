#' @title Message generic
#' @description Formats message padding and alignment.
#' @details This function is designed to be used within other functions. The
#'  function takes a message and aligns it with padding and a preffix and
#'  suffix.
#' @param core_message String of variable length. The message to be padded by
#'  special characters and
#'  justified.
#' @param wdth Integer or NULL. The width of the message line to be printed
#'  in characters. If NULL the width is set to the console width. The default
#'  console width in R is 80 characters.
#' @param pad_char String. The character to be used to pad a message.
#'  Default "-".
#' @param pad_extras Vector of four characters placed around padding on the
#'  left and right padding of the core message. Characters given in order
#'  from right to left: 1. Left preffix, 2. left suffix, 3. left preffix,
#'  4. right suffix.
#' @param force_extras Logical. If TRUE then the `pad_extras' are implemented
#'  even if the total nuber of characters on the message line exceeds the
#'  `wdth'.
#' @param justf Vector with two integers. The first integer indicates
#'  the message justification (-1 = left justified, 0 = centered, 1 = right
#'  justified). The second integer in the input vector gives the number of
#'  characters by which the `core_message' should be offset to the left or
#'  right.
#' @export
#' @noRd
#' @author Gordijn, Paul J.
#' @return Padded string that can be printed to the terminal/console.
#' @keywords Internal, string, padding, pad, message, print.

padr <- function(
  core_message = NULL,
  wdth = NULL,
  pad_char = c("-"),
  pad_extras = c("|", "", "", "|"),
  force_extras = FALSE,
  justf = c(0, 0)
) {
  ## set width if NULL
  if (is.null(wdth)) wdth <- options()$width

  # determine the number of preffix and suffix chars
  buffr_n <- sum(nchar(pad_extras))
  if (justf[1] == 0) {
    if (wdth > (nchar(core_message) + buffr_n)) {
      pad_infill_n <- wdth - (nchar(core_message) + buffr_n)
      l_pad <- paste0(pad_extras[1],
        strrep(pad_char, ceiling(pad_infill_n / 2)), pad_extras[2],
        collapse = ""
      )
      r_pad <- paste0(pad_extras[3],
        strrep(pad_char, floor(pad_infill_n / 2)), pad_extras[4],
        collapse = ""
      )
    } else {
      if (force_extras) {
        l_pad <- paste0(pad_extras[1], pad_extras[2], collapse = "")
        r_pad <- paste0(pad_extras[3], pad_extras[4], collapse = "")
      }
      if (!force_extras) {
        l_pad <- ""
        r_pad <- ""
      }
    }
  }

  # justified left
  if (justf[1] == -1) {
    if (wdth > (nchar(core_message) + buffr_n)) {
      pad_infill_n <- wdth - (nchar(core_message) + buffr_n)
      l_pad <- paste0(pad_extras[1], strrep(pad_char, justf[2]),
        pad_extras[2], collapse = ""
      )
      r_pad <- paste0(pad_extras[3],
        strrep(pad_char, pad_infill_n - justf[2]), pad_extras[4],
        collapse = ""
      )
    } else {
      if (force_extras) {
        l_pad <- paste0(pad_extras[1], pad_extras[2], collapse = "")
        r_pad <- paste0(pad_extras[3], pad_extras[4], collapse = "")
      }
      if (!force_extras) {
        l_pad <- ""
        r_pad <- ""
      }
    }
  }

  # justified right
  if (justf[1] == 1) {
    if (wdth > (nchar(core_message) + buffr_n)) {
      pad_infill_n <- wdth - (nchar(core_message) + buffr_n)
      if (pad_infill_n - justf[2] < 0) {
        l_padjt <- 0
      } else {
        l_padjt <- pad_infill_n - justf[2]
        l_pad <- paste0(pad_extras[1],
          strrep(pad_char, l_padjt),
          pad_extras[2], collapse = ""
        )
        r_pad <- paste0(pad_extras[3], strrep(pad_char, justf[2]),
          pad_extras[4], collapse = ""
        )
      }
    } else {
      if (force_extras) {
        l_pad <- paste0(pad_extras[1], pad_extras[2], collapse = "")
        r_pad <- paste0(pad_extras[3], pad_extras[4], collapse = "")
      }
      if (!force_extras) {
        l_pad <- ""
        r_pad <- ""
      }
    }
  }
  invisible(return(paste0(l_pad, core_message, r_pad, collapse = "")))
}