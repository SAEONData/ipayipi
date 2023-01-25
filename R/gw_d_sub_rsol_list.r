#' @title Generate R data solonist file list
#' @description Interactively or not, create a list (actually vector)
#'  of R data solonist files in a specified directory.
#' @author Paul J Gordijn
#' @keywords R solonist data vector
#' @param input_dir Folder in which to search for R solonist data files.
#' @param recurr Should the function search recursively
#'  i.e., thorugh sub-folders as well - TRUE/FALSE.
#' @param baros Should the function include barometric files
#'  in the final list - TRUE/FALSE.
#' @param prompt Set to TRUE for interactive mode.
#' @export
#' @return A vector of selected R data solonist files.
gw_rsol_list <- function(
  input_dir = ".",
  recurr = FALSE,
  baros = FALSE,
  prompt = FALSE,
  ...) {

  slist <- list.files(path = input_dir, pattern = ".rds", recursive = recurr)

  if (baros == FALSE) {
    bbs <- grep(x = slist, pattern = "BARO", ignore.case = F)
    bbs <- slist[bbs]
    unwanted <- c("rdta_log.rds", "transfer_log.rds", bbs)
  } else unwanted <- c("rdta_log.rds", "transfer_log.rds")

  slist <- setdiff(slist, unwanted)

  chosen <- function() {
    df <- data.frame(Sites = slist, stringsAsFactors = FALSE)
    print(df)
    n <- readline(
      prompt =
        "Enter row names (e.g. c(1:5,9,12) or c(-12)). quit with <q> : "
      )
    if (n != "q") {
      np <- eval(parse(text = n))
      if (!is.vector(np, mode = "integer")) {
        if (!is.numeric(np)) {
          chosen()
        }
      }
      if (length(slist) < max(np)) {
        print("Incorrect vector size")
        chosen()
      } else {
      df <- data.frame(Sites = slist[np], stringsAsFactors = FALSE)
      print(df)
      answer <- readline(prompt = "Confirm chosen boreholes Y/n : ")
        if (answer == "Y") {
          return(slist[np])
        } else {
          chosen()
        }
      }
    } else stop("No files selected!")
  }
  if (prompt == TRUE) {
    chosensiz <- chosen()
  } else {
    chosensiz <- slist
  }
  return(chosensiz)
}