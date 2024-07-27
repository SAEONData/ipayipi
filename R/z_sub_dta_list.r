#' @title Generate file list
#' @description Interactively or not, create a list (actually vector)
#'  of files in a specified directory.
#' @author Paul J. Gordijn
#' @keywords Data files; R solonist data vector; hobo files;
#' @param input_dir Folder in which to search for R solonist data files.
#' @param recurr Should the function search recursively
#'  i.e., thorugh sub-folders as well - TRUE/FALSE.
#' @param file_ext Character string of the file extension of the input
#'  data files. E.g., ".csv" for hobo rainfall file exports. This input
#'  character string should contain the period as in the previous sentence.
#' @param wanted Vector of strings listing files that should not be
#'  included in the import.
#' @param baros Should the function include barometric files
#'  in the final list - TRUE/FALSE. This parameter is specifically for working
#'  with groundwater data.
#' @param prompt Set to TRUE for interactive mode.
#' @param unwanted Vector of strings listing files that should not be included
#'  in the import.
#' @export
#' @return A vector of selected R data solonist files.
dta_list <- function(
  input_dir = ".",
  file_ext = NULL,
  wanted = NULL,
  prompt = FALSE,
  recurr = FALSE,
  baros = FALSE,
  unwanted = NULL,
  single_out = FALSE,
  ...
) {
  "%ilike%" <- NULL
  slist <- list.files(path = input_dir, pattern = file_ext, recursive = recurr)

  if (baros == FALSE) {
    bbs <- grep(x = slist, pattern = "BARO", ignore.case = FALSE)
    bbs <- slist[bbs]
    # add in unwanted file names here
    unwanted <- c("rdta_log.rds", "transfer_log.rds", "nomtab",
      "data_handle.rdhs", "datum_log", "phentab", unwanted, bbs
    )
    unwanted <- unwanted[!unwanted %in% wanted]
  } else {
    unwanted <- c("rdta_log.rds", "transfer_log.rds", "nomtab",
      "data_handle.rdhs", "datum_log", "phentab", unwanted
    )
    unwanted <- unwanted[!unwanted %in% wanted]
  }
  slist <- slist[!slist %ilike% paste0(unwanted, collapse = "|")]
  slist <- slist[slist %ilike% paste0(wanted, collapse = "|")]

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
        answer <- readline(prompt = "Confirm chosen stations Y/n : ")
        if (answer == "Y") {
          return(slist[np])
        } else {
          chosen()
        }
      }
    } else {
      stop("No files selected!")
    }
  }
  if (prompt == TRUE) {
    chosensiz <- chosen()
  } else {
    chosensiz <- slist
  }
  if (length(chosensiz) > 1 && single_out == TRUE) {
    if (prompt) chosensiz <- chosen()[1] else slist[1]
  }
  return(chosensiz)
}
