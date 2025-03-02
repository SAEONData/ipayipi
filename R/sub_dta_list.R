#' @title Generate file list
#' @description Interactively or not, create a list (actually vector) of files in a specified directory.
#' @author Paul J. Gordijn
#' @keywords Data files; R solonist data vector; hobo files;
#' @param input_dir Folder in which to search for R solonist data files.
#' @param file_ext Character string of the file extension of the input data files. E.g., ".csv" for hobo rainfall file exports. This input character string should contain the period as in the previous sentence.
#' @param wanted Regex string of files to select for listing. Seperate search tags by using the bar character '|'.
#' @param unwanted Regex string of files to filter out the listing. Seperate search tags by using the bar character '|'.
#'  in the import.
#' @param prompt Set to TRUE for interactive mode. Note this will not work if embedded in a parallel processing instance.
#' @param single_out Logical. If `TRUE` then only a single item in the list of files will be returned. When `prompt` is `TRUE` then this can be achieved interactively.
#' @param recurr Should the function search recursively i.e., thorugh sub-folders as well - `TRUE`/`FALSE.`
#' @param verbose Logical. Print some details and progress of function progress?
#' @param xtra_v Logical. Should some 'x'tra messaging be done? Use to help diagnose problems, and for guidance.
#' @export
#' @return A vector of selected files.
dta_list <- function(
  input_dir = ".",
  file_ext = NULL,
  wanted = NULL,
  unwanted = NULL,
  prompt = FALSE,
  single_out = FALSE,
  recurr = FALSE,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  "%ilike%" <- NULL
  if (xtra_v) cli::cli_inform(c(
    "i" = "Checking for data files (ext = {file_ext}) here: {input_dir} with:",
    " " = "these regex search patterns {.var wanted} = {wanted}, and",
    " " = "these search filters: {.var unwanted} = {unwanted}."
  ))
  slist <- list.files(path = input_dir, pattern = file_ext, recursive = recurr)

  # more unwanted files
  unwanted <- paste0(c("rdta_log.rds", "transfer_log.rds", "aa_nomtab",
      "data_handle.rdhs", "datum_log", "aa_phentab", unwanted
    ), collapse = "|"
  )
  wanted <- paste0(wanted, collapse = "|")
  unwanted <- gsub(pattern = wanted, replacement = "", x = unwanted)
  unwanted <- gsub(pattern = "\\|\\|", replacement = "|", x = unwanted)
  unwanted <- gsub(pattern = "^\\||\\|$", replacement = "", x = unwanted)
  wanted <- gsub(pattern = "\\|\\|", replacement = "|", x = wanted)
  wanted <- gsub(pattern = "^\\||\\|$", replacement = "", x = wanted)
  slist <- slist[!slist %ilike% unwanted]
  slist <- slist[slist %ilike% wanted]

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
