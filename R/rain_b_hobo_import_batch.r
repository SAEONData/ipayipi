#' @title Import batches of hobo data into the 'wait_room'
#' @description Introduces hobo rainfall data 'csv' exports into the *ipayipi*
#'  rainfall data pipeline.
#' @param source_dir The path directory from where hobo rainfall files are to
#'  be read.
#' @param wait_room Path to the 'wait_room' directory.
#' @param prompt Should the function use an interactive file selection function
#'  otherwise all files are returned. TRUE or FALSE.
#' @param recurr Should the function search recursively into sub directories
#'  for hobo rainfall csv export files? TRUE or FALSE.
#' @param wanted Vector of strings listing files that should not be
#'  included in the import.
#' @param unwanted Vector of strings listing files that should not be included
#'  in the import.
#' @param file_ext The file extension defaults to ".csv". Other file types could
#'  be incorporatted if required.
#' @keywords hoboware, tipping bucket rain gauge, data cleaning, false tips,
#'  data pipeline, data import
#' @author Paul J. Gordijn
#' @return Terminal printout of files transferred indicating the success of
#'  reading the data in the appropriate format.
#' @details This function simply copies the csv hobo exports and pastes these
#'  into the specified 'wait_room' folder. Duplicate file names are handled by
#'  appending a number to the end of file names when pasting these.
#' @export
rain_hobo_import_batch <- function(
  source_dir = ".",
  wait_room = NULL,
  prompt = FALSE,
  recurr = FALSE,
  wanted = NULL,
  unwanted = NULL,
  file_ext = ".csv",
  ...
) {
  # get list of data to be imported
  slist <- dta_list(input_dir = source_dir, file_ext = file_ext,
    prompt = prompt, recurr = recurr, unwanted = unwanted, wanted = wanted)
  cr_msg <- padr(core_message =
    paste0(" Importing ", length(slist),
      " hobo files into the pipeline ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = TRUE, justf = c(0, 0))
  message(cr_msg)
  slist_df <- data.table::data.table(
    name = slist,
    rep = NA
  )
  slist_dfs <- split(slist_df, f = factor(slist_df$name))
  slist_dfs <- lapply(slist_dfs, function(x) {
    x$rep <- seq_len(nrow(x))
    return(x)
  })
  slist_dfs <- data.table::rbindlist(slist_dfs)
  ccat <- lapply(seq_along(slist), function(x) {
    file.copy(
      from = file.path(source_dir, slist[x]),
      to = file.path(wait_room,
        paste0(gsub(pattern = ".csv", replacement = "",
          x = basename(slist[x]), ignore.case = TRUE),
          "__", slist_dfs[x, "rep"], ".csv")
      ), overwrite = TRUE
    )
    cr_msg <- padr(core_message =
      paste0(basename(slist[x]), " done ...", collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = TRUE, justf = c(-1, 2))
    message(cr_msg)
  })
  cr_msg <- padr(core_message =
    paste0(" Import complete ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  return(message(cr_msg))
}
