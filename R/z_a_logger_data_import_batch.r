#' @title Import batches of logger data into the ipayipi 'waiting room'.
#' @description Locates, copies, transforms in R format, then pastes logger
#'  data files into the 'wait_room'.
#' @param source_dir The path directory from where Cambell logger data files
#'  are located.
#' @param wait_room Path to the 'wait_room' directory.
#' @param prompt Should the function use an interactive file selection function
#'  otherwise all files are returned. TRUE or FALSE.
#' @param recurr Should the function search recursively into sub directories
#'  for hobo rainfall csv export files? TRUE or FALSE.
#' @param wanted Vector of strings listing files that should not be
#'  included in the import.
#' @param unwanted Vector of strings listing files that should not be included
#'  in the import.
#' @param file_ext The file extension defaults to ".dat". Other file types could
#'  be incorporatted if required.
#' @keywords import logger data files; meteorological data; automatic weather
#'  station; batch process; hydrological data;
#' @author Paul J. Gordijn
#' @details Copies logger data files into a directory where further data
#'  standardisation will take place in the 'ipayipi' data pipeline.
#' @export
logger_data_import_batch <- function(
  source_dir = ".",
  wait_room = NULL,
  prompt = FALSE,
  recurr = FALSE,
  wanted = NULL,
  unwanted = NULL,
  file_ext = ".dat",
  ...
) {
  # get list of data to be imported
  slist <- dta_list(input_dir = source_dir, file_ext = file_ext,
    prompt = prompt, recurr = recurr, unwanted = unwanted, wanted = wanted)
  cr_msg <- padr(core_message =
    paste0(" Introducing ", length(slist),
      " data files into the pipeline waiting room", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  slist_df <- data.table::data.table(
    name = slist,
    basename = basename(slist),
    rep = rep(NA_integer_, length(slist))
  )
  slist_dfs <- split(slist_df, f = factor(slist_df$basename))
  slist_dfs <- lapply(slist_dfs, function(x) {
    x$rep <- seq_len(nrow(x))
    return(x)
  })
  slist_dfs <- data.table::rbindlist(slist_dfs)
  ccat <- lapply(seq_len(nrow(slist_dfs)), function(x) {
    file.copy(
      from = file.path(source_dir, slist_dfs$name[x]),
      to = file.path(wait_room,
        paste0(gsub(pattern = file_ext, replacement = "",
          x = slist_dfs$basename[x], ignore.case = TRUE),
          "__", slist_dfs$rep[x], file_ext)
      ), overwrite = TRUE
    )
    cr_msg <- padr(core_message =
      paste0(basename(slist[x]), " done ...", collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(-1, 2))
    message(cr_msg)
  })
  rm(ccat)
  cr_msg <- padr(core_message =
    paste0(" Import complete ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  return(message(cr_msg))
}