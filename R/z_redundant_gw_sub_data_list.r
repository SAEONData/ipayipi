#' @title List raw data files files in a directory
#' @description A function to search and list data files files in a directory.
#' @param input_dir The folder, that is, directory in which to search
#'  for solonist xle files.
#' @param file_type String. Select from the following options:
#'  "xle", "rain_hobo".
#' @param recurr Should the search be recursive i.e., also include
#'  subdirectories.
#' @keywords import data files into data processing pipeline.
#' @export
#' @author Paul J. Gordijn
#' @return a vector of xle file paths.
#' @details This is used as a helper function. The search is based on the
#'  file extension for the specified file type. The following strings
#'  are excluded for each of the following file types:
#'  * **xle** '.lev' (old solonist files); 'Compensat' (compensated
#'  solonist files)
#'  * **rain_hobo** N/A
#' Additional files can be added and rules for search exclusion in the
#' function.
#' @md
#' @examples
#' # Generate a list of xle files in the working directory
#' dir <- "." # define the directory
#' gw_xle_list(input_dir = dir, file_type = "xle", recurr = FALSE)

data_list <- function(
  input_dir = NULL,
  file_type = NULL,
  recurr = FALSE,
  ...) {

  supported_data_types <- c(
    "rain_hobo", # hobo logger csv exports
    "xle" # solonist data files
  )
  if (!is.null(file_type) | !file_type %in% c("rain_hobo", "xle")) {
    stop("Specify file type using recognized string")
  }

  # table of which patterns to exclude from the file lists
  exclude_df <- data.table(
    supp_file_type = c(
      "rain_hobo", # hobo logger csv exports
      "xle" # solonist data files
    ),
    excl1 = c(
      NA,
      "Compensat"
    ),
    excl2 = c(
      NA,
      ".lev"
    )
  )
  # extract relevant patterns used to exclude files from list
  exclude_df <- as.character(exclude_df[which(
    exclude_df$supp_file_type == file_type),
    2:length(colnames(exclude_df))])

  ## list files in directory
  dtafiles_path <- list.files(
    path = input_dir, pattern = "*.xle",
    recursive = recurr, include.dirs = T, full.names = T)

  # remove unwanted files by pattern
  for (w in seq_along(exclude_df)) {
    xlefiles_path <- setdiff(
      xlefiles_path,
      xlefiles_path[which(grepl(x = xlefiles_path,
      pattern = exclude_df[w]))]
    )
  }
  if (length(xlefiles_path) > 0) {
    names(xlefiles_path) <- c(seq_along(xlefiles_path))
  }
  return(xlefiles_path)
}