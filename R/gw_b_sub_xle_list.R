#' @title List xle files in a directory
#' @description A function to search and list .xle files in a directory.
#' @param input_dir The folder, that is, directory in which to search
#'  for solonist xle files.
#' @param recurr Should the search be recursive i.e., also include
#'  subdirectories.
#' @keywords solonist xle files
#' @export
#' @author Paul J. Gordijn
#' @return a vector of xle file paths.
#' @details This is used as a helper function. The search is based on the
#'  file extension .xle. Older solonist levelogger files with extension
#'  .lev are excluded.
#' @examples
#' # Generate a list of xle solonist files in the working directory
#' dir <- "." # define the directory
#' gw_xle_list(input_dir = dir, recurr = FALSE)

gw_xle_list <- function(
  input_dir = NULL,
  recurr = FALSE,
  ...) {
  xlefiles_path <- list.files(
    path = input_dir, pattern = "*.xle",
    recursive = recurr, include.dirs = T, full.names = T)
  xlefiles_path <-
    setdiff(
      xlefiles_path,
      xlefiles_path[which(grepl(x = xlefiles_path,
      pattern = "Compensat"))]
    )
  xlefiles_path <- setdiff(
    xlefiles_path,
    xlefiles_path[which(grepl(x = xlefiles_path, pattern = ".lev"))]
  )
  if (length(xlefiles_path) > 0) {
    names(xlefiles_path) <- c(seq_along(xlefiles_path))
  }
  return(xlefiles_path)
}