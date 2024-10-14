#' @title Pipeline function - lists R data files
#' @description Designed for use within the package pipeline. A simple function
#' that generates a list of RDS files in a directory. The function
#' discriminates "rdta_log.rds" and "transfer_log.rds" files - other files with
#' an ".rds" file extension will not be discriminated.
#' @param input_dir Direcytory for which to generate a list of .rds files.
#' @param recurr Logical. Should the function search recursively i.e., through
#' sub folders.
#' @keywords pipeline; RDS
#' @author Paul J Gordijn
#' @return Vector of R solonist data files in a directory.
#' @export
#' @examples
#' ## This function is a helper function not designed for isolated use.
#' ## Get list of solonist data files which have been converted to
#' # 'rds' format.
#' input_dir <- "./solR_dta" # set directory where processed data is maintained.
#' gw_rdta_list(input_dir = input_dir, recurr = FALSE)
#' # Note we did not search recursively through folders by setting recurr
#' # to FALSE
gw_rdta_list <- function(
    input_dir = ".",
    recurr = FALSE) {
  rdta_path <- list.files(path = input_dir, pattern = "*.rds",
    recursive = recurr, include.dirs = TRUE, full.names = TRUE)
  rdta_path <- setdiff(rdta_path,
    rdta_path[which(grepl(x = rdta_path, pattern = "rdta_log"))])
  rdta_path <- setdiff(rdta_path,
    rdta_path[which(grepl(x = rdta_path, pattern = "transfer_log"))])
  return(rdta_path)
}