#' @title Convert rain imported standardised nomvet file into generic format
#' @description Opens hobo rainfall files and coverts these into a new format
#'  compatible with the generic rainfall files.
#' @author Paul J. Gordijn
#' @keywords graphs; data management; hobo files;
#' @param input_dir Folder in which to search for standardised SAEON hobo
#'  rainfall files.
#' @param recurr Should the function search recursively? I.e., thorugh
#'  sub-folders as well --- `TRUE`/`FALSE`.
#' @param wanted Character string of the station keyword. Use this to
#'  filter out stations that should not be plotted alongside eachother. If more
#'  than one search key is included, these should be separated by the bar
#'  character, e.g., `"mcp|manz"`, for use with data.table's `%ilike%` operator.
#' @param unwanted Vector of strings listing files that should not be included
#'  in the import.
#' @param prompt Logical. If `TRUE` the function will prompt the user to
#'  identify stations for plotting. The wanted and unwanted filters
#'  still apply when in interactive mode (`TRUE`).
#' @author Paul J. Gordijn
#' @export
rain2met_format <- function(
  input_dir = ".",
  recurr = FALSE,
  wanted = NULL,
  unwanted = NULL,
  prompt = FALSE,
  ...
) {
  slist <- ipayipi::dta_list(input_dir = input_dir, file_ext = ".rds",
    prompt = FALSE, recurr = recurr, unwanted = unwanted, wanted = wanted)

}