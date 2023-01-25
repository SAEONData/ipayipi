#' @title Appends a list of tables
#' @description Appends tables based on date-time stamps, and unique rows.
#' @param original_tbl_list List of original tables to append. Both original
#'  and new table lists must be named.
#' @param new_tbl_list Named list of new tables to append to the originals.
#' @param overwrite_old NOTE: THIS FUNCTIONALITY HAS NOT YET BEEN INCLUDED.
#'  Logical. If `TRUE` then original data is disgarded
#'  in favour of new data. If there are NA values, in either old or new data,
#'  these are not retained if there are values in the old or new data that can
#'  replace these. Defaults to FALSE.
#' @keywords append data, overwrite data, join tables,
#' @export
#' @author Paul J Gordijn
#' @return List of joined tables.
#' @details This function is an internal function called by others in the
#'  pipeline.
append_tables <- function(
  original_tbl = NULL,
  new_tbl = NULL,
  overwrite_old = FALSE,
  ...) {
  otbl_names <- names(original_tbl)
  ntbl_names <- names(new_tbl)

  # check for unique table names and make blank tables to infill
  uotbl_names <- otbl_names[!names(otbl_names) %in% ntbl_names]
  untbl_names <- ntbl_names[!names(ntbl_names) %in% otbl_names]
  # create empty tables for simple appending so each table name has a pair
  uotbl <- lapply(untbl_names, function(x) {
    original_tbl[x] <- new_tbl[x]
    original_tbl[[x]] <- original_tbl[[x]][0, ]
    return(original_tbl[[x]])
  })
  names(uotbl) <- untbl_names
  untbl <- lapply(uotbl_names, function(x) {
    new_tbl[x] <- original_tbl[x]
    new_tbl[[x]] <- new_tbl[[x]][0, ]
    return(new_tbl[[x]])
  })
  names(untbl) <- uotbl_names
  original_tbl <- c(original_tbl, uotbl)
  new_tbl <- c(new_tbl, untbl)

  # append paired tables
  tbl_names <- unique(otbl_names, ntbl_names)
  tbls <- lapply(seq_along(tbl_names), function(i) {
      tbl <- rbind(original_tbl[[tbl_names[i]]], new_tbl[[tbl_names[i]]],
        fill = TRUE)
      tbl <- unique(tbl)
      return(tbl)
  })
  names(tbls) <- tbl_names
  # sort table order
  tbls <- lapply(tbls, function(x) {
    if ("date_time" %in% names(x)) x <- x[order(date_time)]
    if ("start_dt" %in% names(x)) x <- x[order(start_dt, end_dt)]
    return(x)
  })
  return(tbls)
}