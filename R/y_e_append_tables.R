#' @title Appends a list of tables
#' @description Appends lists of tables (and other list objects) based on date-time stamps, and unique rows.
#' @param original_tbl List of original tables (and other named objects in the list) to append. _Both original and new table lists must be named_.
#' @param new_tbl _Named_ list of new tables (and other objects) to append to the originals.
#' @param overwrite_old If `TRUE` then original data is disgarded in favour of new data. Defaults to FALSE. In the case of non-data.table items new data objects replace old if set to `TRUE`.
#' @keywords Internal, append data, overwrite data, join tables,
#' @export
#' @noRd
#' @author Paul J Gordijn
#' @return List of joined tables.
#' @details This function is an internal function called by others in the pipeline.
append_tables <- function(
    original_tbl = NULL,
    new_tbl = NULL,
    overwrite_old = FALSE,
    ...) {
  "start_dttm" <- "end_dttm" <- "date_time" <- NULL
  # seperate list items and table items
  original_oth <- original_tbl[
    !sapply(original_tbl, FUN = data.table::is.data.table)
  ]
  new_oth <- new_tbl[!sapply(new_tbl, FUN = data.table::is.data.table)]
  original_tbl <- original_tbl[
    sapply(original_tbl, FUN = data.table::is.data.table)
  ]
  new_tbl <- new_tbl[sapply(new_tbl, FUN = data.table::is.data.table)]
  otbl_names <- names(original_tbl)
  ntbl_names <- names(new_tbl)
  if (length(otbl_names) == 0) otbl_names <- NULL
  if (length(ntbl_names) == 0) ntbl_names <- NULL

  # check for unique table names and make blank tables to infill
  uotbl_names <- otbl_names[!otbl_names %in% ntbl_names]
  untbl_names <- ntbl_names[!ntbl_names %in% otbl_names]
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
  tbl_names <- unique(c(otbl_names, ntbl_names))
  tbls <- lapply(seq_along(tbl_names), function(i) {
    tbl <- rbind(original_tbl[[tbl_names[i]]], new_tbl[[tbl_names[i]]],
      fill = TRUE
    )
    tbl <- unique(tbl)
    return(tbl)
  })
  names(tbls) <- tbl_names
  # sort table order
  specl_tbls <- c("gaps", "pipe_seq")
  tbls <- lapply(seq_along(tbls), function(i) {
    x <- tbls[[i]]
    if ("date_time" %in% names(tbls[[i]]) && !names(tbls[i]) %in% specl_tbls) {
      x <- x[order(date_time)]
    }
    if ("start_dttm" %in% tbls[[i]] && !names(tbls[i]) %in% specl_tbls) {
      x <- x[order(start_dttm, end_dttm)]
    }
    return(x)
  })
  names(tbls) <- tbl_names
  # add list items together
  if (overwrite_old) {
    new_oth <- new_oth[!names(new_oth) %in% names(original_oth)]
  } else {
    original_oth <- original_oth[!names(original_oth) %in% names(new_oth)]
  }
  new_oth <- c(original_oth, new_oth)
  tbls <- c(tbls, new_oth)
  return(tbls)
}