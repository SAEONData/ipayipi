#' @title Finds sequential intervals of change
#' @description Determines when an interval changes in a vector based on a
#'  the change in integer values in a vector.
#' @param int_dta A vector of integer values (e.g., an itentifier in a table).
#' @keywords Internal; window size; internal function
#' @author Paul J Gordijn
#' @return Vector with an index indicating the incremental change in a value.
#' @details This function is an internal function called by others in the pipeline.
#' @export
#' @noRd
change_intervals <- function(
  int_dta = NULL,
  ...
) {
  ":=" <- "q1" <- "id" <- NULL
  qt <- data.table::data.table(
    id = seq_along(int_dta),
    int_dta = int_dta,
    q1 = rep(NA, length(int_dta))
  )
  nona_int_dta <- int_dta[!is.na(int_dta)]
  qt[!is.na(int_dta)]$q1 <- c(FALSE, nona_int_dta[
    seq_len(length(nona_int_dta) - 1) + 1
  ] == nona_int_dta[
    seq_len(length(nona_int_dta) - 1)
  ])
  qt_na <- qt[is.na(q1)]
  qt <- qt[!is.na(q1)]
  qt <- qt[q1 == FALSE, aphid := seq_len(nrow(qt[q1 == FALSE, ]))]
  # add penultimate fake row
  fake_id <- max(qt$id) + 1
  qtf <- data.table::data.table(
    id = fake_id,
    int_dta = max(qt$int_dta) + 1,
    q1 = FALSE,
    aphid = max(qt$aphid, na.rm = TRUE) + 1
  )
  qt <- rbind(qt, qtf)
  aphid <- qt[!is.na(aphid)]$aphid
  for (i in seq_along(seq_len(length(aphid) - 1))) {
    qt[which(qt$aphid == i):(which(qt$aphid == i + 1) - 1), "aphid"] <- i
  }
  qt <- rbind(qt[id != fake_id], qt_na, fill = TRUE)
  qt <- qt[order(id)]
  return(qt$aphid)
}