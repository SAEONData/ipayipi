#' @title Join data sets together
#' @description .
#' @param x_tbl The left, that is, 'x' table.
#' @param y_tbl The right, that is, 'y' table.
#' @param join One of the following join types:
#'  1. _full_join_. Joins the 'x' and 'y' tables using keyed column. Retains all the matching and unmatching rows of 'x' and 'y' tables.
#'  1. _left_join_. Adds the matching rows of 'x' to 'y' by keyed columns.
#'  1. _right_join_. Adds the matching rows of 'y' to 'x' by keyed columns.
#'  1. _inner_join_. Retains only rows of 'x' and 'y' that match by keyed columns.
#' __Keys__
#' Keys are the columns of the 'x' (left) and 'y' (right) tables used for finding matches for a join. Because `ipayipi` is based on date-time data the default keys are assumed to be "date_time" if not provided. Unlike conventional joins where rows are only matched if there is an exact match, for 'fuzzy' or 'non-equi' joins are flexible. When using a 'fuzzy' join an inequality sign must be provided in the key list. _See below__.
#' @param x_key Name(s) of the column(s) to key by the 'x' table. Vector of length one or two for conventional or fuzzy or non-equi joins, respectiely.
#' @param y_key Same as above but for the 'y' or right table.
#' @param fuzzy Numeric duration of time used to generate first and second 'x' and 'y' key columns. If provided fuzzy columns will override the `x_key` and `y_key` columns with auto generated default names. The default names for these are xd1 and xd2, and yd1 and yd2, for 'x' and 'y' tables, respectively. Columns are d1 and d2 columns are calculated around the "date_time" (POSIXct) columns specified in the key arguments. By default this will be "date_time" for both 'x' and 'y' tables. Provided as a single value or vector of two.
#' For 'fuzzy' joins where the two keys are provided for each 'x' and 'y' table the join syntax is compiled like this:
#'  '1st x key' 'x inequality' '1st y key', '2nd y key' 'x inequality' '2nd x key'.
#' @param inq Inequality signs, vector of two (i.e., ">", ">=", "<", "<=") for fuzzy (non-equi) joins. Defaults to NULL.
#' @param y_phen_names Names of phenomena (columns) to retain post the join.
#' _Custom data.table arguments_
#' `data.table` has great documentation for these arguments.
#' @param nomatch In outer joins the rows for which no match was found can be retained by setting this argument to NA. If this is set to NULL 'no match' rows are not retained. By default an inner_join has this argument set to NULL. For other joins this is set to NA. Setting the value here will override the default.
#' @param mult Where multiple rows of the right table match the left, this argument controls how more than one match is handled. If specified as "all" (default), all matching rows are returned. Otherwise the "first" or "last" matching rows will be returned.
#' @param roll Can't get better than the `data.table` documentation here.
#' @param rollends Can't get better than the `data.table` documentation here.
#' @param allow.cartesian Can't get better than the `data.table` documentation here.
#' @param time_seq If data is continuous this can be set to TRUE which will trigger time-series sensitive joining of data. Time-series sensitive joining on ensures the date-time series is continuous, i.e., the record intervalis kept constant, and unique (no duplicate time stamps).

#' @param input_dt 
#' @param output_dt 
#' @param f_params
#' @param f_summary A summary table of function parameters. This
#'  summary table is stored in the station file object. If there
#'  is a summary table for the function, differences between the
#'  `f_params` and table are checked and the 'f_summary' table
#'  is updated---triggering a re-calculation of the whole time series instead
#'  of for a slice of the series.
#' @return A list containing the processed data sets 'dts_dt'.
#' @author Paul J. Gordijn
#' @details
#' 
#'
mhlanga <- function(
  x_tbl = NULL,
  y_tbl = NULL,
  join = "full_join",
  x_key = "date_time",
  y_key = "date_time",
  fuzzy = NULL,
  inq = NULL,
  y_phen_names = NULL,
  nomatch = NA,
  mult = "all",
  roll = FALSE,
  rollends = FALSE,
  allow.cartesian = FALSE,
  time_seq = TRUE,

  ...) {
  x_tbl <- data.table::setDT(x_tbl)
  y_tbl <- data.table::setDT(y_tbl)

  # time_seq prep ----
  if (length(time_seq) == 1) time_seq <- c(time_seq, time_seq)

  # fuzzy and key prep ----
  if (!is.null(fuzzy)) { # fuzzy key prep ----
    if (length(fuzzy) == 1) fuzzy <- c(fuzzy, fuzzy)
    if (length(x_key) == 1) x_key[2] <- x_key[1]
    # check keys
    if (x_key[1] %in% "date_time") {
      x_tbl$xd1 <- x_tbl[[x_key[1]]] - fuzzy[1]
      x_key[1] <- "xd1"
    }
    if (x_key[2] %in% "date_time") {
      x_tbl$xd2 <- x_tbl[[x_key[2]]] + fuzzy[1]
      x_key[2] <- "xd2"
    } else {
      x_key[2] <- x_key[1]
    }
    if (length(y_key) == 1) y_key[2] <- y_key[1]
    if (y_key[1] %in% "date_time") {
      y_tbl$yd1 <- y_tbl[[y_key[1]]] - fuzzy[2]
      y_key[1] <- "yd1"
    }
    if (y_key[2] %in% "date_time") {
      y_tbl$yd2 <- y_tbl[[y_key[2]]] + fuzzy[2]
      y_key[2] <- "yd2"
    } else {
      y_key[2] <- y_key[1]
    }
    # add in equality signs for non-equi/fuzzy joins
    if (is.null(inq)) inq <- c(">", "<=")
    if (join %in% "left_join") {
      on <- paste0(", on = .(", y_key[1], inq[1], x_key[1],
        ",", y_key[2], inq[length(inq)], x_key[2], ")", collapse = "")
    }
    if (join %in% "right_join") {
      on <- paste0(", on = .(", x_key[1], inq[1], y_key[1],
        ",", x_key[2], inq[length(inq)], y_key[2], ")", collapse = "")
    }
  } else { ## non-fuzzy key prep ----
    on <- paste0(", , on = .(", x_key[1], ",", y_key[1], ")", collapse = "")
  }

  # full_join ----
  if (join == "full_join") {
    data.table::setkey(x_tbl, x_key[1])
    data.table::setkey(y_tbl, y_key[1])
    
  }

  # prep data.table syntax ----
  dsyn <- on
  if (mult != "all") dsyn <- paste0(dsyn, " , mult=", mult, collapse = "")
  if (join != "inner" && !is.na(nomatch)) {
    dsyn <- paste0(dsyn, " , nomatch=", nomatch, collapse = "")
  }
  if (roll) dsyn <- paste0(dsyn, " , roll=", roll, collapse = "")
  if (rollends != FALSE) {
    dsyn <- paste0(dsyn, " , rollends=c\\(", as.character(rollends[1]), ", ",
      as.character(rollends[length(rollends)]), "\\)", collapse = "")
  }
  # left_join ----
  if (join == "left_join") {
    dsyn <- paste0("y_tbl[x_tbl", dsyn, "]", collapse = "")
  }

  # right join ----
  if (join == "right_join") {
    dsyn <- paste0("x_tbl[y_tbl", dsyn, "]", collapse = "")
  }

  # inner_join ----
  if (join == "inner_join") {
    dsyn <- paste0(dsyn, " , nomatch=", "NULL", collapse = "")
  }

  # parse join
  xy <- eval(parse(text = dsyn))

  return(list(dt = dt))
}