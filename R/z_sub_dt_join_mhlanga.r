#' @title Join data sets
#' @description Serves as a wrapper for __data.table__ joins, plus 'time-sensitive' __ipayipi__ full joins.
#' @param x_tbl The left, that is, 'x' table.
#' @param y_tbl The right, that is, 'y' table.
#' @param join One of the following join types:
#'  1. _full_join_. Joins the 'x' and 'y' tables using keyed column. Retains all the matching and unmatching rows of 'x' and 'y' tables.
#'  1. _left_join_. Adds the matching rows of 'x' to 'y' by keyed columns.
#'  1. _right_join_. Adds the matching rows of 'y' to 'x' by keyed columns.
#'  1. _inner_join_. Retains only rows of 'x' and 'y' that match by keyed columns.
#' __Keys__
#' Keys are the columns of the 'x' (left) and 'y' (right) tables used for finding matches (identifiers) for a join. Because `ipayipi` is based on date-time data the default keys are assumed to be "date_time". Unlike conventional joins where rows are only matched if there is an exact match of keyed values, 'fuzzy' or 'non-equi', joins are flexible. When using a 'fuzzy' join an inequality sign must be provided in the key list. _See below__.
#' @param x_key Name(s) of the column(s) to key by the 'x' table. Vector of length one or two for conventional or fuzzy or non-equi joins, respectively. Defaults to `date_time`.
#' @param y_key Same as above but for the 'y' or right table. Defaults to `date_time`.
#' @param fuzzy A numeric (seconds) vector of length one or two.
#' Fuzzy time-series joins require four keys in total: two keys for the 'x', and two for the 'y' table'. The nature of how these keys are join are controlled by arguments such as `inq` below.
#' Both keys can be provided using the `x_key` and `y_key` arguments. But, if only one key is specified in the `x_key` and `y_key` arguments, the second key for each table can be generated based on the values provided here. For each 'x' and 'y' table, given the `fuzzy` argument is provided, the first and second keys for a table are calculated as:
#'
#' \eqn{x_key[1] = x_key[1] - fuzzy[1]}
#' \eqn{x_key[2] = x_key[2] + fuzzy[1]}
#' \eqn{y_key[1] = y_key[1] - fuzzy[2]}
#' \eqn{y_key[2] = y_key[2] + fuzzy[2]}
#'
#' Note that if only one key is provided in the `x_key` and `y_key` arguments then the first value will be recycled for the second.
#'
#'  Internally, the default names for these are xd1 and xd2, and yd1 and yd2, for 'x' and 'y' tables, respectively.
#'
#' 
#' For 'fuzzy' joins where two keys are provided for each 'x' and 'y' table the join syntax is compiled like this:
#'  '1st x key' 'x inequality' '1st y key', '2nd y key' 'x inequality' '2nd x key'.
#' @param inq Inequality signs, vector of two (i.e., ">", ">=", "<", "<=") for fuzzy (non-equi) joins. Defaults to NULL.
#' @param y_phen_names Names of phenomena (columns) to retain post the join.
#'
#'
#' _Custom data.table arguments_
#' `data.table` has great documentation for these arguments.
#' @param nomatch In outer joins the rows for which no match was found can be retained by setting this argument to NA. If this is set to NULL 'no match' rows are not retained. By default an inner_join has this argument set to NULL. For other joins this is set to NA. Setting the value here will override the default.
#' @param mult Where multiple rows of the right table match the left, this argument controls how more than one match is handled. If specified as "all" (default), all matching rows are returned. Otherwise the "first" or "last" matching rows will be returned.
#' @param roll Can't get better than the `data.table` documentation here.
#' @param rollends Can't get better than the `data.table` documentation here.
#' @param allow.cartesian Can't get better than the `data.table` documentation here.
#' @param time_seq If data is continuous this can be set to TRUE to run time-series sensitive joining of data. Time-series sensitive joining on ensures the date-time series is continuous, i.e., the record interval is kept constant, and unique (no duplicate time stamps).
#' @param ri Record interval of the 'y' or 'x' for left and right joins, respectively, or for both in a time-sensitive full join (_see_ `time_seq`). Single character string of a standardised time-series/record interval. Record intervals can be standardised using `ipayipi::sts_interval_name()`.
#' @param phen_dt If a phenomena table is provided frokm the 'ipayipi' data processing pipeline, columns with listed standardised data types will be standardised accordingly using `ipayipi::phen_vars_sts()`.
#' @param over_right Controls whether 'x' or 'y' table data are overwritten during a full, time-sensitive join (when __seq_time == TRUE__). When TRUE the 'y' table is overwritten by overlapping (by time-series index) data in the 'x' table.
#' @return A table with joined data.
#' @author Paul J. Gordijn
#' @details
#' This function, 'mhlanga' (literally: 'reeds', 'reed bed'; figuratively: come together, in isiZulu) pulls various 'data.table' join syntax commands together, plus added 'ipayipi' time-sensitive joins, into a single function. Time-senstive joins compare 'x' or 'y' data, column by column, to avoid replacing records with `NA` values when overwriting overlapping time-series data sets.
#'
#' 'ipayipi' expects time-series data to have a 'time' column named 'date_time'. This is the default key for joins in 'ipayipi'. Other join columns can be set using the `x_key` and `y_key` arguments.
#' @export
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
  mult = "first",
  roll = FALSE,
  rollends = FALSE,
  allow.cartesian = FALSE,
  time_seq = TRUE,
  ri = NULL,
  phen_dt = NULL,
  over_right = FALSE,
  station = NULL,
  ...
) {
  "%ilike%" <- NULL

  x_tbl <- data.table::setDT(x_tbl)
  y_tbl <- data.table::setDT(y_tbl)

  # time_seq prep ----
  if (is.null(time_seq)) time_seq <- FALSE
  if (length(time_seq) == 1) time_seq <- c(time_seq, time_seq)

  # fuzzy and key prep ----
  if (!is.null(fuzzy) ||
      any(sapply(list(x_key, y_key), function(x) length(x) == 2))
  ) {
    if (is.null(fuzzy)) fuzzy <- 0
    if (length(fuzzy) == 1) fuzzy <- c(fuzzy, fuzzy)
    if (is.na(x_key[2])) x_key[2] <- x_key[1]
    if (is.na(y_key[2])) y_key[2] <- y_key[1]
  }

  # check keys
  if (!is.null(fuzzy)) {
    x_tbl$xd1 <- x_tbl[[x_key[1]]] - fuzzy[1]
    x_key[1] <- "xd1"
    x_tbl$xd2 <- x_tbl[[x_key[2]]] + fuzzy[1]
    x_key[2] <- "xd2"
    y_tbl$yd1 <- y_tbl[[y_key[1]]] - fuzzy[2]
    y_key[1] <- "yd1"
    y_tbl$yd2 <- y_tbl[[y_key[2]]] + fuzzy[2]
    y_key[2] <- "yd2"
  }

  if (all(sapply(list(x_key, y_key), function(x) length(x) == 2))) {
    # add in equality signs for non-equi/fuzzy joins
    if (is.null(inq)) inq <- c("<=", ">=")
    if (join %in% "left_join") {
      on <- paste0(", on = .(", y_key[1], inq[1], x_key[1],
        ",", y_key[2], inq[length(inq)], x_key[2], ")", collapse = ""
      )
    }
    if (join %in% "right_join") {
      on <- paste0(", on = .(", x_key[1], inq[1], y_key[1],
        ",", x_key[2], inq[length(inq)], y_key[2], ")", collapse = ""
      )
    }
  } else { ## non-fuzzy key prep ----
    on <- paste0(", , on = .(", x_key[1], ",", y_key[1], ")", collapse = "")
  }

  # prep data.table syntax ----
  if (mult != "all") on <- paste0(on, ", mult=\'", mult, "\'", collapse = "")
  if (join != "inner" && !is.na(nomatch)) {
    on <- paste0(on, " , nomatch=", nomatch, collapse = "")
  }
  if (roll) on <- paste0(on, " , roll=", roll, collapse = "")
  if (rollends != FALSE) {
    on <- paste0(on, " , rollends=c\\(", as.character(rollends[1]), ", ",
      as.character(rollends[length(rollends)]), "\\)", collapse = ""
    )
  }

  # add preffix to duplicate column names in y_tbl
  if (join %in% c("left_join", "right_join")) {
    names(y_tbl)[names(y_tbl) %in% names(x_tbl)] <-
      paste0("ydt_", names(y_tbl)[names(y_tbl) %in% names(x_tbl)])
  }

  # left_join ----
  if (join == "left_join") {
    dsyn <- paste0("y_tbl[x_tbl", on, "]", collapse = "")
    xy <- eval(parse(text = dsyn))
    xy <- xy[, names(xy)[!names(xy) %in% c("xd1", "xd2")], with = FALSE]
    xy <- xy[, c(names(x_tbl)[names(x_tbl) %in% names(xy)], names(y_tbl)),
      with = FALSE
    ]
  }

  # right join ----
  if (join == "right_join") {
    dsyn <- paste0("x_tbl[y_tbl", on, "]", collapse = "")
    xy <- eval(parse(text = dsyn))
  }

  # inner_join ----
  if (join == "inner_join") {
    dsyn <- paste0(dsyn, " , nomatch=", "NULL", collapse = "")
    xy <- eval(parse(text = dsyn))
  }

  # full_join ----
  if (join == "full_join" && !any(time_seq[1], time_seq[2])) {
    x_key <- names(x_tbl)[names(x_tbl) %ilike% x_key[1]][1]
    y_key <- names(y_tbl)[names(y_tbl) %ilike% y_key[1]][1]
    xy <- merge(x = x_tbl, y = y_tbl, all = TRUE, by.x = x_key, by.y = y_key)

    # clean up duplicate names and cover over x table NA values
    unames <- gsub(".y$|.x$", "", names(xy)[names(xy) %ilike% ".y$|.x$"])
    unames <- unique(unames)
    u <- lapply(unames, function(x) {
      u <- data.table::data.table(
        u = data.table::fifelse(!is.na(xy[[paste0(x, ".x")]]),
          xy[[paste0(x, ".x")]], xy[[paste0(x, ".y")]]
        )
      )
      data.table::setnames(u, "u", x)
      return(u)
    })
    u <- do.call("cbind", u)
    xy <- xy[, c(names(xy)[!names(xy) %ilike% ".x|.y"]), with = FALSE]
    xy <- cbind(xy, u)
  }

  # full_join --- time sensitive
  if (join == "full_join" && all(time_seq[1], time_seq[2])) {
    xy <- ipayipi::append_phen_data(station_file = x_tbl, ndt = y_tbl,
      phen_id = FALSE, phen_dt = phen_dt, overwrite_sf = over_right, ri = ri,
      ...
    )
  }
  # remove fuzzy columns
  xy <- xy[, names(xy)[!names(xy) %in% c("xd1", "xd2", "yd1", "yd2")],
    with = FALSE
  ]
  xy <- xy[, names(xy)[!names(xy) %ilike% "ydt_"], with = FALSE]
  return(xy)
}
