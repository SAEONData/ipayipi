#' @title Evaluate and prepare tables for joins
#' @description Flexible and fast joining of `ipayipi` data.
#' @param x_tbl The name of the 'x' or left table in the merge/join. This
#'  defaults to the working data table, and therefore, this argument need not be supplied.
#' @param y_tbl Argument must be supplied. Name of the 'y' or right table.
#' @param x_key Column name. If not provided, defaults to "date_time".
#' @param y_key Column name. Similar to `x_key` if not provided.
#' @param time_seq If `TRUE` then a continuous sequence of date-time values are ensured for both _x_ and _y_ tables. If a vector of two logial values is provided then this is applied to _x_ and _y_ tables seperately, in that order, and the final joined table.
#' @param fuzzy String or logical. If string, this argument is evaluated to determine the fuzzy interval used to ...
#' param full_eval
#' param f_params
#' param f_summary
#' param ppsi
#' param sf
#' @details
#' __Join types__
#' - `full_join`: Merges matching `x_tbl` and `y_tbl` rows, and retains all unmatching records.
#' - `left_join`: Matching `x_tbl` and `y_tbl` rows, plus all unmatched `x_tbl` rows.
#' - `right_join`: Matching `x_tbl` and `y_tbl` rows, plus all unmatched `y_tbl` rows.
#' - `inner_join`: Only retains matching `x_tbl` and `y_tbl` rows.
#' @author Paul J. Gordijn
#' @export
join_param_eval <- function(
  join = "full_join",
  x_tbl = NULL,
  y_tbl = NULL,
  y_phen_names = NULL,
  x_key = "date_time",
  y_key = "date_time",
  inq = NULL,
  nomatch = NA,
  mult = "all",
  roll = FALSE,
  rollends = FALSE,
  allow.cartesian = FALSE,
  time_seq = TRUE,
  fuzzy = NULL,
  full_eval = FALSE,
  eval_seq = NULL,
  f_summary = NULL,
  f_params = NULL,
  ppsij = NULL,
  sf = NULL,
  ...
) {

  # default data.tableish arguments
  d_args <- list(join = "full_join", x_tbl = "NULL", y_tbl = "NULL",
    x_key = "date_time", y_key = "date_time", nomatch = NA, mult = "all",
    roll = FALSE, rollends = FALSE, allow.cartesian = FALSE
  )
  p_args <- list(y_phen_names = "NULL", time_seq = TRUE, fuzzy = "NULL",
    full_eval = FALSE, eval_seq = "NULL", f_summary = "NULL", ppsij = "NULL",
    sf = "NULL", inq = "NULL"
  )
  pda <- append(p_args, d_args)

  if (!full_eval) {
    ## partial evaluation -----------------------------------------------------
    # check join types
    j <- list("left_join", "right_join", "inner_join", "full_join")
    join <- list(join)
    m <- join[!join %in% j]
    m <- lapply(m, function(x) {
      stop(paste0("Mismatch in \'join\' arg: ", paste0(m, collapse = ", ")),
        call. = FALSE
      )
    })
    join <- unlist(join)
    # time seq arg
    if (!any(time_seq == FALSE)) time_seq <- NULL

    # extract non-default args for f_params
    if (join == "full_join") join <- "fj"
    f_params <- lapply(seq_along(pda), function(i) {
      c_arg <- get(names(pda)[i])
      if (is.null(c_arg)) c_arg <- "NULL"
      if (pda[[i]] %in% c_arg) {
        r <- NULL
      } else {
        r <- get(names(pda)[i])
      }
      return(r)
    })
    names(f_params) <- names(pda)
    if (f_params$join == "fj") f_params$join <- "full_join"
    dt_parse <- f_params[!sapply(f_params, function(x) is.null(x))]
    class(dt_parse) <- c(class(dt_parse), "dt_join_params")
  } else {
    ## full function evaluation -----------------------------------------------
    if (!is.null(ppsij)) {
      # extract parts of ppsij and assign values in env
      ppsi_names <- names(ppsij)[!names(ppsij) %in% c("n", "f", "f_params")]
      for (k in seq_along(ppsi_names)) {
        assign(ppsi_names[k], ppsij[[ppsi_names[k]]][1])
      }
    }
    # extract parts of f_params and assign values in environment
    join_param_names <- names(f_params)
    for (k in seq_along(join_param_names)) {
      assign(join_param_names[k], f_params[[join_param_names[k]]])
    }
    # check args
    m <- list("join", "x_tbl", "y_tbl", "f_summary")
    m <- m[sapply(m, function(x) is.null(x) || is.na(x))]
    m <- lapply(m, function(x) {
      stop(paste0("NULL/NA args: ", paste0(m, collapse = ", ")), call. = FALSE)
    })
    # read in phen_dt
    #  and check key phens
    #  generate new phen_dt
    phens_dt <- eval_seq$phens_dt

    dt_parse <- list(
      f_params = list(join_params = f_params),
      phens_dt = phens_dt
    )
  }
  return(dt_parse)
}
