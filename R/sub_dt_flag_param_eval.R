#' @title Evaluate arguments for pipeline flags and checks filter.
#' @description Evaluation of the input-flag table that will be used by [dt_flag] for performing basic checks on '\bold{raw}' data.
#' @param flag_tbl Flag table .... Must be a data.table object.
#' @param q1 ???
#' @param robust Logical. Set to `TRUE` to use median to interpolate single missing values in finest res data. `FALSE` will use the mean to interpolate.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param xtra_v Logical. Whether or not to report xtra messages, progess, plus print data tables.
#' @param full_eval Logical that indicates the depth of evaluation. This argument is provided parsed internally. Full evaluation can only be completed within `ipayipi::dt_process()` sequence evaluation.
#' param f_params
#' @param ppsij Data processing `pipe_seq` table from which function parameters and data are extracted/evaluated. This is parsed to this function automatically by `ipayipi::dt_process()`.
#' @param sfc  List of file paths to the temporary station file directory. Generated using `ipayipi::sf_open_con()`.
#' @param station_file Name of the station being processed.
#' @details Standard flag tables ...
#'
#' @author Paul J. Gordijn
#' @export
flag_param_eval <- function(
  flag_tbl = NULL,
  q1 = TRUE,
  robust = TRUE,
  full_eval = FALSE,
  station_file = NULL,
  ppsij = NULL,
  f_params = NULL,
  sfc = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  ".SD" <- ":=" <- "%chin%" <- NULL
  "nas" <- "filter_check" <- "lower_thresh" <- "upper_thresh" <- "lts" <-
    "uts" <- NULL

  ## partial evaluation ----
  # default data.tableish arguments
  d_args <- list(q1 = TRUE) # , robust = TRUE
  p_args <- list(station_file = "NULL", f_params = NULL, ppsij = "NULL",
    sfc = "NULL"
  )
  # extract non-default args and save as expression
  args <- lapply(seq_along(d_args), function(i) {
    c_arg <- get(names(d_args)[i])
    if (is.null(c_arg)) c_arg <- "NULL"
    if (d_args[[i]] %in% c_arg) {
      r <- NULL
    } else {
      r <- get(names(d_args)[i])
    }
    r
  })
  names(args) <- names(d_args)
  args <- as.expression(args[!sapply(args, is.null)])

  # get the flag_tbl
  if (!is.expression(flag_tbl)) {
    cli::cli_abort(
      paste0("The {.var flag_tbl} input parameter must be an expression. ",
        "See ?expression ..."
      )
    )
  }
  flag_tbl_exp <- flag_tbl
  flag_tbl <- attempt::try_catch(eval(flag_tbl_exp))

  # check if the flag table is a data.table
  if (!data.table::is.data.table(flag_tbl)) {
    cli::cli_inform(c("i" = "Attempting to evaluate the flag_tbl below:"))
    print(flag_tbl)
    cli::cli_abort(
      "The `flag_tbl` expression must produce a 'data.table' object."
    )
  }

  sts_cols <- c("q_level", "id", "phen_type", "phen", "rule", "rule_sub",
    "filter_check", "w", "lag", "focal", "lts", "lower_thresh", "uts",
    "upper_thresh", "description", "action", "time_interval", "references",
    "notes"
  )

  new_cols <- names(flag_tbl)[!names(flag_tbl) %chin% sts_cols]
  if (length(new_cols) > 0) {
    cli::cli_inform(c(
      "i" = paste0("These columns, '{new_cols}', were in the input ",
        "{.var flag_tbl}, but are not recognised..."
      ),
      " " = "Standard column names for {.var flag_tbl} include:",
      " " = "{sts_cols}"
    ))
  }
  required_cols <- sts_cols[!sts_cols %chin% names(flag_tbl)]
  if (length(required_cols) > 0) {
    cli::cli_inform(c(
      "i" = paste0("These columns, '{required_cols}', are required, but are",
        " not in the input {.var flag_tbl} ..."
      ),
      " " = "Standard column names for {.var flag_tbl} include:",
      " " = "{sts_cols}"
    ))
    stop()
  }

  # flag_tbl logic ---
  # check for logical sense in flag table, i.e. are all appropriate params
  # provided for a row and/or rule
  ## mandatory cols
  man_cols <- c("phen", "rule", "rule_sub", "filter_check",
    "w", "time_interval"
  )
  fman <- flag_tbl[, nas := rowSums(is.na(.SD)), .SDcols = man_cols][nas > 0]
  if (nrow(fman) > 0) {
    cli::cli_inform(c(
      "i" = paste0("The following columns of the {.var flag_tbl} must not have",
        " any 'NA' values:"
      ),
      " " = "{man_cols}.",
      " " = "These rows have 'NA' values:"
    ))
    print(fman[, -c("nas"), with = FALSE])
    err <- TRUE
  } else {
    err <- FALSE
  }

  ## check threshold rows ---
  sgns <- c("==", ">", "<", ">=", "<=")
  th_tbl <- flag_tbl[filter_check %chin% "threshold"]
  # must have inequalities and values
  if (nrow(th_tbl[is.na(lts) & is.na(uts)]) > 0) {
    cli::cli_inform(c("i" = paste0(
      "Missing inequalities in threshold check as below: "
    )))
    print(th_tbl[is.na(lts) & is.na(uts)])
    err <- TRUE
  }
  if (nrow(th_tbl[is.na(lower_thresh) & is.na(upper_thresh)])) {
    cli::cli_inform(c("i" = "Missing values in threshold check as below: "))
    print(th_tbl[is.na(lts) & is.na(uts)])
    err <- TRUE
  }
  if (nrow(
    flag_tbl[(!is.na(lower_thresh) & is.na(lts)) |
        (!is.na(upper_thresh) & is.na(uts))
    ]
  ) > 0) {
    cli::cli_inform(c("i" = "Missing threshold inequalities as below: "))
    print(flag_tbl[(!is.na(lower_thresh) & is.na(lts)) |
          (!is.na(upper_thresh) & is.na(uts))
      ]
    )
    err <- TRUE
  }

  if (err) stop()
  flag_tbl_exp <- gsub("^expression\\(*", "~\\(", deparse1(flag_tbl_exp))
  args$flag_tbl <- noquote(flag_tbl_exp)
  args <- list(args)
  class(args) <- c(class(args), "dt_flag_params")
  if (!full_eval) return(args)

  # full evaluation ----
  # check and populate time intervals

  # retrieve station file info
  # read phens_dt
  phens_dt <- sf_dta_read(sfc = sfc, tv = "phens_dt")[["phens_dt"]]

  # select possible flag checks
  # save flags_dt
  # add flag phens to phens dt, plus phens in dt_q1_5_mins etc
  # save options to row in f_params tables

  return(list(phens_dt = phens_dt))
}
