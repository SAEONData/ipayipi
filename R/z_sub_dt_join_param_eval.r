#' @title Evaluate and prepare tables for joins
#' @description Flexible and fast joining of `ipayipi` data.
#' @param full_eval
#' @param f_params
#' @param f_summary
#' @param ppsi
#' @param sf
#' @author Paul J. Gordijn
#' @export
join_param_eval <- function(
  join_type = NULL,
  x_table = NULL,
  y_table = NULL,
  y_phen_names = NULL,
  y_table_key = NULL,
  x_table_key = NULL,
  full_eval = FALSE,
  f_summary = NULL,
  ppsij = NULL,
  sf = sf,
  ...
  ) {
  # x <- list(
  #   join_type = c("inner_left", "inner_right", "outer_left",
  #     "outer_right", "overlaps"),
  #   y_table = "field_metadata",
  #   y_phen_names = NULL,
  #   y_table_key = NULL,
  #   x_table_keys = NULL,
  # )
  if (!full_eval) { # partial evaluation
    # check args
    m <- list("join_type", "y_table")
    m <- m[sapply(m, function(x) is.null(get(x)) || is.na(get(x)))]
    m <- lapply(m, function(x) {
      stop(paste0("NULL/NA args: ", paste0(m, collapse = ", ")), call. = FALSE)
    })
    j <- list("inner_left", "inner_right", "outer_left", "outer_right",
      "overlaps", "foverlaps")
    join_type <- list(join_type)
    m <- join_type[!join_type %in% j]
    m <- lapply(m, function(x) {
      stop(paste0("Mismatch in \'join_type\' arg: ",
        paste0(m, collapse = ", ")), call. = FALSE)
    })
    f_params <- list(
      join_type = unlist(join_type),
      x_table = x_table,
      y_table = y_table,
      y_phen_names = y_phen_names,
      y_table_key = y_table_key,
      x_table_key = x_table_key
    )
    f_params <- f_params[!sapply(f_params, function(x) is.null(x) || is.na(x))]
    class(f_params) <- c(class(f_params), "dt_join_params")
  } else {
    # full evaluation
    # check args
    m <- list("join_type", "x_table", "y_table", "f_summary")
    m <- m[sapply(m, function(x) is.null(x) || is.na(x))]
    m <- lapply(m, function(x) {
      stop(paste0("NULL/NA args: ", paste0(m, collapse = ", ")), call. = FALSE)
    })
    # read in phen_dt
    #  and check key phens
    #  generate new phen_dt

    dt_parse <- list(
      f_params = f_params, # full list of arguments to be passed
      phens_dt = phens_dt
    )
  }
  return(f_params)
}
