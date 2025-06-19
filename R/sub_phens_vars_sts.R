#' @title Transforms variable types
#' @description Uses the variable types specified in the phenomena table to
#'  standardise the variable type in ipayipi data tables.
#' @param phen_tbl iPayipi standardised phenomena table.
#' @param dta_in Input table with standardised phenomena names.
#' @keywords Internal
#' @return Data table with transformed variables.
#' @author Paul J. Gordijn
#' @export
#' @noRd
phen_vars_sts <- function(
  phen_tbl = NULL,
  dta_in = NULL,
  ...
) {
  "%ilike%" <- ":=" <- "var_type" <- "phen_name" <- ".SD" <- NULL
  # merge data sets into a station for given time periods
  phens <- names(dta_in)[!names(dta_in) %in% c("id", "date_time", "d1", "d2")]
  phen_tbl <- phen_tbl[phen_name %in% phens][,
    c("phen_name", "units", "offset", "var_type"), with = FALSE
  ]
  phen_tbl <- unique(phen_tbl)
  if (length(phen_tbl[var_type %ilike% "num"]$phen_name) > 0) {
    dta_in[, (phen_tbl[var_type %ilike% "num"]$phen_name) :=
        lapply(.SD, as.numeric),
      .SDcols = phen_tbl[var_type %ilike% "num"]$phen_name
    ]
  }
  if (length(phen_tbl[var_type %ilike% "int"]$phen_name) > 0) {
    dta_in[, (phen_tbl[var_type %ilike% "int"]$phen_name) :=
        lapply(.SD, as.integer),
      .SDcols = phen_tbl[var_type %ilike% "int"]$phen_name
    ]
  }
  if (length(phen_tbl[var_type %ilike%
          "str|string|chr|character|fac|factor|fact"
      ]$phen_name
    ) > 0
  ) {
    dta_in[,
      (phen_tbl[
        var_type %ilike% "str|string|chr|character|fac|factor|fact"
      ]$phen_name
      ) := lapply(.SD, as.character), .SDcols = phen_tbl[
        var_type %ilike% "str|string|chr|character|fac|factor|fact"
      ]$phen_name
    ]
  }
  if (length(phen_tbl[var_type %ilike% "date|time|posix"]$phen_name) > 0) {
    dt_tz <- attr(dta_in$date_time[1], "tz")
    dta_in[, (phen_tbl[var_type %ilike% "date|time|posix"]$phen_name) :=
        lapply(.SD, function(x) as.POSIXct(x, tz = dt_tz)),
      .SDcols = phen_tbl[var_type %ilike% "date|time|posix"]$phen_name
    ]
  }
  if (nrow(phen_tbl[var_type %ilike% "fac|factor|fact"]) > 0) {
    dta_in[, (phen_tbl[var_type %ilike% "fac|factor|fact"]$phen_name) :=
        lapply(.SD, as.factor), .SDcols = phen_tbl[
        var_type %ilike% "fac|factor|fact"
      ]$phen_name
    ]
  }
  return(dta_in)
}