#' @title Perform data processing calculations
#' @description Calculations are parsed to data.table.
#' @param sfc List of file paths to the temporary station file directory. Generated using `ipayipi::sf_open_con()`.
#' @param pipe_house A pipeline's directory structure. Generated using `ipayipi::ipip_house()`.
#' @param station_file Name of the station being processed.
#' @param f_params Function parameters evaluated by `ipayipi::calc_param_eval()`. These are internally parsed to `dt_calc()` from `dt_process()`.
#' @param ppsij Data processing `pipe_seq` table from which function parameters and data are extracted/evaluated. This is parsed to this function automatically by `ipayipi::dt_process()`.
#' @param verbose Logical. Whether or not to report messages and progress.
#' @param xtra_v Logical. Whether or not to report xtra messages, progess, plus print data tables.
#' @param chunk_v Whether or not to indicate the start of data chunking.
#' @param dt_format The function attempts to work out the date-time format from a vector of format types supplied to this argument. The testing is done via `lubridate::parse_date_time()`. `lubridate::parse_date_time()` prioritizes the tesing of date-time formats in the order vector of
#'  formats supplied. The default vector of date-time formats supplied should work well for most logger outputs.
#' @param dt_tz Recognized time-zone (character string) of the data locale. The default for the package is South African, i.e., "Africa/Johannesburg" which is equivalent to "SAST".
#' @details Note that gap metadata will be transferred to the station file's 'gap' table.
#' @return A list containing the processed data sets 'dts_dt'.
#' @author Paul J. Gordijn
#' @details *Testing import of gap info and saving to sfc gaps
#'
#' @export
dt_calc <- function(
  sfc = NULL,
  pipe_house = NULL,
  station_file = NULL,
  f_params = NULL,
  ppsij = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  dt_format = c(
    "Ymd HMOS", "Ymd HMS",
    "Ymd IMOSp", "Ymd IMSp",
    "ymd HMOS", "ymd HMS",
    "ymd IMOSp", "ymd IMSp",
    "mdY HMOS", "mdY HMS",
    "mdy HMOS", "mdy HMS",
    "mdy IMOSp", "mdy IMSp",
    "dmY HMOS", "dmY HMS",
    "dmy HMOS", "dmy HMS",
    "dmy IMOSp", "dmy IMSp"
  ),
  dt_tz = "Africa/Johannesburg",
  ...
) {
  "%ilike%" <- "%chin%" <- NULL
  "table_name" <- "phen" <- "gap_type" <- "gap_start" <- "gap_end" <- NULL

  # assignments ----
  sf_ext <- ".ipip"

  # read in the available data ----
  sfcn <- names(sfc)
  dta_in <- NULL
  hsf_dta <- NULL
  if ("dt_working" %chin% sfcn) {
    dta_in <- sf_dta_read(sfc = sfc, tv = "dt_working")
    ng <- sf_dta_read(sfc = sfc, tv = "gaps")[["gaps"]][
      table_name %chin% ppsij$output_dt[1]
    ]
  }

  if (is.null(dta_in) && any(sfcn %ilike% "_hsf_table_")) {
    pstep <- paste0("^", ppsij$dt_n[1], "_.+_hsf_table_")
    hsf_dta <- sfcn[sfcn %ilike% pstep]
    hsf_dta <- hsf_dta[order(as.integer(
      sapply(strsplit(hsf_dta, "_"), function(x) x[2])
    ))]
    hsf_dta <- hsf_dta[length(hsf_dta)]
    dta_in <- sf_dta_read(sfc = sfc, tv = hsf_dta)
    ng <- dta_in[[1]]$gaps
    ng$table_name <- ppsij$output_dt[1]
  }
  # create station object for calc parsing ----
  ssub <- paste(dirname(station_file), sf_ext, "/", sep = "|")
  ssub <- gsub("^['.']['|']", "", ssub)
  station <- gsub(ssub, "", station_file)
  if (xtra_v) cli::cli_inform(c(" " = "Calc station: {station}."))

  # eindx filter
  dta_in <- dt_dta_filter(dta_link = dta_in, ppsij = ppsij)
  # open data ----
  dt <- dt_dta_open(dta_link = dta_in)
  if (xtra_v) {
    cli::cli_inform(c(" " = "Pre-calc data:"))
    cli::cli_inform(c(" " = "data class = {class(dt)}"))
    print(head(dt))
  }

  # organise f_params ----
  # extract ipip arguments from the seperate changes
  f_ipips <- lapply(f_params, function(x) {
    xpos <- regexpr(", ipip\\(", x)[1]
    xr <- substr(x, xpos, nchar(x) - 1)
    if (xpos > 0) {
      x <- eval(parse(text = gsub(", ipip\\(", "list\\(", xr)))
      if (!"fork_table" %chin% names(x)) x <- NULL
      x <- x[sapply(names(x), function(x) x %in% "fork_table")]
      x <- unlist(x, recursive = TRUE)
    } else {
      x <- NULL
    }
  })
  # prep data chain syntax ----
  # remove special ipip aruments for the evaluation
  f_params <- lapply(f_params, function(x) sub(", ipip\\(.*]$", "]", x))
  # chain together fork tables
  fk_params <- lapply(seq_along(f_params), function(i) {
    ic <- f_params
    if (!is.null(f_ipips[[i]])) {
      ic[[i]] <- paste0("[!", substr(ic[[i]], 2, nchar(ic[[i]])), collapse = "")
      r <- paste0(ic[1:i], collapse = "")
    } else {
      r <- NULL
    }
    return(r)
  })
  names(fk_params) <- f_ipips
  fk_params <- fk_params[sapply(fk_params, function(x) !is.null(x))]
  # eval syntax ----
  dt_fk_params <- lapply(fk_params, function(x) {
    attempt::try_catch(expr = eval(parse(text = x)), .e = ~NULL)
  })
  eval_f <- function(fx) {
    attempt::try_catch(eval(parse(text = paste0(fx, collapse = ""))))
  }
  if (xtra_v) {
    cli::cli_inform(c(" " = "Calc data.table syntax:",
      paste0(paste0("> ", f_params), sep = "\n")
    ))
  }
  dte <- list(attempt::attempt(eval_f(f_params), silent = TRUE))
  if (attempt::is_try_error(dte[[1]])) {
    err <- dte[[1]][1]
    known_err <- "only 0's may be mixed with negative subscripts"
    if (!err %ilike% known_err && xtra_v && nrow(dt) > 0) print(dte[[1]][1])
    if (err %ilike% " not found|length of" && xtra_v) {
      cli::cli_inform(c("!" = paste0(
        "Failure to calculate in data.table may be due ",
        "to an attempt to perform an operation on a variable OR phen",
        " introduced in single chain."
      ), ">" = paste0("To solve this begin a new \'calc\' step (chain) to work",
        " on the new phen."
      ), ">" =
        "OR ideally call the phen name in {.var data.table} syntax like:",
      " " = ".SD[[\'phen_name\']]",
      ">" = "OR ideally use data.table operators in one chain:",
      " " = "{.var .SD} and {.var .SDcols = c(phen_name)}"))
    }
    dte <- list(NULL)
  }
  dt_working <- dte
  names(dt_working) <- "dt_working"
  if (xtra_v) {
    cli::cli_inform(c(" " = "Post-calc data :"))
    print(head(dt_working[["dt_working"]]))
    cli::cli_inform(
      c(" " = "data class = {class(dt_working[[\"dt_working\"]])}")
    )
  }

  # gap info ----
  if (!"gaps" %chin% names(sfc)) {
    cli::cli_abort(c(
      "Produce \'gap\' table prior to processing!"
    ))
  }
  g <- sf_dta_read(sfc = sfc, tv = "gaps")[["gaps"]]
  if (nrow(ng) > 0) g <- g[!table_name %chin% ppsij$output_dt[1]]
  g <- rbind(g, ng, fill = TRUE)
  g <- g[order(table_name, phen, gap_type, gap_start, gap_end)]
  g <- unique(g)
  # delete old gaps file
  sf_dta_rm(sfc = sfc, rm = "gaps")
  # write updated gap files
  sl <- sf_dta_wr(dta_room = file.path(dirname(sfc[1]), "gaps"),
    dta = g, tn = "gaps"
  )
  sf_log_update(sfc = sfc, log_update = sl)

  # update pipe_seq ----
  # dttm foor ppsij
  if (all(
    "date_time" %chin% names(dt_working[["dt_working"]]),
    nrow(dt_working[["dt_working"]]) > 0
  )) {
    ppsij$start_dttm <- min(dt_working[["dt_working"]]$date_time, na.rm = TRUE)
    ppsij$end_dttm <- max(dt_working[["dt_working"]]$date_time, na.rm = TRUE)
  }

  # save working data ----
  # work through data to be saved
  dta_sets <- c(dt_fk_params, dt_working)
  sf_dta_rm(sfc = sfc, rm = "dt_working")
  log_update <- lapply(seq_along(dta_sets), function(dsi) {
    d <- dta_sets[[dsi]]
    if (!data.table::is.data.table(d)) d <- unlist(d, recursive = TRUE)
    n <- names(dta_sets)[dsi]
    sl <- sf_dta_wr(dta_room = file.path(dirname((sfc[1])), n[1]),
      dta = d, overwrite = TRUE, tn = n[1], ri = ppsij[1]$time_interval,
      chunk_v = chunk_v
    )
    return(sl)
  })
  sf_log_update(sfc = sfc, log_update = log_update)
  # remove harvest data ----
  if (!is.null(hsf_dta)) {
    ppsid_hsf <- unique(gsub("_hsf_table_?.+", "", hsf_dta))
    hsf_rm <- sfc[
      names(sfc)[names(sfc) %ilike% paste0(ppsid_hsf, "_hsf_table_*.")]
    ]
    sf_dta_rm(sfc = sfc, rm = hsf_rm)
  }
  return(list(ppsij = ppsij))
}