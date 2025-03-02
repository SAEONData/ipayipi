#' @title Standardises data phenomena in 'ipayipi' format.
#' @description Standardise raw data inputs formats and more for the 'ipayipi' pipeline though an interactive process.
#' @inheritParams dta_list
#' @inheritParams logger_data_import_batch
#' @inheritParams imbibe_raw_batch
#' @param remove_dups Logical. If true then a prompt is used to facilitate the removal of duplicate phenomena. Files with duplicate phenomena names will not be processed into a pipeline until the issue of duplication issue is resolved.
#' @param external_phentab File path to phenomena database. By default the function will search in the 'pipe_house' 'wait_room' for a phenomena database ('phentab'). If `external_phentab` is provided this option will override the 'pipe_house' default enabling access to another pipelines phenomena database.
#' @keywords time series data; automatic weather station; batch process; file standardisation; standardise variables; transform variables
#' @author Paul J. Gordijn
#' @export
#' @details
#' ## General
#' The raw data formats, units, and offsets, in an 'ipayipi' station file are standardised using this function. The function keeps a record of phenomena (variable) metadata and uses this to implement transformations to standardise the data types. If 'new' phenomena are detected in the data then a csv file is generated and the user must fill in the 'blank' (NA) values. The most recently modified 'phentab' (phenomena table) csv will be imbibed by this function and used to update the pipelines 'phentab' database.
#'
#' ## Available standards
#' Available standards that ship with ipayipi can be assessed by calling [ipayipi::sts_phens]. If the phenomena names in the raw data align perfectly with these standards then `external_phentab` can be set to `ipayipi::sts_phens`.
#'
#' ## Standard details
#'   The following phenomena (variable) metadata fields are allowed. Required fields are denoted with an asterix.
#'   1. *phen_name_full -- The full name of a phenomenon. This should be described as the phenomenon being recorded : then the measure (_see measure_) used to estimate the phenomenon.
#'       For example, "Atmospheric pressure: sample".
#'   1. *phen_type -- The specific phenomenon being measured, e.g., "Atmospheric pressure".
#'   1. *phen_name -- The shortened name of the phenomenon provided in lower case letters. These values will be used as the column header in the data, e.g., "atm_pressure_smp". __Note no spaces. Only underscores__.
#'   1. *units -- the units used to measure the phenomena. Within a pipeline the way the units are written must be exactly standarised.
#'   1. *measure -- the type of measurement used to represent a single time series event of a phenomenon, e.g., a sample or 'smp' for short. Other measure types include; min, max, sd (standard deviation), tot (total), and avg (mean or average). A sample is an instantaneous measurement.
#'   1. offset -- __not yet implemented__.
#'   1. f_convert -- a numeric multiplier for the phenomenon to be used for unit conversion or similar. If this is left as `NA` no factor conversion is performed on the 'raw_data'.
#'   1. sensor_id -- the serial number of the sensor used to measure the phenomenon.
#'   1. notes -- any pertinent notes on a specific phenomenon.
#'
#'
#'  ## Editing the phenomena table
#'  When unrecognised synonyms are introduced (e.g., after a change in logger program setup, or 'new' variable from an additional station sensor) this function creates a csv table in the `wait_room` that should be edited to maintain a phenomena synonym database. Unstandardised phenomena metadata (columns) are preffixed by 'uz_'._ These unstandardised columns must not be edited by the user: standardised phenomena information can be added by replacing corresponding `NA` values with information. The required fields, marked with an asterix (above), must be described.
#'
#'  ## Duplicate phenomena names:
#'  Duplicate phenomena in a single data series table is not tolerated. If a duplicate phenomena name is detected a prompt will work with the user to delete the duplicate. Most logger programmes avoid generating duplicates, but the situation can arise where perhaps a table is recording the same phenomenon multiple times.
#'
#'  ##_Other
#'  A number of other phenomena standardisation proceedures are included here:
#'  - '_Interference events_': Some logger files, e.g., hobo data logger files include useful columns that describe what within the ipayipi pipeline are termed 'logger interference events'. Logger interference events include events such as manual logger data downloads where the logger, or related data recording mechanism is 'interfered' with, and consequently, data near the time of interference may be altered undesirably. In hobo files interference column data are generally called 'logged' events, but spelling and capitalisation of this may change depending on the data. As part of the phenomena standardisation process, all interference events are marked in the data as 'logged'. Any column/phenomena in the data with the key 'interfere' will be marked as an 'interference' column. Therefore, when selecting phenomena names, take care to include the key 'interfere' so that the column is treated as recording 'interference' events.
#'
#'  ## Special characters/phrases
#'  - '_sn' appended to the 'phen_name' denotes a 'sensor' number. Columns with this suffix are not aggregated during processing.
#'
#'  ## File extensions
#'  - The input file extension is ".iph".
#'  - The output file extension is ".ipi".
#' @md
phenomena_sts <- function(
  pipe_house = NULL,
  remove_dups = FALSE,
  external_phentab = NULL,
  dt_format = c(
    "Ymd HMOS", "Ymd HMS",
    "Ymd IMOSp", "Ymd IMSp",
    "ymd HMOS", "ymd HMS",
    "ymd IMOSp", "ymd IMSp",
    "mdY HMOS", "mdY HMS",
    "mdy HMOS",  "mdy HMS",
    "mdy IMOSp",  "mdy IMSp",
    "dmY HMOS", "dmY HMS",
    "dmy HMOS", "dmy HMS",
    "dmy IMOSp", "dmy IMSp"
  ),
  dt_tz = "Africa/Johannesburg",
  prompt = FALSE,
  recurr = TRUE,
  wanted = NULL,
  unwanted = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  "uz_phen_name" <- "uz_units" <- "uz_measure" <- "phen_name_full" <-
    "var_type" <- "f_convert" <- "%ilike%" <- ".SD" <- "phen_name" <-
    ":=" <- NULL

  # Assign file extensions
  file_ext_in <- ".iph"
  file_ext_out <- ".ipi"

  # get list of data to be imported
  unwanted <- paste0("['.']ipr|['.']ipi|['.']xls|['.']rps|['.']rns",
    "['.']ods|['.']doc|['.']md", unwanted,
    collapse = "|"
  )
  unwanted <- gsub(pattern = "\\|$", replacement = "", x = unwanted)
  slist <- ipayipi::dta_list(input_dir = pipe_house$wait_room, file_ext =
      file_ext_in, prompt = prompt, recurr = recurr, unwanted = unwanted,
    wanted = wanted
  )
  if (length(slist) == 0) {
    cli::cli_inform(c(paste0(
      "No files in waiting room ({pipe_house$wait_room})",
      " ready for phenomena standardisation;"
    ), "i" = paste0("First imbibe files imported files in the waiting room",
      " then run {.var header_sts()}. After {.var header_sts()} is complete,",
      " files are ready for phenomena standardisation."
    ), "i" = paste0("Files ready for phenomena standardisation have the ",
      "\'.iph\' extension."
    )))
    invisible(return())
  }
  if (verbose || xtra_v) cli::cli_h1(
    "Standardising {length(slist)} file{?s} phenomena"
  )

  phentab <- phenomena_chk(pipe_house = pipe_house,
    check_phenomena = TRUE, csv_out = TRUE, wanted = wanted,
    unwanted = unwanted, external_phentab = external_phentab,
    verbose = verbose, xtra_v = xtra_v
  )

  phentab <- phentab$update_phenomena
  # don't start sts if there are no sts
  if (nrow(phentab[!is.na(phen_name)]) == 0) {
    print(phentab)
    cli::cli_abort(c("No standards available",
      "i" = "All {.var phen_name} are {.var NA} in the \'aa_phentab\' file!"
    ))
  }
  if (fcoff()) xtra_v <- FALSE
  if (fcoff() && remove_dups) {
    cli::cli_abort(c(
      "i" = "In order to remove duplicated phen names (columns):",
      " " = paste0("change {.var future} processing to ",
        "{.var plan(sequential, split = TRUE)}"
      )
    ))
  }
  fupdates <- future.apply::future_lapply(seq_along(slist), function(i) {
    if (verbose || xtra_v) cli::cli_inform(c(" " = "Working on {slist[i]} ..."))
    m <- readRDS(file.path(pipe_house$wait_room, slist[i]))
    update <- FALSE # only set to TRUE if phen update is required
    # get original phen table and update
    phen_old <- m$phens
    phen_new <- lapply(seq_len(nrow(phen_old)), function(j) {
      phen_new <- phentab[uz_phen_name == phen_old$phen_name[j] &
          uz_units == phen_old$units[j] & uz_measure == phen_old$measure[j]
      ]
      if (nrow(phen_new) == 0 && !is.null(external_phentab)) {
        phen_new <- external_phentab[phen_name == phen_old$phen_name[j]]
      }
      if (nrow(phen_new) > 1) {
        cli::cli_warn(c(
          "!" = "Multiple possible phenomena matches: printing data summary"
        ))
        print(m$data_summary)
      }
      invisible(phen_new)
    })
    phen_new <- data.table::rbindlist(phen_new)[order(phen_name_full)]
    phen_new_chk <- phen_new[, c("phid", "phen_name_full", "phen_name", "units",
        "measure", "offset", "var_type"
      ), with = FALSE
    ]
    if (anyNA.data.frame(subset(phen_new_chk, select = -offset))) update <- TRUE

    # duplicate phen detection and resolution
    if (any(duplicated(phen_new[!is.na(phen_name)]$phen_name)) && remove_dups) {
      # prompt to remove duplicated column/phen
      cli::cli_inform(c(
        "Duplicate phenomena match!",
        "i" = "Please examine the input data.",
        " " = paste0("Duplicate phenomena: ",
          "{phen_new$phen_name[duplicated(phen_new$phen_name)]}"
        ),
        " " = "Phenomena table:"
      ))
      print(phen_new)
      cli::cli_inform(c("Data head:"))
      print(head(m$raw_data))

      chosen_p <- function() {
        n <- readline(prompt =
            paste0("Would you like to remove duplicate phenomena? (Y/n)  ")
        )
        if (!n %in% c("Y", "n")) {
          chosen_p()
        }
        if (n == "Y") {
          message(paste0("Examine the phenomena table row numbers 1 to ",
            nrow(phen_new)
          ))
          ri <- readline(prompt =
              paste0("Enter duplicate row number(s) e.g., c(1,5): ")
          )
          ri <- eval(parse(text = ri))
          if (all(all(!is.integer(ri)),
            all(ri > nrow(phen_new)),
            all(ri < 1)
          )) {
            chosen_p()
          } else {
            m$raw_data <- m$raw_data[, names(m$raw_data)[
              !names(m$raw_data) %in% phen_new$uz_phen_name[ri]
            ], with = FALSE]
            p <- phen_new[!uz_phen_name %in% phen_new$uz_phen_name[ri]]
          }
        }
        if (n == "n") {
          stop("Please resolve duplicate phenomena!")
        }
        return(list(new_dta = m$raw_data, p = p))
      }
      pn <- chosen_p()
      m$raw_data <- pn$new_dta
      phen_new <- pn$p
    }

    # if there are dups but we don't want to fix them now
    if (any(
      anyDuplicated(phen_new[!is.na(phen_name)]$phen_name)
      && !remove_dups, update
    )) {
      # do nothing return?
      if (verbose || xtra_v) {
        cli::cli_inform(c(
          "i" = "Updates to phenomena required ...",
          ">" = "Update phenomena metadata for {slist[i]}; ",
          ">" = "check for mistakes in the \'aa_phentab\' file"
        ))
      }
      if (anyDuplicated(phen_new[!is.na(phen_name)]$phen_name) &&
          !all(verbose, xtra_v)
      ) {
        cli::cli_inform(c(
          "i" = "Phenomena names were duplicated in some files ...",
          ">" = paste0("Set option {.var remove_dups} to {.var TRUE} and run",
            " this function again with processing plan:"
          ),
          " " = "{.var plan(sequential, split = TRUE)}"
        ))
      }
      orig_fn <- m$data_summary$file_origin
      fn <- file.path(pipe_house$wait_room, slist[i])
      z <- list(update = TRUE, fn = fn, old_fn = fn, orig_fn = orig_fn)
      return(z)
    }

    # if there are no dups
    # generate unique phen id
    phen_new$phid <- seq_len(nrow(phen_new))

    # update raw_data table
    new_names <- sapply(names(m$raw_data)[
      !names(m$raw_data) %in% c("id", "date_time")
    ], function(x) {
      new_name <- phen_new[uz_phen_name == x]$phen_name[1]
      return(new_name)
    })
    new_names <- new_names[!sapply(new_names, is.na)]
    data.table::setnames(m$raw_data, old = names(new_names),
      new = new_names
    )

    # standardise varible type synonyms
    z <- lapply(seq_along(ipayipi::sts_phen_var_type$phen_prop), function(j) {
      v <- ipayipi::sts_phen_var_type$phen_prop[j][1]
      s <- ipayipi::sts_phen_var_type$phen_syn[j][1]
      z <- phen_new[var_type %ilike% s]
      z$var_type <- v
      return(z)
    })
    phen_new <- data.table::rbindlist(z)
    # convert units
    # factors&num included here as they are first converted to character
    if (length(phen_new[var_type %ilike% "chr|fac|num|int"]$phen_name) > 0) {
      m$raw_data[, (phen_new[var_type %ilike% "chr|fac|num|int"]$phen_name) :=
          lapply(.SD, as.character),
        .SDcols = phen_new[var_type %ilike% "chr|fac|num|int"]$phen_name
      ]
    }
    if (length(phen_new[var_type %ilike% "num|int"]$phen_name) > 0) {
      m$raw_data[, (phen_new[var_type %ilike% "num|int"]$phen_name) :=
          lapply(.SD, function(x) readr::parse_number(x, na = c("NA", "NAN"))),
        .SDcols = phen_new[var_type %ilike% "num|int"]$phen_name
      ]
    }
    if (length(phen_new[var_type == "int"]$phen_name) > 0) {
      m$raw_data[, (phen_new[var_type == "int"]$phen_name) :=
          lapply(.SD, as.integer), .SDcols =
          phen_new[var_type == "int"]$phen_name
      ]
    }
    if (length(phen_new[var_type == "posix"]$phen_name) > 0) {
      dt_tz <- attr(m$raw_data$date_time[1], "tz")
      m$raw_data[, (phen_new[var_type == "posix"]$phen_name) :=
        lapply(.SD, function(x) {
          x <- lubridate::parse_date_time(x, tz = dt_tz, orders = dt_format)
          return(x)
        }),
      .SDcols = phen_new[var_type == "posix"]$phen_name]
    }
    if (length(phen_new[phen_name %ilike% "interfere"]$phen_name) > 0) {
      for (ii in seq_len(nrow(phen_new[phen_name %ilike% "interfere"]))) {
        phenz <- phen_new[phen_name %ilike% "interfere"]$phen_name[ii]
        m$raw_data[, ][[phenz]][m$raw_data[, ][[phenz]] %ilike% "log"
        ] <- "logged"
        m$raw_data[, ][[phenz]][m$raw_data[, ][[phenz]] != "logged"] <- NA
      }
    }
    if (nrow(phen_new[var_type == "fac"]) > 0) {
      m$raw_data[, (phen_new[var_type == "fac"]$phen_name) :=
          lapply(.SD, as.factor),
        .SDcols = phen_new[var_type == "fac"]$phen_name
      ]
    }
    for (ii in seq_len(nrow(
      phen_new[!is.na(offset) & var_type %in% c("num|numeric", "int|integer")]
    ))) {
      phenz <- phen_new[
        !is.na(offset) & var_type %in% c("num|numeric", "int|integer")
      ]$phen_name[ii]
      m$raw_data[, ][[phenz]]  <- readr::parse_number(m$raw_data[[phenz]]) +
        rep_len(readr::parse_number(phen_new[!is.na(offset)]$offset[ii]),
          length.out = nrow(m$raw_data)
        )
    }
    for (ii in seq_len(nrow(phen_new[!is.na(f_convert)]))) {
      phenz <- phen_new[!is.na(f_convert)]$phen_name[ii]
      f <- as.character(phen_new[!is.na(f_convert)]$f_convert[ii])
      f <- readr::parse_number(f)
      m$raw_data[, ][[phenz]] <- m$raw_data[[phenz]] * rep_len(f, length.out =
          nrow(m$raw_data)
      )
    }
    m$phens <- phen_new
    # update the phenomena data summary
    m$phen_data_summary <- data.table::data.table(
      phid = as.integer(m$phens$phid),
      #dsid = as.integer(rep(as.integer(m$data_summary$dsid), nrow(m$phens))),
      # removal of dsid in this summary --- generates too much data
      #  when appending these station data tables
      start_dttm = rep(m$data_summary$start_dttm, nrow(m$phens)),
      end_dttm = rep(m$data_summary$end_dttm, nrow(m$phens)),
      table_name = rep(m$data_summary$table_name, nrow(m$phens))
    )
    m$phen_data_summary <- unique(m$phen_data_summary)
    old_fn <- file.path(pipe_house$wait_room, slist[[i]])
    fn <- file.path(pipe_house$wait_room,
      gsub(file_ext_in, file_ext_out, slist[[i]])
    )
    saveRDS(m, old_fn)
    orig_fn <- m$data_summary$file_origin
    z <- list(update = update, fn = fn, old_fn = old_fn, orig_fn = orig_fn)
    invisible(z)
  })

  fupdates <- data.table::rbindlist(fupdates)
  # rename saved files if they don't still need phen updates
  if (nrow(fupdates) == 0) {
    if (verbose || xtra_v) cli::cli_h1("")
    return(list(updated_files = NULL, no_updates = NULL))
  }
  no_update <- fupdates[update == TRUE]
  tbl_update <- fupdates[update == FALSE]
  if (nrow(tbl_update) > 0) {
    fn <- file.path(pipe_house$wait_room, basename(tbl_update$fn))
    old_fn <- file.path(pipe_house$wait_room, basename(tbl_update$old_fn))
    fne <- file.exists(old_fn)
    file.rename(from = old_fn[fne], to = fn[fne])
  } else {
    cli::cli_inform(c("i" = "The files below could not be updated.",
      " " = "After completing phenomena details for standardisation, make",
      " " = "sure that you have enabled duplicate phen name removal by",
      " " = "setting: {.var remove_dups} to {.var TRUE}, and",
      " " = "use option {.var plan(sequential, split = TRUE)} before",
      " " = "running this function again."
    ))
    print(no_update)
  }
  data.table::setnames(tbl_update, old = c("fn", "old_fn", "orig_fn"),
    new = c("file_name", "old_file_name", "original_file_name")
  )
  data.table::setnames(no_update, old = c("fn", "old_fn", "orig_fn"),
    new = c("file_name", "old_file_name", "original_file_name")
  )
  if (verbose || xtra_v) {
    cli::cli_h1("")
    cli::cli_inform(c(
      "What next?",
      "*" = paste0(
        "When phenomena standardisation of a file is complete, the file is ",
        "renamed with the extension \'.ipi\'."
      ),
      "*" = paste0(
        "Follow prompts to complete standardisation if necessary. Which fields",
        " were not standardised?"
      ),
      "v" = paste0("\'.ipi\' files are standardised and ready for transfer to ",
        "the phenomena standardisation."
      ),
      ">" = paste0("Use {.var transfer_sts_files()} to move \'ipi\' files to ",
        "the {.var nomvet_room}."
      ),
      " " = "This will clean up the {.var wait_room}."
    ))
  }
  invisible(list(updated_files = tbl_update, no_updates = no_update))
}