#' @title Standardises data phenomena in 'ipayipi' format.
#' @description Standardise raw data inputs formats and more for the
#'  'ipayipi' pipeline though an interactive process.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param remove_dups Logical. If true then a prompt is used to facilitate the removal of duplicate phenomena. Files with duplicate phenomena names will not be processed into a pipeline until the issue of duplication issue is resolved.
#' @param external_phentab File path to phenomena database. By default the function will search in the 'pipe_house' 'wait_room' for a phenomena database ('phentab'). If `external_phentab` is provided this option will override the 'pipe_house' default enabling access to another pipelines phenomena database.
#' @param wanted A string of keywords to use to filter which stations are selected for processing. Multiple search kewords should be seperated with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param file_ext_in The file extension defaults to ".iph". Other file types could be incorporatted if required.
#' @param file_ext_in The file extension defaults to ".iph". Other file types could be incorporatted if required.
#' @param prompt Should the function use an interactive file selection function otherwise all files are returned. TRUE or FALSE.
#' @param recurr Should the function search recursively into sub directories for hobo rainfall csv export files? TRUE or FALSE.
#' @param verbose Print some details on the files being processed? Logical.
#' @keywords time series data; automatic weather station; batch process;
#'  file standardisation; standardise variables; transform variables
#' @author Paul J. Gordijn
#' @export
#' @details The raw data formats, units, and offsets, in an 'ipayipi' station file are standardised using this function. The function keeps a record of phenomena (variable) metadata and uses this to implement transformations to standardise the data. If 'new' phenomena are detected in the data then a csv file is generated and the user must fill in the 'blank' (NA) values. The most recently modified 'phentab' (phenomena table) csv will be imbibed by this function and used to update the pipelines 'phentab' database.
#'
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
#'  __Editing the phenomena table__:
#'  When unrecognised synonyms are introduced (e.g., after a change in logger program setup, or 'new' variable from an additional station sensor) this function creates a csv table in the `wait_room` that should be edited to maintain a phenomena synonym database. Unstandardised phenomena metadata (columns) are preffixed by 'uz_'._ These unstandardised columns must not be edited by the user: standardised phenomena information can be added by replacing corresponding `NA` values with information. The required fields, marked with an asterix (above), must be described.
#'
#'  __Duplicate phenomena names__:
#'  Duplicate phenomena in a single data series table cannot be tolerated. If a duplicate phenomena name is detected a prompt will work with the user to delete the duplicate. Most logger programmes avoid generating duplicates, but the situation can arise where perhaps a table is recording the same phenomenon multiple times.
#'
#'  __Other__: A number of other phenomena standardisation proceedures are included here:
#'  - '_Interference events_': Some logger files, e.g., hobo data logger files include useful columns that describe what within the ipayipi pipeline are termed 'logger interference events'. Logger interference events include events such as manual logger data downloads where the logger, or related data recording mechanism is 'interfered' with, and consequently, data near the time of interference may be altered undesirably. In hobo files interference column data are generally called 'logged' events, but spelling and capitalisation of this may change depending on the data. As part of the phenomena standardisation process, all interference events are marked in the data as 'logged'. Any column/phenomena in the data with the key 'interfere' will be marked as an 'interference' column. Therefore, when selecting phenomena names, take care to include the key 'interfere' so that the column is treated as recording 'interference' events.
#'
#'  __Special characters/phrases__:
#'  - '_sn' appended to the 'phen_name' denotes a 'sensor' number. Columns with this suffix are not aggregated during processing.
#' @md
phenomena_sts <- function(
  pipe_house = NULL,
  remove_dups = FALSE,
  external_phentab = NULL,
  prompt = FALSE,
  recurr = TRUE,
  wanted = NULL,
  unwanted = NULL,
  file_ext_in = ".iph",
  file_ext_out = ".ipi",
  verbose = FALSE,
  ...
) {
  "uz_phen_name" <- "uz_units" <- "uz_measure" <- "phen_name_full" <-
    "var_type" <- "f_convert" <- "%ilike%" <- ".SD" <- "phen_name" <-
    ":=" <- NULL
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
  cr_msg <- padr(core_message = paste0(" Standardising ", length(slist),
      " file(s) phenomena ", collapse = ""
    ), wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0)
  )
  ipayipi::msg(cr_msg, verbose)

  phentab <- ipayipi::phenomena_chk(pipe_house = pipe_house,
    check_phenomena = TRUE, csv_out = TRUE, wanted = wanted,
    unwanted = unwanted, external_phentab = external_phentab
  )

  if (!is.na(phentab$output_csv_name)) message("Update phenomena")
  phentab <- phentab$update_phenomena
  # don't start sts if there are no sts
  if (nrow(phentab[!is.na(phen_name)]) == 0) {
    return(message("No standards available"))
  }
  fupdates <- lapply(seq_along(slist), function(i) {
    cr_msg <- padr(core_message = paste0(" +> ", slist[i], collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(1, 1)
    )
    ipayipi::msg(cr_msg, verbose)
    m <- readRDS(file.path(pipe_house$wait_room, slist[i]))
    update <- FALSE # only set to TRUE if phen update is required
    # get original phen table and update
    phen_old <- m$phens
    phen_new <- lapply(seq_len(nrow(phen_old)), function(j) {
      phen_new <- phentab[uz_phen_name == phen_old$phen_name[j] &
          uz_units == phen_old$units[j] & uz_measure == phen_old$measure[j]
      ]
      if (nrow(phen_new) > 1) {
        message("Multiple possible phenomena matches: printing data summary")
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
    if (any(duplicated(phen_new$phen_name)) && remove_dups) {
      # prompt to remove duplicated column/phen
      message(paste0("Warning! Duplicate phenomena match!"))
      message(paste0("Please examine the input data."))
      message(paste0("Duplicate phenomena:"))
      print(phen_new$phen_name[duplicated(phen_new$phen_name)])
      message("Phenomena table:")
      print(phen_new)
      message("Data head:")
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
    if (any(any(duplicated(phen_new$phen_name)) && !remove_dups, update)) {
      # do nothing return?
      msg <- paste0(file.path(pipe_house$wait_room, slist[i]),
        ": not processed"
      )
      orig_fn <- m$data_summary$file_origin
      fn <- file.path(pipe_house$wait_room, slist[i])
      z <- list(update = TRUE, fn = fn, old_fn = fn, orig_fn = orig_fn)
      message(msg)
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
    data.table::setnames(m$raw_data, old = names(m$raw_data),
      new = c("id", "date_time", new_names)
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
          lapply(.SD, function(x) as.POSIXct(x, tz = dt_tz)),
        .SDcols = phen_new[var_type == "posix"]$phen_name
      ]
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
    cr_msg <- padr(
      core_message = paste0("  phens standardised  ", collapes = ""),
      wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(0, 0)
    )
    ipayipi::msg(cr_msg, verbose)
    return(list(updated_files = NULL, no_updates = NULL))
  }
  no_update <- fupdates[update == TRUE]
  tbl_update <- fupdates[update == FALSE]
  future.apply::future_lapply(seq_len(nrow(tbl_update)), function(i) {
    fn <- file.path(pipe_house$wait_room, basename(tbl_update$fn[i]))
    old_fn <- file.path(pipe_house$wait_room, basename(tbl_update$old_fn[i]))
    if (file.exists(old_fn)) {
      s <- file.rename(from = old_fn, to = fn)
    } else {
      s <- FALSE
    }
    invisible(s)
  })
  data.table::setnames(tbl_update, old = c("fn", "old_fn", "orig_fn"),
    new = c("file_name", "old_file_name", "original_file_name")
  )
  data.table::setnames(no_update, old = c("fn", "old_fn", "orig_fn"),
    new = c("file_name", "old_file_name", "original_file_name")
  )
  cr_msg <- padr(
    core_message = paste0("  phens standardised  ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0)
  )
  ipayipi::msg(cr_msg, verbose)
  invisible(list(updated_files = tbl_update, no_updates = no_update))
}