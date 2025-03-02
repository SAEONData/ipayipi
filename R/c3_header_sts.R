#' @title Standardises ipayipi header information
#' @description Uses the nomenclature table, to standardise the record-interval string, start and end date-times in the file header, and the file name is copied from the standardised station name, that is, the 'standardised title'. The standardised station file header nomenclature is retained in an ipayipi station file 'data_summary' table.
#' @inheritParams dta_list
#' @inheritParams  logger_data_import_batch

#' @details Standardising header metadata is a key step in a data pipeline and will determine what and how data are collated and processed. Check your standards carefully. Once your header metadata standards have been build following the prompts in this function, run [header_sts()] again. This will read in the most recently updated standards (wither the csv file or 'aa_notab.rns') and apply these to files in the `wait_room`.
#'
#' ## Header metadata
#' When `ipayipi` imbibes logger data the data setup (e.g., `ipayipi::hobo_rain`) allows for harvesting of logger file header information. This function assists with standardising this header information, storing this with header synonyms and other metadata in a table in the data pipelinese 'wait_room' directory. The file name of the table is set by default to 'aa_nomtab.rns' (an rds file). If this file is deleted then the synonym database will need to be rebuilt using this function.
#'
#' ## Editing the nomenclature table:
#'  When unrecognised synonyms are introduced (e.g., after a change in logger program setup, or simply a change in file name spelling) this function creates a csv table in the `wait_room` that should be edited to maintain the synonym database. _Unstandardised header information is preffixed in the nomenclature table (csv) by 'uz_'._ These unstandardised columns must not be edited by the user, but the standardised header information can be added by replacing corresponding `NA` values with appropriate standards.
#' The following fields in the nomenclature table may require editing/standardisation:
#'   - '__location__': The region name used to subset a group of stations by area.
#'   - '__station__': The name of the station collecting logger data.
#'   - '__stnd_title__': The standard station title. It is recommended that this title is given as the concatenation of the location and station field above, seperated by an underscore.
#'   - '__record_interval_type__': The record interval type; one of the following values: 'continuous', 'event_based', or 'mixed'.
#'   - '__record_interval__': [imbibe_raw_logger_dt()] uses [record_interval_eval()] to evaulate a date-time series to determine the record interval. Invariably the record interval is evaluated correctly, but if insufficient data errors may occur. Therefore the record interval parameters need to be verified whilst checking nomenclature.
#'   - '__table_name__': The name of the table. By default the preffix to logger data is 'raw_'. Within the pipeline structure the name appended to this preffix should be the standardised record interval, e.g., 'raw_5_mins', for continuous 5 minute data, or 'raw_discnt' for raw event based data.
#'   __NB!__ Only edit these fields (above) of the nomenclature table during standardisation.
#
#'  Once this csv file has been edited and saved it can be pulled into the pipeline structure by running [read_nomtab_csv()] --- running [header_sts()] will call [read_nomtab_csv()].
#
#' ## What header information is used/standardised?:
#'  Note that this function will only assess files for which the station name is known. If the station name is known, the "uz_station", "logger_type", "record_interval_type", "record_interval", and "uz_table_name" are used to define unique station and station table entries, that require standardisation.
#'  Note that `record_interval_type` and `record_interval` are evaluated during the [imbibe_raw_logger_dt()] function.
#'
#' ## Standards:
#' The 'ipayipi' data pipeline strongly suggests conforming with 'tidyverse' data standards. Therefore, use lower case characters, no special characters, except for the underscore character which should be used for spacing.
#'
#' ## File extensions:
#' Note that after imbibing raw files with [imbibe_raw_batch()] files are given the '.ipr' extension. Only '.ipr' files are processed by this function, that is, `header_sts`. Once their header information has been standardised, files are given the '.iph' extension. '.iph' files are ready for the next step in standardisation --- [phenomena_sts()].
#'
#' ## Other:
#' This function calls [nomenclature_chk()] which will take take the user interactively through a process of standardising station names and titles.
#'  Station phenomena can only be checked once header nomenclature has been standardised.
#'
#'  Note that there is no unstandardised 'uz' location. Whatever location provided to in nomenclature table will be used in naming conventions.
#'  The 'location' field can be provided edited during the [nomenclature_chk()] and [read_nomtab_csv()] functionality.
#'
#' @keywords Cambell Scientific; meteorological data; automatic weather station; batch process; file standardisation; nomenclature; header information
#' @author Paul J. Gordijn
#' @md
#' @export
header_sts <- function(
  pipe_house = NULL,
  prompt = FALSE,
  recurr = TRUE,
  wanted = NULL,
  unwanted = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  ".SD" <- ":=" <- NULL
  "uz_station" <- "logger_type" <- "uz_table_name" <- "old_fn" <-
    "uz_record_interval_type" <- "uz_record_interval" <- NULL

  # file extension assignments
  file_ext_in <- ".ipr"
  file_ext_out <- ".iph"

  # get list of data to be imported
  slist <- ipayipi::dta_list(input_dir = pipe_house$wait_room, file_ext =
      file_ext_in, prompt = prompt, recurr = recurr, unwanted = unwanted,
    wanted = wanted, baros = TRUE
  )
  if (length(slist) == 0) {
    cli::cli_inform(c("!" = paste0(
      "No files read in waiting room ({pipe_house$wait_room})",
      " available for header standardisation."
    ), "i" = paste0("First imbibe imported files in the waiting room",
      " with {.var logger_data_import_batch()} then run {.var header_sts()}."
    ), "i" = paste0("Files ready for header standardisation have the ",
      "\'.ipr\' extension."
    )))
    invisible(NULL)
  }
  if (verbose || xtra_v) cli::cli_h1(c(
    "Standardising header info of {length(slist)} file{?s}"
  ))
  nomtab <- nomenclature_chk(pipe_house = pipe_house,
    csv_out = TRUE, file_ext = file_ext_in
  )
  nomtab <- nomtab$update_nomtab
  if (fcoff()) xtra_v <- FALSE
  file_name_dt <- future.apply::future_lapply(seq_along(slist), function(i) {
    if (verbose || xtra_v) cli::cli_inform(c(
      " " = "working on: {slist[i]} ..."
    ))
    m <- readRDS(file.path(pipe_house$wait_room, slist[i]))

    # update the start and end date_times
    m$data_summary$start_dttm <- min(m$raw_data$date_time)
    m$data_summary$end_dttm <- max(m$raw_data$date_time)

    # update header nomenclature
    m$data_summary$uz_station <- gsub(" ^*|* $", "", m$data_summary$uz_station)
    nt <- nomtab[
      uz_station == m$data_summary$uz_station &
        logger_type == m$data_summary$logger_type &
        uz_record_interval_type == m$data_summary$uz_record_interval_type &
        uz_record_interval == m$data_summary$uz_record_interval &
        uz_table_name == m$data_summary$uz_table_name
    ]
    # flag na_sub -- those that need further nomtab updates
    na_sub <- anyNA.data.frame(
      nt[, c("stnd_title", "location", "station", "record_interval_type",
          "record_interval", "table_name"
        ), with = FALSE
      ]
    )
    if (na_sub | nrow(nt) == 0 | isTRUE(nt$table_name == "raw_NA")) {
      mz <- c(
        ">" = paste0("Update header nomenclature for: {slist[", i, "]}; ")
      )
      z <- list(update = TRUE, mz = mz)
    } else {
      m$data_summary$stnd_title <- nt$stnd_title[1]
      m$data_summary$location <- nt$location[1]
      m$data_summary$station <- nt$station[1]
      m$data_summary$record_interval_type <- nt$record_interval_type[1]
      m$data_summary$record_interval <- nt$record_interval[1]
      m$data_summary$table_name <- nt$table_name[1]
      m$phen_data_summary$table_name <- nt$table_name[1]
      # ensure continuous date-time sequence of continuous data
      m$raw_data <- ipayipi::dttm_extend_long(
        data_sets = list(m$raw_data), ri = nt$record_interval[1],
        intra_check = TRUE
      )
      # add in dttm ie chng cols if necessary
      # this will update data_summary from v0.0.2 to 0.0.4 to include inc exc ti
      if (!"dttm_ie_chng" %in% names(m$data_summary)) {
        m$data_summary
        m$data_summary$dttm_inc_exc <- TRUE
        m$data_summary$dttm_ie_chng <- FALSE
        nds <- c("dsid", "file_format", "uz_station", "location", "station",
          "stnd_title", "start_dttm", "end_dttm", "logger_type", "logger_title",
          "logger_sn", "logger_os", "logger_program_name", "logger_program_sig",
          "uz_record_interval_type", "uz_record_interval",
          "record_interval_type", "record_interval", "dttm_inc_exc",
          "dttm_ie_chng", "uz_table_name", "table_name", "nomvet_name",
          "file_origin"
        )
        data.table::setcolorder(m$data_summary, neworder = nds)
      }
      # adjust inc exc timeline if necessary
      if (nt$record_interval_type[1] %in% "continuous" &&
          m$data_summary$dttm_ie_chng
      ) {
        tbls_ie <- c("data_summary", "phen_data_summary", "raw_data",
          "logg_interfere"
        )
        dttm_cols <- c("date_time", "start_dttm", "end_dttm")
        # round up or down dates
        if (!m$data_summary$dttm_inc_exc) {
          # minus ti
          lapply(seq_along(tbls_ie), function(j) {
            sdc <- names(m[[tbls_ie[j]]])[names(m[[tbls_ie[j]]]) %in% dttm_cols]
            m[[tbls_ie[j]]][, (sdc) := lapply(.SD, function(x) {
              x - lubridate::as.duration(nt$record_interval[1])
            }), .SDcols = sdc]
          })
        } else {
          # add ti
          lapply(seq_along(tbls_ie), function(j) {
            sdc <- names(m[[tbls_ie[j]]])[names(m[[tbls_ie[j]]]) %in% dttm_cols]
            m[[tbls_ie[j]]][, (sdc) := lapply(.SD, function(x) {
              x + lubridate::as.duration(nt$record_interval[1])
            }), .SDcols = sdc]
          })
        }
      }
      z <- list(update = FALSE, mz = NA)
    }
    # save m
    saveRDS(m, file.path(pipe_house$wait_room, slist[i]))

    # get proposed file name
    st_dt <- m$data_summary$start_dttm[1]
    ed_dt <- m$data_summary$end_dttm[1]
    if (is.na(m$data_summary$record_interval[1])) {
      intv_name <- ""
    } else {
      intv_name <- m$data_summary$record_interval[1]
    }
    file_name <- paste0(
      m$data_summary$stnd_title[1], "_",
      gsub(" ", "", intv_name), "_",
      as.character(format(st_dt, "%Y")),
      as.character(format(st_dt, "%m")),
      as.character(format(st_dt, "%d")), "-",
      as.character(format(ed_dt, "%Y")),
      as.character(format(ed_dt, "%m")),
      as.character(format(ed_dt, "%d"))
    )
    ri <- m$data_summary$record_interval
    z <- c(z, list(fn = file_name, old_fn = slist[[i]], ri = ri, rep = 1))
    if (verbose || xtra_v) cli::cli_inform(c(
      "v" = "\'{slist[[i]]}\' updated and renamed: \'{file_name}\'"
    ))
    return(z)
  })
  file_name_dt <- data.table::rbindlist(file_name_dt)

  if (nrow(file_name_dt) > 0) {
    # check for duplicates and make unique integers
    split_file_name_dt <- split(file_name_dt, f = factor(file_name_dt$fn))
    split_file_name_dt <- lapply(split_file_name_dt, function(x) {
      x$rep <- seq_len(nrow(x))
      return(x)
    })
    split_file_name_dt <- data.table::rbindlist(split_file_name_dt)
    split_file_name_dt <- split_file_name_dt[order(old_fn)]

    # rename saved files if they don't still need nomenclature updates
    no_update <- split_file_name_dt[update == TRUE]
    tbl_update <- split_file_name_dt[update == FALSE]
    if (verbose && nrow(no_update) > 0) {
      lapply(no_update$mz, function(x) {
        cli::cli_inform(c(">" = x))
      })
    }
    if (nrow(tbl_update) > 0) {
      if (xtra_v) {
        cli::cli_inform(c("i" =
            "Altering {nrow(tbl_update)} name{?s} owing to duplicate name{?s}",
          " " = "This is done if original file names were not unique.",
          " " = paste0("A suffix {.var __n} is added to files,",
            " where {.var n} is the duplicate number."
          )
        ))
      }
      fn <- file.path(pipe_house$wait_room,
        paste0(tbl_update$fn, "__", tbl_update$rep, file_ext_out)
      )
      old_fn <- file.path(pipe_house$wait_room, basename(tbl_update$old_fn))
      fne <- file.exists(old_fn)
      file.rename(from = old_fn[fne], to = fn[fne])
    }
    data.table::setnames(tbl_update, old = c("fn", "old_fn", "ri"),
      new = c("file_name", "old_file_name", "record_interval")
    )
    data.table::setnames(no_update, old = c("fn", "old_fn", "ri"),
      new = c("file_name", "old_file_name", "record_interval")
    )
  } else {
    tbl_update <- NULL
    no_update <- NULL
  }
  if (verbose || xtra_v) {
    cli::cli_h1("")
    cli::cli_inform(c(
      "What next?",
      "*" = paste0(
        "When header standardisation of a file is complete, the file is ",
        "renamed with the extension \'.iph\'."
      ),
      "*" = paste0(
        "Follow prompt instructions above if there are unprocessed",
        " \'.ipr\' files."
      ),
      "v" = "\'.iph\' files are ready for phenomena standardisation.",
      ">" = "Use {.var phenomena_sts()} to begin phenomena standardisation."
    ))
  }
  invisible(list(updated_files = tbl_update, no_updates = no_update))
}
