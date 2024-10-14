#' @title Standardises ipayipi header information
#' @description Uses the nomenclature table, to standardise the record-interval string, start and end date-times in the file header, and the file name is copied from the standardised station name, that is, the 'standardised title'. The standardised station file header nomenclature is retained in an ipayipi station file 'data_summary' table.
#' @param pipe_house List of pipeline directories. __See__
#'  `ipayipi::ipip_house()` __for details__.
#' @param prompt Should the function use an interactive file selection function
#'  otherwise all files are returned. TRUE or FALSE. Defaults to FALSE.
#' @param recurr Should the function search recursively into sub directories for hobo rainfall csv export files? TRUE or FALSE.
#' @param wanted A strong containing keywords to use to filter which stations are selected for processing. Multiple search kewords should be seperated with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param file_ext_in Extension of the file to check header standards. Defaults to ".ipr".
#' @param file_ext_out Extension of the standardised file (i.e., extension to use for saving a file). Defaults to ".iph".
#' @param verbose Print some details on the files being processed? Logical.
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on Linux.
#' @details This function calls `ipayipi::nomenclature_chk()` which will take take the user interactively through a process of standardising station names and titles.
#'  Station phenomena can only be checked once header nomenclature has been standardised.
#'
#'  Note that there is no unstandardised 'uz' location. Whatever location provided to in nomenclature table will be used in naming conventions.
#'  The 'location' field can be provided edited during the `ipayipi::nomenclature_chk()` and `ipayipi::read_nomtab_csv()` functionality.
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
  file_ext_in = ".ipr",
  file_ext_out = ".iph",
  verbose = FALSE,
  ...
) {
  ".SD" <- ":=" <- NULL
  "uz_station" <- "logger_type" <- "uz_table_name" <- "old_fn" <-
    "uz_record_interval_type" <- "uz_record_interval" <- NULL
  # get list of data to be imported
  slist <- ipayipi::dta_list(input_dir = pipe_house$wait_room, file_ext =
      file_ext_in, prompt = prompt, recurr = recurr, unwanted = unwanted,
    wanted = wanted
  )
  cr_msg <- padr(core_message = paste0(" Standardising ", length(slist),
      " ipayipi header info ", collapes = ""
    ), wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0)
  )
  ipayipi::msg(cr_msg, verbose)
  nomtab <- ipayipi::nomenclature_chk(pipe_house = pipe_house,
    check_nomenclature = TRUE, csv_out = TRUE, file_ext = file_ext_in
  )
  if (!is.na(nomtab$output_csv_name)) message("Update nomenclature")
  nomtab <- nomtab$update_nomtab

  file_name_dt <- future.apply::future_lapply(seq_along(slist), function(i) {
    cr_msg <- padr(core_message = paste0(" +> ", slist[i], collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(1, 1)
    )
    ipayipi::msg(cr_msg, verbose)
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
    if (na_sub | nrow(nt) == 0 | nt$table_name == "raw_NA") {
      msg <- paste0("Update header nomenclature for: ", slist[i], collapse = "")
      message(msg)
      z <- list(update = TRUE)
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
      z <- list(update = FALSE)
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
    future.apply::future_lapply(seq_len(nrow(tbl_update)), function(i) {
      fn <- file.path(pipe_house$wait_room, paste0(tbl_update$fn[i], "__",
        tbl_update$rep[i], file_ext_out
      ))
      old_fn <- file.path(pipe_house$wait_room, paste0(tbl_update$old_fn[i]))
      if (file.exists(old_fn)) {
        s <- file.rename(from = old_fn, to = fn)
      } else {
        s <- FALSE
      }
      invisible(s)
    })
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
  cr_msg <- padr(core_message =
      paste0("  headers standarised  ", collapes = ""), wdth = 80,
    pad_char = "=", pad_extras = c("|", "", "", "|"), force_extras = FALSE,
    justf = c(0, 0)
  )
  ipayipi::msg(cr_msg, verbose)
  invisible(list(updated_files = tbl_update, no_updates = no_update))
}