#' @title Standardises ipayipi header information
#' @description Uses the nomenclature table, standardises the recording
#'  interval string, start and end date times in the file header, and finally
#'  the file name is copied from the standardised station name, that is, the
#'  'standardised title'. The standardised station file header nomenclature
#'  is retained in an ipayipi station file 'data_summary' table.
#' @param pipe_house List of pipeline directories. __See__
#'  `ipayipi::ipip_init()` __for details__.
#' @param prompt Should the function use an interactive file selection function
#'  otherwise all files are returned. TRUE or FALSE. Defaults to FALSE.
#' @param recurr Should the function search recursively into sub directories
#'  for hobo rainfall csv export files? TRUE or FALSE.
#' @param wanted A strong containing keywords to use to filter which stations
#'  are selected for processing. Multiple search kewords should be seperated
#'  with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted
#'  stations.
#' @param file_ext_in
#' @param file_ext_out
#' @details This function calls `ipayipi::nomenclature_sts()` which will take
#'  take the user interactively through a process of standardising station
#'  names and titles.
#'  Station phenomena can only be checked once header nomenclature has been
#'  standardised.
#'
#'  Note that there is no unstandardised 'uz' location. Whatever location
#'  provided to in nomenclature table will be used in naming conventions.
#'  The 'location' field can be provided edited during the
#'  `ipayipi::nomenclature_sts()` and `ipayipi::read_nomtab_csv()`
#'  functionality.
#' @keywords Cambell Scientific; meteorological data; automatic weather
#'  station; batch process; file standardisation; nomenclature; header
#'  information
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
  ...
) {
  "uz_station" <- "logger_type" <- "record_interval_type" <-
    "record_interval" <- "uz_table_name" <- "old_fn" <- NULL
  # get list of data to be imported
  slist <- ipayipi::dta_list(input_dir = pipe_house$wait_room, file_ext =
    file_ext_in, prompt = prompt, recurr = recurr, unwanted = unwanted,
    wanted = wanted)
  cr_msg <- padr(core_message =
    paste0(" Standardising ", length(slist),
      " ipayipi header info ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  nomtab <- ipayipi::nomenclature_sts(pipe_house = pipe_house,
    check_nomenclature = TRUE, csv_out = TRUE, file_ext = file_ext_in)
  if (!is.na(nomtab$output_csv_name)) {
    stop("Update nomenclature")
  }
  nomtab <- nomtab$update_nomtab
  mfiles <- lapply(seq_along(slist), function(i) {
    cr_msg <- padr(core_message = paste0(" +> ", slist[i], collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(1, 1))
    message(cr_msg)
    m <- readRDS(file.path(pipe_house$wait_room, slist[i]))

    # update the start and end date_times
    m$data_summary$start_dttm <- min(m$raw_data$date_time)
    m$data_summary$end_dttm <- max(m$raw_data$date_time)

    # update names
    nt <- nomtab[
      uz_station == m$data_summary$uz_station &
      logger_type == m$data_summary$logger_type &
      record_interval_type == m$data_summary$record_interval_type &
      record_interval == m$data_summary$record_interval &
      uz_table_name == m$data_summary$uz_table_name
    ]
    m$data_summary$stnd_title <- nt$stnd_title[1]
    m$data_summary$location <- nt$location[1]
    m$data_summary$station <- nt$station[1]
    m$data_summary$table_name <- nt$table_name[1]
    m$phen_data_summary$table_name <- nt$table_name[1]
    invisible(m)
  })

  # sort out file names
  file_names <- lapply(seq_along(mfiles), function(i) {
    st_dt <- mfiles[[i]]$data_summary$start_dttm[1]
    ed_dt <- mfiles[[i]]$data_summary$end_dttm[1]
    if (is.na(mfiles[[i]]$data_summary$record_interval[1])) {
      intv_name <- ""
    } else {
      intv_name <- mfiles[[i]]$data_summary$record_interval[1]
    }
    file_name <- paste0(
      mfiles[[i]]$data_summary$stnd_title[1], "_",
        gsub(" ", "", intv_name), "_",
      as.character(format(st_dt, "%Y")),
      as.character(format(st_dt, "%m")),
      as.character(format(st_dt, "%d")), "-",
      as.character(format(ed_dt, "%Y")),
      as.character(format(ed_dt, "%m")),
      as.character(format(ed_dt, "%d"))
    )
    file_name_dt <- data.table::data.table(
      old_fn = slist[[i]],
      file_name = file_name,
      rep = 1,
      record_interval = mfiles[[i]]$data_summary$record_interval
    )
    invisible(file_name_dt)
  })
  file_name_dt <- data.table::rbindlist(file_names)
  # check for duplicates and make unique integers
  split_file_name_dt <- split(file_name_dt, f = factor(file_name_dt$file_name))
  split_file_name_dt <- lapply(split_file_name_dt, function(x) {
    x$rep <- seq_len(nrow(x))
    return(x)
  })
  split_file_name_dt <- data.table::rbindlist(split_file_name_dt)
  split_file_name_dt <- split_file_name_dt[order(old_fn)]

  # save files to the wait room
  saved_files <- lapply(seq_along(mfiles), function(x) {
    fn <- file.path(pipe_house$wait_room, paste0(
      split_file_name_dt$file_name[x], "__", split_file_name_dt$rep[x],
      file_ext_out))
    saveRDS(mfiles[[x]], fn)
    invisible(fn)
  })

  # remove slist files
  del_metn <- lapply(slist, function(x) {
    file.remove(file.path(pipe_house$wait_room, x))
    invisible(file.path(pipe_house$wait_room, x))
  })

  cr_msg <- padr(core_message = paste0(
    "  headers standarised  ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  invisible(list(saved_files = saved_files, deleted_files = del_metn))
}