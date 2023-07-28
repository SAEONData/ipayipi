#' @title Batch imbibe of 'flat' data files into the 'ipayipi' format
#' @description Uses `ipayipi::imbibe_logger_dt()` to read and transfer
#'  data files to the begining stages of the `ipayipi` data pipeline.
#' @param wait_room Path to the 'wait_room' directory.
#' @param file_ext_in The file extension defaults of the raw logger data files.
#' @param file_ext_out The file extension used when raw logger data which has
#'  been imbibed into the `ipayipi` data pipeline.
#' @param col_dlm The column delimter which is fed to `data.table::fread()`.
#'   Defaults to a comma for 'csv' files.
#' @param dt_format The input date-time format of the time series, e.g.,
#'  "%y-%m-%d %H:%M:%S". See ?base::strptime() for details.
#' @param dt_tz recognized time-zone of the data locale.
#' @param record_interval If there are is no discrete record interval set
#'  in the logger program, i.e., the sampling is event based, then this
#'  parameter must be set to "event_based". Defaults to "continuous".
#' @param data_setup List of options used to extract data and metadata from
#'  instrument data outputs. For a description of the `data_setup` see
#'  `ipayipi::read_logger_dt()`.
#' @param prompt Should the function use an interactive file selection function
#'  otherwise all files are returned. TRUE or FALSE.
#' @param wanted A strong containing keywords to use to filter which stations
#'  are selected for processing. Multiple search kewords should be seperated
#'  with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted
#'  stations.
#' @param recurr Should the function search recursively into sub directories
#'  for hobo rainfall csv export files? TRUE or FALSE.
#' @keywords meteorological data; automatic weather station; batch process;
#'  raw data standardisation; data pipeline
#' @author Paul J. Gordijn
#' @export
#' @details 
imbibe_raw_batch <- function(
  wait_room = NULL,
  file_ext_in = NULL,
  file_ext_out = ".ipr",
  col_dlm = ",",
  dt_format = c(
    "Ymd HMS", "Ymd IMSp",
    "ymd HMS", "ymd IMSp",
    "mdY HMS", "mdy IMSp",
    "dmY HMS", "dmy IMSp",
    "Ymd HMOS", "Ymd IMOSp",
    "ymd HMOS", "ymd IMOSp",
    "mdY HMOS", "mdy IMOSp",
    "dmY HMOS", "dmy IMOSp"),
  dt_tz = "Africa/Johannesburg",
  record_interval_type = "continuous",
  data_setup = NULL,
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  recurr = FALSE,
  ...
) {
  # get list of data to be imported
  slist <- ipayipi::dta_list(input_dir = wait_room, file_ext = file_ext_in,
    prompt = prompt, recurr = recurr, unwanted = unwanted)
  cr_msg <- padr(core_message =
    paste0(" Reading ", length(slist),
      " logger files and converting ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)

  cfiles <- lapply(seq_along(slist), function(i) {
    cr_msg <- padr(core_message = paste0(" +> ", slist[i], collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(1, 1))
    message(cr_msg)
      fl <- ipayipi::imbibe_raw_logger_dt(
        file_path = file.path(wait_room, slist[i]),
        file_ext = file_ext_in,
        col_dlm = col_dlm,
        dt_format = dt_format,
        dt_tz = dt_tz,
        record_interval_type = record_interval_type,
        data_setup = data_setup
      )
    invisible(fl)
  })

  # generate file names for all files
  file_names <- lapply(seq_along(cfiles), function(x) {
    st_dt <- min(cfiles[[x]]$raw_data$date_time)
    ed_dt <- max(cfiles[[x]]$raw_data$date_time)
    file_name <- paste0(
      cfiles[[x]]$data_summary$uz_station[1], "_",
      cfiles[[x]]$data_summary$uz_table_name[1], "_",
      as.character(format(st_dt, "%Y")),
      as.character(format(st_dt, "%m")),
      as.character(format(st_dt, "%d")), "-",
      as.character(format(ed_dt, "%Y")),
      as.character(format(ed_dt, "%m")),
      as.character(format(ed_dt, "%d"))
    )
    file_name_dt <- data.table::data.table(
      file_name = file_name,
      rep = 1
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
  if (substr(file_ext_out, 1, 1) != ".") {
    file_ext_out <- paste0(".", file_ext_out)
  }
  # save files in the wait_room
  saved_files <- lapply(seq_along(cfiles), function(x) {
    cs <- cfiles[[x]]
    class(cs) <- "ipayipi_raw"
    saveRDS(cs, file.path(wait_room,
        paste0(split_file_name_dt$file_name[x], "__",
          split_file_name_dt$rep[x], file_ext_out)))
    invisible(cs)
  })
  rm(saved_files)
  del_dats <- lapply(slist, function(x) {
    file.remove(file.path(wait_room, x))
    invisible(x)
  })
  rm(del_dats)
  cr_msg <- padr(core_message = paste0("", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  invisible(cfiles)
}