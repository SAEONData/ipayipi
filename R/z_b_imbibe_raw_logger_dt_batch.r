#' @title Batch imbibe of 'flat' data files into the 'ipayipi' format
#' @description Uses `ipayipi::imbibe_logger_dt()` to read and transfer
#'  data files to the begining stages of the `ipayipi` data pipeline. Option
#'  to archive all 'raw' data files in the pipeline structure, _see details_.
#' @param pipe_house List of pipeline directories. __See__
#'  `ipayipi::ipip_init()` __for details__.
#' @param wipe_source Logical. If `TRUE` then raw data files in the source location will be deleted. _See details_
#' @param file_ext_in The file extension defaults of the raw logger data files. This can be left as `NULL` so 'all' files but those with extensions that cannot be imbibed (".ipr|.ipi|.iph|.xls|.rps|.rns|.ods|.doc").
#' @param file_ext_out The file extension used when raw logger data which has
#'  been imbibed into the `ipayipi` data pipeline. Advisable to leave this as the default (".ipr") for the pipeline structure.
#' @param col_dlm The column delimter which is fed to `data.table::fread()`. Defaults to NULL. When `NULL` the function uses `data.table::fread` ability to 'guess' the delimeter.
#' @param dt_format Argument passed to `ipayipi::imbibe_raw_logger_dt()`. The function attempts to work out the date-time format from a vector of format types supplied to this argument. The testing is done via `lubridate::parse_date_time()`. `lubridate::parse_date_time()` prioritizes the tesing of date-time formats in the order vector of formats supplied. The default vector of date-time formats supplied should work well for most logger outputs.
#' @param dt_tz recognized time-zone of the data locale.
#' @param record_interval If there are is no discrete, record interval set in the logger program, i.e., the sampling is event based, then this parameter must be set to "event_based". Defaults to "continuous".
#' @param data_setup List of options used to extract data and metadata from instrument data outputs. For a description of the `data_setup` _see_ `ipayipi::read_logger_dt()`.
#' @param prompt Should the function use an interactive file selection function
#'  otherwise all files are returned. TRUE or FALSE.
#' @param wanted A string containing keywords to use to filter which stations
#'  are selected for processing. Multiple search kewords should be seperated
#'  with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted
#'  stations.
#' @param recurr Should the function search recursively into sub directories
#'  for hobo rainfall csv export files? TRUE or FALSE.
#' @param silent Logical passed to `attempt::attempt()` which reads the logger
#'  text file in with either `data.table::fread()` or base R.
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on Linux.
#' @details
#'  In the pipeline structure this function should be used post `ipayipi::logger_data_import_batch()`. `ipayipi::imbibe_raw_batch()` is a wrapper for `ipayipi::imbibe_raw_logger_dt()` --- see this functions documentation for more details.
#'
#'  __'Archiving' raw data__
#'  Files brought into the 'wait_room' are only housed there temporarily. In order to archive these raw data files a 'raw_room' directory must be provided in the 'pipe_house' object (_see_ `ipayipi::ipip_init()`). Files will be archived in structured directories in the 'raw_room' named by the last year and month in their respective date time records. Original  file names are maintained, and have a suffix with a unique integer plus the date and time of which they were archived. N.B. Files in the source directory (`source_dir`) are only deleted when `wipe_source` is set to `TRUE`.
#' @keywords meteorological data; automatic weather station; batch process; raw data standardisation; data pipeline
#' @author Paul J. Gordijn
#' @export
imbibe_raw_batch <- function(
  pipe_house = NULL,
  wipe_source = FALSE,
  file_ext_in = NULL,
  file_ext_out = ".ipr",
  col_dlm = NULL,
  dt_format = c(
    "Ymd HMS", "Ymd IMSp", "ymd HMS", "ymd IMSp", "mdY HMS", "mdy IMSp",
    "dmY HMS", "dmy IMSp", "Ymd HMOS", "Ymd IMOSp", "ymd HMOS", "ymd IMOSp",
    "mdY HMOS", "mdy IMOSp", "dmY HMOS", "dmy IMOSp"),
  dt_tz = "Africa/Johannesburg",
  record_interval_type = "continuous",
  data_setup = NULL,
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  recurr = FALSE,
  verbose = FALSE,
  cores = getOption("mc.cores", 2L),
  ...
) {

  # get list of data to be imported
  unwanted <- paste(".ipr|.ipi|.iph|.xls|.rps|.rns|.ods|.doc", unwanted,
    sep = "|")
  unwanted <- gsub(pattern = "\\|$", replacement = "", x = unwanted)
  slist <- ipayipi::dta_list(input_dir = pipe_house$wait_room, file_ext =
    file_ext_in, prompt = prompt, recurr = recurr, unwanted = unwanted)
  if (length(slist) == 0) {
    msg <- "No files to imbibe. Check 'wait_room'."
    return(message(msg))
  }
  cr_msg <- padr(core_message =
    paste0(" Reading ", length(slist),
      " logger files and converting ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  ipayipi::msg(cr_msg, verbose = verbose)

  cfiles <- parallel::mclapply(seq_along(slist), function(i) {
    cr_msg <- padr(core_message = paste0(" +> ", slist[i], collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(1, 1))
    ipayipi::msg(cr_msg, verbose)
      if (is.null(file_ext_in)) {
        file_ext_in <- tools::file_ext(slist[i])
        file_ext_in <- paste0(".",
          sub(pattern = "\\.", replacement = "", file_ext_in))
      }
      fl <- ipayipi::imbibe_raw_logger_dt(
        pipe_house = pipe_house,
        file_path = file.path(pipe_house$wait_room, slist[i]),
        file_ext = file_ext_in,
        col_dlm = col_dlm,
        dt_format = dt_format,
        dt_tz = dt_tz,
        record_interval_type = record_interval_type,
        data_setup = data_setup,
        verbose = verbose,
        cores = cores
      )
    invisible(fl)
  }, mc.cores = cores)
  err_files <- sapply(cfiles, function(x) attempt::is_try_error(x))
  cfiles <- cfiles[!err_files]
  slist_err <- slist[err_files]
  slist <- slist[!err_files]
  # generate file names for all files
  file_names <- parallel::mclapply(seq_along(cfiles), function(x) {
    st_dt <- min(cfiles[[x]]$raw_data$date_time)
    ed_dt <- max(cfiles[[x]]$raw_data$date_time)
    dttm_rng <- paste0(
      as.character(format(st_dt, "%Y")),
      as.character(format(st_dt, "%m")),
      as.character(format(st_dt, "%d")), "_",
      as.character(format(ed_dt, "%Y")),
      as.character(format(ed_dt, "%m")),
      as.character(format(ed_dt, "%d"))
    )
    file_name <- paste0(
      cfiles[[x]]$data_summary$uz_station[1], "_",
      cfiles[[x]]$data_summary$uz_table_name[1], "_",
      gsub(pattern = "_", replacement = "-", x = dttm_rng)
    )
    file_name_dt <- data.table::data.table(
      file_name = file_name,
      st_dt = st_dt,
      ed_dt = ed_dt,
      dttm_rng,
      rep = 1
    )
    invisible(file_name_dt)
  }, mc.cores = cores)
  file_name_dt <- data.table::rbindlist(file_names)

  # archive input raw files ----
  # generate monthly folders if not already there
  file_name_dt$yr_mon_end <- as.character(format(file_name_dt$ed_dt, "%Y%m"))
  file_name_dt$old_filename <- slist
  if (!is.null(pipe_house$raw_room)) {
    arcs <- parallel::mclapply(seq_along(file_name_dt$yr_mon_end), function(i) {
      arc_dir <- file.path(pipe_house$raw_room, file_name_dt$yr_mon_end[i])
      if (!dir.exists(arc_dir)) dir.create(arc_dir)
      if (is.null(file_ext_in)) {
        file_ext_in <- tools::file_ext(slist[i])
        file_ext_in <- paste0(".",
          sub(pattern = "\\.", replacement = "", file_ext_in))
      }
      file.copy(from = file.path(pipe_house$wait_room,  slist[i]),
        to = file.path(arc_dir, paste0(
          gsub(pattern = file_ext_in, replacement = "", x = slist[i]),
          "_arcdttm_", as.character(format(Sys.time(), "%Y%m%d_%Hh%Mm%Ss")),
          file_ext_in, collapse = "")))
    }, mc.cores = cores)
    rm(arcs)
  }

  # check for duplicates and make unique integers
  split_file_name_dt <- split(file_name_dt, f = factor(file_name_dt$file_name))
  split_file_name_dt <- parallel::mclapply(split_file_name_dt, function(x) {
    x$rep <- seq_len(nrow(x))
    return(x)
  }, mc.cores = cores)
  split_file_name_dt <- data.table::rbindlist(split_file_name_dt)
  if (substr(file_ext_out, 1, 1) != ".") {
    file_ext_out <- paste0(".", file_ext_out)
  }
  # save files in the wait_room
  saved_files <- parallel::mclapply(seq_along(cfiles), function(i) {
    cs <- cfiles[[i]]
    class(cs) <- "ipayipi_raw"
    saveRDS(cs, file.path(pipe_house$wait_room,
        paste0(split_file_name_dt$file_name[i], "__",
          split_file_name_dt$rep[i], file_ext_out)))
    invisible(cs)
  }, mc.cores = cores)
  rm(saved_files)
  # remove 'raw' files in wait room
  del_dats <- parallel::mclapply(slist, function(x) {
    file.remove(file.path(pipe_house$wait_room, x))
    invisible(x)
  }, mc.cores = cores)
  rm(del_dats)
  # rm raw files from source
  if (wipe_source) {
    del_source_f <- parallel::mclapply(slist, function(x) {
      file.remove(file.path(pipe_house$source_dir, x))
      invisible(x)
    }, mc.cores = cores)
    rm(del_source_f)
  }
  cr_msg <- padr(core_message = paste0(" imbibed  ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  ipayipi::msg(cr_msg, verbose)
  if (length(slist_err) > 0) {
    warning("The following files could not be read by data.table::fread")
    print(slist_err)
  }
  invisible(cfiles)
}