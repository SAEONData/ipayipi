#' @title Batch imbibe of 'flat' data files into the 'ipayipi' format
#' @description Uses `ipayipi::imbibe_logger_dt()` to read and transfer
#'  data files to the begining stages of the `ipayipi` data pipeline. Option
#'  to archive all 'raw' data files in the pipeline structure, _see details_.
#' @param pipe_house List of pipeline directories. __See__
#'  `ipayipi::ipip_house()` __for details__.
#' @param wipe_source Logical. If `TRUE` then raw data files in the source location will be deleted. _See details_
#' @param file_ext_in The file extension defaults of the raw logger data files. This can be left as `NULL` so 'all' files but those with extensions that cannot be imbibed (".ipr|.ipi|.iph|.xls|.rps|.rns|.ods|.doc").
#' @param file_ext_out The file extension used when raw logger data which has
#'  been imbibed into the `ipayipi` data pipeline. Advisable to leave this as the default (".ipr") for the pipeline structure.
#' @param col_dlm The column delimter which is fed to `data.table::fread()`. Defaults to NULL. When `NULL` the function uses `data.table::fread` ability to 'guess' the delimeter.
#' @param dt_format Argument passed to `ipayipi::imbibe_raw_logger_dt()`. The function attempts to work out the date-time format from a vector of format types supplied to this argument. The testing is done via `lubridate::parse_date_time()`. `lubridate::parse_date_time()` prioritizes the tesing of date-time formats in the order vector of formats supplied. The default vector of date-time formats supplied should work well for most logger outputs.
#' @param dt_tz recognized time-zone of the data locale.
#' @param record_interval If there are is no discrete, record interval set in the logger program, i.e., the sampling is event based, then this parameter must be set to "event_based". Defaults to "continuous".
#' @param data_setup List of options used to extract data and metadata from instrument data outputs. For a description of the `data_setup` _see_ \code{\link{imbibe_raw_logger_dt}}.
#' @param logg_interfere_type Two options here: "remote" or "on_site". Each time a logger is visited is counted as a logger interference event. Type _'remote'_ occurs when data is downloaded remotely. Type _'on_site'_is when data was downloaded on site. _See_ `ipayipi::imbibe_raw_logger_dt()`.
#' @param prompt Should the function use an interactive file selection function
#'  otherwise all files are returned. TRUE or FALSE.
#' @param wanted A string containing keywords to use to filter which stations
#'  are selected for processing. Multiple search kewords should be seperated
#'  with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param recurr Should the function search recursively into sub directories for hobo rainfall csv export files? TRUE or FALSE.
#' @param verbose Logical passed to `attempt::attempt()` which reads the logger text file in with either `data.table::fread()` or base R.
#' @param max_rows The number of rows to use when evaluating the record interval. Argument is parsed to `ipayipi::record_interval_eval()`.
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on Linux.
#' @details
#'  In the pipeline structure this function should be used post `ipayipi::logger_data_import_batch()`. `ipayipi::imbibe_raw_batch()` is a wrapper for `ipayipi::imbibe_raw_logger_dt()` --- see this functions documentation for more details.
#'
#'  __'Archiving' raw data__
#'  Files brought into the 'wait_room' are only housed there temporarily. In order to archive these raw data files a 'raw_room' directory must be provided in the 'pipe_house' object (_see_ `ipayipi::ipip_house()`). Files will be archived in structured directories in the 'raw_room' named by the last year and month in their respective date time records. Original  file names are maintained, and have a suffix with a unique integer plus the date and time of which they were archived. N.B. Files in the source directory (`source_dir`) are only deleted when `wipe_source` is set to `TRUE`.
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
    "Ymd HMOS", "Ymd HMS",
    "Ymd IMOSp", "Ymd IMSp",
    "ymd HMOS", "ymd HMS",
    "ymd IMOSp", "ymd IMSp",
    "mdY HMOS", "mdY HMS",
    "mdy HMOS",  "mdy HMS",
    "mdy IMOSp",  "mdy IMSp",
    "dmY HMOS", "dmY HMS",
    "dmy IMOSp", "dmy IMSp"
  ),
  dt_tz = "Africa/Johannesburg",
  record_interval_type = "continuous",
  remove_prompt = FALSE,
  max_rows = 1000,
  logg_interfere_type = "on_site",
  data_setup = NULL,
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  recurr = FALSE,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  "err" <- NULL
  # get list of data to be imported
  unwanted <- paste0("['.']ipr|['.']ipi|['.']iph|['.']xls|['.']rps",
    "['.']rns|['.']ods|['.']doc|['.']md|wait_room", unwanted,
    collapse = "|"
  )
  unwanted <- gsub(pattern = "^\\||\\|$", replacement = "", x = unwanted)
  slist <- ipayipi::dta_list(input_dir = pipe_house$wait_room, file_ext =
      file_ext_in, prompt = prompt, recurr = recurr, unwanted = unwanted,
    wanted = wanted
  )
  if (length(slist) == 0) {
    msg <- "No files to imbibe. Check 'wait_room'."
    return(message(msg))
  }
  cr_msg <- padr(core_message = paste0(" Reading ", length(slist),
      " logger files and converting ", collapes = ""
    ), wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0)
  )
  ipayipi::msg(cr_msg, verbose = verbose)

  file_name_dt <- future.apply::future_lapply(seq_along(slist), function(i) {
    cr_msg <- padr(core_message = paste0(" +> ", slist[i], collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(1, 1)
    )
    ipayipi::msg(cr_msg, verbose)
    if (is.null(file_ext_in)) {
      file_ext_in <- tools::file_ext(slist[i])
      file_ext_in <- paste0(".",
        sub(pattern = "[.]", replacement = "", file_ext_in)
      )
    }
    fp <- file.path(pipe_house$wait_room, slist[i])
    fl <- attempt::attempt(ipayipi::imbibe_raw_logger_dt(
      pipe_house = pipe_house,
      file_path = fp,
      file_ext = file_ext_in,
      col_dlm = col_dlm,
      dt_format = dt_format,
      dt_tz = dt_tz,
      record_interval_type = record_interval_type,
      data_setup = data_setup,
      remove_prompt = remove_prompt,
      max_rows = max_rows,
      logg_interfere_type = logg_interfere_type,
      verbose = verbose,
      xtra_v = xtra_v
    ))
    if (attempt::is_try_error(fl)) {
      fl$err <- TRUE
    }
    # save as tmp rds if no error
    if (!fl$err) {
      fn_htmp <- tempfile(pattern = "raw_",
        tmpdir = file.path(tempdir(), "wait_room_tmp")
      )
      st_dt <- min(fl$ipayipi_data_raw$raw_data$date_time)
      ed_dt <- max(fl$ipayipi_data_raw$raw_data$date_time)
      dttm_rng <- paste0(
        as.character(format(st_dt, "%Y")),
        as.character(format(st_dt, "%m")),
        as.character(format(st_dt, "%d")), "_",
        as.character(format(ed_dt, "%Y")),
        as.character(format(ed_dt, "%m")),
        as.character(format(ed_dt, "%d"))
      )
      fn <- paste0(fl$ipayipi_data_raw$data_summary$uz_station[1], "_",
        fl$ipayipi_data_raw$data_summary$uz_table_name[1], "_",
        gsub(pattern = "_", replacement = "-", x = dttm_rng)
      )
      class(fl$ipayipi_data_raw) <- "ipayipi_raw"
      if (!file.exists(dirname(fn_htmp))) dir.create(dirname(fn_htmp))
      saveRDS(fl$ipayipi_data_raw, fn_htmp)
    } else {
      fn_htmp <- NA_character_
      fn <- NA_character_
    }
    fn_resolve <- data.table::data.table(
      err = fl$err,
      fn_htmp = fn_htmp,
      fp = fp,
      fn = fn,
      st_dt = st_dt,
      ed_dt = ed_dt,
      file_ext_in = file_ext_in
    )
    return(fn_resolve)
  })
  file_name_dt <- data.table::rbindlist(file_name_dt)

  # archive input raw files ----
  # generate monthly folders if not already there
  file_name_dt$yr_mon_end <- as.character(format(file_name_dt$ed_dt, "%Y%m"))
  file_name_dt$ofn <- basename(file_name_dt$fp)
  fn_dt_arc <- file_name_dt[err != TRUE]

  if (!is.null(pipe_house$raw_room)) {
    future.apply::future_lapply(seq_along(fn_dt_arc$yr_mon_end), function(i) {
      arc_dir <- file.path(pipe_house$raw_room, fn_dt_arc$yr_mon_end[i])
      if (!dir.exists(arc_dir)) dir.create(arc_dir)
      file.copy(from = file.path(fn_dt_arc$fp[i]),
        to = file.path(arc_dir, paste0(gsub(pattern = fn_dt_arc$file_ext_in[i],
            replacement = "", x = fn_dt_arc$ofn[i]
          ), "_arcdttm_", as.character(format(Sys.time(), "%Y%m%d_%Hh%Mm%Ss")
          ), fn_dt_arc$file_ext_in[i], collapse = ""
        ))
      )
    })
  }

  # check for duplicates and make unique integers
  if (!anyNA.data.frame(fn_dt_arc)) {
    split_fn_dt_arc <- split(fn_dt_arc, f = factor(fn_dt_arc$fn))
    split_fn_dt_arc <-
      future.apply::future_lapply(split_fn_dt_arc, function(x) {
        x$rep <- seq_len(nrow(x))
        return(x)
      })
    fn_dt_arc <- data.table::rbindlist(split_fn_dt_arc)
    if (substr(file_ext_out, 1, 1) != ".") {
      file_ext_out <- paste0(".", file_ext_out)
    }
    # rename the temp rds files and delete the raw dat files
    future.apply::future_lapply(seq_along(fn_dt_arc$fn), function(i) {
      # rename temp file
      if (file.exists(fn_dt_arc$fn_htmp[i])) {
        file.copy(from = fn_dt_arc$fn_htmp[i],
          to = file.path(pipe_house$wait_room,
            paste0(fn_dt_arc$fn[i], "__", fn_dt_arc$rep[i], file_ext_out)
          )
        )
        unlink(fn_dt_arc$fn_htmp[i], recursive = TRUE)
      }

      # remove raw file in wait room
      if (file.exists(fn_dt_arc$fp[i])) file.remove(fn_dt_arc$fp[i])

      # remove dat file in source
      if (wipe_source) {
        fr <- gsub(paste0("__*.", fn_dt_arc$file_ext_in[i], "$"),
          fn_dt_arc$file_ext_in[i], fn_dt_arc$ofn[i]
        )
        if (file.exists(fr))  file.remove(fr)
      }
    })
  }
  cr_msg <- padr(core_message = paste0(" imbibed  ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0)
  )
  ipayipi::msg(cr_msg, verbose)
  if (nrow(file_name_dt[err == TRUE]) > 0) {
    message("Files could not be processed")
    print(file_name_dt[err == TRUE])
  }
  invisible(fn_dt_arc$fn)
}
