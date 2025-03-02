#' @title Batch imbibe of 'flat' data files into the 'ipayipi' format
#' @md
#' @description Reads and transfers data files to the begining stages of the `ipayipi` data pipeline. Option to archive all 'raw' data files in the pipeline structure, _see details_.
#' @inheritParams dta_list
#' @inheritParams logger_data_import_batch

#' @param wipe_source Logical. If `TRUE` then raw data files in the source location will be deleted. _See details_
#' @param file_ext_in The file extension defaults of the raw logger data files. This can be left as `NULL` so 'all' files but those with extensions that cannot be imbibed (".ipr|.ipi|.iph|.xls|.rps|.rns|.ods|.doc").
#' @param file_ext_out The file extension used when raw logger data which has
#'  been imbibed into the `ipayipi` data pipeline. Advisable to leave this as the default (".ipr") for the pipeline structure.
#' @param col_dlm The column delimter which is fed to [data.table::fread()]. Defaults to NULL. When `NULL` the function uses `data.table::fread` ability to 'guess' the delimeter.
#' @param dt_format The function attempts to work out the date-time format from a vector of format types supplied to this argument. The testing is done via [lubridate::parse_date_time()]. [lubridate::parse_date_time()] prioritizes the tesing of date-time formats in the order vector of formats supplied. The default vector of date-time formats supplied should work well for most logger outputs. \bold{NB!} seconds are required.
#' @param dt_tz Recognized time-zone (character string) of the data locale. The default for the package is South African, i.e., "Africa/Johannesburg" which is equivalent to "SAST".
#' @param record_interval_type If there are is no discrete record interval set in the logger program, i.e., the sampling is event-based, then this parameter must be set to "event_based". By default this function has this parameter set to "continuous", but the record interval is scrutinized by [record_interval_eval()] --- see the help files for this function for more information.
#'  The parameter supplied here is only used if there is only one data record and the record interval cannot be evaluated by [record_interval_eval()].
#' @param remove_prompt Logical; passed to [record_interval_eval()]. Activate a readline prompt to choose whether or not filter our records from `dta_in` with inconsistent record intervals.
#' @param data_setup List of options used to extract data and metadata from instrument data outputs. These arguments are parsed to [imbibe_raw_flat()].
#' @param logg_interfere_type Two options here: "remote" or "on_site". Each time a logger is visited is counted as a logger interference event. Type _'remote'_ occurs when data is downloaded remotely. Type _'on_site'_is when data was downloaded on site. _See_ [imbibe_raw_logger_dt()].
#' @param max_rows The number of rows to use when evaluating the record interval. Argument is parsed to [ipayipi::record_interval_eval()].
#' @details
#' ## Custom data setup options:
#' Custom data setups include those for hobo rainfall flat files [ipayipi::hobo_rain], generic flat data, solonist xml files [ipayipi::solonist], Cambell Scientific TOA5 data 'dat' files [ipayipi::cs_toa5], and data from the SAEON Terrestrial Observations Monitor [ipayipi::cs_stom]. Take a look at these as examples when setting up your own data setup options following the guidelines outlined in [imbibe_raw_flat()].
#'
#' ## 'Archiving' raw data
#'  Files brought into the 'wait_room' are only housed there temporarily. In order to archive these raw data files a 'raw_room' directory must be provided in the 'pipe_house' object (_see_ [ipip_house()]). Files will be archived in structured directories in the 'raw_room' named by the last year and month in their respective date time records. Original  file names are maintained, and have a suffix with a unique integer plus the date and time of which they were archived. N.B. Files in the source directory (`source_dir`) are only deleted when `wipe_source` is set to `TRUE`.
#'
#' ## Reading flat files
#' This function uses [data.table::fread] for flat text files, which is optimized for processing 'big data'. Apart from usual the usual options which can be parsed to `data.table::fread` this function generates some standardised metadata to complement the read from a logger data table (if `data.table::fread()` is unsuccessful `base::read.csv()` is used). This metadata may vary from one logger output to another. To cater for this variation this function requires a `data_setup` to be completed. Once setup this can be used as a standard for further imports.
#' There is support for 'xml' formatted Solonist 'xle' format files with a default `data_setup` that is parsed automatically for files with the '.xle' extension.
#'
#' ## Record interval
#' This function attempts to check whether the recording interval in the data date-time stamp has been consistent. A prompt is called if there are inconsistent time intervals between record events, and data rows with inconsistent time intervals will be removed if approved.
#'  A basic check is performed to check the success of converting date-time values to a recognised format in R (i.e., POSIXct).
#'
#' ## Logger interference events:
#' Regarding the `logg_interfere_type` parameter. Owing to potential interference of sensors etc when downloading data 'on site' or logger related issues when data is sent/obtained remotely, the date-time stamps of these events must be preserved. A `logg_interfere` data table is generated for this purpose and stored with the data. This data cannot necessarily be extracted from the 'data_summary' once data has been appended as some of this data will be overwritten during the appending process. The purpose of the 'logg_interfere' table is to retain this information, which is used by `ipayipi` for further processing.
#'
#' ## Parallel processing
#' This function can run considerably faster in parallel. ipayipi uses the [future] and [future.apply] libraries for parallel processing. __See__ [future::plan()] for setting up your parallel processing options that can be implemented in ipayipi.
#'
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
    "dmy HMOS", "dmy HMS",
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
    wanted = wanted, baros = TRUE
  )
  if (length(slist) == 0) {
    exit_m <- cli::cli_inform(c(
      "No '{file_ext_in}' files to imbibe in the pipeline {.var wait_room}.",
      "i" = "Pipe house 'wait_room' path: {pipe_house$wait_room}.",
      "i" = "Only flat unencrypted files can be processed."
    ))
    return(exit_m)
  }
  if (verbose || xtra_v) {
    cli::cli_h1("Reading and converting {length(slist)} file{?s}")
  }
  if (fcoff()) xtra_v <- FALSE
  file_name_dt <- future.apply::future_lapply(seq_along(slist), function(i) {
    if (verbose || xtra_v) cli::cli_h2(c(
      " " = "{i}: working on {slist[i]}"
    ))
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
      suppressWarnings((fl$err <- TRUE))
    }
    # save as tmp rds if no error
    if (!fl$err) {
      fn_htmp <- tempfile(pattern = "raw_",
        tmpdir = file.path(tempdir(), "wait_room_tmp")
      )
      # catch for incorrect time import formula
      st_dt <- attempt::attempt(min(fl$ipayipi_data_raw$raw_data$date_time))
      if (attempt::is_try_error(st_dt)) {
        cli::cli_inform(c("!" = "Problem with date-time format read:",
          "i" = "Check raw data and {.var dt_format} function arguments!",
          " " = paste0("Your date-time e.g.:",
            " {fl$ipayipi_data_raw$raw_data$date_time[1]}"
          ),
          "i" = paste0("If ipayipi is reading the incorrect column for info ",
            "then check the {.var data_setup} argument."
          )
        ))
        # return error table
        return(data.table::data.table(
          err = TRUE,
          fn_htmp = fn_htmp,
          fp = fp,
          fn = fn,
          st_dt = NULL,
          ed_dt = NULL,
          file_ext_in = file_ext_in
        ))
      }
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
      if (xtra_v) cli::cli_inform(c(">" = "saved as {fn_htmp}"))
    } else {
      fn_htmp <- NA_character_
      fn <- NA_character_
    }
    fn_resolve <- data.table::data.table(
      err = fl$err,
      fn_htmp = fn_htmp,
      fp = fp,
      fn = fn,
      st_dt = if (exists("st_dt")) st_dt else as.POSIXct(NA_character_),
      ed_dt = if (exists("ed_dt")) ed_dt else as.POSIXct(NA_character_),
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

  # requires non-null raw_room
  if (!is.null(pipe_house$raw_room)) {
    arc_dir <- file.path(pipe_house$raw_room, fn_dt_arc$yr_mon_end)
    dir.create(!arc_dir[file.exists(arc_dir)])
    file.copy(from = file.path(fn_dt_arc$fp), to = file.path(arc_dir,
      paste0(vgsub(
        pattern = fn_dt_arc$file_ext_in, replacement = "", x = fn_dt_arc$ofn
      ), "_arcdttm_", as.character(format(Sys.time(), "%Y%m%d_%Hh%Mm%Ss")
      ), fn_dt_arc$file_ext_in, collapse = ""
      )
    ))
  }

  # check for duplicates and make unique integers
  if (!anyNA.data.frame(fn_dt_arc) && nrow(fn_dt_arc) != 0) {
    split_fn_dt_arc <- split(fn_dt_arc, f = factor(fn_dt_arc$fn))
    split_fn_dt_arc <- lapply(split_fn_dt_arc, function(x) {
      x$rep <- seq_len(nrow(x))
      return(x)
    })
    fn_dt_arc <- data.table::rbindlist(split_fn_dt_arc)
    if (substr(file_ext_out, 1, 1) != ".") {
      file_ext_out <- paste0(".", file_ext_out)
    }
    # rename the temp rds files and delete the raw dat files
    tex <- fn_dt_arc$fn_htmp
    ex <- file.path(
      pipe_house$wait_room,
      paste0(fn_dt_arc$fn, "__", fn_dt_arc$rep, file_ext_out)
    )
    fp <- fn_dt_arc$fp
    file.copy(tex[file.exists(tex)], ex[file.exists(tex)])
    unlink(tex[file.exists(tex)], recursive = TRUE)
    file.remove(fp[file.exists(fp)])
    if (wipe_source) {
      fr <- gsub(paste0("__*.", fn_dt_arc$file_ext_in, "$"),
        fn_dt_arc$file_ext_in, fn_dt_arc$ofn
      )
      file.remove(fr[file.exists(fr)])
    }
  }
  if (verbose || xtra_v) cli::cli_h1("")
  e <- nrow(file_name_dt[err == TRUE])
  if (e > 0) {
    cli::cli_inform(c(
      "{e} of {length(slist)} file{?s} below could not be processed.",
      "i" = "Ensure th{?is/ese} {e} file{?s} are readable flat files;",
      " " = "to ignore files include these search keys in {.var unwanted},",
      "i" = "date-time values and formats must be readable, and",
      "i" = "appropriate {.var data_setup} options must be provided."
    ))
    print(file_name_dt[err == TRUE])
  }
  if (verbose || xtra_v && nrow(file_name_dt[err == FALSE]) > 0) {
    cli::cli_inform(c(
      "What next?",
      "*" = paste0("Imbibed files are now in \'R\' format and are renamed with",
        " the extension \'.ipr\'."
      ),
      "v" = "\'.ipr\' files are ready for standardisation.",
      ">" = "Begin file standardisation using functions:",
      " " = "1) {.var header_sts()}, and next,",
      " " = "2) {.var phenomena_sts()}."
    ))
  }
  invisible(fn_dt_arc)
}