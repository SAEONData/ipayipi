#' @title Imbibes logger data exports
#' @md
#' @description Function to read in 'flat' loggers files into R. A first step towards processing data in `ipayipi`.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param file_path Path and name of file (excluding the file extension).
#' @param file_ext The file extension defaults of the raw logger data files. This can be left as `NULL` so 'all' files but those with extensions that cannot be imbibed (".ipr|.ipi|.iph|.xls|.rps|.rns|.ods|.doc").
#' @param col_dlm The column delimter which is fed to `data.table::fread()`. Defaults to NULL. When `NULL` the function uses `data.table::fread` ability to 'guess' the delimeter.
#' @param dt_format The function attempts to work out the date-time format from a vector of format types supplied to this argument. The testing is done via `lubridate::parse_date_time()`. `lubridate::parse_date_time()` prioritizes the tesing of date-time formats in the order vector of formats supplied. The default vector of date-time formats supplied should work well for most logger outputs.
#' @param dt_tz Recognized time-zone (character string) of the data locale. The default for the package is South African, i.e., "Africa/Johannesburg" which is equivalent to "SAST".
#' @param record_interval_type If there are is no discrete record interval set in the logger program, i.e., the sampling is event-based, then this parameter must be set to "event_based". By default this function has this parameter set to "continuous", but the record interval is scrutinized by 'ipayipi::record_interval_eval()' --- see the help files for this function for more information.
#'  The parameter supplied here is only used if there is only one data record and the record interval cannot be evaluated by `ipayipi::record_interval_eval()`.
#' @param data_setup List of options used to extract data and metadata from instrument data outputs. Mandatory fields are indicated with an '*'.
#'   File header options include\eqn{*^1}:
#'   1. *__file_format__ -- the native/raw file format.
#'   1. *__station_title__ -- the supplied instrument station title.
#'   1. location -- the standardised location (name) of the station.
#'   1. *__logger_type__ -- the type of logger.
#'   1. *__logger_sn__ -- the serial number of the logger.
#'   1. logger_os -- the operating system (or firmware version) on the logger.
#'   1. logger_program_name -- the name of the program installed on the logger
#'       (also 'DLD name' on Cambel Scientific systems).
#'   1. logger_programe_sig -- signature of the logger program (also 'DLD
#'       signature' on Cambel Scientific systems).
#'   1. logger_title -- the custom name given to a logger by the programmer.
#'   1. table_name -- the generic name of the table containing data.
#'
#'   \eqn{*^1} These options must be supplied as a charater string or the row and column index provided as for example, rici(ri = 1, ci = 2). See `?ipayipi::rici()` for more details.
#'
#'   1. date_time -- Only the column index must be provided here as an integer,
#'      e.g., 3.
#'
#'  File phenomena information and data \eqn{*^2}:
#'   1. *__phen_name__ -- a list of row and column numbers corresponding to the names of phenomena (variables).
#'   1. phen_unit -- a list of row and column numbers corresponding to the names of phenomena units.
#'   1. phen_var_type -- a vector of character strings designating the type of variable for each phenomenon.
#'   1. phen_measure -- a list of row and column numbers corresponding to the type of measurement calculated by the logger for each phenomena, e.g., an 'average', 'sample', 'minimum', etc.
#'   1. phen_offset -- a list of offset values that have been pre-applied to the data, i.e, the offset is only noted and not used to transform the data.
#'   1. sensor_id -- a list of row and column numbers corresponding to sensor unique id values. Otherwise a vector of character strings designating the type of 'sensor_id' for each phenomenon.
#'
#'  \eqn{*^2} These options must be supplied as using `ipayipi::rng_rici()`, or input as a vector of character strings with the actual values. If using     `ipayipi::rng_rici()`, at least the row in which phenomena details are found and the columns wherein these lie are required. See ?ipayipi::rng_rici() for more details.
#'
#'   1. *__data_row__ -- a single integer value designating the row where phenomena data begin from.
#'   1. *__id_col__ -- a single integer value designating a data row unique identifier row.
#' 
#' NB! Note that for Solonist xle files there is a prebuilt 'data_setup' `ipayipi::solonist`. If the `file_path` is for an 'xle' file, and `data_setup` is null, then this default data_setup will be used.
#' 
#' @param remove_prompt Logical; passed to `ipayipi::record_interval_eval()`. Activate a readline prompt to choose whether or not filter our records from `dta_in` with inconsistent record intervals.
#' @param logg_interfere_type Two options here: "remote" or "on_site". Each time a logger is visited is counted as a logger interference event. Type _'remote'_ occurs when data is downloaded remotely. Type _'on_site'_ is when data was downloaded on site. _See details_ ...
#' @param verbose Logical passed to `attempt::attempt()` which reads the logger text file in with either `data.table::fread()` or base R. Also whether to print progress.
#' @details This function uses `data.table::fread` for flat text files, which is optimized for processing 'big data'. Apart from usual the usual options which can be parsed to `data.table::fread` this function generates some standardised metadata to complement the read from a logger data table (if `data.table::fread()` is unsuccessful `base::read.csv()` is used). This metadata may vary from one logger output to another. To cater for this variation this function requires a `data_setup` to be completed. Once setup this can be used as a standard for further imports.
#' There is support for 'xml' formatted Solonist 'xle' format files with a default `data_setup` that is parsed automatically for files with the '.xle' extension.
#' 
#'  This function attempts to check whether the recording interval in the data date-time stamp has been consistent. A prompt is called if there are inconsistent time intervals between record events, and data rows with inconsistent time intervals will be removed if approved.
#'  A basic check is performed to check the success of converting date-time values to a recognised format in R (i.e., POSIXct).
#'  Regarding the `logg_interfere_type` parameter. Owing to potential interference of sensors etc when downloading data 'on site' or logger related issues when data is sent/obtained remotely, the date-time stamps of these events must be preserved. A `logg_interfere` data table is generated for this purpose and stored with the data. This data cannot necessarily be extracted from the 'data_summary' once data has been appended as some of this data will be overwritten during the appending process. The purpose of the 'logg_interfere' table is to retain this information, which is used by `ipayipi` for further processing.
#' @return A list of class "ipayipi_raw_data" that contains a 'data_summary', 'phens' (phenomena), and 'raw_data' tables (data.table).
#' @export
#' @author Paul J. Gordijn
imbibe_raw_logger_dt <- function(
  pipe_house = NULL,
  file_path = NULL,
  file_ext = NULL,
  col_dlm = NULL,
  dt_format = c(
    "Ymd HMOS", "Ymd HMS",
    "Ymd IMOSp", "Ymd IMSp",
    "ymd HMOS", "ymd HMS",
    "ymd IMOSp", "ymd IMSp",
    "mdY HMOS", "mdY HMS",
    "mdy HMOS", "mdy HMS",
    "mdy IMOSp", "mdy IMSp",
    "dmY HMOS", "dmY HMS",
    "dmy IMOSp", "dmy IMSp"
  ),
  dt_tz = "Africa/Johannesburg",
  record_interval_type = "continuous",
  max_rows = 1000,
  data_setup = NULL,
  remove_prompt = FALSE,
  logg_interfere_type = "on_site",
  verbose = TRUE,
  xtra_v = FALSE,
  ...
) {
  "%ilike%" <- NULL
  if (is.null(file_ext)) {
    file_ext <- tools::file_ext(file_path)
    file_ext <- paste0("\\.", sub(pattern = "\\.", replacement = "", file_ext))
  }
  if (is.null(col_dlm) && !is.null(file_ext)) {
    fx <- file_ext
    col_dlm <- ipayipi::file_read_meta[file_ext %ilike% fx]$sep[1]
  }

  # read formats flat files vs xml
  if (!file_ext %ilike% "xml$|xle$") {
    if (is.null(data_setup)) data_setup <- ipayipi::solonist
    boolf <- FALSE
    dsi <- 1
    while (boolf == FALSE && dsi <= length(data_setup)) {
      ipayipi::msg(paste0("Attempting \'data_setup\' ", dsi), xtra_v)
      dta_ex <- attempt::try_catch(expr = ipayipi::imbibe_raw_flat(
        file_path = file_path, file_ext = file_ext, col_dlm = col_dlm,
        dt_format = dt_format, dt_tz = dt_tz, data_setup = data_setup[[dsi]],
        verbose = verbose
      ))
      boolf <- !dta_ex$err
      ipayipi::msg(paste0("\'data_setup\' ", dsi, " success!"),
        all(xtra_v, boolf)
      )
      dsi <- dsi + 1
      ipayipi::msg("No successful \'data_setup\' try...",
        all(dsi > length(data_setup), boolf == FALSE, xtra_v)
      )
    }
  } else {
    dta_ex <- ipayipi::imbibe_xml(
      file_path = file_path, dt_format = dt_format, dt_tz = dt_tz,
      data_setup = data_setup, verbose = verbose
    )
  }

  data_summary <- dta_ex$ipayipi_data_raw$data_summary
  phens <- dta_ex$ipayipi_data_raw$phens
  dta <- dta_ex$ipayipi_data_raw$raw_data

  # determine record interval - function run twice to account for
  # FALSE intervals at position one and two of the data
  dri <- ipayipi::record_interval_eval(dt = dta$date_time, dt_format =
      dt_format, dt_tz = dt_tz, dta_in = dta, remove_prompt = remove_prompt,
    record_interval_type = record_interval_type, max_rows = max_rows
  )
  dta <- dri$new_data
  if (remove_prompt) {
    dri <- ipayipi::record_interval_eval(dt = dta$date_time, dt_format =
        dt_format, dt_tz = dt_tz, dta_in = dta, remove_prompt = remove_prompt,
      record_interval_type = record_interval_type, max_rows = max_rows
    )
    dta <- dri$new_data
  }
  data_summary$uz_record_interval_type <- dri$record_interval_type
  data_summary$uz_record_interval <- dri$record_interval

  # generate a logger interference table
  logg_interfere <- data.table::data.table(
    id = as.integer(seq_len(2)),
    date_time = c(data_summary$start_dttm, data_summary$end_dttm),
    logg_interfere_type = as.character(rep(logg_interfere_type, 2))
  )
  # generate a phenomena by data summary table
  phen_data_summary <- data.table::data.table(
    phid = as.integer(phens$phid),
    # removed dsid as it takes up too many rows and slows joins when
    #  generating phenomena data summaries
    # dsid = as.integer(rep(as.integer(data_summary$dsid),
    #    nrow(phens))),
    start_dttm = rep(data_summary$start_dttm, nrow(phens)),
    end_dttm = rep(data_summary$end_dttm, nrow(phens)),
    table_name = NA_character_
  )
  #}
  ipayipi_data_raw <- list(data_summary = data_summary, raw_data = dta,
    phens = phens, phen_data_summary = phen_data_summary,
    logg_interfere = logg_interfere
  )
  class(ipayipi_data_raw) <- "ipayipi_raw_data"
  return(list(ipayipi_data_raw = ipayipi_data_raw, err = FALSE))
}