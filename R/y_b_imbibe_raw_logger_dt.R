#' @title Imbibes logger data exports
#' @md
#' @description Function to read in 'flat' loggers files into R. A first step towards processing data in `ipayipi`.
#' @inheritParams imbibe_raw_batch
#' @param file_path Path and name of file (excluding the file extension).
#' @param file_ext The file extension defaults of the raw logger data files. This can be left as `NULL` so 'all' files but those with extensions that cannot be imbibed (".ipr|.ipi|.iph|.xls|.rps|.rns|.ods|.doc").
#'
#' @return A list of class "ipayipi_raw_data" that contains a 'data_summary', 'phens' (phenomena), and 'raw_data' tables (data.table).
#' @details __See__ [imbibe_raw_batch()].
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
    "dmy HMOS", "dmy HMS",
    "dmy IMOSp", "dmy IMSp"
  ),
  dt_tz = "Africa/Johannesburg",
  record_interval_type = "continuous",
  max_rows = 1000,
  data_setup = NULL,
  remove_prompt = FALSE,
  logg_interfere_type = "on_site",
  verbose = FALSE,
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
    dyno_read_f <- "dyno_read"
    while (boolf == FALSE && dsi <= length(data_setup)) {
      if (xtra_v) cli::cli_h3(c("i" = "Attempting \'data_setup\': {dsi}"))
      dta_ex <- attempt::try_catch(expr = ipayipi::imbibe_raw_flat(
        file_path = file_path, file_ext = file_ext, col_dlm = col_dlm,
        dt_format = dt_format, dt_tz = dt_tz, data_setup = data_setup[[dsi]],
        dyno_read_f = dyno_read_f, dsi = dsi,
        verbose = verbose, xtra_v = xtra_v
      ))
      boolf <- !dta_ex$err
      if (all(xtra_v, boolf)) cli::cli_alert(
        c("v" = "\'data_setup\' {dsi} success!")
      )
      dsi <- dsi + 1
      if (dsi > length(data_setup) && dyno_read_f %in% "dyno_read") {
        dyno_read_f <- "dyno_read2"
        if (xtra_v) {
          cli::cli_inform(c(
            "i" = "relaxing data header read conditions with {.var dyno_read2}."
          ))
        }
        dsi <- 1
      }
      if (all(dsi > length(data_setup) * 2, boolf == FALSE, xtra_v)) {
        cli::cli_inform(c(
          "!" = "0 out of {length(data_setup)} \'data_setup\'(s) successful"
        ))
      }
    }
    dsi <- dta_ex$dsi
    if (!dta_ex$err) {
      dta_ex <- ipayipi::imbibe_raw_flat(
        file_path = file_path, file_ext = file_ext, col_dlm = col_dlm,
        dt_format = dt_format, dt_tz = dt_tz, data_setup = data_setup[[dsi]],
        dyno_read_f = dyno_read_f, dsi = dsi, nrows = Inf,
        verbose = verbose, xtra_v = xtra_v
      )
    } else {
      return(list(ipayipi_data_raw = dta_ex$ipayipi_data_raw, err = TRUE))
    }
  } else {
    dta_ex <- ipayipi::imbibe_xml(
      file_path = file_path, dt_format = dt_format, dt_tz = dt_tz,
      data_setup = data_setup, verbose = verbose, xtra_v
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
  data_summary$logg_interfere <- logg_interfere_type

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
  ipayipi_data_raw <- list(data_summary = data_summary, raw_data = dta,
    phens = phens, phen_data_summary = phen_data_summary,
    logg_interfere = logg_interfere
  )
  class(ipayipi_data_raw) <- "ipayipi_raw_data"
  return(list(ipayipi_data_raw = ipayipi_data_raw, err = FALSE))
}