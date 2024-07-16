#' @title Batch processing and standardisation of hobo rainfall exports
#' @description Converts a batch of hobo rainfall file exports to R format
#'  then performs some standardisastion of the data and metadata. Some initial
#'  data cleaning operations are also performed by
#'  `ipayipi::rain_hobo_clean()`. *Data can also be imported from
#'  `ipayipi::rain_master()`.
#' @details Once data has been imported into the 'nomvet_room' using
#'  `ipayipi::rain_hobo_import()`, `ipayipi::rain_hobo_conversion()` checks
#'  the naming conventions of the files being imported against a nomenclature
#'  table. This is a critical step before going ahead with appending data as
#'  the naming standards determine which data stations have their data appended
#'  into a continuous series. To get this right the nomenclature is verified
#'  using `ipayipi::rain_nomenclature()` whereby a specific data stream
#'  (or station) is identified by the combination of unique *import file names*
#'  (location and station), *logger serial numbers*, and the *'plot title'*
#'  which is specified in Hoboware (or alternatively provided by the user when
#'  using the `ipayipi::rain_master()` import functionality). If there is
#'  incomplete nomenclature data the function will not continue until this
#'  information (`ipayipi::rain_read_nomtab_csv()` can be used to do this)
#'  has been updated. In the process the 'nomtab.rds' file, that is, the
#'  standardised nomenclature table, will be updated in the 'wait_room'.
#'  *Master file* input mode: data can be imported into this function from
#'  `ipayipi::rain_master()`. The following options must be set for reading
#'  files using this functionality.
#'  - `mf_list` This parameter must be supplied with an output from
#'   `ipayipi::rain_master()`.
#'  **Note that when using this function in "mf" mode only one station can be
#'  processed at a time.**
#' @param wait_room The working directory where the hobo csv files are
#'  temporarily stored. This is also where the pipelines nomenclature table
#'  is stored and used for associated standardisation.
#' @param out_csv Logical. If `TRUE` a csv file is made in the working
#'  directory if there are files with unrecognised nomenclature.
#' @param input_format Currently only ".csv" is supported. *Note that the period
#'  (**"."**) must be included in the extension*.
#' @param false_tip_thresh Numeric field; units in seconds. The time buffer
#'  in seconds around logger interferance events (e.g., logger connections/
#'  downloads) in which any tips (logs) should be considered false. The
#'  default used by SAEON is ten minutes, that is, 10x60=600 seconds.
#' @param tip_value Numeric field; units in millimeters. The amount of
#'  rainfall (in mm) a single tip of the tipping bucket represents. This
#'  defaults so 0.254 mm.
#' @param dt_format The datetime format of the csv (or other) file. Note
#'  that **for batch processing all files being read must have the same
#'  datetime format**. The default used is "%x %r" --- check the
#'  `base::strptime` function for a list of available formats.
#' @param tz Timezone format of the datetime values. For South Africa we use
#'  the character string "Africa/Johannesburg" or "Africa/Johannesburg" for
#'  short.
#' @param temp Has the hobo logger been recording temperature? Logical
#'  variable. Defaults to `FALSE`.
#' @param instrument_type The type of instrument wherein the hobo logger was
#'  deployed. For the commonly used *Texus tipping-bucket rainguage* use the
#'  code "txrg".
#' @param mf_list Output from `ipayipi::rain_master()` if reading rainfall
#'  files from a 'master file'. If an object is supplied here then files
#'  are automatically processed in the "*mf*" mode.
#' @keywords data standardization; data pipeline preparation; nomenclature;
#'  batch processing
#' @return If the nomenclature table was updated successfully this function
#'  will return a list containing the updated table, a list of successfully
#'  converted hobo rainfall files, and a list of failed conversions.
#' @author Paul J. Gordijn
#' @export
rain_hobo_conversion_batch <- function(
  wait_room = NULL,
  out_csv = TRUE,
  input_format = ".csv",
  false_tip_thresh = 10 * 60,
  tip_value = 0.254,
  dt_format = c(
    "Ymd HMOS", "Ymd HMS",
    "Ymd IMOSp", "Ymd IMSp",
    "ymd HMOS", "ymd HMS",
    "ymd IMOSp", "ymd IMSp",
    "mdY HMOS", "mdY HMS",
    "mdy HMOS",  "mdy HMS",
    "mdy IMOSp",  "mdy IMSp",
    "dmY HMOS", "dmY HMS",
    "dmy IMOSp", "dmy IMSp"),
  tz = "Africa/Johannesburg",
  temp = FALSE,
  instrument_type = NA,
  mf_list = NA,
  ...) {
  # get list of hobo files in the 'wait_room'
  if (!any(is.na(mf_list))) {
    slist <- mf_list$dwn_list
  } else {
    slist <- ipayipi::dta_list(input_dir = wait_room,
      file_ext = input_format, prompt = FALSE)
  }

  cr_msg <- ipayipi::padr(core_message =
    paste0(" Attempting to read hobo files ... ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)

  ccat <- lapply(slist, function(x) {
    # read the hobo file
    if (any(is.na(mf_list))) {
      input_file_here  <- file.path(wait_room, x)
      input_file_name <- x
      ptitle <- NA
    }
    if (!any(is.na(mf_list))) {
      input_file_here  <- x
      input_format <- "mf"
      ptitle <- mf_list$ptitle
      input_file_name <- mf_list$m_file
    }
    hbf <- rain_hobo_clean(
      input_file = input_file_here,
      input_format = input_format,
      false_tip_thresh = false_tip_thresh,
      tip_value = tip_value,
      dt_format = dt_format,
      tz = tz,
      temp = temp,
      instrument_type = instrument_type,
      input_file_name = input_file_name,
      ptitle = ptitle)
    if (class(hbf) == "SAEON_rainfall_data") {
      nomtabi <- data.table::data.table(
        uz_import_file_name = as.character(
          hbf[["data_summary"]]$import_file_name),
        uz_location_station = as.character(basename(
            substr(x = hbf[["data_summary"]]$import_file_name,
              start = 1,
              stop = nchar(hbf[["data_summary"]]$import_file_name)))),
        uz_title = as.character(hbf[["data_summary"]]$ptitle_original),
        location = as.character(NA),
        station = as.character(NA),
        stnd_title = as.character(hbf[["data_summary"]]$ptitle_standard),
        instrument = as.character(instrument_type),
        logger_sn = as.character(hbf[["data_summary"]]$logger_sn),
        start_dt = as.POSIXct(hbf[["data_summary"]]$start_dt),
        end_dt = as.POSIXct(hbf[["data_summary"]]$end_dt)
      )
      hbf_pkg <- list(hbf, nomtabi)
      cr_msg <- NULL
    } else {
      nomtabi <- data.table::data.table(
        uz_import_file_name = as.character(x),
        uz_location_station = as.character(basename(
            substr(x = hbf[["data_summary"]]$import_file_name,
              start = 1,
              stop = nchar(hbf[["data_summary"]]$import_file_name)))),
        uz_title = as.character(NA),
        location = as.character(NA),
        station = as.character(NA),
        stnd_title = as.character(NA),
        instrument = as.character(instrument_type),
        logger_sn = as.character(NA),
        start_dt = as.POSIXct(NA),
        end_dt = as.POSIXct(NA)
      )
      cr_msg <- ipayipi::padr(core_message =
        paste0(" Failed:  ", input_file_name, collapes = ""),
          wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
          force_extras = FALSE, justf = c(1, 1))
      hbf_pkg <- list(hbf, nomtabi)
    }
    if (!is.null(cr_msg)) message(cr_msg)
    return(hbf_pkg)
  })
  ccata <- lapply(ccat, function(x) return(x[[1]]))
  ccatb <- lapply(ccat, function(x) return(x[[2]]))
  nomtabi <- data.table::rbindlist(ccatb)
  nomtabi[, ":="(start_dt = min(start_dt), end_dt = max(end_dt)),
    by = .(uz_location_station, location, station)]
  # nomenclature checks and then saving the files
  nomtabo <- ipayipi::rain_nomenclature(wait_room = wait_room,
    nomtab_import = nomtabi, check_nomenclature = TRUE, csv_out = TRUE)
  # if the nomenclature is sorted then import standardised fields into the
  #  each "SAEON_rainfall_data" file
  if (is.na(nomtabo[[2]])) {
    qcat <- lapply(seq_along(ccatb), function(x) {
      if (class(ccata[[x]]) == "SAEON_rainfall_data") {
        org_name <- ccatb[[x]]$uz_location_station
        lgsn <- ccatb[[x]]$logger_sn
        tcat <- ccata[[x]]
        tcat[["data_summary"]]$ptitle_standard <-
          nomtabo[[1]][uz_location_station == org_name &
            logger_sn == lgsn]$stnd_title[1]
        tcat[["data_summary"]]$location <-
          nomtabo[[1]][uz_location_station == org_name &
            logger_sn == lgsn]$location[1]
        tcat[["data_summary"]]$station <-
          nomtabo[[1]][uz_location_station == org_name &
            logger_sn == lgsn]$station[1]
        tcat[["data_summary"]]$instrument_type <-
          nomtabo[[1]][uz_location_station == org_name &
            logger_sn == lgsn]$instrument[1]
      }
      return(tcat)
    })
  } else stop("Try again after updating nomenclature")
  # produce output --- need list of fails, successes and nomtab update
  # failures
  hobo_fails <- ccata[sapply(ccata, class) == "SAEON_rainfall_data_fail"]
  hobo_converts <- qcat[sapply(qcat, class) == "SAEON_rainfall_data"]

  rain_hobo_converted <- list(
    hobo_converts = hobo_converts,
    hobo_fails = hobo_fails,
    nomtabo = nomtabo)
  names(rain_hobo_converted) <- c("hobo_converts", "hobo_fails", "nomtabo")
  class(rain_hobo_converted) <- "converted_rain_hobo"
  cr_msg <- ipayipi::padr(core_message =
    paste0(" Finished ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  return(rain_hobo_converted)
}