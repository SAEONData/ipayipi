#' @title Read data handling values
#' @description Read in manually entered values which are used for correcting
#'  anomalous data or removing outliers. The data handle information is
#'  appended to the standardised data files and saved.
#' @param input_dir The working directory where the standardised 'ipayipi'
#'  water level files are located.
#' @param recurr Should the function work recurrsively in the parent
#'  directory.
#' @param wanted Vector of strings listing files that should not be
#'  included in the import.
#' @param unwanted Vector of strings listing files that should not be included
#'  in the import.
#' @param prompt If TRUE, a command line prompt will be used to
#'  enable selection of which files in the working directory require
#'  drift correction.
#' @keywords data cleaning, anomaly removal, outliers, prior-informed outliers
#' @author Paul J. Gordijn
#' @return Saves data with a table of manually declared outliers. List of station names of processed data.
#' @export
gw_data_handler <- function(
  input_dir = ".",
  wanted = NULL,
  unwanted = FALSE,
  prompt = FALSE,
  recurr = FALSE,
  ...) {
  if (file.exists(file.path(input_dir, "data_handle.rdhs"))) {
    dh <- readRDS(file.path(input_dir, "data_handle.rdhs"))
  } else {
    stop("There is no data handle table in the input directory!")
  }
  inx <- ipayipi::dta_list(input_dir = input_dir, file_ext = ".rds",
    prompt = prompt, recurr = recurr, baros = FALSE, unwanted = unwanted,
    wanted = wanted)
  dh$stations <- paste0(dh$location, "_", dh$station, ".rds")
  stations <- c(dh$stations,
    gsub(pattern = ".rds", replacement = "__ltc.rds", dh$stations))
  stations <- unique(stations[stations %in% inx])
  stations <- stations[order(stations)]
  cr_msg <- padr(core_message = " updating data handle info ",
    pad_char = "=", pad_extras = c("|", "", "", "|"), force_extras = FALSE,
    justf = c(0, 0), wdth = 80)
  message(cr_msg)
  gw_names <- lapply(stations, function(x) {
    gw <- readRDS(file.path(input_dir, x))
    gw_name <- gsub(pattern = ".rds", replacement = "", x = x)
    cr_msg <- padr(core_message = gw_name, pad_char = " ",
      pad_extras = c("|", "", "", "|"), force_extras = FALSE,
      justf = c(1, 1), wdth = 80)
    message(cr_msg)
    if (x %ilike% "__ltc.rds") {
      x_ltc <- gsub(pattern = "__ltc.rds", replacement = ".rds", x)
    } else {
      x_ltc <- x
    }
    dh_subset <- dh[stations == x_ltc & qa == TRUE]
    dh_subset <- subset(dh_subset, select = c(did, handle, date_time1,
      date_time2, notes))
    gw$dummies <- dh_subset[handle %in% c("dummy_drift", "dummy_wl")][
      date_time1 >= gw$log_t$Date_time[1]][
        date_time1 <= gw$log_t$Date_time[nrow(gw$log_t)]
      ]
    gw$log_retrieve <- gw$log_retrieve[!Date_time %in% gw$dummies$date_time1]
    man_out <- dh_subset[handle %in% c("manual_outliers")]
    man_out <- subset(man_out, select = c(date_time1, date_time2, notes))
    names(man_out) <- c("Start", "End", "Notes")
    gw$log_t_man_out <- man_out
    saveRDS(gw, file = file.path(input_dir, x))
    invisible(gw_name) # this is the right copy
  })
  cr_msg <- padr(core_message = "",
    pad_char = "=", pad_extras = c("|", "", "", "|"), force_extras = FALSE,
    justf = c(0, 0), wdth = 80)
  message(cr_msg)
  invisible(gw_names)
}
