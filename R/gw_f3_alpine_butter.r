#' @title Generate dummy dipper readings
#' @description Generates dummy dipper readings using linear interpolation and
#'  adds these to the 'log_retreive' table. These will be used for in subsequent
#'  drift correction.
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
#' @keywords
#' @author Paul J. Gordijn
#' @return
#' @export
gw_alpine_butter <- function(
  input_dir = ".",
  wanted = NULL,
  unwanted = FALSE,
  prompt = FALSE,
  recurr = FALSE,
  ...) {
  inx <- ipayipi::dta_list(input_dir = input_dir, file_ext = ".rds",
    prompt = prompt, recurr = recurr, baros = FALSE, unwanted = unwanted,
    wanted = wanted)
  cr_msg <- padr(core_message = " generating dummy values ",
    pad_char = "=", pad_extras = c("|", "", "", "|"), force_extras = FALSE,
    justf = c(0, 0), wdth = 80)
  message(cr_msg)
  inx <- lapply(inx, function(x) {
    gw <- readRDS(file.path(input_dir, x))
    if ("dummies" %in% names(gw)) {
      if (nrow(gw$dummies[handle == "dummy_drift"]) > 0) {
        gw_name  <- x
      } else gw_name <- NA
    } else gw_name <- NA
    invisible(gw_name)
  })
  inx <- unlist(inx)
  inx <- inx[!is.na(inx)]
  gw_names <- lapply(inx, function(x) {
    gw <- readRDS(file.path(input_dir, x))
    gw_name <- gsub(pattern = ".rds", replacement = "", x = x)
    cr_msg <- padr(core_message = gw_name, pad_char = " ",
      pad_extras = c("|", "", "", "|"), force_extras = FALSE,
      justf = c(1, 1), wdth = 80)
    message(cr_msg)
    tie_tbl <- gw$dummies[handle == "dummy_drift"]
    tie_tbl <- tie_tbl[!date_time1 < gw$log_t$Date_time[1] |
      !date_time2 > gw$log_t$Date_time[1]]
    ctbl <- lapply(seq_len(nrow(tie_tbl)), function(z) {
      ctbl <- ipayipi::gw_ties(file = gw, tie_type = "dummy_drift",
        tie_datetime = as.POSIXct(as.character(tie_tbl$date_time1[z])))
      invisible(ctbl)
    })
    ctbl <- data.table::rbindlist(ctbl)
    gw$log_retrieve <- gw$log_retrieve[
      !Date_time %in% as.POSIXct(as.character(ctbl$Date_time))]
    gw$log_retrieve <- rbind(gw$log_retrieve, ctbl)[order(Date_time)]
    saveRDS(gw, file = file.path(input_dir, x))
  })
  cr_msg <- padr(core_message = "", pad_char = "=",
    pad_extras = c("|", "", "", "|"), force_extras = FALSE,
    justf = c(1, 1), wdth = 80)
  message(cr_msg)
}