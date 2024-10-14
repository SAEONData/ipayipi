#' @title Batch compensate function
#' @description This function feeds the gw_baro_comp function
#'  files for processing. The function requires a pregenerated
#'  rdta_log has been generated from the associated data pipeline.
#' @param input_dir Folder where to get R solonist data files.
#' @param out_csv Logical. Defaulted to TRUE.
#'  If there is no baro file specified in the rdta_log this
#'  will spit out a csv that can be edited.
#' @param overwrite_baro Logical. Defaulted to TRUE.
#'  This option is described in the "gw_baro_comp" function.
#' @param overwrite_comp Logical. Defaulted to TRUE.
#'  This option is described in the "gw_baro_comp" function.
#' @param start_t This option is described in the "gw_baro_comp" function.
#' @param end_t This option is described in the "gw_baro_comp" function.
#' @param join_tolerance Join tolerance between the barometric
#'  and level data (for an overlap join).
#' @param na_thresh Feeder to the gw_baro_comp function.
#' @param overwrite_t_bt_level Need to populate detail here.
#' @param prompted Logical and defaults to FALSE.
#'  If TRUE, a command line dialog will be used to
#'  select data files for compensation.
#' @param wanted Vector of strings listing files that should not be
#'  included in the import.
#' @param unwanted Vector of strings listing files that should not be included
#'  in the import.
#' @param recurr Logical. Argument fed to the prompted component of the
#'   rsol_list function.
#' @keywords barometric compensation water level
#' @return Screen print out of all the level logger data that has been
#'  barometrically compensated in this function call.
#' @author Paul J. Gordijn
#' @export
#'
gw_baro_comp_batch <- function(
  input_dir = NULL,
  out_csv = TRUE,
  overwrite_baro = TRUE,
  overwrite_comp = TRUE,
  start_t = NULL,
  end_t = NULL,
  join_tolerance = NULL,
  na_thresh = 5,
  overwrite_t_bt_level = FALSE,
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  recurr = NULL
) {

  # Search the input directory and get a list of the files to compensate.
  #  This is done by reading in the rdta_log file.
  # Check if there is an rdta_log in the directory, otherwise
  #  generate one.
  if (file.exists(file.path(input_dir, "rdta_log.rds"))) {
    rdta_log_file <- readRDS(file.path(input_dir, "rdta_log.rds"))
  } else {
    rdta_log_file <- ipayipi::gw_rdta_log_mk(input_dir = input_dir,
      recurr = FALSE
    )
  }

  # Check that there are barologger and level logger files
  #    that have been brought into the pipeline directory
  if (is.null(rdta_log_file)) {
    message("Check if there are files to process in the working directory!")
    stop("There are no files listed in the rdta_log for processing.")
  }

  # Filter the rdta_log_file of the baro files
  #    which don't need to be compensated.
  rdta_log_file_filt <- rdta_log_file[
    rdta_log_file$Rfile_class == "level_file",
  ]
  input_file_list <- ipayipi::dta_list(input_dir = input_dir, recurr = recurr,
    prompt = prompt, baros = FALSE, wanted = wanted, unwanted = unwanted,
    file_ext = ".rds"
  )
  if (length(input_file_list) == 0) {
    stop("There are no such water level files in this directory!")
  }
  input_file_list <- gsub(pattern = ".rds", replacement = "", input_file_list)
  input_file_list <- intersect(input_file_list, input_file_list)
  rdta_log_file_filt$level_name <-
    paste0(rdta_log_file_filt$Location, "_", rdta_log_file_filt$Borehole)
  rdta_log_file_filt <-
    rdta_log_file_filt[level_name %in% input_file_list, ]

  # For the baro list we just need to check that the appropriate barologger
  #     has been specified in the rdta_log...
  nas <- is.na(rdta_log_file_filt$Baro_name)
  nas <- length(nas[nas == TRUE])
  if (nas > 0) {
    print(rdta_log_file)
    if (out_csv == TRUE) {
      out_name <- paste0("rdta_log_", format(as.POSIXlt(
        Sys.time(),
        Sys.timezone()
      ), format = "%Y%m%d_%Hh%M"), ".csv")
      write.csv(rdta_log_file, file = file.path(input_dir, out_name))
      message(paste0("Please specify the barologger",
        " name in the \'rdta_log.rds file\'"))
      stopit <- TRUE
    }
  } else {
    stopit <- FALSE
  }
  if (!stopit) {
    cr_msg <- padr(core_message = " barometric compensation... ",
      pad_char = "=", pad_extras = c("|", "", "", "|"), force_extras = FALSE,
      justf = c(0, 0), wdth = 80)
    message(cr_msg)
      vv <- lapply(input_file_list,
        FUN = "gw_baro_comp",
        InasRDS = TRUE,
        wk_dir = input_dir,
        pull_baro = TRUE,
        overwrite_baro = overwrite_baro,
        overwrite_comp = overwrite_comp,
        rdta_log = TRUE,
        start_t = start_t,
        end_t = end_t,
        join_tolerance = join_tolerance,
        na_thresh = na_thresh,
        overwrite_t_bt_level = overwrite_t_bt_level
      )
    rm(vv)
    cr_msg <- padr(core_message = "", pad_char = "=",
      pad_extras = c("|", "", "", "|"), force_extras = FALSE,
      justf = c(0, 0), wdth = 80)
    message(cr_msg)
  }
}