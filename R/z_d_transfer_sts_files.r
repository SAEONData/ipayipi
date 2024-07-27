#' @title Transfer standardised ipayipi data files to the 'nomvet_room'
#' @description Moves files from the 'wait_room' to the 'nomvet_room', which
#'  contains standardised logger data files. This function avoids duplicating
#'  files in the 'nomvet_room' and ensures file names are unique. Files will be
#'  named using their station and table names, their date coverage, and an
#'  integer (that is unique per data file & respective date-time stamps).
#' @param pipe_house List of pipeline directories. __See__
#'  `ipayipi::ipip_house()` __for details__.
#' @param prompt Should the function use an interactive file selection function
#'  otherwise all files are returned. TRUE or FALSE.
#' @param recurr Should the function search recursively into sub directories
#'  for hobo rainfall csv export files? TRUE or FALSE.
#' @param wanted Vector of strings listing files that should not be
#'  included in the import.
#' @param unwanted Vector of strings listing files that should not be included
#'  in the import.
#' @param file_ext_in The extension of the files being imported from the
#'  'wait_room'. Defaults to ".ipi". Note the full-stop must be included.
#' @param file_ext_out The file extension that will be used for the saving of
#'  files in the 'nomvet_room'. Defaults to ".ipi".
#' @param verbose Print some details on the files being processed? Logical.
#' @param cores Allocate number of cores for processing function. Utilises `parallel::mclapply()`; only works on linux.
#' @details  Aim: Archive each standardised data files in a
#'  set directory, that is, the '__nomvet_room__' --- part of the
#'  'ipayipi'  data pipeline. Once archived the data will be used for
#'  further processing. Here are some of the specific steps of this funtion:
#'  1. Generate inventories of standardised files (with file extension
#'     `file_ext_in`) that have been processed by `ipayipi::nomenclature_sts()`
#'      and 'ipayipi::phenomena_sts()' in the 'nomvet_room'.
#'  1. Determine which files need to be transferred (to avoid duplication)
#'     into the 'nomvet_room'.
#'  1. Transfer files---saving these in RDS format in the 'nomvet_room' with
#'   the specified extension `file_ext_out`.
#' @keywords Meteorological data; data standardisation; batch processing;
#'  saving files;
#' @return Saves files in native R format for 'ipayipi'. Returns a list of
#'  file names which were successfully transferred.
#' @author Paul J. Gordijn
#' @export
#' @md
transfer_sts_files <- function(
  pipe_house = NULL,
  prompt = FALSE,
  recurr = TRUE,
  unwanted = NULL,
  wanted = NULL,
  file_ext_in = ".ipi",
  file_ext_out = ".ipi",
  verbose = FALSE,
  ...
) {
  # merge data sets into a station for given time periods
  slist <- ipayipi::dta_list(input_dir = pipe_house$wait_room, file_ext =
      file_ext_in, prompt = prompt, recurr = recurr, unwanted = unwanted,
    wanted = wanted
  )
  if (length(slist) < 1) stop(" No files to transfer")
  # make table to guide merging by record interval, date_time, and station
  merge_dt <- future.apply::future_lapply(slist, function(x) {
    m <- readRDS(file.path(pipe_house$wait_room, x))
    mdt <- data.table::data.table(
      file = x,
      stnd_title = m$header_info$stnd_title[1],
      record_interval = m$header_info$record_interval[1],
      start_dttm = m$header_info$start_dttm[1],
      end_dttm = m$header_info$end_dttm[1]
    )
    invisible(mdt)
  })
  merge_dt <- data.table::rbindlist(merge_dt)

  # generate inventories of met files in wait_room and nomvet_room
  # note that in this function only standardised files are being processed
  wr_lg <- ipayipi::ipayipi_data_log(log_dir = pipe_house$wait_room,
    file_ext = file_ext_in
  )
  wr_lg <- wr_lg$log
  nr_lg <- ipayipi::ipayipi_data_log(log_dir = pipe_house$nomvet_room,
    file_ext = file_ext_out
  )
  nr_lg <- nr_lg$log
  to_transfer <- data.table::fsetdiff(wr_lg,
    data.table::fintersect(wr_lg, nr_lg)
  )
  # generate files names
  to_transfer$loc <- "wait_room"
  to_transfer$basename <- paste0(
    to_transfer$stnd_title, "_",
    gsub(" ", "", to_transfer$record_interval), "_",
    as.character(format(to_transfer$start_dttm, "%Y")),
    as.character(format(to_transfer$start_dttm, "%m")),
    as.character(format(to_transfer$start_dttm, "%d")), "-",
    as.character(format(to_transfer$end_dttm, "%Y")),
    as.character(format(to_transfer$end_dttm, "%m")),
    as.character(format(to_transfer$end_dttm, "%d"))
  )
  bnames <- lapply(seq_len(nrow(to_transfer)), function(x) {
    i <- 1
    while (file.exists(
      file.path(pipe_house$nomvet_room,
        paste0(to_transfer$basename[x], "__", i, file_ext_out)
      )
    )) {
      i <- i + 1
    }
    bname <- paste0(to_transfer$basename[x], "__", i, file_ext_out)
    return(bname)
  })
  to_transfer$rds_names <- unlist(bnames)
  # transfer files
  transferrr <-
    future.apply::future_lapply(seq_len(nrow(to_transfer)), function(x) {
      m <- readRDS(file.path(pipe_house$wait_room, to_transfer$input_file[x]))
      class(m) <- "ipayipi_data"
      # tab_name <- paste0("raw_", m$data_summary$table_name[1])
      tab_name <- m$data_summary$table_name[1]
      names(m)[names(m) == "raw_data"] <- tab_name
      m$data_summary$nomvet_name <- to_transfer$rds_names[x]
      saveRDS(m, file.path(pipe_house$nomvet_room, to_transfer$rds_names[x]))
      cr_msg <- padr(core_message = paste0(to_transfer$input_file[x], " --> ",
          to_transfer$rds_names[x], collapes = ""
        ), wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
        force_extras = FALSE, justf = c(-1, 1)
      )
      ipayipi::msg(cr_msg, verbose)
      invisible(to_transfer$rds_names[x])
    })
  # remove files in the waiting room
  deleted_files <- future.apply::future_lapply(
    seq_len(nrow(to_transfer)), function(x) {
      file.remove(file.path(pipe_house$wait_room, to_transfer$input_file[x]))
      invisible(to_transfer$input_file[x])
    }
  )
  rm(deleted_files)
  cr_msg <- padr(core_message = "  transferred  ", wdth = 80, pad_char = "=",
    pad_extras = c("|", "", "", "|"), force_extras = FALSE, justf = c(1, 1)
  )
  ipayipi::msg(cr_msg, verbose)
  invisible(transferrr)
}
