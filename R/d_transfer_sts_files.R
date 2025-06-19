#' @title Transfer standardised ipayipi data files to the 'd3_nomvet_room'
#' @description Moves files from the 'd2_wait_room' to the 'd3_nomvet_room'---which contains standardised logger data files. This function avoids duplicating files in the 'd3_nomvet_room' and ensures file names are unique. Files will be named using their station and table names, their date coverage, and an integer (that is unique per data file & respective date-time stamps).
#' @inheritParams logger_data_import_batch
#' @details  Aim: Move standardised data files into the '__nomvet_room__' --- part of the 'ipayipi'  data pipeline. Once moved, the data will be used for further processing. Here are some of the specific steps of this funtion:
#'  1. Generate inventories of standardised files (with file extension ".ipi"`) that have been processed by [nomenclature_sts()] and [phenomena_sts()] in the 'd3_nomvet_room'.
#'  1. Determine which files need to be transferred (to avoid duplication`*`) into the 'd3_nomvet_room'.
#'  1. Transfer files---saving these in RDS format in the 'd3_nomvet_room' with the ".ipi" file extension.
#'
#'  `*` Duplicate files are determined using the `limiting_fields` parameter. The `limiting_fields` string vector is used to set the columns or fields of each files `data_summary` table used to ascertain duplicate status. Duplicate files will not be transfered to the '__nomvet_room__' --- rather they will be moved to z folder called 'zdups' in the pipe house working directory, i.e., `pipe_house$pipe_house_dir` (__see__ [ipip_house()]).
#'
#' A _aa_nomvet.log_ retains the first (and only) row of each files `data_summary`. This log is queried to determine which files are duplicates before moving files from the '__wait_room__' to the '__nomvet_room__'. NB! Do not delete files in the '__nomvet_room__' manually without updating the _aa_nomvet.log_.
#' @keywords Time series data; data standardisation; batch processing; saving files;
#' @return Saves files in native R format for 'ipayipi' in set folder. Returns a list of file names which were successfully transferred.
#' @author Paul J. Gordijn
#' @export
#' @md
transfer_sts_files <- function(
  pipe_house = NULL,
  limiting_fields = c(
    "location",
    "station",
    "stnd_title",
    "start_dttm",
    "end_dttm",
    "record_interval_type",
    "record_interval",
    "table_name"
  ),
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  "nid" <- "wid" <- "stnd_title" <- "start_dttm" <- "end_dttm" <- NULL
  # assignments
  file_ext_in <- ".ipi"
  file_ext_out <- ".ipi"

  # generate inventories of met files in d2_wait_room and d3_nomvet_room
  # note that in this function only standardised files are being processed
  wr_lg <- ipayipi_data_log(log_dir = pipe_house$d2_wait_room,
    file_ext = file_ext_in
  )
  wr_lg <- wr_lg$log
  # open inventory in nomvet room (else create)
  if (!file.exists(file.path(pipe_house$d3_nomvet_room, "aa_nomvet.log"))) {
    nr_lg <- ipayipi_data_log(log_dir = pipe_house$d3_nomvet_room,
      file_ext = file_ext_out
    )
    nr_lg <- nr_lg$log[, -"input_file", with = FALSE]
    saveRDS(nr_lg, file.path(pipe_house$d3_nomvet_room, "aa_nomvet.log"))
  }
  nr_lg <- readRDS(file.path(pipe_house$d3_nomvet_room, "aa_nomvet.log"))
  # add unique ids and check for duplicates based on 'limiting_fields' param
  nr_lg$nid <- seq_len(nrow(nr_lg))
  wr_lg$wid <- seq_len(nrow(wr_lg))
  dups <- wr_lg[nr_lg, on = limiting_fields]
  dups <- wr_lg[wid %in% dups$wid]
  if (nrow(dups) > 0) {
    cli::cli_inform(c("Duplicates in {.var wait_room} not transfered",
      " " = paste0(
        "The following duplicates were not archived in the {.var nomvet_room}"
      )
    ))
    print(dups)
    # move dups to a zdups folder
    if (!file.exists(file.path(pipe_house$pipe_house_dir, "zdups"))) {
      dir.create(file.path(pipe_house$pipe_house_dir, "zdups"))
    }
    file.copy(file.path(pipe_house$d2_wait_room, dups$input_file),
      file.path(pipe_house$pipe_house_dir, "zdups", dups$input_file),
      overwrite = TRUE
    )
    # remove dups from wait room
    file.remove(file.path(pipe_house$d2_wait_room, dups$input_file))
  }

  to_transfer <- wr_lg[!wid %in% dups$wid]
  if (nrow(to_transfer) < 1) {
    exit_m <- cli::cli_inform(c("No files to transfer",
      "i" = "Only after successful:",
      " " = "1) header ({.var header_sts()}), and",
      " " = "2) phenomena standardisation;",
      " " = paste0("can files be transferred to the nomenclature vetted room",
        "({pipe_house$d3_nomvet_room})."
      ),
      "i" = "Files ready for transfer have the \'.ipi\' extension."
    ))
    return(exit_m)
  }
  if (verbose || xtra_v) {
    cli::cli_h1("Transferring {nrow(to_transfer)} file{?s}")
  }

  # generate files names
  to_transfer <- subset(to_transfer, select = -wid)
  to_transfer$loc <- "d2_wait_room"
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
      file.path(pipe_house$d3_nomvet_room,
        paste0(to_transfer$basename[x], "__", i, file_ext_out)
      )
    )) {
      i <- i + 1
    }
    paste0(to_transfer$basename[x], "__", i, file_ext_out)
  })

  to_transfer$rds_names <- unlist(bnames)

  # transfer files
  transferrr <-
    future.apply::future_lapply(seq_len(nrow(to_transfer)), function(x) {
      m <- readRDS(
        file.path(pipe_house$d2_wait_room, to_transfer$input_file[x])
      )
      class(m) <- "ipayipi_data"
      tab_name <- m$data_summary$table_name[1]
      names(m)[names(m) == "raw_data"] <- tab_name
      m$data_summary$nomvet_name <- to_transfer$rds_names[x]
      saveRDS(m, file.path(pipe_house$d3_nomvet_room, to_transfer$rds_names[x]))
      if (verbose || xtra_v) cli::cli_inform(c("v" = paste0(
        "{to_transfer$input_file[x]} transferred as ",
        "{to_transfer$rds_names[x]}"
      )))
      invisible(m$data_summary[1])
    })
  # update the nomvet log
  nr_lg <- subset(nr_lg, select = -c(nid))
  tr_lg <- data.table::rbindlist(transferrr)
  nr_lg <- rbind(nr_lg, tr_lg)[order(stnd_title, start_dttm, end_dttm)]
  saveRDS(nr_lg, file.path(pipe_house$d3_nomvet_room, "aa_nomvet.log"))
  # remove files in the waiting room
  if (xtra_v) cli::cli_inform(c(
    " " = "Cleaning {nrow(to_transfer)} file{?s} from the {.var d2_wait_room}"
  ))
  fn <- file.path(pipe_house$d2_wait_room, to_transfer$input_file)
  file.remove(fn[file.exists(fn)])
  if (verbose || xtra_v) {
    cli::cli_h1("")
    cli::cli_inform(c(
      "What next?",
      "v" = "Standardised files transferred to the {.var nomtab_room}.",
      "i" = paste0("The {.var nomtab_room} houses the standardised files from ",
        "multiple stations."
      ),
      ">" = paste0("Use {.var append_station_batch()} to generate/update ",
        "station files in the {.var d4_ipip_room}."
      )
    ))
  }
  invisible(tr_lg)
}