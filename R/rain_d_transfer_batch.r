#' @title Hobo rainfall file transfer
#' @description Once the hobo rainfall file exports have been standardised,
#'  and have their nomenclature checked, this function saves the data from
#'  each import into the 'nomvet_room'.
#' @details  Aim: Archive each checked hobo rainfall file in a secure
#'  directory, that is, the '__nomvet_room__', which is part of the 'ipayipi'
#'  data pipeline. Once archived the data will be prepared for further
#'  processing. Here are some of the specifics:
#'  1. Generate inventory of standardised files that have been processed by
#'     `rain_hobo_conversion()` --- this object is 'fed' to the function via
#'     the `hobo_in` argument.
#'  1. Create (if none) or produce an inventory of files in the 'nomvet_room'.
#'  1. Compare the inventories to identify which new files should be
#'    transferred into the 'nomvet_room'.
#'  1. Transfer files---saving these in RDS format in the 'nomvet_room'.
#' @param hobo_in Converted hobo rainfall files as produced by
#'  `rain_hobo_conversion()`.
#' @param nomvet_room The directory where the standardised hobo rainfall files
#'  are archived.
#' @keywords hobo rainfall data pipeline; archive data; save data
#' @return Names of transferred files.
#' @author Paul J. Gordijn
#' @export
rain_transfer_batch <- function(
  hobo_in = NULL,
  nomvet_room = NULL,
  ...) {
    if (class(hobo_in) == "converted_rain_hobo") {
      hobo_in <- hobo_in[["hobo_converts"]]
    }
    # generate inventories of hobos on hand and those in the nomvet_room
    # note that in this function only standardised files are being processed
    hbwr_lg <- ipayipi::rain_log(hobo_in = hobo_in)
    hbnr_lg <- ipayipi::rain_log(log_dir = nomvet_room)
    to_transfer <- fsetdiff(hbwr_lg[["hobo_wins"]],
      fintersect(hbwr_lg[["hobo_wins"]], hbnr_lg[["hobo_wins"]]))
    nrow(to_transfer)
    to_transfer$uid <- as.integer(rep(NA, nrow(to_transfer)))
    available <- lapply(hobo_in, function(x) x$data_summary)
    names(available) <- seq_along(available)
    available_dt <- data.table::rbindlist(available)
    available_dt$uid <- as.integer(names(available))
    to_transfer$key <- paste0(to_transfer$ptitle_standard,
      to_transfer$start_dt, to_transfer$end_dt, to_transfer$instrument_type,
      to_transfer$logger_sn, to_transfer$ptitle_original)
    available_dt$key <- paste0(available_dt$ptitle_standard,
      available_dt$start_dt, available_dt$end_dt, available_dt$instrument_type,
      available_dt$logger_sn, available_dt$ptitle_original)
    transfer_id <- merge(x = to_transfer, y = available_dt,
      all.x = TRUE, all.y = FALSE, by = "key")
    transfer_neat <- transfer_id[, c("ptitle_standard.x", "location.x",
      "station.x", "start_dt.x", "end_dt.x", "instrument_type.x",
      "logger_sn.x", "sensor_sn.x", "tip_value_mm.x", "import_file_name.x",
      "ptitle_original.x", "nomvet_name.x")]
    names(transfer_neat) <- gsub(".x", "", names(transfer_neat))

    # archive files in the nomvet_room
    cr_msg <- padr(core_message =
      paste0(" Transferring ", nrow(transfer_id), " files", collapes = ""),
      wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(0, 0))
    message(cr_msg)
    zunk <- lapply(transfer_id$uid.y, function(x) {
      if (nrow(hbnr_lg[["hobo_wins"]]) > 0) {
        slist <- ipayipi::dta_list(input_dir = nomvet_room, file_ext = ".rds")
        slist_dt <- data.table::data.table(
          nr_name = basename(slist),
          nr_simp = substr(slist, 1, sapply(slist, function(x) tail(
            unlist(gregexpr("__", x)), n = 1)) - 1),
          nr_cntr = gsub(".rds", "", substr(slist,
            sapply(slist, function(x) tail(unlist(gregexpr("__", x)), n = 1))
              + 2, nchar(slist)))
        )
      } else { #generate empty table
        slist_dt <- data.table::data.table(
          nr_name = character(),
          nr_simp = character(),
          nr_cntr = character()
        )
      }
      st_dt <- hobo_in[[x]]$data_summary$start_dt
      ed_dt <- hobo_in[[x]]$data_summary$end_dt
      file_name <- paste0(
        hobo_in[[x]]$data_summary$ptitle_standard, "_",
        as.character(format(st_dt, "%Y")),
        as.character(format(st_dt, "%m")),
        as.character(format(st_dt, "%d")), "-",
        as.character(format(ed_dt, "%Y")),
        as.character(format(ed_dt, "%m")),
        as.character(format(ed_dt, "%d")), "__1"
      )
      if (nrow(hbnr_lg[["hobo_wins"]]) > 0 & file_name %in% slist_dt$nr_name) {
        new_cntr <- max(as.integer(slist_dt[nr_name == file_name]$nr_cntr)) + 1
        file_name <- paste0(
        hobo_in[[x]]$data_summary$ptitle_standard, "_",
          as.character(format(st_dt, "%Y")),
          as.character(format(st_dt, "%m")),
          as.character(format(st_dt, "%d")), "-",
          as.character(format(ed_dt, "%Y")),
          as.character(format(ed_dt, "%m")),
          as.character(format(ed_dt, "%d")), "__", new_cntr
        )
      }
      # update the nomvet_name in the data_summary
      hobo_in[[x]]$data_summary$nomvet_name <- file_name
      saveRDS(hobo_in[[x]], file.path(nomvet_room,
        paste0(file_name, ".rds")))
      cr_msg <- padr(core_message = paste0(
          basename(hobo_in[[x]]$data_summary$import_file_name), " --> ",
          file_name, collapes = ""),
        wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
        force_extras = FALSE, justf = c(-1, 1))
      message(cr_msg)
      invisible(cr_msg)
    })
    return(transfer_neat)
    cr_msg <- padr(core_message = paste0(
          "Transfer complete ...", collapes = "="),
        wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
        force_extras = FALSE, justf = c(-1, 1))
    message(cr_msg)
  }
