#' @title Append Rsol files
#' @description This is a feeder function and not tested thoroughly
#'  outside of the package pipeline. The function appends R solonist
#'  data files and is used in the gw_xle_to_R() function.
#' @param new_file R data solonist file to add to an existing file.
#' @param old_file Existing R data solonist file to which the new data
#'  is appended to.
#' @param InasRDS Logical. Is the input data format RDS (file extension ".rds")
#' @param new_dir Directory in which the appended file is saved to.
#' @param old_dir Directory in which the existing or old R data
#'  solonist file is located.
#' @param save_rdta Logical. Save the output file in RDS format?
#' @param RDS_name Output RDS file name.
#' @keywords data append; pipeline
#' @author Paul J Gordijn
#' @return Appended data file.
#' @export
#' @examples
#' ## This function is for use within the pipeline, especially the 'xle_to_R'
#' # function.
#' # Will add in an example when there is data included in the package. 
gw_rdta_append <- function(
  new_file = NULL,
  old_file = NULL,
  InasRDS = TRUE,
  new_dir = ".",
  old_dir = ".",
  save_rdta = FALSE,
  RDS_name = NULL) {

  if (InasRDS == TRUE) {
    new <- readRDS(file.path(new_dir, basename(new_file)))
    old <- readRDS(file.path(old_file, basename(old_file)))
  } else if (InasRDS == FALSE) {
    new <- new_file
    old <- old_file
  }

  if (class(new) %in% c("level_file", "baro_file") &
    class(old) %in% c("level_file", "baro_file")) {
  } else {
    stop(paste0("New and old input data objects ",
      " must be of classes 1. level_file or 2. baro_file",
      collapse = ""))
  }
  # list of all table names
  tbl_names_all <- unique(names(new), names(old))
  # some tables need to be appended in a custom way
  custom_tables <- c("xle_FileInfo", "xle_LoggerInfo", "xle_LoggerHeader",
    "xle_phenomena", "log_t")
  generic_tbls <- tbl_names_all[!tbl_names_all %in% custom_tables]

  # append custom tables
  # Extract tables for each NEW & OLD item and append the objects in the lists
  # xle_FileInfo ---- table
  new$xle_FileInfo <- rbind(
    data.table::setDT(old$xle_FileInfo),
    data.table::setDT(new$xle_FileInfo),
    fill = TRUE
  )
  new$xle_FileInfo <- new$xle_FileInfo[,
    ":="(Start = min(Start), End = max(End)),
    by = .(Company, Licence, Created_by, FileName)
  ]
  new$xle_FileInfo <- unique(new$xle_FileInfo)[order(Date_time)]

  # xle_LoggerInfo ---- table
  new$xle_LoggerInfo <- rbind(
    data.table::setDT(old$xle_LoggerInfo),
    data.table::setDT(new$xle_LoggerInfo),
    fill = TRUE
  )
  new$xle_LoggerInfo <- new$xle_LoggerInfo[,
    ":="(Start = min(Start), End = max(End)),
    by = .(Instrument_type, Model_number, Instrument_state,
      Serial_number, Battery_level, Channel, Firmware)
    ]
  new$xle_LoggerInfo <- unique(new$xle_LoggerInfo)[order(Start, End)]

  # xle_LoggerHeader ---- table
  new$xle_LoggerHeader <- rbind(
    data.table::setDT(old$xle_LoggerHeader),
    data.table::setDT(new$xle_LoggerHeader),
    fill = TRUE
  )
  new$xle_LoggerHeader <- unique(new$xle_LoggerHeader)[
    order(Start_time, Stop_time)]

  # xle_phenomena ---- table
  # check if the number of phenomena is different between the
  # old and new files
  phensdf <- setdiff(names(old$xle_phenomena), names(new$xle_phenomena))
  if (length(phensdf) > 0) {
    if (InasRDS == TRUE) {
    message(paste("Phenomena mismatch noted:", quote(new_file), "<>",
      quote(old_file)))
    } else {
    message(paste0(
      new$xle_LoggerHeader$Project_ID[nrow(new$xle_LoggerHeader$Project_ID)],
      "_",
      new$xle_LoggerHeader$Location[nrow(new$xle_LoggerHeader$Location)],
      collapse = ""
    ))
    }
  }

  same_phen <- union(
    intersect(names(old$xle_phenomena), names(new$xle_phenomena)),
    intersect(names(new$xle_phenomena), names(old$xle_phenomena))
  )
  diff_phen <- union(
    setdiff(names(old$xle_phenomena), names(new$xle_phenomena)),
    setdiff(names(new$xle_phenomena), names(old$xle_phenomena))
  )
  new$xle_phenomena <- append(
    lapply(same_phen, function(x) {
      data.table::rbindlist(
        list(old$xle_phenomena[[x]], new$xle_phenomena[[x]]),
        fill = TRUE, use.names = TRUE
      )
    }),
    lapply(diff_phen, function(x) {
    return(data.table::rbindlist(
      list(
        old$xle_phenomena[[x]],
        new$xle_phenomena[[x]]
      ), fill = TRUE, use.names = TRUE))
    })
  )
  names(new$xle_phenomena) <- c(same_phen, diff_phen)

  # log_t ---- table
  new$log_t <- rbind(old$log_t, new$log_t,
    fill = TRUE
  )[order(Date_time)]
  new$log_t <- unique(new$log_t, by = "Date_time")

  # Append generic tables
  for (i in seq_along(generic_tbls)) {
    if (!generic_tbls[i] %in% names(new)) {
      new[[generic_tbls[i]]] <- old[[generic_tbls[i]]][0, ]
    }
    if (!generic_tbls[i] %in% names(old)) {
      old[[generic_tbls[i]]] <- new[[generic_tbls[i]]][0, ]
    }
    new[[generic_tbls[i]]] <- rbind(
      old[[generic_tbls[i]]], new[[generic_tbls[i]]], fill = TRUE)
    new[[generic_tbls[i]]] <- unique(new[[generic_tbls[i]]])
    col_of_interest <- "date_time"
    if (any(names(new[[generic_tbls[i]]]) %ilike% col_of_interest)) {
      sort_col <- names(new[[generic_tbls[i]]])[
        names(new[[generic_tbls[i]]]) %ilike% col_of_interest
      ][1]
      data.table::setorderv(new[[generic_tbls[i]]], sort_col)
    } else {
      data.table::setorderv(new[[generic_tbls[i]]],
        names(new[[generic_tbls[i]]])[1])
    }
    col_of_interest <- "start|end"
    if (any(names(new[[generic_tbls[i]]]) %ilike% col_of_interest)) {
      sort_col <- names(new[[generic_tbls[i]]])[
        names(new[[generic_tbls[i]]]) %ilike% col_of_interest
      ]
      data.table::setorderv(new[[generic_tbls[i]]], sort_col)
    } else {
      data.table::setorderv(new[[generic_tbls[i]]],
        names(new[[generic_tbls[i]]])[1])
    }
  }
  class(new) <- class(old)

  # save the file as an RDS with filename
  if (save_rdta == TRUE || !is.null(RDS_name)) {
    if (is.null(RDS_name)) {
    RDS_name <- paste0(
      new$xle_LoggerHeader$Location[max(
      row_number(new$xle_LoggerHeader$Location)
      )], "_",
      new$xle_LoggerHeader$Project_ID[max(
      row_number(new$xle_LoggerHeader$Project_ID)
      )], ".rds"
    )
    } else if (!is.null(RDS_name)) {
    RDS_name <- gsub(pattern = ".rds", replacement = "", x = RDS_name)
    RDS_name <- paste0(RDS_name, ".rds")
    }
    saveRDS(new, file.path(new_dir, RDS_name))
  }
  return(new)
}