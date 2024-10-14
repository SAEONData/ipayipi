#' @title Ammulgamate site data into time series
#' @description This function explores the 'nomvet' data file log and
#'  determines which xle files to amulgamate to generate master files
#'  for each different site (or borehole).
#' @param solr_room Directory, that is, folder in the package pipeline
#'  that holds the R solonist data. Files will be appended to the R
#'  solonist data files in this folder.
#' @param nomvet_room Folder in the data pipeline where the standardised
#'  xle data is stored/archived.
#' @keywords append, pipeline
#' @author Paul J Gordijn
#' @return Vector of files transferred and/or appended to R data in the
#'  designated repository.
#' @export
#' @examples
#' ## Batch processing of groundwater or water level data
#' # Set the directory in where the r data files must be pushed/appended to.
#' solr_room <- "./solr_room"
#' # Set the directory where standardized xle files are stored.
#' nomvet_room <- "./nomvet_room"
#'
#' # Batch run xle to R and append where appropriate.
#' gw_site_series(solr_room = solr_room, nomvet_room = nomvet_room)
gw_site_series <- function(
  solr_room = NULL,
  nomvet_room = NULL,
  ...
) {

  # update the nomvet log and check if data files exist
  # the log is always updated to avoid error buildup
  # Generate a list of the xle data files in the nomvet folder.
  xlefiles_path <- gw_xle_list(input_dir = nomvet_room, recurr = FALSE)
  xlefiles <- basename(xlefiles_path)
  # check if there is a nomvet log
  nomvet_log_vet <- list.files(
    path = nomvet_room, pattern = "nomvet_log.rds",
    recursive = FALSE, full.names = TRUE
  )

  # if there are standardised xle files in the nomvet room
  if (length(xlefiles) > 0) {
    nomvet_log <- gw_xle_log(log_dir = nomvet_room, xlefiles = xlefiles,
      place = "nomvet_room"
    )
    saveRDS(nomvet_log, file.path(nomvet_room, "nomvet_log.rds"))
  } else if (length(xlefiles) <= 0) {
    stop("There are no files in the nomvet folder")
  }

  # check if there is a transfer log which documents
  #  which files have been pushed/appended to files
  #  in the solr_room

  # Return the trasfer log if in existence
  #  the xle_to_R function handles creation
  #  of a transfer log if not in existence
  transfer_log_vet <- list.files(
    path = solr_room,
    pattern = "transfer_log.rds",
    recursive = FALSE, full.names = TRUE
  )
  if (length(transfer_log_vet) > 0) {
    transfer_log <- readRDS(file.path(solr_room, "transfer_log.rds"))
  }

  # files are handled by the 'xle_to_R'
  # plus 'rdta_append' functions
  # although more intensive processing, this precludes the possibility of
  # ignoring data within the 'log' file data range
  if (length(transfer_log_vet) > 0) {
    transfer_rows <- sapply(transfer_log$xle_file, function(z) {
      grep(pattern = z, x = nomvet_log$file.name)
    })
    transfer_rows <- unlist(transfer_rows, use.names = FALSE)
    transfer_rows <- as.vector(transfer_rows)
    fnomvet_log <- nomvet_log[-transfer_rows, ]
    fnomvet_log$period <- fnomvet_log$End - fnomvet_log$Start
    if (nrow(fnomvet_log) > 0) {
      fnomvet_log <- fnomvet_log[, period := max(period),
        by = .(Location, Borehole, SN, Start)
      ]
    }
      # If there is no transfer_log_vet file BUT
      #  there are xle files in the nomvet folder...
      #  Just go ahead and generate an all encompassing fnomvet_log
  } else if (length(transfer_log_vet) <= 0) {
    fnomvet_log <- nomvet_log
    fnomvet_log$period <- fnomvet_log$End - fnomvet_log$Start
    fnomvet_log <-
      fnomvet_log[, period := max(period),
        by = .(Location, Borehole, SN, Start)
    ]
  }

  # The 'xle_to_R' function handles whether files should be appended or not
  appfiles <- unique(fnomvet_log)
  appfiles <- appfiles$file.name
  cr_msg <- padr(core_message = " batch append data + metadata... ",
    pad_char = "=", pad_extras = c("|", "", "", "|"), force_extras = FALSE,
    justf = c(0, 0), wdth = 80)
  message(cr_msg)
  if (length(appfiles) > 0) {
    sapply(appfiles,
      FUN = "gw_xle_to_R", save_rdta = TRUE,
      filename = NULL, overwrite = TRUE, app_data = TRUE,
      app_dates = FALSE, input_dir = nomvet_room,
      output_dir = solr_room, transfer_log = TRUE
    )
  }

  # Update the Rdta log which summaries the periods covered per site.
  #  This log can also be updated with correct class types
  #   and the barologger file name that should be used for
  #   compensation.
  rdta_log_mk <- gw_rdta_log_mk(solr_room, recurr = FALSE)

  # Return the transfer log
  transfer_log_vet <-
    list.files(
      path = solr_room, pattern = "transfer_log.rds",
      recursive = FALSE, full.names = TRUE
    )
  cr_msg <- padr(core_message = " data seriez appended... ",
    pad_char = "=", pad_extras = c("|", "", "", "|"), force_extras = FALSE,
    justf = c(0, 0), wdth = 80)
  message(cr_msg)
  if (length(transfer_log_vet) > 0) {
    transfer_log <- readRDS(file.path(solr_room, "transfer_log.rds"))
    return(transfer_log)
  }
}