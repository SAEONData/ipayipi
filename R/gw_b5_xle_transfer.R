#' @title Archive xle files
#' @description A function to transfer standardised xle files into an
#'        archive folder.
#' @details Export renamed files into a central data folder called , 'nomvet'.
#' This function forms part of the xle to R data pipeline and will function
#' best where the suggested folders are created and utilised.
#' \enumerate{
#'     \item Before cutting files to the 'nomvet' folder check for duplicates.
#'            - generate a 'wait_log' table with file names and start and end
#'            dates for the level logger data.
#'     \item Update or create a 'nomvet_log' file that lists all stored .xle
#'            files in the 'nomvet' folder.
#'     \item Compare the 'wait_log' and 'nomvet_log' files and create:
#'        - a list of potential duplicate files.
#'        - list of non-duplicate files.
#'     \item Copy the non-duplicate files across to the 'nomvet' folder.
#'     \item Delete the non-duplicate files in the 'wait_room' folder.
#'     \item If duplicates exist generate a message plus list/table of
#'        detected duplicates.
#' }
#' NB! If your files are being transferred into the 'nomvet_room' you may
#'  need to update the 'nomvet_log.rds' in the 'nomvet_room'. Do this using
#'  gw_xle_log() and save the result in the nomvet folder. This should only be
#' necessary if you have removed xle files from the 'nomvet_room'.
#' @param wait_room Folder path where the standardised xle files are stored.
#' @param nomvet_room Folder path where the standardised xle files will be
#'    moved. The xle files should remained archived in this folder.
#' @keywords move file; xle; cut;
#' @return List of dupplicate files if existant.
#' @author Paul J Gordijn
#' @export
#' @examples
#' ## Transfer the xle files that have updated nomenclature to a folder
#' # which contains xle data files used to generate continuous records.
#' ## Set the directory (temporary) with xle data for which the nomenclature
#' # has been standardized.
#' wait_room <- "./temp"
#' ## Set the directory to where the standardized xle files should be cut to
#' # (i.e. the files in the waiting room will be deleted.)
#' nomvet_room <- "./nomvet"
#' 
#' ## Transfer the xle files in the temporary folder.
#' gw_xle_transfer(wait_room = wait_room, nomvet_room = nomvet_room)
#' 
#' ## Note that a log file, 'nomvet_log.rds', will be updated as files are
#' # transferred.
gw_xle_transfer <- function(
  wait_room = NULL,
  nomvet_room = NULL,
  ...) {

  #### 1 ####
  #setwd(wait_room)
  # Generate a list of the files to be copied.
  xlefiles_path <- gw_xle_list(input_dir = wait_room, recurr = FALSE)
  xlefiles <- basename(xlefiles_path)

  # only continue if there are files in the waiting room folder
  if (length(xlefiles) <= 0) {
    stop("There are no xle files in the waiting room folder")
  }
  wait_log <- setDT(gw_xle_log(
    log_dir = wait_room,
    xlefiles = xlefiles, place = "wait_room"
  ))

  #### 2 and 3
  #setwd(nomvet_room)
  # Generate a list of the files in the nomvet folder.
  xlefiles_path <- gw_xle_list(input_dir = nomvet_room, recurr = FALSE)
  xlefiles <- basename(xlefiles_path)

  # update/create the 'nomvet_log'folder
  nomvet_log_vet <- list.files(
    path = nomvet_room,
    pattern = "nomvet_log.rds", recursive = FALSE, full.names = T
  )

  # check if there is an old nomvet file
  if (length(nomvet_log_vet) > 0) {
    nomvet_log <-
      data.table::setDT(readRDS(file.path(nomvet_room, "nomvet_log.rds")))
    tocopy <- rbind(nomvet_log, wait_log)
    tocopy <- unique(tocopy,
      by = c("SN", "Start", "End")
        )[place == "wait_room", ]
    dups <- setdiff(nomvet_log, tocopy)
    # If there is no nomvet file BUT there are xle files in the
    #    nomvet folder...create a log file
  } else if (length(nomvet_log_vet) <= 0 & length(xlefiles) > 0) {
      nomvet_log <-
        setDT(gw_xle_log(
          log_dir = nomvet_room,
          xlefiles = xlefiles, place = "nomvet_room"
        ))
    tocopy <- rbind(nomvet_log, wait_log)
    tocopy <- unique(tocopy,
      by = c("SN", "Start", "End")
        )[place == "wait_room", ]
    dups <- setdiff(nomvet_log, tocopy)
  } else if (length(xlefiles) <= 0) {
    message("There are no files in the nomvet folder")
    tocopy <- wait_log
    dups <- NULL
  }

  #### 4 and 5
  # copy non-duplicate files
  # check if there are non-duplicate files
  if (is.null(tocopy) == TRUE) {
    stop("There are no files to copy")
  }

  # copy files
  #setwd(wait_room)
  tocopy_files <- tocopy$file.name
  cr_msg <- padr(core_message = paste0(" moving ", length(tocopy_files),
      " files to the nomvet room... "), wdth = 80, pad_char = "=",
    pad_extras = c("|", "", "", "|"), force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  for (f in seq_along(tocopy_files)) {
    file.copy(from = file.path(wait_room, tocopy_files[f]),
      to = nomvet_room)
  }
  for (f in seq_along(tocopy_files)) {
    file.remove(file.path(wait_room, tocopy_files[f]))
  }

  # update the 'nomvet_log'
  #setwd(nomvet_room)
  # Generate a list of the files in the nomvet folder.
  xlefiles_path <- gw_xle_list(input_dir = nomvet_room, recurr = F)

  nomvet_log_vet <-
    list.files(
      path = nomvet_room, pattern = "nomvet_log.rds",
      recursive = FALSE, full.names = TRUE
    )

  nomvet_log <- gw_xle_log(
    log_dir = nomvet_room,
    xlefiles = gw_xle_list(input_dir = nomvet_room, recurr = F),
    place = "nomvet_room"
  )
  data.table::setDT(nomvet_log)
  saveRDS(nomvet_log, file.path(nomvet_room, "nomvet_log.rds"))

  #### 6 ####
  if (length(dups$SN) > 0) {
    message(paste0(
      "There were duplicates that were not copied. ",
      "Try checking the duplicate table for details."
    ))
  }
  transfer <- list(dups = dups, copied = tocopy, nomvet_log = nomvet_log)
  class(transfer) <- "SAEON_solonist"
  cr_msg <- padr(core_message = paste0(" arrived safely in the nomvet room "),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  return(transfer)
}