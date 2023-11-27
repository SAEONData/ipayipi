#' @title Setup an `ipayipi` data pipeline directory system
#' @description Creates the folders for the ipayipi data processing pipeline.
#' @param work_dir The folder, that is, directory in which to create the
#'  pipeline directories.
#' @keywords initiate pipeline, folder creation, directory structure.
#' @return List of 'room' directories.
#' @details This function automates the creation of four folders/directories
#'  that are requried for bulk processing of files in the ipayipi groundwater
#'  pipeline structure. The directories are:
#' * **wait_room** --- temporary directory where raw data imports are
#'    standardised.
#' * **nomvet_room** --- for housing standardized native solonist data files.
#' * **ipip_room** --- transforming and processing standardized native data.
#' * **raw_room** --- for archiving 'raw' data files.
#' * **source_dir** --- directory in which new raw data files are 'sourced'.
#' _NB_! Existing directories (and their contents) with similar names are not
#'  deleted/removed by this function.
#' @md
#' @examples
#' # Inititate pipeline
#' dir <- "." # define the working directory
#' pipe_init(work_dir = dir)
#' @author Paul J. Gordijn
#' @export
ipip_init <- function(
    work_dir = ".",
    source_dir = NULL,
    wait_room = NULL,
    nomvet_room = NULL,
    ipip_room = NULL,
    raw_room = NULL,
    ...
) {
  # dir 'names'
  dirs <- list("wait_room", "nomvet_room", "ipip_room", "raw_room")
  dirs <- lapply(dirs, function(x) {
    z <- get(x)
    if (is.null(z)) z <- file.path(work_dir, x)
    names(z) <- x
    return(z)
  })
  names(dirs) <- c("wait_room", "nomvet_room", "ipip_room", "raw_room")
  dirs_gen <- lapply(dirs, function(x) {
    if (!dir.exists(x)) {
      dir.create(x)
    }
  })
  rm(dirs_gen)
  dirs$source_dir <- source_dir
  return(dirs)
}
