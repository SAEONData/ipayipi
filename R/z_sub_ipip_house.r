#' @title Build `ipayipi` data pipeline housing
#' @description Compiles/creates a list of 'rooms' (folders/directories) required for an 'ipayipi' data-processing pipeline.
#' @param work_dir The working directory/'room' (or folder) in which all pipeline rooms are 'housed'.
#' 
#' The data is moved through an 'ipayipi' pipeline housing system through the following rooms.
#' @param source_room 'Room' from which raw-data files are imported.
#' @param wait_room Directory into which raw-data moves to from the `source_room`. This is also the room where standardisation of raw-data files begins.
#' @param nomvet_room 'Room' into which imbibed, standardised raw-data is stored.
#' @param ipip_room 'Room' where 'station files' are housed. Station files consist of appended, standardised raw-data files---pulled from the `nomvet_room`---that have been, or are ready for further processing.
#' @param raw_room 'Room' wherein raw-data files from the `source_room` are systematically archived. If this is set to `NULL` then raw-data files will not be archived by 'ipayipi'.
#' @keywords initiate pipeline, folder creation, directory structure
#' @return List of pipeline housing 'rooms' (filepaths).
#' @details This function automates the creation of four/five folders/directories that are requried for bulk processing of files in the ipayipi pipeline structure. The flow of data through an 'ipayipi' pipeline housing system is illustrated below.
#' 
#' 1-|--work_dir -------------------------------------------------------------|
#' 2-|--source_room-->|                                                       |
#' 3-|                |--wait_room-->|                                        |
#' 4-|                |              |--nomvet_room-->|                       |
#' 5-|                |                               |--ipip_room            |
#'   |                |                                                       |
#' 6-|                |--> raw_room                                           |
#' 
#' 1. The working directory within which other pipeline directories are housed.
#' 2. The source directory where raw data are harvested from.
#' 3. A staging directory where raw data are standardised before being transferred/archived in the,
#' 4. `nomvet_room`.
#' 5. The directory that compiles standardised data in the `nomvet_room` by stations. Station files can be further processed and exported into other formats from here.
#' 6. If the `raw_room` is defined `ipayipi::imbibe_raw_batch()` can harvest (copy or cut---see funtion documentation) and archive 'raw data' files from the `source_room` and will archive them in the `raw_room` in monthly folders.
#' @md
#' @examples
#' # Inititate pipeline
#' wd <- "." # define the working directory
#' pipe_house <- ipip_house(work_dir = wd)
#' print(pipe_house)
#' @author Paul J. Gordijn
#' @export
ipip_house <- function(
  work_dir = ".",
  source_room = NULL,
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
    # lean and trailing forward and backward slashes
    if (!is.null(z)) z <- gsub("^[/]|^[//]|^[\\]|[/]$|[\\]$", "", z)
    if (is.null(z)) z <- file.path(work_dir, x)
    z <- gsub("^[/]|^[//]|^[\\]|[/]$|[\\]$", "", z)
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
  dirs$source_room <- source_room
  return(dirs)
}
