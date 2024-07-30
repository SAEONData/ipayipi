#' @title Build `ipayipi` data pipeline housing
#' @description Compiles/creates a list of 'rooms' (folders/directories) required for an 'ipayipi' data-processing pipeline.
#' @param work_dir The working directory/'room' (or folder) in which all pipeline rooms are 'housed'.
#' 
#' The data is moved through an 'ipayipi' pipeline housing system through the following rooms.
#' @param source_room 'Room' from which raw-data files are imported. If left `NULL` a 'source_room' will be created in the pipe_house working directory.
#' @param wait_room Directory into which raw-data moves to from the `source_room`. This is also the room where standardisation of raw-data files begins.
#' @param nomvet_room 'Room' into which imbibed, standardised raw-data is stored.
#' @param ipip_room 'Room' where 'station files' are housed. Station files consist of appended, standardised raw-data files---pulled from the `nomvet_room`---that have been, or are ready for further processing.
#' @param raw_room 'Room' wherein raw-data files from the `source_room` are systematically archived. If this is set to `NULL` then raw-data files will not be archived by 'ipayipi'.
#' @param full If `TRUE` additonal folders in the working directory will be generated for 1) 
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
#' _Note the above illustration shows data flow not the structure of an ipayipi directory._
#'
#' 1. *work_dir*: The working directory within which other pipeline directories are housed.
#' 2. The source directory where raw data are harvested from.
#' 3. A staging directory where 'raw data' are standardised before being transferred/archived in the,
#' 4. `nomvet_room`.
#' 5. The directory that compiles standardised data from the `nomvet_room` by stations. Station files can be further processed and exported into other formats from here.
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
  r = NULL,
  source_room = NULL,
  wait_room = NULL,
  nomvet_room = NULL,
  ipip_room = NULL,
  dta_out = NULL,
  reports = NULL,
  raw_room = NULL,
  sf_con = TRUE,
  ...
) {

  # dir 'names'
  dirs <- list("r", "source_room", "wait_room", "nomvet_room",
    "ipip_room", "dta_out", "reports", "raw_room"
  )
  dirs <- lapply(dirs, function(x) {
    z <- get(x)
    # lean and trailing forward and backward slashes
    if (!is.null(z)) z <- gsub("^[/]|^[//]|^[\\]|[/]$|[\\]$", "", z)
    if (is.null(z)) z <- file.path(work_dir, x)
    z <- gsub("^[/]|^[//]|^[\\]|[/]$|[\\]$", "", z)
    names(z) <- x
    return(z)
  })
  names(dirs) <- c("r", "source_room", "wait_room", "nomvet_room",
    "ipip_room", "dta_out", "reports", "raw_room"
  )
  # check that the working directory exists
  if (!file.exists(work_dir)) {
    message("Error: Working direcotry not found; check path string!")
    return(NULL)
  }
  dirs_gen <- lapply(seq_along(dirs), function(i) {
    if (!dir.exists(dirs[[i]])) {
      dir.create(dirs[[i]])
    }
    if (names(dirs[[i]]) %in% "r") {
      dir.create(file.path(dirs[[i]], "pipe_seq"), showWarnings = FALSE)
    }
    if (names(dirs[[i]]) %in% "reports") {
      dir.create(file.path(dirs[[i]], "markdown"), showWarnings = FALSE)
    }
  })
  rm(dirs_gen)
  dirs$work_dir <- work_dir
  return(dirs)
}
