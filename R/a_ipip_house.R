#' @title Build `ipayipi` data pipeline housing for a data family
#' @description Creates a list of 'rooms', that is, folders/directories, required for an 'ipayipi' data-processing pipeline within an \strong{existing} `pipe_house_dir` folder. Suggestion is to use default options: only provide the `pipe_house_dir` that must refer to an existing directory for housing your data pipeline. By providing other parameters the directories can be customised for special-use cases.
#' The recommentation is to build seperate 'pipe_houses' for different data streams or families, for example, separate out automatic weather stations, stand alone rain gauges, and water-level transducers. Whilst the families are housed in different 'pipe houses', iPayipi enables efficient quering and processing of data across different data families.
#' @param pipe_house_dir An existing base pipeline directory/'room' (or folder) in which all pipeline rooms are 'housed'. \bold{NB!} The `pipe_house_dir` directory must be relative to the active console/terminal working directory, or the full path name.
#'\cr \cr
#' @param r Directory for miscellaneous scripts. Within the 'r' room, a sub directory is created called 'pipe_seq' for housing processing pipeline sequence scripts for `ipayipi::dt_process()`. Suggestion is to leave `NULL` so that this 'room' is nested within the `pipe_house_dir`.
#' The data is moved through an 'ipayipi' pipeline housing system through the following rooms.
#' \cr \cr
#' Data moves through an 'ipayipi' pipeline through rooms 1 to 4 outlined below. Along the way raw data is organised and archived, and data is standardised before being processed. \cr
#' @param source_room **1**. 'Room' from which raw-data files are imported into the pipeline. If left `NULL` a directory named 'source_room' will be created in the pipe_house working directory.
#' @param wait_room **2**. Directory into which raw-data moves to from the `source_room`. This is also the room where standardisation of raw-data files begins.
#' @param nomvet_room **3**. 'Room' into which imbibed, standardised raw-data is stored.
#' @param ipip_room **4**. 'Room' where 'station files' are housed. Station files consist of appended, standardised raw-data files---pulled from the `nomvet_room`---that have been, or are ready for further processing.
#' @param dta_out Directory housing select data products from the pipeline; typically in 'csv' file format.
#' @param reports Intended to house 'rmarkdown' reports used for inspection and analysis of data.
#' @param raw_room 'Room' wherein raw-data files from the `source_room` are systematically archived. If this is set to `NULL` then raw-data files will not be archived by 'ipayipi'.
#' @param verbose Logical. If `TRUE` some terminal/console messages may provide useful insight.
#' @keywords initiate pipeline, folder creation, directory structure
#' @return List of pipeline housing 'rooms' (filepaths).
#' @details
#' ## Pipeline directories and data flow
#' This function automates the creation of four/five folders/directories that are requried for bulk processing of files in the ipayipi pipeline structure. The flow of data through an 'ipayipi' pipeline housing system is described below. The preffix to the folders created represents this flow of data.
#'
#' 1. *pipe_house_dir**: The main directory within which other directories are housed for preparing, standardising and processing data.
#' 2. *source_room*: The source directory where raw data are harvested from.
#' 3. *wait_room*: A staging directory where 'raw data' are standardised before being transferred/archived in the,
#' 4. *nomvet_room*: Folder housing standardised input data from multiple stations. By appending these standarised files station records (databases) are compiled. 
#' 5. *ipip_room*: The directory that compiles standardised data from the `nomvet_room` for each station. Station files can be further processed and exported into other formats from here.
#' 6. *raw_room*:  If defined [imbibe_raw_batch()] can harvest (copy or cut---see funtion documentation) and archive 'raw data' files from the `source_room` and will archive them in the `raw_room` in monthly folders.
#' \cr
#' {*} In older versions of 'ipayipi' the 'pipe_house_dir' argument was named 'work_dir'; this alteration prevents confusion with the R terminal/console working directory.
#'
#' ## Pushing data through the pipeline
#' Follow the sequence of functions below for an outline of processing data using ipayipi.
#' 1. Initiate pipeline housing.
#'     - [ipip_house()]: this function builds a pipe house directory.
#' 2. Importing raw data.
#'     - [logger_data_import_batch()]: to bring in raw data from a source directory.
#' 3. Imbibing and standardising raw data.
#'     - [imbibe_raw_batch()]: Reads imported data into the pipeline format.
#'     - [header_sts()]: For standardising header data, e.g., the station name or title.
#'     - [phenomena_sts()]: To get variables or phenomena data and metadata standardised.
#'     - [transfer_sts()]: Pushes standardised data to an archive for building station files.
#' 4. Appending data streams.
#'     - [append_station_batch()]: Builds station records.
#'     - [gap_eval_batch()]: Clarify & visualize data gaps.
#'     - [meta_read()] & [meta_to_station()]: Optional functions to incorporate field or other metadata into station records for further processing.
#' 5. Processing data.
#'     In order to process data a sequence of processing stages need to be defined (see `?pipe_seq`). Once defined, this sequence gets embedded into respective stations, evaluated and used to process data.
#'     - [dt_process_batch()]: To batch process data.
#' 6. Querying and visualising data.
#'     There are a few built-in plotting functions utilizing dygraphs, ggplot2, and plotly libraries.
#'     - [dta_availability()]: Check data availability within and across pipelines.
#'     - [dta_flat_pull()]: To harvest continuous data from stations in long or wide formats.
#'     - [dta_flat_pull_discnt()]: Similar to the above except for discontinuous data.
#'     - [plot_cumm()]: Visualise cummulative response of phenomena.
#'     - [plot_bar_agg()], [plotdy_drift()], [plot_cleanr()], and [plot_m_anomaly()] are more plotting functions.
#'     - Various plotting functions to examine/cross-examine data.
#' @md
#' @examples
#' # Inititate default pipeline in current working directory ----
#' pd <- "." # define the working directory
#' # assign pipeline directory list to `pipe_house`
#' pipe_house <- ipip_house(pipe_house_dir = pd)
#' print(pipe_house) # print the pipe_house object
#'
#' # Set the `raw_room` to NULL -- this will prevent archiving
#' # of raw data in the `raw_room`. Note that this can only be set to NULL
#' # after running the line of code above.
#' pipe_house$raw_room <- NULL
#'
#' # Attemp to create pipe house directory in random folder
#' # Note the message that the line below produces when the random directory
#' # does not exist.
#' ipip_house(pipe_house_dir = "rand_dir_doesnt_exist", verbose = TRUE)
#'
#' # Create a custom 'source_room' called 'dta_in'
#' sr <- "dta_in"
#' pipe_house <- ipip_house(pipe_house_dir = pd, source_room = sr)
#' print(pipe_house)
#' # Note how in the last example, now the source room is at "./dta_in"
#'
#' @author Paul J. Gordijn
#' @export
ipip_house <- function(
  pipe_house_dir = ".",
  r = NULL,
  source_room = NULL,
  wait_room = NULL,
  nomvet_room = NULL,
  ipip_room = NULL,
  dta_out = NULL,
  reports = NULL,
  raw_room = NULL,
  work_dir = NULL,
  verbose = FALSE,
  ...
) {

  if (!is.null(work_dir)) pipe_house_dir <- work_dir

  # dir 'names'
  dirs <- list("r", "source_room", "wait_room", "nomvet_room",
    "ipip_room", "dta_out", "reports", "raw_room"
  )
  dirs <- lapply(dirs, function(x) {
    z <- get(x)
    # lean and trailing forward and backward slashes
    if (!is.null(z)) z <- gsub("^[/]|^[//]|^[\\]|[/]$|[\\]$", "", z)
    if (is.null(z)) z <- file.path(pipe_house_dir, x)
    z <- gsub("^[/]|^[//]|^[\\]|[/]$|[\\]$", "", z)
    names(z) <- x
    return(z)
  })
  names(dirs) <- c("r", "source_room", "wait_room", "nomvet_room",
    "ipip_room", "dta_out", "reports", "raw_room"
  )
  # check that the working directory exists
  if (!file.exists(pipe_house_dir)) {
    cli::cli_abort(c(
      paste0("\'Pipe_room\' directory not found;",
        " manually create {.var pipe_house_dir} OR check path string!"
      ), "i" = "Current working directory is: \'{getwd()}\'.",
      "i" = paste0("The specified {.var pipe_house_dir} (= {pipe_house_dir})",
        " must be relative to the current working directory."
      ), "i" = "In older versions of ipayipi \'pipe_house\' was \'work_dir\'"
    ))
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
  dirs$pipe_house_dir <- pipe_house_dir
  if (verbose) {
    cli::cli_inform(c(
      "What next?",
      "v" = "Pipe house directory structure created.",
      " " = paste0("Begin processing data by housing new, raw-flat files in",
        " the {.var source_room}.",
      ), "i" = paste0("From the {.var source_room} data is copied to the ",
        "{.var wait_room} using the function:"
      ), " " = "{.var logger_data_import_batch()}"
    ))
  }
  return(dirs)
}
