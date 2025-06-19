#' @title Import logger data
#' @description Copies and pastes data matching search criteria from `d1_source_room` into 'd2_wait_room'.
#' @inheritParams dta_list
#' @param pipe_house List of pipeline directories. \emph{See} [ipip_house()] \emph{for details}.
#' @keywords import logger data files; meteorological data; automatic weather station; batch process; hydrological data; time series data;
#' @author Paul J. Gordijn
#' @details `logger_data_import_batch()` copies logger data files from multiple stations in 'd1_source_room' into 'd2_wait_room' where data standardisation will take place using: 1) the `imbibe_raw_batch()`, 2) `header_sts()`, and 3) `phenomena_sts()`, in that order. Once standardised in native R format files the data is transferred into the `d3_nomvet_room` with `transfer_sts_files()`. The standardised files in the 'd3_nomvet_room' are kept there and station records are developed in the 'd4_ipip_room' using `append_station_batch()`. Data can be furhter processed with `dt_process_batch()`. \cr
#' # Notes:
#' * This function will process data from multiple stations where their raw-flat files are stored in the 'd1_source_room'. 
#' * Duplicate file names will have unique consequtive integers added as a suffix---so no files are ignored if named similarly.
#' @export
logger_data_import_batch <- function(
  pipe_house = NULL,
  wanted = NULL,
  unwanted = NULL,
  recurr = TRUE,
  file_ext = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  prompt = FALSE,
  ...
) {
  ":=" <- NULL
  "fsnew" <- NULL
  # get list of data to be imported
  unwanted <- paste("['.']ipr|['.']ipi|['.']iph|['.']xls|['.']rps",
    "['.']rns|['.']ods|['.']doc|['.']md|d2_wait_room",
    unwanted, sep = "|"
  )
  unwanted <- gsub(pattern = "^\\||\\|$", replacement = "", x = unwanted)
  slist <- ipayipi::dta_list(input_dir = pipe_house$d1_source_room,
    file_ext = file_ext, prompt = prompt, recurr = recurr,
    unwanted = unwanted, wanted = wanted
  )
  if (length(slist) == 0) {
    exit_m <- cli::cli_inform(c(
      paste0("No files detected in the pipe house \'source room\'",
        " ({pipe_house$d1_source_room})"
      ), "i" = paste0("New data files are pulled into the pipeline from",
        " {.var pipe_house$d1_source_room}."
      )
    ))
    return(exit_m)
  }
  if (verbose || xtra_v) cli::cli_h1(
    "Importing {length(slist)} data file{?s} into the {.var d2_wait_room}"
  )
  slist_df <- data.table::data.table(name = slist, basename = basename(slist),
    rep = rep(NA_integer_, length(slist))
  )
  slist_dfs <- split(slist_df, f = factor(slist_df$basename))
  slist_dfs <- lapply(slist_dfs, function(x) {
    x$rep <- seq_len(nrow(x))
    x
  })
  slist_dfs <- data.table::rbindlist(slist_dfs)
  slist_dfs$file_ext <- tools::file_ext(slist_dfs$name)
  slist_dfs[, file_ext := paste0(
    ".", sub(pattern = "\\.", replacement = "", file_ext)
  )]
  slist_dfs$fsf <- file.path(pipe_house$d1_source_room, slist_dfs$name)
  slist_dfs[, fsnew := vgsub(
    pattern = file_ext, replacement = "", x = basename, ignore.case = TRUE
  )]
  slist_dfs[, fsnew := file.path(pipe_house$d2_wait_room, paste0(
    fsnew, "__", rep, file_ext
  ))]
  file.copy(from = slist_dfs$fsf, to = slist_dfs$fsnew, overwrite = TRUE)
  if (verbose || xtra_v) cli::cli_bullets(c(
    "v" = "{length(slist_dfs$basename)} file{?s} imported ..."
  ))

  if (verbose || xtra_v) cli::cli_h1("")
  if (verbose || xtra_v) cli::cli_inform(c(
    " " = "What next?",
    "v" = "data successfully copied to the {.var d2_wait_room}.",
    ">" = paste0("Now use the {.var imbibe_raw_batch()} function to convert",
      " into \'ipayipi' format data files."
    ),
    " " = paste0("Be sure to select the correct \'data_setup\' option ",
      "for your data being imbibed --- see the imbibe_raw_batch() ",
      "helpfiles."
    ),
    "i" = paste0("If your import was incorrect check the pipe house",
      " \'d1_source_room\' was defined correctly. And use \'wanted\' and",
      " \'unwanted\' args to filter files being imported."
    ),
    "i" = paste0("See {.var imbibe_raw_batch()} helpfiles for more detail!",
      " ({.var ?imbibe_raw_batch()})."
    )
  ))
  invisible(slist_dfs)
}