#' @title ipayipi station to 'csv'
#' @description Exports standardised ipayipi data objects to csv format.
#' @inheritParams logger_data_import_batch
#' @inheritParams dta_list
#' @param input_dir The directory where standardised ipayipi data files to be are stored.
#' @param output_dir The outpur directory where the standardised ipayipi data object will be saved/exported to.
#' @param file_ext Character string of the file extension of the input data files. E.g., ".csv" for hobo rainfall file exports. This input character string should contain the period as in the previous sentence.
#' @param wanted_tabs Data items/tables that should be exported to csvs.  '%ilike%': ergo seperate multiple search phrases by '|'.
#' @keywords export R data; save data; csv format; batch export.
#' @export
#' @author Paul J. Gordijn
#' @return A list of station files which had tables exported to csv files.
#' @details This function will only export tables within a list item, not a list of tables within the list.
#' @md
ipayipi2csv <- function(
  input_dir = ".",
  pipe_house = NULL,
  output_dir = NULL,
  file_ext = ".ipip",
  wanted_tabs = NULL,
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  recurr = TRUE,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  "%ilike%" <- NULL
  # orgainise directories
  if (!is.null(pipe_house)) input_dir <- pipe_house$ipip_room

  # get list of available files
  station_files <- ipayipi::dta_list(
    input_dir = input_dir, file_ext = file_ext, prompt = prompt,
    recurr = FALSE, unwanted = NULL, wanted = wanted
  )

  if (length(station_files) == 0) {
    cli::cli_abort(c(
      "!" = "No station files found in: {pipe_house$ipip_room}"
    ))
  }

  # read in each file and export respective tables
  exported_stations <- future.apply::future_lapply(
    seq_along(station_files), function(z) {
      file <- readRDS(file.path(input_dir, station_files[z]))
      file_name <- basename(station_files[z])
      file_name <- gsub(pattern = file_ext, replacement = "", x = file_name,
        ignore.case = TRUE
      )
      tbl_names <- names(file)
      if (!is.null(wanted_tabs)) {
        tbl_names <- tbl_names[tbl_names %ilike% wanted_tabs]
        if (length(tbl_names) == 0 &&
            (verbose == TRUE || xtra_v == TRUE)
        ) {
          cli::cli_inform(c(
            "i" = "In station: {station_files[z]}",
            " " = "No tables matching: {.var wanted_tabs ==} {wanted_tabs}"
          ))
        }
      }
      xfiles <- lapply(seq_along(tbl_names), function(x) {
        if (any(!class(file[[tbl_names[x]]]) %in% "list")) {
          data.table::fwrite(file[[tbl_names[x]]],
            file = file.path(output_dir,
              paste0(file_name, "-", tbl_names[x], ".csv")
            ), row.names = FALSE, dateTimeAs = "write.csv",
            na = "NA"
          )
        }
        return(file_name)
      })
      rm(xfiles)
      invisible(file_name)
    }
  )
  invisible(exported_stations)
}