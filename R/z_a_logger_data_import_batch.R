#' @title Import logger data
#' @description Locates, copies, then pastes logger data files into the 'wait_room'. Duplicate file names will have unique consequtive integers added as a suffix.
#' @param pipe_house List of pipeline directories. __See__ `ipayipi::ipip_house()` __for details__.
#' @param prompt Should the function use an interactive file selection function otherwise all files are returned. `TRUE` or `FALSE`.
#' @param recurr Should the function search recursively into sub directories for hobo rainfall csv export files? `TRUE` or `FALSE`.
#' @param wanted Vector of strings listing files that should not be included in the import.
#' @param unwanted Vector of strings listing files that should not be included in the import.
#' @param file_ext The file extension defaults to ".dat". Other file types could be incorporatted if required.
#' @param verbose Print some details on the files being processed? Logical.
#' @keywords import logger data files; meteorological data; automatic weather station; batch process; hydrological data;
#' @author Paul J. Gordijn
#' @details Copies logger data files into a directory where further data standardisation will take place in the 'ipayipi' data pipeline. Once the files have been standardised in native R format files the data is transferred into `nomvet_room`.
#' @export
logger_data_import_batch <- function(
  pipe_house = NULL,
  prompt = FALSE,
  recurr = TRUE,
  wanted = NULL,
  unwanted = NULL,
  file_ext = NULL,
  verbose = FALSE,
  ...
) {

  # get list of data to be imported
  unwanted <- paste0("['.']ipr|['.']ipi|['.']iph|['.']xls|['.']rps",
    "['.']rns|['.']ods|['.']doc|['.']md|wait_room",
    unwanted, collapse = "|"
  )
  unwanted <- gsub(pattern = "^\\||\\|$", replacement = "", x = unwanted)
  slist <- ipayipi::dta_list(input_dir = pipe_house$source_room,
    file_ext = file_ext, prompt = prompt, recurr = recurr,
    unwanted = unwanted, wanted = wanted
  )
  cr_msg <- padr(core_message = paste0(" Introducing ", length(slist),
      " data files into the pipeline waiting room", collapes = ""
    ), wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0)
  )
  ipayipi::msg(cr_msg, verbose)
  slist_df <- data.table::data.table(name = slist, basename = basename(slist),
    rep = rep(NA_integer_, length(slist))
  )
  slist_dfs <- split(slist_df, f = factor(slist_df$basename))
  slist_dfs <- lapply(slist_dfs, function(x) {
    x$rep <- seq_len(nrow(x))
    return(x)
  })
  slist_dfs <- data.table::rbindlist(slist_dfs)
  future.apply::future_lapply(seq_len(nrow(slist_dfs)), function(x) {
    if (is.null(file_ext)) {
      file_ext <- tools::file_ext(slist_dfs$name[x])
      file_ext <- paste0(".", sub(pattern = "\\.", replacement = "", file_ext))
    }
    file.copy(
      from = file.path(pipe_house$source_room, slist_dfs$name[x]),
      to = file.path(pipe_house$wait_room,
        paste0(gsub(pattern = file_ext, replacement = "",
            x = slist_dfs$basename[x], ignore.case = TRUE
          ), "__", slist_dfs$rep[x], file_ext
        )
      ), overwrite = TRUE
    )
    cr_msg <- padr(core_message =
        paste0(basename(slist[x]), " done ...", collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(-1, 2)
    )
    ipayipi::msg(cr_msg, verbose)
  })

  cr_msg <- padr(core_message =
      paste0("  import complete  ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0)
  )
  return(ipayipi::msg(cr_msg, verbose))
}