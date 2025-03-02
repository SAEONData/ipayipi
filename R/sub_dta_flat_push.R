#' @title Pushes flattened station data into station files
#' @description Effectively the reverse of `ipayipi::dta_flat_pull()`.
#' @param pipe_house The directory in which to search for stations form which to extract time-series data. If working across different 'pipe_house' directories a 'pseudo' pipehouse object can be created, e.g., pipe_house <- list(ipip_room = "."); only the 'ipip_room' is listed here as this is the standard folder where stations are kept.
#' @param flat_dta Long format data to be pushed back to respective stations and tables replacing extant station data.
#' @param tbl_names Vector of table names in a station files from which to extract data. Add items to the vector such that only one table per station is selected. Table names are selected via the `%in%` argument.
#' @param phen_name The field/column/name of the phenomena which to join into a single data table.
#' @param ri Record-interval string. This function will guess the record interval used to generate a flat table. However, the guess may not be possible if there is insufficient data.
#' @param output_dir The output directory where an output csv file is saved.
#' @param wanted A strong containing keywords to use to filter which stations are selected for processing. Multiple search kewords should be seperated with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param out_csv Logical. If TRUE a csv file is exported to the output directory.
#' @param out_csv_preffix Preffix for the output csv file. The phenomena name and then time interval by which the data are summarised are used as a suffix.
#' @param file_ext The extension of the stations from where data will be extracted. Defaults to ".ipip".
#' @param prompt Logical. If `TRUE` a prompt will be called so that the user can interactively select station files.
#' @details Notethis function only extracts data from compressed station files. Development is required to modify this function to extract from 'temporary station files' to spped up data extraction from large files.
#' @keywords data pipeline; summarise time-series data; long-format data.
#' @return A list containing 1) the summarised data in a single data.table, 2) a character string representing the time interval by which the data has been summarised, 3) the list of stations used for the summary, 4) the name of the table and the name of the field for which data was summarised.
#' @author Paul J. Gordijn
#' @export
dta_flat_push <- function(
  pipe_house = NULL,
  flat_dta = NULL,
  tbl_names = NULL,
  phen_name = NULL,
  ri = NULL,
  preds = TRUE,
  stations = NULL,
  recurr = TRUE,
  file_ext = ".ipip",
  ...
) {
  ":=" <- "%ilike%" <- "%chin%" <- NULL
  "date_time" <- NULL
  if (length(stations) == 0) return(NULL)

  slist <- file.path(pipe_house$ipip_room, paste0(stations))
  names(slist) <- gsub(paste0(file_ext, "$"), "", basename(slist))

  # check uniqueness of station names
  if (length(unique(names(slist))) != length(slist)) {
    cli::cli_warn("Non-unique station names---can't process!")
    return(NULL)
  }
  # check all stations exist
  if (any(!file.exists(gsub("^\\./", "", slist), recursive = TRUE))) {
    cli::cli_warn("Missing station in set directory!")
    return(NULL)
  }

  # write data to stations--tables via sfc connection
  t <- lapply(seq_along(slist), function(i) {
    sfc <- sf_open_con(pipe_house = pipe_house, station_file = slist[i],
      tv = tbl_names
    )
    sfn <- gsub(file_ext, "", basename(slist[i]))
    sfn <- gsub("^['.']|['.']$", "", sfn)
    ndt <- flat_dta[!is.na(sfn), names(flat_dta)[
      names(flat_dta) %ilike% paste0("date_time|", names(slist[i]))
    ], with = FALSE, env = list(sfn = sfn)]
    nname <- paste0(phen_name, "_newww")
    tn <- tbl_names[tbl_names %chin% names(sfc)][1]
    data.table::setnames(ndt, names(slist[i]), nname)
    # read old data, merge new, then overwrite
    dta <- sf_dta_read(
      sfc = sfc, tv = tbl_names[tbl_names %chin% names(sfc)][1]
    )
    ppsij <- data.table::data.table(start_dttm = min(ndt$date_time),
      end_dttm = max(ndt$date_time)
    )
    dta <- dt_dta_open(dta_link = dt_dta_filter(dta, ppsij = ppsij))
    # left join data sets together
    dtn <- ndt[dta, on = "date_time", mult = "first"][,
      phen_name := data.table::fifelse(!is.na(nname), nname, phen_name),
      env = list(phen_name = phen_name, nname = nname)
    ]
    dtn <- dtn[, names(dtn)[!names(dtn) %ilike% "_newww$"], with =  FALSE]
    data.table::setcolorder(dtn)
    if ("id" %in% names(dtn)) {
      data.table::setcolorder(dtn, "id", before = "date_time")
    }

    # write data to sfc
    sf_dta_wr(dta_room = file.path(dirname(sfc[1]), tn), dta = dtn,
      tn = tn, sfc = sfc, overwrite = TRUE, rit = "continuous",
      ri = ri
    )

    # add on extra date_time stamps -- preds
    if (preds) {
      pdta <- ndt[!date_time %in% dtn$date_time]
      data.table::setnames(pdta, nname, phen_name)
      sf_dta_wr(dta_room = file.path(dirname(sfc[1]), tn), dta = pdta,
        tn = tn, sfc = sfc, overwrite = TRUE, rit = "continuous",
        ri = ri
      )
    }

    # write station
    sf_write(pipe_house = pipe_house, station_file = slist[i])
    return(tn)
  })
  return(t)
}