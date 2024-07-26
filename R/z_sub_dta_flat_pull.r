#' @title Pulls multiple station data into a 'flat' file
#' @description Summarises data by the longest time interval for a particular phenomena wihtin the specified data table in an `ipayipi` station object.
#'  The function can be used to summarise time-series data from a number of stations and the output saved to a csv file.
#' @param input_dir The directory in which to search for stations form which to extract time-series data.
#' @param pipe_house If the `pipe_house` argument is provided the `pipe_house$ipip_room` will be used instead of the `input_dir`.
#' @param tab_names Vector of table names in a station files from which to extract data. Add items to the vector such that only the first matching table from a station is selected. Table names are selected via the `%in%` argument.
#' @param phen_name The field/column/name of the phenomena which to join into a single data table.
#' @param ri Record-interval string. This function will guess the record interval used to generate a flat table. However, the guess may not be possible if there is insufficient data.
#' @param output_dir The output directory where an output csv file is saved.
#' @param wanted A strong containing keywords to use to filter which stations are selected for processing. Multiple search kewords should be seperated with a bar ('|'), and spaces avoided unless part of the keyword.
#' @param unwanted Similar to wanted, but keywords for filtering out unwanted stations.
#' @param out_csv Logical. If TRUE a csv file is exported to the output directory.
#' @param out_csv_preffix Preffix for the output csv file. The phenomena name and then time interval by which the data are summarised are used as a suffix.
#' @param recurr Whether to search recursively through folders. Defaults to TRUE.
#' @param cores  Number of CPU's to use for processing in parallel. Only applies when working on Linux systems.
#' @param file_ext The extension of the stations from where data will be extracted. Defaults to ".ipip".
#' @param prompt Logical. If `TRUE` a prompt will be called so that the user can interactively select station files.
#' @param verbose Whether to report on progress.
#' @param xtra_v Extra verbose. Logical.
#' @details Note this function only extracts data from decompressed station files. If a station has not be decompressed the function will take time decompressing.
#' @keywords data pipeline; summarise time-series data; long-format data.
#' @return A list containing 1) the summarised data in a single data.table, 2) a character string representing the time interval by which the data has been summarised, 3) the list of stations used for the summary, 4) the name of the table and the name of the field for which data was summarised.
#' @author Paul J. Gordijn
#' @export
dta_flat_pull <- function(
  input_dir = ".",
  pipe_house = NULL,
  tab_names = NULL,
  phen_name = NULL,
  ri = NULL,
  output_dir = NULL,
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  out_csv = FALSE,
  out_tab_name = NULL,
  out_csv_preffix = "",
  recurr = TRUE,
  cores = getOption("mc.cores", 2L),
  file_ext = ".ipip",
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  ":=" <- NULL
  "stnd_title" <- NULL

  # orgainise directories
  if (!is.null(pipe_house)) input_dir <- pipe_house$ipip_room

  # merge data sets into a station for given time periods
  slist <- ipayipi::dta_list(input_dir = input_dir, file_ext = file_ext,
    prompt = prompt, recurr = recurr, unwanted = unwanted, wanted = wanted
  )
  sn <- gsub(paste0(file_ext, "$"), "", basename(slist))
  if (anyDuplicated(sn) > 0) {
    message("Reading duplicated stations ('stnd_title') not allowed!")
    message("Refine search keywords using the 'un\\wanted arguments")
    print(slist[order(sn)])
    return(NULL)
  }
  if (length(slist) == 0) return(NULL)
  # extract all relevant tables from the data
  t <- lapply(slist, function(x) {
    # open station file connections
    sfc <- ipayipi::open_sf_con(station_file = file.path(input_dir, x),
      verbose = verbose, xtra_v = xtra_v, cores = cores, tmp = TRUE
    )
    tab_name <- names(sfc)[names(sfc) %in% tab_names]
    if (length(tab_name) > 0) {
      tn <- basename(sfc[[tab_name[1]]])
    } else {
      tn <- NULL
    }
    t <- sf_dta_read(sfc = sfc, tv = tn, tmp = TRUE)
    invisible(t)
  })
  names(t) <- slist
  t <- t[!sapply(t, is.null)]
  t <- t[sapply(t, function(x) phen_name %in% names(x[[1]]$indx$dta_n))]

  # prep to join datasets together
  # check dataset ri's
  if (!is.null(ri)) ri <- ipayipi::sts_interval_name(ri)[["sts_intv"]]
  if (is.null(ri)) ri <- t[[1]][[1]]$indx$ri
  ri_chk <- sapply(t, function(x) x[[1]]$indx$ri) %in% ri
  if (any(!ri_chk)) {
    ipayipi::msg("Record-interval mismatch", xtra_v)
    print(sapply(t, function(x) x[[1]]$indx$ri))
  }
  t <- t[ri_chk]
  mn <- data.table::rbindlist(
    lapply(t, function(x) data.table::data.table(mn = x[[1]]$indx$mn))
  )
  # check that all starting points are equal
  mn$dur <- lubridate::as.duration(mn$mn - lubridate::round_date(mn$mn))
  d <- mn$dur[1]
  mn$dur_chk <- mn$dur == mn$dur[1]
  mn[, stnd_title := gsub(paste0(file_ext, "$"), "", basename(names(t)))]
  if (!all(mn$dur_chk)) {
    ipayipi::msg("Unequal date-time starting points. Removing station tables: ")
    names(mn)[1] <- "Start_dttm"
    print(mn)
    t <- t[mn$dur_chk]
  }
  mn <- data.table::rbindlist(
    lapply(t, function(x) data.table::data.table(mn = x[[1]]$indx$mn))
  )
  mx <- data.table::rbindlist(
    lapply(t, function(x) data.table::data.table(mx = x[[1]]$indx$mx))
  )
  dt <- data.table::data.table(
    date_time = seq(min(mn$mn), max(mx$mx), by = ri) + d
  )

  dti <- parallel::mclapply(seq_along(t), function(i) {
    # add hsf_phens to the dta_link
    t[[i]][[names(t[[i]])[1]]]$hsf_phens <- phen_name
    dti <- dt_dta_open(t[[i]])
    dti <- dti[dt, on = "date_time"][, phen_name, with = FALSE]
    data.table::setnames(dti, phen_name,
      gsub(paste0(file_ext, "$"), "", basename(names(t[i])))
    )
    return(dti)
  }, mc.cores = cores)
  dti <- do.call(cbind, args = c(list(dt), dti))
  data.table::setcolorder(
    dti, c("date_time", names(dti)[!names(dti) %in% "date_time"])
  )
  slist <- names(t)

  if (is.null(out_tab_name)) out_tab_name <- gsub(" ", "_", ri)
  dta <- list(dta = dti, time_interval = gsub(" ", "_", ri), stations = slist,
    tab_names = tab_names, phen_name = phen_name, file_ext = file_ext
  )
  if (out_csv) {# save the csv file
    data.table::fwrite(dta$dta, file = file.path(output_dir, paste0(
      out_csv_preffix, "_", out_tab_name, "_", phen_name, ".csv"
    )), na = NA, dateTimeAs = "write.csv")
  }
  return(dta)
}