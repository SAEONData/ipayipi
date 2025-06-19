#' Short internal functions
#'
#' Turns parameters into a list
#' An alias for list in pipe processing.
#' Paul J. Gordijn
#' @noRd
#' @export

# internal function to return logical if future plan mode is not sequential
fcoff <- function(...) {
  "%ilike%" <- NULL
  x <- future::future(getOption("future.class"))
  if (any(class(x) %ilike% "sequential")) {
    return(FALSE)
  }
  TRUE
}

# flat file imbibe options for fread ----
# default read for imbibing data
#' @export
deft_read <- function(
  file_path = NULL,
  col_dlm = NULL,
  nrows = 250,
  dsi = NULL,
  xtra_v = FALSE,
  ...
) {
  file <- data.table::fread(file = file_path, header = FALSE, nrows = nrows,
    check.names = FALSE, blank.lines.skip = FALSE, sep = col_dlm,
    showProgress = xtra_v, strip.white = FALSE, fill = TRUE
  )
  return(list(file = file, dsi = dsi))
}
#' @export
# function to dynamically read in flat file and correct header info
dyno_read <- function(file_path = NULL, col_dlm = NULL, nrows = 250,
  dsi = NULL, xtra_v = FALSE, ...
) {
  file <- data.table::fread(file_path, header = FALSE, nrows = nrows, ...)
  l <- R.utils::countLines(file_path)[1]
  if (is.infinite(nrows)) {
    r <- nrow(file)
  } else {
    r <- nrow(data.table::fread(file_path, header = FALSE, nrows = Inf, ...))
  }
  file_head <- readLines(file_path)[1:(l - r)]
  file_head <- lapply(file_head, function(x) {
    x <- strsplit(x, split = col_dlm)
    x <- unlist(c(x, rep(NA, ncol(file) - length(x))))
    x <- lapply(x, function(z) gsub("\"", "", z))
    names(x) <- names(file)
    data.table::as.data.table(x)
  })
  file_head <- suppressWarnings(data.table::rbindlist(file_head, fill = TRUE))
  file <- suppressWarnings(rbind(file_head, file, use.names = TRUE))
  return(list(file = file, dsi = dsi))
}
#' @export
# function to dynamically read in flat file and correct header info
# relaxed version of dyno read that relaxes the condition of
# inconsistent column numbers with rbind arg, fill = TRUE
dyno_read2 <- function(file_path = NULL, col_dlm = NULL, nrows = 250,
  dsi = NULL, xtra_v = FALSE, ...
) {
  file <- data.table::fread(file_path, header = FALSE, nrows = nrows, ...)
  l <- R.utils::countLines(file_path)[1]
  if (is.infinite(nrows)) {
    r <- nrow(file)
  } else {
    r <- nrow(data.table::fread(file_path, header = FALSE, nrows = Inf, ...))
  }
  file_head <- readLines(file_path)[1:(l - r)]
  file_head <- lapply(file_head, function(x) {
    x <- strsplit(x, split = col_dlm)
    x <- unlist(c(x, rep(NA, ncol(file) - length(x))))
    x <- lapply(x, function(z) gsub("\"", "", z))
    names(x) <- names(file)
    data.table::as.data.table(x)
  })
  suppressWarnings(file_head <- data.table::rbindlist(file_head, fill = TRUE))
  file <- suppressWarnings(
    rbind(file_head, file, use.names = TRUE, fill = TRUE)
  )
  return(list(file = file, dsi = dsi))
}

#' Turns parameters into a list
#' An alias for list in pipe processing.
#' Paul J. Gordijn
#' @noRd
#' @export

# internal function to return logical if future plan mode is not sequential
xp <- function(x) {
  expression(x)
  class(x) <- c(class(x), "ipip_expr")
  x
}

#' @export
# flat_cor_mat ----
# function to get 'flat' table from Hmisc matrix
flat_cor_mat <- function(cor_mat) {
  "stnd_title" <- "r2" <- NULL
  ut <- upper.tri(cor_mat$r)
  cor_flat1 <- data.table::data.table(
    stnd_title = rownames(cor_mat$r)[col(cor_mat$r)[ut]],
    stnd_title_cor = rownames(cor_mat$r)[row(cor_mat$r)[ut]],
    r2  = (cor_mat$r)[ut],
    p = cor_mat$P[ut]
  )
  cor_flat2 <- cor_flat1
  names(cor_flat2)[1:2] <- names(cor_flat1)[2:1]
  cor_flat <- unique(rbind(cor_flat1, cor_flat2))
  cor_flat[order(stnd_title, -r2)]
}

#' @export
# fna ----
# Function to fill na's in a data table with non-na values in column
fna <- function(x, y) {
  if (any(!is.na(x[[y]]))) {
    x[is.na(x[[y]]), y]  <- x[[y]][!is.na(x[[y]])][1]
  }
  x
}

#' @export
# get_raw_gaps ----
# internal function to extact info for gap plotting from raw data
# used in function: dta_availability()
get_raw_gaps <- function(
  pipe_house = NULL,
  slist = NULL,
  input_dir = ".",
  phen_names = NULL,
  phen_eval = TRUE,
  station_ext = ".ipip",
  gap_problem_thresh_s = 6 * 60 * 60,
  event_thresh_s = 10 * 60,
  start_dttm = NULL,
  end_dttm = NULL,
  tbl_names = "^raw_",
  meta_events = "meta_events",
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  ...
) {
  "." <- ":=" <- "%chin%" <- "%ilike%" <- NULL
  "stnd_title" <- "station" <- "table_name" <- "gid" <-
    "problem_gap" <- "gap_start" <- "gap_end" <- "phen" <-  NULL

  # check if the plot tbls are present in data summaries
  sedta <- lapply(seq_along(slist), function(i) {
    sfc <- sf_open_con(pipe_house = pipe_house, station_file = slist[i])
    ds_null <- data.table::data.table(
      start_dttm = as.POSIXct(NA_character_),
      end_dttm = as.POSIXct(NA_character_),
      stnd_title = NA_character_, table_name = NA_character_
    )[0]
    # need an option here for non-raw data
    if (any(tbl_names %ilike% "^dt_")) {
      tbl_ndt <-  tbl_names[tbl_names %ilike% "^dt_"]
      ds_ndt <- data.table::rbindlist(lapply(tbl_ndt, function(x) {
        if (!x %chin% names(sfc)) return(ds_null)
        ndsx <- sf_dta_read(sfc = sfc, tv = x)
        ndsx <- data.table::data.table(
          start_dttm = ndsx[[x]]$indx$mn, end_dttm = ndsx[[x]]$indx$mx,
          stnd_title = gsub(station_ext, "", basename(slist[i])),
          table_name = x
        )
      }))
    } else {
      ds_ndt <- ds_null
    }
    if (any(tbl_names %ilike% "raw_")) {
      ds <- sf_dta_read(sfc = sfc, tv = "data_summary")[["data_summary"]]
      if (is.null(ds)) cli::cli_abort("No data summary in station!")
      ds <- ds[, .(start_dttm, end_dttm, stnd_title, table_name)]
      tbl <- unique(ds$table_name)
      # select tbl based on ordering of tbl_names
      if (!is.null(tbl_names)) {
        tbl_names <- paste(tbl_names, collapse = "|")
        tbln <- tbl[tbl %ilike% tbl_names]
        tbl <- tbln[order(tbln)]
        ds <- ds[table_name %chin% c(tbl)]
      }
    } else {
      ds <- ds_null
    }
    ds <- rbind(ds, ds_ndt)
    # set ds to null if no tbl name match
    if (nrow(ds) == 0) ds <- NULL
    ds
  })

  slist_null <- slist[sapply(sedta, is.null)]
  if (length(slist_null) > 0) {
    cli::cli_inform(c(
      "i" = "These stations: {slist_null}",
      " " = "Have no tables named {tbl_names} in their data summary."
    ))
  }
  # remove stations with no appropriate tables
  slist <- slist[!sapply(sedta, is.null)]
  sedta <- sedta[!sapply(sedta, is.null)]

  # generate station numbers ----
  sedta <- lapply(seq_along(sedta), function(i) {
    sedta[[i]]$station_n <- rev(seq_along(slist))[i]
    sedta[[i]] <- split.data.frame(sedta[[i]],
      f = factor(paste(sedta[[i]]$station_n, sedta[[i]]$table_name, sep = "; "))
    )
    sedta[[i]]
  })
  sedta <- unlist(sedta, recursive = FALSE)

  # get max and min dttm's for table data
  sedta <- lapply(seq_along(sedta), function(i) {
    ds <- sedta[[i]]
    ds$start_dttm <- min(ds$start_dttm)
    ds$end_dttm <- max(ds$end_dttm)
    ds <- ds[c(1, nrow(ds)), ]
    ds <- unique(ds)
    ds$data_yes <- rev(seq_along(sedta))[i] # data_yes = plt y-axis
    ds
  })
  sedta <- data.table::rbindlist(sedta)

  # extract data for gaps ----
  gaps <- lapply(slist, function(x) {
    sfc <- sf_open_con(station_file = x, pipe_house = pipe_house, tv = "gaps")
    dta <- sf_dta_read(sfc = sfc, tv = "gaps")
    dta$gaps
  })

  # produce gap tables where they are missing
  run_gaps <- slist[sapply(gaps, is.null)]
  run_gap_gaps <- lapply(run_gaps, function(x) {
    print(x)
    sfc <- sf_open_con(pipe_house = pipe_house, station_file = x)
    g <- gap_eval(pipe_house = pipe_house, station_file = x, sfc = sfc,
      gap_problem_thresh_s = gap_problem_thresh_s, event_thresh_s =
        event_thresh_s, meta_events = meta_events,
      tbl_n = "^raw_*",
      phen_eval = phen_eval, verbose = verbose, xtra_v = xtra_v,
      chunk_v = chunk_v
    )
    ipayipi::sf_write(pipe_house = pipe_house, sf = g$gaps, station_file = x,
      overwrite = TRUE, append = TRUE, chunk_v = chunk_v
    )
    g$gaps <- g$gaps[problem_gap == TRUE]
    invisible(g$gaps)
  })
  names(run_gap_gaps) <- run_gaps

  gaps <- c(gaps[!sapply(gaps, is.null)], run_gap_gaps)
  gaps_null <- gaps[[length(gaps)]][0]
  gs <- lapply(seq_along(gaps), function(i) {
    s <- sub("\\.ipip", "", names(gaps[i]))
    gaps[[i]]$station <- s
    # select tbl based on seddta tbls
    tbl <- unique(sedta[stnd_title %chin% s]$table_name)
    gaps[[i]] <- gaps[[i]][table_name %chin% tbl]
    if (!is.null(phen_names)) {
      gaps[[i]] <- gaps[[i]][phen %chin% unique(c("logger", phen_names))]
    }
    split.data.frame(gaps[[i]][problem_gap == TRUE],
      f = as.factor(gaps[[i]][problem_gap == TRUE]$table_name)
    )
  })
  gs <- unlist(gs, recursive = FALSE)

  no_gaps <- sapply(gs, function(x) x$station[1])
  no_gaps <- sedta$stnd_title[!sedta$stnd_title %in% no_gaps]
  no_gaps <- sedta[stnd_title %in% no_gaps]
  no_gaps <- data.table::data.table(
    dtt0 = c(no_gaps$start_dttm, no_gaps$end_dttm)
  )

  gs <- lapply(gs, function(x) {
    tn <- x$table_name[1]
    s <- basename(x$station[1])
    x$station_n <- sedta[stnd_title == s]$station_n[1]
    data_yes <- sedta[stnd_title == s & table_name == tn]$data_yes[1]
    x$data_yes <- data_yes
    x$gap_yes <- data_yes
    x
  })

  gs <- data.table::rbindlist(c(gs, list(gaps_null)), fill = TRUE,
    use.names = TRUE
  )
  gs <- gs[, ":="(start_dttm = gap_start, end_dttm = gap_end)]
  sedta <- sedta[, station := stnd_title][, gid := ""]
  gs <- rbind(gs, sedta, use.names = TRUE, fill = TRUE)
  gs$table_wrd <- paste0(gs$station, ": ", gs$data_yes)

  return(list(gs = gs, sedta = sedta))
}

#' @export
# median_f ----
# Estimates the median
median_f <- function(x, na.rm = TRUE, ...) {
  stats::median(x, na.rm = na.rm)
}

#' @export
# mad_f ----
# Estimates the median absolute deviation
mad_f <- function(x, na.rm = TRUE, ...) {
  stats::mad(x, na.rm = na.rm)
}

#' @export
# dt param eval ----
params <- function(...) list(...)

#' @export
# general fs ----
# Function that returns the mode
getmode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}

#' @export
# vectoriesd version of gsub for using in data.table
vgsub <- Vectorize(gsub, vectorize.args = c("pattern", "replacement", "x"))

#' @export
# test function to purge a pipe house dir of a station
spurge <- function(
  pipe_house = NULL,
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  ...
) {
  "%chin%" <- "%ilike%" <- NULL
  "stnd_title" <- NULL
  chosen <- function() {
    input_dir <- pipe_house$d4_ipip_room
    slist <- dta_list(input_dir = input_dir, file_ext = ".ipip",
      wanted = wanted, unwanted = unwanted, prompt = prompt, recurr = TRUE
    )
    cli::cli_inform(c(
      "!" = paste0("Are you sure you want to delete {length(slist)}",
        " file{?s} from the \'d4_ipip_room\'?"
      )
    ))
    df <- data.frame(Stations = slist, stringsAsFactors = FALSE)
    print(df)
    n <- readline(
      prompt =
        "Confirm chosen stations with Y/n:  quit with {.var q} : "
    )
    # trim white space with regex
    n <- gsub("^\\s+|\\s+$", "", n)
    if (!n %chin% c("Y", "n", "q")) {
      cli::cli_inform(
        c("i" = "Please enter recognised characters {.var Y/n/q}.")
      )
    }
    # quit as prompted
    if (n %in% c("q", "n")) {
      cli::cli_inform("\'{n}\' entered. Quitting station purge.")
      return(invisible())
    }
    # delete files
    if (n == "Y") {
      unlink(file.path(input_dir, slist), recursive = TRUE)
      sf_tmp <- file.path(getOption("chunk_dir"), "sf", basename(slist))
      sf_tmp <- sf_tmp[file.exists(sf_tmp)]
      unlink(sf_tmp, recursive = TRUE)
    }
    n <- readline(
      prompt = paste0("Remove associated files in the \'d3_nomvet_room\'?",
        " Y/n:  quit with <q> : "
      )
    )
    # trim white space with regex
    n <- gsub("^\\s+|\\s+$", "", n)
    if (!n %chin% c("Y", "n", "q")) {
      cli::cli_inform(c("i" = "Please enter recognised characters."))
    }
    # quit as prompted
    if (n %in% c("q", "n")) {
      cli::cli_inform("\'{n}\' entered. Quitting station purge.")
      return(invisible())
    }
    if (length(slist) == 0) slist <- wanted
    if (n == "Y") {
      z <- sapply(slist, function(x) gsub("*['.']ipip$", ".*.ipi$", x))
      z <- paste0("^", z)
      input_dir <- pipe_house$d3_nomvet_room
      sapply(z, function(x) {
        nlist <- dta_list(input_dir = input_dir, wanted = x,
          unwanted = NULL, prompt = FALSE, recurr = TRUE
        )
        unlink(file.path(input_dir, nlist))
      })
      # update the aa_nomvet.log file
      nomfs <- gsub("*['.']ipip$", "", slist)
      nomfs <- paste(nomfs, collapse = "|")
      # open inventory in nomvet room (else create)
      if (!file.exists(file.path(pipe_house$d3_nomvet_room, "aa_nomvet.log"))) {
        nr_lg <- ipayipi_data_log(log_dir = pipe_house$d3_nomvet_room,
          file_ext = ".ipi"
        )
        nr_lg <- nr_lg$log[, -"input_file", with = FALSE]
        saveRDS(nr_lg, file.path(pipe_house$d3_nomvet_room, "aa_nomvet.log"))
      }
      nr_lg <- readRDS(file.path(pipe_house$d3_nomvet_room, "aa_nomvet.log"))
      nr_lg <- nr_lg[!stnd_title %ilike% nomfs]
      saveRDS(nr_lg, file.path(pipe_house$d3_nomvet_room, "aa_nomvet.log"))
      if (nrow(nr_lg) < 1) {
        unlink(file.path(pipe_house$d3_nomvet_room, "aa_nomvet.log"))
      }
    }
  }
  chosen()
}

#' @export
# test function to purge a pipe house wait room
wpurge <- function(
  pipe_house = NULL,
  prompt = FALSE,
  wanted = NULL,
  unwanted = NULL,
  ...
) {
  ".SD" <- ":=" <- NULL
  "nas" <- NULL
  input_dir <- pipe_house$d2_wait_room
  # remove imported files
  slist <- dta_list(input_dir = input_dir, wanted = wanted,
    unwanted = unwanted, prompt = prompt, recurr = TRUE
  )
  unlink(file.path(input_dir, slist))

  # clean up the header and phenomena standards
  nf <- list.files(path = input_dir, pattern = "^aa_nomtab.*.rns")
  lapply(nf, function(x) {
    n <- readRDS(file.path(input_dir, x))
    sdcols <- c("location", "station", "stnd_title", "logger_type",
      "record_interval_type", "record_interval", "table_name"
    )
    n <- n[, nas := rowSums(is.na(.SD)) > 0, .SDcols = sdcols][,
      -c("nas"), with = FALSE
    ]
    saveRDS(n, file.path(input_dir, x))
  })
  nf <- list.files(path = input_dir, pattern = "^aa_nomtab.*.csv")
  unlink(file.path(input_dir, nf))
  pf <- list.files(path = input_dir, pattern = "^aa_phentab.*.rps")
  lapply(pf, function(x) {
    n <- readRDS(file.path(input_dir, x))
    sdcols <- c("phen_name_full", "phen_type", "phen_name", "units",
      "measure", "var_type"
    )
    n <- n[, nas := rowSums(is.na(.SD)) > 0, .SDcols = sdcols][,
      -c("nas"), with = FALSE
    ]
    saveRDS(n, file.path(input_dir, x))
  })
  pf <- list.files(path = input_dir, pattern = "^aa_phentab.*.csv")
  unlink(file.path(input_dir, pf))
}

#' @export
# rename station and location nomenclature
sf_rename <- function(
  old_stnd_title = NULL,
  new_stnd_title = NULL,
  pipe_house = NULL,
  old_location = NULL,
  new_location = NULL,
  old_station = NULL,
  new_station = NULL,
  ...
) {
  "%chin%" <- NULL
  "location" <- "logger_title" <- "logger_type" <-
    "record_interval" <- "record_interval_type" <- "station" <-
    "stnd_title" <- "table_name" <- "uz_record_interval" <-
    "uz_record_interval_type" <- "uz_station" <- "uz_table_name" <- NULL
  cli::cli_inform(c("i" = paste0("Renaming is done in the \'d2_wait_room\',",
      " \'d3_nomvet_room\', and \'d4_ipip_room\'."
    ),
    " " = "Nomenclature in data pipe sequences is not dealt with.",
    "!" = "This will rename files!"
  ))
  ## wait room ----
  # only rename if the standard title is listed in the nomtab structure
  # and if the new name is not a duplicate
  if (file.exists(file.path(pipe_house$d2_wait_room, "aa_nomtab.rns"))) {
    nt <- readRDS(file.path(pipe_house$d2_wait_room, "aa_nomtab.rns"))
  } else {
    cli::cli_abort(
      "No nomenclature standards in the waiting room{pipe_house$d2_wait_room}"
    )
  }
  if (!is.character(old_stnd_title) || !is.character(new_stnd_title)) {
    cli::cli_abort(
      "Null/NA arguments. Supply old and new station standard titles."
    )
  }
  # check of the old title is there
  nt1 <- nt[stnd_title %chin% old_stnd_title & location %chin% old_location &
      station %chin% old_station
  ]
  if (nrow(nt1) == 0) {
    cli::cli_abort(paste0("No station/location/title file with title",
      " \'{old_stnd_title}\' in nomtab records!"
    ))
  }

  # change nomtab file
  nt[stnd_title %chin% old_stnd_title & location %chin% old_location &
      station %chin% old_station
  ]$location <- new_location
  nt[stnd_title %chin% old_stnd_title & location %chin% new_location &
      station %chin% old_station
  ]$station <- new_station
  nt[stnd_title %chin% old_stnd_title & location %chin% old_location &
      station %chin% new_station
  ]$stnd_title <- new_stnd_title

  nt <- nt[order(stnd_title, uz_station, station, logger_type,
    logger_title, uz_record_interval_type, uz_record_interval,
    record_interval_type, record_interval, uz_table_name, table_name
  )]
  nt <- unique(nt, by = c("uz_station", "logger_type",
    "uz_record_interval_type", "uz_record_interval", "uz_table_name"
  ))
  # remove temporary csv files
  nom_csvs <- list.files(path = pipe_house$d2_wait_room,
    pattern = "aa_nomtab.*.csv", full.names = TRUE
  )
  unlink(nom_csvs)

  # save nomtab. rns
  saveRDS(nt, file.path(pipe_house$d2_wait_room, "aa_nomtab.rns"))

  # change files in the nomtab room
  f <- dta_list(input_dir = pipe_house$d3_nomvet_room, file_ext = ".ipi",
    wanted = old_stnd_title
  )
  lapply(seq_along(f), function(i) {
    s <- readRDS(file.path(pipe_house$d3_nomvet_room, f[i]))
    s$data_summary$location <- new_location
    s$data_summary$station <- new_station
    s$data_summary$stnd_title <- new_stnd_title
    s$data_summary$nomvet_name <- f[i]
    saveRDS(s,
      file.path(
        pipe_house$d3_nomvet_room,
        gsub(old_stnd_title, new_stnd_title, f[i])
      )
    )
    unlink(file.path(pipe_house$d3_nomvet_room, f[i]))
  })
  # change name in the transfer log file
  if (file.exists(file.path(pipe_house$d3_nomvet_room, "aa_nomvet.log"))) {
    nl <- readRDS(file.path(pipe_house$d3_nomvet_room, "aa_nomvet.log"))
    nl[station %chin% old_station]$station <- new_station
    nl[stnd_title %chin% old_stnd_title]$location <- new_location
    nl[stnd_title %chin% old_stnd_title]$stnd_title <- new_stnd_title
    nl[stnd_title %chin% old_stnd_title]$nomvet_name <- gsub(
      old_stnd_title, new_stnd_title,
      nl[stnd_title %chin% old_stnd_title]$nomvet_name
    )
    saveRDS(nl, file.path(pipe_house$d3_nomvet_room, "aa_nomvet.log"))
  }

  # renaming in d4_ipip_room
  f <- dta_list(input_dir = pipe_house$d4_ipip_room, file_ext = ".ipip",
    wanted = old_stnd_title
  )[1]
  s <- readRDS(file.path(pipe_house$d4_ipip_room, f))
  s$data_summary$location <- new_location
  s$data_summary$station <- new_station
  s$data_summary$stnd_title <- new_stnd_title
  s$data_summary$nomvet_name <- gsub(
    old_stnd_title, new_stnd_title, s$data_summary$nomvet_name
  )
  saveRDS(s,
    file.path(
      pipe_house$d4_ipip_room,
      gsub(old_stnd_title, new_stnd_title, f)
    )
  )
  unlink(file.path(pipe_house$d4_ipip_room, f))

  # remove tmp file
  chunk_dir_f <- file.path(getOption("chunk_dir"), "sf", basename(f))
  unlink(chunk_dir_f, recursive = TRUE)
}

#' @export
# update file log in tmp location for a station
sf_log_update <- function(
  sfc = NULL,
  log_update = NULL,
  ...
) {
  "%chin%" <- NULL
  "tbl_n" <- NULL
  if (!data.table::is.data.table(log_update)) {
    log_update <- data.table::rbindlist(log_update, fill = TRUE,
      use.names = TRUE
    )
  }
  log_update <- unique(log_update)
  sf_log <- readRDS(file.path(dirname(sfc[1]), "sf_log"))
  sf_log$lg_tbl <- sf_log$lg_tbl[!tbl_n %chin% log_update$tbl_n]
  sf_log$lg_tbl <- rbind(sf_log$lg_tbl, log_update, fill = TRUE)[order(tbl_n)]
  saveRDS(sf_log, file.path(dirname(sfc[1]), "sf_log"))
  return(sf_log)
}

#' @export
# delete obects/tables and update file log in tmp location for a station
sf_dta_rm <- function(
  sfc = NULL,
  rm = NULL,
  ...
) {
  "%chin%" <- NULL
  "tbl_n" <- NULL
  # get tmp sf log file
  sf_log <- readRDS(file.path(dirname(sfc[1]), "sf_log"))

  # if there are no files to return, return sflog as is
  if (is.null(rm) || length(rm) == 0) return(sf_log)
  # ensure that rm's are in sf_log
  rm_del <- rm[sapply(rm, function(x) x %in% sf_log$lg_tbl$tbl_n)]
  sf_log$lg_tbl <- sf_log$lg_tbl[!tbl_n %in% rm_del]

  # check for unlogged files
  ulist_fs <- list.files(dirname(sfc[1]))
  ulist_fs <- ulist_fs[!ulist_fs %in% c(sf_log$lg_tbl$tbl_n, "sf_log")]
  rm_del <- rm_del <- c(rm_del, ulist_fs)
  # delete files and directories
  unlink(file.path(dirname(sfc[1]), rm_del), recursive = TRUE)
  # save the new log file
  saveRDS(sf_log, file.path(dirname(sfc[1]), "sf_log"))
  # generate new sfc
  sfc <- sfc[names(sfc) %chin% sf_log$lg_tbl$tbl_n]
  return(sf_log)
}