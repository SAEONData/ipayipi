#' @title Appends data tables by phenomena
#' @description Custom full join of overlapping time series data. The function is designed to append phenomena data and associated phenomena standards efficiently, and can retain temporal metadata records for each phenomena if a phenomena summary data table is provided. Missing data is evaluated to avoid loosing data.
#' @param sfo Overlapping phenomena data table from the 'x' (left) table or station file.
#' @param ndt Data from the new data file (or 'y' or right table).
#' @param nd_min Minimum date-time of the 'new data' being added to the station file.
#' @param nd_max Maximum date-time of the 'new data' being added to the station file.
#' @param phen_id Set this to TRUE for generating updated station file phenomena data summaries. When set to FALSE processing to produce this data summary will not be executed.
#' @param phens Ordered names of phenomena to include in the joined table.
#' @param phen_dt Phenomena table of both the station and new data files combined. These are combined and phids standarised in `ipayipi::station_append()`.
#' @param sf_phen_ds Phenomena summary from the station file.
#' @param new_phen_ds Phenomena summary for the new data.
#' @param ri The record interval associated with the data sets. As a standardised string. Used to generate a time-series sequence using base::seq().
#' @param rit Record interval type. One of the following options for time-series data: 'continuous', 'event_based', or 'mixed'.
#' @param tn The name of the phenomena data tables. Argument only required for processing phenomena summary data.
#' @param overwrite_sf Logical. If TRUE the station file or 'x' table values, if not NA, will be overwritten by values from the 'y' or new data table.
#' @keywords Internal, append phenomena data, overwrite data, join tables,
#' @author Paul J Gordijn
#' @return List of useful items for internal use. Overap data.
#' @export
#' @noRd
#' @details This function is an internal function called by `ipayipi::append_phen_data()`.
append_phen_overlap_data <- function(
  ndt = NULL,
  nd_min = NULL,
  nd_max = NULL,
  phen_id = FALSE,
  phens = NULL,
  phen_dt = NULL,
  sf_phen_ds = NULL,
  new_phen_ds = NULL,
  ri = NULL,
  rit = NULL,
  tn = NULL,
  overwrite_sf = FALSE,
  sf_eindx = NULL,
  verbose = FALSE,
  xtra_v = xtra_v,
  ...
) {
  "date_time" <- "%ilike%" <- "phen_name" <- "d1" <- "d2" <- "table_name" <-
    "phid" <- "d1_old" <- "d2_old" <- "var_type" <- "d1_new" <- "d2_new" <-
    "dttm" <- NULL
  # set tmp working dir
  dta_room <- paste0(tempfile("app"))
  dir.create(dta_room, showWarnings = FALSE)

  # remove the zero fs
  i0 <- paste0("i_", sprintf(
    paste0(
      "%0", nchar(gsub("^i_", "", basename(sf_eindx$fs[1]))), "d"
    ), 0
  ))
  sf_eindx$fs <- sf_eindx$fs[!basename(sf_eindx$fs) %in% i0]
  # append overlapping data for each station file data chunk
  phen_idx <- lapply(sf_eindx$fs, function(fsi) {
    sfo <- readRDS(fsi)
    # if sfo has no rows
    if (all(
      nrow(sfo) == 0, nd_min > sf_eindx$indx$mx |
        nd_max < sf_eindx$indx$mn
    )) {
      sfo_dttms <- data.table::data.table(
        sfo_min = NULL, sfo_max = NULL
      )
      return(list(phen_dtos = NULL, sfo_dttms = sfo_dttms))
    }
    # if there is limited overlap
    if (all(
      nrow(sfo) == 0, nd_min < sf_eindx$indx$mx,
      nd_max > sf_eindx$indx$mn
    )) {
      ipayipi::sf_dta_chunkr(dta_room = dta_room, dta_sets = list(ndt), tn = tn,
        ri = ri, rit = rit, verbose = verbose, xtra_v = xtra_v
      )
      sfo_dttms <- data.table::data.table(
        sfo_min = NULL, sfo_max = NULL
      )
      return(list(phen_dtos = new_phen_ds, sfo_dttms = sfo_dttms))
    }
    # mod sfo min max dates
    sfo_min <- min(sfo$date_time)
    sfo_max <- max(sfo$date_time)

    # overlap data --- sf = station file, nd = new data
    ndo <- ndt[date_time >= sfo_min][date_time <= sfo_max]
    # second exit for limited overlap
    if (nrow(ndo) == 0) {
      sfo_dttms <- data.table::data.table(
        sfo_min = NULL, sfo_max = NULL
      )
      return(list(phen_dtos = NULL, sfo_dttms = sfo_dttms))
    }
    phens <- phens[!phens %in% c("id", "date_time")]

    # join prep with phenomena data summary
    if (phen_id) {
      sfo$d1 <- sfo$date_time
      sfo$d2 <- sfo$date_time
      ndo$d1 <- ndo$date_time
      ndo$d2 <- ndo$date_time
      data.table::setcolorder(sfo,
        c("id", "date_time",
          names(sfo)[!names(sfo) %in% c("id", "date_time", "d1", "d2")],
          "d1", "d2")
      )
      data.table::setcolorder(ndo,
        c("id", "date_time",
          names(ndo)[!names(ndo) %in% c("id", "date_time", "d1", "d2")],
          "d1", "d2")
      )

      dold_l <- lapply(seq_along(phens), function(i) {
        if (phens[i] %in% names(sfo)) {
          dold <- sfo[, c("id", "date_time", phens[i], "d1", "d2"),
            with = FALSE
          ]
          data.table::setkey(dold, d1, d2)
          # get phids of the phen name
          phid_pl <- phen_dt[phen_name == phens[i]]$phid
          st_phens <- unique(sf_phen_ds[phid %in% phid_pl][table_name == tn])
          data.table::setkey(st_phens, start_dttm, end_dttm)
          dtp <- data.table::foverlaps(st_phens, dold, mult = "all",
            type = "any"
          )
          dtp <- dtp[, -c("start_dttm", "end_dttm")][!is.na(date_time)]
        } else {
          dtp <- data.table::data.table(id = NA, date_time = NA, pheni = NA,
            d1 = NA, d2 = NA, phid = NA, dsid = NA, table_name = NA
          )[0, ]
          data.table::setnames(dtp, old = "pheni", new = phens[i])
        }
        old_names <- c("id", "date_time", "phid", phens[i], "d1", "d2",
          "table_name"
        )
        data.table::setnames(dtp,
          old = c("id", "date_time", "phid", phens[i], "d1", "d2",
            "table_name"
          ),
          new = paste0(old_names, "_old")
        )
        data.table::setkey(dtp, d1_old, d2_old)
        return(dtp)
      })
      names(dold_l) <- phens
      # standardise variables
      dold_l <- lapply(dold_l, function(x) {
        ptab <- phen_dt
        ptab$phen_name <- paste0(ptab$phen_name, "_old")
        ptab[var_type %ilike% "fac|factor"]$var_type <- "chr"
        dta <- ipayipi::phen_vars_sts(phen_table = ptab, dta_in = x)
        return(dta)
      })

      dnew_l <- lapply(seq_along(phens), function(i) {
        if (phens[i] %in% names(ndo)) {
          dnew <- ndo[, c("id", "date_time", phens[i], "d1", "d2"),
            with = FALSE
          ]
          data.table::setkey(dnew, d1, d2)
          # get phids of the phen name
          phid_pl <- phen_dt[phen_name == phens[i]]$phid
          st_phens <- unique(new_phen_ds[phid %in% phid_pl][table_name == tn])
          data.table::setkey(st_phens, start_dttm, end_dttm)
          dtp <- data.table::foverlaps(st_phens, dnew, mult = "all",
            type = "any"
          )
          dtp <- dtp[, -c("start_dttm", "end_dttm")][!is.na(date_time)]
        } else {
          dtp <- data.table::data.table(id = NA, date_time = NA, pheni = NA,
            d1 = NA, d2 = NA, phid = NA, table_name = NA
          )[0, ]
          data.table::setnames(dtp, old = "pheni", new = phens[i])
        }
        old_names <- c("id", "date_time", "phid", phens[i], "d1", "d2",
          "table_name"
        )
        data.table::setnames(dtp,
          old = c("id", "date_time", "phid", phens[i], "d1", "d2",
            "table_name"
          ),
          new = paste0(old_names, "_new")
        )
        data.table::setkey(dtp, d1_new, d2_new)
        return(dtp)
      })
      names(dnew_l) <- phens
      # standardise variable
      dnew_l <- lapply(dnew_l, function(x) {
        ptab <- phen_dt
        ptab$phen_name <- paste0(ptab$phen_name, "_new")
        ptab[var_type %ilike% "fac|factor"]$var_type <- "chr"
        dta <- ipayipi::phen_vars_sts(phen_table = ptab, dta_in = x)
        return(dta)
      })
    }
    # if phen_id is FALSE ----
    if (!phen_id) {
      ndo <- rbind(ndo, sfo[0, ], fill = TRUE)
      dnew_l <- lapply(seq_along(phens), function(i) {
        old_names <- names(ndo)[names(ndo) %in% c("id", "date_time", phens[i])]
        dtp <- sfo[, old_names, with = FALSE][!is.na(date_time)]
        data.table::setnames(
          dtp, old = old_names, new = paste0(old_names, "_new")
        )
        return(dtp)
      })
      names(dnew_l) <- phens
      if (!is.null(phen_dt)) {
        # standardise variables
        dnew_l <- lapply(dnew_l, function(x) {
          ptab <- phen_dt
          ptab$phen_name <- paste0(ptab$phen_name, "_new")
          ptab[var_type %ilike% "fac|factor"]$var_type <- "chr"
          dta <- ipayipi::phen_vars_sts(phen_table = ptab, dta_in = x)
          return(dta)
        })
      }

      sfo <- rbind(sfo, ndo[0, ], fill = TRUE)
      dold_l <- lapply(seq_along(phens), function(i) {
        old_names <- names(sfo)[names(sfo) %in% c("id", "date_time", phens[i])]
        dtp <- sfo[, old_names, with = FALSE][!is.na(date_time)]
        data.table::setnames(
          dtp, old = old_names, new = paste0(old_names, "_old")
        )
        return(dtp)
      })
      names(dold_l) <- phens
      if (!is.null(phen_dt)) {
        dold_l <- lapply(dold_l, function(x) { # standardise variables
          ptab <- phen_dt
          ptab$phen_name <- paste0(ptab$phen_name, "_old")
          ptab[var_type %ilike% "fac|factor"]$var_type <- "chr"
          dta <- ipayipi::phen_vars_sts(phen_table = ptab, dta_in = x)
          return(dta)
        })
      }
    }
    # bind these data to a common date-time series
    if (!"discnt" %in% ri) {
      start_dttm <- min(c(sfo$date_time, ndo$date_time))
      end_dttm <- max(c(sfo$date_time, ndo$date_time))
      start_dttm <- lubridate::round_date(start_dttm, unit = ri)
      end_dttm <- lubridate::round_date(end_dttm, unit = ri)
      dt_seq <- seq(from = start_dttm, to = end_dttm, by = ri)
    } else {
      dt_seq <- unique(c(sfo$date_time, ndo$date_time))
    }
    dt_seq <- data.table::data.table(dttm = dt_seq)
    dt_seq <- dt_seq[order(dttm)]
    if (overwrite_sf) f <- c("_old", "_new") else f <- c("_new", "_old")
    dto <- future.apply::future_lapply(seq_along(phens), function(i) {
      # join old to dt_seq then new
      z <- dold_l[[i]]
      if (nrow(dold_l[[i]]) > 0) {
        z <- merge(x = z, y = dt_seq, by.x = "date_time_old", by.y = "dttm",
          all.y = TRUE, all.x = FALSE
        )
      } else {
        z <- rbind(dt_seq, dold_l[[i]], fill = TRUE)
        z$date_time_old <- z$dttm
      }
      if (nrow(dnew_l[[i]]) > 0) {
        z <- merge(x = z, y = dnew_l[[i]], by.x = "date_time_old",
          by.y = "date_time_new", all.x = TRUE, all.y = FALSE
        )
      } else {
        z <- rbind(z, dnew_l[[i]], fill = TRUE)
      }
      z[[phens[i]]] <- data.table::fifelse(
        is.na(z[[paste0(phens[i], f[2])]]),
        z[[paste0(phens[i], f[1])]],
        z[[paste0(phens[i], f[2])]]
      )
      if ("id" %in% names(z)) {
        z[["id"]] <- data.table::fifelse(
          is.na(z[[paste0("id", f[2])]]),
          as.numeric(z[[paste0("id", f[1])]]),
          as.numeric(z[[paste0("id", f[2])]])
        )
      } else {
        z[["id"]] <- seq_len(nrow(z))
      }
      if (paste0("phid", f[2]) %in% names(z)) {
        z[["phid"]] <- data.table::fifelse(
          is.na(z[[paste0("phid", f[2])]]),
          as.integer(z[[paste0("phid", f[1])]]),
          as.integer(z[[paste0("phid", f[2])]])
        )
      }
      z <- z[, names(z)[names(z) %in% c(phens[i], "id", "phid")], with = FALSE]
    })
    names(dto) <- phens
    # put all phenomena in one table
    naj <- lapply(seq_along(dto), function(i) {
      naj <- is.na(dto[[i]][["id"]][])[
        is.na(dto[[i]][["id"]][]) == TRUE
      ]
      naj <- length(naj)
      return(naj)
    })
    naj <- unlist(naj)
    naj <- which(naj == min(naj))[1]
    raw_dto <- lapply(dto, function(x) {
      x <- x[, names(x)[!names(x) %in% c("id", "phid")], with = FALSE]
      x$date_time <- dt_seq$dttm
      data.table::setkey(x, date_time)
      return(x)
    })
    raw_dto <- Reduce(function(...) merge(..., all = TRUE), raw_dto)
    raw_dto$id <- dto[[naj]][["id"]]
    data.table::setcolorder(raw_dto, c("id", "date_time", names(dto)))
    if (!is.null(phen_dt)) {
      raw_dto <- ipayipi::phen_vars_sts(dta_in = raw_dto, phen_table = phen_dt)
    }
    # save raw_dto to tmp location --- indexed format
    dta_sets <- list(raw_dto)
    names(dta_sets) <- basename(fsi)
    oindxr <- ipayipi::sf_dta_chunkr(dta_room = dta_room, dta_sets = dta_sets,
      tn = tn, ri = ri, rit = rit, verbose = verbose,
      xtra_v = xtra_v
    )
    sfo_dttms <- data.table::data.table(
      sfo_min = oindxr$mn, sfo_max = oindxr$mx
    )

    # generate phen table for the overlapping data
    if (phen_id) {
      zap <- lapply(dto, function(x) {
        all(!is.na(x$id))
      })
      rm(zap)
      dto <- dto[unlist(lapply(dto, function(x) all(!is.na(x$id))))]
      phen_dtos <- lapply(dto, function(x) {
        x$date_time <- raw_dto$date_time
        if (nrow(x[!is.na(phid)]) == 0) return(NULL)
        x$ph_int <- ipayipi::change_intervals(int_dta = x$phid)
        x$f <- as.integer(as.factor(paste0(x$ph_int)))
        x$f <- ipayipi::change_intervals(int_dta = x$f)
        x <- split.data.frame(x, f = as.factor(x$f))
        x <- lapply(x, function(z) {
          z <- data.table::data.table(
            start_dttm = min(z$date_time),
            end_dttm = max(z$date_time),
            phid = z$phid[1]
          )
          return(z)
        })
        x <- data.table::rbindlist(x)
        x <- x[!is.na(phid)]
        return(x)
      })
      phen_dtos <- data.table::rbindlist(phen_dtos)
    } else {
      phen_dtos <- NULL
    }
    return(list(phen_dtos = phen_dtos, sfo_dttms = sfo_dttms))
  })
  phen_dtos <- lapply(phen_idx, function(x) {
    x <- x$phen_dtos
    x <- x[, c("start_dttm", "end_dttm", "phid"), with = FALSE]
    return(x)
  })
  sfo_dttms <- data.table::rbindlist(
    lapply(phen_idx, function(x) x$sfo_dttms)
  )
  if (is.null(sfo_dttms$sfo_min)) {
    sfo_min <- NULL
    sfo_max <- NULL
  } else {
    sfo_min <- min(sfo_dttms$sfo_min)
    sfo_max <- min(sfo_dttms$sfo_max)
  }
  return(list(phen_dtos = phen_dtos, dta_room = dta_room,
    sfo_min = sfo_min, sfo_max = sfo_max
  ))
}
