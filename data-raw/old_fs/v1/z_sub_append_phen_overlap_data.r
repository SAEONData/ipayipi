#' @title Appends data tables by phenomena
#' @description This function is for internal use. The function is designed to
#'  append phenomena data and associated phenomena standards efficiently, but
#'  retaining metadata records for each phenomena. Moreover, the function
#'  evaluates missing data and compares new and older data records to avoid
#'  loosing data. This part of the function only deals with overlap data.
#' @param phens
#' @param sf_phen_ds Phenomena summary from the station file.
#' @param sfo
#' @param ndt Data from the new data file.
#' @param nd_min
#' @param nd_max
#' @param new_phen_ds Phenomena summary for the new data.
#' @param phen_dt Phenomena table of from both the station and new data files
#'  combined. These are combined and phids standarised in
#'  ipayipi::station_append().
#' @param tn The name of the phenomena data tables.
#' @param ri The record interval associated with the data sets. As a
#'  standardised string.
#' @keywords append phenomena data, overwrite data, join tables,
#' @author Paul J Gordijn
#' @return List of useful items for internal use. Overap data.
#' @export
#' @details This function is an internal function called by others in the
#'  pipeline.
append_phen_overlap_data <- function(
  nd_min = NULL,
  nd_max = NULL,
  sfo = NULL,
  ndt = NULL,
  phen_dt = NULL,
  phens = NULL,
  sf_phen_ds = NULL,
  new_phen_ds = NULL,
  ri = NULL,
  tn = tn,
  overwrite_sf = FALSE,
  ...
  ) {
  "date_time" <- "%ilike%" <- "phen_name" <- "d1" <- "d2" <- "table_name" <-
    "phid" <- "d1_old" <- "d2_old" <- "var_type" <- "d1_new" <- "d2_new" <-
    "dttm" <- NULL
  # if there is overlap data
  if (nrow(sfo) > 0) {
    sfo_min <- min(sfo$date_time)
    sfo_max <- max(sfo$date_time)
    # overlap data --- sf = station file, nd = new data
    ndo <- ndt[date_time >= sfo_min][date_time <= sfo_max]
    # if (nrow(sfo) < 1 && nrow(ndo) > 0) ndo <- ndo[0, ]
    # if (nrow(ndo) < 1 && nrow(sfo) > 0) sfo <- sfo[0, ]
    if (nrow(sfo) > 0 && nrow(ndo) > 0) {
      sfo$d1 <- sfo$date_time
      sfo$d2 <- sfo$date_time
      ndo$d1 <- ndo$date_time
      ndo$d2 <- ndo$date_time
      data.table::setcolorder(sfo, c("id", "date_time", names(sfo)[
          !names(sfo) %in% c("id", "date_time", "d1", "d2")], "d1", "d2"))
      data.table::setcolorder(ndo,
        c("id", "date_time", names(ndo)[
          !names(ndo) %in% c("id", "date_time", "d1", "d2")], "d1", "d2"))

      dold_l <- lapply(seq_along(phens), function(i) {
        if (phens[i] %in% names(sfo)) {
          dold <- sfo[, c("id", "date_time", phens[i], "d1", "d2"),
            with = FALSE]
          data.table::setkey(dold, d1, d2)
          # get phids of the phen name
          phid_pl <- phen_dt[phen_name == phens[i]]$phid
          st_phens <- unique(sf_phen_ds[phid %in% phid_pl][table_name == tn])
          data.table::setkey(st_phens, start_dttm, end_dttm)
          dtp <- data.table::foverlaps(st_phens, dold, mult = "all",
            type = "any")
          dtp <- dtp[, -c("start_dttm", "end_dttm")][!is.na(date_time)]
        } else {
          dtp <- data.table::data.table(id = NA, date_time = NA, pheni = NA,
            d1 = NA, d2 = NA, phid = NA, dsid = NA, table_name = NA)[0, ]
          data.table::setnames(dtp, old = "pheni", new = phens[i])
        }
        old_names <- c("id", "date_time", "phid", phens[i], "d1", "d2",
            "table_name")
        data.table::setnames(dtp,
          old = c("id", "date_time", "phid", phens[i], "d1", "d2",
            "table_name"),
          new = paste0(old_names, "_old")
        )
        data.table::setkey(dtp, d1_old, d2_old)
        return(dtp)
      })
      names(dold_l) <- phens
      dold_l <- lapply(dold_l, function(x) { # standardise variable types
        ptab <- phen_dt
        ptab$phen_name <- paste0(ptab$phen_name, "_old")
        ptab[var_type %ilike% "fac|factor"]$var_type <- "chr"
        dta <- ipayipi::phen_vars_sts(phen_table = ptab, dta_in = x)
        return(dta)
      })

      dnew_l <- lapply(seq_along(phens), function(i) {
        if (phens[i] %in% names(ndo)) {
          dnew <- ndo[, c("id", "date_time", phens[i], "d1", "d2"),
            with = FALSE]
          data.table::setkey(dnew, d1, d2)
          # get phids of the phen name
          phid_pl <- phen_dt[phen_name == phens[i]]$phid
          st_phens <- unique(new_phen_ds[phid %in% phid_pl][table_name == tn])
          data.table::setkey(st_phens, start_dttm, end_dttm)
          dtp <- data.table::foverlaps(st_phens, dnew, mult = "all",
            type = "any")
          dtp <- dtp[, -c("start_dttm", "end_dttm")][!is.na(date_time)]
        } else {
          dtp <- data.table::data.table(id = NA, date_time = NA, pheni = NA,
            d1 = NA, d2 = NA, phid = NA, table_name = NA)[0, ]
          data.table::setnames(dtp, old = "pheni", new = phens[i])
        }
        old_names <- c("id", "date_time", "phid", phens[i], "d1", "d2",
            "table_name")
        data.table::setnames(dtp,
          old = c("id", "date_time", "phid", phens[i], "d1", "d2",
            "table_name"),
          new = paste0(old_names, "_new")
        )
        data.table::setkey(dtp, d1_new, d2_new)
        return(dtp)
      })
      names(dnew_l) <- phens
      dnew_l <- lapply(dnew_l, function(x) { # standardise variable types
        ptab <- phen_dt
        ptab$phen_name <- paste0(ptab$phen_name, "_new")
        ptab[var_type %ilike% "fac|factor"]$var_type <- "chr"
        dta <- ipayipi::phen_vars_sts(phen_table = ptab, dta_in = x)
        return(dta)
      })

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
      dto <- lapply(seq_along(phens), function(i) {
        # join old to dt_seq then new
        z <- dold_l[[i]]
        if (nrow(dold_l[[i]]) > 0) {
          z <- merge(x = z, y = dt_seq, by.x = "date_time_old", by.y = "dttm",
            all.y = TRUE, all.x = FALSE)
        } else {
          z <- rbind(dt_seq, dold_l[[i]], fill = TRUE)
        }
        if (nrow(dnew_l[[i]]) > 0) {
          z <- merge(x = z, y = dnew_l[[i]], by.x = "date_time_old",
            by.y = "date_time_new", all.x = TRUE, all.y = FALSE)
        } else {
          z <- rbind(z, dnew_l[[i]], fill = TRUE)
        }
        z[[phens[i]]] <- data.table::fifelse(
          is.na(z[[paste0(phens[i], f[2])]]),
          z[[paste0(phens[i], f[1])]],
          z[[paste0(phens[i], f[2])]]
        )
        z[["id"]] <- data.table::fifelse(
          is.na(z[[paste0("id", f[2])]]),
          as.integer(z[[paste0("id", f[1])]]),
          as.integer(z[[paste0("id", f[2])]])
        )
        z[["phid"]] <- data.table::fifelse(
          is.na(z[[paste0("phid", f[2])]]),
          as.integer(z[[paste0("phid", f[1])]]),
          as.integer(z[[paste0("phid", f[2])]])
        )
        z <- z[, c(phens[i], "id", "phid"),
          with = FALSE]
      })
      names(dto) <- phens
      # put all phenomena in one table
      naj <- lapply(seq_along(dto), function(i) {
        naj <- is.na(dto[[i]][["id"]][])[
          is.na(dto[[i]][["id"]][]) == TRUE]
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
      raw_dto <- ipayipi::phen_vars_sts(dta_in = raw_dto, phen_table = phen_dt)
      # generate phen table for the overlapping data
      zap <- lapply(dto, function(x) {
        all(!is.na(x$id))
      })
      rm(zap)
      dto <- dto[unlist(lapply(dto, function(x) all(!is.na(x$id))))]
      phen_dtos <- lapply(dto, function(x) {
        x$date_time <- raw_dto$date_time
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
        return(x)
      })
      phen_dtos <- data.table::rbindlist(phen_dtos)
    }
  } else {# else there is no overlap data
    phen_dtos <- NULL
    raw_dto <- NULL
    sfo_max <- NULL
    sfo_min <- NULL
  }

  return(list(phen_dtos = phen_dtos, raw_dto = raw_dto,
    sfo_max = sfo_max, sfo_min = sfo_min))
}
