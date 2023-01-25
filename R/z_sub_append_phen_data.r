#' @title Appends data tables by phenomena
#' @description This function is for internal use. The function is designed to
#'  append phenomena data and associated phenomena standards efficiently, but
#'  retaining a metadata records for each phenomena. Moreover, the function
#'  evaluates missing data and compares new and older data records so avoid
#'  loosing data.
#' @param phen_dt Phenomena table of from both the station and new data files
#'  combined. These are combined and phids standarised in
#'  ipayipi::station_append().
#' @param station_data_table Data from the station file.
#' @param new_data_table Data from the new data file.
#' @param overwrite_old If `TRUE` then original data is disgarded
#'  in favour of new data. If TRUE both data sets will be evaluated and where
#'  there are NA values, a replacement, if available, will be used to replace
#'  the NA value. Defaults to FALSE.
#' @keywords append phenomena data, overwrite data, join tables,
#' @author Paul J Gordijn
#' @return A phenomena table that contains the start and end dates of each
#'  phenomena and the respective data source identification numbers of the
#'  data. Also, the appended data.
#' @export
#' @details This function is an internal function called by others in the
#'  pipeline.
append_phen_data <- function(
  old_dta = NULL,
  old_phen_ds = NULL,
  new_dta = NULL,
  new_phen_ds = NULL,
  phen_dt = NULL,
  overwrite_old = FALSE,
  tn = NULL,
  ...) {
  # old_dta <- station_file[[new_data_cm]]
  # new_dta <- new_data[[new_data_cm]]
  # old_phen_ds <- station_file$phen_data_summary[table_name == tn]
  # new_phen_ds <- new_data$phen_data_summary[table_name == tn]
  
  phens <- unique(phen_dt[table_name == tn]$phen_name)

  # # standardise variable format
  # old_dta <- ipayipi::phen_vars_sts(dta_in = old_dta,
  #   phen_table = phen_dt)
  # new_dta <- ipayipi::phen_vars_sts(dta_in = new_dta,
  #   phen_table = phen_dt)
  if (overwrite_old) {
    old_dta <- old_dta[
      date_time < min(new_dta$date_time) | date_time > max(new_dta$date_time)]
  }
  if (nrow(old_dta) > 0 && nrow(new_dta) > 0) {
    sno <- old_dta[  # overlap data
      date_time <= max(new_dta$date_time)][date_time >= min(new_dta$date_time)]
    nso <- new_dta[
      date_time <= max(old_dta$date_time)][date_time >= min(old_dta$date_time)]
    snno <- old_dta[ # not overlap data
      date_time > max(new_dta$date_time) | date_time < min(new_dta$date_time)]
    nsno <- new_dta[
      date_time > max(old_dta$date_time) | date_time < min(old_dta$date_time)]
    if (nrow(sno) < 1 && nrow(nso) > 0) {
      nsno <- rbind(nsno, nso, fill = TRUE)[order(date_time)]
      nso <- nso[0, ]
    }
    if (nrow(nso) < 1 && nrow(sno) > 0) {
      snno <- rbind(snno, sno, fill = TRUE)[order(date_time)]
      sno <- sno[0, ]
    }
    if (nrow(snno) < 1) {
      snno_pdt <- old_phen_ds[0, ]
    } else {
      snno_pdt <- old_phen_ds
      snno_pdt$start_dt <- data.table::fifelse(
        snno_pdt$start_dt < min(snno$date_time),
        min(snno$date_time), snno_pdt$start_dt)
      snno_pdt$end_dt <- data.table::fifelse(
        snno_pdt$end_dt > max(snno$date_time),
        max(snno$date_time), snno_pdt$end_dt)
    }
    if (nrow(nsno) < 1) {
      nsno_pdt <- new_phen_ds[0, ]
    } else {
      nsno_pdt <- new_phen_ds
      nsno_pdt$start_dt <- data.table::fifelse(
        nsno_pdt$start_dt < min(nsno$date_time),
        min(nsno$date_time), nsno_pdt$start_dt)
      nsno_pdt$end_dt <- data.table::fifelse(
        nsno_pdt$end_dt > max(nsno$date_time),
        max(nsno$date_time), nsno_pdt$end_dt)
    }
    snno <- rbind(snno, nsno, fill = TRUE)
    snno_pdt <- rbind(snno_pdt, nsno_pdt, fill = TRUE)
  } else {
    sno <- old_dta[0, ]
    nso <- new_dta[0, ]
    snno <- rbind(old_dta, new_dta)
  }
  if (nrow(sno) > 0 && nrow(nso) > 0) {
    sno$d1 <- sno$date_time
    sno$d2 <- sno$date_time
    nso$d1 <- nso$date_time
    nso$d2 <- nso$date_time
    data.table::setcolorder(sno, c("id", "date_time", names(sno)[
        !names(sno) %in% c("id", "date_time", "d1", "d2")], "d1", "d2"))
    data.table::setcolorder(nso,
      c("id", "date_time", names(nso)[
        !names(nso) %in% c("id", "date_time", "d1", "d2")], "d1", "d2"))

    dold_l <- lapply(seq_along(phens), function(i) {
      if (phens[i] %in% names(sno)) {
        dold <- sno[, c("id", "date_time", phens[i], "d1", "d2"),
          with = FALSE]
        data.table::setkey(dold, d1, d2)
        # get phids of the phen name
        phid_pl <- phen_dt[phen_name == phens[i]]$phid
        st_phens <- old_phen_ds[phid %in% phid_pl][table_name == tn]
        data.table::setkey(st_phens, start_dt, end_dt)
        dtp <- data.table::foverlaps(st_phens, dold, mult = "all", type = "any")
        dtp <- dtp[, -c("start_dt", "end_dt")][!is.na(date_time)]
      } else {
        dtp <- data.table::data.table(id = NA, date_time = NA, pheni = NA,
          d1 = NA, d2 = NA, phid = NA, dsid = NA, table_name = NA)[0, ]
        data.table::setnames(dtp, old = "pheni", new = phens[i])
      }
      old_names <- c("id", "date_time", "dsid", "phid", phens[i], "d1", "d2",
          "table_name")
      data.table::setnames(dtp,
        old = c("id", "date_time", "dsid", "phid", phens[i], "d1", "d2",
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
      if (phens[i] %in% names(nso)) {
        dnew <- nso[, c("id", "date_time", phens[i], "d1", "d2"),
          with = FALSE]
        data.table::setkey(dnew, d1, d2)
        # get phids of the phen name
        phid_pl <- phen_dt[phen_name == phens[i]]$phid
        st_phens <- new_phen_ds[phid %in% phid_pl][table_name == tn]
        data.table::setkey(st_phens, start_dt, end_dt)
        dtp <- data.table::foverlaps(st_phens, dnew, mult = "all", type = "any")
        dtp <- dtp[, -c("start_dt", "end_dt")][!is.na(date_time)]
      } else {
        dtp <- data.table::data.table(id = NA, date_time = NA, pheni = NA,
          d1 = NA, d2 = NA, phid = NA, dsid = NA, table_name = NA)[0, ]
        data.table::setnames(dtp, old = "pheni", new = phens[i])
      }
      old_names <- c("id", "date_time", "dsid", "phid", phens[i], "d1", "d2",
          "table_name")
      data.table::setnames(dtp,
        old = c("id", "date_time", "dsid", "phid", phens[i], "d1", "d2",
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
    dt_seq <- unique(c(sno$date_time, nso$date_time))
    dt_seq <- dt_seq[order(dt_seq)]
    dt_seq <- data.table::data.table(
      dt_seq = dt_seq, dt_fuzz_1 = dt_seq, dt_fuzz_2 = dt_seq)
    data.table::setkey(dt_seq, dt_fuzz_1, dt_fuzz_2)
    if (overwrite_old) f <- c("_old", "_new") else f <- c("_new", "_old")
    dto <- lapply(seq_along(phens), function(i) {
      # join old to dt_seq then new
      if (nrow(dold_l[[i]]) > 0) {
        z <- data.table::foverlaps(x = dt_seq, y = dold_l[[i]], mult = "first",
          type = "any")
      } else {
        z <- rbind(dt_seq, dold_l[[i]], fill = TRUE)
      }
      data.table::setkey(z, dt_fuzz_1, dt_fuzz_2)
      if (nrow(dnew_l[[i]]) > 0) {
        z <- data.table::foverlaps(x = z, y = dnew_l[[i]], mult = "first",
          type = "any")
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
      z[["dsid"]] <- data.table::fifelse(
        is.na(z[[paste0("dsid", f[2])]]),
        as.integer(z[[paste0("dsid", f[1])]]),
        as.integer(z[[paste0("dsid", f[2])]])
      )
      z[["phid"]] <- data.table::fifelse(
        is.na(z[[paste0("phid", f[2])]]),
        as.integer(z[[paste0("phid", f[1])]]),
        as.integer(z[[paste0("phid", f[2])]])
      )
      z <- z[, c(phens[i], "id", "dsid", "phid"),
        with = FALSE]
    })
    names(dto) <- phens
    # put all phenomena in one table
    raw_dto <- lapply(dto, function(x) {
      x[, names(x)[!names(x) %in% c("id", "phid", "dsid")], with = FALSE]
    })
    raw_dto <- do.call(cbind, raw_dto)
    names(raw_dto) <- names(dto)
    naj <- lapply(seq_along(dto), function(i) {
      naj <- is.na(dto[[i]][["id"]][])[
        is.na(dto[[i]][["id"]][]) == TRUE]
      naj <- length(naj)
      return(naj)
    })
    naj <- unlist(naj)
    naj <- which(naj == min(naj))[1]
    raw_dto$id <- dto[[naj]][["id"]]
    raw_dto$date_time <- dt_seq$dt_seq
    data.table::setcolorder(raw_dto, c("id", "date_time", names(dto)))
    raw_dto <- ipayipi::phen_vars_sts(dta_in = raw_dto, phen_table = phen_dt)
    # generate phen table for the overlapping data
    zap <- lapply(dto, function(x) {
      all(!is.na(x$id))
    })
    dto <- dto[unlist(lapply(dto, function(x) all(!is.na(x$id))))]
    phen_dtos <- lapply(dto, function(x) {
      x$date_time <- raw_dto$date_time
      x$ph_int <- ipayipi::change_intervals(int_dta = x$phid)
      x$ds_int <- ipayipi::change_intervals(int_dta = x$dsid)
      x$f <- as.integer(as.factor(paste0(x$ph_int, "_", x$ds_int)))
      x$f <- ipayipi::change_intervals(int_dta = x$f)
      x <- split.data.frame(x, f = as.factor(x$f))
      x <- lapply(x, function(z) {
        z <- data.table::data.table(
          start_dt = min(z$date_time),
          end_dt = max(z$date_time),
          dsid = z$dsid[1],
          phid = z$phid[1]
        )
        return(z)
      })
      x <- data.table::rbindlist(x)
      return(x)
    })
    phen_dtos <- data.table::rbindlist(phen_dtos)
  } else {
    phen_dtos <- snno_pdt[0, ]
    raw_dto <- snno[0, ]
  }
  # join overlap data and non-overlap data
  if (nrow(phen_dtos) > 0) {
    phen_ds <- rbind(phen_dtos, snno_pdt, fill = TRUE)
    phen_ds$table_name <- tn
  } else {
    phen_ds <- snno_pdt
  }
  phen_ds <- phen_ds[!is.na(dsid) | !is.na(phid)][
    order(phid, start_dt, end_dt)]
  split_phen_ds <- split.data.frame(phen_ds,
    f = factor(paste(phen_ds$dsid, phen_ds$phid)))
  split_phen_ds <- lapply(split_phen_ds, function(z) {
    z <- data.table::data.table(start_dt = min(z$start_dt),
      end_dt = max(z$end_dt), dsid = z$dsid[1], phid = z$phid[1])
    return(z)
  })
  phen_ds <- data.table::rbindlist(split_phen_ds)[order(phid, start_dt)][,
    table_name := tn]
  app_dta <- rbind(snno, raw_dto, fill = TRUE)[order(date_time)]
  return(list(app_dta = app_dta, phen_ds = phen_ds))
}
