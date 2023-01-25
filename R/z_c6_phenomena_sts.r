#' @title Standardises data phenomena in 'ipayipi' format.
#' @description 
#' @param wait_room Path to the 'wait_room' directory.
#' @param prompt Should the function use an interactive file selection function
#'  otherwise all files are returned. TRUE or FALSE.
#' @param recurr Should the function search recursively into sub directories
#'  for hobo rainfall csv export files? TRUE or FALSE.
#' @param unwanted Vector of strings listing files that should not be included
#'  in the import.
#' @param file_ext_in The file extension defaults to ".iph". Other file types
#'  could be incorporatted if required.
#' @param file_ext_in The file extension defaults to ".iph". Other file types
#'  could be incorporatted if required.
#' @keywords meteorological data; automatic weather station; batch process;
#'  file standardisation; standardise variables; transform variables
#' @author Paul J. Gordijn
#' @export
phenomena_sts <- function(
  wait_room = NULL,
  prompt = FALSE,
  recurr = TRUE,
  unwanted = NULL,
  file_ext_in = ".iph",
  file_ext_out = ".ipi",
  ...
) {
  # get list of data to be imported
  slist <- ipayipi::dta_list(input_dir = wait_room, file_ext = file_ext_in,
    prompt = prompt, recurr = recurr, unwanted = unwanted)
  cr_msg <- padr(core_message =
    paste0(" Standardising ", length(slist),
      " file(s) phenomena ", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)

  phentab <- ipayipi::phenomena_chk(wait_room = wait_room,
    check_phenomena = TRUE, csv_out = TRUE)
  if (!is.na(phentab$output_csv_name)) {
    stop("Update phenomena")
  }
  phentab <- phentab$update_phenomena
  mfiles <- lapply(seq_along(slist), function(i) {
    cr_msg <- padr(core_message = paste0(" +> ", slist[i], collapes = ""),
      wdth = 80, pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(1, 1))
    message(cr_msg)
    m <- readRDS(file.path(wait_room, slist[i]))

    # get original phen table and update
    phen_old <- m$phens
    phen_new <- lapply(seq_len(nrow(phen_old)), function(j) {
      phen_new <- phentab[
        uz_phen_name == phen_old$phen_name[j] & uz_units == phen_old$units[j] &
          uz_measure == phen_old$measure[j]]
      if (nrow(phen_new) > 1) stop("Multiple possible phenomena matches!")
      invisible(phen_new)
    })
    phen_new <- data.table::rbindlist(phen_new)[order(phen_name_full)]

    # update raw_data table
    new_names <- sapply(names(m$raw_data)[
      !names(m$raw_data) %in% c("id", "date_time")], function(x) {
      new_name <- phen_new[uz_phen_name == x]$phen_name[1]
      return(new_name)
    })
    data.table::setnames(m$raw_data, old = names(m$raw_data),
      new = c("id", "date_time", new_names))

    for (ii in seq_len(nrow(phen_new[!is.na(offset)]))) {
      phenz <- phen_new[!is.na(offset)]$phen_name[ii]
      m$raw_data[, ][[phenz]]  <- as.numeric(
        m$raw_data[[phenz]]) + rep_len(as.numeric(
          phen_new[!is.na(offset)]$offset[ii]),
            length.out = nrow(m$raw_data))
    }
    for (ii in seq_len(nrow(phen_new[!is.na(f_convert)]))) {
      phenz <- phen_new[!is.na(f_convert)]$phen_name[ii]
      m$raw_data[, ][[phenz]]  <- as.numeric(
        m$raw_data[[phenz]]) +
          as.numeric(phen_new[!is.na(f_convert)]$f_convert[ii])
    }
    # convert units
    if (length(phen_new[var_type %ilike% "num"]$phen_name) > 0) {
      m$raw_data[, (phen_new[var_type %ilike% "num"]$phen_name) :=
        lapply(.SD, as.numeric),
          .SDcols = phen_new[var_type %ilike% "num"]$phen_name]
    }
    if (length(phen_new[var_type %ilike% "int"]$phen_name) > 0) {
    m$raw_data[, (phen_new[var_type %ilike% "int"]$phen_name) :=
      lapply(.SD, as.integer),
        .SDcols = phen_new[var_type %ilike% "int"]$phen_name]
    }
    # factors included here as they are first converted to character
    if (length(phen_new[var_type %ilike%
      "str|string|chr|character|fac|factor|fact"]$phen_name) > 0) {
      m$raw_data[, (phen_new[
          var_type %ilike%
            "str|string|chr|character|fac|factor|fact"]$phen_name) :=
            lapply(.SD, as.character), .SDcols = phen_new[
              var_type %ilike%
              "str|string|chr|character|fac|factor|fact"]$phen_name
      ]
    }
    if (length(phen_new[var_type %ilike% "date|time|posix"]$phen_name) > 0) {
      dt_tz <- attr(m$raw_data$date_time[1], "tz")
      m$raw_data[, (phen_new[
          var_type %ilike% "date|time|posix"]$phen_name) :=
            lapply(.SD, as.POSIXct(tz = dt_tz)), .SDcols = phen_new[
              var_type %ilike% "date|time|posix"]$phen_name
      ]
    }
    if (length(phen_new[phen_name %ilike% "interfere"]$phen_name) > 0) {
      for (ii in seq_len(nrow(phen_new[phen_name %ilike% "interfere"]))) {
        phenz <- phen_new[phen_name %ilike% "interfere"]$phen_name[ii]
        m$raw_data[, ][[phenz]][
          m$raw_data[, ][[phenz]] %ilike% "log"] <- "logged"
        m$raw_data[, ][[phenz]][
          m$raw_data[, ][[phenz]] != "logged"] <- NA
      }
    }
    if (nrow(phen_new[var_type %ilike% "fac|factor|fact"]) > 0) {
      m$raw_data[, (phen_new[
          var_type %ilike% "fac|factor|fact"]$phen_name) :=
            lapply(.SD, as.factor), .SDcols = phen_new[
              var_type %ilike% "fac|factor|fact"]$phen_name
      ]
    }
    m$phens <- phen_new
    # update the phenomena data summary
    m$phen_data_summary <- data.table::data.table(
      phid = as.integer(m$phens$phid),
      dsid = as.integer(rep(as.integer(m$data_summary$dsid), nrow(m$phens))),
      start_dt = rep(m$data_summary$start_dt, nrow(m$phens)),
      end_dt = rep(m$data_summary$end_dt, nrow(m$phens)),
      table_name = rep(m$data_summary$table_name, nrow(m$phens))
    )
    invisible(m)
  })

  # save files to the wait room
  saved_files <- lapply(seq_along(mfiles), function(x) {
    fn <- file.path(wait_room, gsub(file_ext_in, file_ext_out, slist[[x]]))
    saveRDS(mfiles[[x]], fn)
    invisible(fn)
  })

  # remove slist files
  del_metn <- lapply(slist, function(x) {
    file.remove(file.path(wait_room, x))
    invisible(file.path(wait_room, x))
  })

  cr_msg <- padr(core_message = paste0("", collapes = ""),
    wdth = 80, pad_char = "=", pad_extras = c("|", "", "", "|"),
    force_extras = FALSE, justf = c(0, 0))
  message(cr_msg)
  invisible(list(saved_files = saved_files, deleted_files = del_metn))
}