#' @title  'dt' processing pipeline: harvest data
#' @description Used to extract data from other table/s, stations, or source/s.
#' @param station_file File path of the station being processed.
#' @param hsf_table The path of the directory in which to search for the external data and/or data table.
#' @param ppsij The desirsed time_interval associated with the extracted data. If this is `NULL` then the table is extracted as is.
#' @param f_params Function parameters parsed by `dt_process()`.
#' @param sfc Station file connection; see `open_sf_con()`.
#' @param verbose Print some basic information on progress? Logical.
#' @param xtra_v Print some more details on function processing. Logical.
#' @author Paul J. Gordijn
#' @details Harvesting data during processing will function differently depending on type of data. If chunked data is being harvested, essentially only the link to the chunked data and associated chunk index is copied. If unchunked data (that is data without a 'date_time' column processed by 'ipayipi') then the table is copied into the stations station file connection directory as a 'data.table'.
#' Importantly, if the data has an associated 'gap' table; this table (filtered by the harvest table name and phenomena names --- logger gaps always inluded) is harvested and placed with the copied chunked data link. This 'gap' data is transferred down the pipeline as data is processed by `ipayipi::dt_process()`.
#' @export
dt_harvest <- function(
  station_file = NULL,
  f_params = NULL,
  ppsij = NULL,
  sfc = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
) {
  "%ilike%"  <- ":=" <- ".SD" <- "." <- NULL
  "table_name" <- "phen" <- "gz_gap_start" <- "gz_gap_end" <- "gz_phen" <-
    "gap_start" <- "gap_end" <- "gl2" <- "gi1" <- "gl1" <- "gi2" <-
    "gz_p" <- "p" <- "gz_gid" <- "gid" <- "gz_eid" <- "eid" <- "gz_gap_type" <-
    "gap_type" <- "gz_table_name" <- "gz_problem_gap" <- "problem_gap" <-
    "gz_notes" <- "notes" <- "dt_diff_s" <- NULL

  # harvest data from tables
  if (station_file == unique(f_params$hsf_station)[1]) {
    hsfc <- sfc
  } else {
    hsfc <- attempt::try_catch(
      expr = open_sf_con(
        station_file = unique(f_params$hsf_station)[1], verbose = verbose,
        xtra_v = xtra_v
      ), .w = ~stop
    )
  }
  ppsid <- paste0(ppsij$dt_n, "_", ppsij$dtp_n)
  hsf_params <- f_params[ppsid %in% ppsid]
  hsf_names <- unique(hsf_params$hsf_table)

  # get gap table from the hsf station
  g <- sf_dta_read(sfc = hsfc, tv = "gaps")[["gaps"]]
  g <- g[table_name %in% hsf_names]

  # extract and save dataset or dt/raw chunked data index
  lapply(hsf_names, function(x) {
    h <- sf_dta_read(sfc = hsfc, tv = x, start_dttm = NULL, end_dttm = NULL)
    # unchunked files ----
    if (is.null(h[[x]]$indx) && data.table::is.data.table(h[[x]])) {
      h <- list(list(dta = h[[x]]))
      names(h) <- x
      n <- c(names(h[[x]]$dta)[names(h[[x]]$dta) %in% hsf_params$phen_name])
      n <- c(n[n %in% "date_time"], n[!n %in% "date_time"])
      n <- c(n[n %ilike% "*id$"], n[!n %ilike% "*id$"])
      h[[x]]$dta <- h[[x]]$dta[, n, with = FALSE]
    }
    # chunked files ----
    if ("ipip-sf_chnk" %in% class(h[[x]])) {
      n1 <- names(h[[x]]$indx$dta_n)
      n <- unique(c(n1[n1 %ilike% "*id$|date_time"],
        n1[n1 %in% c(f_params$phen_name, f_params$phen_gap)
        ]
      ))
      h[[x]]$hsf_phens <- n
    }
    # add in the associated gap table ----
    if (!is.null(g) && nrow(g) > 0) {
      n2 <- f_params[phen_name != phen_gap]
      gi <- g[table_name %in% x][phen %in% c("logger", n, n2$phen_name)]
      gl <- gi[phen %in% "logger"]
      gi <- gi[!phen %in% "logger"]
      n <- h[[x]]$hsf_phens[!h[[x]]$hsf_phens %ilike% "*id$|date_time"]
      glz <- data.table::rbindlist(lapply(n, function(nx) {
        glx <- gl
        glx$phen <- nx
        return(glx)
      }))
      if (nrow(gi) > 0) {
        if (!h[[x]]$indx$ri %in% "discnt") {
          glzovlp  <- lubridate::as.duration(h[[x]]$indx$ri)
        } else {
          glzovlp <- lubridate::as.duration(0)
        }
        glz_n <- names(glz)
        names(glz) <- paste0("gz_", names(glz))
        # join gaps onto fore logger gap
        glz <- glz[, ":="(gl1 = gz_gap_start - glzovlp, gl2 = gz_gap_end,
            gz_p = gz_phen
          )
        ]
        gi <- gi[, ":="(gi1 = gap_start, gi2 = gap_end, p = phen)]
        # join data
        z <- glz[gi, on = .(gl2 >= gi1, gl1 >= gi2, gz_p == p)][
          order(phen, gap_start, gap_end)
        ]
        sdc <- c("gap_start", "gap_end", "gz_gap_start", "gz_gap_end")
        z <- z[, gz_gap_start := do.call(pmin, c(.SD, na.rm = TRUE)),
          .SDcols = sdc
        ]
        z <- z[, gz_gap_end := do.call(pmax, c(.SD, na.rm = TRUE)),
          .SDcols = sdc
        ]
        z <- z[, ":="(gz_gid = data.table::fifelse(
          is.na(gz_gid), gid, gz_gid
        ), gz_eid = data.table::fifelse(
          is.na(gz_eid), eid, gz_eid
        ), gz_gap_type = data.table::fifelse(
          is.na(gz_gap_type), gap_type, gz_gap_type
        ), gz_phen = data.table::fifelse(
          is.na(gz_phen), phen, gz_phen
        ), gz_table_name = data.table::fifelse(
          is.na(gz_table_name), table_name, gz_table_name
        ), gz_problem_gap = data.table::fifelse(
          is.na(gz_problem_gap), problem_gap, gz_problem_gap
        ), gz_notes = data.table::fifelse(
          is.na(gz_notes), notes, gz_notes
        ))][, c(paste0("gz_", glz_n)), with = FALSE]
        z <- z[, ":="(gl1 = gz_gap_start, gl2 = gz_gap_end + glzovlp,
            gz_p = gz_phen
          )
        ]
        # join phen gaps onto post logger gap ends
        z <- gi[z, on = .(gi1 <= gl2, gi2 >= gl1, p == gz_p)]
        z <- z[, gz_gap_start := do.call(pmin, c(.SD, na.rm = TRUE)),
          .SDcols = sdc
        ]
        z <- z[, gz_gap_end := do.call(pmax, c(.SD, na.rm = TRUE)),
          .SDcols = sdc
        ]
        z <- z[order(phen, gz_gap_start, gz_gap_end)]
        z <- z[, gz_gap_start := do.call(min, c(.SD, na.rm = TRUE)),
          .SDcols = sdc, by = .(phen, gz_gap_start)
        ]
        z <- z[, gz_gap_end := do.call(max, c(.SD, na.rm = TRUE)),
          .SDcols = sdc, by = .(phen, gz_gap_start)
        ]
        z <- z[, gz_gap_start := do.call(min, c(.SD, na.rm = TRUE)),
          .SDcols = sdc, by = .(phen, gz_gap_end)
        ]
        z <- z[, gz_gap_end := do.call(max, c(.SD, na.rm = TRUE)),
          .SDcols = sdc, by = .(phen, gz_gap_end)
        ]
        # organise merged gaps
        z <- unique(z,
          by = c("phen", "table_name", "gz_gap_start", "gz_gap_end")
        )
        z <- z[, c(paste0("gz_", glz_n)), with = FALSE]
        names(z) <- gsub("^gz_", "", names(z))
        z <- z[, dt_diff_s := difftime(gap_end, gap_start, units = "secs")]
      } else {
        z <- glz
      }
      gxg <- z[, p := phen]
      gxg <- n2[gxg, on = .(phen_name == p)][,
        phen := data.table::fifelse(!is.na(phen_gap), phen_gap, phen)
      ][, names(g), with = FALSE]
    } else {
      gxg <- g
    }
    # change phen name if so specified by phen gaps
    h[[x]]$gaps <- gxg
    # save the data/index
    saveRDS(h[[x]], file.path(dirname(sfc)[1], paste0(ppsid, "_hsf_table_", x)))
    ipayipi::msg("Harvested data link info", xtra_v)
    if (xtra_v) print(h[[x]]$indx$dta_n)
    return(x)
  })
  return(list(hsf_dts = hsf_names, ppsij = ppsij))
}