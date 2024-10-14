#' @title update_data summary
#' @description Temporary function to update the data summary of nomvet files to include the inc exc columns
update_ds <- function(x) {
  "%ilike%" <- NULL
  if (!any(names(x) %ilike% "^dttm_i")) {
    # add default options
    x$dttm_inc_exc <- TRUE
    x$dttm_ie_chng <- FALSE
    ns <- c("dsid", "file_format", "uz_station", "location", "station",
      "stnd_title", "start_dttm", "end_dttm", "logger_type", "logger_title",
      "logger_sn", "logger_os", "logger_program_name", "logger_program_sig",
      "uz_record_interval_type", "uz_record_interval", "record_interval_type",
      "record_interval", "dttm_inc_exc", "dttm_ie_chng", "uz_table_name",
      "table_name", "nomvet_name", "file_origin"
    )
    data.table::setcolorder(x, ns)
  }
  return(x)
}