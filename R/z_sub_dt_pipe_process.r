#' @title Organise and update a data processing pipeline
#' @description 
#' @param pipe_seq Pipeline processing steps generated using a list of
#'  `ipayipi::p_dt()`. Both or either the `pipe_seq` or `pipe_memory` must be
#'  supplied.
#' @param pipe_memory A station processing pipeline summary in table format.
#'  If supplied this will be updated via comparison with pipe_seq.
#' @param overwrite_pipe_memory If TRUE then extant pipeline processing
#'  summaries are overwritten, and _vice versa_.
#' @param start_dttm Earliest date time for which data have been processed in
#'   this table.
#' @param end_dttm Latest date time for which data have been processed in
#'   this table.
#' @author Paul J. Gordijn
#' @export
#' @return A standardised pipe process summary and a tag indicating whether an
#'  update has been made to the pipeline. If an update has been made the
#'  updates have to the entire length/duration, not only a time slice, of the
#'  processing pipelines data.
pipe_process <- function(
  pipe_seq = NULL,
  pipe_memory = NULL,
  overwrite_pipe_memory = TRUE,
  pipe_eval = TRUE,
  update_pipe_data = FALSE,
  ...) {
  # run basic pipe sequence check
  pipe_seq <- ipayipi::pipe_seq(pipe_seq = pipe_seq, pipe_eval = pipe_eval)
  pipe_memory <- pipe_seq
  # compare standardised pipe_seq with pipe_memory --- this doesn't eval
  #  function parameters
  if (!is.null(pipe_memory)) {
    if (overwrite_pipe_memory == TRUE && !is.null(pipe_memory)) {
      # generate old and new pipe seq tables
      pps_old <- pipe_memory[order(dt_n, dtp_n)]
      pps_old$id <- paste0(pps_old$dt_n, "_", pps_old$dtp_n)
      pps_new <- pipe_seq[order(dt_n, dtp_n)]
      pps_new$id <- paste0(pps_new$dt_n, "_", pps_new$dtp_n)
      pps_common <- pps_old[id %in% pps_new$id]
      pps_common$f <- data.table::fifelse(
        pps_common$f != pps_new[id %in% pps_old$id]$f,
        pps_new[id %in% pps_old$id]$f, pps_common$f)
      pps_common$input_dt <- data.table::fifelse(
        pps_common$input_dt != pps_new[id %in% pps_old$id]$input_dt,
        pps_new[id %in% pps_old$id]$input_dt, pps_common$input_dt)
      pps_common$output_dt <- data.table::fifelse(
        pps_common$output_dt != pps_new[id %in% pps_old$id]$output_dt,
        pps_new[id %in% pps_old$id]$output_dt, pps_common$output_dt)
      pps <- rbind(
        pps_new[!id %in% pps_common$id],
        pps_new[!id %in% pps_common$id],
        pps_common)[order(dt_n, dtp_n)]
      pps <- subset(pps, select = -id)
    } else {
      pps <- pipe_seq[order(dt_n, dtp_n)]
    }
  }
  return(list(pipe_seq = pipe_seq, update_pipe_data = update_pipe_data,
    overwrite_pipe_memory = overwrite_pipe_memory))
}
