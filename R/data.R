#' Standardised phenomena or variable types
#'
#' Table of short names describing variable types to aid standardisation
#' and definition of phenomena/variable types in `ipayipi`. Short names
#' are similar to that used by the `tidyverse`.
#'
#' @format ## 'sts_phen_var_type'
#' A data.table with two columns:
#' \describe{
#'   \item{phen_prop}{The 'proper'/standardised shortname used for a
#'    variable type}
#'   \item{phen_syn}{A synonym of the variable type. These are the keywords
#'    used by `ipayipi` to match up and standardise to the proper variable
#'     name}
#' }
#' @source Internal.
"sts_phen_var_type"

#' Standardesed phenomena or variable measures
#'
#' Table of measure types with synonyms and descriptions.
#'
#' @format ## 'sts_phen_measure'
#' \describe{
#'   \item{phen_prop}{The 'proper' or standardised shortname used to denote
#'     a type of measure}
#'   \item{phen_syn}{Measure synonyms. Multiple arguments are seperated by
#'     a bar character.}
#'   \item{phen_desc}{Full description of measure.}}
#' @source Internal.
"sts_phen_measure"

#' Aggregation function table
#'
#' Table of aggregation functions based on phenomena or variable types, the
#'  type of measure (e.g., mean or standard deviation).
#' Based on the variable type and measure the aggregataion function is
#'  selected from this table.
#'
#' @format ## 'sts_agg_functions'
#' A data.table with six columns:
#' \describe{
#'   \item{measure}{The standardised measure, e.g., an average or sample.}
#'   \item{f_continuous_desc}{Brief description of the measure function that is
#'    used by default when aggregating different types of data.}
#'   \item{f_continuous}{Text to be evaluated during aggregation of continuous.
#'     variables. When aggregating data the '<>' is replaced with the values and
#'      extra arguments to be evaluated.}
#'   \item{f_factor}{Text to be evaluated during aggregation of factoral
#'     variables.}
#'   \item{f_circular}{Text to be evaluated during aggregation of circular
#'     variables.}
#'   \item{f_logical}{Text to be evaluated during aggregation of circular
#'     variables.}
#' }
#' @source Internal.
"sts_agg_functions"

#' Header harvest: Cambell Scientific, TOA5
#'
#' ipayipi data setup list for Cambell Scientific TOA5 format files
#'
#' @format ## 'cs_toa5'
#' An ipayipi data_setup list for imbibing (`ipayipi::imbibe_raw_logger_dt()`)
#' TOA5 formatted data. The 'location' name is not specified. Only generic
#' information is provided for harvesting of logger file header information.
#' __See `ipayipi::imbibe_raw_logger_dt()` documentation for a describtion of
#' a `data_setup`.
#' @source Internal.
"cs_toa5"

#' Header harvest: Exported hobo rainfall files
#'
#' ipayipi data setup for Hobo logger files exported from a 'rainfall pendant'
#'
#' @format ## 'hobo_rain'
#' iPayipi data_setup list for imibing (`ipayipi::imbibe_raw_logger_dt()`)
#' Hobo logger data from a Hobo rainfall logger pendant. The setup includes
#' harvseting of the loggers serial number using the search pattern
#' 'LGN.S.N..'. The 'location' name is not specified. Only generic
#' information is provided for harvesting of logger file header information.
#' __See `ipayipi::imbibe_raw_logger_dt()` documentation for a describtion of
#' a `data_setup`.
#' This data_setup contains two varieties of hobo pendant logger outputs, the
#' first option contains an id column, whilst the second does not.
#' @source Internal
"hobo_rain"

#' File reading metadata
#'
#' File defaults used when imbibing raw logger data
#'
#' @format ## 'file_read_meta'
#' iPayipi file_read_meta table used for setting defaults when attempting to
#' read/imbibe raw data files. This table is used in:
#' `ipayipi::imbibe_raw_batch()`. If the `file_ext` and/or `col_dlm`
#' arguments are `NULL` this table assists with reading in data in the
#' correct format.
#' @source Internal
"file_read_meta"