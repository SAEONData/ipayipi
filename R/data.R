#' Standardised phenomena name and metadata table
#'
#' Descriptions with metadata of phenomena standardised as per the SAEON
#' iPayipi format.
#'
#' @format ## 'sts_phens'
#' A data.table with 15 columns:
#' \describe{
#'   \item{phid}{The unique phenomena identifier}
#'   \item{phen_name_full}{A description of the phenomena. Follow convention of 'phen_type' : description of measure/units/extras}
#'   \item{phen_type}{The type of phenomena being measured. Add new values to the 'phen_type_tbl_sts' sheet.}
#'   \item{phen_name}{"The 'column' or 'field' name used in ipayipi. Duplicate 'phen_name' are not allowed in this worksheet. 
#' The measure is include as a suffix to 'phen_name' except 1) where the measure is a sample ('smp'), and/or 2) NB! the suffix '_sn' denotes a serial number or identifier."}
#'   \item{units}{Units standardised for ipayipi. Add new values to the 'var_type_tbl_sts' sheet.}
#'   \item{measure}{Measure standardised for ipayipi. Add new values to the 'measure_type_tbl_sts' sheet.}
#'   \item{offset}{A numeric additive that is applied to raw data during standardisation, e.g., 'phenomena_sts()'.  Add new values to the 'var_type_tbl_sts' sheet.}
#'   \item{var_type}{The phenomena variable type standardised for ipayipi.}
#'   \item{uz_phen_name}{The 'column' or 'field' name used in the raw data which is 'unstandardised' ('_uz') for ipayipi.}
#'   \item{uz_units}{The units used in the raw data which is 'unstandardised' ('_uz') for ipayipi.}
#'   \item{uz_measure}{The measure used in the raw data which is 'unstandardised' ('_uz') for ipayipi.}
#'   \item{f_convert}{A conversion factor. Applies as the product (multiplier) of the 'raw' data. Can be used to transform units allowing the standardised data units to differ from 'raw' data. The conversion factor is applied by the 'phenomena_sts()' ipayipi function.}
#'   \item{sensor_id}{The phenomena's unique identifier.}
#'   \item{notes}{Additional notes, user input.}
#' }
#' @examples
#' # print phenomena standards table
#' ipayipi::sts_phens
#'
#' @source Internal.
"sts_phens"

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

#' Header harvest: SAEON Terrestrial Observation Monitor
#'
#' ipayipi data setup list for Cambell Scientific TOA5 format files
#'
#' @format ## 'cs_stom'
#' An ipayipi data_setup list for imbibing (`ipayipi::imbibe_raw_logger_dt()`)
#' data from the SAEON Terrestrial Observation Monitor (STOM).
#' Data from STOM follows SAEON iPayipi's phenomena nomenclature---when running
#' `phenomena_sts()` set `external_phentab = ipayipi::sts_phens`.
#' a `data_setup`.
#' @source Internal.
"cs_stom"

#' Header harvest: Cambell Scientific, TOA5
#'
#' ipayipi data setup list for Cambell Scientific TOA5 format files
#'
#' @format ## 'cs_toa5'
#' An ipayipi data_setup list for imbibing (`ipayipi::imbibe_raw_logger_dt()`)
#' TOA5 formatted data (Cambell Scientific). The 'location' name in the data
#' setupis not specified. Only generic information is provided for harvesting
#' of logger file header information.
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
#' read/imbibe raw data files. This table is used internally in:
#' `ipayipi::imbibe_raw_batch()`. If the `file_ext` and/or `col_dlm`
#' arguments are `NULL` this table assists with reading in data in the
#' correct format. Additional formats can be inserted into this data object.
#' @source Internal
"file_read_meta"

#' Met flags
#'
#' Flagging thresholds and other checks for meteorological data
#'
#' @format ## 'flag_tbl'
#' iPayipi flag_tbl table used as input for ... when attempting to
#' @source Internal
"file_read_meta"