% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/h0_dt_process_batch.R
\name{dt_process_batch}
\alias{dt_process_batch}
\title{Batch processing of pipeline data: \emph{dt}}
\usage{
dt_process_batch(
  pipe_house = NULL,
  pipe_seq = NULL,
  stages = NULL,
  overwrite_pipe_memory = FALSE,
  output_dt_preffix = "dt_",
  output_dt_suffix = NULL,
  wanted = NULL,
  unwanted = NULL,
  prompt = FALSE,
  unwanted_tbls = "_tmp",
  verbose = FALSE,
  xtra_v = FALSE,
  chunk_v = FALSE,
  dt_format = c("Ymd HMOS", "Ymd HMS", "Ymd IMOSp", "Ymd IMSp", "ymd HMOS", "ymd HMS",
    "ymd IMOSp", "ymd IMSp", "mdY HMOS", "mdY HMS", "mdy HMOS", "mdy HMS", "mdy IMOSp",
    "mdy IMSp", "dmY HMOS", "dmY HMS", "dmy HMOS", "dmy HMS", "dmy IMOSp", "dmy IMSp"),
  dt_tz = "Africa/Johannesburg",
  future.stdout = TRUE,
  ...
)
}
\arguments{
\item{pipe_house}{List of pipeline directories. \emph{See} \code{\link[=ipip_house]{ipip_house()}} \emph{for details}.}

\item{pipe_seq}{A \code{pipe_seq} object descirbes sequential data processing stages and steps in iPayipi. A \code{pipe_seq} can be build using \code{ipayipi::pipe_seq()}. Defaults to \code{NULL}.
\itemize{
\item If this argument is provided, then upon its successful evaluation, the \code{pipe_seq} is embedded a respective station file with associated metadata.
\item If this argument is not provided then the embedded \code{pipe_seq} (if there is one) will be used for data processing. To overwrite an existing stations \code{pipe_seq} set \code{overwrite_pipe_memory} to \code{TRUE}, and provide the new \code{pipe_seq} object to this argument.
}}

\item{stages}{Integer vector denoting the consecutive stages of the \code{pipe_seq} object to process. Can be used to split a processing pipeline to allow additional processing between stages in the interlude (note: each stage contains multiple steps).  Set to \code{0} (zero) for only running \code{pipe_seq} evaluation.}

\item{overwrite_pipe_memory}{Logical. If \code{TRUE} then the stations pipeline steps, that are described by the \code{pipe_seq} object/table, are overwritten by the new \code{pipe_seq} object.}

\item{output_dt_preffix}{The output table preffix which defaults to "dt_".}

\item{output_dt_suffix}{A custom suffix to be appended to the output tables name.}

\item{wanted}{Regex string of files to select for listing. Seperate search tags by using the bar character '|'.}

\item{unwanted}{Regex string of files to filter out the listing. Seperate search tags by using the bar character '|'.
in the import.}

\item{prompt}{Set to TRUE for interactive mode. Note this will not work if embedded in a parallel processing instance.}

\item{unwanted_tbls}{Some tables generated by the processing pipeline don't need to be stored permanently in the station file object. By adding keywords to this argument (separated by the '|' character) these 'unwanted' (or temporary) tables will be removed from the station file. Defaults to \verb{_tmp} --- so any table with this search key in its name will be removed.}

\item{verbose}{Logical. Print some details and progress of function progress?}

\item{xtra_v}{Logical. Should some 'x'tra messaging be done? Use to help diagnose problems, and for guidance.}

\item{dt_format}{The function attempts to work out the date-time format from a vector of format types supplied to this argument. The testing is done via \code{\link[lubridate:parse_date_time]{lubridate::parse_date_time()}}. \code{\link[lubridate:parse_date_time]{lubridate::parse_date_time()}} prioritizes the tesing of date-time formats in the order vector of formats supplied. The default vector of date-time formats supplied should work well for most logger outputs. \bold{NB!} seconds are required.}

\item{dt_tz}{Recognized time-zone (character string) of the data locale. The default for the package is South African, i.e., "Africa/Johannesburg" which is equivalent to "SAST".}

\item{station_ext}{The extension of the station file. Defaults to 'ipip'.}
}
\description{
Batch process ipayipi station data data by stages and steps. First processing steps are evaluated to build relevant metadata, then data is accordingly processed.
}
\details{
This function forms the basis of setting up a sequential, data-processing pipeline. In this process, raw or other data from an 'ipayipi' station file is harvested, and further processing of this data can be run.

The first part of the processing stage of the \code{ipayipi} data pipeline is to set up pipe stages, each with its own sequence of processing steps, using \code{\link[=pipe_seq]{pipe_seq()}}. Once the sequence is set up it can be parsed to \code{\link[=dt_process]{dt_process()}}. The six main functions that \code{\link[=dt_process_batch]{dt_process_batch()}} uses to process data are:
\itemize{
\item \code{\link[=dt_harvest]{dt_harvest()}}: for harvesting station/other data.
\item \code{\link[=dt_calc]{dt_calc()}}: running \code{data.table} chained calculations on harvested data.
\item \code{\link[=dt_agg]{dt_agg()}}: Aggregate phenomena/variables by custom or default functions.
Defaults functions are determined by phenomena descriptions in \code{phens} tables, i.e., their measure, variable type, and units. Default aggregation functions are housed in the \link{sts_agg_functions} table.
\item \code{\link[=dt_join]{dt_join()}}: Used to merge harvested data sets via simple or comlex 'fuzzy' type joins based on time intervals using \code{data.table}s join syntax, and ipayipi's own time-series sensitive data joining \code{\link[=append_phen_data]{append_phen_data()}}, implemented through \code{\link[=mhlanga]{mhlanga()}}.
\item \code{\link[=dt_clean]{dt_clean()}}: A recursive, temporally segmented filter used to detect anomalies and impute data through applicaiton of a generalised Hampel signal processing. \emph{This is a new function and under current development.}
\item \code{\link[=dt_drift]{dt_drift()}}: Used to correct time series data based on calibraion readings.  \emph{This is a new function and under current development.}
\item dt_flag(): \emph{To develop}. Implementation of standardised flags. Options should include what type of flags (variance, spikes, thresholds etc. Generates flag tables that can be visualised in the pipeline.
}

The above functions can be specified in the \code{pipe_seq} function. \code{pipe_seq} runs partial to fuller evaluation of pipeline structure to promote seemless processing. Fuller evaluation of a \code{\link[=pipe_seq]{pipe_seq()}} is performed within \code{\link[=dt_process_batch]{dt_process_batch()}}; during this process, station data (both internally and externally harvested data) are read so new-phenomena metadata can be generated. All this to minimise potential error during data processing.
Processed data, function parameters, and new phenomena summaries are returned and appended to station files for future use.
\subsection{Raw station data: It is not recommended to edit raw station data. The recommended/default preffix for raw station data table<s in iPayipi begins with the preffix \strong{raw_}, followed by the standard record interval strong, e.g., 'raw_5_mins'.}{

##\code{stages}: When splitting up stages, if proceeeding stages rely on the outputs of preceeding steps, there will be problems in processing down the pipeline. Note that any \code{output_dt} ending in '_tmp' is not saved to a station file. Therefore, if a processing pipeline has to be interupted using \code{stages}, e.g. for data imputation, then avoid saving outputs with the '_tmp' suffix.
}

\subsection{\code{pipe_seq} and pipeline evaluation}{

Pipeline evaluation is done parially when first describing the various pipeline stages and steps. Evaluation is done using a separate function for each step operation.
}

\subsection{Reserved characters}{
\itemize{
\item 'dt_': This is the recommended suffix for tables produced using \code{dt_process_batch()} or \code{dt_process}.
\item '_tmp': used as a suffix on temporary output tables. These are removed from station files at the end of processing steps (or after completing set \code{stages}).
\item '\emph{fltr}': substring in the table name used to store original and replacement values as produced by \code{\link[=dt_clean]{dt_clean()}}.
}
}
}
\author{
Paul J. Gordijn
}
\keyword{aggregation;}
\keyword{calculations;}
\keyword{data}
\keyword{data;}
\keyword{event}
\keyword{join}
\keyword{metadata}
\keyword{processing;}
\keyword{series}
\keyword{time}
\keyword{transformations;}
