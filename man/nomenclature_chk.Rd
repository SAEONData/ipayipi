% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/y_c1_nomenclature_chk.R
\name{nomenclature_chk}
\alias{nomenclature_chk}
\title{Check ipayipi logger data file/header nomenclature}
\usage{
nomenclature_chk(
  pipe_house = NA,
  csv_out = TRUE,
  file_ext = "ipr",
  verbose = TRUE,
  xtra_v = FALSE,
  ...
)
}
\arguments{
\item{pipe_house}{List of pipeline directories. \emph{See} \code{\link[=ipip_house]{ipip_house()}} \emph{for details}.}

\item{file_ext}{The extension of the file for which the nomenclature is being assessed.}

\item{verbose}{Logical. Print some details and progress of function progress?}

\item{xtra_v}{Logical. Should some 'x'tra messaging be done? Use to help diagnose problems, and for guidance.}

\item{out_csv}{Logical. If TRUE a csv file is made in the working directory if there are files with unrecognised nomenclature.}
}
\value{
Returns a csv nomenclature file when unrecognised synonyms/data attributes are detected. Screen output notes whether the nomenclature table is complete.
}
\description{
Critical step in the data pipeline. A function to check standardisation of logger data header information. If an unrecognised
synonym/data attribute appears the nomenclature database will have to be updated.
}
\details{
Standardising header metadata is a key step in a data pipeline and will determine what and how data are collated and processed. Check your standards carefully. Once your header metadata standards have been build following the prompts in this function, run \code{\link[=header_sts]{header_sts()}} again. This will read in the most recently updated standards (wither the csv file or 'aa_notab.rns') and apply these to files in the \code{d2_wait_room}.
\subsection{Header metadata}{

When \code{ipayipi} imbibes logger data the data setup (e.g., \code{ipayipi::hobo_rain}) allows for harvesting of logger file header information. This function assists with standardising this header information, storing this with header synonyms and other metadata in a table in the data pipelinese 'd2_wait_room' directory. The file name of the table is set by default to 'aa_nomtab.rns' (an rds file). If this file is deleted then the synonym database will need to be rebuilt using this function.
}

\subsection{Editing the nomenclature table:}{

When unrecognised synonyms are introduced (e.g., after a change in logger program setup, or simply a change in file name spelling) this function creates a csv table in the \code{d2_wait_room} that should be edited to maintain the synonym database. \emph{Unstandardised header information is preffixed in the nomenclature table (csv) by 'uz}'._ These unstandardised columns must not be edited by the user, but the standardised header information can be added by replacing corresponding \code{NA} values with appropriate standards.
The following fields in the nomenclature table may require editing/standardisation:
\itemize{
\item '\strong{location}': The region name used to subset a group of stations by area.
\item '\strong{station}': The name of the station collecting logger data.
\item '\strong{stnd_title}': The standard station title. It is recommended that this title is given as the concatenation of the location and station field above, seperated by an underscore.
\item '\strong{record_interval_type}': The record interval type; one of the following values: 'continuous', 'event_based', or 'mixed'.
\item '\strong{record_interval}': \code{\link[=imbibe_raw_logger_dt]{imbibe_raw_logger_dt()}} uses \code{\link[=record_interval_eval]{record_interval_eval()}} to evaulate a date-time series to determine the record interval. Invariably the record interval is evaluated correctly, but if insufficient data errors may occur. Therefore the record interval parameters need to be verified whilst checking nomenclature.
\item '\strong{table_name}': The name of the table. By default the preffix to logger data is 'raw_'. Within the pipeline structure the name appended to this preffix should be the standardised record interval, e.g., 'raw_5_mins', for continuous 5 minute data, or 'raw_discnt' for raw event based data.
\strong{NB!} Only edit these fields (above) of the nomenclature table during standardisation.
Once this csv file has been edited and saved it can be pulled into the pipeline structure by running \code{\link[=read_nomtab_csv]{read_nomtab_csv()}} --- running \code{\link[=header_sts]{header_sts()}} will call \code{\link[=read_nomtab_csv]{read_nomtab_csv()}}.
}
}

\subsection{What header information is used/standardised?:}{

Note that this function will only assess files for which the station name is known. If the station name is known, the "uz_station", "logger_type", "record_interval_type", "record_interval", and "uz_table_name" are used to define unique station and station table entries, that require standardisation.
Note that \code{record_interval_type} and \code{record_interval} are evaluated during the \code{\link[=imbibe_raw_logger_dt]{imbibe_raw_logger_dt()}} function.
}

\subsection{Standards:}{

The 'ipayipi' data pipeline strongly suggests conforming with 'tidyverse' data standards. Therefore, use lower case characters, no special characters, except for the underscore character which should be used for spacing.
}

\subsection{File extensions:}{

Note that after imbibing raw files with \code{\link[=imbibe_raw_batch]{imbibe_raw_batch()}} files are given the '.ipr' extension. Only '.ipr' files are processed by this function, that is, \code{header_sts}. Once their header information has been standardised, files are given the '.iph' extension. '.iph' files are ready for the next step in standardisation --- \code{\link[=phenomena_sts]{phenomena_sts()}}.
}

\subsection{Other:}{

This function calls \code{\link[=nomenclature_chk]{nomenclature_chk()}} which will take take the user interactively through a process of standardising station names and titles.
Station phenomena can only be checked once header nomenclature has been standardised.

Note that there is no unstandardised 'uz' location. Whatever location provided to in nomenclature table will be used in naming conventions.
The 'location' field can be provided edited during the \code{\link[=nomenclature_chk]{nomenclature_chk()}} and \code{\link[=read_nomtab_csv]{read_nomtab_csv()}} functionality.
}
}
\author{
Paul J. Gordijn
}
\keyword{file}
\keyword{file-header}
\keyword{information,}
\keyword{interval}
\keyword{metadata,}
\keyword{nomenclature,}
\keyword{record}
\keyword{station}
\keyword{synonyms,}
