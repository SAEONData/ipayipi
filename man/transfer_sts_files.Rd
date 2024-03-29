% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/z_d_transfer_sts_files.r
\name{transfer_sts_files}
\alias{transfer_sts_files}
\title{Transfer standardised ipayipi data files to the 'nomvet_room'}
\usage{
transfer_sts_files(
  pipe_house = NULL,
  prompt = FALSE,
  recurr = TRUE,
  unwanted = NULL,
  wanted = NULL,
  file_ext_in = ".ipi",
  file_ext_out = ".ipi",
  verbose = FALSE,
  cores = getOption("mc.cores", 2L),
  ...
)
}
\arguments{
\item{pipe_house}{List of pipeline directories. \strong{See}
\code{ipayipi::ipip_init()} \strong{for details}.}

\item{prompt}{Should the function use an interactive file selection function
otherwise all files are returned. TRUE or FALSE.}

\item{recurr}{Should the function search recursively into sub directories
for hobo rainfall csv export files? TRUE or FALSE.}

\item{unwanted}{Vector of strings listing files that should not be included
in the import.}

\item{wanted}{Vector of strings listing files that should not be
included in the import.}

\item{file_ext_in}{The extension of the files being imported from the
'wait_room'. Defaults to ".ipi". Note the full-stop must be included.}

\item{file_ext_out}{The file extension that will be used for the saving of
files in the 'nomvet_room'. Defaults to ".ipi".}

\item{verbose}{Print some details on the files being processed? Logical.}

\item{cores}{Allocate number of cores for processing function. Utilises \code{parallel::mclapply()}; only works on linux.}
}
\value{
Saves files in native R format for 'ipayipi'. Returns a list of
file names which were successfully transferred.
}
\description{
Moves files from the 'wait_room' to the 'nomvet_room', which
contains standardised logger data files. This function avoids duplicating
files in the 'nomvet_room' and ensures file names are unique. Files will be
named using their station and table names, their date coverage, and an
integer (that is unique per data file & respective date-time stamps).
}
\details{
Aim: Archive each standardised data files in a
set directory, that is, the '\strong{nomvet_room}' --- part of the
'ipayipi'  data pipeline. Once archived the data will be used for
further processing. Here are some of the specific steps of this funtion:
\enumerate{
\item Generate inventories of standardised files (with file extension
\code{file_ext_in}) that have been processed by \code{ipayipi::nomenclature_sts()}
and 'ipayipi::phenomena_sts()' in the 'nomvet_room'.
\item Determine which files need to be transferred (to avoid duplication)
into the 'nomvet_room'.
\item Transfer files---saving these in RDS format in the 'nomvet_room' with
the specified extension \code{file_ext_out}.
}
}
\author{
Paul J. Gordijn
}
\keyword{Meteorological}
\keyword{batch}
\keyword{data}
\keyword{data;}
\keyword{files;}
\keyword{processing;}
\keyword{saving}
\keyword{standardisation;}
