% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sub_dt_flag_param_eval.R
\name{flag_param_eval}
\alias{flag_param_eval}
\title{Evaluate arguments for pipeline flags and checks filter.}
\usage{
flag_param_eval(
  flag_tbl = NULL,
  q1 = TRUE,
  robust = TRUE,
  full_eval = FALSE,
  station_file = NULL,
  ppsij = NULL,
  f_params = NULL,
  sfc = NULL,
  verbose = FALSE,
  xtra_v = FALSE,
  ...
)
}
\arguments{
\item{flag_tbl}{Flag table .... Must be a data.table object.}

\item{q1}{???}

\item{robust}{Logical. Set to \code{TRUE} to use median to interpolate single missing values in finest res data. \code{FALSE} will use the mean to interpolate.}

\item{full_eval}{Logical that indicates the depth of evaluation. This argument is provided parsed internally. Full evaluation can only be completed within \code{ipayipi::dt_process()} sequence evaluation.
param f_params}

\item{station_file}{Name of the station being processed.}

\item{ppsij}{Data processing \code{pipe_seq} table from which function parameters and data are extracted/evaluated. This is parsed to this function automatically by \code{ipayipi::dt_process()}.}

\item{sfc}{List of file paths to the temporary station file directory. Generated using \code{ipayipi::sf_open_con()}.}

\item{verbose}{Logical. Whether or not to report messages and progress.}

\item{xtra_v}{Logical. Whether or not to report xtra messages, progess, plus print data tables.}
}
\description{
Evaluation of the input-flag table that will be used by \link{dt_flag} for performing basic checks on '\bold{raw}' data.
}
\details{
Standard flag tables ...
}
\author{
Paul J. Gordijn
}
