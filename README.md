<!-- badges: start -->
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/pauljeco/ipayipi?branch=dev_remote)
<!-- badges: end -->

<img align="right" width="40%" height="40%" src="https://github.com/pauljeco/ipayipi/blob/main/img/ipayipi_120.png">

## **iPayipi** — data processing pipeline

- 'iPayipi' (say 'ee-pie-ee-pea'; [\\ē\\p\\ī\\ē\\ˈē]; isiZulu for a 'pipe', or a homeless person)
- Build a traceable data pipeline
- Standardise, archive, and processing of time series data and metadata
- Cutting-edge speed whilst processing `big data'---uses [data.table](https://github.com/Rdatatable/data.table) under the hood
- Generic pipeline construction for open data
- Queries, corrections, plotting, and imputation

## Package installation

Install the R package 'ipayipi' from Github using `devtools` in your R terminal. If you don't have devtools installed you'll have to run `install.packages('devtools')` in your R console (Windows users will need Rtools, a 'devtools' prerequisite [follow this link to download Rtools for Windows](https://cran.r-project.org/bin/windows/Rtools/)). To install 'ipayipi' from github run the following in your R console:


```
#' for latest releases
devtools::install_github("pauljeco/ipayipi/dev_remote")
#' stable releases
devtools::install_github("SAEONdata/ipayipi")
```

## Building and running a data pipeline
'ipayipi' pipelines consist of six main phases:

1. Initiate pipeline housing.
    - `ipip_house()`: this function builds a pipe house directory.
2. Importing raw data.
    - `logger_data_import_batch()`: to bring in raw data from a source directory.
3. Imbibing and standardising raw data.
    - `imbibe_raw_batch()`: Reads imported data into the pipeline format.
    - `header_sts()`: For standardising header data, e.g., the station name or title.
    - `phenomena_sts()`: To get variables or phenomena data and metadata standardised.
    - `transfer_sts()`: Pushes standardised data to an archive for building station files.
4. Appending data streams.
    - `append_station_batch()`: Builds station records.
    - `gap_eval_batch()`: Clarify & visualize data gaps.
    - `meta_read()` & `meta_to_station()`: Optional functions to incorporate field or other metadata into station records for further processing.
5. Processing data.
    In order to process data a sequence of processing stages need to be defined (see `?pipe_seq`). Once defined, this sequence gets embedded into respective stations, evaluated and used to process data.
    - `dt_process_batch()`: To batch process data.
6. Querying and visualising data.
    There are a few built-in plotting functions utilizing dygraphs, ggplot2, and plotly libraries.
    - `dta_flat_pull()`: To harvest continuous data from stations in long or wide formats.
    - Various plotting functions to examine/cross-examine data.

## Common questions
 - What types of data can be processed? iPayipi processes raw-flat files from loggers collecting continuous and discontinuous time-series data in unencrypted formats, e.g., csv, txt, tsv. There are tested import formats for generic flat-data files, hobo-rainfall data, xml Solonist water-level logger files, COA5 formatted Cambell Scientific dat files, and meteorological data from SAEON's Terrestrial Observation Monitor. Additional custom formats can be implemented by the user.
 Whiles organising these raw-data files into the pipeline structure can be straight forward, the processing/analysis of the data is highly customisable, requiring a good understanding of the data and desired outcome.
 - Are there 'cut and paste' scripts for processing my data? Yes, but ...
 You should know up to what quality level you need your data processed at. Level 0 is raw data, and maybe aggregated. Level 1 works towards checking removing obviously faulty data with some basic imputation. Checks are generally univariate in level one. Level 2 involves multivariate corrections and checks, plus drift correction, and level 3 gets into cross validation of data. Level 4 involves rigorous cross validation, regression models and understanding more about uncertainty around models developed for advanced data imputation.
 - What standards does iPayipi use? iPayipi is a diverse ecosystem and highly customisable. The pipeline structure standards attempt to preserve raw data so that working up and back down (for later updates) are possible. In terms of nomenclature standards, iPayipi follows SAEON's nomenclature 'machine readable', 'tidyverse' aligned formatting. 

## Under the hood
iPayipi utilizes R [data.table](https://github.com/Rdatatable/data.table) for processing data, widely known as R's most efficient (for speed and big data) processing package, and has been out competing native python in this regard. Owing to 'data.table's intuitive syntax (which inspired the python [datatable](https://github.com/h2oai/datatable) package) 'ipayipi' has been built using 'data.table' functionality. This is important for the user to know, especially where familiarity with 'data.table' syntax is necessary to customise data processing sequences (e.g., `dt_calc()`).