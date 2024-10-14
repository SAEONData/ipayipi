<!-- badges: start -->
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/pauljeco/ipayipi?branch=dev_remote)
<!-- badges: end -->

<img align="right" width="40%" height="40%" src="https://github.com/pauljeco/ipayipi/blob/main/img/ipayipi_120.png">

## **iPayipi** — data processing pipeline

- 'iPayipi' (say 'ee-pie-ee-pea'; [\\ē\\p\\ī\\ē\\ˈē]; isiZulu for a 'pipe', or a homeless person)
- Build a traceable data pipeline
- Standardise, archive, and processing of time series data and metadata
- Cutting-edge speed whilst processing `big data'---uses [data.table](https://github.com/Rdatatable/data.table) under the hood
- Advanced processing of groundwater data
- Generic pipeline construction for open data

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
    - `logger_data_import_batch()`: to bring in raw data from a source directory. The source may be be cleaned upon data imports, in which case the source directory will be emptied and original raw data archived.
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
    - `dta_flat_pull()`: To harvest data from stations in long or wide formats.
    - Various plotting functions to examine/cross-examine data.

## Common questions
 - iPayipi will process raw-flat files in unencrypted formats, e.g., csv, txt, tsv, but has been mainly used for event-based rainfall data, as well as various types of meteorological data. iPayipi comtains a seperate pipeline for advanced processing and visualisation of groundwater data.

## Under the hood
iPayipi utilizes R [data.table](https://github.com/Rdatatable/data.table) for processing data, widely known as R's most efficient (for speed and big data) processing package, and has been out competing native python in this regard. Owing to 'data.table's intuitive syntax (which inspired the python [datatable](https://github.com/h2oai/datatable) package) 'ipayipi' has been built using 'data.table' functionality. This is important for the user to know, especially where familiarity with 'data.table' syntax is necessary to customise data processing sequences (e.g., `dt_calc()`).
