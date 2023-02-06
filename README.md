<img align="right" width="40%" height="40%" src="https://github.com/SAEONData/ipayipi/blob/master/img/ipayipi_120.png">

# ipayipi — data processing pipeline

- Build a traceable data pipeline
- Standardise, archive, and processing of time series data and metadata
- Speed when handling `big data' through
'data.table'
- Advanced processing of groundwater data*
- Generic pipeline construction for open data

Currently, advanced groundwater and rainfall processing does not use the generic pipeline functions, but does follow the same principles. A merger of the advanced processing functions into the generic pipeline is under way.

# Package installation

You can install dependencies in your R terminal like so ...

```
# list required packages for ipayipi
packages <- c("attempt", "data.table", "devtools", "egg", "DT", "dygraphs",
    "ggplot2", "googlesheets4", "khroma", "lubridate", "parallel", "plotly",
    "readxl", "XML", "xml2", "xts")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
}
```

Then install 'ipayipi' with `devtools` in your R terminal.

```
devtools::install_github("SAEONData/ipayipi")
```
