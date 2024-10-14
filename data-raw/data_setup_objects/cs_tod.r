# Code to import data from the SAEON terrestrial observation monitor
library(ipayipi)
cs_tod <- list(try1 = list(
  file_format = "saeon_tod", # saeon terresetrial observation download
  station_title = "!fp!", # extract title from download file name
  logger_type = "cambell", # miscellaneous cambell loggers
  table_name = "raw", # just raw for now
  date_time = 1, # column where date_time data are located
  dttm_inc_exc = c(FALSE, TRUE), # does the data include the current dttm
  # stamp, must this be changed?
  phen_name = rng_rici(
    r_fx = 2, # row from which to extract phen names
    c_fx_start = 2, # from from which (left to right) to start extracting
    # phen names
    c_fx_end = "extract", # unless a number is provided then the function
    # will readin the last data column dynamically if set to 'exact'.
    setup_name = "phen_name" # the name of the parameter being extracted
  ),
  # similar to phen_name above, but for extracting units
  phen_unit = rng_rici(r_fx = 3, c_fx_start = 2,
    c_fx_end = "extract", setup_name = "phen_unit"
  ),
  data_row = 4 # first row where data begins, i.e., no header info.
))
# write this to R package data
usethis::use_data(cs_tod, overwrite = TRUE)
