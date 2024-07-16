# Code to prepare a logger setup for Sononist loggers
#  xle file format.
library(ipayipi)
solonist <- list(
  file_format = "xle",
  logger_type = "instrument_type",
  logger_sn = "serial",
  logger_os = "firmware",
  logger_program_name = "created",
  logger_title = "model",
  table_name = "raw_data",
  date_time = "date_time",
  data_tree = "//Body_xle/Data/Log",
  log_file_tree = "//Body_xle/File_info",
  log_meta_tree = "//Body_xle/Instrument_info",
  instrument_info_tree = "//Body_xle/Instrument_info_data_header",
  phen_info_tree = "//Body_xle/*[starts-with(name(), 'Ch')]",
  date_time_tree = c("//Body_xle/Data/Log/Date", "//Body_xle/Data/Log/Time")
  )
usethis::use_data(solonist, overwrite = TRUE)
