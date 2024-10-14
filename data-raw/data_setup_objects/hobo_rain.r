# Code to prepare a logger setup for exported hobo rainfall logger files.
library(ipayipi)
hobo_string_extract_logger_sn <- list(
  rng_pattern = "LGR.S.N..",
  rel_start = 9,
  rel_end = 16
)
hobo_rain <- list(try_1 = list(
  file_format = "hobo_csv_export",
  station_title = rici(ri = 1, ci = 1),
  logger_type = "hobo_pendant",
  logger_sn = rng_rici(r_fx = 2, c_fx_start = 3,
    c_fx_end = "extract", setup_name = "logger_sn",
    string_extract = hobo_string_extract_logger_sn
  ),
  table_name = "hobo_data",
  date_time = 2,
  phen_name = rng_rici(r_fx = 2, c_fx_start = 3,
    c_fx_end = "extract", setup_name = "phen_name"
  ),
  data_row = 3, id_col = 1
), try_2 = list( # in try two there is no 'id' column
  file_format = "hobo_csv_export",
  station_title = rici(ri = 1, ci = 1),
  logger_type = "hobo_pendant",
  logger_sn = rng_rici(r_fx = 2, c_fx_start = 2,
    c_fx_end = "extract", setup_name = "logger_sn",
    string_extract = hobo_string_extract_logger_sn
  ),
  table_name = "hobo_data",
  date_time = 1,
  phen_name = rng_rici(r_fx = 2, c_fx_start = 2,
    c_fx_end = "extract", setup_name = "phen_name"
  ),
  data_row = 3
), try_3 = list( # in try two there is no 'id' column and stnd title
  file_format = "hobo_csv_export",
  station_title = "!fp!",
  logger_type = "hobo_pendant",
  logger_sn = rng_rici(r_fx = 1, c_fx_start = 1,
    c_fx_end = "extract", setup_name = "logger_sn",
    string_extract = hobo_string_extract_logger_sn
  ),
  table_name = "hobo_data",
  date_time = 1,
  phen_name = rng_rici(r_fx = 1, c_fx_start = 2,
    c_fx_end = "extract", setup_name = "phen_name"
  ),
  data_row = 2
), try_4 = list( # in try four there is no 'id' column and stnd title
  # and the date and time are in seperate columns
  file_format = "hobo_csv_export",
  station_title = "!fp!",
  logger_type = "hobo_pendant",
  logger_sn = rng_rici(r_fx = 1, c_fx_start = 1,
    c_fx_end = "extract", setup_name = "logger_sn",
    string_extract = hobo_string_extract_logger_sn
  ),
  table_name = "hobo_data",
  date_time = 1:2,
  phen_name = rng_rici(r_fx = 1, c_fx_start = 3,
    c_fx_end = "extract", setup_name = "phen_name"
  ),
  data_row = 2
))
usethis::use_data(hobo_rain, overwrite = TRUE)
