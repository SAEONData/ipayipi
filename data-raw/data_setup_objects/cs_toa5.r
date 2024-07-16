# Code to prepare a logger setup for the Cambell Scientific
#  TOA5 file format.
library(ipayipi)
cs_toa5 <- list(try1 = list(
  file_format = rici(ri = 1, ci = 1),
  station_title = rici(ri = 1, ci = 2),
  logger_type = rici(ri = 1, ci = 3),
  logger_sn = rici(ri = 1, ci = 4),
  logger_os = rici(ri = 1, ci = 5),
  logger_program_name = rici(ri = 1, ci = 6),
  logger_program_sig = rici(ri = 1, ci = 7),
  logger_title = NULL,
  table_name = rici(ri = 1, ci = 8),
  date_time = 1,
  dttm_inc_exc = c(FALSE, TRUE),
  phen_name = rng_rici(r_fx = 2, c_fx_start = 3,
    c_fx_end = "extract", setup_name = "phen_name"
  ),
  phen_unit = rng_rici(r_fx = 3, c_fx_start = 3,
    c_fx_end = "extract", setup_name = "phen_unit"
  ),
  phen_measure = rng_rici(r_fx = 4, c_fx_start = 3,
    c_fx_end = "extract", setup_name = "phen_measure"
  ),
  data_row = 5, id_col = 2
))
usethis::use_data(cs_toa5, overwrite = TRUE)
