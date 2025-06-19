# Code to establish default file extensions and associated types
library(ipayipi)
file_read_meta <- data.table::data.table(
  file_ext = c("\\.csv|\\.dat", ".txt|.tsv", "", "\\.xle|\\.xml"),
  sep = c(",", "\t", "auto", NA_character_)
)
usethis::use_data(file_read_meta, overwrite = TRUE)
