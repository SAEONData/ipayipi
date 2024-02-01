# Code to establish default file extensions and associated types
library(ipayipi)
file_read_meta <- data.table::data.table(
  file_ext = c("\\.csv|\\.dat", ".txt|.tsv", ""),
  sep = c(",", "\t", "auto")
)
usethis::use_data(file_read_meta, overwrite = TRUE)
