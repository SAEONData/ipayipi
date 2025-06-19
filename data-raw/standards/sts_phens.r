library(ipayipi)
# download saeon phenomena standards from google sheets and
# save as standards table
rl <- googlesheets4::read_sheet(
  ss = "13jeNbd_nQh3EWjd7krw_PKnycF_tKbH59LQ7lmUA9aY",
  sheet = "phentab_sts",
  range = "A3:I",
  col_names = TRUE,
  col_types = c("lcccccccc"),
  na = c("NA", "")
)
rl <- as.data.table(rl) # set as data.table
# only retain auality assured rows and order the table
sts_phens <- rl[qa == TRUE & !is.na(phen_name)
][order(phen_name, units, measure)]

# generate a phentab for this info
sts_phens <- data.table::data.table(
  phid = seq_len(nrow(sts_phens)),
  phen_name_full = as.character(sts_phens$phen_name_full),
  phen_type = as.character(sts_phens$phen_type),
  phen_name = as.character(sts_phens$phen_name),
  units = as.character(sts_phens$units),
  measure = as.character(sts_phens$measure),
  offset = NA_character_,
  var_type = as.character(sts_phens$var_type),
  uz_phen_name = as.character(sts_phens$phen_name),
  uz_units = as.character(sts_phens$units),
  uz_measure = "no_spec",
  f_convert = NA_real_,
  sensor_id = "no_spec",
  notes = as.character(sts_phens$notes)
)
sts_phens[phen_name == "lat", "uz_phen_name"] <- "Latitude"
sts_phens[phen_name == "long", "uz_phen_name"] <- "Longitude"
sts_phens[uz_phen_name == "Latitude", "phen_name"] <- "lat"
sts_phens[uz_phen_name == "Longitude", "phen_name"] <- "long"
sts_phens[uz_phen_name == "Latitude", "uz_units"] <- "no_spec"
sts_phens[uz_phen_name == "Longitude", "uz_units"] <- "no_spec"

class(sts_phens) <- c(class(sts_phens), "sts_phens")

# write this to R package data
usethis::use_data(sts_phens, overwrite = TRUE)
