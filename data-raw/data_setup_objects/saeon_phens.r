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
phens_sts <- rl[qa == TRUE & !is.na(phen_name)
][order(phen_name, units, measure)]

# generate a phentab for this info
phens_sts <- data.table::data.table(
  phid = seq_len(nrow(phens_sts)),
  phen_name_full = as.character(phens_sts$phen_name_full),
  phen_type = as.character(phens_sts$phen_type),
  phen_name = as.character(phens_sts$phen_name),
  units = as.character(phens_sts$units),
  measure = as.character(phens_sts$measure),
  offset = NA_character_,
  var_type = as.character(phens_sts$var_type),
  uz_phen_name = as.character(phens_sts$phen_name),
  uz_units = as.character(phens_sts$units),
  uz_measure = "no_spec",
  f_convert = NA_real_,
  sensor_id = "no_spec",
  notes = as.character(phens_sts$notes)
)
phens_sts[phen_name == "lat", "uz_phen_name"] <- "Latitude"
phens_sts[phen_name == "long", "uz_phen_name"] <- "Longitude"
phens_sts[uz_phen_name == "Latitude", "phen_name"] <- "lat"
phens_sts[uz_phen_name == "Longitude", "phen_name"] <- "long"
phens_sts[uz_phen_name == "Latitude", "uz_units"] <- "no_spec"
phens_sts[uz_phen_name == "Longitude", "uz_units"] <- "no_spec"

class(phens_sts) <- c(class(phens_sts), "phens_sts")
# write this to R package data
usethis::use_data(phens_sts, overwrite = TRUE)
