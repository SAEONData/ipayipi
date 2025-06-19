# set met flag standards
# import met flags
flag_met <- googlesheets4::read_sheet(
  ss = file.path("https://docs.google.com/spreadsheets/d",
    "11O8uoiZxAG1AGD-6S5hpZgQ_OotIN_uQ7P8GNoXFMfg",
    "edit?gid=0#gid=0"
  ),
  sheet = "q_level",
  range = "A1:S",
  col_names = TRUE,
  col_types = c("iiccccciiccdcdccccc"),
  na = ""
)
flag_met <- data.table::as.data.table(flag_met)
usethis::use_data(flag_met, overwrite = TRUE)
