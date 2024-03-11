## code to prepare `sts_phen_var_type` dataset goes here
sts_phen_var_type <- data.table::data.table(
  phen_prop = c("num", "int", "chr", "fac", "posix", "logi", "difftime"),
  phen_syn = c("numeric|num|double|dbl", "integer|int",
    "str|string|chr|character|char",
    "fac|factor|fact", "date|time|posix|date-time|posixct|dttm",
    "logical|lgcl|lcl|lgc|logi",
    "difftime")
)
usethis::use_data(sts_phen_var_type, overwrite = TRUE)
