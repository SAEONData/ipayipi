## code to prepare `sts_phen_measure` dataset
sts_phen_measure <- data.table::data.table(
  phen_prop = c("smp", "tot", "min", "max", "avg", "cumm", "logi"),
  phen_syn = c("smp|samp|smpl|sample", "total|tot", "min|mn|minimum",
    "max|mx|maximum", "average|avg|mean", "cum|cumm|cml|cummulative",
    "logical"),
  phen_desc = c("A sample or instantantaneous measure.",
    "The total amound or sum of values.",
    "The minimum.",
    "The maximum.",
    "The average or mean.",
    "The cummulative amount.",
    "A logical variable, that is, true or false.")
)
usethis::use_data(sts_phen_measure, overwrite = TRUE)
