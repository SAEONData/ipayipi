## code to prepare `ftbl`
sts_agg_functions <- data.table::data.table(
  measure = c("smp", "avg", "tot", "min", "max", "sd", "logi"),
  f_continuous_desc = c("mean", "mean", "sum", "min", "max", "mean", NA),
  f_continuous = c("mean(<>)", "mean(<>)", "sum(<>)", "min(<>)", "max(<>)",
    "mean(<>)", NA),
  f_factor = c("modal", NA, NA, NA, NA, NA, NA),
  f_circular = c("ipayipi::circular_mean(<>)", "ipayipi::circular_mean(<>)",
    "sum(<>)", "min(<>)", "max(<>)", "mean(<>)", NA),
  f_logical = c(NA, NA, NA, NA, NA, NA, "sum(<>)")
)
usethis::use_data(sts_agg_functions, overwrite = TRUE)
