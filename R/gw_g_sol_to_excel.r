#' @title Exports R solonist data and transformations to csv format
#' @description Function that exports selected solonist R objects to csv tables.
#' @param input_dir Directory in which to search for R solonist objects to
#'  export as csv files.
#' @param output_dir Directory to which the csv files will be exported.
#' @param recurr Searches recursively in the input directory for R solonist
#'  objects when set to TRUE.
#' @param prompted If TRUE (as default), then the user has the option to
#'  interactively select the R solonist objects.
#' @return Exports csv files.
#' @author Paul J Gordijn
#' @export
sol_to_csv <- function(input_dir=".",
                         output_dir=".",
                         recurr=FALSE,
                         prompted=TRUE,
                         ...) {
  slist <- gw_rsol_list(input_dir = input_dir, recurr = recurr, prompt = prompted)
  if (length(slist) == 0) {
    stop("There are no R data solonist files in the working directory")
    }

  setwd(input_dir)
  print(
    "******************* exporting Rsol files to Excel **********************")
  sapply(slist, FUN = function(emm) {
    emma <- sub(x = emm, pattern = ".rds", replacement = "")
    print(paste0(">~ ", emma, " ~<"))
    emmat <- gsub("__[^__]+$", "", emma)
    sol_temp <- readRDS(file = emm)
    for (i in 1:length(sol_temp)) {
        if (i == 1) {
            appended <- FALSE
        }else appended <- TRUE
      sheet_name <- names(sol_temp[i])
      if (sheet_name != "xle_phenomena") {
        suppressWarnings({
            write.csv(sol_temp[i],
            file = paste0(output_dir, "/", emma, "_", sheet_name, ".csv"),
                na = "NA", col.names = T)})
        }else{
        for (j in 1:length(sol_temp[i])) {
          sheet_name <- sheet_name[j]
          suppressWarnings({
                write.csv(sol_temp[j],
                    file = paste0(output_dir, "/", emma, "_xle_phenomena_",
                    sheet_name, ".csv"), na = "NA", col.names = T)})
          }
          }
    }
    }
    )
}