#' @title Remove sites from processed data and logs
#' @description This function removes the site from the processed data folder
#' and records of this site in log files.
#' @param solr_room The repository where the processed solonist data is located.
#' @param recurr Should the search for data include subrepositories i.e. be
#' recursive? Logical
#' @details This function will help remove the data file and associated history
#' of a site from the processed data repository. The function may be useful for
#' simply removing a site from the repository or before rerunning the site
#' series_ function.
#' @author Paul J Gordijn
#' @export

gw_remove_site <- function(
  solr_room = NULL,
  recurr = FALSE,
  ...
) {
  cr_msg <- padr(core_message = paste0("Warning! Removing site data",
    " and traces from ipayipi."), pad_char = " ",
    pad_extras = c("!", "", "", "!"),
    force_extras = FALSE, justf = c(-1, 3))
  message(cr_msg)

  if (is.null(solr_room)) {
    stop("Please specify the data directory")
  }

  slist <- gw_rsol_list(input_dir = solr_room, recurr = recurr,
    baros = TRUE, prompt = TRUE)

  sapply(slist, FUN = function(x) {
    site <- sub(x = x, pattern = ".rds", replacement = "")

    # remove file history from the rdta_log
    llist <- list.files(path = solr_room, pattern = "rdta_log.rds",
      recursive = recurr)
    if (!length(llist) == 1) {
      stop("No rdta_log.rds file or duplicate exists!")
    }
    rdta_log <- readRDS(file.path(solr_room, "rdta_log.rds"))
    rdta_log$full_name <- paste0(rdta_log$Location, "_", rdta_log$Borehole)
    rdta_log <- rdta_log[which(rdta_log$full_name != site), ]
    saveRDS(rdta_log, file.path(solr_room, "rdta_log.rds"))

    # remove file history from the transfer log
    llist <- list.files(path = solr_room, pattern = "transfer_log.rds",
        recursive = recurr)
    if (!length(llist) == 1) {
      stop("No transfer_log.rds file or duplicate exists!")
    }
    t_log <- readRDS(file.path(solr_room, "transfer_log.rds"))
    t_log$full_name <- gsub(".{22}$", "", t_log$xle_file)
    t_log <- t_log[which(t_log$full_name != site), ]
    saveRDS(t_log, file.path(solr_room, "transfer_log.rds"))

    # we don't need to remove site info from the datum and retreive logs

    # remove site rds file
    file.remove(file.path(solr_room, x))
    cr_msg <- padr(core_message = paste0("Site removed: ", x,
      collapse = ""), pad_char = " ", pad_extras = c("|", "", "", "|"),
      force_extras = FALSE, justf = c(-1, 3))
    return(message(cr_msg))
  })
}