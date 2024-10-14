.onAttach <- function(libname, ipayipi) {
  # load some packages quitely
  # list required packages
  packages <- c("crayon")

  # Install packages not yet installed
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
  }
  # Packages loading
  invisible(lapply(packages, library, character.only = TRUE))
  packageStartupMessage(
    cat(
      white("Loading saeon ") %+%
        bold$cyan("ipayipi ") %+%
        white("v0.0.4  ") %+%
        silver(" --- https://github.com/SAEONData/ipayipi")
    )
  )
}
.onLoad <- function(libname, ipayipi) {
  op <- options()
  op.ipip <- list(
    chunk_dir = tempdir()
  )
  toset <- !(names(op.ipip) %in% names(op))
  if (any(toset)) options(op.ipip[toset])
  invisible()
}