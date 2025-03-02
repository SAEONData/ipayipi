.onAttach <- function(libname, ipayipi) {
  packageStartupMessage(
    cat(
      cli::style_blurred(c(cli::col_white("Loading saeon "))),
      cli::style_bold(cli::col_magenta("ipayipi ")),
      cli::col_br_magenta("v0.0.7  "),
      cli::style_blurred(
        cli::col_white("|> https://github.com/SAEONData/ipayipi\n")
      )#,
      # cli::col_blue(c("\nNB! If migrating from package versions < 0.0.7 |> ",
      #   "version upgrade notes:"
      # ))
    )
  )
  packageStartupMessage(cli::cli_inform(c(
    "*" = cli::col_grey(
      "Always check fuction help files ;)"
    ),
    "*" = cli::col_grey(
      paste0("Use options {.var verbose} and {.var xtra_v} for messages ",
        "on setting up and processing"
      )
    )
  )))
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