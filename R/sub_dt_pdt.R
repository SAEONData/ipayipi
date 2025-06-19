#' @title Merge pipe steps into processing pipeline
#' @description Joins `ipayipi::pdt_steps()` into a processing pipeline.
#' @export
pdt <- function(...) {
  p <- list(...)
  # retains only objects of native class
  p <- p[sapply(p, function(x) any(class(x) %in% "pipe_process"))]

  # add or check sequential number of pipe
  dtns <- unique(sapply(p, function(x) x$dt_n))
  pi <- lapply(seq_along(dtns), function(i) {
    pi <- lapply(p, function(y) {
      if (y$dt_n == dtns[i]) {
        y$dt_n <- i
      } else {
        y <- NULL
      }
      y
    })
    pi[!sapply(pi, is.null)]
  })
  pi <- lapply(seq_along(pi), function(i) {
    lapply(seq_along(pi[[i]]), function(j) {
      pi[[i]][[j]]["dtp_n"] <- j
      pi[[i]][[j]]
    })
  })
  unlist(pi, recursive = FALSE)
}
