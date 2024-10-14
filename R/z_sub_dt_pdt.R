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
      return(y)
    })
    pi <- pi[!sapply(pi, is.null)]
    return(pi)
  })
  pi <- lapply(seq_along(pi), function(i) {
    y <- lapply(seq_along(pi[[i]]), function(j) {
      pi[[i]][[j]]["dtp_n"] <- j
      return(pi[[i]][[j]])
    })
    return(y)
  })
  p <- unlist(pi, recursive = FALSE)
  return(p)
}
