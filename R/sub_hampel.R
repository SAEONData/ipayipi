#' @title iPayipi Generalised Hampel filter
#' @description Applies the non-linear (non-parametric) hampel filter to elements of a vector. The hampel is used to detect ourliers and produce estimates for imputation. If the focal value is an NA value or an 'outlier' the median is returned.
#' @param x Values on which to perform the hampel filter evaluation.
#' @param w Total width of the filter window.
#' @param d The number of deviations from the central tendacy of the window used to detect outlier values.
#' @param align String matching one of the following options: 1) 'right' where the value under evaluation is on the right of a window; 2) 'left' where the value being evaluated is on the left of the window; and 3) 'centre' meaning that the value being evaluated is in the centre of the window. If the window length is even, then the 'centre' is biased to the left.
#' @param series_mad The median absolute deviation of the entire series being evaluated.
#' @keywords outlier detection; imputation; missing data; patching;
#' @details
#' # General:
#'  The hampel filter has been widely adopted for outlier detection in time-series data. The filter is non-parametric with overall robust behaviour (Pearson et al. 2016). Unlike the Tukey filter that has been based on standard deviation from the mean, the Hampel assesses median-absolute deviation (MAD) within the window of interest (ref). The filter is fully customisable and can be tued by adjusting the window size (`w`) and the amount of accceptable deviation from the MAD (`d`). If the focal value falls outside the range of the window median, plus minus MAD * d, it is considered an outliler and the median returned.
#'
#' ## `align`:
#' In addition to tuning `w` and `d` the focal value of the window can be set as `centre`, `left`, or `right`. See the parameter description for more.
#'
#' ## NA values:
#' The filter will remove NA values when determining the median and MAD, and continue to function with these up until the proportional-tolderance level of NA's in a window is surpassed, or there are no non-NA values in a window. The default proportion of NAs in a window, until where the function will operate is 0.25. When this level of NAs is reached the focal value in a window is not assessed in relation to the MAD and retured as is. When the focal value is NA the median is returned.
#'
#' # Implosion sequences:
#' Owing to the failure of the Hampel where MAD approximates 0 when there is little to no deviation in data (i.e., an implosion sequence) (Pearson et al. 2016); this function temporariliy adjusts window data by adding minimal, normally-distributed random noise. This noise is approximated with a thousanth of the series MAD around a mean of zero. By adding this noise this customisation avoids the implosion sequence caveat of the Hampel filter.
#'
#' # Advanced implementation:
#' This filter can be recursively deployed using `dt_clean()`. Moreover, deployement in a data stream can be segmented where appropriate, e.g., for data with level shifts that require correction post outlier detection---segmentation can be implemented in between levels.
#'
#' # References:
#' Pearson, R.K., Y. Neuvo, J. Astola, and M. Gabbouj. 2016. Generalized Hampel filters. EURASIP Journal on Advances in Signal Processing, 2016(87). [doi:10.1186/s13634-016-0383-6](https://asp-eurasipjournals.springeropen.com/articles/10.1186/s13634-016-0383-6)
#' @export
#' @md
#' @author Paul J Gordijn
hampel <- function(
  x = NULL,
  w = 21,
  d = 3,
  na_t = 0.25,
  #tighten = 1, not yet implemented
  #robust = TRUE, not yet implemented
  align = "right",
  series_mad = NULL,
  ...
) {

  # get position of v in x
  # follows data.table align argument
  if (align %in% "right") {
    pos <- length(x)
  } else if (align %in% "left") {
    pos <- 1
  } else { # centre align is biased to the left
    pos <- ceiling(w / 2)
  }

  # get v
  v <- x[pos]

  # get number of nas
  if ((length(x[is.na(x)]) / length(x)) > na_t) return(v)

  # determine the proportion of unique values in series
  # if it is greater than 50 % add a thousandth of the
  # overall series mad to all values
  if (max(table(x)) >= (w / 2)) {
    x <- x + rnorm(mean = series_mad * 0.001, sd = 0.001, n = w)
  }

  # if the focal value falls outside the accepted madf or
  # the value is na replace with median
  x <- x[!seq_len(w) %in% pos]
  m <- median_f(x)
  if (is.na(v)) return(m)
  mdf <- mad_f(x) * d
  if (any(v < (m - mdf), v > (m + mdf)) && v != m) return(m)

  return(v)
}