#' @title Plot 
log_interval <- function(file = NULL,
                         ylim_upper = NULL # can be used to set the upper limit of the y axis
                         ){
  interval <- file$log_data$Date_time[2:length(file$log_data$Date_time)] - file$log_data$Date_time[1:(length(file$log_data$Date_time)-1)]
  file$log_data$interval[2:length(file$log_data$Date_time)] <- interval
  require(ggplot2)
  p1 <- ggplot(file$log_data) +
    aes(x=Date_time, y=interval) +
    xlab('Date-time') +
    ylab("Time interval")
  p1 <- p1 + geom_point()
  p1 <- p1 + ggtitle(label = 'Logger recording interval',
                     subtitle = paste0('Borehole: ',file$xle_LoggerHeader$Project_ID[1]," ",file$xle_LoggerHeader$Location[1]))
  if(!is.null(ylim_upper)){p1 <- p1 + ylim(c(0,ylim_upper))}
  return(p1)
}
