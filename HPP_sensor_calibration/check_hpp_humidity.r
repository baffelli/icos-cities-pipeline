
library(tidyverse)

con <- carboutil::get_conn()

q <- 'SELECT DISTINCT SensorUnit_ID FROM Deployment WHERE Date_UTC_to > CURDATE() AND SensorUnit_ID BETWEEN 400 AND 500'
sens <- dplyr::tbl(con, sql(q)) %>% collect()


get_cal <- function(sens){
  key <- carboutil::get_decentlab_api_key()
  carboutil::query_influxdb.csv(domain = 'swiss.co2.live',
                                apiKey = key,
                                sensor ="/^(calibration|sensirion-sht21-humidity)$/",
                                device = stringr::str_interp("/^(${sens})$/"),
                                timeFilter = 'time > now() - 7d')
}


prepare_cal_info <- function(cal_data){
  cal_data %>%
    group_by(node) %>%
    arrange(time) %>%
    mutate(cal_change=c(0,cumsum(diff(calibration)!=0))) %>%
    group_by(cal_change, node) %>%
    arrange(time) %>%
    mutate(cal_dur=(time - min(time))/1000) %>%
    filter(calibration==1)
}


plot_cal_result <- function(cal_data, base_dir='/project/'){
  sens <- unique(cal_data$node)
  ggplot(cal_data, aes(x=cal_dur,y=`sensirion-sht21-humidity`)) +
    geom_point(color='lightblue') +
    geom_smooth()+
    xlab('Duration since calibration start [s]') +
    ylab('Sensirion SHT21 RH') + ggtitle(stringr::str_interp('Bottle Calibration for ${sens}'))
}

save_cal_result <-

map(sens$SensorUnit_ID,~ get_cal(.x) %>%
      prepare_cal_info() %>%
      plot_cal_result()
)
