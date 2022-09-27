library(checkpoint)
checkpoint("2020-04-01")
library(DBI)
library(dplyr)
library(tidyverse)
library(magrittr)
library(data.table)
library(sf)
library(rstan)
library(carboutil)
library(broom)


con <- carboutil::get_conn(user="basi", host='emp-sql-cs1')

#Get list of sensors
sens_q <- sql("SELECT DISTINCT Type, 
              Sensors.SensorUnit_ID FROM SensorUnits 
              JOIN Sensors ON 
              SensorUnits.SensorUnit_ID = Sensors.SensorUnit_ID WHERE Type IN ('HPP', 'LP8')
              AND (SensorUnits.SensorUnit_ID BETWEEN 1010 AND 1400) OR (SensorUnits.SensorUnit_ID BETWEEN 390 AND 500)")
sens_tb <- tbl(con, sens_q)


get_data_from_decentlab <- function(con, SensorUnit_ID, full=TRUE){
  
  lp8_cols <- c( 'battery',
                 'senseair-lp8-temperature-last'
                 ,'senseair-lp8-temperature'
                 ,'sensirion-sht21-temperature-last'
                 ,'sensirion-sht21-temperature'
                 ,'sensirion-sht21-humidity'
                 ,'senseair-lp8-vcap2'
                 ,'senseair-lp8-vcap1'
                 ,'senseair-lp8-co2-filtered'
                 ,'senseair-lp8-co2'
                 ,'senseair-lp8-ir-filtered'
                 ,'senseair-lp8-ir-last'
                 ,'senseair-lp8-ir'
                 ,'senseair-lp8-status'
                 ,'time')
  
  sens <- paste('/',glue::glue_collapse(lp8_cols, sep = '|'),'/', sep='')
  dev_q <- paste('/', SensorUnit_ID, '/', sep='')
  print(paste("Getting data for", SensorUnit_ID))
  print(sens)
  time_filter <- ifelse(full, 'time < now()', 'time > now() - 30d')

  dt <- carboutil::query_decentlab(carboutil::get_decentlab_api_key(), 
                                   device=dev_q, 
                                   time_filter = time_filter , 
                                   sensor = sens) %>%
  dplyr::rename('SensorUnit_ID'='sensor') %>%
    dplyr::rename_all(function(x) stringr::str_replace('-','_'))
  #Write in temp table
  db_write_table(con, dt, table = 'LP8_Data_temp')
  #Copy 
}

copy_data_to_db <- function(data, con){
  db_drop_table(con, 'LP8_Data_temp')
  #Write data to temporary table
  db_write_table(con, data,  table = 'LP8_Data_temp')
  #Query to copy data
  cp_q <-"
  INSERT INTO CarboSense.LP8_Data
  SELECT
  * 
  FROM LP8_Data_temp
  WHERE timestamp >  UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 30 DAY))
  ON DUPLICATE KEY UPDATE"
  DBI::dbExecute(con, cp_q)
  
}






sens_tb %>%
  collect() %>%
  filter(Type=='LP8') %>%
  group_by(Type, SensorUnit_ID) %>%
  group_walk(
    ~get_data_from_decentlab(con, .y$SensorUnit_ID, .y$Type) %>%
      copy_data_to_db(., con)
  )


