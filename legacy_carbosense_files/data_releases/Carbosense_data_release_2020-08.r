# Carbosense_data_release_2019-10.r
# ---------------------------------
# Original Author: Michael M?ller 
# Edited by: Simone Baffelli
#
#
# ---------------------------------

# Remarks:
# - Computations refer to UTC.
#
#

## clear variables
rm(list=ls(all=TRUE))
gc()
setwd("Z:/CarboSense/Software/")
## libraries
library(checkpoint)
checkpoint("2020-04-01")
library(DBI)
require(RMySQL)
library(sf)
library(tidyverse)

#



### ----------------------------------------------------------------------------------------------------------------------------

## Directories and file names

resultdir <- "K:/Carbosense/ICOS_Data_Release/Carbosense_CO2_Data_Release_September_2020/Carbosense_CO2_Data_Release/Data/"
plotdir <-  "K:/Carbosense/ICOS_Data_Release/Carbosense_CO2_Data_Release_September_2020/Carbosense_CO2_Data_Release/Plots/"


fn_Locations   <- paste(resultdir,"Location.csv",sep="")

fn_Deployments <- paste(resultdir,"Deployment.csv",sep="")

### ----------------------------------------------------------------------------------------------------------------------------

## Release period

Release_Date_UTC_from  <- lubridate::as_datetime("2017-07-01 00:00:00")
Release_Date_UTC_to    <- lubridate::as_datetime("2020-09-01 00:00:00")
exc_loc <- c('DUE1','DUE2','DUE3','DUE4','DUE5','MET1')
### ----------------------------------------------------------------------------------------------------------------------------

## Deployment information
con <- carboutil::get_conn(user="basi", host = 'emp-sql-cs1')

dep_q <-
"
SELECT
  * 
FROM Deployment
WHERE LocationName NOT IN ({exc_loc*}) AND Date_UTC_to >= {Release_Date_UTC_from} AND Date_UTC_from <= {Release_Date_UTC_to}
"

deps <- tbl(con, sql(glue::glue_sql(dep_q, .con = con))) %>%
  collect() %>%
  mutate(Date_UTC_from = lubridate::as_datetime(Date_UTC_from),
         Date_UTC_to = lubridate::as_datetime(Date_UTC_to)) %>%
  #If the deployment lasted longer than the export period, replace the date
  mutate(Date_UTC_from = if_else(Date_UTC_from < Release_Date_UTC_from,Release_Date_UTC_from, Date_UTC_from),
         Date_UTC_to = if_else(Date_UTC_to > Release_Date_UTC_to ,Release_Date_UTC_to, Date_UTC_to)) %>%
  # Omit some deployments due to known issues
  # ORDS, SU 1313, 2017-07-17 -- 2018-02-02 : affected by station ventilation
  filter(!(SensorUnit_ID   == 1313 
                 & LocationName  == 'ORDS' 
                 & Date_UTC_from == strptime("20170717230000","%Y%m%d%H%M%S",tz="UTC")
                 & Date_UTC_to   == strptime("20180202000000","%Y%m%d%H%M%S",tz="UTC")))


## Location information

loc_q <-
  "
  SELECT
  *
  FROM Location
  WHERE LocationName IN
  (
    SELECT DISTINCT
    LocationName
    FROM Deployment
    WHERE LocationName NOT IN ({exc_loc*})
  )
  "
loc_tbl <- tbl(con, sql(glue::glue_sql(.con=con, loc_q))) %>%
  collect() %>%
  #Apply swisstopo formula
  mutate(H_WGS84=h + 49.55 - 12.60 * (Y_LV03 - 600000)/1e6 - 22.64 * (X_LV03 - 200000)/1e6) %>%
  st_as_sf(coords=c("Y_LV03", "X_LV03"), crs=21781, remove=F) %>%
  st_transform(crs=4326) %>%
  mutate(
    LON_WGS84_TRSF=st_coordinates(.)[,1],
    LAT_WGS84_TRSF=st_coordinates(.)[,2]) %>%
  filter(abs(LON_WGS84-LON_WGS84_TRSF)<3e-5 & abs(LAT_WGS84-LAT_WGS84_TRSF)<3e-5) %>%
  st_drop_geometry() %>%
  mutate(
    Y_LV03       = round(Y_LV03,0),
    X_LV03       = round(X_LV03,0),
    h            = round(h,1),
    LON_WGS84    = round(LON_WGS84,5),
    LAT_WGS84    = round(LAT_WGS84,5),
    H_WGS84      = round(H_WGS84,2)
  ) %>%
  dplyr::select(LocationName,
         Y_LV03,      
         X_LV03,      
         h,           
         LON_WGS84,   
         LAT_WGS84,  
         H_WGS84,     
         Canton,      
         SiteType)

#Save locations table
readr::write_csv(loc_tbl, fn_Locations, append = FALSE)



DBI::dbDisconnect(con)
### ----------------------------------------------------------------------------------------------------------------------------
con <- carboutil::get_conn(user="basi", host = 'emp-sql-cs1')

#Query to get data
lp8_query <- "
              SELECT
                timestamp,
                FROM_UNIXTIME(CO2.timestamp) AS Date,
                (CASE WHEN LRH_FLAG = 1 THEN CO2_A ELSE -999 END) AS CO2,
                 timestamp - COALESCE(LAG(timestamp) OVER (PARTITION BY CO2.SensorUnit_ID ORDER BY timestamp), timestamp) AS dt,
                H2O,
                SHT21_T AS T,
                SHT21_RH AS RH
              FROM CarboSense_CO2_TEST00 AS CO2
                LEFT JOIN SensorExclusionPeriods AS excl
                ON CO2.SensorUnit_ID = excl.SensorUnit_ID
                AND (NOT (timestamp BETWEEN UNIX_TIMESTAMP(excl.Date_UTC_from) AND UNIX_TIMESTAMP(excl.Date_UTC_to)) OR ISNULL(excl.Date_UTC_from))
              WHERE  CO2.SensorUnit_ID = {SensorUnit_ID} AND LRH_FLAG = 1
              AND LocationName = {LocationName}
              AND timestamp BETWEEN UNIX_TIMESTAMP({Date_UTC_from}) AND UNIX_TIMESTAMP({Date_UTC_to}) AND
              NOT (SHT21_T = -999 OR SHT21_RH = -999)
              "


hpp_query <- "
    SELECT
      timestamp,
      FROM_UNIXTIME(CO2.timestamp) AS Date,
      CO2_CAL_ADJ AS CO2,
      H2O,
      T,
      RH
      FROM CarboSense_HPP_CO2 AS CO2
    LEFT JOIN SensorExclusionPeriods AS excl
    ON CO2.SensorUnit_ID = excl.SensorUnit_ID
    AND (NOT (timestamp BETWEEN UNIX_TIMESTAMP(excl.Date_UTC_from) AND UNIX_TIMESTAMP(excl.Date_UTC_to)) OR ISNULL(excl.Date_UTC_from))
    WHERE  CO2.SensorUnit_ID = {SensorUnit_ID} AND Valve = 0
    AND LocationName = {LocationName}
    AND timestamp BETWEEN UNIX_TIMESTAMP({Date_UTC_from}) AND UNIX_TIMESTAMP({Date_UTC_to}) AND
    NOT (T = -999 OR RH = -999)
 "


get_exclusion_periods <-  function(con, SensorUnit_ID){
  exclusion_query <- 
    "
    SELECT
    *
    FROM SensorExclusionPeriods
    WHERE SensorUnit_ID = {SensorUnit_ID}
    "
  excl_tbl <- tbl(con, sql(glue::glue_sql(exclusion_query, .con=con))) %>%  collect()
}

filter_lp8_data <- function(x){
  
  mutate(x,
         delta_timestamp = c(0, diff(timestamp)),
         delta_SHT21_T_1 = c(0,diff(T)),
         delta_SHT21_T_2 = c(0,0,diff(T)[1:(nrow(x)-2)]),
         alien_meas = ((delta_SHT21_T_1 > 3 & delta_SHT21_T_2 < -3) | (delta_SHT21_T_1 < -3 & delta_SHT21_T_2 > 3)) 
         & delta_SHT21_T_1 < 700 & delta_SHT21_T_2 < 700
  ) %>%
    filter(delta_timestamp < 700 & 
             delta_timestamp >= 540 &
           !alien_meas) %>%
    mutate_if(is_numeric, function(x) na_if(x, carboutil::carbosense_na))
}


get_sensor_data <- function(con, .x.,.y, type='LP8', plot=T){
  str(type)
  prefix <- ifelse(type=='LP8','LP8','HPP')
  query <- ifelse(type=='LP8',lp8_query,hpp_query)
  column_fmt <- "%Y-%m-%dT%H:%M:%SZ"
  fmt <- "%Y%m%dT%H%M%SZ"
  from_str <- strftime(.y$Date_UTC_from, fmt)
  to_str <- strftime(.y$Date_UTC_to, fmt)
  fn <- paste(resultdir,prefix,'_',.y$LocationName,"_",.y$SensorUnit_ID,"_",from_str,"_",to_str,".csv",sep="")
  plot_n <-  paste(plotdir,prefix,'_',.y$LocationName,"_",.y$SensorUnit_ID,"_",from_str,"_",to_str,".pdf",sep="")
  #Get data
  query_interp <- sql(glue::glue_data_sql(.y,query, .con=con))
  print(query_interp)
  co2_data <- tbl(con, query_interp) %>%
    collect() %>%
    mutate(CO2 = na_if(CO2, carboutil::carbosense_na),
           Date = lubridate::as_datetime(Date))
  #Get exclusion periods
  excl <- get_exclusion_periods(con, .y$SensorUnit_ID)
  if(nrow(excl)>1){
    print(excl)
  }
  if(nrow(co2_data)>1)
  {
    co2_data %>%
    filter_lp8_data(.) %>%
    select(Date, 
           CO2,
           "H2O",
           "T",
           RH) %>%
    mutate_at(vars(c("CO2", "T", "RH", "H2O")), function(x) round(x,2)) %>%
    select(Date, CO2, H2O, "T", RH) %>%
      mutate(Date=strftime(Date, format=column_fmt)) %>%
  readr::write_delim(., 
                     fn, 
                     delim=';', 
                     append = F, 
                     col_names = T)
    CO2_avg <- openair::timeAverage(co2_data %>% rename("date"=Date), avg.time = "day")
    plt <- ggplot(CO2_avg, aes(x=date, y=CO2)) + geom_point()
    ggsave(plt, filename = plot_n)
  }
  data.frame(ndata= nrow(co2_data))

}




## Loop over all LP8 deployments
data <- deps %>% 
  mutate(type=ifelse(SensorUnit_ID<500,'HPP','LP8')) %>%
  filter(SensorUnit_ID > 1010) %>%
  group_by(SensorUnit_ID, LocationName,Date_UTC_from, Date_UTC_to, type) %>%
  group_walk(function(x,y) get_sensor_data(con, x,y,type=first(y$type)))


deps %>% 
  filter(SensorUnit_ID > 1010) %>%
  write_delim(delim=';', path = fn_Deployments)
  