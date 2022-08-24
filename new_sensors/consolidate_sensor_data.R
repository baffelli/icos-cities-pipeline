library(carboutil)
library(DBI)
library(dplyr)
library(zoo)
library(tidyverse)
#Load data from DB
q <- "
WITH ns AS
(
SELECT
  rs.timestamp AS date,
  rs.SensorUnit_ID,
  rs.sensor_temperature AS temperature,
  rs.air_pressure AS pressure,
  rs.air_moisture AS rh,
  rs.CO2 AS CO2,
  rs.H2O AS H2O,
  rs.Valve AS valve
FROM raw_sensor_data AS rs
LEFT JOIN Deployment AS dep
	ON dep.SensorUnit_ID = rs.SensorUnit_ID AND rs.timestamp BETWEEN dep.Date_UTC_from and dep.Date_UTC_to
LEFT JOIN RefGasCylinder_Deployment AS dep_gas
	ON dep.LocationName = dep_gas.LocationName AND dep.SensorUnit_ID = dep_gas.SensorUnit_ID AND dep_gas.Date_UTC_from >= dep.Date_UTC_from AND dep_gas.Date_UTC_to <= dep.Date_UTC_to
LEFT JOIN RefGasCylinder AS ref_cylinder
  ON ref_cylinder.CylinderID = dep_gas.CylinderID AND ref_cylinder.Date_UTC_from <= dep_gas.Date_UTC_from
  WHERE rs.CO2 IS NOT NULL AND rs.timestamp > {start_date}
),
pic AS
(
SELECT
  FROM_UNIXTIME(timestamp) AS date,
  1 AS SensorUnit_ID,
  T AS temperature,
  pressure AS pressure,
  RH AS rh,
  (CASE WHEN CO2_DRY_CAL <> -999 OR CO2_F = 1 THEN CO2_DRY_CAL ELSE NULL END) AS CO2,
  H2O AS H2O,
  0 AS valve
FROM NABEL_DUE 
WHERE FROM_UNIXTIME(timestamp) > {start_date}
),
HPP AS
(
SELECT
  FROM_UNIXTIME(timestamp) AS date,
  SensorUnit_ID AS SensorUnit_ID,
  T AS temperature,
  pressure AS pressure,
  RH AS rh,
  CO2_CAL_DRY AS CO2,
  NULL AS H2O,
  Valve AS valve
FROM CarboSense_HPP_CO2 
WHERE FROM_UNIXTIME(timestamp) > {start_date} AND LocationName = 'DUE1' AND SensorUnit_ID = 442 AND Valve = 0
)

SELECT
*
FROM ns
UNION ALL
SELECT
*
FROM pic
UNION ALL
SELECT
*
FROM HPP
"
con <- carboutil::get_conn(user="basi")
start_date <- lubridate::as_datetime('2021-08-26 17:00:00')
q_interp <- glue::glue_sql(q, .con = con)
meas <- lubridate::with_tz(collect(tbl(con, sql(q_interp)),"UTC"))

#Read calibration parameters
cal_path <- "G:/503_Themen/CarboSense/Kalibrationen\ Picarros.xlsx"
cal_path <- "/project/CarboSense/Win_G/503_Themen/CarboSense/Kalibrationen\ Picarros.xlsx"



cal_table_raw <- readxl::read_excel(cal_path, range=cell_limits(c(7,1),c(NA,6)), col_names = FALSE) %>% 
  set_names(c("date","location","cylinder_id","compound", "cylinder_concentration","measured_concentration"))


#Start of series
start_date <- lubridate::as_date('2021-08-25 16:00:00')

#Load K96 data


k96_files <- Sys.glob("/project/CarboSense/temp/K96_data/*.csv")

valid <- map(k96_files, function(x) lubridate::as_date(stringr::str_extract(x, "\\d{8}"))) >= start_date

k96_data <- bind_rows(purrr::map(k96_files[valid], readr::read_csv))


# Function for aggregation
sum_fun <- list(mean = function(x) mean(x, na.rm=TRUE), sd = function(x) sd(x, na.rm=TRUE))



#Average K96 to the minute

k96_minute <- k96_data %>%
  rename(SensorUnit_ID = sensor_id) %>%
  mutate(date = lubridate::as_datetime(date)) %>%
  filter(date > start_date) %>%
  group_by(SensorUnit_ID, date = lubridate::floor_date(date ,"10 minutes")) %>%
  complete(SensorUnit_ID, date = seq(min(date), max(date), "10 mins")) %>%
  #Water compensation
  mutate(
    CO2_WET = CO2,
    CO2 = CO2 * (1 - 1e-6 *  H2O)) %>%
  readr::write_csv(file="/project/CarboSense/Win_CS/Data/K96_second.csv") %>%
  summarise_all(sum_fun)


dt_minute <-
  meas %>%
  filter(date > start_date) %>%
  group_by(SensorUnit_ID, date = lubridate::floor_date(date ,"10 minutes")) %>%
  complete(SensorUnit_ID, date = seq(min(date), max(date), "10 mins")) %>%
  summarise_all(sum_fun) 


sensor_data <- 
  bind_rows(
    k96_minute,
    dt_minute %>% filter(SensorUnit_ID != 1) %>% mutate(SensorUnit_ID = as.character(SensorUnit_ID))
    )  %>%
  inner_join(dt_minute %>% ungroup() %>% filter(SensorUnit_ID == 1) %>% select(date, CO2_pic =CO2_mean, H2O_pic = H2O_mean) , by="date") %>%
  filter(date > lubridate::as_datetime('2021-08-25 17:00:00')) %>%
  mutate(sensor = case_when(SensorUnit_ID == "4056" ~ "Vaisala GMP",
                            SensorUnit_ID == "4057" ~ "Licor LI 850",
                            str_detect(SensorUnit_ID , "4\\d{2}") ~ "SenseAir HPP",
                            str_detect(SensorUnit_ID , "0100.*") ~ "SenseAir K96")) 






#Store it in K: (to make sharing it easier)
readr::write_csv(sensor_data, "/project/CarboSense/Win_CS/Data/sensor_data.csv")


