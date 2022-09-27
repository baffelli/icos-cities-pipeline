require(RMySQL)
library(dplyr)
library(readr)
library(carboutil)
library(tidyverse)


#Get database connection
con <- dbConnect(dbDriver("MySQL"),group='CarboSense_MySQL', host='du-gsn1')

data_sources <- "/project/CarboSense/Data/METEO/MCH_DAILY_DATA_DUMP"




#Date format for import
meteoswiss_date_format <- '%Y%m%d%H%M'

#Delay in delivery of meteoswiss data
meteoswiss_delivery_delay <- lubridate::days(x=2)

#Data names mapping between Meteoschweiz and CarboSense
name_mapping <- c('LocationName'='stn',
                  'Date'='Date',
                  'Radiance'="gre000z0",
                  "Windspeed"="fkl010z0",
                  "Winddirection"="dkl010z0",
                  "Pressure"="prestas0",
                  "Rain"="rre150z0",
                  "Temperature"="tre200s0",
                  "RH"="ure200s0",
                  "Sunshine"="sre000z0")


#Function to read CSV in meteoswiss format, with option to
#load it according to the old format definition
read_ms_csv <- function(path, old_format=T){
  #Delimiter and NA definition
  meteoswiss_delim <- ';'
  meteoswiss_na <- c('-')
  #Old format: we need to skip two lines, 
  #and construct a date by combining two columns
  if(old_format){
    cn_skip <- 2
    data_skip <- 2
    column_names <- names(readr::read_delim(path, meteoswiss_delim, skip=cn_skip, n_max = 1))
   
    complete_column_names <-  stringr::str_replace(column_names, "X3","time_of_day")
    temp_data <- readr::read_delim(path, meteoswiss_delim, skip=cn_skip+data_skip, col_names=complete_column_names, na = meteoswiss_na)
    data <- select(mutate(temp_data, 
           date=lubridate::ymd(time),
           Date = lubridate::make_datetime(
             year=lubridate::year(date), 
             day=lubridate::day(date),
             month=lubridate::month(date), 
             hour=lubridate::hour(time_of_day), 
             min=lubridate::minute(time_of_day))), -time, -time_of_day, -date)
  }
  else{
    #New format: station name was changed, date in one column only
    data <- mutate(
      rename(readr::read_delim(path, meteoswiss_delim, na=meteoswiss_na), 'stn'=`Station/Location`),
      Date = lubridate::parse_date_time(Date,meteoswiss_date_format))
  }
  #Apply new Column names
  select(data, !!!name_mapping)
 
}

#Function to set to NA the data in case it is in the list of eclusion periods
set_excl <- function(data, excl_data){
  
}

#Function to filter only stations that exist in the deployment
filter_stn <- function(data, stns, colName='StationName'){
  filter(data, !!colName %in% stns)
}


#Get all days already in  DB: we only want to load files that are not already in the database
ms_all_days_query <- 
"
SELECT DISTINCT
DATE(FROM_UNIXTIME(timestamp)) AS Date
FROM METEOSWISS_Measurements
"
ms_all_days <- carboutil::get_query_parametrized(con, ms_all_days_query) %>%
  mutate(Date=lubridate::as_date(Date))

#Decide wether to import all data or only the last two days
complete_import <- F
partial_import_time <- lubridate::days(x=2)
#This data set the change point where the data format was changed
format_change_date <- lubridate::date('2020-06-10')


#Get the exclusion times
excl_times <- carboutil::get_query_parametrized(con, "SELECT * FROM MCHMeasExclusionPeriods;")
#Get the list of locations
locs <- carboutil::get_query_parametrized(con, "SELECT DISTINCT LocationName FROM Location;")

  
#Find all datasets matching pattern
paths <- Sys.glob(stringr::str_interp("${data_sources}/VQEA33*.csv"))



#Load the datasets of interest
paths_df <- tibble::tibble(path = paths) %>%
  mutate(date = stringr::str_extract(path, "\\d{8}")) %>%
  #Interpret datres
  mutate(
    #Subtract two days as the deliveries are delayed
    date = lubridate::parse_date_time(date, "%Y%m%d%") - meteoswiss_delivery_delay,
    old_format = lubridate::date(date) <= format_change_date
  ) %>%
  #Only import the partial data from the partial period or days that are not in the database
  filter(complete_import |
           (
             date >= lubridate::today() - partial_import_time |
               !(lubridate::date(date) %in% ms_all_days$Date)
           )) %>%
  #Run the import function for each entry
  mutate(data = map2(path, old_format, function(x, y)
    read_ms_csv(x, old_format = y))) %>%
  #Now mutate the NA to the carbosense format
  mutate(data = map(data, function(x)
    dplyr::mutate_if(x, is.numeric, function(x)
      ifelse(is.na(x), carboutil::carbosense_na, x))),
    last_date = map(data, function(x) min(x$Date)))

#Get the exclusion periods
excl <- DBI::dbGetQuery(con, 'SELECT *  FROM MCHMeasExclusionPeriods;')
#For each entry in the exclusion period, mask the corresponding data
#map(excl, )


filter_excl <- function(data, excl){
  excl_rows <- ''
}



#Import NABEL data

#Mapping between nabel stations and LocationNames
nabel_location_mapping <-
  c(
    "NABRIG" = "nabelnrt_rig",
    "NABHAE" = "nabelnrt_hae",
    "NABZUE" = "nabelnrt_zue",
    "NABDUE" = "nabelnrt_due"
  )

#Get last entries for these NABEL stations

last_stat_query <-
  "
SELECT 
  LocationName,
  MAX(timestamp) AS TS_LAST_ENTRY,
 UNIX_TIMESTAMP() - MAX(timestamp)  AS how_long_ago
FROM METEOSWISS_Measurements WHERE LocationName IN ({stat*})
  GROUP BY LocationName
"
last_nabel_stat <- carboutil::get_query_parametrized(con, last_stat_query, stat=names(nabel_location_mapping)) %>%
  rowwise() %>%
  mutate(table_name = nabel_location_mapping[which(names(nabel_location_mapping)==LocationName)])



#Copy the data from the Nabel Station into the database 

#This query iterates over the table mapping, and copy only the data from each NABLEL
#station which is missing in the database
copy_nabel_query <-
  "
  SELECT
    {ln} AS LocationName,
    FROM_UNIXTIME(mt.timed/1e3) AS Date,
    COALESCE(mt.GLOBALRADIATION, -999) AS Radiance,
    COALESCE(mt.WINDSPEED, -999) AS Windspeed,
    COALESCE(mt.WINDDIRECTION, -999) AS Winddirection,
    COALESCE(mt.PRESSURE, -999) AS Pressure,
    COALESCE(mt.PRECIPITATION, -999) AS Rain,
    COALESCE(mt.TEMPERATURE, -999) AS Temperature,
    COALESCE(mt.RELATIVEHUMIDITY, -999) AS RH,
    -999 AS Sunshine
  FROM NabelGsn.{sn} AS mt
    LEFT JOIN CarboSense.METEOSWISS_Measurements AS mo
    	ON mo.LocationName = {ln}
    	#Subtract 600 to align to the CarboSense measurements
    	and (mt.timed / 1e3 - 600) = mo.timestamp
  WHERE timed >= ({le} + 1200)*1e3 AND mo.timestamp IS NULL
"

nabel_data <- purrr::pmap(
  list(last_nabel_stat$table_name, last_nabel_stat$LocationName, last_nabel_stat$TS_LAST_ENTRY),
  function(x,y,z) get_query_parametrized(con, copy_nabel_query, ln=y, sn=DBI::dbQuoteIdentifier(conn=con, x), le=z))


#Create a list of all data
all_data <- append(paths_df$data, nabel_data)

#Create a temporary table where to write the loaded meteo data
temp_q <-
  "
CREATE TABLE CarboSense.meteoswiss_temp(
  LocationName VARCHAR(8),
  Date DATETIME,
  Radiance DOUBLE,
  Windspeed DOUBLE,
  Winddirection DOUBLE,
  Pressure DOUBLE,
  Rain DOUBLE,
  Temperature DOUBLE,
  RH DOUBLE,
  Sunshine DOUBLE
);
"

#Copy the Meteoswiss data into the temporary table
DBI::dbExecute(conn = con, 'DROP TABLE IF EXISTS CarboSense.meteoswiss_temp')
DBI::dbGetQuery(conn = con, temp_q)
#For each entry in the dataframe to be written, write into temporary table of MeteoSwiss data
map(all_data,
    function(x)
      DBI::dbWriteTable(
        conn = con,
        name = 'meteoswiss_temp',
        value = as.data.frame(x),
        overwrite = 0,
        append = 1,
        temporary = FALSE,
        row.names = 0
      ))




#Once this is done, transfer the data using an antijoin with the existing data
copy_query <-
"
INSERT INTO CarboSense.METEOSWISS_Measurements 
  (LocationName, timestamp, Radiance, Windspeed, Winddirection, Pressure, Rain, Temperature, RH, Sunshine)
(
  SELECT  
  	mt.LocationName,
  	#Subtract 600 to align NABEL and Meteosuisse timestamp to CarboSense
  	UNIX_TIMESTAMP(mt.Date) - 600 AS timestamp,
  	mt.Radiance,
  	mt.Windspeed,
  	mt.Winddirection,
  	mt.Pressure,
  	mt.Rain,
  	mt.Temperature,
  	mt.RH,
  	mt.Sunshine
  FROM CarboSense.meteoswiss_temp AS mt
  	LEFT JOIN CarboSense.METEOSWISS_Measurements AS mo
  	ON mo.LocationName = mt.LocationName
  	#align to the extisting data
  	and UNIX_TIMESTAMP(mt.Date) - 600 = mo.timestamp
  WHERE mt.LocationName  IN
  	(
  		SELECT DISTINCT
  			LocationName
  		FROM CarboSense.Location AS lo
  	)
  AND mo.timestamp IS NOT NULL
)
"



