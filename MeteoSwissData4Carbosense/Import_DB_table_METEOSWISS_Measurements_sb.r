require(RMySQL)
library(dplyr)
library(readr)
library(carboutil)
library(tidyverse)
library(lubridate)
ca <- commandArgs()
complete_import <- ifelse(is.na(ca[[1]]),ca[[1]],F)
#Get database connection
conn<-carboutil::get_conn(group="CarboSense_MySQL")


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
           time_of_day = lubridate::hm(time_of_day),
           Date = lubridate::make_datetime(
             year=lubridate::year(date), 
             day=lubridate::day(date),
             month=lubridate::month(date), 
             hour=lubridate::hour(time_of_day), 
             min=lubridate::minute(time_of_day))), -time, -time_of_day, -date)
  }
  else{
    ct <- 
      cols(
        'Station/Location'=col_character(),
        'Date'=col_character(),
        "gre000z0"=col_number(),
        "fkl010z0"=col_number(),
        "dkl010z0"=col_number(),
        "prestas0"=col_number(),
        "rre150z0"=col_number(),
        "tre200s0"=col_number(),
        "ure200s0"=col_number(),
        "sre000z0"=col_number()
      )
    #New format: station name was changed, date in one column only
    data <- mutate(
      rename(readr::read_delim(path, meteoswiss_delim, na=meteoswiss_na, col_types = ct), 'stn'=`Station/Location`),
      Date = lubridate::parse_date_time(Date,meteoswiss_date_format))
  }
  #Apply new Column names
  select(data, !!!name_mapping)
}


load_and_copy_meteoswiss <- function(path, format, dst_table, conn){
  str(path)
  str(format)
  #Load meteoswiss data and copy  it to the DB
  data <- 
    mutate_if(
    read_ms_csv(path, old_format = format),
    function(x) is.numeric(x) & !lubridate::is.Date(x),
    function(x) ifelse(is.na(x), carboutil::carbosense_na, x)) %>%
    mutate(
      #Align dates
      Date=Date  - lubridate::seconds(600),
      timestamp=as.numeric(Date))
  str(data)
  carboutil::write_chuncks(
    conn,
    as.data.frame(data),
    dst_table)
  # carboutil::write_chuncks(
  #   conn = conn,
  #   name = dst_table,
  #   value = as.data.frame(data),
  #   overwrite = FALSE,
  #   append = TRUE,
  #   temporary = FALSE,
  #   row.names = FALSE
  # )
}


create_temp_table <- function(conn, dst_table='meteoswiss_temp'){
  #Create a temporary table where to write the loaded meteo data
  temp_q <-
    "
  CREATE TABLE CarboSense.{nm}(
    LocationName VARCHAR(6),
    timestamp INT(10),
    Date DATETIME,
    Radiance DOUBLE,
    Windspeed DOUBLE,
    Winddirection DOUBLE,
    Pressure DOUBLE,
    Rain DOUBLE,
    Temperature DOUBLE,
    RH DOUBLE,
    Sunshine DOUBLE,
    CONSTRAINT PK_ms_temp PRIMARY KEY (LocationName, timestamp)
  )
  CHARACTER SET latin1 COLLATE latin1_swedish_ci;
"
  nm <- DBI::dbQuoteIdentifier(conn, dst_table)
  #Copy the Meteoswiss data into the temporary table
  DBI::dbExecute(conn = conn, glue::glue_sql('DROP TABLE IF EXISTS CarboSense.{nm}'))
  DBI::dbExecute(conn = conn, glue::glue_sql(temp_q))
}









#Get all days already in  DB: we only want to load files that are not already in the database
ms_all_days_query <- 
"
SELECT DISTINCT
DATE(FROM_UNIXTIME(timestamp)) AS Date
FROM METEOSWISS_Measurements
"
ms_all_days <- carboutil::get_query_parametrized(conn, ms_all_days_query) %>%
  mutate(Date=lubridate::as_date(Date))

#One data delivery covers two days
ms_time_coverage <- lubridate::days(x=2)
#This data set the change point where the data format was changed
format_change_date <- lubridate::date('2020-06-10')


#Get the exclusion times
excl_times <- carboutil::get_query_parametrized(conn, "SELECT * FROM MCHMeasExclusionPeriods;")

 
#Find all datasets matching pattern of the CSV delivery
paths <- Sys.glob(stringr::str_interp("${data_sources}/VQEA33*.csv"))



#Create a list of datasets delivered by meteoswiss
paths_df <- tibble::tibble(path = paths[seq(1,length(paths),2)]) %>%
  mutate(date = stringr::str_extract(path, "\\d{12}")) %>%
  #Interpret dates as PosixCT
  mutate(
    #Subtract two days as the deliveries are delayed 
    #(The filename contains data which corresponds to two days ago)
    delivery_date = lubridate::parse_date_time(date, "%Y%m%d%%h%m"),
    end_period = delivery_date,
    start_period = delivery_date - ms_time_coverage ,
    old_format = (delivery_date <= format_change_date),
    timespan = lubridate::interval(start_period, end = end_period)
  ) %>%
  filter(!(lubridate::date(delivery_date) %in% ms_all_days$Date)) 
  

str(paths_df)
#Only import the partial data from the partial period or days that are not in the CarboSense database
#





#Create the temporary table
create_temp_table(conn)
str(paths_df)
#Load the files and copy them into the DB
map2(paths_df$path, paths_df$old_format, function(x,y) load_and_copy_meteoswiss(x, y, 'meteoswiss_temp', conn))



## Import NABEL data ##




#Copy the data from the Nabel Station into the database 

get_last_nabel <- function(con){
  #This query iterates over the table mapping, and copy only the data from each NABLEL
  #station which is missing in the database
  #Mapping between nabel stations and LocationNames
  nabel_location_mapping <-
    c(
      "NABRIG" = "nabelnrt_rig",
      "NABHAE" = "nabelnrt_hae",
      "NABZUE" = "nabelnrt_zue",
      "NABDUE" = "nabelnrt_due",
      "NABLUG" = "nabelnrt_lug"
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
  last_nabel_stat <-
    carboutil::get_query_parametrized(con, last_stat_query, stat = names(nabel_location_mapping)) %>%
    rowwise() %>%
    mutate(table_name = nabel_location_mapping[which(names(nabel_location_mapping) ==
                                                       LocationName)])
  
}

copy_nabel_data <- function(in_con, out_con, tmp_table = 'meteoswiss_temp') {
  #Get information of the last nabel data

  last_nabel_stat <- get_last_nabel(out_con)
  print(last_nabel_stat)
  copy_nabel_query <-
  "
    SELECT
      {ln} AS LocationName,
      mt.timed/1e3 AS timestamp,
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
    WHERE timed > ({le} + 1200)*1e3 
"
  #Iterate over the entries
  #of the nabel measurements 
  #and copy data from one table to the other
  nabel_data <- purrr::pmap(list(
    last_nabel_stat$table_name,
    last_nabel_stat$LocationName,
    last_nabel_stat$TS_LAST_ENTRY
  ),
  function(x, y, z)
  {
    #Copy the data locally
    temp_data <- get_query_parametrized(
      in_con,
      copy_nabel_query,
      ln = y,
      sn = DBI::dbQuoteIdentifier(conn = in_con, x),
      le = z
    )
    #Write it
    carboutil::write_chuncks(
    out_con,
    as.data.frame(temp_data),
    tmp_table) 

  
  }
  )
}


nabel_con <- carboutil::get_conn(group='nabelGSN')
cs_con <- carboutil::get_conn(group='CarboSense_MySQL')
#Exceute the function to copy the data
copy_nabel_data(nabel_con, cs_con)



#Once this is done, transfer the data to the final table using an antijoin with the existing data
copy_query <-
"
INSERT INTO CarboSense.METEOSWISS_Measurements 
  (LocationName, timestamp, Radiance, Windspeed, Winddirection, Pressure, Rain, Temperature, RH, Sunshine)
(
  SELECT  
  	mt.LocationName,
    mt.timestamp,
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
  	and mt.timestamp = mo.timestamp
  WHERE mt.LocationName  IN
  	(
  		SELECT DISTINCT
  			LocationName
  		FROM CarboSense.Location AS lo
  	)
  AND mo.timestamp IS NULL
)
"
DBI::dbExecute(cs_con, copy_query)


