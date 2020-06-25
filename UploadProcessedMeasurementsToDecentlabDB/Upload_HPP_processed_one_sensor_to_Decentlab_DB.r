# Upload_HPP_processed_one_sensor_to_Decentlab_DB.r
# -------------------------------------------------
#
# Author: Michael Mueller
#
# -------------------------------------------------
#
#
# Uploads last XX days of measurements (every day) / all measurements (once per month) to Decentlab database.
#
#
#
#

## clear variables
rm(list=ls(all=TRUE))
gc()

## libraries
library(openair)
library(httr)
library(DBI)
require(RMySQL)
require(chron)
library(data.table)
library(devtools)

#

source("/project/muem/CarboSense/Software/api-v1.3_2019-09-03.r")


### ------------------------------------------------------------------------------------------------------------------------------

## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

# 

if(length(args)!=1){
  stop("Provide SensorUnit_ID as an argument!")
}

SensorUnit_ID <- as.integer(args[1]) 

if(SensorUnit_ID < 426 | SensorUnit_ID > 445){
  stop("Provided SensorUnit_ID not valid!")
}

### ----------------------------------------------------------------------------------------------------------------------------------------------

n_last_days              <- 21
completeUploadDayOfMonth <- 1

### ----------------------------------------------------------------------------------------------------------------------------------------------

## Decentlab DB information

DL_DB_domain  <- "swiss.co2.live"
DL_DB_apiKey  <- "eyJrIjoiSFd4bWJhczJjclpaUnpHeXluck1WYlJ0MkdINWhneFciLCJuIjoibWljaGFlbC5tdWVsbGVyQGVtcGEuY2giLCJpZCI6MX0="

### ----------------------------------------------------------------------------------------------------------------------------------------------

## Processing window: (now - 21d) to (now)

date_now            <- strptime(strftime(Sys.time(), "%Y-%m-%d %H:%M:%S",tz="UTC"), "%Y-%m-%d %H:%M:%S",tz="UTC")
timestamp_now       <- as.numeric(difftime(time1=date_now,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

COMPLETE_UPLOAD     <- as.numeric(strftime(date_now,"%d",tz="UTC")) == completeUploadDayOfMonth

date_interval_start <- date_now      - n_last_days*86400
timestamp_first     <- timestamp_now - n_last_days*86400

### ----------------------------------------------------------------------------------------------------------------------------------------------

## Deployments for SensorUnit_ID

if(!COMPLETE_UPLOAD){
  query_str       <- paste("SELECT * FROM Deployment WHERE LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1') ")
  query_str       <- paste(query_str, "AND SensorUnit_ID = ",SensorUnit_ID," ",sep="")
  query_str       <- paste(query_str, "AND Date_UTC_from < '",strftime(date_now,"%Y-%m-%d %H:%M:%S",tz="UTC"),"' ",sep="")
  query_str       <- paste(query_str, "AND Date_UTC_to   > '",strftime(date_interval_start,"%Y-%m-%d %H:%M:%S",tz="UTC"),"';",sep="")    
}else{
  query_str       <- paste("SELECT * FROM Deployment WHERE LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1') ")
  query_str       <- paste(query_str, "AND SensorUnit_ID = ",SensorUnit_ID," ",sep="")
  query_str       <- paste(query_str, "AND Date_UTC_from > '2017-07-01 00:00:00';",sep="")
}
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_deployment  <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_deployment$Date_UTC_from <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to   <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

tbl_deployment$timestamp_from <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_from,time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
tbl_deployment$timestamp_to   <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_to,  time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))

## Exclusion periods for SensorUnit_ID

if(!COMPLETE_UPLOAD){
  query_str       <- paste("SELECT * FROM SensorExclusionPeriods WHERE Type = 'HPP' and SensorUnit_ID = ",SensorUnit_ID," ",sep="")
  query_str       <- paste(query_str, "AND Date_UTC_from < '",strftime(date_now,"%Y-%m-%d %H:%M:%S",tz="UTC"),"' ",sep="")
  query_str       <- paste(query_str, "AND Date_UTC_to   > '",strftime(date_interval_start,"%Y-%m-%d %H:%M:%S",tz="UTC"),"';",sep="")   
}else{
  query_str       <- paste("SELECT * FROM SensorExclusionPeriods WHERE Type = 'HPP' and SensorUnit_ID = ",SensorUnit_ID," ",sep="")
  query_str       <- paste(query_str, "AND Date_UTC_from > '2018-08-01 00:00:00';",sep="")
}
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_SEP         <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

if(dim(tbl_SEP)[1]>=1){
  
  tbl_SEP$Date_UTC_from  <- strptime(tbl_SEP$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_SEP$Date_UTC_to    <- strptime(tbl_SEP$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
  tbl_SEP$timestamp_from <- as.numeric(difftime(time1=tbl_SEP$Date_UTC_from,time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
  tbl_SEP$timestamp_to   <- as.numeric(difftime(time1=tbl_SEP$Date_UTC_to,  time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
  
}

### ----------------------------------------------------------------------------------------------------------------------------------------------

## Loop over deployments

if(dim(tbl_deployment)[1]>0){
  for(ith_depl in 1:dim(tbl_deployment)[1]){
    
    ## Import measurements
    if(!COMPLETE_UPLOAD){
      query_str       <- paste("SELECT timestamp, CO2_CAL_ADJ FROM CarboSense_HPP_CO2 WHERE LocationName = '",tbl_deployment$LocationName[ith_depl],"' ",sep="")
      query_str       <- paste(query_str, "AND SensorUnit_ID = ",tbl_deployment$SensorUnit_ID[ith_depl]," ",sep="")
      query_str       <- paste(query_str, "AND Valve = 0 AND CO2_CAL_ADJ != -999 ",sep="")
      query_str       <- paste(query_str, "AND timestamp >= ",max(c(tbl_deployment$timestamp_from[ith_depl],timestamp_first))," ",sep="")
      query_str       <- paste(query_str, "AND timestamp <= ",timestamp_now,";",sep="")
    }else{
      query_str       <- paste("SELECT timestamp, CO2_CAL_ADJ FROM CarboSense_HPP_CO2 WHERE LocationName = '",tbl_deployment$LocationName[ith_depl],"' ",sep="")
      query_str       <- paste(query_str, "AND SensorUnit_ID = ",tbl_deployment$SensorUnit_ID[ith_depl]," ",sep="")
      query_str       <- paste(query_str, "AND Valve = 0 AND CO2_CAL_ADJ != -999 ",sep="")
      query_str       <- paste(query_str, "AND timestamp >= ",tbl_deployment$timestamp_from[ith_depl]," ",sep="")
      query_str       <- paste(query_str, "AND timestamp <= ",tbl_deployment$timestamp_to[ith_depl],";",sep="")
    }
    drv             <- dbDriver("MySQL")
    con             <- dbConnect(drv, group="CarboSense_MySQL")
    res             <- dbSendQuery(con, query_str)
    tbl_HPP_data    <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    
    ## Processing of exclusion periods
    
    ok <- rep(T,dim(tbl_HPP_data)[1])
    
    if(dim(tbl_SEP)[1]>0){
      
      for(ith_SEP in 1:dim(tbl_SEP)[1]){
        
        id_nok <- which(tbl_HPP_data$timestamp>=tbl_SEP$timestamp_from[ith_SEP] & tbl_HPP_data$timestamp<=tbl_SEP$timestamp_to[ith_SEP])
        
        if(length(id_nok)>0){
          ok[id_nok] <- F
        }
        
        rm(id_nok)
      }
    }
    
    if(all(ok==F)){
      next
    }
    
    if(any(ok==F)){
      tbl_HPP_data <- tbl_HPP_data[ok,]
    }
    
    rm(ok)
    gc()
    
    
    ## Date
    
    tbl_HPP_data$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_HPP_data$timestamp
    
    
    ## Average to 10 minutes
    
    upload_df    <- data.frame(date        = tbl_HPP_data$date,
                               CO2_CAL_ADJ = tbl_HPP_data$CO2_CAL_ADJ,
                               stringsAsFactors = F)
    
    upload_df    <- timeAverage(mydata = upload_df,
                                avg.time = "10 min",statistic = "mean",
                                start.date = strptime(strftime(min(tbl_HPP_data$date)-600,"%Y%m%d000000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
    
    
    ## Data filter
    
    id_upload    <- which(!is.na(upload_df$CO2_CAL_ADJ) & !is.nan(upload_df$CO2_CAL_ADJ) & upload_df$CO2_CAL_ADJ > 370 & upload_df$CO2_CAL_ADJ < 800)
    n_id_upload  <- length(id_upload) 
    
    
    ## Upload to swiss.co2.live
    
    if(n_id_upload>0){
      
      upload_df      <- upload_df[id_upload,]

      # library(devtools)
      # devtools::source_gist("79f52bef3778e0dbbd5dc58437621d88", filename = "api-v1.3.r")

      upload_df           <- data.frame(time   = upload_df$date,
                                        series = upload_df$CO2_CAL_ADJ,
                                        stringsAsFactors = F)
      
      colnames(upload_df) <- c("time","series")
      
      #
      
      attr(upload_df, "tags") <- list(series = list(node = paste(tbl_deployment$SensorUnit_ID[ith_depl],sep=""), sensor = tolower("CO2"), channel = tolower("HPP_LEVEL_00"), unit = "ppm"))
      
      #
      
      store(domain=DL_DB_domain, apiKey=DL_DB_apiKey, dataFrame=upload_df)
    }
  }
}