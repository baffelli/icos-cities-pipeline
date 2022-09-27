# Compute_CarboSense_T_RH_values.r
# --------------------------------
#
# Author: Michael Mueller
#
#
# --------------------------------
#
# Remarks:
# - Computations refer to UTC.
#

## clear variables
rm(list=ls(all=TRUE))
gc()

## libraries
library(openair)
library(DBI)
require(RMySQL)
require(chron)
library(data.table)


## source

source("/project/muem/CarboSense/Software/api-v1.3.r")
source("/project/muem/CarboSense/Software/CarboSenseFunctions.r")

### ----------------------------------------------------------------------------------------------------------------------------

InsertDBTableName <- "CarboSense_T_RH"

### ----------------------------------------------------------------------------------------------------------------------------

## Decentlab DB information

DL_DB_domain      <- "swiss.co2.live"
DL_DB_apiKey      <- "eyJrIjoiSFd4bWJhczJjclpaUnpHeXluck1WYlJ0MkdINWhneFciLCJuIjoibWljaGFlbC5tdWVsbGVyQGVtcGEuY2giLCJpZCI6MX0="

DL_DB_domain_EMPA <- "empa-503.decentlab.com"
DL_DB_apiKey_EMPA <- "eyJrIjoiWkJaZjFDTEhUYm5sNmdWUG14a3NpdVcwTmZCaHloZVEiLCJuIjoibWljaGFlbC5tdWVsbGVyQGVtcGEuY2giLCJpZCI6MX0="

### ----------------------------------------------------------------------------------------------------------------------------

SensorUnit_ID_2_proc   <- c(1007,1008)
SensorUnit_ID_2_proc   <- c(1010:1334)

# 

SensorUnit_ID_2_proc   <- sort(unique(SensorUnit_ID_2_proc))
n_SensorUnit_ID_2_proc <- length(SensorUnit_ID_2_proc)


### ----------------------------------------------------------------------------------------------------------------------------

MEAS_MODE    <- "MEAN"

### ----------------------------------------------------------------------------------------------------------------------------

# Date of LP8 software upgrade (version 0 -> version 1 [10 minutes means and last single measurements])

date_UTC_LP8_SU_upgrade_01  <- strptime("20170628000000","%Y%m%d%H%M%S",tz="UTC")


timestamp_LP8_SU_upgrade_01 <- as.numeric(difftime(time1=date_UTC_LP8_SU_upgrade_01,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

### ----------------------------------------------------------------------------------------------------------------------------

# Database queries

query_str       <- paste("SELECT * FROM Deployment WHERE LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1');",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_deployment  <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_deployment$Date_UTC_from <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to   <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

#

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_location    <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

#

query_str       <- paste("SELECT * FROM SensorExclusionPeriods;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_SEP         <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_SEP$Date_UTC_from <- strptime(tbl_SEP$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_SEP$Date_UTC_to   <- strptime(tbl_SEP$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

### ----------------------------------------------------------------------------------------------------------------------------

## Loop over all LP8 sensor units

for(ith_SensorUnit_ID_2_proc in 1:n_SensorUnit_ID_2_proc){
  
  # if(SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]!=1033){
  #   next
  # }
  
  # Get the deployment periods of this SensorUnit
  
  id_depl     <- which(tbl_deployment$SensorUnit_ID==SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc] )
  n_id_depl   <- length(id_depl)
  
  # Get measurements from this SensorUnit for each deployment period 
  
  sensor_data <- NULL 
  
  if(n_id_depl>0){
    
    for(ith_depl in 1:n_id_depl){
      
      # Some periods may be excluded
      
      if(tbl_deployment$LocationName[id_depl[ith_depl]]=="RIG" & tbl_deployment$Date_UTC_from[id_depl[ith_depl]]<strptime("20170717000000","%Y%m%d%H%M%S",tz="UTC")){
        next
      }
      
      
      # Import measurement from Decentlab Influx-DB
      
      timestamp_depl_from <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_from[id_depl[ith_depl]],time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
      timestamp_depl_to   <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_to[id_depl[ith_depl]],  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
      
      timeFilter <- paste("time >= ",timestamp_depl_from,"s AND time < ",timestamp_depl_to,"s",sep="")
      device     <- paste("/",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],"/",sep="")
      
      if(SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]%in%c(1007,1008)){
        
        tmp0 <- tryCatch({
          query(domain=DL_DB_domain_EMPA,
                apiKey=DL_DB_apiKey_EMPA,
                timeFilter=timeFilter,
                device = device,
                location = "//",
                sensor = "//",
                channel = "//",
                aggFunc = "",
                aggInterval = "",
                doCast = FALSE,
                timezone = 'UTC')
        },
        error=function(cond){
          return(NULL)
        }
        )
        
      }else{
        
        tmp0 <- tryCatch({
          query(domain=DL_DB_domain,
                apiKey=DL_DB_apiKey,
                timeFilter=timeFilter,
                device = device,
                location = "//",
                sensor = "//",
                channel = "//",
                aggFunc = "",
                aggInterval = "",
                doCast = FALSE,
                timezone = 'UTC')
        },
        error=function(cond){
          return(NULL)
        }
        )
        
      }
      
      if(is.null(tmp0)){
        next
      }
      if(dim(tmp0)[1]==0){
        next
      }
      
      
      tmp0      <- as.data.table(tmp0)
      tmp0$time <- round(as.numeric(difftime(time1=tmp0$time,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))*1e3)
      tmp0      <- dcast.data.table(tmp0, time ~ series, fun.aggregate = mean,value.var = "value")
      tmp0$time <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp0$time/1e3
      tmp0      <- as.data.frame(tmp0)
      
      # Modification of colnames
      
      cn             <- colnames(tmp0)
      cn             <- gsub(pattern = "-",  replacement = "_", x = cn)
      cn             <- gsub(pattern = "\\.",replacement = "_", x = cn)
      cn             <- gsub(pattern = paste(SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],"_",sep=""), replacement = "", x = cn)
      cn             <- gsub(pattern = "senseair_lp8_temperature_last",   replacement = "lp8_T_l",    x=cn)
      cn             <- gsub(pattern = "senseair_lp8_temperature",        replacement = "lp8_T_m",    x=cn)   # LAST or MEAN depending on time
      cn             <- gsub(pattern = "sensirion_sht21_temperature_last",replacement = "sht21_T_l",  x=cn)
      cn             <- gsub(pattern = "sensirion_sht21_temperature",     replacement = "sht21_T_m",  x=cn)   # LAST or MEAN depending on time
      cn             <- gsub(pattern = "sensirion_sht21_humidity",        replacement = "sht21_RH",   x=cn)
      cn             <- gsub(pattern = "senseair_lp8_co2_filtered",       replacement = "lp8_CO2_f",  x=cn)
      cn             <- gsub(pattern = "senseair_lp8_co2",                replacement = "lp8_CO2",    x=cn)
      cn             <- gsub(pattern = "senseair_lp8_ir_filtered",        replacement = "lp8_ir_f",   x=cn)
      cn             <- gsub(pattern = "senseair_lp8_ir_last",            replacement = "lp8_ir_l",   x=cn)
      cn             <- gsub(pattern = "senseair_lp8_ir",                 replacement = "lp8_ir_m",   x=cn)   # LAST or MEAN depending on time
      cn             <- gsub(pattern = "senseair_lp8_status",             replacement = "lp8_status", x=cn)
      cn             <- gsub(pattern = "time",                            replacement = "date",       x=cn)
      colnames(tmp0) <- cn
      
      # Ensure that all required columns are defined
      
      cn_required   <- c("date","lp8_T_m","lp8_T_l","sht21_T_m","sht21_T_l","sht21_RH","lp8_status","LocationName")
      n_cn_required <- length(cn_required)
      
      tmp <- as.data.frame(matrix(NA,ncol=n_cn_required,nrow=dim(tmp0)[1]),stringsAsFactors=F)
      
      for(ith_cn_req in 1:n_cn_required){
        pos_tmp0 <- which(colnames(tmp0)==cn_required[ith_cn_req])
        if(length(pos_tmp0)>0){
          tmp[,ith_cn_req] <- tmp0[,pos_tmp0]
        }else{
          tmp[,ith_cn_req] <- NA
        }
      }
      
      colnames(tmp)    <- cn_required
      
      # Add location name
      
      tmp$LocationName <- tbl_deployment$LocationName[id_depl[ith_depl]]
      
      #
      
      rm(cn_required,n_cn_required,tmp0,pos_tmp0)
      gc()
      
      # Force time to full preceding minute
      
      tmp$date      <- as.POSIXct(tmp$date)
      tmp$date      <- strptime(strftime(tmp$date,"%Y%m%d%H%M00",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
      tmp$secs      <- as.numeric(difftime(time1=tmp$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
      
      # Check data imported from Influx-DB (no duplicates, delta timestamp>300s)
      
      tmp           <- tmp[!duplicated(tmp$secs),]
      tmp           <- tmp[order(tmp$secs),]
      tmp           <- tmp[c(T,abs(diff(tmp$secs))>300),]
      
      # merge sensor_data and tmp
      if(is.null(sensor_data)){
        sensor_data <- tmp
      }else{
        sensor_data <- rbind(sensor_data,tmp)
      }
    }
    
  }else{
    next
  }
  
  if(is.null(sensor_data)){
    next
  }
  
  # Check that data frame sensor_data is ordered correctly
  
  sensor_data <- sensor_data[order(sensor_data$date),] 
  
  # Select correct measuring mode
  
  # Note: 
  # _m can be last or mean measurement depending on time (before/after LP8_SU_upgrade_01)
  # _l is alway the last measurement but only availiable after LP8_SU_upgrade_01
  
  if(MEAS_MODE=="LAST"){
    
    sensor_data$lp8_T   <- sensor_data$lp8_T_l
    sensor_data$sht21_T <- sensor_data$sht21_T_l
    
    id_vers_01 <- which(sensor_data$date<=date_UTC_LP8_SU_upgrade_01)
    
    if(length(id_vers_01)>0){
      sensor_data$lp8_T[id_vers_01]     <- sensor_data$lp8_T_m[id_vers_01]
      sensor_data$sht21_T[id_vers_01]   <- sensor_data$sht21_T_m[id_vers_01]
      
      sensor_data$lp8_T_m[id_vers_01]   <- NA
      sensor_data$sht21_T_m[id_vers_01] <- NA
    }
    
    rm(id_vers_01)
  }
  
  if(MEAS_MODE=="MEAN"){
    
    sensor_data$lp8_T   <- sensor_data$lp8_T_m
    sensor_data$sht21_T <- sensor_data$sht21_T_m
    
    id_vers_01 <- which(sensor_data$date<=date_UTC_LP8_SU_upgrade_01)
    
    if(length(id_vers_01)>0){
      sensor_data$lp8_T_m[id_vers_01]   <- NA
      sensor_data$sht21_T_m[id_vers_01] <- NA
    }
    
    rm(id_vers_01)
  }
  
  
  # Check if time periods should be excluded (by means of LP8 sensor)
  
  id_SEP_SU <- which(tbl_SEP$SensorUnit_ID ==SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]
                     & tbl_SEP$Type == "LP8")
  
  n_id_SEP_SU <- length(id_SEP_SU)
  
  
  if(n_id_SEP_SU > 0){
    for(ith_id_SEP_SU in 1:n_id_SEP_SU){
      id_setToNA <- which(sensor_data$date>=tbl_SEP$Date_UTC_from[id_SEP_SU[ith_id_SEP_SU]] & sensor_data$date<=tbl_SEP$Date_UTC_to[id_SEP_SU[ith_id_SEP_SU]])
      if(length(id_setToNA)>0){
        sensor_data$sht21_T[id_setToNA]  <- -999
        sensor_data$sht21_RH[id_setToNA] <- -999
        sensor_data$lp8_T[id_setToNA]    <- -999
      }
    }
    rm(id_setToNA)
    gc()
  }
  
  rm(id_SEP_SU,n_id_SEP_SU)
  gc()
  
  # LP8 status
  
  id_setToNA <- which(sensor_data$lp8_status!=0)
  
  if(length(id_setToNA)>0){
    sensor_data$lp8_T[id_setToNA] <- -999
  }
  
  rm(id_setToNA)
  gc()
  
  
  # Adjust time stamp (End of 10 minute interval --> begin of 10 minute intervall)
  
  sensor_data$timestamp_DB <- sensor_data$secs - 600
  
  # Delete all the data from this SensorUnit from table "CarboSense_CO2" (or InsertDBTableName)
  
  query_str       <- paste("DELETE FROM ",InsertDBTableName," WHERE SensorUnit_ID = ",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],";",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  dbClearResult(res)
  dbDisconnect(con)
  
  # Insert data from this SensorUnit into table "CarboSense_CO2"  (or InsertDBTableName)
  
  id_setToNA <- which(is.na(sensor_data$sht21_T))
  
  if(length(id_setToNA)>0){
    sensor_data$sht21_T[id_setToNA] <- -999
  }
  
  id_setToNA <- which(is.na(sensor_data$sht21_RH))
  
  if(length(id_setToNA)>0){
    sensor_data$sht21_RH[id_setToNA] <- -999
  }
  
  id_setToNA <- which(is.na(sensor_data$lp8_T))
  
  if(length(id_setToNA)>0){
    sensor_data$lp8_T[id_setToNA] <- -999
  }
  
  #
  
  id_insert   <- which(sensor_data$sht21_T != -999 
                       | sensor_data$sht21_RH != -999 
                       | sensor_data$lp8_T != -999)
  
  n_id_insert <- length(id_insert)
  
  #
  
  if(n_id_insert>0){
    
    print(paste("Insert data for SU",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]))
    
    query_str <- paste("INSERT INTO ",InsertDBTableName," (`LocationName`, `SensorUnit_ID`, `timestamp`, `LP8_T`,`SHT21_T`,`SHT21_RH`) VALUES ",sep="")
    query_str <- paste(query_str,
                       paste("(",paste("'",sensor_data$LocationName[id_insert],"',",
                                       rep(SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],n_id_insert),",",
                                       sensor_data$timestamp_DB[id_insert],",",
                                       sensor_data$lp8_T[id_insert],",",
                                       sensor_data$sht21_T[id_insert],",",
                                       sensor_data$sht21_RH[id_insert],
                                       collapse = "),(",sep=""),");",sep="")
    )
    
    drv             <- dbDriver("MySQL")
    con             <- dbConnect(drv, group="CarboSense_MySQL")
    res             <- dbSendQuery(con, query_str)
    dbClearResult(res)
    dbDisconnect(con)
  }

}


