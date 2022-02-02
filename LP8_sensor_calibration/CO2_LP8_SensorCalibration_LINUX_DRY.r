# CO2_LP8_SensorCalibration_DRY.r
# --------------------------------
#
# Author: Michael Mueller
#
#
# --------------------------------

# Remarks:
# - Computations refer to UTC.
#
#

## clear variables
rm(list=ls(all=TRUE))
gc()

## libraries
library(openair)
library(DBI)
require(RMySQL)
require(chron)
library(MASS)
library(data.table)

## source

source("/project/CarboSense/Software/CarboSenseUtilities/api-v1.3.r")
source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

### ----------------------------------------------------------------------------------------------------------------------------

## NOTES

# Cross-validation (CV) modes

# CV_mode : 0 ->  no CV

# Measurement modes:

# "LAST": last one minute measurement in 10 minute intervall
# "MEAN": measurement mean of 10 minutes intervall

### ----------------------------------------------------------------------------------------------------------------------------

## ARGS

args      <- commandArgs(trailingOnly=TRUE)
resultdir <- args[1]
CV_mode   <- args[2]
MEAS_MODE <- args[3]


if(!MEAS_MODE%in%c("LAST","MEAN")){
  stop("MEAS_MODE not correctly specified! Must be 'LAST' or 'MEAN'.")
}
if(!CV_mode%in%c(0:5)){
  stop("CV_mode not correctly specified! Valid: [0].")
}
if(!dir.exists(resultdir)){
  dir.create(resultdir)
  if(!dir.exists(resultdir)){
    stop("'resultdir' not correctly specified!")
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

## Decentlab DB information

DL_DB_domain      <- "swiss.co2.live"
DL_DB_apiKey      <- "eyJrIjoiSFd4bWJhczJjclpaUnpHeXluck1WYlJ0MkdINWhneFciLCJuIjoibWljaGFlbC5tdWVsbGVyQGVtcGEuY2giLCJpZCI6MX0="

DL_DB_domain_EMPA <- "empa-503.decentlab.com"
DL_DB_apiKey_EMPA <- "eyJrIjoiWkJaZjFDTEhUYm5sNmdWUG14a3NpdVcwTmZCaHloZVEiLCJuIjoibWljaGFlbC5tdWVsbGVyQGVtcGEuY2giLCJpZCI6MX0="

### ----------------------------------------------------------------------------------------------------------------------------

# Selection of sensorUnits to be calibrated
SensorUnit_ID_2_cal   <- 1334:1010

if(T){
  ## Instead dplyr::collect(dplyr::tbl(con, dplyr::sql(your query here)))
  query_str  <- "SELECT SensorUnit_ID FROM Calibration WHERE LocationName='DUE1' AND Date_UTC_to='2100-01-01 00:00:00' AND SensorUnit_ID>1008"
  con <- carboutil::get_conn(group="CarboSense_MySQL")
  # res        <- dbSendQuery(con, query_str)
  # tmp        <- dbFetch(res, n=-1)
  # dbClearResult(res)
  tmp <- dplyr::collect(dplyr::tbl(con, dplyr::sql(query_str)))
  #Do not forget to disconnect
  dbDisconnect(con)
  #Extract results
  SensorUnit_ID_2_cal <- as.numeric(tmp$SensorUnit_ID)
}


if(F){
  
  query_str  <- paste("SELECT SensorUnit_ID FROM Calibration where LocationName='DUE2' and Date_UTC_to = '2019-05-29 07:10:00' and SensorUnit_ID>=1010;",sep="")
  drv        <- dbDriver("MySQL")
  con<-carboutil::get_conn(group="CarboSense_MySQL")
  res        <- dbSendQuery(con, query_str)
  tmp        <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  SensorUnit_ID_2_cal <- as.numeric(tmp$SensorUnit_ID)
}

#Instead: change the query with "SELECT distinct SensorUnit_ID FROM Calibration 
#WHERE LocationName='DUE1' AND Date_UTC_to='2100-01-01 00:00:00' AND SensorUnit_ID>1008
#ORDER BY SensorUnit_ID"
SensorUnit_ID_2_cal   <- sort(unique(SensorUnit_ID_2_cal))
n_SensorUnit_ID_2_cal <- length(SensorUnit_ID_2_cal)

### ----------------------------------------------------------------------------------------------------------------------------

# Parameters for data selection
max_RH_4_cal <-  95

# Set variables to NULL
statistics           <- NULL
SHT_CORRECTIONS      <- NULL
statistics_H2O       <- NULL
CalibrationModelInfo <- NULL

### ----------------------------------------------------------------------------------------------------------------------------

# Create directories

if(!dir.exists(resultdir)){
  dir.create((gsub(pattern = "/$",replacement = "",resultdir)))
}

plotdir_allModels <- paste(resultdir,"AllModels/",sep="")
if(!dir.exists(plotdir_allModels)){
  dir.create((plotdir_allModels))
}

### ----------------------------------------------------------------------------------------------------------------------------

# Date when LP8 sensor unit firmware was upgraded 
#
# Upgrade
# - Performed on 28 June 2018
# - Change of LP8/SHT21 measurement frequency and averaging: 
# - before: Only last measurements transmitted
# - after: Mean + last measurements transmitted
#

date_UTC_LP8_SU_upgrade_01  <- strptime("20170628000000","%Y%m%d%H%M%S",tz="UTC")
timestamp_LP8_SU_upgrade_01 <- as.numeric(difftime(time1=date_UTC_LP8_SU_upgrade_01,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

#Very complex solution to compute timestamp.
#Modern solution 
date_UTC_LP8_SU_upgrade_01 <- lubridate::as_datetime("2017-06-28 00:00:00 UTC")
timestamp_LP8_SU_upgrade_01 <- as.numeric(timestamp_LP8_SU_upgrade_01)
### ----------------------------------------------------------------------------------------------------------------------------


#TODO make a nicer list

# sensor model versions

sensor_models   <- list()

## IR RAW


## Only Beer-Lambert without compensation terms

if(F){
  sensor_models[[length(sensor_models)+1]] <- list(name      ="CO2_MODEL_CONSTIR",
                                                   modelType ="LM_IR",
                                                   formula   ="CO2 ~ lp8_logIR_orig",
                                                   formula_IR=NA)
}

## Beer-Lambert with compensation terms for temperature f(T)

if(F){
  sensor_models[[length(sensor_models)+1]] <- list(name      ="CO2_MODEL_pLIN_c0_CONSTIR",
                                                   modelType ="LM_IR",
                                                   formula   ="CO2 ~ lp8_logIR_orig +I(lp8_T+273.15) + I((lp8_T+273.15)^2) + I((lp8_T+273.15)^3)",
                                                   formula_IR=NA)
  
}

## Beer-Lambert with compensation terms for temperature f(T), f(T,1/IR)

#TODO: define a new t_abs variable instead of using 273.15 (Very dangerous)

if(T){
  
  sensor_models[[length(sensor_models)+1]] <- list(name      ="CO2_MODEL_pLIN_c0_TIR_CONSTIR",
                                                   modelType ="LM_IR",
                                                   formula   ="CO2 ~ lp8_logIR_orig + I(lp8_T+273.15) + I((lp8_T+273.15)^2) + I((lp8_T+273.15)^3) + I((lp8_T+273.15)/lp8_ir_orig) + I((lp8_T+273.15)^2/lp8_ir_orig) + I((lp8_T+273.15)^3/lp8_ir_orig)",
                                                   formula_IR=NA)
}

## Beer-Lambert with compensation terms for temperature f(T), pressure f(p)

if(T){
  sensor_models[[length(sensor_models)+1]] <- list(name      ="CO2_MODEL_pLIN_c0_P_CONSTIR",
                                                   modelType ="LM_IR",
                                                   formula   ="CO2 ~ lp8_logIR_orig +I(lp8_T+273.15) + I((lp8_T+273.15)^2) + I((lp8_T+273.15)^3) + I((((pressure-1013.25)/1013.25)))",
                                                   formula_IR=NA)
  
}

## Beer-Lambert with compensation terms for temperature f(T),f(1/IR), pressure f(p)

if(T){
  
  sensor_models[[length(sensor_models)+1]] <- list(name      ="CO2_MODEL_pLIN_c0_TIR_P_CONSTIR",
                                                   modelType ="LM_IR",
                                                   formula   ="CO2 ~ lp8_logIR_orig + I(lp8_T+273.15) + I((lp8_T+273.15)^2) + I((lp8_T+273.15)^3) + I((lp8_T+273.15)/lp8_ir_orig) + I((lp8_T+273.15)^2/lp8_ir_orig) + I((lp8_T+273.15)^3/lp8_ir_orig) + I((((pressure-1013.25)/1013.25)))",
                                                   formula_IR=NA)
}

## Beer-Lambert with compensation terms for temperature f(T),f(1/IR), pressure f(p),f(p,1/IR)

if(T){
  sensor_models[[length(sensor_models)+1]] <- list(name      ="CO2_MODEL_pLIN_c0_P_PIR_CONSTIR",
                                                   modelType ="LM_IR",
                                                   formula   ="CO2 ~ lp8_logIR_orig + I(lp8_T+273.15) + I((lp8_T+273.15)^2) + I((lp8_T+273.15)^3) + I((lp8_T+273.15)/lp8_ir_orig) + I((lp8_T+273.15)^2/lp8_ir_orig) + I((lp8_T+273.15)^3/lp8_ir_orig) + I((((pressure-1013.25)/1013.25)))+ I((((pressure-1013.25)/1013.25))/lp8_ir_orig)",
                                                   formula_IR=NA)
}


### ----------------------------------------------------------------------------------------------------------------------------

## --- Loop over all SensorUnits

for(ith_SensorUnit_ID_2_cal in 1:n_SensorUnit_ID_2_cal){
  
  
  # calibration information (only CalMode "1","2","3")
  
  #TODO use parametrised queries (see here https://db.rstudio.com/best-practices/run-queries-safely/)

  query_str       <- paste("SELECT * FROM Calibration where SensorUnit_ID=",SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal]," and CalMode IN (1,2,3);",sep="")
  query_str <- glue::glue_sql("SELECT * FROM Calibration where SensorUnit_ID = {SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal]} and CalMode IN (1,2,3)")

  
  
  drv             <- dbDriver("MySQL")
  con <-carboutil::get_conn( group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tbl_calibration <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  # TODO do this in the database
  tbl_calibration$Date_UTC_from <- strptime(tbl_calibration$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_calibration$Date_UTC_to   <- strptime(tbl_calibration$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  # Replace 2100-01-01 with current data (USE LEAST(Date_UTC_To, CURDATE()) directly in query instead)
  id <- which(tbl_calibration$Date_UTC_to == strptime("2100-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"))
  
  if(length(id)>0){
    tbl_calibration$Date_UTC_to[id] <- strptime(strftime(Sys.time(),"%Y-%m-%d %H:%M:%S",tz="UTC"),"%Y-%m-%d %H:%M:%S",tz="UTC")
  }
  
  rm(id)
  gc()
  
  # -----------
  
  #Probably can be deleted
  ## Case "2017-12-12": Limitation of calibration period to 2017-12-12
  
  if(F){
    
    id <- which(tbl_calibration$Date_UTC_from < strptime("201712120000000","%Y%m%d%H%M%S",tz="UTC"))
    
    if(length(id)>0){
      tbl_calibration <- tbl_calibration[id,]
    }
    
    id <- which(tbl_calibration$Date_UTC_to > strptime("201712120000000","%Y%m%d%H%M%S",tz="UTC"))
    
    if(length(id)>0){
      tbl_calibration$Date_UTC_to[id] <- strptime("201712120000000","%Y%m%d%H%M%S",tz="UTC")
    }
    
    rm(id)
    gc()
  }
  
  # -----------
  
  ## Case "2017-12-01": Limitation of calibration period to 2017-12-01
  
  if(F){
    
    id <- which(tbl_calibration$Date_UTC_from < strptime("201712010000000","%Y%m%d%H%M%S",tz="UTC"))
    
    if(length(id)>0){
      tbl_calibration <- tbl_calibration[id,]
    }
    
    id <- which(tbl_calibration$Date_UTC_to > strptime("201712010000000","%Y%m%d%H%M%S",tz="UTC"))
    
    if(length(id)>0){
      tbl_calibration$Date_UTC_to[id] <- strptime("201712010000000","%Y%m%d%H%M%S",tz="UTC")
    }
    
    rm(id)
    gc()
  }
  
  # ------------------------------------------------------------------
  
  # Get information about the LP8 sensors of this particular sensor unit
  
  query_str   <- paste("SELECT * FROM Sensors where SensorUnit_ID=",SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal]," and Type='LP8';",sep="")
  drv         <- dbDriver("MySQL")
  con <-carboutil::get_conn( group="CarboSense_MySQL")
  res         <- dbSendQuery(con, query_str)
  tbl_sensors <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  tbl_sensors$Date_UTC_from <- strptime(tbl_sensors$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_sensors$Date_UTC_to   <- strptime(tbl_sensors$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
  
  # ------------------------------------------------------------------
  
  # Build data frame "sensor_calibration_info"
  
  sensor_calibration_info <- NULL
  
  for(ith_sensor in 1:dim(tbl_sensors)[1]){
    
    for(ith_cal in 1:dim(tbl_calibration)[1]){
      
      if(tbl_calibration$Date_UTC_to[ith_cal]   < tbl_sensors$Date_UTC_from[ith_sensor]){
        next
      }
      if(tbl_calibration$Date_UTC_from[ith_cal] > tbl_sensors$Date_UTC_to[ith_sensor]){
        next
      }
      
      if(tbl_calibration$Date_UTC_from[ith_cal] <= tbl_sensors$Date_UTC_from[ith_sensor]){
        date_UTC_from <- tbl_sensors$Date_UTC_from[ith_sensor]
      }else{
        date_UTC_from <- tbl_calibration$Date_UTC_from[ith_cal]
      }
      
      if(tbl_calibration$Date_UTC_to[ith_cal] <= tbl_sensors$Date_UTC_to[ith_sensor]){
        date_UTC_to <- tbl_calibration$Date_UTC_to[ith_cal]
      }else{
        date_UTC_to <- tbl_sensors$Date_UTC_to[ith_sensor]
      }
      
      
      
      sensor_calibration_info <- rbind(sensor_calibration_info, data.frame(SensorUnit_ID      = SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],
                                                                           Type               = tbl_sensors$Type[ith_sensor],
                                                                           Serialnumber       = tbl_sensors$Serialnumber[ith_sensor],
                                                                           Date_UTC_from      = date_UTC_from,
                                                                           Date_UTC_to        = date_UTC_to,
                                                                           LocationName       = tbl_calibration$LocationName[ith_cal],
                                                                           DBTableNameRefData = tbl_calibration$DBTableNameRefData[ith_cal],
                                                                           CalMode            = tbl_calibration$CalMode[ith_cal],
                                                                           stringsAsFactors=F))
      
      rm(date_UTC_from,date_UTC_to)
      gc()
    }
  }
  
  #
  
  sensor_calibration_info$timestamp_from <- as.numeric(difftime(time1=sensor_calibration_info$Date_UTC_from,
                                                                time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                                                                units="secs",
                                                                tz="UTC"))
  
  sensor_calibration_info$timestamp_to   <- as.numeric(difftime(time1=sensor_calibration_info$Date_UTC_to,
                                                                time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                                                                units="secs",
                                                                tz="UTC"))
  

  # ------------------------------------------------------------------
  
  
  ## --- Loop over each individual sensor of particular sensor unit
  
  sensors2cal   <- sort(unique(sensor_calibration_info$Serialnumber))
  n_sensors2cal <- length(sensors2cal)
  
  for(ith_sensor2cal in 1:n_sensors2cal){
    
    id_cal_periods   <- which(sensor_calibration_info$Serialnumber==sensors2cal[ith_sensor2cal])
    n_id_cal_periods <- length(id_cal_periods)
    
    # if(n_id_cal_periods<=1){
    #   next
    # }
    if(n_id_cal_periods<1){
      next
    }

    # ------------------------------------------------------------------
    
    # Get time periods where data of this sensor should not be used
    
    query_str   <- paste("SELECT * FROM SensorExclusionPeriods where SensorUnit_ID=",SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal]," and Serialnumber='",sensors2cal[ith_sensor2cal],"' and Type='LP8';",sep="")
    drv         <- dbDriver("MySQL")
    con <-carboutil::get_conn( group="CarboSense_MySQL")
    res         <- dbSendQuery(con, query_str)
    SEP         <- dbFetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    if(dim(SEP)[1]>0){
      SEP$Date_UTC_from <- strptime(SEP$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
      SEP$Date_UTC_to   <- strptime(SEP$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
    }
    
    # ------------------------------------------------------------------
    
    ## Import of relevant sensor data from Decentlab database --> Build data frame "sensor_data"
    ##
    ## Loop over each calibration period
    ##
    
    sensor_data <- NULL
    
    for(ith_id_cal_period in 1:n_id_cal_periods){
      
      
      # Import measurements from Decentlab Influx-DB
      
      timeFilter <- paste("time >= ",sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]],"s AND time < ",sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]],"s",sep="")
      device     <- paste("/",sensor_calibration_info$SensorUnit_ID[id_cal_periods[ith_id_cal_period]],"/",sep="")
      
      if(SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal]%in%c(1007,1008)){
        tmp0 <- query(domain=DL_DB_domain_EMPA,
                      apiKey=DL_DB_apiKey_EMPA,
                      timeFilter=timeFilter,
                      device = device,
                      location = "//",
                      sensor = "/(^frame-counter$)|senseair-lp8-temperature-last|senseair-lp8-temperature|sensirion-sht21-temperature-last|sensirion-sht21-temperature|sensirion-sht21-humidity|senseair-lp8-co2-filtered|senseair-lp8-co2|senseair-lp8-ir-filtered|senseair-lp8-ir-last|senseair-lp8-ir|senseair-lp8-status|time/",
                      channel = "//",
                      aggFunc = "",
                      aggInterval = "",
                      doCast = FALSE,
                      timezone = 'UTC')
      }else{
        tmp0 <- query(domain=DL_DB_domain,
                      apiKey=DL_DB_apiKey,
                      timeFilter=timeFilter,
                      device = device,
                      location = "//",
                      sensor = "/(^frame-counter$)|senseair-lp8-temperature-last|senseair-lp8-temperature|sensirion-sht21-temperature-last|sensirion-sht21-temperature|sensirion-sht21-humidity|senseair-lp8-co2-filtered|senseair-lp8-co2|senseair-lp8-ir-filtered|senseair-lp8-ir-last|senseair-lp8-ir|senseair-lp8-status|time/",
                      channel = "//",
                      aggFunc = "",
                      aggInterval = "",
                      doCast = FALSE,
                      timezone = 'UTC')
      }
      
      
      #
      
      if(is.null(tmp0)){
        next
      }
      if(dim(tmp0)[1]==0){
        next
      }
      
      #
      
      tmp0      <- as.data.table(tmp0)
      tmp0$time <- round(as.numeric(difftime(time1=tmp0$time,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))*1e3)
      tmp0      <- dcast.data.table(tmp0, time ~ series, fun.aggregate = mean,value.var = "value")
      tmp0$time <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp0$time/1e3
      tmp0      <- as.data.frame(tmp0)
      
      
      # Modification of colnames
      
      cn             <- colnames(tmp0)
      cn             <- gsub(pattern = "-",  replacement = "_", x = cn)
      cn             <- gsub(pattern = "\\.",replacement = "_", x = cn)
      cn             <- gsub(pattern = paste(sensor_calibration_info$SensorUnit_ID[id_cal_periods[ith_id_cal_period]],"_",sep=""), replacement = "", x = cn)
      cn             <- gsub(pattern = "senseair_lp8_temperature_last",   replacement = "lp8_T_l",    x=cn)
      cn             <- gsub(pattern = "senseair_lp8_temperature",        replacement = "lp8_T_m",    x=cn)   # LAST or MEAN depending on date
      cn             <- gsub(pattern = "sensirion_sht21_temperature_last",replacement = "sht21_T_l",  x=cn)
      cn             <- gsub(pattern = "sensirion_sht21_temperature",     replacement = "sht21_T_m",  x=cn)   # LAST or MEAN depending on date
      cn             <- gsub(pattern = "sensirion_sht21_humidity",        replacement = "sht21_RH",   x=cn)
      cn             <- gsub(pattern = "senseair_lp8_co2_filtered",       replacement = "lp8_CO2_f",  x=cn)
      cn             <- gsub(pattern = "senseair_lp8_co2",                replacement = "lp8_CO2",    x=cn)
      cn             <- gsub(pattern = "senseair_lp8_ir_filtered",        replacement = "lp8_ir_f",   x=cn)
      cn             <- gsub(pattern = "senseair_lp8_ir_last",            replacement = "lp8_ir_l",   x=cn)
      cn             <- gsub(pattern = "senseair_lp8_ir",                 replacement = "lp8_ir_m",   x=cn)   # LAST or MEAN depending on date
      cn             <- gsub(pattern = "senseair_lp8_status",             replacement = "lp8_status", x=cn)
      cn             <- gsub(pattern = "frame_counter_link_lora",         replacement = "frame_counter", x=cn)
      cn             <- gsub(pattern = "time",                            replacement = "date",       x=cn)
      colnames(tmp0) <- cn
      
      
      # Ensure that all required columns are defined
      
      cn_required   <- c("date","lp8_T_m","lp8_T_l","sht21_T_m","sht21_T_l","sht21_RH","lp8_CO2_f","lp8_CO2","lp8_ir_f","lp8_ir_l","lp8_ir_m","lp8_status","frame_counter")
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
      
      colnames(tmp) <- cn_required
      
      rm(cn_required,n_cn_required,tmp0,pos_tmp0)
      gc()
      
      
      # Force time to full preceding minute
      
      tmp$date      <- as.POSIXct(tmp$date)
      tmp$date      <- strptime(strftime(tmp$date,"%Y%m%d%H%M00",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
      tmp$timestamp <- as.numeric(difftime(time1=tmp$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
      
      # Check data imported from Influx-DB (no duplicates, delta timestamp>300s)
      
      tmp           <- tmp[!duplicated(tmp$timestamp),]
      tmp           <- tmp[order(tmp$timestamp),]
      tmp           <- tmp[c(T,abs(diff(tmp$timestamp))>300),]
      
      # Check frame counter
      
      # TO BE DONE
      
      # set CO2 measurements to NA if status !=0
      
      id_set_to_NA <- which(tmp$lp8_status!=0)
      if(length(id_set_to_NA)){
        tmp$lp8_CO2[id_set_to_NA]   <- NA
        tmp$lp8_CO2_f[id_set_to_NA] <- NA
        tmp$lp8_ir_f[id_set_to_NA]  <- NA
        tmp$lp8_ir_l[id_set_to_NA]  <- NA
        tmp$lp8_ir_m[id_set_to_NA]  <- NA
        tmp$lp8_T_m[id_set_to_NA]   <- NA
        tmp$lp8_T_l[id_set_to_NA]   <- NA
      }
      
      rm(id_set_to_NA)
      gc()
      
      
      # set CO2 measurements to NA if malfunctioning is known for specific period
      
      if(dim(SEP)[1]>0){
        for(ith_SEP in 1:dim(SEP)[1]){
          id_set_to_NA <- which(tmp$date>=SEP$Date_UTC_from[ith_SEP] & tmp$date<=SEP$Date_UTC_to[ith_SEP])
          if(length(id_set_to_NA)>0){
            tmp$lp8_CO2[id_set_to_NA]   <- NA
            tmp$lp8_CO2_f[id_set_to_NA] <- NA
            tmp$lp8_ir_f[id_set_to_NA]  <- NA
            tmp$lp8_ir_l[id_set_to_NA]  <- NA
            tmp$lp8_ir_m[id_set_to_NA]  <- NA
            tmp$lp8_T_m[id_set_to_NA]   <- NA
            tmp$lp8_T_l[id_set_to_NA]   <- NA
          }
        }
        
        rm(id_set_to_NA)
        gc()
      }
      
      # combine sensor_data and tmp
      if(is.null(sensor_data)){
        sensor_data <- tmp
      }else{
        sensor_data <- rbind(sensor_data,tmp)
      }
    }
    
    # Check that data frame sensor_data is ordered correctly
    
    sensor_data <- sensor_data[order(sensor_data$date),] 
    
    # Select correct measuring mode
    
    # Note: 
    # _m can be last or mean measurement depending on time (before/after LP8_SU_upgrade_01)
    # _l is always the last measurement but only available after LP8_SU_upgrade_01
    
    if(MEAS_MODE=="LAST"){
      
      sensor_data$lp8_T   <- sensor_data$lp8_T_l
      sensor_data$sht21_T <- sensor_data$sht21_T_l
      sensor_data$lp8_ir  <- sensor_data$lp8_ir_l
      
      id_vers_01 <- which(sensor_data$date<=date_UTC_LP8_SU_upgrade_01)
      
      if(length(id_vers_01)>0){
        sensor_data$lp8_T[id_vers_01]     <- sensor_data$lp8_T_m[id_vers_01]
        sensor_data$sht21_T[id_vers_01]   <- sensor_data$sht21_T_m[id_vers_01]
        sensor_data$lp8_ir[id_vers_01]    <- sensor_data$lp8_ir_m[id_vers_01]
        
        sensor_data$lp8_T_m[id_vers_01]   <- NA
        sensor_data$sht21_T_m[id_vers_01] <- NA
        sensor_data$lp8_ir_m[id_vers_01]  <- NA
      }
      
      rm(id_vers_01)
      gc()
    }
    
    if(MEAS_MODE=="MEAN"){
      
      sensor_data$lp8_T   <- sensor_data$lp8_T_m
      sensor_data$sht21_T <- sensor_data$sht21_T_m
      sensor_data$lp8_ir  <- sensor_data$lp8_ir_m
      
      id_vers_01 <- which(sensor_data$date<=date_UTC_LP8_SU_upgrade_01)
      
      if(length(id_vers_01)>0){
        sensor_data$lp8_T_m[id_vers_01]   <- NA
        sensor_data$sht21_T_m[id_vers_01] <- NA
        sensor_data$lp8_ir_m[id_vers_01]  <- NA
      }
      
      rm(id_vers_01)
      gc()
    }
    
    # ------------------------------------------------------------------
    
    # Import of reference data. Build of data frame "ref_data"
    
    ref_data  <- NULL
    
    cn_required   <- c("timestamp","CO2_DRY","CO2_DRY_F","H2O","H2O_F","T","T_F","RH","RH_F","pressure","pressure_F","CalMode","WIGE1","WIGE1_F")
    n_cn_required <- length(cn_required)
    
    for(ith_id_cal_period in 1:n_id_cal_periods){
      
      if(sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]] == "NABEL_DUE"){
        query_str <- paste("SELECT timestamp, CO2_DRY_CAL, CO2_DRY_F, CO2_DRY_CAL_10MIN_AV, H2O, H2O_F, H2O_10MIN_AV, T, T_F, T_10MIN_AV, RH, RH_F, RH_10MIN_AV, pressure, pressure_F, pressure_10MIN_AV, WIGE1, WIGE1_F ",sep="")
        query_str <- paste(query_str, "FROM ",sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]," ",sep="")
        query_str <- paste(query_str, "WHERE timestamp >= ",sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]]," ",sep="")
        query_str <- paste(query_str, "AND timestamp < ",sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]],";",sep="")
        
        drv       <- dbDriver("MySQL")
        con<-carboutil::get_conn(group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        tmp0      <- dbFetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        tmp           <- as.data.frame(matrix(NA,nrow=dim(tmp0)[1],ncol=n_cn_required))
        colnames(tmp) <- cn_required
        
        tmp$timestamp  <- tmp0$timestamp - 60 # Pumping air from inlet to Picarro
        tmp$CO2_DRY    <- tmp0$CO2_DRY_CAL
        tmp$CO2_DRY_F  <- tmp0$CO2_DRY_F
        tmp$H2O        <- tmp0$H2O
        tmp$H2O_F      <- tmp0$H2O_F
        tmp$T          <- tmp0$T
        tmp$T_F        <- tmp0$T_F
        tmp$RH         <- tmp0$RH
        tmp$RH_F       <- tmp0$RH_F
        tmp$pressure   <- tmp0$pressure
        tmp$pressure_F <- tmp0$pressure_F
        tmp$WIGE1      <- tmp0$WIGE1
        tmp$WIGE1_F    <- tmp0$WIGE1_F
        
        tmp$CalMode    <- 1
        
        
        
        if(MEAS_MODE=="MEAN"){
          id_version_02 <- which(tmp0$timestamp >= timestamp_LP8_SU_upgrade_01)
          
          if(length(id_version_02)>0){
            tmp$CO2_DRY[id_version_02]    <- tmp0$CO2_DRY_CAL_10MIN_AV[id_version_02]
            tmp$CO2_DRY_F[id_version_02]  <- as.numeric(tmp0$CO2_DRY_CAL_10MIN_AV[id_version_02] != -999)
            tmp$H2O[id_version_02]        <- tmp0$H2O_10MIN_AV[id_version_02]
            tmp$H2O_F[id_version_02]      <- as.numeric(tmp0$H2O_10MIN_AV[id_version_02] != -999)
            tmp$T[id_version_02]          <- tmp0$T_10MIN_AV[id_version_02]
            tmp$T_F[id_version_02]        <- as.numeric(tmp0$T_10MIN_AV[id_version_02] != -999)
            tmp$RH[id_version_02]         <- tmp0$RH_10MIN_AV[id_version_02]
            tmp$RH_F[id_version_02]       <- as.numeric(tmp0$RH_10MIN_AV[id_version_02] != -999)
            tmp$pressure[id_version_02]   <- tmp0$pressure_10MIN_AV[id_version_02]
            tmp$pressure_F[id_version_02] <- as.numeric(tmp0$pressure_10MIN_AV[id_version_02] != -999)
          }
        }
        
        rm(tmp0)
        gc()
      }
      
      if(sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]] == "NABEL_RIG"){
        query_str <- paste("SELECT timestamp, CO2_DRY_CAL, CO2_DRY_F, CO2_DRY_CAL_10MIN_AV, H2O, H2O_F, H2O_10MIN_AV, T, T_F, T_10MIN_AV, RH, RH_F, RH_10MIN_AV, pressure, pressure_F, pressure_10MIN_AV ",sep="")
        query_str <- paste(query_str, "FROM ",sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]," ",sep="")
        query_str <- paste(query_str, "WHERE timestamp >= ",sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]]," ",sep="")
        query_str <- paste(query_str, "AND timestamp < ",sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]],";",sep="")
        
        drv       <- dbDriver("MySQL")
        con<-carboutil::get_conn(group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        tmp0      <- dbFetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        tmp           <- as.data.frame(matrix(NA,nrow=dim(tmp0)[1],ncol=n_cn_required))
        colnames(tmp) <- cn_required
        
        tmp$timestamp  <- tmp0$timestamp
        tmp$CO2_DRY    <- tmp0$CO2_DRY_CAL
        tmp$CO2_DRY_F  <- tmp0$CO2_DRY_F  * as.numeric(tmp0$CO2_DRY_CAL_10MIN_AV != -999)
        tmp$H2O        <- tmp0$H2O
        tmp$H2O_F      <- tmp0$H2O_F      * as.numeric(tmp0$H2O_10MIN_AV != -999)
        tmp$T          <- tmp0$T
        tmp$T_F        <- tmp0$T_F        * as.numeric(tmp0$T_10MIN_AV != -999)
        tmp$RH         <- tmp0$RH
        tmp$RH_F       <- tmp0$RH_F       * as.numeric(tmp0$RH_10MIN_AV != -999)
        tmp$pressure   <- tmp0$pressure
        tmp$pressure_F <- tmp0$pressure_F * as.numeric(tmp0$pressure_10MIN_AV != -999)
        tmp$WIGE1      <- -999
        tmp$WIGE1_F    <- 0
        
        tmp$CalMode    <- 1
        
        
        
        if(MEAS_MODE=="MEAN"){
          id_version_02 <- which(tmp0$timestamp >= timestamp_LP8_SU_upgrade_01)
          
          if(length(id_version_02)>0){
            tmp$CO2_DRY[id_version_02]    <- tmp0$CO2_DRY_CAL_10MIN_AV[id_version_02]
            tmp$CO2_DRY_F[id_version_02]  <- as.numeric(tmp0$CO2_DRY_CAL_10MIN_AV[id_version_02] != -999)
            tmp$H2O[id_version_02]        <- tmp0$H2O_10MIN_AV[id_version_02]
            tmp$H2O_F[id_version_02]      <- as.numeric(tmp0$H2O_10MIN_AV[id_version_02] != -999)
            tmp$T[id_version_02]          <- tmp0$T_10MIN_AV[id_version_02]
            tmp$T_F[id_version_02]        <- as.numeric(tmp0$T_10MIN_AV[id_version_02] != -999)
            tmp$RH[id_version_02]         <- tmp0$RH_10MIN_AV[id_version_02]
            tmp$RH_F[id_version_02]       <- as.numeric(tmp0$RH_10MIN_AV[id_version_02] != -999)
            tmp$pressure[id_version_02]   <- tmp0$pressure_10MIN_AV[id_version_02]
            tmp$pressure_F[id_version_02] <- as.numeric(tmp0$pressure_10MIN_AV[id_version_02] != -999)
          }
        }
        
        rm(tmp0)
        gc()
      }

      if(sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]] == "PressureChamber_00_METAS"){
        query_str <- paste("SELECT timestamp, CO2_DRY, CO2_DRY_F, CO2_DRY_10MIN_AV, H2O, H2O_F, H2O_10MIN_AV, pressure, pressure_F, pressure_10MIN_AV ",sep="")
        query_str <- paste(query_str, "FROM ",sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]," ",sep="")
        query_str <- paste(query_str, "WHERE timestamp >= ",sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]]," ",sep="")
        query_str <- paste(query_str, "AND timestamp < ",sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]],";",sep="")
        
        drv       <- dbDriver("MySQL")
        con<-carboutil::get_conn(group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        tmp0      <- dbFetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        tmp           <- as.data.frame(matrix(NA,nrow=dim(tmp0)[1],ncol=n_cn_required))
        colnames(tmp) <- cn_required
        
        tmp$timestamp  <- tmp0$timestamp
        tmp$CO2_DRY    <- tmp0$CO2_DRY
        tmp$CO2_DRY_F  <- tmp0$CO2_DRY_F
        tmp$H2O        <- tmp0$H2O
        tmp$H2O_F      <- tmp0$H2O_F
        tmp$T          <- -999
        tmp$T_F        <- 0
        tmp$RH         <- -999
        tmp$RH_F       <- 0
        tmp$pressure   <- tmp0$pressure_10MIN_AV
        tmp$pressure_F <- as.numeric(tmp0$pressure_10MIN_AV != -999)
        tmp$WIGE1      <- -999
        tmp$WIGE1_F    <- 0
        
        tmp$CalMode    <- 3
        
        rm(tmp0)
        gc()
      }      
      
      if(sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]] == "PressureChamber_01_DUE"){
        query_str <- paste("SELECT timestamp, CO2_DRY, CO2_DRY_F, CO2_DRY_10MIN_AV, H2O, H2O_F, H2O_10MIN_AV, T, T_F, T_10MIN_AV, pressure, pressure_F, pressure_10MIN_AV ",sep="")
        query_str <- paste(query_str, "FROM ",sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]," ",sep="")
        query_str <- paste(query_str, "WHERE timestamp >= ",sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]]," ",sep="")
        query_str <- paste(query_str, "AND timestamp < ",sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]],";",sep="")
        
        drv       <- dbDriver("MySQL")
        con<-carboutil::get_conn(group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        tmp0      <- dbFetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        tmp           <- as.data.frame(matrix(NA,nrow=dim(tmp0)[1],ncol=n_cn_required))
        colnames(tmp) <- cn_required
        
        tmp$timestamp  <- tmp0$timestamp
        tmp$CO2_DRY    <- tmp0$CO2_DRY_10MIN_AV
        tmp$CO2_DRY_F  <- as.numeric(tmp0$CO2_DRY_10MIN_AV != -999)
        tmp$H2O        <- tmp0$H2O_10MIN_AV
        tmp$H2O_F      <- as.numeric(tmp0$H2O_10MIN_AV != -999)
        tmp$T          <- tmp0$T_10MIN_AV
        tmp$T_F        <- as.numeric(tmp0$T_10MIN_AV != -999)
        tmp$RH         <- -999
        tmp$RH_F       <- 0
        tmp$pressure   <- tmp0$pressure_10MIN_AV
        tmp$pressure_F <- as.numeric(tmp0$pressure_10MIN_AV != -999)
        tmp$WIGE1      <- -999
        tmp$WIGE1_F    <- 0
        
        tmp$CalMode    <- 3
        
        rm(tmp0)
        gc()
      }
      
      if(sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]] == "ClimateChamber_00_DUE"){
        query_str <- paste("SELECT timestamp, CO2_DRY, CO2_F, CO2_DRY_10MIN_AV, H2O, H2O_F, H2O_10MIN_AV, T, T_F, T_10MIN_AV, RH, RH_F, RH_10MIN_AV, pressure, pressure_F, pressure_10MIN_AV ",sep="")
        query_str <- paste(query_str, "FROM ",sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]," ",sep="")
        query_str <- paste(query_str, "WHERE timestamp >= ",sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]]," ",sep="")
        query_str <- paste(query_str, "AND timestamp < ",sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]],";",sep="")
        
        drv       <- dbDriver("MySQL")
        con<-carboutil::get_conn(group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        tmp0      <- dbFetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        tmp           <- as.data.frame(matrix(NA,nrow=dim(tmp0)[1],ncol=n_cn_required))
        colnames(tmp) <- cn_required
        
        tmp$timestamp  <- tmp0$timestamp
        tmp$CO2_DRY    <- tmp0$CO2_DRY
        tmp$CO2_DRY_F  <- tmp0$CO2_F
        tmp$H2O        <- tmp0$H2O
        tmp$H2O_F      <- tmp0$H2O_F
        tmp$T          <- tmp0$T
        tmp$T_F        <- tmp0$T_F        * as.numeric(tmp0$T_10MIN_AV != -999)
        tmp$RH         <- tmp0$RH
        tmp$RH_F       <- tmp0$RH_F
        tmp$pressure   <- tmp0$pressure
        tmp$pressure_F <- tmp0$pressure_F * as.numeric(tmp0$pressure_10MIN_AV != -999)
        tmp$WIGE1      <- -999
        tmp$WIGE1_F    <- 0
        
        tmp$CalMode    <- 2
        
        
        
        if(MEAS_MODE=="MEAN"){
          id_version_02 <- which(tmp0$timestamp >= timestamp_LP8_SU_upgrade_01)
          
          if(length(id_version_02)>0){
            tmp$CO2_DRY[id_version_02]    <- tmp0$CO2_DRY_10MIN_AV[id_version_02]
            tmp$CO2_DRY_F[id_version_02]  <- as.numeric(tmp0$CO2_DRY_10MIN_AV[id_version_02] != -999)
            tmp$H2O[id_version_02]        <- tmp0$H2O_10MIN_AV[id_version_02]
            tmp$H2O_F[id_version_02]      <- as.numeric(tmp0$H2O_10MIN_AV[id_version_02] != -999)
            tmp$T[id_version_02]          <- tmp0$T_10MIN_AV[id_version_02]
            tmp$T_F[id_version_02]        <- as.numeric(tmp0$T_10MIN_AV[id_version_02] != -999)
            tmp$RH[id_version_02]         <- tmp0$RH_10MIN_AV[id_version_02]
            tmp$RH_F[id_version_02]       <- as.numeric(tmp0$RH_10MIN_AV[id_version_02] != -999)
            tmp$pressure[id_version_02]   <- tmp0$pressure_10MIN_AV[id_version_02]
            tmp$pressure_F[id_version_02] <- as.numeric(tmp0$pressure_10MIN_AV[id_version_02] != -999)
          }
        }
        
        rm(tmp0)
        gc()
      }
        
      #
      
      if(all(tmp$RH==-999)){
        tmp$RH   <- 50
        tmp$RH_F <- 1
      }
        
      ref_data <- rbind(ref_data,tmp)

    }
    
    if(dim(ref_data)[1]==0){
      stop("Error: No reference data found.")
    }
    
    # Check reference data
    
    ref_data      <- ref_data[!duplicated(ref_data$timestamp),]
    colnames(ref_data)[which(colnames(ref_data)=="timestamp")] <- "date"
    ref_data$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + ref_data$date
    ref_data      <- ref_data[order(ref_data$date),]
    
    # SET data to NA if flag==0 or data == -999
    
    u_cn   <- gsub(pattern = "_F",replacement = "",colnames(ref_data))
    u_cn   <- sort(unique(u_cn))
    u_cn   <- u_cn[which(!u_cn%in%c("date"))]
    n_u_cn <- length(u_cn)
    
    for(ith_col in 1:n_u_cn){
      pos   <- which(colnames(ref_data)==u_cn[ith_col])
      pos_F <- which(colnames(ref_data)==paste(u_cn[ith_col],"_F",sep=""))
      
      id_set_to_NA <- which(ref_data[,pos]== -999
                            | is.na(ref_data[,pos])
                            | is.na(ref_data[,pos_F])
                            | ref_data[,pos_F]==0)
      
      if(length(id_set_to_NA)>0){
        ref_data[id_set_to_NA,pos] <- NA
      }
    }
    
    rm(pos,pos_F,id_set_to_NA,u_cn,n_u_cn)
    gc()
    
    
    # ------------------------------------------------------------------
    
    # Merge sensor and reference data into data.frame "data"
    
    sensor_data$date <- as.POSIXct(sensor_data$date)
    ref_data$date    <- as.POSIXct(ref_data$date)
    
    data             <- merge(sensor_data,ref_data,by="date")
    
    data$timestamp   <- as.numeric(difftime(time1=data$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
    
    rm(sensor_data,ref_data)
    gc()
    
    # ------------------------------------------------------------------
    
    # Additional columns in data.frame "data"
    
    # LP8 Sensor terms
    data$lp8_logIR <- -log(data$lp8_ir)
    
    # timestamp (continuous time)
    data$timestamp <- as.numeric(difftime(time1=data$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
    
    # days (continuous time)
    data$days      <- as.numeric(difftime(time1=data$date,time2=strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="days"))
    
    
    # Humidity measures
    # [W. Wagner and A. Pru�: The IAPWS Formulation 1995 for the Thermodynamic Properties of Ordinary Water Substance for General and Scientific Use, Journal of Physical and Chemical Reference Data, June 2002 ,Volume 31, Issue 2, pp. 387535]
    
    # abs humidity (reference, SHT21)
    
    coef_1 <-  -7.85951783
    coef_2 <-   1.84408259
    coef_3 <-  -11.7866497
    coef_4 <-   22.6807411
    coef_5 <-  -15.9618719
    coef_6 <-   1.80122502
    
    theta     <- 1 - (273.15+data$T)/647.096
    Pws       <- 220640 * exp( 647.096/(273.15+data$T) * (coef_1*theta + coef_2*theta^1.5 + coef_3*theta^3 + coef_4*theta^3.5 + coef_5*theta^4 + coef_6*theta^7.5))
    Pw        <- data$RH*(Pws*100)/100
    data$AH   <- 2.16679 * Pw/(273.15+data$T)
    data$AH_F <- 1
    
    id_set_to_NA   <- which((data$T_F * data$RH_F)==0 | data$T == -999 | is.na(data$T) | data$RH == -999 | is.na(data$RH))
    
    if(length(id_set_to_NA)>0){
      data$AH[id_set_to_NA]   <- NA
      data$AH_F[id_set_to_NA] <- 0
    }
    
    
    theta             <- 1 - (273.15+data$sht21_T)/647.096
    Pws_sht21         <- 220640 * exp( 647.096/(273.15+data$sht21_T) * (coef_1*theta + coef_2*theta^1.5 + coef_3*theta^3 + coef_4*theta^3.5 + coef_5*theta^4 + coef_6*theta^7.5))
    Pw_sht21          <- data$sht21_RH*(Pws_sht21*100)/100
    data$sht21_AH     <- 2.16679 * Pw_sht21/(273.15+data$sht21_T)
    
    # H2O (reference, SHT21)
    
    data$H2O_COMP           <- Pw           / (data$pressure*1e2)*1e2
    data$sht21_H2O_COMP     <- Pw_sht21     / (data$pressure*1e2)*1e2
    
    rm(coef_1,coef_2,coef_3,coef_4,coef_4,coef_5,coef_6,theta,Pws,Pw,id_set_to_NA,theta,theta,Pws_sht21,Pw_sht21)
    gc()
    
    
    # Dew point using Magnus formula (reference, SHT21)
    
    K2 <-  17.62
    K3 <- 243.12
    
    data$DP              <- K3 * ( ( (K2*data$T)      /(K3 + data$T)       + log(data$RH/1e2) )      /( (K2*K3)/(K3+data$T)       - log(data$RH/1e2) ) )
    data$diff_T_DP       <- data$T - data$DP
    
    data$sht21_DP        <- K3 * ( ( (K2*data$sht21_T)/(K3 + data$sht21_T) + log(data$sht21_RH/1e2) )/( (K2*K3)/(K3+data$sht21_T) - log(data$sht21_RH/1e2) ) )
    data$diff_sht21_T_DP <- data$sht21_T - data$sht21_DP
    
    
    # Difference SHT21 - LP8 / REF - LP8 / SHT21 - REF
    
    data$diff_T_SHT_LP8 <- data$sht21_T - data$lp8_T
    data$diff_T_REF_LP8 <- data$T       - data$lp8_T
    data$diff_T_SHT_REF <- data$sht21_T - data$T
    
    # Temperatur gradient
    
    data$d_sht21_T <- NA
    data$d_lp8_T   <- NA
    
    for(ith_row in 6:dim(data)[1]){
      id_n <- (ith_row-5):ith_row
      id   <- which(data$days[ith_row]-data$days[id_n]<=0.041667 & !is.na(data$sht21_T[id_n]) & !is.na(data$lp8_T[id_n]))
      n_id <- length(id)
      
      if(n_id>=3){
        id <- id_n[id]
        
        tmp_sht21_T <- lm.fit(x=matrix(c(rep(1,n_id),data$days[id]),ncol=2),y=data$sht21_T[id])
        tmp_lp8_T   <- lm.fit(x=matrix(c(rep(1,n_id),data$days[id]),ncol=2),y=data$lp8_T[id])
        
        data$d_sht21_T[ith_row] <- tmp_sht21_T$coefficients[2]/1440
        data$d_lp8_T[ith_row]   <- tmp_lp8_T$coefficients[2]/1440
      }
    }
    
    rm(tmp_sht21_T,tmp_lp8_T,id_n,id,n_id)
    gc()
    
    # gap
    
    data$noGAP <- c(F,diff(data$timestamp)>500 & diff(data$timestamp)<700)
    
    # Temperature difference (LP8)
    
    data$delta_lp8_T <- c(NA,diff(data$lp8_T))
    data$delta_lp8_T[!data$noGAP] <- NA
    
    # Temperature difference (SHT21)
    
    data$delta_sht21_T <- c(NA,diff(data$sht21_T))
    data$delta_sht21_T[!data$noGAP] <- NA
    
    # CO2 difference (reference)
    
    data$delta_CO2 <- c(NA,diff(data$CO2))
    data$delta_CO2[!data$noGAP] <- NA
    
    # IR difference (LP8)
    
    data$delta_lp8_ir <- c(NA,diff(data$lp8_ir))
    data$delta_lp8_ir[!data$noGAP] <- NA
    
    
    # ------------------------------------------------------------------
    
    ## Apply corrections to CO2 reference values [Picarro [mol/mol], LP8 [mol/V]]
    
    # CO2 dry --> CO2 wet
    
    data$CO2     <- data$CO2_DRY * (1-data$H2O/100)
    
    data$CO2_ppm <- data$CO2
    
    # SenseAir formula
    
    data$lp8_CO2_pcorr <- data$lp8_CO2 /( 1 + 1.5924 * ((data$pressure-1012.4)/1012.4) + 0.5924 * ((data$pressure-1012.4)/1012.4)^2)
    
    # CO2 wet --> CO2 wet [variable number of molecules due to pressure]
    
    data$CO2 <- data$CO2 * data$pressure / 1013.25
    
    # CO2 wet --> CO2 wet [variable number of molecules due to temperature]
    
    data$CO2 <- data$CO2 * 273.15 / (data$lp8_T+273.15)
    
    
    # ------------------------------------------------------------------
    
    sensor_descriptor <- paste("SU", SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],"_S",sensors2cal[ith_sensor2cal],sep="")
    
    # ------------------------------------------------------------------
    
    # Check SHT21 T and RH
    
    stable_conditions <- rep(F,dim(data)[1])
    complete_cases    <- (!is.na(data$T) & !is.na(data$RH) & !is.na(data$sht21_T) & !is.na(data$lp8_T) & !is.na(data$sht21_RH) & !is.na(data$WIGE1) & data$CalMode==1)
    
    for(ith_row in 6:dim(data)[1]){
      
      index <- ((ith_row-5):ith_row)
      index <- index[(data$timestamp[ith_row] - data$timestamp[index]) <= 3200 & complete_cases[index]]
      
      if(length(index)==6){
        if(abs(diff(range(data$T[index])))          < 1.0
           & abs(diff(range(data$RH[index])))       < 5.0
           & abs(diff(range(data$sht21_T[index])))  < 1.0
           & abs(diff(range(data$sht21_RH[index]))) < 5.0
           & mean(data$WIGE1[index])                >=1.0
           & all(data$RH[index]                     <= 95)){
          
          stable_conditions[ith_row] <- T
        }
      }
    }
    
    
    id <- which(as.numeric(strftime(data$date,"%H",tz="UTC"))%in%c(0,1,2) 
                & complete_cases
                & stable_conditions)
    
    if(length(id)>10){
      sht21_T_DIFF       <- median(data$sht21_T[id]  - data$T[id])
      sht21_T_DIFF_MAD   <- mad(data$sht21_T[id]     - data$T[id])
      sht21_RH_DIFF      <- median(data$sht21_RH[id] - data$RH[id])
      sht21_RH_DIFF_MAD  <- mad(data$sht21_RH[id]    - data$RH[id])
      lp8_T_DIFF         <- median(data$lp8_T[id]    - data$T[id])
      lp8_T_DIFF_MAD     <- mad(data$lp8_T[id]       - data$T[id])
    }else{
      sht21_T_DIFF       <- NA
      sht21_T_DIFF_MAD   <- NA
      sht21_RH_DIFF      <- NA
      sht21_RH_DIFF_MAD  <- NA
      lp8_T_DIFF         <- NA
      lp8_T_DIFF_MAD     <- NA
    }
    
    
    SHT_CORRECTIONS <- rbind(SHT_CORRECTIONS,data.frame(SensorUnit_ID     = SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],
                                                        Date_UTC_from     = strftime(min(data$date[id]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                        Date_UTC_to       = strftime(max(data$date[id]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                        N                 = length(id),
                                                        LP8_ID            = sensors2cal[ith_sensor2cal],
                                                        sht21_T_DIFF      = sht21_T_DIFF,
                                                        sht21_T_DIFF_MAD  = sht21_T_DIFF_MAD,
                                                        sht21_RH_DIFF     = sht21_RH_DIFF,
                                                        sht21_RH_DIFF_MAD = sht21_RH_DIFF_MAD,
                                                        lp8_T_DIFF        = lp8_T_DIFF,
                                                        lp8_T_DIFF_MAD    = lp8_T_DIFF_MAD,
                                                        stringsAsFactors = F))
    
    rm(id,stable_conditions,complete_cases,index)
    rm(sht21_T_DIFF,sht21_T_DIFF_MAD,sht21_RH_DIFF,sht21_RH_DIFF_MAD,sht21_T_DIFF,sht21_RH_DIFF,lp8_T_DIFF,lp8_T_DIFF_MAD)
    gc()
    
    # ------------------------------------------------------------------
    
    ## Sensor measurements HIST
    
    if(F){ # TURN TO F
      
      # HIST delta CO2
      
      if(F){ # TURN TO F
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_delta_CO2_HIST.pdf",sep="")
        
        res    <- data$delta_CO2[which(!is.na(data$delta_CO2))]
        
        str_00 <- paste("Q000:",sprintf("%7.1f",quantile(res,probs=0.00)))
        str_01 <- paste("Q005:",sprintf("%7.1f",quantile(res,probs=0.05)))
        str_02 <- paste("Q050:",sprintf("%7.1f",quantile(res,probs=0.50)))
        str_03 <- paste("Q095:",sprintf("%7.1f",quantile(res,probs=0.95)))
        str_04 <- paste("Q100:",sprintf("%7.1f",quantile(res,probs=1.00)))
        
        str_05 <- paste("MEAN:",sprintf("%7.1f",mean(res)))
        str_06 <- paste("SD:  ",sprintf("%7.1f",sd(res)))
        str_07 <- paste("N:   ",sprintf("%7.0f",length(res)))
        
        xlabString <- expression(paste("CO"[2]*" (t) - CO"[2]*" (t-10Min) [ppm]"))
        ylabString <- expression(paste("Number"))
        legend_str <- c(str_00,str_01,str_02,str_03,str_04,str_05,str_06,str_07)
        
        
        def_par <- par()
        pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
        par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
        
        hist_range <- range(quantile(res,probs=c(0.005,0.995)))
        seq_range  <- seq(-1e3,1e3,0.25)
        hh=hist(res,seq_range,col="slategray",xlim=hist_range,xlab=xlabString,ylab=ylabString,main="")
        
        boxplot(res,horizontal = T,col=2, at=0.25*max(hh$counts),add=T,outline = F, boxwex=0.1*max(hh$counts))
        
        par(family="mono")
        legend("topright",legend=legend_str,bg="white")
        par(family="")
        
        dev.off()
        par(def_par)
        
        rm(res,seq_range,hh)
        gc()
        
      }
      
      # HIST delta T
      
      if(F){ # TURN TO F
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_delta_T_HIST.pdf",sep="")
        
        res    <- data$delta_lp8_T[which(!is.na(data$delta_lp8_T))]
        
        str_00 <- paste("Q000:",sprintf("%7.1f",quantile(res,probs=0.00)))
        str_01 <- paste("Q005:",sprintf("%7.1f",quantile(res,probs=0.05)))
        str_02 <- paste("Q050:",sprintf("%7.1f",quantile(res,probs=0.50)))
        str_03 <- paste("Q095:",sprintf("%7.1f",quantile(res,probs=0.95)))
        str_04 <- paste("Q100:",sprintf("%7.1f",quantile(res,probs=1.00)))
        
        str_05 <- paste("MEAN:",sprintf("%7.1f",mean(res)))
        str_06 <- paste("SD:  ",sprintf("%7.1f",sd(res)))
        str_07 <- paste("N:   ",sprintf("%7.0f",length(res)))
        
        xlabString <- "lp8_T (t) - lp8_T (t-10Min) [deg C]" 
        ylabString <- expression(paste("Number"))
        legend_str <- c(str_00,str_01,str_02,str_03,str_04,str_05,str_06,str_07)
        
        
        
        def_par <- par()
        pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
        par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
        
        hist_range <- range(quantile(res,probs=c(0.005,0.995)))
        seq_range  <- seq(-1e3,1e3,0.1)
        hh=hist(res,seq_range,col="slategray",xlim=hist_range,xlab=xlabString,ylab=ylabString,main="")
        
        boxplot(res,horizontal = T,col=2, at=0.25*max(hh$counts),add=T,outline = F, boxwex=0.1*max(hh$counts))
        
        par(family="mono")
        legend("topright",legend=legend_str,bg="white")
        par(family="")
        
        dev.off()
        par(def_par)
        
        rm(res,seq_range,hh)
        gc()
      }
    }
    
    ## Sensor measurements Boxplots
    
    if(T){
      
      # BOXPLOT diff T/RH
      
      data_hour <- as.numeric(strftime(data$date,"%H",tz="UTC"))
      
      if(T){
        
        for(CM in 1:1){
          
          figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_delta_T_RH_Boxplot_CM",sprintf("%02.0f",CM),".pdf",sep="")
          
          def_par <- par()
          pdf(file = figname, width=10, height=8, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.1,1),mfrow=c(1,1))
          
          for(ith_diff in 1:7){
            
            if(ith_diff==1){
              difference <- data$lp8_T - data$T
              xlabString <- "Hour of day [UTC]"
              ylabString <- "LP8_T - REF_T [deg C]"
              yrange     <- c(-3,3)
            }
            if(ith_diff==2){
              difference <- data$sht21_T - data$T
              xlabString <- "Hour of day [UTC]"
              ylabString <- "SHT21_T - REF_T [deg C]"
              yrange     <- c(-3,3)
            }
            if(ith_diff==3){
              difference <- data$sht21_T-data$lp8_T
              xlabString <- "Hour of day [UTC]"
              ylabString <- "SHT21_T - LP8_T [deg C]"
              yrange     <- c(-3,3)
            }
            if(ith_diff==4){
              difference <- data$sht21_RH-data$RH
              xlabString <- "Hour of day [UTC]"
              ylabString <- "SHT21_RH - REF_RH [%]"
              yrange     <- c(-20,20)
            }
            if(ith_diff==5){
              difference <- data$sht21_H2O_COMP-data$H2O
              xlabString <- "Hour of day [UTC]"
              ylabString <- "SHT21_H2O - REF_H2O [vol-%]"
              yrange     <- c(-2,2)
            }
            if(ith_diff==6){
              difference <- data$H2O_COMP-data$H2O
              xlabString <- "Hour of day [UTC]"
              ylabString <- "H2O_COMP - REF_H2O [vol-%]"
              yrange     <- c(-2,2)
            }
            if(ith_diff==7){
              difference <- data$sht21_AH-data$AH
              xlabString <- "Hour of day [UTC]"
              ylabString <- "SHT21_AH - REF_AH [g/m3]"
              yrange     <- c(-2,2)
            }
            
            tmp <- matrix(NA,ncol=24,nrow=dim(data)[1])
            
            for(hh in 0:23){
              id   <- which(data_hour==hh & !is.na(difference) & data$CalMode==CM)
              n_id <- length(id)
              
              if(n_id > 0){
                tmp[1:n_id,hh+1] <- difference[id]
              }
            }
            
            boxplot(tmp,ylim=yrange,xlim=c(0,25),xaxt="n",col="slategray",cex.axis=1.25,cex.lab=1.25,ylab=ylabString,xlab=xlabString,pch=16,cex=0.5)
            lines(c(-1e3,1e3),c(0,0),col=2,lwd=1,lty=1)
            points(1:24,apply(tmp,2,median,na.rm=T),pch=15,cex=1,col=2)
            axis(side = 1,at=seq(1,24,2),labels = seq(0,23,2),cex.axis=1.25,cex.lab=1.25)
            
            par(new=T)
            
            plot(1:24,apply(!is.na(tmp),2,sum,na.rm=T),pch=15,cex=1,col=4,xaxt="n",yaxt="n",ylab="",xlab="")
            axis(side = 4,cex.axis=1.25,cex.lab=1.25)
            mtext(text = "Number of samples",side = 4,line = 2,cex=1.25)
          }
          
          dev.off()
          par(def_par)
          
        }
        
        rm(difference,tmp,id,n_id,ylabString,xlabString,data_hour)
        gc()
      }
      
    }
    
    ## Sensor measurement time-series
    
    if(T){
      
      # T / dew point
      
      if(T){
        figname    <- paste(plotdir_allModels,"/",sensor_descriptor,"_T_DEWPOINT_TS.pdf",sep="")
        
        yyy        <- cbind(data$T,
                            data$lp8_T,
                            data$sht21_T,
                            data$DP,
                            data$sht21_DP,
                            data$T - data$DP,
                            data$sht21_T - data$sht21_DP)
        
        xlabString <- "Date" 
        ylabString <- expression(paste("T [deg C]"))
        legend_str <- c("T REF","T LP8","T SHT","DP REF","DP SHT","T REF-DP REF","T SHT-DP SHT")
        plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-15,55),xlabString,ylabString,legend_str)
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_T_DEWPOINT_TS_DAYS.pdf",sep="")
        plot_ts(figname,data$date,yyy,"day",NULL,c(-15,55),xlabString,ylabString,legend_str)
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_T_DEWPOINT_TS_WEEKS.pdf",sep="")
        plot_ts(figname,data$date,yyy,"week",NULL,c(-15,55),xlabString,ylabString,legend_str)
      }
      
      # Diff T
      
      if(T){
        figname    <- paste(plotdir_allModels,"/",sensor_descriptor,"_DIFF_T_TS.pdf",sep="")
        
        yyy        <- cbind(data$T -  data$lp8_T,
                            data$sht21_T -  data$lp8_T)
        
        xlabString <- "Date" 
        ylabString <- expression(paste("T [deg C]"))
        legend_str <- c("T REF - T LP8","T SHT - T LP8")
        plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-5,5),xlabString,ylabString,legend_str)
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_DIFF_T_TS_DAYS.pdf",sep="")
        plot_ts(figname,data$date,yyy,"day",NULL,c(-5,5),xlabString,ylabString,legend_str)
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_DIFF_T_TS_WEEKS.pdf",sep="")
        plot_ts(figname,data$date,yyy,"week",NULL,c(-5,5),xlabString,ylabString,legend_str)
      }
      
      # RH
      
      if(T){
        figname    <- paste(plotdir_allModels,"/",sensor_descriptor,"_RH_TS.pdf",sep="")
        
        yyy        <- cbind(data$RH,
                            data$sht21_RH)
        
        xlabString <- "Date" 
        ylabString <- expression(paste("RH [%]"))
        legend_str <- c("RH REF","RH SHT")
        plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(0,110),xlabString,ylabString,legend_str)
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_RH_TS_DAYS.pdf",sep="")
        plot_ts(figname,data$date,yyy,"day",NULL,c(0,110),xlabString,ylabString,legend_str)
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_RH_TS_WEEKS.pdf",sep="")
        plot_ts(figname,data$date,yyy,"week",NULL,c(0,110),xlabString,ylabString,legend_str)
      }
      
      # AH
      
      if(T){
        figname    <- paste(plotdir_allModels,"/",sensor_descriptor,"_AH_TS.pdf",sep="")
        
        yyy        <- cbind(data$AH,
                            data$sht21_AH)
        
        xlabString <- "Date" 
        ylabString <- expression(paste("Absolute humidity [g/m^3]"))
        legend_str <- c("AH REF","AH SHT")
        plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(0,15),xlabString,ylabString,legend_str)
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_AH_TS_DAYS.pdf",sep="")
        plot_ts(figname,data$date,yyy,"day",NULL,c(0,15),xlabString,ylabString,legend_str)
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_AH_TS_WEEKS.pdf",sep="")
        plot_ts(figname,data$date,yyy,"week",NULL,c(0,15),xlabString,ylabString,legend_str)
      }
      
      # H2O
      
      if(T){
        figname    <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_TS.pdf",sep="")
        
        yyy        <- cbind(data$H2O,
                            data$H2O_COMP,
                            data$sht21_H2O_COMP)
        
        xlabString <- "Date" 
        ylabString <- expression(paste("Water [Vol-%]"))
        legend_str <- c("H2O REF","H2O COMP", "H2O COMP SHT")
        plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(0,3),xlabString,ylabString,legend_str)
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_TS_DAYS.pdf",sep="")
        plot_ts(figname,data$date,yyy,"day",NULL,c(0,3),xlabString,ylabString,legend_str)
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_TS_WEEKS.pdf",sep="")
        plot_ts(figname,data$date,yyy,"week",NULL,c(0,3),xlabString,ylabString,legend_str)
      }
      
      # pressure
      
      if(T){
        figname    <- paste(plotdir_allModels,"/",sensor_descriptor,"_pressure_TS.pdf",sep="")
        yyy        <- cbind(data$pressure)
        xlabString <- "Date" 
        ylabString <- expression(paste("Pressure [hPa]"))
        legend_str <- c("Pressure")
        plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(750,1050),xlabString,ylabString,legend_str)
      }
      
      # Wind speed
      
      if(F){
        figname    <- paste(plotdir_allModels,"/",sensor_descriptor,"_windspeed_TS_WEEKS.pdf",sep="")
        yyy        <- cbind(data$WIGE1)
        xlabString <- "Date" 
        ylabString <- expression(paste("Wind speed [m/s]"))
        legend_str <- c("Wind speed")
        plot_ts(figname,data$date,yyy,"week",NULL,c(0,15),xlabString,ylabString,legend_str)
      }
      
      # Radiation
      
      if(F){
        figname    <- paste(plotdir_allModels,"/",sensor_descriptor,"_radiation_TS_WEEKS.pdf",sep="")
        yyy        <- cbind(data$STRGLO)
        xlabString <- "Date"
        ylabString <- expression(paste("Radiation [W/m2]"))
        legend_str <- c("Radiation")
        plot_ts(figname,data$date,yyy,"week",NULL,c(0,1400),xlabString,ylabString,legend_str)
      }
    }
    
    
    
    
    ## Comparison of selected parameters
    
    if(T){
      
      comparison_info <- NULL
      
      comparison_info <- rbind(comparison_info,data.frame(factor_1="lp8_T_m",  unit_1="[deg C]" ,factor_2="lp8_T_l",  unit_2="[deg C]",one2one=T))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="sht21_T_m",unit_1="[deg C]" ,factor_2="sht21_T_l",unit_2="[deg C]",one2one=T))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="lp8_ir_m", unit_1="[XX]" ,   factor_2="lp8_ir_l", unit_2="[XX]",   one2one=T))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="T",        unit_1="[deg C]", factor_2="lp8_T",    unit_2="[deg C]",one2one=T))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="T",        unit_1="[deg C]", factor_2="sht21_T",  unit_2="[deg C]",one2one=T))
      
      comparison_info <- rbind(comparison_info,data.frame(factor_1="CO2",     unit_1="[ppm]",    factor_2="lp8_CO2",  unit_2="[ppm]"  ,one2one=T))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="sht21_T", unit_1="[deg C]",  factor_2="lp8_T",    unit_2="[deg C]",one2one=T))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="RH",      unit_1="[%]",      factor_2="sht21_RH", unit_2="[%]"    ,one2one=T))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="DP",      unit_1="[deg C]",  factor_2="sht21_DP", unit_2="[deg C]",one2one=T))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="RH",      unit_1="[%]"    ,  factor_2="AH",       unit_2="[g/m^3]",one2one=F))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="sht21_RH",unit_1="[%]"    ,  factor_2="sht21_AH", unit_2="[g/m^3]",one2one=F))
      
      comparison_info <- rbind(comparison_info,data.frame(factor_1="CO2",     unit_1="[ppm]"  ,  factor_2="sht21_T",  unit_2="[deg C]",one2one=F))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="CO2",     unit_1="[ppm]"  ,  factor_2="sht21_RH", unit_2="[%]"    ,one2one=F))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="CO2",     unit_1="[ppm]",    factor_2="H2O",      unit_2="[Vol-%]",one2one=F))
      
      comparison_info <- rbind(comparison_info,data.frame(factor_1="AH",      unit_1="[g/m^3]",  factor_2="sht21_AH", unit_2="[g/m^3]",  one2one=T))
  
      
      date_str         <- "Data: "
      for(ii in 1:n_id_cal_periods){
        if(ii>1){
          date_str <- paste(date_str, paste("      "),sep="")
        }
        
        date_str <- paste(date_str, paste(strftime(sensor_calibration_info$Date_UTC_from[id_cal_periods[ii]],"%d/%m/%y",tz="UTC"),"-",strftime(sensor_calibration_info$Date_UTC_to[id_cal_periods[ii]],"%d/%m/%y",tz="UTC")),sep="")
        
        if(ii<n_id_cal_periods){
          date_str <- paste(date_str,"\n",sep="")
        }
      }
      
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_COMPARISON_RAW_DATA.pdf",sep="")
      
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
      
      for(ith_cmp in 1:dim(comparison_info)[1]){
        
        pos_factor_1 <- which(colnames(data)==comparison_info$factor_1[ith_cmp])
        pos_factor_2 <- which(colnames(data)==comparison_info$factor_2[ith_cmp])
        
        id_ok        <- which(!is.na(data[,pos_factor_1]) & !is.na(data[,pos_factor_2]))
        
        pointColors  <- 1
        
        if(comparison_info$factor_1[ith_cmp]=="lp8_T_m" & comparison_info$factor_2[ith_cmp]=="lp8_T_l"){
          id_ok       <- which(!is.na(data[,pos_factor_1]) & !is.na(data[,pos_factor_2]) & data$date>=date_UTC_LP8_SU_upgrade_01)
          pointColors <- rep(1,length(id_ok))
          pointColors[which(data$delta_lp8_T[id_ok]<0)] <- 4
          pointColors[which(data$delta_lp8_T[id_ok]>0)] <- 2
        }
        if(comparison_info$factor_1[ith_cmp]=="sht21_T_m" & comparison_info$factor_2[ith_cmp]=="sht21_T_l"){
          id_ok       <- which(!is.na(data[,pos_factor_1]) & !is.na(data[,pos_factor_2]) & data$date>=date_UTC_LP8_SU_upgrade_01)
          pointColors <- rep(1,length(id_ok))
          pointColors[which(data$delta_sht21_T[id_ok]<0)] <- 4
          pointColors[which(data$delta_sht21_T[id_ok]>0)] <- 2
        }
        if(comparison_info$factor_1[ith_cmp]=="lp8_ir_m" & comparison_info$factor_2[ith_cmp]=="lp8_ir_l"){
          id_ok       <- which(!is.na(data[,pos_factor_1]) & !is.na(data[,pos_factor_2]) & data$date>=date_UTC_LP8_SU_upgrade_01)
          pointColors <- rep(1,length(id_ok))
          pointColors[which(data$delta_lp8_ir[id_ok]<0)] <- 4
          pointColors[which(data$delta_lp8_ir[id_ok]>0)] <- 2
        }
        
        n_id_ok     <- length(id_ok)
        
        if(n_id_ok<10){
          next
        }
        
        
        if(comparison_info$one2one[ith_cmp]){
          xrange <- range(c(data[id_ok,pos_factor_1],data[id_ok,pos_factor_2]))
          yrange <- xrange
        }else{
          xrange <- range(data[id_ok,pos_factor_1])
          yrange <- range(data[id_ok,pos_factor_2])
        }
        
        fit_cmp     <- lm(y~x,data.frame(x=data[id_ok,pos_factor_1],y=data[id_ok,pos_factor_2]))
        corCoef     <- cor(x=data[id_ok,pos_factor_1],y=data[id_ok,pos_factor_2],use="complete.obs",method="pearson")
        
        xlabString  <- paste(comparison_info$factor_1[ith_cmp],comparison_info$unit_1[ith_cmp])
        ylabString  <- paste(comparison_info$factor_2[ith_cmp],comparison_info$unit_2[ith_cmp])
        mainString  <- paste(sensor_descriptor,"[",paste("y = ",sprintf("%.2f",fit_cmp$coefficients[2]),"x + ",sprintf("%.2f",fit_cmp$coefficients[1]),sep=""),"/",paste("COR = ",sprintf("%.2f",corCoef),"]",sep="")) 
        
        plot(  data[id_ok,pos_factor_1], data[id_ok,pos_factor_2] ,xlim=xrange,ylim=yrange,xlab=xlabString,ylab=ylabString,main=mainString,pch=16,cex=0.5,cex.axis=1.5,cex.lab=1.5,col=1)
        if(comparison_info$one2one[ith_cmp]){
          lines(c(-1e4,1e4),c(-1e4,1e4),lwd=1,lty=1,col=1)
        }
        lines(c(-1e6,1e6),c(fit_cmp$coefficients[1]-1e6*fit_cmp$coefficients[2],fit_cmp$coefficients[1]+1e6*fit_cmp$coefficients[2]),col=2,lwd=1,lty=5)
        
        par(family="mono")
        text(xrange[1]+0.8*(xrange[2]-xrange[1]),yrange[1]+0.10*(yrange[2]-yrange[1]),date_str)
        par(family="")
      }
      
      par(def_par)
      dev.off()
      
    }
    
    # ------------------------------------------------------------------
    
    ## Sensor calibration
    
    if(CV_mode==0){
      n_temp_data_selections <- 0
    }

    ## --- Loop over temporal data selections (CV)
    
    for(ith_temp_data_selections in 0:n_temp_data_selections){
      
      CV_P <- sprintf("%02.0f",ith_temp_data_selections)
      
      if(CV_mode==0 | ith_temp_data_selections==0){
        data_training        <- rep(T,dim(data)[1])
        data_test            <- rep(T,dim(data)[1])
      }
      
      # ------------------------------------------------------------------
      
      
      ## Check/filter sensor measurements
      
      measurement_ok <- rep(T,dim(data)[1])
      
      if(T){
        
        ts_secs        <- as.numeric(difftime(time1=data$date,time2=data$date[1],tz="UTC",units="secs"))
        delta_time     <- c(0,diff(ts_secs))
        delta_LP8_IR   <- c(0,diff(data$lp8_ir))
        delta_sht21_RH <- c(0,diff(data$sht21_RH))
        delta_sht21_T  <- c(0,diff(data$sht21_T))
        
        # Filtering 0 : based on sensor state
        
        if(T){
          
          measurement_ok <- measurement_ok & !is.na(data$sht21_RH) & data$sht21_RH <= max_RH_4_cal
          
          measurement_ok <- measurement_ok & !is.na(data$sht21_RH) & (data$CalMode!=2 | (data$CalMode==2 & data$sht21_RH<=70))
          
          # subject to be optimized !!
          measurement_ok <- measurement_ok & (data$CalMode%in%c(2,3) | (data$CalMode==1 & (data$diff_sht21_T_DP>=5 
                                                                                           | data$diff_T_SHT_LP8 < -0.2 | data$diff_T_SHT_LP8 > 0.7 
                                                                                           | data$sht21_RH < 75
                                                                                           | data$d_lp8_T < -0.05 | data$d_lp8_T > 0.05)))
        }
        
        # Filtering 1 : based on change of sensor state: effects particular measurement only
        
        if(T){
          
          id_set_to_F    <- which((is.na(delta_time) | is.na(delta_LP8_IR) | is.na(delta_sht21_RH))
                                  | (delta_time>500 & delta_time<700 & abs(delta_sht21_RH)>5)
                                  | (delta_time>500 & delta_time<700 & abs(delta_LP8_IR)>400 & data$CalMode==1)
                                  | (delta_time>500 & delta_time<700 & abs(delta_LP8_IR)>200 & data$CalMode==2))
          
          
          n_id_set_to_F <- length(id_set_to_F)
          
          if(n_id_set_to_F>0){
            measurement_ok[id_set_to_F] <- F
          }
          
          rm(id_set_to_F,n_id_set_to_F)
          gc()
          
        }
        
        #
        
        # Filtering 2 : based on change of sensor state: effects also consecutive measurements
        
        if(T){
          
          id_bad    <- NULL
          hours_bad <- NULL
          
          id_new    <- which(delta_time>500 & delta_time<700 & abs(delta_LP8_IR)>400 & data$CalMode==2 & !is.na(delta_time) & !is.na(delta_LP8_IR) & !is.na(delta_sht21_RH))
          if(length(id_new)>0){
            id_bad    <- c(id_bad,id_new)
            hours_bad <- c(hours_bad,rep(2,length(id_new)))
          }
          
          id_new    <- which(delta_time>500 & delta_time<700 & abs(delta_LP8_IR)>800 & data$CalMode==2 & !is.na(delta_time) & !is.na(delta_LP8_IR) & !is.na(delta_sht21_RH))
          if(length(id_new)>0){
            id_bad    <- c(id_bad,id_new)
            hours_bad <- c(hours_bad,rep(4,length(id_new)))
          }
          
          id_new    <- which(delta_time>500 & delta_time<700 & abs(delta_LP8_IR)>2500 & data$CalMode==2 & !is.na(delta_time) & !is.na(delta_LP8_IR) & !is.na(delta_sht21_RH))
          if(length(id_new)>0){
            id_bad    <- c(id_bad,id_new)
            hours_bad <- c(hours_bad,rep(8,length(id_new)))
          }
          
          id_new    <- which(delta_time>500 & delta_time<700 & abs(delta_sht21_RH)>5  & data$CalMode==2 & !is.na(delta_time) & !is.na(delta_LP8_IR) & !is.na(delta_sht21_RH))
          if(length(id_new)>0){
            id_bad    <- c(id_bad,id_new)
            hours_bad <- c(hours_bad,rep(2,length(id_new)))
          }
          
          n_id_bad <- length(id_bad)
          
          if(n_id_bad>0){
            oo        <- order(id_bad)
            id_bad    <- id_bad[oo]
            hours_bad <- hours_bad[oo]
          }
          
          if(n_id_bad>0){
            for(ith_id_bad in 1:n_id_bad){
              id_set_to_F   <- which(data$days>=data$days[id_bad[ith_id_bad]] & data$days<(data$days[id_bad[ith_id_bad]]+hours_bad[ith_id_bad]/24))
              n_id_set_to_F <- length(id_set_to_F)
              
              if(n_id_set_to_F>0){
                measurement_ok[id_set_to_F] <- F
              }
            }
          }
        }
        
        #
        
        rm(delta_time,delta_LP8_IR,id_set_to_F,n_id_set_to_F,id_bad,n_id_bad,oo)
        
      }
      
      # ------------------------------------------------------------------
      
      # data selection
      # --------------
      
      use4cal <- rep(T,dim(data)[1])
      
      # Data completeness
      use4cal <- use4cal &  !is.na(data$CO2)
      use4cal <- use4cal &  !is.na(data$pressure)
      
      use4cal <- use4cal &  !is.na(data$lp8_CO2)
      use4cal <- use4cal &  !is.na(data$lp8_ir)
      use4cal <- use4cal &  !is.na(data$lp8_T)
      use4cal <- use4cal &  !is.na(data$sht21_T)
      use4cal <- use4cal &  !is.na(data$sht21_RH)
      
      
      # Measurements
      use4cal <- use4cal & measurement_ok
      
      # training data / test data
      
      id_use4cal        <- which(use4cal & data_training)
      n_id_use4cal      <- length(id_use4cal)
      
      id_use4pred       <- which(use4cal & data_test)
      n_id_use4pred     <- length(id_use4pred)
      
      id_use4pred_OP    <- which(use4cal & data_test)
      n_id_use4pred_OP  <- length(id_use4pred_OP)
      
      
      # 
      
      N_data_01               <- length(which(data$CalMode==1))
      N_data_02               <- length(which(data$CalMode==2))
      N_data_03               <- length(which(data$CalMode==3))
      
      N_data_01_use4cal       <- length(which(data$CalMode==1 & use4cal))
      N_data_02_use4cal       <- length(which(data$CalMode==2 & use4cal))
      N_data_03_use4cal       <- length(which(data$CalMode==3 & use4cal))
      
      #
      
      if(n_id_use4cal<144 | n_id_use4pred<20){
        next
      }
      
      ##
      
      data$lp8_ir_orig       <- data$lp8_ir
      data$lp8_logIR_orig    <- data$lp8_logIR
      
      ##
      
      id_use4cal_orig        <- id_use4cal
      id_use4pred_orig       <- id_use4pred
      id_use4pred_OP_orig    <- id_use4pred_OP
      
      
      # ------------------------------------------------------------------
      
      
      for(ith_sensor_model in 1:length(sensor_models)){
        
        ##
        
        data$lp8_ir       <- data$lp8_ir_orig
        data$lp8_logIR    <- data$lp8_logIR_orig
        
        id_keep           <- which(!colnames(data)%in%paste("CONST",sprintf("%02.0f",1:99),sep=""))
        data              <- data[,id_keep]
        
        ##
        
        id_use4cal        <- id_use4cal_orig 
        id_use4pred       <- id_use4pred_orig        
        id_use4pred_OP    <- id_use4pred_OP_orig
        
        n_id_use4cal      <- length(id_use4cal_orig)
        n_id_use4pred     <- length(id_use4pred_orig)
        n_id_use4pred_OP  <- length(id_use4pred_OP_orig)
        
        ##
        
        if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","LM_CO2")){
          
          formula_str <- sensor_models[[ith_sensor_model]]$formula
          
          
          ## --> CONSTM/CONSTRIR/CONSTIR
          if(length(grep(pattern="CONSTM",    x=sensor_models[[ith_sensor_model]]$name))>0
             | length(grep(pattern="CONSTIR", x=sensor_models[[ith_sensor_model]]$name))>0
             | length(grep(pattern="CONSTRIR",x=sensor_models[[ith_sensor_model]]$name))>0){
            
            breaks_from    <- NULL
            breaks_to      <- NULL
            breaks_CalMode <- NULL
            
            # include one dIR for 14 days or cc/pc calibration
            for(ith_id_cal_period in 1:n_id_cal_periods){
              
              if(sensor_calibration_info$CalMode[id_cal_periods[ith_id_cal_period]]%in%c(2,3)){
                breaks_from    <- c(breaks_from,    sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]])
                breaks_to      <- c(breaks_to,      sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]])
                breaks_CalMode <- c(breaks_CalMode, sensor_calibration_info$CalMode[id_cal_periods[ith_id_cal_period]])
              }
              
              if(sensor_calibration_info$CalMode[id_cal_periods[ith_id_cal_period]]%in%c(1)){
                
                n_int       <- round((sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]]-sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]])/(86400*14))
                
                if(n_int < 1){
                  n_int <- 1
                }
                
                breaks      <- seq(sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]],
                                   sensor_calibration_info$timestamp_to[  id_cal_periods[ith_id_cal_period]],
                                   length.out=n_int+1)
                
                breaks_from    <- c(breaks_from,breaks[1:n_int])
                breaks_to      <- c(breaks_to,  breaks[2:(n_int+1)])
                breaks_CalMode <- c(breaks_CalMode,rep(1,n_int))
              }
            }
            
            breaks_CalMode <- breaks_CalMode[ order(breaks_to)]
            breaks_from    <- breaks_from[    order(breaks_to)]
            breaks_to      <- breaks_to[      order(breaks_to)]
            n_int          <- length(breaks_from)
            
            # climate chamber (2) / pressure chamber (3) calibrations partly partitioned in database (e.g. calibration interrupted by weekend) -> resolve
            
            for(ith_break in 2:n_int){
              if(breaks_from[ith_break] - breaks_from[ith_break-1] < 12*86400 & breaks_CalMode[ith_break]==2 & breaks_CalMode[ith_break-1]==2){
                breaks_from[ith_break]   <- breaks_from[ith_break-1]
                
                breaks_from[ith_break-1] <- NA
                breaks_to[ith_break-1]   <- NA
              }
              if(breaks_from[ith_break] - breaks_from[ith_break-1] < 12*86400 & breaks_CalMode[ith_break]==3 & breaks_CalMode[ith_break-1]==3){
                breaks_from[ith_break]   <- breaks_from[ith_break-1]
                
                breaks_from[ith_break-1] <- NA
                breaks_to[ith_break-1]   <- NA
              }
            }
            
            id_keep        <- which(!is.na(breaks_from) & !is.na(breaks_to))
            breaks_CalMode <- breaks_CalMode[id_keep]
            breaks_from    <- breaks_from[   id_keep]
            breaks_to      <- breaks_to[     id_keep]
            n_int          <- length(breaks_from)
            
            # if(length(which(breaks_CalMode==3))>1){
            #   print("Problem with PC")
            # }
            
            # for(iii in 1:n_int){
            #   print(paste(strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+breaks_from[iii],"%Y-%m-%d %H:%M:%S",tz="UTC")," - ",strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+breaks_to[iii],"%Y-%m-%d %H:%M:%S",tz="UTC"),sep=""))
            # }
            
            #
            
            pos            <- NULL
            ii_start       <- 1
            
            for(ii in 1:n_int){
              id_a <- which(data$timestamp            >=breaks_from[ii] & data$timestamp            <breaks_to[ii])
              id   <- which(data$timestamp[id_use4cal]>=breaks_from[ii] & data$timestamp[id_use4cal]<breaks_to[ii])
              n_id <- length(id)
              
              if(ii==ii_start & n_id==0){
                ii_start <- ii + 1
              }
              
              if(ii>ii_start){
                if(n_id>0){
                  colName         <- paste("DELTA",sprintf("%02.0f",ii),sep="")
                  data$NCol       <- 0
                  data$NCol[id_a] <- 1
                  colnames(data)  <- c(colnames(data)[1:(dim(data)[2]-1)],colName)
                  
                  formula_str     <- paste(formula_str,"+I(",colName,")",sep="")
                  pos             <- c(pos,which(colnames(data)==colName))
                  
                  
                  CalibrationModelInfo <- rbind(CalibrationModelInfo,data.frame(SensorUnit_ID = SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],
                                                                                sensor        = sensors2cal[ith_sensor2cal],
                                                                                sensor_model  = sensor_models[[ith_sensor_model]]$name,
                                                                                ParameterName = colName,
                                                                                Timestamp_from= breaks_from[ii],
                                                                                Timestamp_to  = breaks_to[ii],
                                                                                Date_UTC_from = strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+breaks_from[ii],"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                                                Date_UTC_to   = strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+breaks_to[ii],  "%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                                                stringsAsFactors = F))
                }
              }
            }
            
            rm(pos)
            gc()
          }
          
          # fit
          
          fit <- rlm(as.formula(formula_str),data[id_use4cal,],psi=psi.huber,k=1.345)
          # fit <- lm(as.formula(formula_str),data[id_use4cal,])
          
          max_par          <- 150
          n_par            <- length(fit$coefficients)
          parameters       <- rep(NA,max_par)
          if(n_par>max_par){
            stop("Number of parameters exceeds maximum number of parameters.")
          }else{
            parameters[1:n_par] <- as.numeric(fit$coefficients)
          }
          
          # model predictions
          
          tmp                    <- predict(fit,data,se.fit=T)
          CO2_predicted          <- tmp$fit
          CO2_predicted_se       <- tmp$se.fit
          
          CO2_predicted_ppm      <- CO2_predicted * 1013.25 / data$pressure * (data$lp8_T+273.15) / 273.15
          
          rm(tmp)
          gc()
          
          
          # ANALYSIS : Residuals vs SHT21_RH / difference T and DP --> SHT21_RH / diff_sht21_T_DP THRESHOLDS
          
          SHT21_RH_max_data   <- -999
          SHT21_T_DP_min_data <- -999
          
          if(ith_temp_data_selections==0 & any(data$CalMode==1)){
            
            #
            
            plotdir <- paste(resultdir,sensor_models[[ith_sensor_model]]$name,sep="")
            if(!dir.exists(plotdir)){
              dir.create((plotdir))
            }
            
            #
            
            residuals <- CO2_predicted - data$CO2
            
            #
            
            RES_SHT21_RH_STAT           <- as.data.frame(matrix(NA,ncol=13,nrow=50))
            RES_RH_STAT                 <- as.data.frame(matrix(NA,ncol=13,nrow=50))
            colnames(RES_SHT21_RH_STAT) <- c("RH_min","N","mean","sd","Q000","Q001","Q005","Q010","Q050","Q090","Q095","Q099","Q100")
            colnames(RES_RH_STAT)       <- c("RH_min","N","mean","sd","Q000","Q001","Q005","Q010","Q050","Q090","Q095","Q099","Q100")
            
            for(ii in 1:50){
              
              id <- which(data$CalMode==1
                          & !is.na(data$sht21_RH)
                          & !is.na(residuals)
                          & data$sht21_RH>=(ii-1)*2 & data$sht21_RH<2*ii)
              
              n_id <- length(id)
              
              if(n_id>=50){
                RES_SHT21_RH_STAT$RH_min[ii] <- (ii-1)*2
                RES_SHT21_RH_STAT$N[ii]      <- n_id
                RES_SHT21_RH_STAT$mean[ii]   <- mean(residuals[id])
                RES_SHT21_RH_STAT$sd[ii]     <- sd(residuals[id])
                
                RES_SHT21_RH_STAT$Q000[ii]   <- quantile(residuals[id],probs=0.00)
                RES_SHT21_RH_STAT$Q001[ii]   <- quantile(residuals[id],probs=0.01)
                RES_SHT21_RH_STAT$Q005[ii]   <- quantile(residuals[id],probs=0.05)
                RES_SHT21_RH_STAT$Q010[ii]   <- quantile(residuals[id],probs=0.10)
                RES_SHT21_RH_STAT$Q050[ii]   <- quantile(residuals[id],probs=0.50)
                RES_SHT21_RH_STAT$Q090[ii]   <- quantile(residuals[id],probs=0.90)
                RES_SHT21_RH_STAT$Q095[ii]   <- quantile(residuals[id],probs=0.95)
                RES_SHT21_RH_STAT$Q099[ii]   <- quantile(residuals[id],probs=0.99)
                RES_SHT21_RH_STAT$Q100[ii]   <- quantile(residuals[id],probs=1.00)
              }
              
              #
              
              id <- which(data$CalMode==1
                          & !is.na(data$RH)
                          & !is.na(residuals)
                          & data$RH>=(ii-1)*2 & data$RH<2*ii)
              
              n_id <- length(id)
              
              if(n_id>=50){
                RES_RH_STAT$RH_min[ii] <- (ii-1)*2
                RES_RH_STAT$N[ii]      <- n_id
                RES_RH_STAT$mean[ii]   <- mean(residuals[id])
                RES_RH_STAT$sd[ii]     <- sd(residuals[id])
                
                RES_RH_STAT$Q000[ii]   <- quantile(residuals[id],probs=0.00)
                RES_RH_STAT$Q001[ii]   <- quantile(residuals[id],probs=0.01)
                RES_RH_STAT$Q005[ii]   <- quantile(residuals[id],probs=0.05)
                RES_RH_STAT$Q010[ii]   <- quantile(residuals[id],probs=0.10)
                RES_RH_STAT$Q050[ii]   <- quantile(residuals[id],probs=0.50)
                RES_RH_STAT$Q090[ii]   <- quantile(residuals[id],probs=0.90)
                RES_RH_STAT$Q095[ii]   <- quantile(residuals[id],probs=0.95)
                RES_RH_STAT$Q099[ii]   <- quantile(residuals[id],probs=0.99)
                RES_RH_STAT$Q100[ii]   <- quantile(residuals[id],probs=1.00)
              }
            }
            
            #
            
            SD_40_70   <- median(RES_SHT21_RH_STAT$sd[21:35],na.rm=T)
            id_Q095_ok <- which(RES_SHT21_RH_STAT$Q095<=3*SD_40_70)
            if(length(id_Q095_ok)>=1){
              SHT21_RH_max_data <- 2*max((0:49)[which(RES_SHT21_RH_STAT$Q095<=3*SD_40_70)]) + 2
            }else{
              SHT21_RH_max_data <- -999
            }
            
            # --
            
            RES_SHT21_T_DP_STAT           <- as.data.frame(matrix(NA,ncol=13,nrow=100))
            RES_T_DP_STAT                 <- as.data.frame(matrix(NA,ncol=13,nrow=100))
            colnames(RES_SHT21_T_DP_STAT) <- c("T_DP_min","N","mean","sd","Q000","Q001","Q005","Q010","Q050","Q090","Q095","Q099","Q100")
            colnames(RES_T_DP_STAT)       <- c("T_DP_min","N","mean","sd","Q000","Q001","Q005","Q010","Q050","Q090","Q095","Q099","Q100")
            
            for(ii in 1:100){
              
              id <- which(data$CalMode==1
                          & !is.na(data$diff_sht21_T_DP)
                          & !is.na(residuals)
                          & data$diff_sht21_T_DP>=(ii-1)/2 & data$diff_sht21_T_DP<ii/2)
              
              n_id <- length(id)
              
              if(n_id>=50){
                RES_SHT21_T_DP_STAT$T_DP_min[ii] <- (ii-1)/2
                RES_SHT21_T_DP_STAT$N[ii]        <- n_id
                RES_SHT21_T_DP_STAT$mean[ii]     <- mean(residuals[id])
                RES_SHT21_T_DP_STAT$sd[ii]       <- sd(residuals[id])
                
                RES_SHT21_T_DP_STAT$Q000[ii]     <- quantile(residuals[id],probs=0.00)
                RES_SHT21_T_DP_STAT$Q001[ii]     <- quantile(residuals[id],probs=0.01)
                RES_SHT21_T_DP_STAT$Q005[ii]     <- quantile(residuals[id],probs=0.05)
                RES_SHT21_T_DP_STAT$Q010[ii]     <- quantile(residuals[id],probs=0.10)
                RES_SHT21_T_DP_STAT$Q050[ii]     <- quantile(residuals[id],probs=0.50)
                RES_SHT21_T_DP_STAT$Q090[ii]     <- quantile(residuals[id],probs=0.90)
                RES_SHT21_T_DP_STAT$Q095[ii]     <- quantile(residuals[id],probs=0.95)
                RES_SHT21_T_DP_STAT$Q099[ii]     <- quantile(residuals[id],probs=0.99)
                RES_SHT21_T_DP_STAT$Q100[ii]     <- quantile(residuals[id],probs=1.00)
              }
              
              id <- which(data$CalMode==1
                          & !is.na(data$diff_T_DP)
                          & !is.na(residuals)
                          & data$diff_T_DP>=(ii-1)/2 & data$diff_T_DP<ii/2)
              
              n_id <- length(id)
              
              if(n_id>=50){
                RES_T_DP_STAT$T_DP_min[ii] <- (ii-1)/2
                RES_T_DP_STAT$N[ii]        <- n_id
                RES_T_DP_STAT$mean[ii]     <- mean(residuals[id])
                RES_T_DP_STAT$sd[ii]       <- sd(residuals[id])
                
                RES_T_DP_STAT$Q000[ii]     <- quantile(residuals[id],probs=0.00)
                RES_T_DP_STAT$Q001[ii]     <- quantile(residuals[id],probs=0.01)
                RES_T_DP_STAT$Q005[ii]     <- quantile(residuals[id],probs=0.05)
                RES_T_DP_STAT$Q010[ii]     <- quantile(residuals[id],probs=0.10)
                RES_T_DP_STAT$Q050[ii]     <- quantile(residuals[id],probs=0.50)
                RES_T_DP_STAT$Q090[ii]     <- quantile(residuals[id],probs=0.90)
                RES_T_DP_STAT$Q095[ii]     <- quantile(residuals[id],probs=0.95)
                RES_T_DP_STAT$Q099[ii]     <- quantile(residuals[id],probs=0.99)
                RES_T_DP_STAT$Q100[ii]     <- quantile(residuals[id],probs=1.00)
              }
            }
            
            #
            
            SD_5_15    <- median(RES_SHT21_T_DP_STAT$sd[11:30],na.rm=T)
            id_Q095_ok <- which(RES_SHT21_T_DP_STAT$Q095<=3*SD_5_15)
            if(length(id_Q095_ok)>=1){
              SHT21_T_DP_min_data <- (min((1:100)[which(RES_SHT21_T_DP_STAT$Q095<=3*SD_5_15)])-1)/2
            }else{
              SHT21_T_DP_min_data <- -999
            }
            
            #
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_RES_SHT21_RH_STAT.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
            
            plot(c(0,100),c(0,0),xlim=c(0,100),ylim=c(-50,50),t="l",lwd=1,col="gray50",xlab="RH SHT21/REF [%]", ylab=expression(paste("CO"[2]*" pred - CO"[2]*" REF [ppm]")),cex.lab=1.75,cex.axis=1.75)
            
            lines((0:49)*2,RES_RH_STAT$Q050,col=1,lwd=1,lty=5)
            lines((0:49)*2,RES_RH_STAT$Q095,col=1,lwd=1,lty=5)
            lines((0:49)*2,RES_RH_STAT$Q005,col=1,lwd=1,lty=5)
            
            lines((0:49)*2,RES_SHT21_RH_STAT$Q050,col=2,lwd=1,lty=1)
            lines((0:49)*2,RES_SHT21_RH_STAT$Q095,col=2,lwd=1,lty=1)
            lines((0:49)*2,RES_SHT21_RH_STAT$Q005,col=2,lwd=1,lty=1)
            
            lines(c(SHT21_RH_max_data,SHT21_RH_max_data),c(-1e4,1e4),col=4,lwd=2,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c("SHT21","REF"),lty=c(1,5),col=c(2,1),bg="white")
            par(family="")
            
            dev.off()
            par(def_par)
            
            #
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_RES_SHT21_T_DP_STAT.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
            
            plot(c(0,40),c(0,0),xlim=c(0,21),ylim=c(-50,50),t="l",lwd=1,col="gray50",xlab="DP-T SHT21/REF [deg C]", ylab=expression(paste("CO"[2]*" pred - CO"[2]*" REF [ppm]")),cex.lab=1.75,cex.axis=1.75)
            
            lines((0:99)/2,RES_SHT21_T_DP_STAT$Q050,col=1,lwd=1,lty=5)
            lines((0:99)/2,RES_SHT21_T_DP_STAT$Q095,col=1,lwd=1,lty=5)
            lines((0:99)/2,RES_SHT21_T_DP_STAT$Q005,col=1,lwd=1,lty=5)
            
            lines((0:99)/2,RES_T_DP_STAT$Q050,col=2,lwd=1,lty=1)
            lines((0:99)/2,RES_T_DP_STAT$Q095,col=2,lwd=1,lty=1)
            lines((0:99)/2,RES_T_DP_STAT$Q005,col=2,lwd=1,lty=1)
            
            lines(c(SHT21_T_DP_min_data,SHT21_T_DP_min_data),c(-1e4,1e4),col=4,lwd=2,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c("SHT21","REF"),lty=c(1,5),col=c(2,1),bg="white")
            par(family="")
            
            dev.off()
            par(def_par)
            
          }
          
          
          
          # write sensor model parameters into database
          
          MODELS_INSERT_DB <- c("CO2_MODEL_pLIN_c0_P_PIR_CONSTIR",
                                "CO2_MODEL_pLIN_c0_TIR_P_CONSTIR",
                                "CO2_MODEL_pLIN_c0_P_CONSTIR") 
          
          if(T & sensor_models[[ith_sensor_model]]$name %in% MODELS_INSERT_DB & 
             ((CV_mode==0 & ith_temp_data_selections==0) | (CV_mode==5 & ith_temp_data_selections==1))) {
            
            
            CMN <- sensor_models[[ith_sensor_model]]$name
            # CMN <- paste("TILL_01Dec2017_",sensor_models[[ith_sensor_model]]$name,sep="")
            
            query_str       <- paste("DELETE FROM `CalibrationParameters` WHERE Type = 'LP8' and Serialnumber = '",sensors2cal[ith_sensor2cal],"' and Mode = ",CV_mode," and CalibrationModelName = '",CMN,"';",sep="");
            
            drv             <- dbDriver("MySQL")
            con <-carboutil::get_conn( group="CarboSense_MySQL")
            res             <- dbSendQuery(con, query_str)
            tbl_calibration <- dbFetch(res, n=-1)
            dbClearResult(res)
            dbDisconnect(con)
            
            #
            
            INDEX <- sqrt(sum((CO2_predicted[id_use4cal] - data$CO2[id_use4cal])^2)/n_id_use4cal)
            
            #
            
            parameters2DB <- parameters
            id_NA         <- which(is.na(parameters2DB))
            if(length(id_NA)>0){
              parameters2DB[id_NA] <- 0
            }
            
            #
            
            Data_timestamp_from  <- min(sensor_calibration_info$timestamp_from[id_cal_periods])
            Data_timestamp_to    <- max(sensor_calibration_info$timestamp_to[id_cal_periods])
            Computation_date_UTC <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S",tz="UTC")
            
            CMN <- sensor_models[[ith_sensor_model]]$name
            # CMN <- paste("TILL_01Dec2017_",sensor_models[[ith_sensor_model]]$name,sep="")
            

            query_str       <- paste("INSERT INTO `CalibrationParameters`(`Type`, `Serialnumber`, `CalibrationModelName`, `Data_timestamp_from`, `Data_timestamp_to`,`Mode`,`Index`, `N_PAR`,`Computation_date_UTC`,`PAR_00`, `PAR_01`, `PAR_02`, `PAR_03`, `PAR_04`, `PAR_05`, `PAR_06`, `PAR_07`, `PAR_08`, `PAR_09`, `PAR_10`, `PAR_11`, `PAR_12`, `PAR_13`, `PAR_14`, `PAR_15`, `PAR_16`, `PAR_17`, `PAR_18`, `PAR_19`,`PAR_20`, `PAR_21`, `PAR_22`, `PAR_23`, `PAR_24`, `PAR_25`, `PAR_26`, `PAR_27`, `PAR_28`, `PAR_29`,`PAR_30`, `PAR_31`, `PAR_32`, `PAR_33`, `PAR_34`, `PAR_35`, `PAR_36`, `PAR_37`, `PAR_38`, `PAR_39`,`PAR_40`, `PAR_41`, `PAR_42`, `PAR_43`, `PAR_44`, `PAR_45`, `PAR_46`, `PAR_47`, `PAR_48`, `PAR_49`,`PAR_50`, `PAR_51`, `PAR_52`, `PAR_53`, `PAR_54`, `PAR_55`, `PAR_56`, `PAR_57`, `PAR_58`, `PAR_59`,`PAR_60`, `PAR_61`, `PAR_62`, `PAR_63`, `PAR_64`, `PAR_65`, `PAR_66`, `PAR_67`, `PAR_68`, `PAR_69`,`PAR_70`, `PAR_71`, `PAR_72`, `PAR_73`, `PAR_74`, `PAR_75`, `PAR_76`, `PAR_77`, `PAR_78`, `PAR_79`,`PAR_80`, `PAR_81`, `PAR_82`, `PAR_83`, `PAR_84`, `PAR_85`, `PAR_86`, `PAR_87`, `PAR_88`, `PAR_89`,`PAR_90`, `PAR_91`, `PAR_92`, `PAR_93`, `PAR_94`, `PAR_95`, `PAR_96`, `PAR_97`, `PAR_98`, `PAR_99`,`PAR_100`, `PAR_101`, `PAR_102`, `PAR_103`, `PAR_104`, `PAR_105`, `PAR_106`, `PAR_107`, `PAR_108`, `PAR_109`,`PAR_110`, `PAR_111`, `PAR_112`, `PAR_113`, `PAR_114`, `PAR_115`, `PAR_116`, `PAR_117`, `PAR_118`, `PAR_119`,`PAR_120`, `PAR_121`, `PAR_122`, `PAR_123`, `PAR_124`, `PAR_125`, `PAR_126`, `PAR_127`, `PAR_128`, `PAR_129`,`PAR_130`, `PAR_131`, `PAR_132`, `PAR_133`, `PAR_134`, `PAR_135`, `PAR_136`, `PAR_137`, `PAR_138`, `PAR_139`,`PAR_140`, `PAR_141`, `PAR_142`, `PAR_143`, `PAR_144`, `PAR_145`, `PAR_146`, `PAR_147`, `PAR_148`, `PAR_149`,`SHT21_RH_max`,`SHT21_T_DP_min`) ",sep="")
            query_str       <- paste(query_str, paste("VALUES ('",sensor_calibration_info$Type[1],"','",sensors2cal[ith_sensor2cal],"','",CMN,"',",Data_timestamp_from,",",Data_timestamp_to,",",CV_mode,",",ceiling(INDEX),",",n_par,",'",Computation_date_UTC,"',",
                                                      parameters2DB[01],",",parameters2DB[02],",",parameters2DB[03],",",parameters2DB[04],",",parameters2DB[05],",",parameters2DB[06],",",parameters2DB[07],",",parameters2DB[08],",",parameters2DB[09],",",parameters2DB[10],",",
                                                      parameters2DB[11],",",parameters2DB[12],",",parameters2DB[13],",",parameters2DB[14],",",parameters2DB[15],",",parameters2DB[16],",",parameters2DB[17],",",parameters2DB[18],",",parameters2DB[19],",",parameters2DB[20],",",
                                                      parameters2DB[21],",",parameters2DB[22],",",parameters2DB[23],",",parameters2DB[24],",",parameters2DB[25],",",parameters2DB[26],",",parameters2DB[27],",",parameters2DB[28],",",parameters2DB[29],",",parameters2DB[30],",",
                                                      parameters2DB[31],",",parameters2DB[32],",",parameters2DB[33],",",parameters2DB[34],",",parameters2DB[35],",",parameters2DB[36],",",parameters2DB[37],",",parameters2DB[38],",",parameters2DB[39],",",parameters2DB[40],",",
                                                      parameters2DB[41],",",parameters2DB[42],",",parameters2DB[43],",",parameters2DB[44],",",parameters2DB[45],",",parameters2DB[46],",",parameters2DB[47],",",parameters2DB[48],",",parameters2DB[49],",",parameters2DB[50],",",
                                                      parameters2DB[51],",",parameters2DB[52],",",parameters2DB[53],",",parameters2DB[54],",",parameters2DB[55],",",parameters2DB[56],",",parameters2DB[57],",",parameters2DB[58],",",parameters2DB[59],",",parameters2DB[60],",",
                                                      parameters2DB[61],",",parameters2DB[62],",",parameters2DB[63],",",parameters2DB[64],",",parameters2DB[65],",",parameters2DB[66],",",parameters2DB[67],",",parameters2DB[68],",",parameters2DB[69],",",parameters2DB[70],",",
                                                      parameters2DB[71],",",parameters2DB[72],",",parameters2DB[73],",",parameters2DB[74],",",parameters2DB[75],",",parameters2DB[76],",",parameters2DB[77],",",parameters2DB[78],",",parameters2DB[79],",",parameters2DB[80],",",
                                                      parameters2DB[81],",",parameters2DB[82],",",parameters2DB[83],",",parameters2DB[84],",",parameters2DB[85],",",parameters2DB[86],",",parameters2DB[87],",",parameters2DB[88],",",parameters2DB[89],",",parameters2DB[90],",",
                                                      parameters2DB[91],",",parameters2DB[92],",",parameters2DB[93],",",parameters2DB[94],",",parameters2DB[95],",",parameters2DB[96],",",parameters2DB[97],",",parameters2DB[98],",",parameters2DB[99],",",parameters2DB[100],",",
                                                      parameters2DB[101],",",parameters2DB[102],",",parameters2DB[103],",",parameters2DB[104],",",parameters2DB[105],",",parameters2DB[106],",",parameters2DB[107],",",parameters2DB[108],",",parameters2DB[109],",",parameters2DB[110],",",
                                                      parameters2DB[111],",",parameters2DB[112],",",parameters2DB[113],",",parameters2DB[114],",",parameters2DB[115],",",parameters2DB[116],",",parameters2DB[117],",",parameters2DB[118],",",parameters2DB[119],",",parameters2DB[120],",",
                                                      parameters2DB[121],",",parameters2DB[122],",",parameters2DB[123],",",parameters2DB[124],",",parameters2DB[125],",",parameters2DB[126],",",parameters2DB[127],",",parameters2DB[128],",",parameters2DB[129],",",parameters2DB[130],",",
                                                      parameters2DB[131],",",parameters2DB[132],",",parameters2DB[133],",",parameters2DB[134],",",parameters2DB[135],",",parameters2DB[136],",",parameters2DB[137],",",parameters2DB[138],",",parameters2DB[139],",",parameters2DB[140],",",
                                                      parameters2DB[141],",",parameters2DB[142],",",parameters2DB[143],",",parameters2DB[144],",",parameters2DB[145],",",parameters2DB[146],",",parameters2DB[147],",",parameters2DB[148],",",parameters2DB[149],",",parameters2DB[150],",",SHT21_RH_max_data,",",SHT21_T_DP_min_data,");",sep=""))
            
            
            drv             <- dbDriver("MySQL")
            con <-carboutil::get_conn( group="CarboSense_MySQL")
            res             <- dbSendQuery(con, query_str)
            tbl_calibration <- dbFetch(res, n=-1)
            dbClearResult(res)
            dbDisconnect(con)
            
            rm(id_NA,parameters2DB)
            gc()
            
          }
          
        }
        
        
        
        # Statistics
        
        residuals_FIT     <- CO2_predicted[id_use4cal] - data$CO2[id_use4cal]
        RMSE_FIT          <- sqrt(sum((residuals_FIT)^2)/n_id_use4cal)
        COR_COEF_FIT      <- cor(x = CO2_predicted[id_use4cal],y=data$CO2[id_use4cal],method="pearson",use="complete.obs")
        
        residuals_INI     <- data$lp8_CO2[id_use4cal] - data$CO2[id_use4cal]
        RMSE_INI          <- sqrt(sum((residuals_INI)^2)/n_id_use4cal)
        COR_COEF_INI      <- cor(x = data$lp8_CO2[id_use4cal],y=data$CO2[id_use4cal],method="pearson",use="complete.obs")
        
        residuals_PRED    <- CO2_predicted[id_use4pred] - data$CO2[id_use4pred]
        RMSE_PRED         <- sqrt(sum((residuals_PRED)^2)/n_id_use4pred)
        COR_COEF_PRED     <- cor(x = CO2_predicted[id_use4pred],y=data$CO2[id_use4pred],method="pearson",use="complete.obs")
        
        residuals_PRED_ppm<- CO2_predicted_ppm[id_use4pred] - data$CO2_ppm[id_use4pred]
        RMSE_PRED_ppm     <- sqrt(sum((residuals_PRED_ppm)^2)/n_id_use4pred)
        COR_COEF_PRED_ppm <- cor(x = CO2_predicted_ppm[id_use4pred],y=data$CO2_ppm[id_use4pred],method="pearson",use="complete.obs")
        
        residuals_PRED_OP <- CO2_predicted[id_use4pred_OP] - data$CO2[id_use4pred_OP]
        RMSE_PRED_OP      <- sqrt(sum((residuals_PRED_OP)^2)/n_id_use4pred_OP)
        COR_COEF_PRED_OP  <- cor(x = CO2_predicted[id_use4pred_OP],y=data$CO2[id_use4pred_OP],method="pearson",use="complete.obs")
        
        Q000_FIT          <- quantile(residuals_FIT,probs=0.00)
        Q001_FIT          <- quantile(residuals_FIT,probs=0.01)
        Q005_FIT          <- quantile(residuals_FIT,probs=0.05)
        Q010_FIT          <- quantile(residuals_FIT,probs=0.10)
        Q050_FIT          <- quantile(residuals_FIT,probs=0.50)
        Q090_FIT          <- quantile(residuals_FIT,probs=0.90)
        Q095_FIT          <- quantile(residuals_FIT,probs=0.95)
        Q099_FIT          <- quantile(residuals_FIT,probs=0.99)
        Q100_FIT          <- quantile(residuals_FIT,probs=1.00)
        
        Q000_PRED         <- quantile(residuals_PRED,probs=0.00)
        Q001_PRED         <- quantile(residuals_PRED,probs=0.01)
        Q005_PRED         <- quantile(residuals_PRED,probs=0.05)
        Q010_PRED         <- quantile(residuals_PRED,probs=0.10)
        Q050_PRED         <- quantile(residuals_PRED,probs=0.50)
        Q090_PRED         <- quantile(residuals_PRED,probs=0.90)
        Q095_PRED         <- quantile(residuals_PRED,probs=0.95)
        Q099_PRED         <- quantile(residuals_PRED,probs=0.99)
        Q100_PRED         <- quantile(residuals_PRED,probs=1.00)
        
        Q000_PRED_ppm     <- quantile(residuals_PRED_ppm,probs=0.00)
        Q001_PRED_ppm     <- quantile(residuals_PRED_ppm,probs=0.01)
        Q005_PRED_ppm     <- quantile(residuals_PRED_ppm,probs=0.05)
        Q010_PRED_ppm     <- quantile(residuals_PRED_ppm,probs=0.10)
        Q050_PRED_ppm     <- quantile(residuals_PRED_ppm,probs=0.50)
        Q090_PRED_ppm     <- quantile(residuals_PRED_ppm,probs=0.90)
        Q095_PRED_ppm     <- quantile(residuals_PRED_ppm,probs=0.95)
        Q099_PRED_ppm     <- quantile(residuals_PRED_ppm,probs=0.99)
        Q100_PRED_ppm     <- quantile(residuals_PRED_ppm,probs=1.00)
        
        Q000_PRED_OP      <- quantile(residuals_PRED_OP,probs=0.00)
        Q001_PRED_OP      <- quantile(residuals_PRED_OP,probs=0.01)
        Q005_PRED_OP      <- quantile(residuals_PRED_OP,probs=0.05)
        Q010_PRED_OP      <- quantile(residuals_PRED_OP,probs=0.10)
        Q050_PRED_OP      <- quantile(residuals_PRED_OP,probs=0.50)
        Q090_PRED_OP      <- quantile(residuals_PRED_OP,probs=0.90)
        Q095_PRED_OP      <- quantile(residuals_PRED_OP,probs=0.95)
        Q099_PRED_OP      <- quantile(residuals_PRED_OP,probs=0.99)
        Q100_PRED_OP      <- quantile(residuals_PRED_OP,probs=1.00)
        
        
        # 
        
        statistics <- rbind(statistics,
                            data.frame(SensorUnit=SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],
                                       sensor=sensors2cal[ith_sensor2cal],
                                       sensor_model = sensor_models[[ith_sensor_model]]$name,
                                       CALP=CV_P,
                                       CAL_01 = any(data$CalMode[id_use4cal]==1),
                                       CAL_02 = any(data$CalMode[id_use4cal]==2),
                                       CAL_03 = any(data$CalMode[id_use4cal]==3),
                                       date_FIT_UTC_from=strftime(min(data$date[id_use4cal]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       date_FIT_UTC_to  =strftime(max(data$date[id_use4cal]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       date_PRED_UTC_from=strftime(min(data$date[id_use4pred]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       date_PRED_UTC_to  =strftime(max(data$date[id_use4pred]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       n_FIT     = n_id_use4cal,
                                       n_PRED    = n_id_use4pred,
                                       n_PRED_OP = n_id_use4pred_OP,
                                       RMSE_FIT      = RMSE_FIT,
                                       COR_FIT       = COR_COEF_FIT,
                                       RMSE_PRED     = RMSE_PRED,
                                       COR_PRED      = COR_COEF_PRED,
                                       RMSE_PRED_ppm = RMSE_PRED_ppm,
                                       COR_PRED_ppm  = COR_COEF_PRED_ppm,
                                       RMSE_PRED_OP  = RMSE_PRED_OP,
                                       COR_PRED_OP   = COR_COEF_PRED_OP,
                                       SHT21_RH_max_data=SHT21_RH_max_data,
                                       par00=parameters[01],
                                       par01=parameters[02],
                                       par02=parameters[03],
                                       par03=parameters[04],
                                       par04=parameters[05],
                                       par05=parameters[06],
                                       par06=parameters[07],
                                       par07=parameters[08],
                                       par08=parameters[09],
                                       par09=parameters[10],
                                       par10=parameters[11],
                                       par11=parameters[12],
                                       par12=parameters[13],
                                       par13=parameters[14],
                                       par14=parameters[15],
                                       par15=parameters[16],
                                       par16=parameters[17],
                                       par17=parameters[18],
                                       par18=parameters[19],
                                       par19=parameters[20],
                                       par20=parameters[21],
                                       par21=parameters[22],
                                       par22=parameters[23],
                                       par23=parameters[24],
                                       par24=parameters[25],
                                       par25=parameters[26],
                                       par26=parameters[27],
                                       par27=parameters[28],
                                       par28=parameters[29],
                                       par29=parameters[30],
                                       par30=parameters[31],
                                       par31=parameters[32],
                                       par32=parameters[33],
                                       par33=parameters[34],
                                       par34=parameters[35],
                                       par35=parameters[36],
                                       par36=parameters[37],
                                       par37=parameters[38],
                                       par38=parameters[39],
                                       par39=parameters[40],
                                       par40=parameters[41],
                                       par41=parameters[42],
                                       par42=parameters[43],
                                       par43=parameters[44],
                                       par44=parameters[45],
                                       par45=parameters[46],
                                       par46=parameters[47],
                                       par47=parameters[48],
                                       par48=parameters[49],
                                       par49=parameters[50],
                                       par50=parameters[51],
                                       par51=parameters[52],
                                       par52=parameters[53],
                                       par53=parameters[54],
                                       par54=parameters[55],
                                       par55=parameters[56],
                                       par56=parameters[57],
                                       par57=parameters[58],
                                       par58=parameters[59],
                                       par59=parameters[60],
                                       par60=parameters[61],
                                       par61=parameters[62],
                                       par62=parameters[63],
                                       par63=parameters[64],
                                       par64=parameters[65],
                                       par65=parameters[66],
                                       par66=parameters[67],
                                       par67=parameters[68],
                                       par68=parameters[69],
                                       par69=parameters[70],
                                       par70=parameters[71],
                                       par71=parameters[72],
                                       par72=parameters[73],
                                       par73=parameters[74],
                                       par74=parameters[75],
                                       par75=parameters[76],
                                       par76=parameters[77],
                                       par77=parameters[78],
                                       par78=parameters[79],
                                       par79=parameters[80],
                                       par80=parameters[81],
                                       par81=parameters[82],
                                       par82=parameters[83],
                                       par83=parameters[84],
                                       par84=parameters[85],
                                       par85=parameters[86],
                                       par86=parameters[87],
                                       par87=parameters[88],
                                       par88=parameters[89],
                                       par89=parameters[90],
                                       par90=parameters[91],
                                       par91=parameters[92],
                                       par92=parameters[93],
                                       par93=parameters[94],
                                       par94=parameters[95],
                                       par95=parameters[96],
                                       par96=parameters[97],
                                       par97=parameters[98],
                                       par98=parameters[99],
                                       par99=parameters[100],
                                       par100=parameters[101],
                                       par101=parameters[102],
                                       par102=parameters[103],
                                       par103=parameters[104],
                                       par104=parameters[105],
                                       par105=parameters[106],
                                       par106=parameters[107],
                                       par107=parameters[108],
                                       par108=parameters[109],
                                       par109=parameters[110],
                                       par110=parameters[111],
                                       par111=parameters[112],
                                       par112=parameters[113],
                                       par113=parameters[114],
                                       par114=parameters[115],
                                       par115=parameters[116],
                                       par116=parameters[117],
                                       par117=parameters[118],
                                       par118=parameters[119],
                                       par119=parameters[120],
                                       par120=parameters[121],
                                       par121=parameters[122],
                                       par122=parameters[123],
                                       par123=parameters[124],
                                       par124=parameters[125],
                                       par125=parameters[126],
                                       par126=parameters[127],
                                       par127=parameters[128],
                                       par128=parameters[129],
                                       par129=parameters[130],
                                       par130=parameters[131],
                                       par131=parameters[132],
                                       par132=parameters[133],
                                       par133=parameters[134],
                                       par134=parameters[135],
                                       par135=parameters[136],
                                       par136=parameters[137],
                                       par137=parameters[138],
                                       par138=parameters[139],
                                       par139=parameters[140],
                                       par140=parameters[141],
                                       par141=parameters[142],
                                       par142=parameters[143],
                                       par143=parameters[144],
                                       par144=parameters[145],
                                       par145=parameters[146],
                                       par146=parameters[147],
                                       par147=parameters[148],
                                       par148=parameters[149],
                                       par149=parameters[150],
                                       CO2_min_FIT      = min(data$CO2[id_use4cal]),
                                       CO2_max_FIT      = max(data$CO2[id_use4cal]),
                                       CO2_min_PRED     = min(data$CO2[id_use4pred]),
                                       CO2_max_PRED     = max(data$CO2[id_use4pred]),
                                       T_min_FIT        = min(data$T[id_use4cal]),
                                       T_max_FIT        = max(data$T[id_use4cal]),
                                       T_min_PRED       = min(data$T[id_use4pred]),
                                       T_max_PRED       = max(data$T[id_use4pred]),
                                       T_sht21_min_FIT  = min(data$sht21_T[id_use4cal]),
                                       T_sht21_max_FIT  = max(data$sht21_T[id_use4cal]),
                                       T_sht21_min_PRED = min(data$sht21_T[id_use4pred]),
                                       T_sht21_max_PRED = max(data$sht21_T[id_use4pred]),
                                       pressure_min_FIT = min(data$pressure[id_use4cal]),
                                       pressure_max_FIT = max(data$pressure[id_use4cal]),
                                       pressure_min_PRED = min(data$pressure[id_use4pred]),
                                       pressure_max_PRED = max(data$pressure[id_use4pred]),
                                       N_01               = N_data_01,
                                       N_01_use4cal       = N_data_01_use4cal,
                                       N_02               = N_data_02,
                                       N_02_use4cal       = N_data_02_use4cal,
                                       N_03               = N_data_03,
                                       N_03_use4cal       = N_data_03_use4cal,
                                       Q001_FIT           = Q001_FIT,
                                       Q005_FIT           = Q005_FIT,
                                       Q095_FIT           = Q095_FIT,
                                       Q099_FIT           = Q099_FIT,
                                       Q001_PRED          = Q001_PRED,
                                       Q005_PRED          = Q005_PRED,
                                       Q095_PRED          = Q095_PRED,
                                       Q099_PRED          = Q099_PRED,
                                       stringsAsFactors = F))
        
        
        
        
        ## Plot : SCATTER RAW OBS - REF OBS / FITTED OBS - REF_OBS
        
        if(T){
          
          plotdir <- paste(resultdir,sensor_models[[ith_sensor_model]]$name,sep="")
          if(!dir.exists(plotdir)){
            dir.create((plotdir))
          }
          
          sensor_descriptor <- paste("SU", SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],"_S",sensors2cal[ith_sensor2cal],sep="")
          
          
          # SCATTER CAL
          
          if(T){
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=16, height=16, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5),mfrow=c(2,2))
            
            # P1
            
            if(sensor_models[[ith_sensor_model]]$modelType %in% c("SM_CO2","LM_CO2")){
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," CO2 [ppm]",sep="")
              
              plot(data$CO2,data$lp8_CO2,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.75,cex.axis=1.75)
            }
            
            if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","SM_IR")){
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," IR [xx]",sep="")
              
              plot(data$CO2,data$lp8_ir,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.75,cex.axis=1.75)
            }
            
            # P2
            
            if(ith_temp_data_selections==0){
              
              if(sensor_models[[ith_sensor_model]]$modelType %in% c("SM_CO2","LM_CO2")){
                str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_INI))
                str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_INI))
                str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
                leg_vec    <- c(str21,str22,str23)
                
                xrange     <- range(c(data$CO2[id_use4cal],data$lp8_CO2[id_use4cal]))
                yrange     <- xrange
                xlabString <- paste("PIC CO2 [ppm]",sep="")
                ylabString <- paste(sensor_descriptor," CO2 [ppm]",sep="")
                
                plot(data$CO2[id_use4cal],data$lp8_CO2[id_use4cal],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.75,cex.axis=1.75)
              }
              
              if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","SM_IR")){
                # str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_INI))
                # str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_INI))
                str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
                leg_vec    <- c(str23)
                
                xrange     <- range(c(data$CO2[id_use4cal],data$lp8_CO2[id_use4cal]))
                yrange     <- xrange
                xlabString <- paste("PIC CO2 [ppm]",sep="")
                ylabString <- paste(sensor_descriptor," IR [xx]",sep="")
                
                plot(data$CO2[id_use4cal],data$lp8_ir[id_use4cal],pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.75,cex.axis=1.75)
              }
              
              
            }else{
              
              str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_FIT))
              str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_FIT))
              str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
              leg_vec    <- c(str21,str22,str23)
              
              xrange     <- range(c(data$CO2[id_use4cal],CO2_predicted[id_use4cal]))
              yrange     <- xrange
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," CO2 [ppm] (CAL)",sep="")
              subString  <- paste("Data:",strftime(min(data$date[id_use4cal]),"%d/%m/%Y %H:%M",tz="UTC"),"-",strftime(max(data$date[id_use4cal]),"%d/%m/%Y %H:%M",tz="UTC"))
              
              plot(data$CO2[id_use4cal],CO2_predicted[id_use4cal],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.75,cex.axis=1.75,cex.sub=1.5)
              
            }
            
            lines(c(0,1e5),c(0,1e5),   col=2,lwd=1)
            lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
            lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=leg_vec,bg="white",cex=1.75)
            par(family="")
            
            # P3
            
            str31 <- paste("RMSE:", sprintf("%6.2f",RMSE_PRED))
            str32 <- paste("COR: ", sprintf("%6.2f",COR_COEF_PRED))
            str33 <- paste("N:   ", sprintf("%6.0f",n_id_use4pred))
            
            xlabString <- paste("PIC CO2 [ppm]",sep="")
            ylabString <- paste(sensor_descriptor," CO2 predicted [ppm]",sep="")
            subString  <- paste("Data:",strftime(min(data$date[id_use4pred]),"%d/%m/%Y %H:%M",tz="UTC"),"-",strftime(max(data$date[id_use4pred]),"%d/%m/%Y %H:%M",tz="UTC"))
            
            plot(data$CO2[id_use4pred], CO2_predicted[id_use4pred],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.75,cex.axis=1.75,cex.sub=1.5)
            lines(c(0,1e5),c(0,1e5),col=2,lwd=1)
            lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
            lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c(str31,str32,str33),bg="white",cex=1.75)
            par(family="")
            
            # P4
            
            str41 <- paste("Q000:",sprintf("%6.2f",Q000_PRED))
            str42 <- paste("Q001:",sprintf("%6.2f",Q001_PRED))
            str43 <- paste("Q005:",sprintf("%6.2f",Q005_PRED))
            str44 <- paste("Q050:",sprintf("%6.2f",Q050_PRED))
            str45 <- paste("Q095:",sprintf("%6.2f",Q095_PRED))
            str46 <- paste("Q099:",sprintf("%6.2f",Q099_PRED))
            str47 <- paste("Q100:",sprintf("%6.2f",Q100_PRED))
            
            xlabString     <- paste(sensor_descriptor," CO2 predicted - PIC CO2 [ppm]",sep="")
            residuals_plot <- residuals_PRED[which(abs(residuals_PRED)<=1e3)]
            
            hist(residuals_plot,breaks = seq(-1e3,1e3,2.5),xlim=c(-50,50),col="slategray",xlab=xlabString,main="",cex.lab=1.75,cex.axis=1.75)
            lines(c(  0,  0),c(-1e7,1e7),col=2,lwd=1)
            lines(c( 20, 20),c(-1e7,1e7),col=2,lwd=1,lty=5)
            lines(c(-20,-20),c(-1e7,1e7),col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c(str41,str42,str43,str44,str45,str46,str47,str33),bg="white",cex=1.75)
            par(family="")
            
            #
            
            dev.off()
            par(def_par)
          }
          
          # SCATTER CAL PPM
          
          if(T){
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER_PPM.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=16, height=16, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5),mfrow=c(2,2))
            
            # P1
            
            if(sensor_models[[ith_sensor_model]]$modelType %in% c("SM_CO2","LM_CO2")){
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," CO2 [ppm]",sep="")
              
              plot(data$CO2_ppm,data$lp8_CO2,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.75,cex.axis=1.75)
            }
            
            if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","SM_IR")){
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," IR [xx]",sep="")
              
              plot(data$CO2_ppm,data$lp8_ir,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.75,cex.axis=1.75)
            }
            
            # P2
            
            if(ith_temp_data_selections==0){
              
              if(sensor_models[[ith_sensor_model]]$modelType %in% c("SM_CO2","LM_CO2")){
                str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_INI))
                str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_INI))
                str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
                leg_vec    <- c(str21,str22,str23)
                
                xrange     <- range(c(data$CO2_ppm[id_use4cal],data$lp8_CO2[id_use4cal]))
                yrange     <- xrange
                xlabString <- paste("PIC CO2 [ppm]",sep="")
                ylabString <- paste(sensor_descriptor," CO2 [ppm]",sep="")
                
                plot(data$CO2_ppm[id_use4cal],data$lp8_CO2[id_use4cal],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.75,cex.axis=1.75)
              }
              
              if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","SM_IR")){
                # str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_INI))
                # str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_INI))
                str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
                leg_vec    <- c(str23)
                
                xrange     <- range(c(data$CO2_ppm[id_use4cal],data$lp8_CO2[id_use4cal]))
                yrange     <- xrange
                xlabString <- paste("PIC CO2 [ppm]",sep="")
                ylabString <- paste(sensor_descriptor," IR [xx]",sep="")
                
                plot(data$CO2_ppm[id_use4cal],data$lp8_ir[id_use4cal],pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.75,cex.axis=1.75)
              }
              
              
            }else{
              
              str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_FIT))
              str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_FIT))
              str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
              leg_vec    <- c(str21,str22,str23)
              
              xrange     <- range(c(data$CO2[id_use4cal],CO2_predicted[id_use4cal]))
              yrange     <- xrange
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," CO2 [ppm] (CAL)",sep="")
              subString  <- paste("Data:",strftime(min(data$date[id_use4cal]),"%d/%m/%Y %H:%M",tz="UTC"),"-",strftime(max(data$date[id_use4cal]),"%d/%m/%Y %H:%M",tz="UTC"))
              
              plot(data$CO2_ppm[id_use4cal],CO2_predicted_ppm[id_use4cal],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.75,cex.axis=1.75,cex.sub=1.5)
              
            }
            
            lines(c(0,1e5),c(0,1e5),   col=2,lwd=1)
            lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
            lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topright",legend=leg_vec,bg="white",cex=1.75)
            par(family="")
            
            # P3
            
            str31 <- paste("RMSE:", sprintf("%6.2f",RMSE_PRED_ppm))
            str32 <- paste("COR: ", sprintf("%6.2f",COR_COEF_PRED_ppm))
            str33 <- paste("N:   ", sprintf("%6.0f",n_id_use4pred))
            
            xlabString <- paste("PIC CO2 [ppm]",sep="")
            ylabString <- paste(sensor_descriptor," CO2 predicted [ppm]",sep="")
            subString  <- paste("Data:",strftime(min(data$date[id_use4pred]),"%d/%m/%Y %H:%M",tz="UTC"),"-",strftime(max(data$date[id_use4pred]),"%d/%m/%Y %H:%M",tz="UTC"))
            
            plot(data$CO2_ppm[id_use4pred], CO2_predicted_ppm[id_use4pred],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.75,cex.axis=1.75,cex.sub=1.5)
            lines(c(0,1e5),c(0,1e5),col=2,lwd=1)
            lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
            lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c(str31,str32,str33),bg="white",cex=1.75)
            par(family="")
            
            # P4
            
            str41 <- paste("Q000:",sprintf("%6.2f",Q000_PRED_ppm))
            str42 <- paste("Q001:",sprintf("%6.2f",Q001_PRED_ppm))
            str43 <- paste("Q005:",sprintf("%6.2f",Q005_PRED_ppm))
            str44 <- paste("Q050:",sprintf("%6.2f",Q050_PRED_ppm))
            str45 <- paste("Q095:",sprintf("%6.2f",Q095_PRED_ppm))
            str46 <- paste("Q099:",sprintf("%6.2f",Q099_PRED_ppm))
            str47 <- paste("Q100:",sprintf("%6.2f",Q100_PRED_ppm))
            
            xlabString         <- paste(sensor_descriptor," CO2 predicted - PIC CO2 [ppm]",sep="")
            residuals_plot_ppm <- residuals_PRED[which(abs(residuals_PRED_ppm)<=1e3)]
            
            hist(residuals_plot_ppm,breaks = seq(-1e3,1e3,2.5),xlim=c(-50,50),col="slategray",xlab=xlabString,main="",cex.lab=1.75,cex.axis=1.75)
            lines(c(  0,  0),c(-1e7,1e7),col=2,lwd=1)
            lines(c( 20, 20),c(-1e7,1e7),col=2,lwd=1,lty=5)
            lines(c(-20,-20),c(-1e7,1e7),col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c(str41,str42,str43,str44,str45,str46,str47,str33),bg="white",cex=1.75)
            par(family="")
            
            #
            
            dev.off()
            par(def_par)
          }
          
          # SCATTER CAL PPM (for AMT paper ...)
          
          if(T){
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER_PPM_02.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5))
            
            
            if(ith_temp_data_selections==0){
              
              if(sensor_models[[ith_sensor_model]]$modelType %in% c("SM_CO2","LM_CO2")){
                str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_INI))
                str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_INI))
                str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
                leg_vec    <- c(str21,str22,str23)
                
                xrange     <- range(c(data$CO2_ppm[id_use4cal],data$lp8_CO2[id_use4cal]))
                yrange     <- xrange
                xlabString <- paste("PIC CO2 [ppm]",sep="")
                ylabString <- paste(sensor_descriptor," CO2 [ppm]",sep="")
                
                plot(data$CO2_ppm[id_use4cal],data$lp8_CO2[id_use4cal],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.75,cex.axis=1.75)
              }
              
              if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","SM_IR")){
                # str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_INI))
                # str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_INI))
                str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
                leg_vec    <- c(str23)
                
                xrange     <- range(c(data$CO2_ppm[id_use4cal],data$lp8_CO2[id_use4cal]))
                yrange     <- xrange
                xlabString <- expression(paste("Picarro CO"[2]*" [ppm]",sep=""))
                ylabString <- paste(sensor_descriptor," IR [xx]",sep="")
                
                plot(data$CO2_ppm[id_use4cal],data$lp8_ir[id_use4cal],pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.75,cex.axis=1.75)
              }
              
              
            }else{
              
              str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_FIT))
              str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_FIT))
              str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
              leg_vec    <- c(str21,str22,str23)
              
              xrange     <- range(c(data$CO2[id_use4cal],CO2_predicted[id_use4cal]))
              yrange     <- xrange
              xlabString <- expression(paste("Picarro CO"[2]*" [ppm]",sep=""))
              ylabString <- paste(sensor_descriptor," CO2 [ppm] (CAL)",sep="")
              subString  <- paste("Data:",strftime(min(data$date[id_use4cal]),"%d/%m/%Y",tz="UTC"),"-",strftime(max(data$date[id_use4cal]),"%d/%m/%Y",tz="UTC"))
              
              plot(data$CO2_ppm[id_use4cal],CO2_predicted_ppm[id_use4cal],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.75,cex.axis=1.75,cex.sub=1.5)
              
            }
            
            lines(c(0,1e5),c(0,1e5),   col=2,lwd=1)
            lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
            lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topright",legend=leg_vec,bg="white",cex=1.75)
            par(family="")
            
            dev.off()
            par(def_par)
            
            ###
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER_PPM_03.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5))
            
            str31 <- paste("RMSE:", sprintf("%6.2f",RMSE_PRED_ppm))
            str32 <- paste("COR: ", sprintf("%6.2f",COR_COEF_PRED_ppm))
            str33 <- paste("N:   ", sprintf("%6.0f",n_id_use4pred))
            
            xlabString <- expression(paste("Picarro CO"[2]*" [ppm]",sep=""))
            ylabString <- bquote(.(sensor_descriptor) ~ "CO"[2] ~ "predicted [ppm]")
            
            subString  <- paste("Data:",strftime(min(data$date[id_use4pred]),"%d/%m/%Y",tz="UTC"),"-",strftime(max(data$date[id_use4pred]),"%d/%m/%Y",tz="UTC"))
            
            plot(data$CO2_ppm[id_use4pred], CO2_predicted_ppm[id_use4pred],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.75,cex.axis=1.75,cex.sub=1.5)
            lines(c(0,1e5),c(0,1e5),col=2,lwd=1)
            lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
            lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c(str31,str32,str33),bg="white",cex=1.75)
            par(family="")
            
            dev.off()
            par(def_par)
          }
          
          # SCATTER OP
          
          if(T){
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER_OP.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=16, height=16, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5),mfrow=c(2,2))
            
            # P1
            
            if(sensor_models[[ith_sensor_model]]$modelType %in% c("SM_CO2","LM_CO2")){
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," CO2 [ppm]",sep="")
              
              plot(data$CO2,data$lp8_CO2,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.5,cex.axis=1.5)
            }
            
            if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","SM_IR")){
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," IR [xx]",sep="")
              
              plot(data$CO2,data$lp8_ir,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.5,cex.axis=1.5)
            }
            
            # P2
            
            if(ith_temp_data_selections==0){
              
              if(sensor_models[[ith_sensor_model]]$modelType %in% c("SM_CO2","LM_CO2")){
                str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_INI))
                str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_INI))
                str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
                leg_vec    <- c(str21,str22,str23)
                
                xrange     <- range(c(data$CO2[id_use4cal],data$lp8_CO2[id_use4cal]))
                yrange     <- xrange
                xlabString <- paste("PIC CO2 [ppm]",sep="")
                ylabString <- paste(sensor_descriptor," CO2 [ppm]",sep="")
                
                plot(data$CO2[id_use4cal],data$lp8_CO2[id_use4cal],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.5,cex.axis=1.5)
              }
              
              if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","SM_IR")){
                # str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_INI))
                # str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_INI))
                str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
                leg_vec    <- c(str23)
                
                xrange     <- range(c(data$CO2[id_use4cal],data$lp8_CO2[id_use4cal]))
                yrange     <- xrange
                xlabString <- paste("PIC CO2 [ppm]",sep="")
                ylabString <- paste(sensor_descriptor," CO2 [ppm]",sep="")
                
                plot(data$CO2[id_use4cal],data$lp8_ir[id_use4cal],pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.5,cex.axis=1.5)
              }
              
              
            }else{
              
              str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_FIT))
              str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_FIT))
              str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
              leg_vec    <- c(str21,str22,str23)
              
              xrange     <- range(c(data$CO2[id_use4cal],CO2_predicted[id_use4cal]))
              yrange     <- xrange
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," CO2 [ppm] (CAL)",sep="")
              subString  <- paste("Data:",strftime(min(data$date[id_use4cal]),"%d/%m/%Y %H:%M",tz="UTC"),"-",strftime(max(data$date[id_use4cal]),"%d/%m/%Y %H:%M",tz="UTC"))
              
              plot(data$CO2[id_use4cal],CO2_predicted[id_use4cal],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.5,cex.axis=1.5,cex.sub=1.25)
              
            }
            
            lines(c(0,1e5),c(0,1e5),   col=2,lwd=1)
            lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
            lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=leg_vec,bg="white",cex=1.5)
            par(family="")
            
            # P3
            
            str31 <- paste("RMSE:", sprintf("%6.2f",RMSE_PRED_OP))
            str32 <- paste("COR: ", sprintf("%6.2f",COR_COEF_PRED_OP))
            str33 <- paste("N:   ", sprintf("%6.0f",n_id_use4pred_OP))
            
            xlabString <- paste("PIC CO2 [ppm]",sep="")
            ylabString <- paste(sensor_descriptor," CO2 predicted [ppm]",sep="")
            subString  <- paste("Data:",strftime(min(data$date[id_use4pred_OP]),"%d/%m/%Y %H:%M",tz="UTC"),"-",strftime(max(data$date[id_use4pred_OP]),"%d/%m/%Y %H:%M",tz="UTC"))
            
            plot(data$CO2[id_use4pred_OP], CO2_predicted[id_use4pred_OP],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.5,cex.axis=1.5,cex.sub=1.25)
            lines(c(0,1e5),c(0,1e5),col=2,lwd=1)
            lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
            lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c(str31,str32,str33),bg="white",cex=1.5)
            par(family="")
            
            # P4
            
            str41 <- paste("Q000:",sprintf("%6.2f",Q000_PRED_OP))
            str42 <- paste("Q001:",sprintf("%6.2f",Q001_PRED_OP))
            str43 <- paste("Q005:",sprintf("%6.2f",Q005_PRED_OP))
            str44 <- paste("Q050:",sprintf("%6.2f",Q050_PRED_OP))
            str45 <- paste("Q095:",sprintf("%6.2f",Q095_PRED_OP))
            str46 <- paste("Q099:",sprintf("%6.2f",Q099_PRED_OP))
            str47 <- paste("Q100:",sprintf("%6.2f",Q100_PRED_OP))
            
            xlabString     <- paste(sensor_descriptor," CO2 predicted - PIC CO2 [ppm]",sep="")
            residuals_plot <- residuals_PRED_OP[which(abs(residuals_PRED_OP)<=1e3)]
            
            hist(residuals_plot,breaks = seq(-1e3,1e3,2.5),xlim=c(-50,50),col="slategray",xlab=xlabString,main="",cex.lab=1.5,cex.axis=1.5)
            lines(c(  0,  0),c(-1e7,1e7),col=2,lwd=1)
            lines(c( 20, 20),c(-1e7,1e7),col=2,lwd=1,lty=5)
            lines(c(-20,-20),c(-1e7,1e7),col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c(str41,str42,str43,str44,str45,str46,str47,str33),bg="white",cex=1.5)
            par(family="")
            
            #
            
            dev.off()
            par(def_par)
            
          }
          
          # SCATTER FACTORY CAL
          
          if(T){
            
            id_fac_cal_spec_ok <- which(data$lp8_T >= 0 & data$lp8_T <= 50 & data$sht21_RH >= 0 & data$sht21_RH <= 85)
            id_fac_cal_spec_ok <- id_fac_cal_spec_ok[which(id_fac_cal_spec_ok%in%id_use4pred)]
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER_FACTORY_CAL.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
            
            xrange <- range(data$CO2_ppm[id_use4cal],data$lp8_CO2[id_use4pred])
            yrange <- xrange
            
            RMSE_FACTORY_CAL <- sqrt(sum((data$lp8_CO2[id_fac_cal_spec_ok] - data$CO2_ppm[id_fac_cal_spec_ok])^2)/length(id_fac_cal_spec_ok))
            COR_COEF_FACTORY_CAL <- cor(x=data$lp8_CO2[id_fac_cal_spec_ok],y=data$CO2_ppm[id_fac_cal_spec_ok],method = "pearson",use = "complete.obs")
            
            str11 <- paste("RMSE:", sprintf("%6.2f",RMSE_FACTORY_CAL))
            str12 <- paste("COR: ", sprintf("%6.2f",COR_COEF_FACTORY_CAL))
            str13 <- paste("N:   ", sprintf("%6.0f",length(id_fac_cal_spec_ok)))
            
            xlabString <- expression(paste("Picarro CO"[2]*" [ppm]",sep=""))
            ylabString <- bquote(.(sensor_descriptor) ~ "CO"[2] ~ "factory calibration [ppm]")
            subString  <- paste("Data:",strftime(min(data$date[id_use4pred]),"%d/%m/%Y",tz="UTC"),"-",strftime(max(data$date[id_use4pred]),"%d/%m/%Y",tz="UTC"))
            
            plot(data$CO2_ppm[id_use4pred], data$lp8_CO2[id_use4pred],col="gray70",xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.75,cex.axis=1.75,cex.sub=1.5)
            points(data$CO2_ppm[id_fac_cal_spec_ok], data$lp8_CO2[id_fac_cal_spec_ok],col=1,pch=16,cex=0.5)
            
            
            lines(c(0,1e5),c(0,1e5),col=2,lwd=1)
            lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
            lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c(str11,str12,str13),bg="white",cex=1.75)
            par(family="")
            
            #
            
            dev.off()
            par(def_par)
            
            #
            
            rm(fac_cal_spec_ok)
            gc()
          }
          
          
          ## PLOT : Time-series
          
          # CO2
          
          if(T){
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS.pdf",sep="")
            
            tmp_1              <- rep(NA,dim(data)[1])
            tmp_2              <- rep(NA,dim(data)[1])
            tmp_3              <- rep(NA,dim(data)[1])
            
            tmp_1[id_use4cal]  <- CO2_predicted[id_use4cal]
            tmp_2[id_use4pred] <- CO2_predicted[id_use4pred]
            tmp_3[id_use4cal]  <- data$lp8_ir[id_use4cal]
            
            
            yyy1 <- cbind(data$CO2,
                          CO2_predicted,
                          tmp_2,
                          tmp_1)
            
            
            yyy2 <- cbind(data$lp8_ir,
                          tmp_3)
            
            
            
            xlabString  <- "Date" 
            ylabString1 <- expression(paste("CO"[2]*" * p/p0 * T0/T [ppm]"))
            ylabString2 <- expression(paste("IR [XX]"))
            
            legend_str  <- c("PIC CO2",paste(sensor_descriptor,"PRED A"),paste(sensor_descriptor,"PRED"),paste(sensor_descriptor,"4CAL"),paste(sensor_descriptor,"RAW"),paste(sensor_descriptor,"4CAL"))
            
            plot_ts_2YAxes(figname,data$date,yyy1,yyy2,"all_day2day",NULL,c(300,1200),c(28000,45000),xlabString,ylabString1,ylabString2,legend_str)
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS_DAYS.pdf",sep="")
            plot_ts_2YAxes(figname,data$date,yyy1,yyy2,"day",NULL,c(300,1200),c(28000,45000),xlabString,ylabString1,ylabString2,legend_str)
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS_WEEKS.pdf",sep="")
            plot_ts_2YAxes(figname,data$date,yyy1,yyy2,"week",NULL,c(300,1200),c(28000,45000),xlabString,ylabString1,ylabString2,legend_str)
            
            rm(tmp_1,tmp_2,tmp_3,yyy1,yyy2)
            gc()
            
          }
          
          ## PLOT : Time-series
          
          # CO2 : OP
          
          if(T){
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS_OP.pdf",sep="")
            
            tmp_1   <- rep(NA,dim(data)[1])
            tmp_2   <- rep(NA,dim(data)[1])
            tmp_3   <- rep(NA,dim(data)[1])
            tmp_4   <- rep(NA,dim(data)[1])
            
            tmp_1[id_use4cal]     <- CO2_predicted[id_use4cal]
            tmp_2[id_use4pred]    <- CO2_predicted[id_use4pred]
            tmp_3[id_use4cal]     <- data$lp8_ir[id_use4cal]
            tmp_4[id_use4pred_OP] <- CO2_predicted[id_use4pred_OP]
            
            yyy1 <- cbind(data$CO2,
                          tmp_4,
                          tmp_2,
                          tmp_1)
            
            
            yyy2 <- cbind(data$lp8_ir,
                          tmp_3)
            
            
            
            xlabString <- "Date" 
            ylabString1 <- expression(paste("CO"[2]*" * p/p0 * T0/T [ppm]"))
            ylabString2 <- expression(paste("IR [XX]"))
            
            legend_str <- c("PIC CO2",paste(sensor_descriptor,"PRED OP"),paste(sensor_descriptor,"PRED"),paste(sensor_descriptor,"4CAL"),paste(sensor_descriptor,"RAW"),paste(sensor_descriptor,"4CAL"))
            
            plot_ts_2YAxes(figname,data$date,yyy1,yyy2,"all_day2day",NULL,c(300,1200),c(28000,45000),xlabString,ylabString1,ylabString2,legend_str)
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS_DAYS_OP.pdf",sep="")
            plot_ts_2YAxes(figname,data$date,yyy1,yyy2,"day",NULL,c(300,1200),c(28000,45000),xlabString,ylabString1,ylabString2,legend_str)
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS_WEEKS_OP.pdf",sep="")
            plot_ts_2YAxes(figname,data$date,yyy1,yyy2,"week",NULL,c(300,1200),c(28000,45000),xlabString,ylabString1,ylabString2,legend_str)
            
            rm(tmp_1,tmp_2,tmp_3,tmp_4,yyy1,yyy2)
            gc()
            
          }
          
          # CO2 (REF)
          
          if(T){
            
            figname    <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_REF_TS.pdf",sep="")
            
            yyy        <- cbind(data$CO2*((data$lp8_T+273.15)/273.15)*(1013.25/data$pressure))
            
            xlabString <- "Date" 
            ylabString <- expression(paste("CO"[2]*" [ppm]"))
            legend_str <- c("PIC CO2")
            plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(300,1200),xlabString,ylabString,legend_str)
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_REF_TS_WEEKS.pdf",sep="")
            plot_ts(figname,data$date,yyy,"week",NULL,c(300,1200),xlabString,ylabString,legend_str)
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_REF_TS_DAYS.pdf",sep="")
            plot_ts(figname,data$date,yyy,"day",NULL,c(300,1200),xlabString,ylabString,legend_str)
            
            rm(tmp_1,tmp_2)
            gc()
            
          }
          
          # CO2-Residuals
          
          if(T){
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_TS.pdf",sep="")
            
            tmp_1   <- rep(NA,dim(data)[1])
            tmp_2   <- rep(NA,dim(data)[1])
            
            tmp_1[id_use4cal]  <- CO2_predicted[id_use4cal]  - data$CO2[id_use4cal]
            tmp_2[id_use4pred] <- CO2_predicted[id_use4pred] - data$CO2[id_use4pred]
            
            yyy     <- cbind(tmp_1,tmp_2)
            
            xlabString <- "Date" 
            ylabString <- expression(paste("CO"[2]*" PRED - CO"[2]*" REF [ppm]"))
            legend_str <- c("Residuals (FIT)","Residuals (PRED)")
            plot_ts(figname,data$date,yyy,"all_day2day",NULL,NULL,xlabString,ylabString,legend_str)
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_TS_DAYS.pdf",sep="")
            plot_ts(figname,data$date,yyy,"day",NULL,NULL,xlabString,ylabString,legend_str)
            
            rm(tmp_1,tmp_2)
            gc()
            
          }
          
          
          ## PLOT : SCATTER : Residuals vs different factors
          
          if(T){
            if(ith_temp_data_selections == 0){
              
              factors <- data.frame(cn     =c("lp8_CO2","lp8_ir","CO","CO2","gradCO2",  "NO2",  "pressure","days","RH","sht21_RH","AH","sht21_AH","T","sht21_T","lp8_T","diff_T_DP","diff_sht21_T_DP","sht21_gradT","rain","WIGE1","STRGLO","H2O","sht21_H2O_COMP"),
                                    cn_unit=c("[ppm]","[xx]","[ppb]","[ppm]","[ppm / 10 min]","[ppb]","[hPa]","[days]","[%]","[%]","[g/m3]","[g/m3]","[deg C]","[deg C]","[deg C]","[deg C]","[deg C]","[deg C / 10 min]","[mm]","m/s","J/m2","Vol %","Vol %"),
                                    flag   =c(F,F,T,T,F,T,T,F,T,F,T,F,T,F,F,F,F,F,T,T,T,T,F),
                                    stringsAsFactors = F)
              
              
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER_RES_FACTORS.pdf",sep="")
              
              def_par <- par()
              pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
              par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
              
              for(ith_factor in 1:dim(factors)[1]){
                
                pos_factor <- which(colnames(data)==factors$cn[ith_factor])
                
                if(length(pos_factor)==0){
                  next
                }
                
                if(factors$flag[ith_factor]){
                  pos_factor_F <- which(colnames(data)==paste(factors$cn[ith_factor],"_F",sep=""))
                  id_data4cmp  <- which(data[,pos_factor]!= -999 & data[,pos_factor_F] == 1)
                }else{
                  id_data4cmp  <- which(data[,pos_factor]!= -999)
                }
                
                id_res_ok   <- which(!is.na(data$CO2)
                                     & !is.na(data$lp8_CO2))
                
                id_grp_01   <- sort(c(id_data4cmp,id_use4cal),decreasing = F)
                id_grp_01   <- id_grp_01[duplicated(id_grp_01)]
                n_id_grp_01 <- length(id_grp_01)
                
                id_grp_02   <- sort(c(id_data4cmp,id_res_ok),decreasing = F)
                id_grp_02   <- id_grp_02[duplicated(id_grp_02)]
                n_id_grp_02 <- length(id_grp_02)
                
                if(n_id_grp_01>0 & n_id_grp_02>0){
                  
                  xrange <- range(data[id_grp_02,pos_factor])
                  yrange <- range(CO2_predicted[id_grp_02]-data$CO2[id_grp_02])
                  yrange <- c(-50,50)
                  
                  xlabString <- paste(factors$cn[ith_factor],factors$cn_unit[ith_factor])
                  ylabString <- paste(sensor_descriptor,"residual [ppm]")
                  
                  corCoef <- cor(x=data[id_grp_01,pos_factor],y=CO2_predicted[id_grp_01]-data$CO2[id_grp_01],method="pearson",use="complete.obs")
                  fit_res <- lm(y~x,data.frame(x=data[id_grp_01,pos_factor],y=CO2_predicted[id_grp_01]-data$CO2[id_grp_01]))
                  
                  plot(  data[id_grp_02,pos_factor], CO2_predicted[id_grp_02]-data$CO2[id_grp_02],xlim=xrange,ylim=yrange,xlab=xlabString,ylab=ylabString,pch=16,cex=0.5,cex.axis=1.5,cex.lab=1.5,col="grey70")
                  points(data[id_grp_01,pos_factor], CO2_predicted[id_grp_01]-data$CO2[id_grp_01],pch=16,cex=0.5,col=1)
                  
                  lines(c(-1e6,1e6),c(fit_res$coefficients[1]-1e6*fit_res$coefficients[2],fit_res$coefficients[1]+1e6*fit_res$coefficients[2]),col=2,lwd=1,lty=5)
                  lines(c(-1e6,1e6),c(0,0),col=1,lwd=1,lty=5)
                  
                  par(family="mono")
                  legend("topright",legend=c(paste("y=",sprintf("%.2f",fit_res$coefficients[2]),"x+",sprintf("%.1f",fit_res$coefficients[1]),sep=""),
                                             paste("COR: ",sprintf("%5.2f",corCoef),sep="")),
                         bg="white",lty=c(5,NA),col=2)
                  par(family="")
                }
              }
              
              par(def_par)
              dev.off()
            }
          }
          
          ## PLOT : SCATTER : Residuals vs RH
          
          if(T){
            if(ith_temp_data_selections == 0){
              
              factors <- data.frame(cn     =c("sht21_RH"),
                                    cn_unit=c("[%]"),
                                    flag   =c(F),
                                    stringsAsFactors = F)
              
              
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER_RES_HUMIDITY.pdf",sep="")
              
              def_par <- par()
              pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
              par(mai=c(1,1,0.5,0.1),mfrow=c(1,1))
              
              for(ith_factor in 1:dim(factors)[1]){
                
                pos_factor <- which(colnames(data)==factors$cn[ith_factor])
                
                if(length(pos_factor)==0){
                  next
                }
                
                if(factors$flag[ith_factor]){
                  pos_factor_F <- which(colnames(data)==paste(factors$cn[ith_factor],"_F",sep=""))
                  id_data4cmp  <- which(data[,pos_factor]!= -999 & data[,pos_factor_F] == 1)
                }else{
                  id_data4cmp  <- which(data[,pos_factor]!= -999)
                }
                
                id_res_ok   <- which(!is.na(data$CO2)
                                     & !is.na(data$lp8_CO2)
                                     & !is.na(CO2_predicted)
                                     & data$CalMode==1)
                
                
                id_grp_02   <- sort(c(id_data4cmp,id_res_ok),decreasing = F)
                id_grp_02   <- id_grp_02[duplicated(id_grp_02)]
                n_id_grp_02 <- length(id_grp_02)
                
                if(n_id_grp_02>0){
                  
                  xrange <- range(data[id_grp_02,pos_factor])
                  yrange <- range(CO2_predicted[id_grp_02]-data$CO2[id_grp_02])
                  yrange <- c(-50,100)
                  
                  if(factors$cn[ith_factor] == "sht21_RH"){
                    xlabString <- c("SHT21 RH [%]")
                  }else{
                    xlabString <- paste(factors$cn[ith_factor],factors$cn_unit[ith_factor])
                  }
                  
                  if(factors$cn[ith_factor] == "sht21_RH"){
                    ylabString <- "Residuals [ppm]"
                  }else{
                    ylabString <- paste(sensor_descriptor,"residual [ppm]")
                  }
                  
                  mainString <- paste("SU", SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],sep="")
                  
                  subString <- paste("Data:",strftime(min(data$date[id_grp_02]),"%Y-%m-%d",tz="UTC"),"-",strftime(max(data$date[id_grp_02]),"%Y-%m-%d",tz="UTC"))
                  
                  plot(  data[id_grp_02,pos_factor], CO2_predicted[id_grp_02]-data$CO2[id_grp_02],xlim=xrange,ylim=yrange,xlab=xlabString,ylab=ylabString,main=mainString,sub=subString,pch=16,cex=0.5,cex.axis=1.75,cex.lab=1.75,cex.main=1.75,cex.sub=1.5,col=1)
                  lines(c(-1e6,1e6),c(0,0),col="grey40",lwd=1,lty=1)
                  lines(c(SHT21_RH_max_data,SHT21_RH_max_data),c(-1e9,1e9),col=2,lwd=2,lty=5)
                  
                  lines((0:49)*2,RES_SHT21_RH_STAT$Q005,col=2,lwd=1,lty=5)
                  lines((0:49)*2,RES_SHT21_RH_STAT$Q050,col=2,lwd=1,lty=5)
                  lines((0:49)*2,RES_SHT21_RH_STAT$Q095,col=2,lwd=1,lty=5)
                  
                  # lines(c(0,110),3*c(SD_40_70,SD_40_70),col=2,lwd=2,lty=5)
                }
              }
              
              par(def_par)
              dev.off()
            }
          }
          
          ## CO2 vs IR/CO2_LP8/CO2_LP8_PRED
          
          if(F){
            
            if(CV_mode==0){
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2PIC_vs_IR_LP8CO2_LP8CO2PRED.pdf",sep="")
              
              def_par <- par()
              pdf(file = figname, width=18, height=6, onefile=T, pointsize=12, colormodel="srgb")
              par(mai=c(1,1,0.1,0.1),mfrow=c(1,3))
              
              for(ith_plot in 1:3){
                
                id <- which(data$CalMode==ith_plot 
                            & !is.na(CO2_predicted_ppm) 
                            & !is.na(data$lp8_CO2_pcorr) 
                            & !is.na(data$CO2)
                            & !is.na(data$lp8_ir)
                            & use4cal)
                
                n_id <- length(id)
                
                if(n_id>0){
                  
                  RMSE_02 <- sqrt(sum((data$lp8_CO2_pcorr[id]-data$CO2_ppm[id])^2)/n_id)
                  RMSE_03 <- sqrt(sum((CO2_predicted_ppm[id] -data$CO2_ppm[id])^2)/n_id) 
                  
                  CO2_xrange <- range(c(data$lp8_CO2_pcorr[id],CO2_predicted_ppm[id],data$CO2_ppm[id]),na.rm=T)
                  CO2_yrange <- CO2_xrange
                  IR_range   <- range(data$lp8_ir,na.rm=T)
                  
                  plot(data$CO2_ppm[id], data$lp8_ir[id],         xlim=CO2_xrange, ylim=IR_range,  pch=16,cex=0.5,xlab="CO2 PIC [ppm]", ylab="LP8 IR [XX]",      cex.lab=1.75,cex.axis=1.75)
                  
                  plot(data$CO2_ppm[id], data$lp8_CO2_pcorr[id],  xlim=CO2_xrange, ylim=CO2_yrange,pch=16,cex=0.5,xlab="CO2 PIC [ppm]", ylab="LP8 CO2 [ppm]",    cex.lab=1.75,cex.axis=1.75)
                  lines(c(0,1e4),c(0,1e4),col=2,lwd=1)
                  
                  par(family="mono")
                  legend("bottomright",legend=c(paste("RMSE:",sprintf("%6.1f",RMSE_02)),paste("N:   ",sprintf("%6.0f",n_id))),bg="white",cex=2)
                  par(family="")
                  
                  plot(data$CO2_ppm[id], CO2_predicted_ppm[id],   xlim=CO2_xrange, ylim=CO2_yrange,pch=16,cex=0.5,xlab="CO2 PIC [ppm]", ylab="LP8_IR CO2 [ppm]", cex.lab=1.75,cex.axis=1.75)
                  lines(c(0,1e4),c(0,1e4),col=2,lwd=1)
                  
                  par(family="mono")
                  legend("bottomright",legend=c(paste("RMSE:",sprintf("%6.1f",RMSE_03)),paste("N:   ",sprintf("%6.0f",n_id))),bg="white",cex=2)
                  par(family="")
                }
              }
              
              dev.off()
              par(def_par)
            }
          }
          
          
          ## LP8_T vs CO2 [ppm]
          
          if(T){
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER_CO2_LP8T.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
            
            plot(data$CO2_ppm[id_use4cal],data$lp8_T[id_use4cal],cex=0.5, pch=16,xlab=expression(paste("CO"[2]*" [ppm]")),ylab="LP8 T [degC]",main="",cex.lab=1.75,cex.axis=1.75,cex.main=1.75)
            lines(c(-1e4,1e4),c(0,0),lwd=1,col=2)
            
            dev.off()
            par(def_par)
          }
          
          ## pressure vs CO2 [ppm]
          
          if(T){
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER_CO2_pressure.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
            
            plot(data$CO2_ppm[id_use4cal],data$pressure[id_use4cal],cex=0.5, pch=16,xlab=expression(paste("CO"[2]*" [ppm]")),ylab="Pressure [hPa]",main="",cex.lab=1.75,cex.axis=1.75,cex.main=1.75)
            lines(c(-1e4,1e4),c(0,0),lwd=1,col=2)
            
            dev.off()
            par(def_par)
          }
          
          
          ## extract data for analysis of residuals
          
          if(F){
            if(ith_temp_data_selections == 0 & sensor_models[[ith_sensor_model]]$name=="CO2_MODEL_pLIN_c0_P_PIR_CONSTIR"){
              
              data$d_lp8_ir   <- NA
              data$d_CO2_pred <- NA
              data$CO2_pred   <- CO2_predicted
              
              data$d_lp8_ir[data$noGAP]   <- c(NA,diff(data$lp8_ir))[data$noGAP]
              data$d_CO2_pred[data$noGAP] <- c(NA,diff(data$CO2_pred))[data$noGAP]
              
              df2extract <- cbind(data.frame(residuals=CO2_predicted-data$CO2,stringsAsFactors = F),
                                  data[,which(colnames(data)%in%c("d_lp8_ir","d_CO2_pred","CO2_pred","CO2","lp8_T","lp8_ir","lp8_CO2","sht21_T","sht21_RH","diff_sht21_T_DP","diff_T_DP","pressure","delta_sht21_T","delta_lp8_T","T","RH","AH","sht21_AH"))])
              
              df2extract <- df2extract[which(data$CalMode==1 & !is.na(df2extract$residuals)),]
              
              if(dim(df2extract)[1]>0){
                write.table(df2extract,paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_residuals_covariates.csv",sep=""),sep=";",col.names=T,row.names=F)
              }
            }
          }
        }
      }
    }
  }
}

## --------------------------------------------------------------------------------------------

# Write statistics into file

write.table(x = statistics,
            file = paste(resultdir,"statistics_LP8.csv",sep=""),
            sep=";",
            col.names = T, 
            row.names = F)

# write sht corrections into file

write.table(x = SHT_CORRECTIONS,
            file = paste(resultdir,"corrections_sht21.csv",sep=""),
            sep=";",
            col.names = T, 
            row.names = F)

# write H2O differences into file

write.table(x = statistics_H2O,
            file = paste(resultdir,"statistics_H2O.csv",sep=""),
            sep=";",
            col.names = T, 
            row.names = F)


# write CalibrationModelInfo into file

write.table(x = CalibrationModelInfo,
            file = paste(resultdir,"CalibrationModelInfo.csv",sep=""),
            sep=";",
            col.names = T, 
            row.names = F)


## --------------------------------------------------------------------------------------------

