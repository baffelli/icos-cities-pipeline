# Compute_CarboSense_CO2_values.r
# -------------------------------
#
# Author: Michael Mueller
#
#
# Remarks:
# - Computations refer to UTC.
#
# -------------------------------
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
library(data.table)

## source
source("/project/CarboSense/Software/CarboSenseUtilities/api-v1.3.r")
source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

### ----------------------------------------------------------------------------------------------------------------------------

## ARGUMENTS
#
#  Rscript Compute_CarboSense_CO2_values.r PARTIAL_COMPUTATION MODEL [DUE]
#
#  PARTIAL_COMPUTATION: Processing of all measurements [--> F] or only of measurements of the last 61 days [--> T].
#  MODEL: Selection of calibration model and corresponding database tables/directories. Three versions implemented: 1, 2 [20 (AMT)] and 3 [30 (AMT)].
#  [DUE]: If "DUE" is specified: Processing of measurements from sensors located at location DUE from 2017-12-01 and later with special model parameters. 
#         If omitted only sensors deployed in Carbosense network are processed.
#
#

args = commandArgs(trailingOnly=TRUE)

#

PARTIAL_COMPUTATION <- as.logical(args[1])

if(!(PARTIAL_COMPUTATION==T | PARTIAL_COMPUTATION == F)){
  stop("PARTIAL_COMPUTATION: T or F!")
}

if(PARTIAL_COMPUTATION==T){
  Computation_date_from      <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC") - 61 * 86400
  Computation_timestamp_from <- as.numeric(difftime(time1=Computation_date_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
}

# 

if(!as.integer(args[2])%in%c(1,2,3,20,30)){
  stop("MODEL: 1, 2 or 3!")
}else{
  
  # Standard 
  if(as.integer(args[2])==1){
    ## MODEL
    CalibrationModelName <- "CO2_MODEL_pLIN_c0_P_PIR_CONSTIR"
    ## Insert results into CarboSense DB table
    InsertDBTableName    <- "CarboSense_CO2"
  }
  
  # Test 00
  if(as.integer(args[2])==2){
    ## MODEL
    CalibrationModelName <- "CO2_MODEL_pLIN_c0_P_CONSTIR"
    ## Insert results into CarboSense DB table
    InsertDBTableName    <- "CarboSense_CO2_TEST00"
  }
  
  # Test 01
  if(as.integer(args[2])==3){
    ## MODEL
    CalibrationModelName <- "CO2_MODEL_pLIN_c0_TIR_P_CONSTIR"
    ## Insert results into CarboSense DB table
    InsertDBTableName    <- "CarboSense_CO2_TEST01"
  }
  
  # Test 00 AMT
  if(as.integer(args[2])==20){
    ## MODEL
    CalibrationModelName <- "CO2_MODEL_pLIN_c0_P_CONSTIR"
    ## Insert results into CarboSense DB table
    InsertDBTableName    <- "CarboSense_CO2_TEST00_AMT"
  }
  
  # Test 01 AMT
  if(as.integer(args[2])==30){
    ## MODEL
    CalibrationModelName <- "CO2_MODEL_pLIN_c0_TIR_P_CONSTIR"
    ## Insert results into CarboSense DB table
    InsertDBTableName    <- "CarboSense_CO2_TEST01_AMT"
  }
}

#

COMP_DUE <- F

if(length(args)>2){
  if(args[3]=="DUE"){
    COMP_DUE            <- T
    PARTIAL_COMPUTATION <- F
  }
}

#

statistics <- NULL

if(COMP_DUE==F){
  if(PARTIAL_COMPUTATION==T){
    if(as.integer(args[2])==1){
      statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/statistics_Compute_CarboSense_CO2_values_PARTIAL.csv"
    }
    if(as.integer(args[2])==2){
      statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/statistics_Compute_CarboSense_CO2_values_PARTIAL.csv"
    }
    if(as.integer(args[2])==3){
      statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/statistics_Compute_CarboSense_CO2_values_PARTIAL.csv"
    }
    if(as.integer(args[2])==20){
      statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT/statistics_Compute_CarboSense_CO2_values_PARTIAL.csv"
    }
    if(as.integer(args[2])==30){
      statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT/statistics_Compute_CarboSense_CO2_values_PARTIAL.csv"
    }
  }
  if(PARTIAL_COMPUTATION==F){
    if(as.integer(args[2])==1){
      statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/statistics_Compute_CarboSense_CO2_values_COMPLETE.csv"
    }
    if(as.integer(args[2])==2){
      statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/statistics_Compute_CarboSense_CO2_values_COMPLETE.csv"
    }
    if(as.integer(args[2])==3){
      statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/statistics_Compute_CarboSense_CO2_values_COMPLETE.csv"
    }
    if(as.integer(args[2])==20){
      statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT/statistics_Compute_CarboSense_CO2_values_COMPLETE.csv"
    }
    if(as.integer(args[2])==30){
      statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT/statistics_Compute_CarboSense_CO2_values_COMPLETE.csv"
    }
  }
}else{
  if(as.integer(args[2])==1){
    statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/statistics_Compute_CarboSense_CO2_values_DUE_COMPLETE.csv"
  }
  if(as.integer(args[2])==2){
    statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/statistics_Compute_CarboSense_CO2_values_DUE_COMPLETE.csv"
  }
  if(as.integer(args[2])==3){
    statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/statistics_Compute_CarboSense_CO2_values_DUE_COMPLETE.csv"
  }
  if(as.integer(args[2])==20){
    statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT/statistics_Compute_CarboSense_CO2_values_DUE_COMPLETE.csv"
  }
  if(as.integer(args[2])==30){
    statistics_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT/statistics_Compute_CarboSense_CO2_values_DUE_COMPLETE.csv"
  }
}



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






# # -------

MEAS_MODE    <- "MEAN"

### ----------------------------------------------------------------------------------------------------------------------------

# Date of LP8 software upgrade (version 0 -> version 1 [10 minutes means and last single measurements])

date_UTC_LP8_SU_upgrade_01  <- strptime("20170628000000","%Y%m%d%H%M%S",tz="UTC")

timestamp_LP8_SU_upgrade_01 <- as.numeric(difftime(time1=date_UTC_LP8_SU_upgrade_01,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

### ----------------------------------------------------------------------------------------------------------------------------

# Database queries

query_str       <- paste("SELECT * FROM CalibrationParameters;",sep="")
drv             <- dbDriver("MySQL")
con<-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_calPar      <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

#

if(!COMP_DUE){
  
  query_str       <- paste("SELECT * FROM Deployment WHERE LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1') ",sep="")
  query_str       <- paste(query_str, "AND SensorUnit_ID >= 1010 AND SensorUnit_ID <= 1334;",sep="")
  drv             <- dbDriver("MySQL")
  con<-carboutil::get_conn()
  res             <- dbSendQuery(con, query_str)
  tbl_deployment  <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  tbl_deployment$Date_UTC_from <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_deployment$Date_UTC_to   <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
}else{
  
  query_str       <- paste("SELECT * FROM Deployment WHERE LocationName = 'DUE1' and Date_UTC_to > '2017-12-01 00:00:00' ",sep="")
  query_str       <- paste(query_str, "AND SensorUnit_ID >= 1010 AND SensorUnit_ID <= 1334;",sep="")
  drv             <- dbDriver("MySQL")
  con<-carboutil::get_conn()
  res             <- dbSendQuery(con, query_str)
  tbl_deployment  <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  tbl_deployment$Date_UTC_from <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_deployment$Date_UTC_to   <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
  id <- which(tbl_deployment$Date_UTC_from < strptime("2017-12-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"))
  
  if(length(id)>0){
    tbl_deployment$Date_UTC_from[id] <- strptime("2017-12-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC")
  }
}

# AMT Paper: Limitation of time period to 2019-09-01 00:00:00

if(as.integer(args[2])%in%c(20,30)){
  
  id <- which(tbl_deployment$Date_UTC_from < strptime("2019-09-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"))
  
  if(length(id)>0){
    tbl_deployment <- tbl_deployment[id,]
  }else{
    stop()
  }
  
  #
  
  id <- which(tbl_deployment$Date_UTC_to > strptime("2019-09-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"))
  
  if(length(id)>0){
    tbl_deployment$Date_UTC_to[id] <- strptime("2019-09-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC")
  }
}

#

query_str       <- paste("SELECT * FROM Sensors WHERE Type = 'LP8';",sep="")
drv             <- dbDriver("MySQL")
con<-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_sensors     <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_sensors$Date_UTC_from <- strptime(tbl_sensors$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_sensors$Date_UTC_to   <- strptime(tbl_sensors$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

#

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con<-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_location    <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

#

query_str       <- paste("SELECT * FROM SensorExclusionPeriods WHERE Type = 'LP8';",sep="")
drv             <- dbDriver("MySQL")
con<-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_SEP         <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_SEP$Date_UTC_from <- strptime(tbl_SEP$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_SEP$Date_UTC_to   <- strptime(tbl_SEP$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

### ----------------------------------------------------------------------------------------------------------------------------

## Loop over all LP8 sensor units

for(ith_SensorUnit_ID_2_proc in 1:n_SensorUnit_ID_2_proc){
  
  
  # Get LP8 sensors of this SensorUnit
  
  id_sensors   <- which(tbl_sensors$SensorUnit_ID == SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc] & tbl_sensors$Type == 'LP8')
  n_id_sensors <- length(id_sensors)
  
  if(n_id_sensors==0){
    next
  }
  
  # -- 
  
  # Get calibration parameters of each individual sensor of this SensorUnit
  
  id_cal <- NULL
  
  for(ith_sensor in 1:n_id_sensors){
    
    if(!COMP_DUE){
      
      id_cal_tmp <- which(tbl_calPar$Serialnumber           == tbl_sensors$Serialnumber[id_sensors[ith_sensor]]
                          & tbl_calPar$Type                 == tbl_sensors$Type[id_sensors[ith_sensor]]
                          & tbl_calPar$CalibrationModelName == CalibrationModelName
                          & tbl_calPar$Mode                 == 0)
      
      if(length(id_cal_tmp)==1){
        id_cal <- c(id_cal, id_cal_tmp)
      }else{
        id_cal <- c(id_cal, NA)
      }
      
    }else{
      
      id_cal_tmp <- which(tbl_calPar$Serialnumber           == tbl_sensors$Serialnumber[id_sensors[ith_sensor]]
                          & tbl_calPar$Type                 == tbl_sensors$Type[id_sensors[ith_sensor]]
                          & tbl_calPar$CalibrationModelName == paste("TILL_01Dec2017_",CalibrationModelName,sep="")
                          & tbl_calPar$Mode                 == 0)
      
      if(length(id_cal_tmp)==1){
        id_cal <- c(id_cal, id_cal_tmp)
      }else{
        id_cal <- c(id_cal, NA)
      }
    }
  }
  
  
  
  #
  
  id_sensors   <- id_sensors[!is.na(id_cal)]
  n_id_sensors <- length(id_sensors)
  id_cal       <- id_cal[!is.na(id_cal)]
  n_id_cal     <- length(id_cal)
  
  #
  
  if(n_id_sensors!=n_id_cal | is.null(id_cal)){
    next
  }
  
  
  # -- 
  
  # Get the deployment periods of this SensorUnit
  
  if(PARTIAL_COMPUTATION==T){
    id_depl     <- which(tbl_deployment$SensorUnit_ID==SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc] & tbl_deployment$Date_UTC_to > Computation_date_from)
    n_id_depl   <- length(id_depl)
    
    if(n_id_depl>0){
      id_adj_date <- which(tbl_deployment$Date_UTC_from[id_depl] < Computation_date_from)
      if(length(id_adj_date)>0){
        tbl_deployment$Date_UTC_from[id_depl[id_adj_date]] <- Computation_date_from
      }
    }
  }else{
    id_depl     <- which(tbl_deployment$SensorUnit_ID==SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc] )
    n_id_depl   <- length(id_depl)
  }
  
  
  # Get measurements from this SensorUnit for each deployment period 
  
  sensor_data <- NULL 
  
  if(n_id_depl>0){
    
    for(ith_depl in 1:n_id_depl){
      
      # Some periods may be excluded
      
      if(tbl_deployment$LocationName[id_depl[ith_depl]]=="RIG" & tbl_deployment$Date_UTC_from[id_depl[ith_depl]]<strptime("20170717000000","%Y%m%d%H%M%S",tz="UTC")){
        next
      }
      
      # Get information about the location of the deployment and the installation
      
      id_loc <- which(tbl_location$LocationName==tbl_deployment$LocationName[id_depl[ith_depl]])
      
      if(length(id_loc)==0){
        next
      }
      
      if(tbl_deployment$HeightAboveGround[id_depl[ith_depl]] == -999){
        SensorUnit_height_depl  <- 0
      }else{
        SensorUnit_height_depl  <- tbl_deployment$HeightAboveGround[id_depl[ith_depl]]
      }
      
      # Import measurement from Decentlab Influx-DB
      
      timestamp_depl_from <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_from[id_depl[ith_depl]],time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
      timestamp_depl_to   <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_to[id_depl[ith_depl]],  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
      
      timeFilter <- paste("time >= ",timestamp_depl_from,"s AND time < ",timestamp_depl_to,"s",sep="")
      device     <- paste("/",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],"/",sep="")
      print(paste("Getting data for SU", SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]))

      #Define the cols to query from decentlab
      cols2query <- c("senseair-lp8-temperature-last",
                      "senseair-lp8-temperature",
                      "sensirion-sht21-temperature-last",
                      "sensirion-sht21-temperature",
                      "sensirion-sht21-humidity",
                      "senseair-lp8-co2-filtered",
                      "senseair-lp8-co2",
                      "senseair-lp8-ir-filtered",
                      "senseair-lp8-ir-last",
                      "senseair-lp8-ir",
                      "senseair-lp8-status")
      cols_query_str <- paste("/^(",glue::glue_collapse(cols2query,sep = '|'), ")$/",sep="")
      if(SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]%in%c(1007,1008)){
        
        tmp0 <- tryCatch({
          query(domain=DL_DB_domain_EMPA,
                apiKey=DL_DB_apiKey_EMPA,
                timeFilter=timeFilter,
                device = device,
                location = "//",
                sensor = cols_query_str,
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
                sensor = cols_query_str,
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
      str(tmp0)
      if(is.null(tmp0)){
        next
      }
      if(dim(tmp0)[1]==0){
        next
      }
      print(paste("Data for SU", SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc], "is returned from influxDB"))
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
      
      cn_required   <- c("date","lp8_T_m","lp8_T_l","sht21_T_m","sht21_T_l","sht21_RH","lp8_CO2_f","lp8_CO2","lp8_ir_f","lp8_ir_l","lp8_ir_m","lp8_status","LocationName","pressure")
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
      
      N_extracted_DB <- dim(tmp)[1] # (Statistics on data transmission)
      
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

      print(paste("Data for SU", SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc], "is ready"))
      # Statistics on data transmission
      
      ts_now <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
      ts_now <- as.numeric(difftime(time1=ts_now,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
      
      if(timestamp_depl_to < ts_now){
        ts_now <- timestamp_depl_to
      }
      
      statistics    <- rbind(statistics,data.frame(SensorUnit_ID  = SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],
                                                   LocationName   = tbl_deployment$LocationName[id_depl[ith_depl]],
                                                   timestamp_from = timestamp_depl_from,
                                                   timestamp_to   = ts_now,
                                                   N_extracted_DB = N_extracted_DB,
                                                   N_trans_exp    = round((ts_now-timestamp_depl_from)/600 + 1),
                                                   N_trans_actual = dim(tmp)[1],
                                                   N_flag_zero    = sum(tmp$lp8_status==0,na.rm=T),
                                                   N_flag_nonzero = sum(tmp$lp8_status> 0,na.rm=T),
                                                   stringsAsFactors = F))
      
      # Add pressure data
      
      query_str       <- paste("SELECT * FROM PressureInterpolation WHERE LocationName = '",tbl_deployment$LocationName[id_depl[ith_depl]],"' and timestamp > ",timestamp_depl_from," and timestamp < ",timestamp_depl_to,";",sep="")
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn()
      res             <- dbSendQuery(con, query_str)
      tbl_PressureInterpolation   <- dbFetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      if(dim(tbl_PressureInterpolation)[1]>0){
        tbl_PressureInterpolation <- tbl_PressureInterpolation[order(tbl_PressureInterpolation$timestamp),]
      }
      
      #
      
      query_str       <- paste("SELECT * FROM PressureParameter WHERE timestamp > ",timestamp_depl_from," and timestamp < ",timestamp_depl_to,";",sep="")
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn()
      res             <- dbSendQuery(con, query_str)
      tbl_PressureParameter   <- dbFetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      if(dim(tbl_PressureInterpolation)[1]>0){
        tbl_PressureParameter <- tbl_PressureParameter[order(tbl_PressureParameter$timestamp),]
      }
      
      # Temporal interpolation of height and spatially interpolated pressure
      
      if(dim(tbl_PressureInterpolation)[1]>0 & dim(tbl_PressureParameter)[1]>0){
        
        tmp_PressureInterpolation <- approx(x=tbl_PressureInterpolation$timestamp,y=tbl_PressureInterpolation$pressure,xout=tmp$secs-300,method="linear",rule=1)
        tmp_PressureParameter     <- approx(x=tbl_PressureParameter$timestamp,    y=tbl_PressureParameter$height,      xout=tmp$secs-300,method="linear",rule=1)
        
        if(length(tmp_PressureInterpolation$y)!=length(tmp_PressureParameter$y)){
          stop("ERROR_2")
        }
        
        tmp$pressure                   <- NA
        id_comp_pressure               <- which(tmp_PressureInterpolation$y > 500)
        tmp$pressure[id_comp_pressure] <- tmp_PressureInterpolation$y[id_comp_pressure] * exp(-SensorUnit_height_depl/tmp_PressureParameter$y[id_comp_pressure])
        
        
        rm(tbl_PressureParameter,tmp_PressureInterpolation,tmp_PressureParameter,tbl_PressureInterpolation)
        gc()
      }
      
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
  }
  
  
  # Check if time periods should be excluded
  
  id_SEP_SU <- which(tbl_SEP$SensorUnit_ID ==SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]
                     & tbl_SEP$Type == "LP8")
  
  n_id_SEP_SU <- length(id_SEP_SU)
  
  
  if(n_id_SEP_SU > 0){
    for(ith_id_SEP_SU in 1:n_id_SEP_SU){
      id_setToNA <- which(sensor_data$date>=tbl_SEP$Date_UTC_from[id_SEP_SU[ith_id_SEP_SU]] & sensor_data$date<=tbl_SEP$Date_UTC_to[id_SEP_SU[ith_id_SEP_SU]])
      if(length(id_setToNA)>0){
        sensor_data$lp8_ir[id_setToNA] <- NA
      }
    }
  }
  
  rm(id_SEP_SU,n_id_SEP_SU,id_setToNA)
  gc()
  
  # Apply calibration for individual sensor
  
  sensor_data$CO2_CAL   <- NA
  sensor_data$CO2_CAL_L <- -999
  
  FLAG <- rep(0,   dim(sensor_data)[1])
  
  for(ith_sensor in 1:n_id_sensors){
    
    data2cal   <- rep(T,dim(sensor_data)[1])
    data2cal_L <- rep(T,dim(sensor_data)[1])
    
    # status
    data2cal   <- data2cal & sensor_data$lp8_status == 0
    data2cal_L <- data2cal & sensor_data$lp8_status == 0
    
    # All sensors provide data
    data2cal   <- data2cal & !is.na(sensor_data$lp8_ir)   & !is.na(sensor_data$lp8_T)   & !is.na(sensor_data$sht21_T) & !is.na(sensor_data$pressure)
    data2cal_L <- data2cal & !is.na(sensor_data$lp8_ir_l) & !is.na(sensor_data$lp8_T_l) & !is.na(sensor_data$sht21_T) & !is.na(sensor_data$pressure)
    
    
    # sensor period 
    data2cal   <- data2cal & sensor_data$date>=tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]] & sensor_data$date<=tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]]
    data2cal_L <- data2cal & sensor_data$date>=tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]] & sensor_data$date<=tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]]
    
    # apply model to mean measurement
    
    if(sum(data2cal)>0){
      
      if(CalibrationModelName=="CO2_MODEL_pLIN_c0_P_PIR_CONSTIR"){
        
        last_par <- which(colnames(tbl_calPar)=="PAR_00") - 1 + tbl_calPar$N_PAR[id_cal[ith_sensor]]
        
        sensor_data$CO2_CAL[data2cal] <- (tbl_calPar$PAR_00[id_cal[ith_sensor]]
                                          + tbl_calPar$PAR_01[id_cal[ith_sensor]] * -log(sensor_data$lp8_ir[data2cal])
                                          + tbl_calPar$PAR_02[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^1
                                          + tbl_calPar$PAR_03[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^2
                                          + tbl_calPar$PAR_04[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^3
                                          + tbl_calPar$PAR_05[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^1/sensor_data$lp8_ir[data2cal]
                                          + tbl_calPar$PAR_06[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^2/sensor_data$lp8_ir[data2cal]
                                          + tbl_calPar$PAR_07[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^3/sensor_data$lp8_ir[data2cal]
                                          + tbl_calPar$PAR_08[id_cal[ith_sensor]] * (sensor_data$pressure[data2cal]-1013.25)/1013.25
                                          + tbl_calPar$PAR_09[id_cal[ith_sensor]] * ((sensor_data$pressure[data2cal]-1013.25)/1013.25)/sensor_data$lp8_ir[data2cal]
                                          + tbl_calPar[id_cal[ith_sensor],last_par] *  rep(1,sum(data2cal)))
      }
      
      #
      
      if(CalibrationModelName=="CO2_MODEL_pLIN_c0_TIR_P_CONSTIR"){
        
        last_par <- which(colnames(tbl_calPar)=="PAR_00") - 1 + tbl_calPar$N_PAR[id_cal[ith_sensor]]
        
        sensor_data$CO2_CAL[data2cal] <- (tbl_calPar$PAR_00[id_cal[ith_sensor]]
                                          + tbl_calPar$PAR_01[id_cal[ith_sensor]] * -log(sensor_data$lp8_ir[data2cal])
                                          + tbl_calPar$PAR_02[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^1
                                          + tbl_calPar$PAR_03[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^2
                                          + tbl_calPar$PAR_04[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^3
                                          + tbl_calPar$PAR_05[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^1/sensor_data$lp8_ir[data2cal]
                                          + tbl_calPar$PAR_06[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^2/sensor_data$lp8_ir[data2cal]
                                          + tbl_calPar$PAR_07[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^3/sensor_data$lp8_ir[data2cal]
                                          + tbl_calPar$PAR_08[id_cal[ith_sensor]] * (sensor_data$pressure[data2cal]-1013.25)/1013.25
                                          + tbl_calPar[id_cal[ith_sensor],last_par] *  rep(1,sum(data2cal)))
      }
      
      #
      
      
      if(CalibrationModelName=="CO2_MODEL_pLIN_c0_P_CONSTIR"){
        
        last_par <- which(colnames(tbl_calPar)=="PAR_00") - 1 + tbl_calPar$N_PAR[id_cal[ith_sensor]]
        
        sensor_data$CO2_CAL[data2cal] <- (tbl_calPar$PAR_00[id_cal[ith_sensor]]
                                          + tbl_calPar$PAR_01[id_cal[ith_sensor]] * -log(sensor_data$lp8_ir[data2cal])
                                          + tbl_calPar$PAR_02[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^1
                                          + tbl_calPar$PAR_03[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^2
                                          + tbl_calPar$PAR_04[id_cal[ith_sensor]] * (sensor_data$lp8_T[data2cal]+273.15)^3
                                          + tbl_calPar$PAR_05[id_cal[ith_sensor]] * (sensor_data$pressure[data2cal]-1013.25)/1013.25
                                          + tbl_calPar[id_cal[ith_sensor],last_par] *  rep(1,sum(data2cal)))
      }
      
      
      # FLAG
      
      FLAG[data2cal & sensor_data$sht21_RH<=tbl_calPar$SHT21_RH_max[id_cal[ith_sensor]]] <- 1
      
      # CO2_cal --> [mol/mol]
      
      # pressure
      sensor_data$CO2_CAL[data2cal] <- sensor_data$CO2_CAL[data2cal] * (rep(1013.25,sum(data2cal)) / sensor_data$pressure[data2cal])
      
      # temperature
      sensor_data$CO2_CAL[data2cal] <- sensor_data$CO2_CAL[data2cal] * ((sensor_data$lp8_T[data2cal]+rep(273.15,sum(data2cal))) / rep(273.15,sum(data2cal)))
      
      # H2O [Vol-%]
      
      coef_1 <-  -7.85951783
      coef_2 <-   1.84408259
      coef_3 <-  -11.7866497
      coef_4 <-   22.6807411
      coef_5 <-  -15.9618719
      coef_6 <-   1.80122502
      
      sensor_data$H2O <- NA
      
      theta                     <- 1 - (273.15+sensor_data$sht21_T[data2cal])/647.096
      Pws_sht21                 <- 220640 * exp( 647.096/(273.15+sensor_data$sht21_T[data2cal]) * (coef_1*theta + coef_2*theta^1.5 + coef_3*theta^3 + coef_4*theta^3.5 + coef_5*theta^4 + coef_6*theta^7.5))
      Pw_sht21                  <- sensor_data$sht21_RH[data2cal]*(Pws_sht21*100)/100
      sensor_data$H2O[data2cal] <- Pw_sht21 / (sensor_data$pressure[data2cal]*1e2)*1e2
    }
    
    # apply model to last measurement
    
    if(sum(data2cal_L)>0){
      
      #
      
      if(CalibrationModelName=="CO2_MODEL_pLIN_c0_P_PIR_CONSTIR"){
        
        last_par <- which(colnames(tbl_calPar)=="PAR_00") - 1 + tbl_calPar$N_PAR[id_cal[ith_sensor]]
        
        sensor_data$CO2_CAL_L[data2cal_L] <- (tbl_calPar$PAR_00[id_cal[ith_sensor]]
                                              + tbl_calPar$PAR_01[id_cal[ith_sensor]] * -log(sensor_data$lp8_ir_l[data2cal_L])
                                              + tbl_calPar$PAR_02[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^1
                                              + tbl_calPar$PAR_03[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^2
                                              + tbl_calPar$PAR_04[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^3
                                              + tbl_calPar$PAR_05[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^1/sensor_data$lp8_ir_l[data2cal_L]
                                              + tbl_calPar$PAR_06[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^2/sensor_data$lp8_ir_l[data2cal_L]
                                              + tbl_calPar$PAR_07[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^3/sensor_data$lp8_ir_l[data2cal_L]
                                              + tbl_calPar$PAR_08[id_cal[ith_sensor]] * (sensor_data$pressure[data2cal_L]-1013.25)/1013.25
                                              + tbl_calPar$PAR_09[id_cal[ith_sensor]] * ((sensor_data$pressure[data2cal_L]-1013.25)/1013.25)/sensor_data$lp8_ir_l[data2cal_L]
                                              + tbl_calPar[id_cal[ith_sensor],last_par] *  rep(1,sum(data2cal_L)))
      }
      
      #
      
      if(CalibrationModelName=="CO2_MODEL_pLIN_c0_TIR_P_CONSTIR"){
        
        last_par <- which(colnames(tbl_calPar)=="PAR_00") - 1 + tbl_calPar$N_PAR[id_cal[ith_sensor]]
        
        sensor_data$CO2_CAL_L[data2cal_L] <- (tbl_calPar$PAR_00[id_cal[ith_sensor]]
                                              + tbl_calPar$PAR_01[id_cal[ith_sensor]] * -log(sensor_data$lp8_ir_l[data2cal_L])
                                              + tbl_calPar$PAR_02[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^1
                                              + tbl_calPar$PAR_03[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^2
                                              + tbl_calPar$PAR_04[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^3
                                              + tbl_calPar$PAR_05[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^1/sensor_data$lp8_ir_l[data2cal_L]
                                              + tbl_calPar$PAR_06[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^2/sensor_data$lp8_ir_l[data2cal_L]
                                              + tbl_calPar$PAR_07[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^3/sensor_data$lp8_ir_l[data2cal_L]
                                              + tbl_calPar$PAR_08[id_cal[ith_sensor]] * (sensor_data$pressure[data2cal_L]-1013.25)/1013.25
                                              + tbl_calPar[id_cal[ith_sensor],last_par] *  rep(1,sum(data2cal_L)))
      }
      
      #
      
      if(CalibrationModelName=="CO2_MODEL_pLIN_c0_P_CONSTIR"){
        
        last_par <- which(colnames(tbl_calPar)=="PAR_00") - 1 + tbl_calPar$N_PAR[id_cal[ith_sensor]]
        
        sensor_data$CO2_CAL_L[data2cal_L] <- (tbl_calPar$PAR_00[id_cal[ith_sensor]]
                                              + tbl_calPar$PAR_01[id_cal[ith_sensor]] * -log(sensor_data$lp8_ir_l[data2cal_L])
                                              + tbl_calPar$PAR_02[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^1
                                              + tbl_calPar$PAR_03[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^2
                                              + tbl_calPar$PAR_04[id_cal[ith_sensor]] * (sensor_data$lp8_T_l[data2cal_L]+273.15)^3
                                              + tbl_calPar$PAR_05[id_cal[ith_sensor]] * (sensor_data$pressure[data2cal_L]-1013.25)/1013.25
                                              + tbl_calPar[id_cal[ith_sensor],last_par] *  rep(1,sum(data2cal_L)))
      }
      
      # CO2_cal --> [mol/mol]
      
      # pressure
      sensor_data$CO2_CAL_L[data2cal_L] <- sensor_data$CO2_CAL_L[data2cal_L] * (rep(1013.25,sum(data2cal_L)) / sensor_data$pressure[data2cal_L])
      
      # temperature
      sensor_data$CO2_CAL_L[data2cal_L] <- sensor_data$CO2_CAL_L[data2cal_L] * ((sensor_data$lp8_T_l[data2cal_L]+rep(273.15,sum(data2cal_L))) / rep(273.15,sum(data2cal_L)))
    }
    
    
  }
  
  
  # Adjust time stamp (end of 10 minute interval --> begin of 10 minute intervall)
  
  sensor_data$timestamp_DB <- sensor_data$secs - 600
  
  # lp8_ir_l : NA --> -999 (due to database entry)
  
  id_ir_l_NA <- which(is.na(sensor_data$lp8_ir_l))
  
  if(length(id_ir_l_NA)>0){
    sensor_data$lp8_ir_l[id_ir_l_NA] <- -999
  }
  
  rm(id_ir_l_NA)
  gc()
  
  
  # Delete all the data from this SensorUnit from table "CarboSense_CO2" (or InsertDBTableName)
  
  if(!COMP_DUE){
    if(PARTIAL_COMPUTATION==T){
      query_str <- paste("DELETE FROM ",InsertDBTableName," WHERE LocationName != 'DUE1' and SensorUnit_ID = ",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]," and timestamp >= ",(Computation_timestamp_from - 600),";",sep="")
    }else{
      query_str <- paste("DELETE FROM ",InsertDBTableName," WHERE LocationName != 'DUE1' and SensorUnit_ID = ",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],";",sep="")
    }
  }else{
    query_str   <- paste("DELETE FROM ",InsertDBTableName," WHERE LocationName = 'DUE1' and SensorUnit_ID = ",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],";",sep="")
  }
  
  drv             <- dbDriver("MySQL")
  con<-carboutil::get_conn()
  res             <- dbSendQuery(con, query_str)
  dbClearResult(res)
  dbDisconnect(con)
  
  # Insert data from this SensorUnit into table "CarboSense_CO2"  (or InsertDBTableName)
  
  id_insert   <- which(!is.na(sensor_data$CO2_CAL))
  n_id_insert <- length(id_insert)
  
  if(n_id_insert>0){
    
    print(paste("Insert data for SU",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]))
    
    n_inserted_rows <- 52560
    n_inserts       <- ceiling(n_id_insert/n_inserted_rows)
    
    for(ith_insert in 1:n_inserts){
      
      id_insert_from <- (ith_insert-1)*n_inserted_rows+1
      id_insert_to   <- ith_insert*n_inserted_rows
      
      if(id_insert_to > n_id_insert){
        id_insert_to <- n_id_insert
      }
      
      id   <- id_insert_from:id_insert_to
      n_id <- length(id)
    


      #Create tibbles to insert
      data2_insert <- dplyr::mutate_if(dplyr::mutate(tibble::tibble(sensor_data[id_insert[id],]),
      SensorUnit_ID=SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],
      FLAG=FLAG[id_insert[id]]),
      is.numeric, function(x) dplyr::coalesce(x, -999))

      cols2_to_insert <- dplyr::select(data2_insert,
      'LocationName'=LocationName,
      'SensorUnit_ID'=SensorUnit_ID,
      'timestamp'=timestamp_DB,
      'CO2'=CO2_CAL,
      'CO2_L'=CO2_CAL_L,
      H2O,
      'LP8_IR'=lp8_ir,
      'LP8_IR_L'=lp8_ir_l,
      'LP8_T'=lp8_T,
      'SHT21_T'=sht21_T,
      'SHT21_RH'=sht21_RH,
      FLAG
      )
      #Write data
      con<-carboutil::get_conn()
      carboutil::write_chuncks(con , as.data.frame(cols2_to_insert), InsertDBTableName)
      # DBI::dbWriteTable(
      # conn=con, 
      # name=InsertDBTableName, 
      # value=as.data.frame(cols2_to_insert),
      # overwrite = 0,
      # append = 1,
      # temporary = FALSE,
      # row.names = 0)

      #res             <- dbSendQuery(con, query_str)
      #dbClearResult(res)
      dbDisconnect(con)
      print(paste("Done inserting data for SU",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]))
      rm(id_insert_from,id_insert_to,id,n_id,cols2_to_insert,data2_insert )
      rm()
    }
  }
}

### -----------------------------------------------------------------------------------------------------------------

write.table(x = statistics,file = statistics_fn,sep = ";",col.names = T,row.names = F)


