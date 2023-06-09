# Compute_CarboSense_HPP_CO2_values.r
# -----------------------------------
#
# Author: Michael Mueller
#
#
# -----------------------------------
#
# Remarks:
# - Computations refer to UTC.
#
# -----------------------------------


## clear variables
rm(list=ls(all=TRUE))
gc()

## libraries
library(openair)
library(DBI)
require(RMySQL)
require(chron)
library(data.table)
library(dplyr)
library(dbplyr)
library(argparse)
## source
source("/project/CarboSense/Software/CarboSenseUtilities/api-v1.3.r")
source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

### ----------------------------------------------------------------------------------------------------------------------------


#Trick to make things work with timezones
dbFetch <- function(...) lubridate::with_tz(DBI::dbFetch(...), tz='UTC')


## ARGUMENTS

parser <- ArgumentParser(description='Process HPP data')
parser$add_argument('SelectedLocationName', type="character", nargs='?', default=NULL, help='Location to process')
parser$add_argument('--partial', help='Perform partial computation?', action="store_true")
args <- parser$parse_args(commandArgs(trailingOnly=TRUE))


if(args$partial){

  Computation_date_from      <- lubridate::now() - lubridate::days(x=20)
  Computation_timestamp_from <- as.numeric(Computation_date_from)
}
PARTIAL_COMPUTATION  <- args$partial

  

if(is.null(args$SelectedLocationName)){
  SelectedLocationName <- args$SelectedLocationName
}else{
  SelectedLocationName <- NULL
}
### ----------------------------------------------------------------------------------------------------------------------------

## MODEL

# CalibrationModelName <- "HPP_CO2_IR_CP1_pIR_SHTT_H2O_DRIFT"
CalibrationModelName <- "HPP_CO2_IR_CP1_pIR_SHTT_H2O_DCAL_PLF14d"
# CalibrationModelName <- "HPP_CO2_IR_CP1_pIR_SHTT_DCAL_PLF14d"

CV_mode <- 0

## Insert results into CarboSense DB table name

InsertDBTableName <- "CarboSense_HPP_CO2"

## Result file directory / result file names

resultdir <- "/project/CarboSense/Carbosense_Network/HPP_PerformanceAnalysis/"

if(is.null(SelectedLocationName)){
  figname_CYL_PRESSURE              <- paste(resultdir,"CYL_PRESSURE.pdf",sep="")
  figname_HPP_CALIBRATIONS_T        <- paste(resultdir,"HPP_CALIBRATIONS_T.pdf",sep="")
  figname_HPP_CALIBRATIONS_H2O      <- paste(resultdir,"HPP_CALIBRATIONS_H2O.pdf",sep="")
  figname_HPP_CALIBRATIONS_HUMIDITY <- paste(resultdir,"HPP_CALIBRATIONS_HUMIDITY.pdf",sep="")
  figname_HPP_CALIBRATIONS          <- paste(resultdir,"HPP_CALIBRATIONS.pdf",sep="")
  filename_HPP_DataProcessing_csv   <- paste(resultdir,"HPP_DataProcessing.csv",sep="")
}else{
  print(paste("Processing only", SelectedLocationName))
  figname_CYL_PRESSURE              <- paste(resultdir,SelectedLocationName,"_","CYL_PRESSURE.pdf",sep="")
  figname_HPP_CALIBRATIONS_T        <- paste(resultdir,SelectedLocationName,"_","HPP_CALIBRATIONS_T.pdf",sep="")
  figname_HPP_CALIBRATIONS_H2O      <- paste(resultdir,SelectedLocationName,"_","HPP_CALIBRATIONS_H2O.pdf",sep="")
  figname_HPP_CALIBRATIONS_HUMIDITY <- paste(resultdir,SelectedLocationName,"_","HPP_CALIBRATIONS_HUMIDITY.pdf",sep="")
  figname_HPP_CALIBRATIONS          <- paste(resultdir,SelectedLocationName,"_","HPP_CALIBRATIONS.pdf",sep="")
  filename_HPP_DataProcessing_csv   <- paste(resultdir,SelectedLocationName,"_","HPP_DataProcessing.csv",sep="")
}

### ----------------------------------------------------------------------------------------------------------------------------

## Variables

BCP_df <- NULL

### ----------------------------------------------------------------------------------------------------------------------------

## Decentlab DB information

DL_DB_domain      <- "swiss.co2.live"
DL_DB_apiKey      <- "eyJrIjoiSFd4bWJhczJjclpaUnpHeXluck1WYlJ0MkdINWhneFciLCJuIjoibWljaGFlbC5tdWVsbGVyQGVtcGEuY2giLCJpZCI6MX0="

### ----------------------------------------------------------------------------------------------------------------------------

## SensorUnits to be processed

SensorUnit_ID_2_proc   <- c(426:445)

SensorUnit_ID_2_proc   <- sort(unique(SensorUnit_ID_2_proc))
n_SensorUnit_ID_2_proc <- length(SensorUnit_ID_2_proc)

### ----------------------------------------------------------------------------------------------------------------------------

## Database queries

# CalibrationParameters

query_str       <- paste("SELECT * FROM CalibrationParameters;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn(group='CarboSense_MySQL')
res             <- dbSendQuery(con, query_str)
tbl_calPar      <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

# Deployment
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn(group='CarboSense_MySQL')
if(is.null(SelectedLocationName)){
  date_start <- strptime('2018-07-01 00:00:00',"%Y-%m-%d %H:%M:%S",tz="UTC")
  query_str <- "SELECT *, UNIX_TIMESTAMP(Date_UTC_from) AS timestamp_from, UNIX_TIMESTAMP(Date_UTC_to) AS timestamp_to  FROM Deployment WHERE LocationName NOT IN ('DUE2','DUE3','DUE4','DUE5','MET1')
  AND SensorUnit_ID BETWEEN 426 AND 445 AND Date_UTC_to >'2018-07-01 00:00:00' ORDER BY SensorUnit_ID ASC, Date_UTC_from ASC;"
  res <- dbSendQuery(con, query_str)
}else{
  query_str <- "SELECT *, UNIX_TIMESTAMP(Date_UTC_from) AS timestamp_from, UNIX_TIMESTAMP(Date_UTC_to) AS timestamp_to FROM Deployment WHERE LocationName = ? AND SensorUnit_ID BETWEEN 426 AND 445 AND Date_UTC_to > '2018-03-01 00:00:00';"
  date_start <- strptime("2018-03-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC")
  res <- dbSendQuery(con, query_str)
  dbBind(res, SelectedLocationName)
}






tbl_deployment  <- dbFetch(res, n=-1) %>% mutate(timestamp_to=coalesce(timestamp_to, as.integer(lubridate::now())))
dbClearResult(res)
dbDisconnect(con)



# Sensors

query_str       <- paste("SELECT * FROM Sensors WHERE SensorUnit_ID BETWEEN 426 AND 445;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn(group='CarboSense_MySQL')
res             <- dbSendQuery(con, query_str)
tbl_sensors     <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_sensors$Date_UTC_from <- strptime(tbl_sensors$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_sensors$Date_UTC_to   <- strptime(tbl_sensors$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")



# Location

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn(group='CarboSense_MySQL')
res             <- dbSendQuery(con, query_str)
tbl_location    <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

# SensorExclusionPeriods

query_str       <- paste("SELECT * FROM SensorExclusionPeriods;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn(group='CarboSense_MySQL')
res             <- dbSendQuery(con, query_str)
tbl_SEP         <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_SEP$Date_UTC_from <- strptime(tbl_SEP$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_SEP$Date_UTC_to   <- strptime(tbl_SEP$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

### ----------------------------------------------------------------------------------------------------------------------------
## Loop over all HPP sensor units (which are in the table of sensors to process)
for(ith_SensorUnit_ID_2_proc in 1:n_SensorUnit_ID_2_proc){
  
  print(paste("The sensors to process are", SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]))
  # Get HPP sensors of this SensorUnit
  
  id_sensors   <- which(tbl_sensors$SensorUnit_ID == SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc] & tbl_sensors$Type == 'HPP')
  n_id_sensors <- length(id_sensors)

  
  if(n_id_sensors==0){
    next
  }
  
  # reference gas cylinder information


  query_str <- "SELECT rgc_d.*, rgc.CO2, rgc.pressure AS InitialPressure FROM RefGasCylinder_Deployment AS rgc_d JOIN RefGasCylinder AS rgc ON rgc_d.CylinderID = rgc.CylinderID AND rgc_d.Date_UTC_from BETWEEN rgc.Date_UTC_from AND rgc.Date_UTC_to where SensorUnit_ID=?"
  con <-carboutil::get_conn(group='CarboSense_MySQL')
  pq <- dbSendQuery(con, query_str)
  dbBind(pq, SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc])
  tbl_refGasCylDepl <-  dbFetch(pq, n=-1)
  dbClearResult(pq)
  dbDisconnect(con)


  
  # Get calibration parameters (HPP/HPP pressure) of each individual HPP sensor of this SensorUnit
  
  id_cal          <- NULL
  id_cal_pressure <- NULL
  
  for(ith_sensor in 1:n_id_sensors){
    
    tmp <-  which(tbl_calPar$Serialnumber           == tbl_sensors$Serialnumber[id_sensors[ith_sensor]]
                  & tbl_calPar$Type                 == tbl_sensors$Type[id_sensors[ith_sensor]]
                  & tbl_calPar$CalibrationModelName == CalibrationModelName
                  & tbl_calPar$Mode                 == CV_mode)
    
    if(length(tmp)==0){
      id_cal   <- c(id_cal,-999)
    }else{
      id_cal   <- c(id_cal,tmp)
    }
    
    #
    
    tmp <- which(tbl_calPar$Serialnumber           == tbl_sensors$Serialnumber[id_sensors[ith_sensor]]
                 & tbl_calPar$Type                 == "HPP_pressure"
                 & tbl_calPar$CalibrationModelName == "HPP_pressure_linear"
                 & tbl_calPar$Mode                 == 0)
    
    if(length(tmp)==0){
      id_cal_pressure   <- c(id_cal_pressure,-999)
    }else{
      id_cal_pressure   <- c(id_cal_pressure,tmp)
    }
  }
  
  n_id_cal          <- length(id_cal)
  n_id_cal_pressure <- length(id_cal_pressure)
  
  
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
      
      # Get information about the location of the deployment and the installation
      
      id_loc <- which(tbl_location$LocationName==tbl_deployment$LocationName[id_depl[ith_depl]]&tbl_location$Date_UTC_from <= tbl_deployment$Date_UTC_from[id_depl[ith_depl]])
      print(paste("ID loc is:", id_loc))
      if(length(id_loc)==0){
        next
      }
      
      
      # Import measurement from Decentlab Influx-DB
      start_query <- as.integer(tbl_deployment$Date_UTC_from[id_depl[ith_depl]])
      stop_query <- tbl_deployment$timestamp_to[id_depl[ith_depl]]
      timeFilter <- paste("time >= ",start_query,"s AND time < ",stop_query,"s",sep="")
      device     <- paste("/",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],"/",sep="")
      print(timeFilter)
      print(paste("requesting data for", SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]))
      tmp0 <- tryCatch({
        # query(domain=DL_DB_domain,
        #       apiKey=DL_DB_apiKey,
        #       timeFilter=timeFilter,
        #       device = device,
        #       location = "//",
        #       sensor = "/calibration|senseair-hpp-ir-signal|senseair-hpp-pressure-filtered|senseair-hpp-co2-filtered|senseair-hpp-status|senseair-hpp-temperature-mcu|sensirion-sht21-humidity|sensirion-sht21-temperature/",
        #       channel = "//",
        #       aggFunc = "mean",
        #       aggInterval = "1m",
        #       doCast = FALSE,
        #       timezone = 'UTC')
      carboutil::query_decentlab(
      carboutil::get_decentlab_api_key(),
      time_filter =timeFilter,
      device = device,
      location = "//",
      sensor = "//",
      agg_func = "mean",
      agg_int = "1m"
    )
      },
      error=function(cond){
        print(cond)
        return(NULL)
      }
      )
      
      
      if(is.null(tmp0)){
        next
      }
      print(paste("received data for", SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]))
      # previously: doCast = TRUE and without following two lines (31 Jan 2019)
      # require(reshape)
      # tmp0 <- data.frame(reshape::cast(tmp0, time ~ series, fun.aggregate = mean), check.names = FALSE)
      
      # require(reshape)
      # tmp0$time <- round(as.numeric(difftime(time1=tmp0$time,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))*1e6)
      # tmp0      <- data.frame(reshape::cast(tmp0, time ~ series, fun.aggregate = mean), check.names = FALSE)
      # tmp0$time <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp0$time/1e6
      # print(tmp0)
      # tmp0      <- as.data.table(tmp0)
      # tmp0$time <- round(as.numeric(difftime(time1=tmp0$time,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))*1e3)
      # tmp0      <- dcast.data.table(tmp0, time ~ series, fun.aggregate = mean,value.var = "value")
      # tmp0$time <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp0$time/1e3
      # tmp0      <- as.data.frame(tmp0)
      # print(paste("Finished reshaping data for", SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]))
      
      # Modification of colnames
      
      new_names <- c(
      "battery"                  = "battery",                           
      "valve"                    = "calibration",                       
      "hpp_co2"                  = "senseair_hpp_co2_filtered",         
      "hpp_ir"                   = "senseair_hpp_ir_signal",            
      "hpp_lpl"                  = "senseair_hpp_lpl_signal",           
      "hpp_ntc5_dT"              = "senseair_hpp_ntc5_diff_temp",       
      "hpp_ntc6_T"               = "senseair_hpp_ntc6_se_temp",         
      "hpp_status"               = "senseair_hpp_status",               
      "hpp_pressure"             = "senseair_hpp_pressure_filtered",    
      "hpp_temperature_detector" = "senseair_hpp_temperature_detector", 
      "hpp_temperature_mcu"      = "senseair_hpp_temperature_mcu",      
      "sht21_RH"                 = "sensirion_sht21_humidity",          
      "sht21_T"                  = "sensirion_sht21_temperature",       
      "date"                     = "time")                              
      
      sensor_data <- 
      dplyr::rename_all(tmp0, function(x) stringr::str_replace_all(x, "-", "_")) %>%
      dplyr::rename(!!!new_names)

    
      # cn            <- colnames(tmp0)
      # cn            <- gsub(pattern = "-",  replacement = "_", x = cn)
      # cn            <- gsub(pattern = "\\.",replacement = "_", x = cn)
      # cn            <- gsub(pattern = paste(SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],"_",sep=""), replacement = "", x = cn)
      
      # cn            <- gsub(pattern = "battery",                           replacement = "battery",                  x=cn)
      # cn            <- gsub(pattern = "calibration",                       replacement = "valve",                    x=cn)
      # cn            <- gsub(pattern = "senseair_hpp_co2_filtered",         replacement = "hpp_co2",                  x=cn)
      # cn            <- gsub(pattern = "senseair_hpp_ir_signal",            replacement = "hpp_ir",                   x=cn)
      # cn            <- gsub(pattern = "senseair_hpp_lpl_signal",           replacement = "hpp_lpl",                  x=cn)
      # cn            <- gsub(pattern = "senseair_hpp_ntc5_diff_temp",       replacement = "hpp_ntc5_dT",              x=cn)
      # cn            <- gsub(pattern = "senseair_hpp_ntc6_se_temp",         replacement = "hpp_ntc6_T",               x=cn)
      # cn            <- gsub(pattern = "senseair_hpp_status",               replacement = "hpp_status",               x=cn)
      # cn            <- gsub(pattern = "senseair_hpp_pressure_filtered",    replacement = "hpp_pressure",             x=cn)
      # cn            <- gsub(pattern = "senseair_hpp_temperature_detector", replacement = "hpp_temperature_detector", x=cn)
      # cn            <- gsub(pattern = "senseair_hpp_temperature_mcu",      replacement = "hpp_temperature_mcu",      x=cn)
      # cn            <- gsub(pattern = "sensirion_sht21_humidity",          replacement = "sht21_RH",                 x=cn)
      # cn            <- gsub(pattern = "sensirion_sht21_temperature",       replacement = "sht21_T",                  x=cn)
      # cn            <- gsub(pattern = "time",                              replacement = "date",                     x=cn)
      
      # colnames(tmp0) <- cn
      
      
      # # Ensure that all required columns are defined
      
      # cn_required   <- c("date","battery","valve","hpp_ir","hpp_co2","hpp_pressure","hpp_status","hpp_temperature_mcu","sht21_RH","sht21_T")
      # n_cn_required <- length(cn_required)
      
      # sensor_data <- as.data.frame(matrix(NA,ncol=n_cn_required,nrow=dim(tmp0)[1]),stringsAsFactors=F)
      
      # for(ith_cn_req in 1:n_cn_required){
      #   pos_tmp0 <- which(colnames(tmp0)==cn_required[ith_cn_req])
      #   if(length(pos_tmp0)>0){
      #     sensor_data[,ith_cn_req] <- tmp0[,pos_tmp0]
      #   }else{
      #     sensor_data[,ith_cn_req] <- NA
      #   }
      # }
      
      # colnames(sensor_data) <- cn_required
      
   

      # rm(cn_required,n_cn_required,tmp0)
      # gc()
      
      
      # Apply pressure calibration
      
      for(ith_sensor in 1:n_id_sensors){
        
        if(id_cal_pressure[ith_sensor]==-999){
          next
        }
        
        #
        
        data2cal   <- rep(T,dim(sensor_data)[1])
        
        # status
        data2cal   <- data2cal & sensor_data$hpp_status == 0
        
        # All sensors provide data
        data2cal   <- data2cal & !is.na(sensor_data$hpp_pressure)
        
        # sensor period
        data2cal   <- data2cal & sensor_data$date>=tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]] & sensor_data$date<=tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]]
        
        
        sensor_data$hpp_pressure[data2cal] <- sensor_data$hpp_pressure[data2cal] / 1e2
        
        sensor_data$hpp_pressure[data2cal] <- tbl_calPar$PAR_00[id_cal_pressure[ith_sensor]] + tbl_calPar$PAR_01[id_cal_pressure[ith_sensor]] * sensor_data$hpp_pressure[data2cal]
      }
      
      print(paste("Before averaging",nrow(sensor_data)))
    

      # Force time to full preceding minute / check data imported from Influx-DB (no duplicates)
      sensor_data <- dplyr::arrange(sensor_data, date)
      #sensor_data <- openair::timeAverage(sensor_data, avg.time='1 min')
      
      lubridate::second(sensor_data$date) <- 0
      sensor_data$timestamp <- as.numeric(sensor_data$date)
      # Add location name
      sensor_data$LocationName <- tbl_deployment$LocationName[id_depl[ith_depl]]
      
      # sensor_data           <- sensor_data[c(T,diff(sensor_data$timestamp)>20),]
      # sensor_data           <- sensor_data[order(sensor_data$date),]
      # sensor_data$date      <- as.POSIXct(sensor_data$date)
      
      #sensor_data$timestamp <- as.numeric(difftime(time1=sensor_data$date,time2=lubridate::origin))
      #sensor_data           <- sensor_data[c(T,diff(sensor_data$timestamp)>50),]

      sensor_data$date      <- strptime(strftime(sensor_data$date,"%Y%m%d%H%M00",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
      # sensor_data$timestamp <- as.numeric(difftime(time1=sensor_data$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
      sensor_data           <- sensor_data[!duplicated(sensor_data$date),]
      print(paste("After averaging",nrow(sensor_data)))
      # Set CO2 measurements to NA if status !=0
      
      id_set_to_NA <- which(!sensor_data$hpp_status%in%c(0,32))
     
      if(length(id_set_to_NA)){
        sensor_data$hpp_co2[id_set_to_NA] <- NA
      }
      
      id_set_to_NA <- which(sensor_data$hpp_status==32)
      if(length(id_set_to_NA)>0){
        write.table(x = sensor_data[id_set_to_NA,],file = paste("/project/CarboSense/",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],".csv",sep=""),sep=";",col.names=T,row.names=F)
      }
      
      
      # Set CO2 measurements to NA if malfunctioning is known for specific period
      
      id_SEP_SU       <- which(tbl_SEP$SensorUnit_ID ==SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]
                               & tbl_SEP$Type == "HPP")
      
      n_id_SEP_SU     <- length(id_SEP_SU)
      
      sensor_data$SEP <- F
      
      if(n_id_SEP_SU > 0){
        
        for(ith_id_SEP_SU in 1:n_id_SEP_SU){
          id_setToNA <- which(  sensor_data$date>=tbl_SEP$Date_UTC_from[id_SEP_SU[ith_id_SEP_SU]]
                                & sensor_data$date<=tbl_SEP$Date_UTC_to[id_SEP_SU[ith_id_SEP_SU]])
          
          if(length(id_setToNA)>0){
            sensor_data$SEP[id_setToNA] <- T
          }
        }
      }
      
      rm(id_SEP_SU,n_id_SEP_SU,id_setToNA)
      gc()
      
      #Absolute humidity
      sensor_data$sht21_H2O <- carboutil::relative_to_absolute_humidity(sensor_data$sht21_RH, sensor_data$sht21_T+273.15, sensor_data$hpp_pressure*1e2)
      # BCP
      print("Applying BCP")
      if(!is.null(tbl_refGasCylDepl)){
        
        sensor_data$CO2        <- NA
        sensor_data$CylinderID <- NA
        
        for(ith_rgcd in 1:dim(tbl_refGasCylDepl)[1]){
          
          id <- which(sensor_data$valve==1
                      & sensor_data$date>=tbl_refGasCylDepl$Date_UTC_from[ith_rgcd]
                      & sensor_data$date<=tbl_refGasCylDepl$Date_UTC_to[ith_rgcd])
          
          if(length(id)>0){
            sensor_data$CO2[id]        <- tbl_refGasCylDepl$CO2[ith_rgcd] * sensor_data$hpp_pressure[id]/1013.25
            sensor_data$CylinderID[id] <- tbl_refGasCylDepl$CylinderID[ith_rgcd]
          }
        }
        
        sensor_data$valveNo <- 0
        dplyr::mutate(sensor_data,
        d=c(0,diff(coalesce(valve,0))),
        valveNo = cumsum(d==1))
        #sensor_data$valveNo[sensor_data$valve==1] <- cumsum( c(0,as.numeric(diff(sensor_data$timestamp[sensor_data$valve==1])>600))  )
        
      }
      
      
      # Apply calibration for individual sensor
      sensor_data$CO2_CAL         <- NA
      sensor_data$CO2_CAL_BCP     <- NA
      sensor_data$CO2_CAL_DRY     <- NA
      sensor_data$CO2_CAL_BCP_DRY <- NA
      
      sensor_data$days  <- as.numeric(difftime(time1=sensor_data$date,time2=strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="days"))
      
      
      for(ith_sensor in 1:n_id_sensors){
        
        if(id_cal[ith_sensor]==-999){
          next
        }
        
        #
        
        data2cal   <- rep(T,dim(sensor_data)[1])
        
        # status
        data2cal   <- data2cal & sensor_data$hpp_status == 0
        
        # All sensors provide data
        data2cal   <- data2cal & !is.na(sensor_data$hpp_ir) & !is.na(sensor_data$sht21_T) & !is.na(sensor_data$sht21_RH) & !is.na(sensor_data$valve) & !is.na(sensor_data$hpp_pressure) & !is.na(sensor_data$hpp_temperature_mcu)
        
        # sensor period
        data2cal   <- data2cal & sensor_data$date>=tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]] & sensor_data$date<=tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]]
        

        # apply model to mean
        
        if(sum(data2cal)>0){
          
          if(CalibrationModelName=="HPP_CO2_IR_CP1_pIR_SHTT_H2O_DRIFT"){
            
            sensor_data$CO2_CAL[data2cal] <- (    tbl_calPar$PAR_00[id_cal[ith_sensor]]
                                                  + tbl_calPar$PAR_01[id_cal[ith_sensor]] * -log(sensor_data$hpp_ir[data2cal])
                                                  + tbl_calPar$PAR_02[id_cal[ith_sensor]] * 1/sensor_data$hpp_ir[data2cal]
                                                  + tbl_calPar$PAR_03[id_cal[ith_sensor]] * (sensor_data$hpp_pressure[data2cal]-1013.25)/1013.25
                                                  + tbl_calPar$PAR_04[id_cal[ith_sensor]] * ((sensor_data$hpp_pressure[data2cal]-1013.25)/1013.25)/sensor_data$hpp_ir[data2cal]
                                                  + tbl_calPar$PAR_05[id_cal[ith_sensor]] *  sensor_data$sht21_T[data2cal]
                                                  + tbl_calPar$PAR_06[id_cal[ith_sensor]] *  sensor_data$sht21_H2O[data2cal]
                                                  + tbl_calPar$PAR_07[id_cal[ith_sensor]] *  sensor_data$days[data2cal])
          }
          
          if(CalibrationModelName=="HPP_CO2_IR_CP1_pIR_SHTT_H2O_DCAL_PLF14d"){
            
            last_par <- which(colnames(tbl_calPar)=="PAR_00") - 1 + tbl_calPar$N_PAR[id_cal[ith_sensor]]
            
            sensor_data$CO2_CAL[data2cal] <- (      tbl_calPar$PAR_00[id_cal[ith_sensor]]   * -log(sensor_data$hpp_ir[data2cal])
                                                    + tbl_calPar$PAR_01[id_cal[ith_sensor]]   * 1/sensor_data$hpp_ir[data2cal]
                                                    + tbl_calPar$PAR_02[id_cal[ith_sensor]]   * (sensor_data$hpp_pressure[data2cal]-1013.25)/1013.25
                                                    + tbl_calPar$PAR_03[id_cal[ith_sensor]]   * ((sensor_data$hpp_pressure[data2cal]-1013.25)/1013.25)/sensor_data$hpp_ir[data2cal]
                                                    + tbl_calPar$PAR_04[id_cal[ith_sensor]]   *  sensor_data$sht21_T[data2cal]
                                                    + tbl_calPar$PAR_05[id_cal[ith_sensor]]   *  sensor_data$sht21_H2O[data2cal]
                                                    + tbl_calPar$PAR_06[id_cal[ith_sensor]]   *  as.numeric(sensor_data$valve[data2cal])
                                                    + tbl_calPar[id_cal[ith_sensor],last_par] *  rep(1,sum(data2cal)))
            
          }
          
          if(CalibrationModelName=="HPP_CO2_IR_CP1_pIR_SHTT_DCAL_PLF14d"){
            
            last_par <- which(colnames(tbl_calPar)=="PAR_00") - 1 + tbl_calPar$N_PAR[id_cal[ith_sensor]]
            
            sensor_data$CO2_CAL[data2cal] <- (      tbl_calPar$PAR_00[id_cal[ith_sensor]]   * -log(sensor_data$hpp_ir[data2cal])
                                                    + tbl_calPar$PAR_01[id_cal[ith_sensor]]   * 1/sensor_data$hpp_ir[data2cal]
                                                    + tbl_calPar$PAR_02[id_cal[ith_sensor]]   * (sensor_data$hpp_pressure[data2cal]-1013.25)/1013.25
                                                    + tbl_calPar$PAR_03[id_cal[ith_sensor]]   * ((sensor_data$hpp_pressure[data2cal]-1013.25)/1013.25)/sensor_data$hpp_ir[data2cal]
                                                    + tbl_calPar$PAR_04[id_cal[ith_sensor]]   *  sensor_data$sht21_T[data2cal]
                                                    + tbl_calPar$PAR_05[id_cal[ith_sensor]]   *  as.numeric(sensor_data$valve[data2cal])
                                                    + tbl_calPar[id_cal[ith_sensor],last_par] *  rep(1,sum(data2cal)))
            
          }
          
          # apply BCP
          
          sensor_data_ok <- rep(T,dim(sensor_data)[1])
          
          if(!is.null(tbl_refGasCylDepl)){
            
            u_valveNo   <- sort(unique(sensor_data$valveNo[sensor_data$valve==1]))
            n_u_valveNo <- length(u_valveNo)
            
            for(ith_vno in 1:n_u_valveNo){
              id   <- which(u_valveNo[ith_vno] == sensor_data$valveNo)
              n_id <- length(id)
              
              if(n_id>0){
                
                timestamp_BCP_start <- min(sensor_data$timestamp[id])
                timestamp_BCP_end   <- max(sensor_data$timestamp[id])
                
                sensor_data_ok[which(sensor_data$timestamp>=timestamp_BCP_start & (sensor_data$timestamp<=(timestamp_BCP_start + 120)))] <- F
                sensor_data_ok[which(sensor_data$timestamp> timestamp_BCP_end   & (sensor_data$timestamp<=(timestamp_BCP_end   + 300)))] <- F
                
                id   <- id[sensor_data_ok[id] & !is.na(sensor_data$CO2[id]) & !is.na(sensor_data$CO2_CAL[id])]
                n_id <- length(id)
              }
              
              if(n_id>5){
                BCP_df <- rbind(BCP_df,data.frame(SensorUnit_ID  = SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],
                                                  Sensor         = tbl_sensors$Serialnumber[id_sensors[ith_sensor]],
                                                  Type           = tbl_sensors$Type[id_sensors[ith_sensor]],
                                                  LocationName   = tbl_deployment$LocationName[id_depl[ith_depl]],
                                                  CylinderID     = sensor_data$CylinderID[id[1]],
                                                  NoOfDepl       = ith_depl,
                                                  CalibrationModelName = CalibrationModelName,
                                                  timestamp      = mean(sensor_data$timestamp[id]),
                                                  date           = strftime(min(sensor_data$date[id]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                  valid          = all(sensor_data$SEP[id]==F),
                                                  MEAN_CO2       = mean(sensor_data$CO2[id]),
                                                  MEDIAN_CO2     = median(sensor_data$CO2[id]),
                                                  MEAN_CO2_CAL   = mean(sensor_data$CO2_CAL[id]),
                                                  MEDIAN_CO2_CAL = median(sensor_data$CO2_CAL[id]),
                                                  N              = n_id,
                                                  RH_MIN         = min(sensor_data$sht21_RH[id]),
                                                  RH_FIRST       = sensor_data$sht21_RH[id[1]],
                                                  RH_LAST        = sensor_data$sht21_RH[id[n_id]],
                                                  H2O_MIN        = min(sensor_data$sht21_H2O[id]),
                                                  H2O_FIRST      = sensor_data$sht21_H2O[id[1]],
                                                  H2O_LAST       = sensor_data$sht21_H2O[id[n_id]],
                                                  T_MIN          = min(sensor_data$sht21_T[id]),
                                                  T_FIRST        = sensor_data$sht21_T[id[1]],
                                                  T_LAST         = sensor_data$sht21_T[id[n_id]],
                                                  stringsAsFactors = F))
              }

            }
            
            
            if(!is.null(BCP_df)){
              
              id_BCP_df <- which(BCP_df$SensorUnit_ID == SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]
                                 & BCP_df$Sensor      == tbl_sensors$Serialnumber[id_sensors[ith_sensor]]
                                 & BCP_df$NoOfDepl    == ith_depl
                                 & BCP_df$valid       == T)
              
              n_id_BCP_df <- length(id_BCP_df)
              
              if(n_id_BCP_df > 0){
                
                id_BCP_df <- id_BCP_df[order(BCP_df$timestamp[id_BCP_df])]
                
                if(n_id_BCP_df>=2){
                  BCP_correction                    <- approx(x=BCP_df$timestamp[id_BCP_df],y=BCP_df$MEDIAN_CO2[id_BCP_df]-BCP_df$MEDIAN_CO2_CAL[id_BCP_df],xout=sensor_data$timestamp[data2cal],method="linear",rule = 2)
                  sensor_data$CO2_CAL_BCP[data2cal] <- sensor_data$CO2_CAL[data2cal] + BCP_correction$y
                }
                
                if(n_id_BCP_df==1){
                  sensor_data$CO2_CAL_BCP[data2cal] <- sensor_data$CO2_CAL[data2cal] + (BCP_df$MEDIAN_CO2[id_BCP_df]-BCP_df$MEDIAN_CO2_CAL[id_BCP_df])
                }
              }
            }
          }
          
          
       
          
          # CO2_cal --> [mol/mol]
          
          # pressure
          sensor_data$CO2_CAL[data2cal]     <- sensor_data$CO2_CAL[data2cal]     * (rep(1013.25,sum(data2cal)) / sensor_data$hpp_pressure[data2cal])
          sensor_data$CO2_CAL_BCP[data2cal] <- sensor_data$CO2_CAL_BCP[data2cal] * (rep(1013.25,sum(data2cal)) / sensor_data$hpp_pressure[data2cal])
          
          
          # WET --> DRY
          sensor_data$CO2_CAL_DRY[data2cal]     <- sensor_data$CO2_CAL[data2cal]     / (1 - sensor_data$sht21_H2O[data2cal]/100)
          sensor_data$CO2_CAL_BCP_DRY[data2cal] <- sensor_data$CO2_CAL_BCP[data2cal] / (1 - sensor_data$sht21_H2O[data2cal]/100)
        }
      }
      
      # 
      
      # if(!is.null(SelectedLocationName) & SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]!=443){
      #   next
      # }
    
      # Delete all the data from this SensorUnit from table "CarboSense_CO2" (or InsertDBTableName)
      
      if(PARTIAL_COMPUTATION==T){
        # if(Computation_timestamp_from <= tbl_deployment$timestamp_from[id_depl[ith_depl]]){
        #   Computation_timestamp_from_DB <- tbl_deployment$timestamp_from[id_depl[ith_depl]]
        # }

        Computation_timestamp_from_DB <- min(c( tbl_deployment$timestamp_from[id_depl[ith_depl]], Computation_timestamp_from))

        query_str <- stringr::str_interp("DELETE FROM ${InsertDBTableName} WHERE SensorUnit_ID = ? AND LocationName = ? AND timestamp >= ?;")
        query_params <- list(SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc], tbl_deployment$LocationName[id_depl[ith_depl]], Computation_timestamp_from_DB)
        #query_str <- paste("DELETE FROM ",InsertDBTableName," WHERE SensorUnit_ID = ",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]," and LocationName = '",tbl_deployment$LocationName[id_depl[ith_depl]],"' ",sep="")
        #query_str <- paste(query_str, "AND timestamp >= ",Computation_timestamp_from_DB,";",sep="")
      }else{
        query_str <- stringr::str_interp("DELETE FROM ${InsertDBTableName} WHERE SensorUnit_ID = ? AND LocationName = ? AND timestamp >= ? AND timestamp <= ?;")
        query_params <- list(SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc], tbl_deployment$LocationName[id_depl[ith_depl]], tbl_deployment$timestamp_from[id_depl[ith_depl]], tbl_deployment$timestamp_to[id_depl[ith_depl]])
        #query_str <- paste("DELETE FROM ",InsertDBTableName," WHERE SensorUnit_ID = ",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]," and LocationName = '",tbl_deployment$LocationName[id_depl[ith_depl]],"' ",sep="")
        #query_str <- paste(query_str, "AND timestamp >= ",tbl_deployment$timestamp_from[id_depl[ith_depl]]," AND timestamp <= ",tbl_deployment$timestamp_to[id_depl[ith_depl]],";",sep="")
      }


      
      print(paste("After",nrow(sensor_data))) 
      print(sensor_data)
      drv             <- dbDriver("MySQL")
      con <-carboutil::get_conn()
      res             <- dbSendQuery(con, query_str)
      dbBind(res, query_params)
      dbClearResult(res)
      dbDisconnect(con)
      
      # Insert data from this SensorUnit into table "CarboSense_HPP_CO2"  (or InsertDBTableName)
      
      id_insert   <- which(!is.na(sensor_data$CO2_CAL) & sensor_data_ok & !sensor_data$SEP)
      n_id_insert <- length(id_insert)
      # Substitution of NA with "-999"
      sensor_data <- mutate_at(sensor_data, c("CO2_CAL_BCP","CO2_CAL_DRY","CO2_CAL_BCP_DRY"), function(x) replace(x, is.na(x), -999) )
      #Find duplicates
      dp <- which(duplicated(sensor_data[,c("timestamp","LocationName")]))
      #
      print(n_id_insert)
      if(n_id_insert>0){
        #print(paste("TO insert", sensor_data))
        print(paste("Insert data for SU",SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc]))
        nins <- length(sensor_data$timestamp[id_insert])
        #List of values to insert
        insert_df <- tibble(
          LocationName=sensor_data$LocationName[id_insert],
          SensorUnit_ID=rep(SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],nins),
          timestamp=sensor_data$timestamp[id_insert],
          CO2_CAL=sensor_data$CO2_CAL[id_insert],
          CO2_CAL_DRY=sensor_data$CO2_CAL_DRY[id_insert],
          CO2_CAL_ADJ=sensor_data$CO2_CAL_BCP[id_insert],
          CO2_CAL_ADJ_DRY=sensor_data$CO2_CAL_BCP_DRY[id_insert],
          H2O=sensor_data$sht21_H2O[id_insert],
          Pressure=sensor_data$hpp_pressure[id_insert],
          T=sensor_data$sht21_T[id_insert],
          RH=sensor_data$sht21_RH[id_insert],
          Valve=sensor_data$valve[id_insert])
          
          con <-carboutil::get_conn()
          dbWriteTable(con, InsertDBTableName, insert_df, append=TRUE, overwrite=FALSE)
          # #Parametrised query
          # insert_query <- stringr::str_interp(
          #   "INSERT INTO ${InsertDBTableName} (`LocationName`, `SensorUnit_ID`, `timestamp`, `CO2_CAL`, `CO2_CAL_DRY`,`CO2_CAL_ADJ`,`CO2_CAL_ADJ_DRY`,`H2O`,`Pressure`,`T`,`RH`,`Valve`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
          # )
          # print(insert_query)
          
          # drv             <- dbDriver("MySQL")
          # con <-carboutil::get_conn()
          # res             <- dbSendQuery(con, query_str)
          # dbBind(res, insert_df)
          # dbFetch(res)
          # dbClearResult(res)
          # dbDisconnect(con)
        
        # insert_pos_1 <- 1
        
        # while(insert_pos_1<=n_id_insert){
          
        #   #
          
        #   insert_pos_2 <- insert_pos_1 + 1e4
          
        #   if(insert_pos_2>n_id_insert){
        #     insert_pos_2 <- n_id_insert
        #   }
          
        #   n_id_insert_partly <- insert_pos_2 - insert_pos_1 + 1
        #   id_insert_partly   <- id_insert[insert_pos_1:insert_pos_2]
          
        #   #
          
          
        #   query_str <- paste("INSERT INTO ",InsertDBTableName," (`LocationName`, `SensorUnit_ID`, `timestamp`, `CO2_CAL`, `CO2_CAL_DRY`,`CO2_CAL_ADJ`,`CO2_CAL_ADJ_DRY`,`H2O`,`Pressure`,`T`,`RH`,`Valve`) VALUES ",sep="")
        #   query_str <- paste(query_str,
        #                      paste("(",paste("'",sensor_data$LocationName[id_insert_partly],"',",
        #                                      rep(SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],n_id_insert_partly),",",
        #                                      sensor_data$timestamp[id_insert_partly],",",
        #                                      sensor_data$CO2_CAL[id_insert_partly],",",
        #                                      sensor_data$CO2_CAL_DRY[id_insert_partly],",",
        #                                      sensor_data$CO2_CAL_BCP[id_insert_partly],",",
        #                                      sensor_data$CO2_CAL_BCP_DRY[id_insert_partly],",",
        #                                      sensor_data$sht21_H2O[id_insert_partly],",",
        #                                      sensor_data$hpp_pressure[id_insert_partly],",",
        #                                      sensor_data$sht21_T[id_insert_partly],",",
        #                                      sensor_data$sht21_RH[id_insert_partly],",",
        #                                      sensor_data$valve[id_insert_partly],
        #                                      collapse = "),(",sep=""),");",sep="")
        #   )
        #   #List of values to insert
        #   insert_df <- list(
        #     sensor_data$LocationName[id_insert_partly],
        #     rep(SensorUnit_ID_2_proc[ith_SensorUnit_ID_2_proc],n_id_insert_partly),
        #     sensor_data$timestamp[id_insert_partly],
        #     sensor_data$CO2_CAL[id_insert_partly],
        #     sensor_data$CO2_CAL_DRY[id_insert_partly],
        #     sensor_data$CO2_CAL_BCP[id_insert_partly],
        #     sensor_data$CO2_CAL_BCP_DRY[id_insert_partly],
        #     sensor_data$sht21_H2O[id_insert_partly],
        #     sensor_data$hpp_pressure[id_insert_partly],
        #     sensor_data$sht21_T[id_insert_partly],
        #     sensor_data$sht21_RH[id_insert_partly],
        #     sensor_data$valve[id_insert_partly])
        #   #Parametrised query
        #   insert_query <- stringr::str_interp(
        #     "INSERT INTO ${InsertDBTableName} (`LocationName`, `SensorUnit_ID`, `timestamp`, `CO2_CAL`, `CO2_CAL_DRY`,`CO2_CAL_ADJ`,`CO2_CAL_ADJ_DRY`,`H2O`,`Pressure`,`T`,`RH`,`Valve`) VALUES 
        #     (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        #     "
        #   )
          
        #   drv             <- dbDriver("MySQL")
        #   con <-carboutil::get_conn()
        #   res             <- dbSendQuery(con, query_str)
        #   dbBind(res, insert_df)
        #   dbClearResult(res)
        #   dbDisconnect(con)
          
        #   #
          
        #   insert_pos_1 <- insert_pos_1 + 1e4 + 1
        # }
      }
    }
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

# Label dates
print("Labeling")
label_date_00   <- strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC")
label_date_01   <- strptime("20220101000000","%Y%m%d%H%M%S",tz="UTC")

#label_date_01   <- max(sensor_data$date)
label_dates     <- seq(label_date_00,label_date_01,by = "month")
label_dates_str <- strftime(label_dates,"%m/%y",tz="UTC")

### ----------------------------------------------------------------------------------------------------------------------------

# Figure: Time series of differences between CO2,CYL and CO2,CAL

def_par <- par()
pdf(file = figname_HPP_CALIBRATIONS, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,1))


for(ith_depl in 1:dim(tbl_deployment)[1]){
  
  id_ok    <- which(BCP_df$SensorUnit_ID    == tbl_deployment$SensorUnit_ID[ith_depl]
                    & BCP_df$LocationName   == tbl_deployment$LocationName[ith_depl]
                    & BCP_df$timestamp      >= tbl_deployment$timestamp_from[ith_depl]
                    & BCP_df$timestamp      <= tbl_deployment$timestamp_to[ith_depl]
                    & BCP_df$valid == T)
  
  id_nok   <- which(BCP_df$SensorUnit_ID    == tbl_deployment$SensorUnit_ID[ith_depl]
                    & BCP_df$LocationName   == tbl_deployment$LocationName[ith_depl]
                    & BCP_df$timestamp      >= tbl_deployment$timestamp_from[ith_depl]
                    & BCP_df$timestamp      <= tbl_deployment$timestamp_to[ith_depl]
                    & BCP_df$valid == F)
  
  n_id_ok  <- length(id_ok)
  n_id_nok <- length(id_nok)
  
  if(n_id_ok==0){
    next
  }
  
  
  id       <- sort(c(id_ok,id_nok))
  n_id     <- length(id)
  
  minDate <- as.POSIXct(BCP_df$date[id[1]])
  maxDate <- as.POSIXct(BCP_df$date[id[n_id]])
  minTS   <- BCP_df$timestamp[id[1]]
  maxTS   <- BCP_df$timestamp[id[n_id]]
  
  fit  <- lm(y~x,data.frame(x=BCP_df$timestamp[id_ok],
                            y=BCP_df$MEDIAN_CO2[id_ok]-BCP_df$MEDIAN_CO2_CAL[id_ok],
                            stringsAsFactors = F))
  
  RMSE <- sqrt( sum( (fit$residuals)^2 ) / n_id_ok )
  
  #
  
  tmp1 <- min(BCP_df$MEDIAN_CO2[id_ok]-BCP_df$MEDIAN_CO2_CAL[id_ok])
  tmp2 <- max(BCP_df$MEDIAN_CO2[id_ok]-BCP_df$MEDIAN_CO2_CAL[id_ok])
  tmp0 <- 0.5*(tmp1+tmp2)
  
  yrange  <- c(tmp0 - 1.25*(tmp0-tmp1),tmp0 + 1.25*(tmp2-tmp0))
  
  mainStr <- paste("HPP",tbl_deployment$SensorUnit_ID[ith_depl],"@",tbl_deployment$LocationName[ith_depl],strftime(tbl_deployment$Date_UTC_from[ith_depl],"%d/%m/%Y",tz="UTC"),"-",strftime(tbl_deployment$Date_UTC_to[ith_depl],"%d/%m/%Y",tz="UTC"))
  
  plot(as.POSIXct(BCP_df$date[id]),BCP_df$MEDIAN_CO2[id]-BCP_df$MEDIAN_CO2_CAL[id],ylim=yrange,cex.lab=1.25,cex.axis=1.25,cex.main=1.25,col=1,pch=16,cex=1,xlab="Date",ylab="CO2 CYL - CO2 CAL [ppm]",main=mainStr,xaxt="n")
  axis(side = 1,at = label_dates,labels = label_dates_str,cex.axis=1.25,cex.lab=1.25)
  
  if(n_id_ok>0){
    points(as.POSIXct(BCP_df$date[id_nok]),BCP_df$MEDIAN_CO2[id_nok]-BCP_df$MEDIAN_CO2_CAL[id_nok],col=2,pch=16,cex=1)
  }
  
  lines(c(minDate,maxDate),fit$coefficients[1]+c(minTS,maxTS)*fit$coefficients[2],lwd=1,col=2,lty=5)
  
  
  leg_str_1 <- paste("SLOPE:",sprintf("%5.2f",fit$coefficients[2]*30*86400),"[ppm/month]")
  leg_str_2 <- paste("RMSE: ",sprintf("%5.2f",RMSE),"[ppm]")
  
  par(family="mono")
  legend("top",legend=c(leg_str_1,leg_str_2),cex=1.25,bg="white")
  par(family="")
  
}


dev.off()
par(def_par)


### ----------------------------------------------------------------------------------------------------------------------------

# Figure: Time series of humidity (SHT21) during calibration

def_par <- par()
pdf(file = figname_HPP_CALIBRATIONS_HUMIDITY, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,1))



for(ith_depl in 1:dim(tbl_deployment)[1]){
  
  id_ok    <- which(BCP_df$SensorUnit_ID    == tbl_deployment$SensorUnit_ID[ith_depl]
                    & BCP_df$LocationName   == tbl_deployment$LocationName[ith_depl]
                    & BCP_df$timestamp      >= tbl_deployment$timestamp_from[ith_depl]
                    & BCP_df$timestamp      <= tbl_deployment$timestamp_to[ith_depl]
                    & BCP_df$valid == T)
  
  id_nok   <- which(BCP_df$SensorUnit_ID    == tbl_deployment$SensorUnit_ID[ith_depl]
                    & BCP_df$LocationName   == tbl_deployment$LocationName[ith_depl]
                    & BCP_df$timestamp      >= tbl_deployment$timestamp_from[ith_depl]
                    & BCP_df$timestamp      <= tbl_deployment$timestamp_to[ith_depl]
                    & BCP_df$valid == F)
  
  n_id_ok  <- length(id_ok)
  n_id_nok <- length(id_nok)
  
  if(n_id_ok==0){
    next
  }
  
  id       <- sort(c(id_ok,id_nok))
  n_id     <- length(id)
  
  minDate <- as.POSIXct(BCP_df$date[id[1]])
  maxDate <- as.POSIXct(BCP_df$date[id[n_id]])
  minTS   <- BCP_df$timestamp[id[1]]
  maxTS   <- BCP_df$timestamp[id[n_id]]
  
  #
  
  fit <- lm(RH~date,data.frame(date = BCP_df$timestamp[id_ok],
                               RH   = BCP_df$RH_MIN[id_ok],
                               stringsAsFactors = T))
  
  #
  
  yrange  <- c(-5,20)
  
  mainStr <- paste("HPP",tbl_deployment$SensorUnit_ID[ith_depl],"@",tbl_deployment$LocationName[ith_depl],strftime(tbl_deployment$Date_UTC_from[ith_depl],"%d/%m/%Y",tz="UTC"),"-",strftime(tbl_deployment$Date_UTC_to[ith_depl],"%d/%m/%Y",tz="UTC"))
  
  print(BCP_df$date[id])
  plot(  as.POSIXct(BCP_df$date[id]),BCP_df$RH_MIN[id],ylim=yrange,cex.lab=1.25,cex.axis=1.25,cex.main=1.25,col=2,pch=15,cex=0.5,xlab="Date",ylab="RH [%]",main=mainStr,xaxt="n")
  axis(side = 1,at = label_dates,labels = label_dates_str,cex.axis=1.25,cex.lab=1.25)
  
  if(n_id_ok>0){
    for(ii in 1:n_id_ok){
      lines(c(as.POSIXct(BCP_df$date[id_ok[ii]]),as.POSIXct(BCP_df$date[id_ok[ii]])),c(BCP_df$RH_FIRST[id_ok[ii]],BCP_df$RH_LAST[id_ok[ii]]),lwd=1,col=1)
    }
    points(as.POSIXct(BCP_df$date[id_ok]),BCP_df$RH_MIN[id_ok],ylim=yrange,cex.lab=1.25,cex.axis=1.25,cex.main=1.25,col=1,pch=15,cex=1.0)
  }
  
  if(n_id_nok>0){
    for(ii in 1:n_id_ok){
      lines(c(as.POSIXct(BCP_df$date[id_nok[ii]]),as.POSIXct(BCP_df$date[id_nok[ii]])),c(BCP_df$RH_FIRST[id_nok[ii]],BCP_df$RH_LAST[id_nok[ii]]),lwd=1,col="gray50")
    }
    points(as.POSIXct(BCP_df$date[id_nok]),BCP_df$RH_MIN[id_nok],ylim=yrange,cex.lab=1.25,cex.axis=1.25,cex.main=1.25,col=2,pch=15,cex=1.0)
  }
  
  lines(c(minDate,maxDate),c( 0.0, 0.0),lwd=1,col=2,lty=1)
  lines(c(minDate,maxDate),c(-2.0,-2.0),lwd=1,col=2,lty=5)
  lines(c(minDate,maxDate),c( 2.0, 2.0),lwd=1,col=2,lty=5)
  
  lines(c(minDate,maxDate),fit$coefficients[1] + c(minTS,maxTS)*fit$coefficients[2],col=4,lwd=1,lty=5)
  
  leg_str_1 <- paste("Drift:",sprintf("%6.2f",fit$coefficients[2]*86400*365),"[%/yr]")
  par(family="mono")
  legend("topright",legend=c(leg_str_1),bg="white",cex=1.25)
  par(family="")
  
}


dev.off()
par(def_par)

### ----------------------------------------------------------------------------------------------------------------------------

# Figure: Time series of H2O (computed from SHT21 T, SHT21 RH, pressure) during calibration

def_par <- par()
pdf(file = figname_HPP_CALIBRATIONS_H2O, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,1))


for(ith_depl in 1:dim(tbl_deployment)[1]){
  
  id_ok    <- which(BCP_df$SensorUnit_ID    == tbl_deployment$SensorUnit_ID[ith_depl]
                    & BCP_df$LocationName   == tbl_deployment$LocationName[ith_depl]
                    & BCP_df$timestamp      >= tbl_deployment$timestamp_from[ith_depl]
                    & BCP_df$timestamp      <= tbl_deployment$timestamp_to[ith_depl]
                    & BCP_df$valid == T)
  
  id_nok   <- which(BCP_df$SensorUnit_ID    == tbl_deployment$SensorUnit_ID[ith_depl]
                    & BCP_df$LocationName   == tbl_deployment$LocationName[ith_depl]
                    & BCP_df$timestamp      >= tbl_deployment$timestamp_from[ith_depl]
                    & BCP_df$timestamp      <= tbl_deployment$timestamp_to[ith_depl]
                    & BCP_df$valid == F)
  
  n_id_ok  <- length(id_ok)
  n_id_nok <- length(id_nok)
  
  if(n_id_ok==0){
    next
  }
  
  id       <- sort(c(id_ok,id_nok))
  n_id     <- length(id)
  
  minDate <- as.POSIXct(BCP_df$date[id[1]])
  maxDate <- as.POSIXct(BCP_df$date[id[n_id]])
  minTS   <- BCP_df$timestamp[id[1]]
  maxTS   <- BCP_df$timestamp[id[n_id]]
  
  #
  
  yrange  <- c(-0.2,0.2)
  
  mainStr <- paste("HPP",tbl_deployment$SensorUnit_ID[ith_depl],"@",tbl_deployment$LocationName[ith_depl],strftime(tbl_deployment$Date_UTC_from[ith_depl],"%d/%m/%Y",tz="UTC"),"-",strftime(tbl_deployment$Date_UTC_to[ith_depl],"%d/%m/%Y",tz="UTC"))
  
  
  plot(as.POSIXct(BCP_df$date[id]),BCP_df$H2O_MIN[id],ylim=yrange,cex.lab=1.25,cex.axis=1.25,cex.main=1.25,col=2,pch=15,cex=0.5,xlab="Date",ylab="H2O [Vol-%]",main=mainStr,xaxt="n")
  axis(side = 1,at = label_dates,labels = label_dates_str,cex.axis=1.25,cex.lab=1.25)
  
  if(n_id_ok>0){
    for(ii in 1:n_id_ok){
      lines(c(as.POSIXct(BCP_df$date[id_ok[ii]]),as.POSIXct(BCP_df$date[id_ok[ii]])),c(BCP_df$H2O_FIRST[id_ok[ii]],BCP_df$H2O_LAST[id_ok[ii]]),lwd=1,col=1)
    }
    points(as.POSIXct(BCP_df$date[id_ok]),BCP_df$H2O_MIN[id_ok],ylim=yrange,cex.lab=1.25,cex.axis=1.25,cex.main=1.25,col=1,pch=15,cex=1.0)
  }
  
  if(n_id_nok>0){
    for(ii in 1:n_id_ok){
      lines(c(as.POSIXct(BCP_df$date[id_nok[ii]]),as.POSIXct(BCP_df$date[id_nok[ii]])),c(BCP_df$H2O_FIRST[id_nok[ii]],BCP_df$H2O_LAST[id_nok[ii]]),lwd=1,col="gray50")
    }
    points(as.POSIXct(BCP_df$date[id_nok]),BCP_df$H2O_MIN[id_nok],ylim=yrange,cex.lab=1.25,cex.axis=1.25,cex.main=1.25,col=2,pch=15,cex=1.0)
  }
  
  lines(c(minDate,maxDate),c( 0.0, 0.0),lwd=1,col=2,lty=1)
  
}


dev.off()
par(def_par)

### ----------------------------------------------------------------------------------------------------------------------------

# Figure: Time series of T (SHT21) during calibration

def_par <- par()
pdf(file = figname_HPP_CALIBRATIONS_T, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,1))


for(ith_depl in 1:dim(tbl_deployment)[1]){
  
  id_ok    <- which(BCP_df$SensorUnit_ID    == tbl_deployment$SensorUnit_ID[ith_depl]
                    & BCP_df$LocationName   == tbl_deployment$LocationName[ith_depl]
                    & BCP_df$timestamp      >= tbl_deployment$timestamp_from[ith_depl]
                    & BCP_df$timestamp      <= tbl_deployment$timestamp_to[ith_depl]
                    & BCP_df$valid == T)
  
  id_nok   <- which(BCP_df$SensorUnit_ID    == tbl_deployment$SensorUnit_ID[ith_depl]
                    & BCP_df$LocationName   == tbl_deployment$LocationName[ith_depl]
                    & BCP_df$timestamp      >= tbl_deployment$timestamp_from[ith_depl]
                    & BCP_df$timestamp      <= tbl_deployment$timestamp_to[ith_depl]
                    & BCP_df$valid == F)
  
  n_id_ok  <- length(id_ok)
  n_id_nok <- length(id_nok)
  
  if(n_id_ok==0){
    next
  }
  
  id       <- sort(c(id_ok,id_nok))
  n_id     <- length(id)
  
  minDate <- as.POSIXct(BCP_df$date[id[1]])
  maxDate <- as.POSIXct(BCP_df$date[id[n_id]])
  minTS   <- BCP_df$timestamp[id[1]]
  maxTS   <- BCP_df$timestamp[id[n_id]]
  
  #
  
  yrange  <- range(c(BCP_df$T_FIRST[id],BCP_df$T_LAST[id],BCP_df$T_MIN[id]))
  
  mainStr <- paste("HPP",tbl_deployment$SensorUnit_ID[ith_depl],"@",tbl_deployment$LocationName[ith_depl],strftime(tbl_deployment$Date_UTC_from[ith_depl],"%d/%m/%Y",tz="UTC"),"-",strftime(tbl_deployment$Date_UTC_to[ith_depl],"%d/%m/%Y",tz="UTC"))
  
  
  plot(as.POSIXct(BCP_df$date[id]),BCP_df$T_MIN[id],ylim=yrange,cex.lab=1.25,cex.axis=1.25,cex.main=1.25,col=2,pch=15,cex=0.5,xlab="Date",ylab="T [deg C]",main=mainStr,xaxt="n")
  axis(side = 1,at = label_dates,labels = label_dates_str,cex.axis=1.25,cex.lab=1.25)
  
  if(n_id_ok>0){
    for(ii in 1:n_id_ok){
      lines(c(as.POSIXct(BCP_df$date[id_ok[ii]]),as.POSIXct(BCP_df$date[id_ok[ii]])),c(BCP_df$T_FIRST[id_ok[ii]],BCP_df$T_LAST[id_ok[ii]]),lwd=1,col=1)
    }
    points(as.POSIXct(BCP_df$date[id_ok]),BCP_df$T_MIN[id_ok],ylim=yrange,cex.lab=1.25,cex.axis=1.25,cex.main=1.25,col=1,pch=15,cex=1.0)
  }
  
  if(n_id_nok>0){
    for(ii in 1:n_id_ok){
      lines(c(as.POSIXct(BCP_df$date[id_nok[ii]]),as.POSIXct(BCP_df$date[id_nok[ii]])),c(BCP_df$T_FIRST[id_nok[ii]],BCP_df$T_LAST[id_nok[ii]]),lwd=1,col="gray50")
    }
    points(as.POSIXct(BCP_df$date[id_nok]),BCP_df$T_MIN[id_nok],ylim=yrange,cex.lab=1.25,cex.axis=1.25,cex.main=1.25,col=2,pch=15,cex=1.0)
  }
  
  lines(c(minDate,maxDate),c( 0.0, 0.0),lwd=1,col=2,lty=1)
  
}


dev.off()
par(def_par)

### ----------------------------------------------------------------------------------------------------------------------------

# Figure: Time series of expected cylinder pressure

def_par <- par()
pdf(file = figname_CYL_PRESSURE, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,1))

u_SU_ID   <- sort(unique(BCP_df$SensorUnit_ID))
n_u_SU_ID <- length(u_SU_ID)

#

BCP_df$CylPressure         <- NA
BCP_df$CylPressure_initial <- NA
BCP_df$cal_number          <- NA

#

for(SU_ID in u_SU_ID){
  
  
  # Initial pressure in gas cylinders
  
  query_str         <- paste("SELECT * FROM RefGasCylinder_Deployment where SensorUnit_ID=",SU_ID,";",sep="")
  drv               <- dbDriver("MySQL")
  con <-carboutil::get_conn()
  res               <- dbSendQuery(con, query_str)
  tbl_refGasCylDepl <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  tbl_refGasCylDepl$Date_UTC_from  <- strptime(tbl_refGasCylDepl$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_refGasCylDepl$Date_UTC_to    <- strptime(tbl_refGasCylDepl$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
  tbl_refGasCylDepl$timestamp_from <- as.numeric(difftime(time1=tbl_refGasCylDepl$Date_UTC_from,time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
  tbl_refGasCylDepl$timestamp_to   <- as.numeric(difftime(time1=tbl_refGasCylDepl$Date_UTC_to,  time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
  
  tbl_refGasCylDepl$InitialPressure <- NA
  
  #
  
  for(ith_refGasCylDepl in 1:dim(tbl_refGasCylDepl)[1]){
    
    query_str       <- paste("SELECT * FROM RefGasCylinder where CylinderID='",tbl_refGasCylDepl$CylinderID[ith_refGasCylDepl],"' and Date_UTC_from <= '",strftime(tbl_refGasCylDepl$Date_UTC_from[ith_refGasCylDepl],"%Y-%m-%d %H:%M:%S",tz="UTC"),"' and Date_UTC_to >= '",strftime(tbl_refGasCylDepl$Date_UTC_to[ith_refGasCylDepl],"%Y-%m-%d %H:%M:%S",tz="UTC"),"';",sep="")
    drv             <- dbDriver("MySQL")
    con <-carboutil::get_conn()
    res             <- dbSendQuery(con, query_str)
    tbl             <- dbFetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    if(dim(tbl)[1]!=1){
      stop(paste("Sensor Unit:",SU_ID, " : problem with finding attached gas cylinder with ID:",tbl_refGasCylDepl$CylinderID[ith_refGasCylDepl]))
    }
    
    tbl_refGasCylDepl$InitialPressure[ith_refGasCylDepl] <- tbl$pressure[1]
  }
  
  #
  print(tbl_refGasCylDepl)
  for(ith_refGasCylDepl in 1:dim(tbl_refGasCylDepl)[1]){
    
    id_cyl   <- which(  BCP_df$timestamp     >= tbl_refGasCylDepl$timestamp_from[ith_refGasCylDepl] 
                        & BCP_df$timestamp     <= tbl_refGasCylDepl$timestamp_to[ith_refGasCylDepl]
                        & BCP_df$SensorUnit_ID == SU_ID) 
    
    n_id_cyl <- length(id_cyl)
    
    BCP_df$CylPressure[id_cyl]         <- (tbl_refGasCylDepl$InitialPressure[ith_refGasCylDepl]*5 - (1:n_id_cyl)*7.5)/5
    BCP_df$CylPressure_initial[id_cyl] <- tbl_refGasCylDepl$InitialPressure[ith_refGasCylDepl]
    BCP_df$cal_number[id_cyl]          <- (1:n_id_cyl)
  }
  
  
  #
  
  id   <- which(BCP_df$SensorUnit_ID == SU_ID)
  n_id <- length(id)
  
  #
  
  tmp1 <- min(BCP_df$CylPressure[id])
  tmp2 <- max(BCP_df$CylPressure[id])
  tmp0 <- 0.5*(tmp1+tmp2)
  
  yrange <- c(tmp0 - 1.25*(tmp0-tmp1),tmp0 + 1.25*(tmp2-tmp0))
  
  plot(as.POSIXct(BCP_df$date[id]),BCP_df$CylPressure[id],ylim=yrange,cex.lab=1.25,cex.axis=1.25,cex.main=1.25,col=1,pch=16,cex=1,xlab="Date",ylab="Estimated cylinder pressure [bar]",main=paste("HPP",SU_ID,"/",BCP_df$LocationName[id[n_id]]),xaxt="n")
  axis(side = 1,at = label_dates,labels = label_dates_str,cex.axis=1.25,cex.lab=1.25)
  
  leg_str_1 <- paste("Current pressure:",sprintf("%5.0f",BCP_df$CylPressure[id[n_id]]),"[bar]")
  par(family="mono")
  legend("topright",legend=c(leg_str_1),bg="white")
  par(family="")
  
}

dev.off()
par(def_par)


### ----------------------------------------------------------------------------------------------------------------------------

write.table(BCP_df,file = filename_HPP_DataProcessing_csv,sep = ";",col.names=T,row.names=F)

### ----------------------------------------------------------------------------------------------------------------------------
