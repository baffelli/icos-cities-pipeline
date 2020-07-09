# Check_CarboSense_SensorHealth.r
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
library(DBI)
require(RMySQL)
require(chron)
library(data.table)
library(carboutil)

##

source("/project/CarboSense/Software/CarboSenseUtilities/api-v1.3.r")

### ----------------------------------------------------------------------------------------------------------------------------

# SensorUnits

SensorUnit_IDs   <- c(1010:1334)
n_SensorUnit_IDs <- length(SensorUnit_IDs)

### ----------------------------------------------------------------------------------------------------------------------------

# report name

report_fn <- "/project/CarboSense/Carbosense_Network/CarboSense_SensorHealth/SensorUnitHealth.csv"

### ----------------------------------------------------------------------------------------------------------------------------

# Current date

current_date      <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")

# current_date      <- strptime("20171025000000","%Y%m%d%H%M%S",tz="UTC")

current_timestamp <- as.numeric(difftime(time1=current_date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))


### ----------------------------------------------------------------------------------------------------------------------------

## Decentlab DB information

DL_DB_domain      <- "swiss.co2.live"
DL_DB_apiKey      <- "eyJrIjoiSFd4bWJhczJjclpaUnpHeXluck1WYlJ0MkdINWhneFciLCJuIjoibWljaGFlbC5tdWVsbGVyQGVtcGEuY2giLCJpZCI6MX0="

DL_DB_domain_EMPA <- "empa-503.decentlab.com"
DL_DB_apiKey_EMPA <- "eyJrIjoiWkJaZjFDTEhUYm5sNmdWUG14a3NpdVcwTmZCaHloZVEiLCJuIjoibWljaGFlbC5tdWVsbGVyQGVtcGEuY2giLCJpZCI6MX0="

### ----------------------------------------------------------------------------------------------------------------------------


con <- carboutil::get_conn()
query_str       <- paste("SELECT * FROM Deployment;",sep="")
drv             <- dbDriver("MySQL")
#con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_deployment  <- fetch(res, n=-1)
dbClearResult(res)
#dbDisconnect(con)

tbl_deployment$Date_UTC_from <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to   <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

id <- which(tbl_deployment$Date_UTC_to==strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"))

if(length(id)>0){
  tbl_deployment$Date_UTC_to[id] <- current_date
}

#

query_str       <- paste("SELECT * FROM Calibration;",sep="")
drv             <- dbDriver("MySQL")
#con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_calibration <- fetch(res, n=-1)
dbClearResult(res)
#dbDisconnect(con)

tbl_calibration$Date_UTC_from <- strptime(tbl_calibration$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_calibration$Date_UTC_to   <- strptime(tbl_calibration$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

id <- which(tbl_calibration$Date_UTC_to==strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"))

if(length(id)>0){
  tbl_calibration$Date_UTC_to[id] <- current_date
}

#

query_str       <- paste("SELECT * FROM Sensors;",sep="")
drv             <- dbDriver("MySQL")
#con             <- dbConnect(drv,group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_sensors     <- fetch(res, n=-1)
dbClearResult(res)
#dbDisconnect(con)

tbl_sensors$Date_UTC_from <- strptime(tbl_sensors$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_sensors$Date_UTC_to   <- strptime(tbl_sensors$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

#

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
#con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_location    <- fetch(res, n=-1)
dbClearResult(res)
#dbDisconnect(con)

#

query_str       <- paste("SELECT * FROM SensorUnits;",sep="")
drv             <- dbDriver("MySQL")
#con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_sensorUnits <- fetch(res, n=-1)
dbClearResult(res)
#dbDisconnect(con)

### ----------------------------------------------------------------------------------------------------------------------------


cn               <- c("SU","CurrentDate","LocationName","DateOfStartDeployment","DaysSinceStartDeployment", "DaysSinceLastCalibration","nCO2ValuesLast24h","nSHTValuesLast24h","battery","Y_LV03","X_LV03","h","LAT_WGS84","LON_WGS84","HeightAboveGround","Network","MIN_Status_LP8","MAX_Status_LP8","LAST_Status_LP8","LAST_SHT21_T","MIN_SHT21_T","MAX_SHT21_T","LAST_SHT21_RH","MIN_SHT21_RH","MAX_SHT21_RH")
report           <- as.data.frame(matrix(NA,ncol=length(cn),nrow=n_SensorUnit_IDs),stringsAsFactors=F)
colnames(report) <- cn
report           <- as.data.frame(report,stringsAsFactors=F)


for(ith_SUID in 1:n_SensorUnit_IDs){
  
  go <- T
  
  #
  
  report$SU[ith_SUID]          <- SensorUnit_IDs[ith_SUID]
  report$CurrentDate[ith_SUID] <- strftime(current_date,"%Y-%m-%d %H:%M:%S",tz="UTC")
  
  # Current deployment
  
  id_SU_depl <- which(tbl_deployment$SensorUnit_ID   == SensorUnit_IDs[ith_SUID]
                      & tbl_deployment$Date_UTC_from <= current_date
                      & tbl_deployment$Date_UTC_to   >= current_date)
  
  if(length(id_SU_depl)==0){
    report$LocationName[ith_SUID]             <- "NOT DEPLOYED"
    report$DateOfStartDeployment[ith_SUID]    <- "NA"
    report$DaysSinceStartDeployment[ith_SUID] <- -999
    report$HeightAboveGround[ith_SUID]        <- -999
  }
  
  if(length(id_SU_depl)==1){
    report$LocationName[ith_SUID]             <- tbl_deployment$LocationName[id_SU_depl]
    report$DateOfStartDeployment[ith_SUID]    <- strftime(tbl_deployment$Date_UTC_from[id_SU_depl],"%Y-%m-%d %H:%M:%S",tz="UTC")
    report$DaysSinceStartDeployment[ith_SUID] <- sprintf("%.1f",as.numeric(difftime(time1=current_date,time2=tbl_deployment$Date_UTC_from[id_SU_depl],units="days",tz="UTC")))
    report$HeightAboveGround[ith_SUID]        <- tbl_deployment$HeightAboveGround[id_SU_depl]
  }
  
  
  # Location info
  
  id_loc <- which(tbl_deployment$LocationName[id_SU_depl]==tbl_location$LocationName)
  
  if(length(id_loc)==1){
    report$Y_LV03[ith_SUID]    <- tbl_location$Y_LV03[id_loc]
    report$X_LV03[ith_SUID]    <- tbl_location$X_LV03[id_loc]
    report$h[ith_SUID]         <- tbl_location$h[id_loc]
    report$LON_WGS84[ith_SUID] <- tbl_location$LON_WGS84[id_loc]
    report$LAT_WGS84[ith_SUID] <- tbl_location$LAT_WGS84[id_loc]
    report$Network[ith_SUID]   <- tbl_location$Network[id_loc]
  }else{
    report$Y_LV03[ith_SUID]    <- NA
    report$X_LV03[ith_SUID]    <- NA
    report$h[ith_SUID]         <- NA
    report$LON_WGS84[ith_SUID] <- NA
    report$LAT_WGS84[ith_SUID] <- NA
    report$Network[ith_SUID]   <- "None"
  }
  
  
  # Last calibration
  
  id_SU_cal <- which(tbl_calibration$SensorUnit_ID   == SensorUnit_IDs[ith_SUID])
  
  if(length(id_SU_cal)>=1){
    id_SU_cal <- id_SU_cal[which(tbl_calibration$Date_UTC_to[id_SU_cal]==max(tbl_calibration$Date_UTC_to[id_SU_cal]))]
  }
  
  if(length(id_SU_depl)==0){
    report$DaysSinceLastCalibration[ith_SUID] <- -999
  }
  
  if(length(id_SU_depl)==1){
    report$DaysSinceLastCalibration[ith_SUID] <- sprintf("%.1f",as.numeric(difftime(time1=current_date,time2=tbl_calibration$Date_UTC_to[id_SU_cal],units="days",tz="UTC")))
  }
  
  
  # SensorUnit alive
  
  
  timeFilter <- paste("time >= ",current_timestamp-86400,"s AND time < ",current_timestamp,"s",sep="")
  device     <- paste("/",SensorUnit_IDs[ith_SUID],"/",sep="")
  
  if(SensorUnit_IDs[ith_SUID]%in%c(1007,1008)){
    
    tmp0 <- tryCatch({
      query(domain=DL_DB_domain_EMPA,
            apiKey=DL_DB_apiKey_EMPA,
            timeFilter=timeFilter,
            device = device,
            location = "//",
            sensor = "/^(senseair-lp8-co2|senseair-lp8-status|sensirion-sht21-humidity|sensirion-sht21-temperature|battery)$/",
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
            sensor = "/^(senseair-lp8-co2|senseair-lp8-status|sensirion-sht21-humidity|sensirion-sht21-temperature|battery)$/",
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
  
  if(!is.null(tmp0)){
    if(dim(tmp0)[1]==0){
      tmp0 <- NULL
    }else{
      # require(reshape)
      # tmp0 <- data.frame(reshape::cast(tmp0, time ~ series, fun.aggregate = mean), check.names = FALSE)
      
      # require(reshape)
      # tmp0$time <- round(as.numeric(difftime(time1=tmp0$time,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))*1e6)
      # tmp0      <- data.frame(reshape::cast(tmp0, time ~ series, fun.aggregate = mean), check.names = FALSE)
      # tmp0$time <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp0$time/1e6
      
      tmp0      <- as.data.table(tmp0)
      tmp0$time <- round(as.numeric(difftime(time1=tmp0$time,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))*1e3)
      tmp0      <- dcast.data.table(tmp0, time ~ series, fun.aggregate = mean,value.var = "value")
      tmp0$time <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp0$time/1e3
      tmp0      <- as.data.frame(tmp0)
    }
  }
  
  if(!is.null(tmp0)){
    
    cn             <- colnames(tmp0)
    cn             <- gsub(pattern = "-",  replacement = "_", x = cn)
    cn             <- gsub(pattern = "\\.",replacement = "_", x = cn)
    cn             <- gsub(pattern = paste(SensorUnit_IDs[ith_SUID],"_",sep=""), replacement = "", x = cn)
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
    cn             <- gsub(pattern = "battery",                         replacement = "battery",    x=cn)
    cn             <- gsub(pattern = "time",                            replacement = "date",       x=cn)
    colnames(tmp0) <- cn
    
    # Force time to full preceding minute
    
    tmp0$date      <- as.POSIXct(tmp0$date)
    tmp0$date      <- strptime(strftime(tmp0$date,"%Y%m%d%H%M00",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
    tmp0$secs      <- as.numeric(difftime(time1=tmp0$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
    
    # Check data imported from Influx-DB (no duplicates, delta timestamp>300s)
    
    tmp0           <- tmp0[!duplicated(tmp0$secs),]
    tmp0           <- tmp0[order(tmp0$secs),]
    tmp0           <- tmp0[c(T,abs(diff(tmp0$secs))>300),]
    
    #
    
    if(is.null(tmp0$lp8_status)){
      report$MAX_Status_LP8[ith_SUID]  <- -999
    }else{
      report$MIN_Status_LP8[ith_SUID]  <- min(tmp0$lp8_status,na.rm=T)
    }
    
    if(is.null(tmp0$lp8_status)){
      report$MIN_Status_LP8[ith_SUID]  <- -999
    }else{
      report$MAX_Status_LP8[ith_SUID]  <- max(tmp0$lp8_status,na.rm=T)
    }
    
    if(is.null(tmp0$lp8_status)){
      report$LAST_Status_LP8[ith_SUID]  <- -999
    }else{
      report$LAST_Status_LP8[ith_SUID]  <- tmp0$lp8_status[which(!is.na(tmp0$lp8_status))[sum(!is.na(tmp0$lp8_status))]]
    }
    
    #
    
    if(is.null(tmp0$lp8_CO2)){
      report$nCO2ValuesLast24h[ith_SUID] <- -999
    }else{
      report$nCO2ValuesLast24h[ith_SUID] <- sum(!is.na(tmp0$lp8_CO2))
    }
    
    #
    
    if(is.null(tmp0$sht21_RH)){
      report$nSHTValuesLast24h[ith_SUID] <- -999
    }else{
      report$nSHTValuesLast24h[ith_SUID] <- sum(!is.na(tmp0$sht21_RH))
    }
    
    #
    
    if(is.null(tmp0$battery)){
      report$battery[ith_SUID] <- -999
    }else{
      report$battery[ith_SUID] <- round(min(tmp0$battery,na.rm=T),2)
    }
    
    #
    
    if(is.null(tmp0$sht21_T_m)){
      report$LAST_SHT21_T[ith_SUID] <- -999
    }else{
      report$LAST_SHT21_T[ith_SUID] <- round(tmp0$sht21_T_m[which(!is.na(tmp0$sht21_T_m))[sum(!is.na(tmp0$sht21_T_m))]],1)
    }
    
    if(is.null(tmp0$sht21_T_m)){
      report$MIN_SHT21_T[ith_SUID] <- -999
    }else{
      report$MIN_SHT21_T[ith_SUID] <- round(min(tmp0$sht21_T_m,na.rm=T),1)
    }
    
    if(is.null(tmp0$sht21_T_m)){
      report$MAX_SHT21_T[ith_SUID] <- -999
    }else{
      report$MAX_SHT21_T[ith_SUID] <- round(max(tmp0$sht21_T_m,na.rm=T),1)
    }
    
    #
    
    if(is.null(tmp0$sht21_RH)){
      report$LAST_SHT21_RH[ith_SUID] <- -999
    }else{
      report$LAST_SHT21_RH[ith_SUID] <- round(tmp0$sht21_RH[which(!is.na(tmp0$sht21_RH))[sum(!is.na(tmp0$sht21_RH))]],1)
    }
    
    if(is.null(tmp0$sht21_RH)){
      report$MIN_SHT21_RH[ith_SUID] <- -999
    }else{
      report$MIN_SHT21_RH[ith_SUID] <- round(min(tmp0$sht21_RH,na.rm=T),1)
    }
    
    if(is.null(tmp0$sht21_RH)){
      report$MAX_SHT21_RH[ith_SUID] <- -999
    }else{
      report$MAX_SHT21_RH[ith_SUID] <- round(max(tmp0$sht21_RH,na.rm=T),1)
    }
    
    #
    
  }else{
    
    report$MIN_Status_LP8[ith_SUID]    <- -999
    report$MAX_Status_LP8[ith_SUID]    <- -999
    report$LAST_Status_LP8[ith_SUID]   <- -999
    
    report$MIN_SHT21_RH[ith_SUID]      <- -999
    report$MAX_SHT21_RH[ith_SUID]      <- -999
    report$LAST_SHT21_RH[ith_SUID]     <- -999
    
    report$MIN_SHT21_T[ith_SUID]       <- -999
    report$MAX_SHT21_T[ith_SUID]       <- -999
    report$LAST_SHT21_T[ith_SUID]      <- -999
    
    report$nCO2ValuesLast24h[ith_SUID] <- -999
    report$nSHTValuesLast24h[ith_SUID] <- -999
    report$battery[ith_SUID]           <- -999
  }
}

#

write.table(report,report_fn,sep=";",col.names=T,row.names=F,quote=F)

