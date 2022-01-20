# Carbosense_data_release_2019-10.r
# ---------------------------------
#
# Author: Michael Mueller
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

## libraries
library(DBI)
require(RMySQL)
require(chron)

#

source("/project/muem/CarboSense/Software/CoordinateTransformations/WGS84_CH1903.R")

### ----------------------------------------------------------------------------------------------------------------------------

## Directories and file names

resultdir      <- "/project/muem/CarboSense/Carbosense_data_release/"

fn_Locations   <- paste(resultdir,"Location.csv",sep="")

fn_Deployments <- paste(resultdir,"Deployment.csv",sep="")

### ----------------------------------------------------------------------------------------------------------------------------

## Release period

Release_Date_UTC_from  <- strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC")
Release_Date_UTC_to    <- strptime("20191001000000","%Y%m%d%H%M%S",tz="UTC")

Release_timestamp_from <- as.numeric(difftime(time1=Release_Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
Release_timestamp_to   <- as.numeric(difftime(time1=Release_Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

### ----------------------------------------------------------------------------------------------------------------------------

## Read tables

## Deployment

query_str       <- paste("SELECT * FROM Deployment;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_deployment  <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_deployment$Date_UTC_from <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to   <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

# Filter: LocationName, SensorUnit_ID (LP8s), date

id_depl_keep <- which(!tbl_deployment$LocationName%in%c("DUE1","DUE2","DUE3","DUE4","DUE5","MET1")
                      & tbl_deployment$SensorUnit_ID >= 1010
                      & tbl_deployment$SensorUnit_ID <= 1334
                      & tbl_deployment$Date_UTC_to   > Release_Date_UTC_from
                      & tbl_deployment$Date_UTC_from < Release_Date_UTC_to)

tbl_deployment <- tbl_deployment[id_depl_keep,]


# Adjust deployment periods according to release period

id_adjust <- which(tbl_deployment$Date_UTC_from < Release_Date_UTC_from)
if(length(id_adjust)>0){
  tbl_deployment$Date_UTC_from[id_adjust] <- Release_Date_UTC_from
}

id_adjust <- which(tbl_deployment$Date_UTC_to > Release_Date_UTC_to)
if(length(id_adjust)>0){
  tbl_deployment$Date_UTC_to[id_adjust] <- Release_Date_UTC_to
}

tbl_deployment$timestamp_from <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
tbl_deployment$timestamp_to   <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))


# Omit some deployments due to known issues

# ORDS, SU 1313, 2017-07-17 -- 2018-02-02 : affected by station ventilation

id_rm <- which(tbl_deployment$SensorUnit_ID   == 1313 
               & tbl_deployment$LocationName  == 'ORDS' 
               & tbl_deployment$Date_UTC_from == strptime("20170717230000","%Y%m%d%H%M%S",tz="UTC")
               & tbl_deployment$Date_UTC_to   == strptime("20180202000000","%Y%m%d%H%M%S",tz="UTC"))

tbl_deployment <- tbl_deployment[!(1:dim(tbl_deployment)[1])%in%id_rm,]


rm(id_depl_keep,id_adjust,id_rm)
gc()


## Location

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_location    <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)


tbl_location$LAT_WGS84_TRSF <- CH.to.WGS.lat(y = tbl_location$Y_LV03,x = tbl_location$X_LV03)
tbl_location$LON_WGS84_TRSF <- CH.to.WGS.lng(y = tbl_location$Y_LV03,x = tbl_location$X_LV03)
tbl_location$H_WGS84_TRSF   <- tbl_location$h + 49.55 - 12.60 * (tbl_location$Y_LV03 - 600000)/1e6 - 22.64 * (tbl_location$X_LV03 - 200000)/1e6 ## Formula by Swisstopo


## SensorExclusionPeriods

query_str       <- paste("SELECT * FROM SensorExclusionPeriods;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_SEP         <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_SEP$Date_UTC_from  <- strptime(tbl_SEP$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_SEP$Date_UTC_to    <- strptime(tbl_SEP$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_SEP$timestamp_from <- as.numeric(difftime(time1=tbl_SEP$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
tbl_SEP$timestamp_to   <- as.numeric(difftime(time1=tbl_SEP$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

### ----------------------------------------------------------------------------------------------------------------------------

## Loop over all LP8 deployments

n_depl <- dim(tbl_deployment)[1]

tbl_deployment_data_exported <- rep(F,n_depl) # only retain in table "deployment" if corresponding data is exported

for(ith_depl in 1:n_depl){
  
  # Import LP8 data
  
  query_str       <- paste("SELECT timestamp, SHT21_T, SHT21_RH FROM CarboSense_T_RH ",sep="")
  query_str       <- paste(query_str, "WHERE timestamp >= ",tbl_deployment$timestamp_from[ith_depl]," AND timestamp <= ",tbl_deployment$timestamp_to[ith_depl]," ",sep="")
  query_str       <- paste(query_str, "AND LocationName = '",tbl_deployment$LocationName[ith_depl],"' AND SensorUnit_ID = ",tbl_deployment$SensorUnit_ID[ith_depl],";",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tbl_LP8_data    <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  if(dim(tbl_LP8_data)[1]==0){
    next
  }
  
  tbl_LP8_data <- tbl_LP8_data[order(tbl_LP8_data$timestamp),]
  
  
  # Check for sensor exclusion periods
  
  id_SU_SEP   <- which(tbl_SEP$SensorUnit_ID==tbl_deployment$SensorUnit_ID[ith_depl])
  n_id_SU_SEP <- length(id_SU_SEP)
  
  keep        <- rep(T,dim(tbl_LP8_data)[1])
  
  if(n_id_SU_SEP>0){
    for(ith_SU_SEP in 1:n_id_SU_SEP){
      # In script Compute_CarboSense_T_RH_values.r timestamp is shifted by 600s
      id   <- which(tbl_LP8_data$timestamp+600 >= tbl_SEP$timestamp_from[id_SU_SEP[ith_SU_SEP]]
                    & tbl_LP8_data$timestamp+600 <= tbl_SEP$timestamp_to[id_SU_SEP[ith_SU_SEP]])
      n_id <- length(id)
      
      if(n_id>0){
        keep[id] <- rep(F,n_id)
        print(tbl_deployment[ith_depl,])
      }
    }
  }
  
  tbl_LP8_data <- tbl_LP8_data[keep,]
  
  rm(keep)
  gc()
  
  
  # Check for "-999" values
  
  id_keep   <- which(tbl_LP8_data$SHT21_T!=-999 & tbl_LP8_data$SHT21_RH!=-999)
  n_id_keep <- length(id_keep)
  
  if(length(id_keep) == 0){
    next
  }
  
  if(length(id_keep)>0){
    tbl_LP8_data <- tbl_LP8_data[id_keep,]
  }
  
  rm(id_keep)
  gc()
  
  
  # Remove "duplicated" measurements and "alien" measurements
  
  delta_timestamp    <- diff(tbl_LP8_data$timestamp)
  
  delta_SHT21_T_1    <- c(0,diff(tbl_LP8_data$SHT21_T))
  delta_SHT21_T_2    <- c(0,0,diff(tbl_LP8_data$SHT21_T)[1:(length(diff(tbl_LP8_data$SHT21_T))-1)])
  
  delta_SHT21_T_1_ok <- c(0,delta_timestamp) < 700
  delta_SHT21_T_2_ok <- c(0,0,delta_timestamp[1:(length(delta_timestamp)-1)]) < 700
  
  alienMeas          <- ((delta_SHT21_T_1 > 3 & delta_SHT21_T_2 < -3) | (delta_SHT21_T_1 < -3 & delta_SHT21_T_2 > 3)) & delta_SHT21_T_1_ok & delta_SHT21_T_2_ok
  alienMeas          <- c(F,alienMeas[3:length(alienMeas)],F)
  id_alienMeas       <- which(alienMeas==T)
  
  id_keep            <- which(c(T,delta_timestamp>=540) & !alienMeas)
  
  if(T){

    id_A <- which(c(T,delta_timestamp%in%c(540,600,660)))
    
    tbl_LP8_data$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_LP8_data$timestamp
    
    print(paste(tbl_deployment$SensorUnit_ID[ith_depl],
                sprintf("%5.1f",sum(delta_timestamp <540)/dim(tbl_LP8_data)[1]*100),
                sprintf("%5.1f",sum(delta_timestamp==540)/dim(tbl_LP8_data)[1]*100),
                sprintf("%5.1f",sum(delta_timestamp==600)/dim(tbl_LP8_data)[1]*100),
                sprintf("%5.1f",sum(delta_timestamp==660)/dim(tbl_LP8_data)[1]*100),
                sprintf("%5.1f",sum(delta_timestamp >660)/dim(tbl_LP8_data)[1]*100),
                sprintf("%5.1f",max(abs(c(0,diff(tbl_LP8_data$SHT21_T))[id_A]))),
                tbl_LP8_data$date[id_A[which(max(abs(c(0,diff(tbl_LP8_data$SHT21_T))[id_A]))==abs(c(0,diff(tbl_LP8_data$SHT21_T))[id_A]))]],
                sprintf("%3.0f",(dim(tbl_LP8_data)[1] - length(id_keep))),
                sprintf("%6.0f",dim(tbl_LP8_data)[1]),
                sprintf("%3.0f",length(id_alienMeas))))
    
    if(length(id_alienMeas)>0){
      print(tbl_LP8_data$date[id_alienMeas])
      print(tbl_LP8_data$SHT21_T[id_alienMeas])
    }
    
    rm(id_A)
    gc()
  }
  
  
  if(length(id_keep) == 0){
    next
  }
  
  if(length(id_keep)>0){
    tbl_LP8_data <- tbl_LP8_data[id_keep,]
  }
  
  rm(id_keep,alienMeas,id_alienMeas,delta_timestamp,delta_SHT21_T_1,delta_SHT21_T_2,delta_SHT21_T_1_ok,delta_SHT21_T_2_ok)
  gc()
  
  
  # Date
  
  tbl_LP8_data$date             <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_LP8_data$timestamp
  tbl_LP8_data_date_from_str    <- strftime(min(tbl_LP8_data$date),"%Y%m%dT%H%M%SZ",tz="UTC")
  tbl_LP8_data_date_to_str      <- strftime(max(tbl_LP8_data$date),"%Y%m%dT%H%M%SZ",tz="UTC")
  tbl_LP8_data$date_str_ISO8601 <- strftime(tbl_LP8_data$date,"%Y-%m-%dT%H:%M:%SZ",tz="UTC")
  
  # Export data into file
  
  fn_LP8_data <- paste(resultdir,"LP8_",tbl_deployment$LocationName[ith_depl],"_",tbl_deployment$SensorUnit_ID[ith_depl],"_",tbl_LP8_data_date_from_str,"_",tbl_LP8_data_date_to_str,".csv",sep="")
  
  if(dim(tbl_LP8_data)[1]>0){
    
    tbl_LP8_data <- data.frame(Date      = tbl_LP8_data$date_str_ISO8601,
                               SHT21_T   = round(tbl_LP8_data$SHT21_T, 1),
                               SHT21_RH  = round(tbl_LP8_data$SHT21_RH,1),
                               stringsAsFactors = F)
    
    write.table(x = tbl_LP8_data,file = fn_LP8_data,append = F,row.names = F,col.names = T,sep=";",quote = F)
    
    tbl_deployment_data_exported[ith_depl] <- T
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

# Export table "Location"

tbl_location <- tbl_location[which(tbl_location$LocationName%in%unique(tbl_deployment$LocationName[tbl_deployment_data_exported])),]

tbl_location_export <- data.frame(LocationName = tbl_location$LocationName,
                                  Y_LV03       = round(tbl_location$Y_LV03,0),
                                  X_LV03       = round(tbl_location$X_LV03,0),
                                  h            = round(tbl_location$h,1),
                                  LON_WGS84    = round(tbl_location$LON_WGS84,5),
                                  LAT_WGS84    = round(tbl_location$LAT_WGS84,5),
                                  H_WGS84      = round(tbl_location$H_WGS84_TRSF,2),
                                  Canton       = tbl_location$Canton,
                                  SiteType     = tbl_location$SiteType,
                                  stringsAsFactors = F)

if(  any(tbl_location$LON_WGS84 == -999) | any(abs(tbl_location$LON_WGS84_TRSF-tbl_location$LON_WGS84)>3e-5)
     | any(tbl_location$LAT_WGS84 == -999) | any(abs(tbl_location$LAT_WGS84_TRSF-tbl_location$LAT_WGS84)>3e-5)
     | any(tbl_location$Y_LV03 == -999) | any(tbl_location$Y_LV03 < 477500) | any(tbl_location$Y_LV03 > 838000)
     | any(tbl_location$X_LV03 == -999) | any(tbl_location$X_LV03 <  71250) | any(tbl_location$X_LV03 > 302000)){
  
  stop("ERROR: EXPORT TABLE LOCATION")
}

tbl_location_export <- tbl_location_export[order(tbl_location_export$LocationName),]

write.table(x = tbl_location_export,file = fn_Locations,append = F,row.names = F,col.names = T,sep=";",quote = F)

### ----------------------------------------------------------------------------------------------------------------------------

# Export table "Deployment"

tbl_deployment <- tbl_deployment[tbl_deployment_data_exported,]

tbl_deployment_export <- data.frame(SensorUnit_ID           = tbl_deployment$SensorUnit_ID,
                                    LocationName            = tbl_deployment$LocationName,
                                    Date_UTC_from           = strftime(tbl_deployment$Date_UTC_from,"%Y-%m-%dT%H:%M:%SZ",tz="UTC"),
                                    Date_UTC_to             = strftime(tbl_deployment$Date_UTC_to,  "%Y-%m-%dT%H:%M:%SZ",tz="UTC"),
                                    HeightAboveGround       = tbl_deployment$HeightAboveGround,
                                    stringsAsFactors = F)

if(any(tbl_deployment$HeightAboveGround < 0) | any(tbl_deployment$HeightAboveGround > 200)){
  stop("ERROR: EXPORT TABLE DEPLOYMENT --> HeightAboveGround")
}
if(length(which(!tbl_deployment$LocationName%in%tbl_location_export$LocationName))>0){
  stop("ERROR: EXPORT TABLE DEPLOYMENT --> LocationName")
}

tbl_deployment_export <- tbl_deployment_export[order(tbl_deployment_export$SensorUnit_ID),]

write.table(x = tbl_deployment_export,file = fn_Deployments,append = F,row.names = F,col.names = T,sep=";",quote = F)

### ----------------------------------------------------------------------------------------------------------------------------

