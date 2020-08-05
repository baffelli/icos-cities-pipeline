# LP8_DriftCorrection.r
# -----------------------------------------------------
#
# Author: Michael Mueller
#
# -----------------------------------------------------
#
# Remarks:
# - Computations refer to UTC.
#
# -----------------------------------------------------
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
library(rpart)


## source

source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

### ----------------------------------------------------------------------------------------------------------------------------

## PARAMETERS

apply_SU_events <- T

### ----------------------------------------------------------------------------------------------------------------------------

## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

# 

if(!length(args)%in%c(2,3)){
  stop("ARGS: MODEL(1/2) PARTIAL_COMPUTING(T/F) [DUE]")
}

if(!as.integer(args[1])%in%c(1,2,3,20,30)){
  stop("MODEL: 1, 2, 3, 20, 30!")
}else{
  
  # Standard 
  if(as.integer(args[1])==1){
    # Directories
    resultdir           <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis"
    anchor_events_fn    <- paste(resultdir,"/anchor_events.csv",sep="") 
    SU_events_fn        <- paste(resultdir,"/SU_events.csv",sep="")
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2"
  }
  
  # Test 00 
  if(as.integer(args[1])==2){
    # Directories
    resultdir           <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00"
    anchor_events_fn    <- paste(resultdir,"/anchor_events.csv",sep="") 
    SU_events_fn        <- paste(resultdir,"/SU_events.csv",sep="")
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST00"
  }
  
  # Test 01 
  if(as.integer(args[1])==3){
    # Directories
    resultdir           <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01"
    anchor_events_fn    <- paste(resultdir,"/anchor_events.csv",sep="") 
    SU_events_fn        <- paste(resultdir,"/SU_events.csv",sep="")
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST01"
  }
  
  # Test 00 AMT
  if(as.integer(args[1])==20){
    # Directories
    resultdir           <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT"
    anchor_events_fn    <- paste(resultdir,"/anchor_events.csv",sep="") 
    SU_events_fn        <- paste(resultdir,"/SU_events.csv",sep="")
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST00_AMT"
  }
  
  # Test 01 AMT
  if(as.integer(args[1])==30){
    # Directories
    resultdir           <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT"
    anchor_events_fn    <- paste(resultdir,"/anchor_events.csv",sep="") 
    SU_events_fn        <- paste(resultdir,"/SU_events.csv",sep="")
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST01_AMT"
  }
}

if(!as.logical(args[2])%in%c(T,F)){
  stop("PARTIAL_COMPUTING: T or F!")
}else{
  
  if(as.logical(args[2])==T){
    PARTIAL_COMPUTING <- T
    DATE_NOW                            <- strptime(strftime(Sys.time(),"%Y%m%d000000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC") + 86400
    REF_data_timestamp_from             <- as.numeric(difftime(time1=DATE_NOW-300*86400,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    REF_data_timestamp_to               <- as.numeric(difftime(time1=DATE_NOW,          time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    SENSOR_data_ANALYSE_timestamp_from  <- as.numeric(difftime(time1=DATE_NOW-300*86400,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    SENSOR_data_ANALYSE_timestamp_to    <- as.numeric(difftime(time1=DATE_NOW,          time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    SENSOR_data_DBINSERT_timestamp_from <- as.numeric(difftime(time1=DATE_NOW-150*86400,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    SENSOR_data_DBINSERT_timestamp_to   <- as.numeric(difftime(time1=DATE_NOW,          time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
  }else{
    PARTIAL_COMPUTING <- F
    REF_data_timestamp_from             <- as.numeric(difftime(time1=strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC"),time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    REF_data_timestamp_to               <- as.numeric(difftime(time1=strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    SENSOR_data_ANALYSE_timestamp_from  <- as.numeric(difftime(time1=strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC"),time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    SENSOR_data_ANALYSE_timestamp_to    <- as.numeric(difftime(time1=strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    SENSOR_data_DBINSERT_timestamp_from <- as.numeric(difftime(time1=strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC"),time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    SENSOR_data_DBINSERT_timestamp_to   <- as.numeric(difftime(time1=strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
  }
}

# AMT
if(as.integer(args[1])%in%c(20,30)){
  PARTIAL_COMPUTING <- F
  REF_data_timestamp_from             <- as.numeric(difftime(time1=strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC"),time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
  REF_data_timestamp_to               <- as.numeric(difftime(time1=strptime("20190901000000","%Y%m%d%H%M%S",tz="UTC"),time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
  SENSOR_data_ANALYSE_timestamp_from  <- as.numeric(difftime(time1=strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC"),time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
  SENSOR_data_ANALYSE_timestamp_to    <- as.numeric(difftime(time1=strptime("20190901000000","%Y%m%d%H%M%S",tz="UTC"),time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
  SENSOR_data_DBINSERT_timestamp_from <- as.numeric(difftime(time1=strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC"),time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
  SENSOR_data_DBINSERT_timestamp_to   <- as.numeric(difftime(time1=strptime("20190901000000","%Y%m%d%H%M%S",tz="UTC"),time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
}

COMP_DUE <- F

if(length(args)==3){
  if(args[3]=="DUE"){
    COMP_DUE      <- T
    SU_events_fn  <- paste(resultdir,"/SU_events_DUE1.csv",sep="")
  }else{
    stop("ARGS[3] must be DUE or must be not specified!")
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

# Coordinates of Picarro and HPP sites
#
# status: use [0] / do not use [1] data from this station for the correction of measurements from LP8 sensors 
# Date_UTC_from, Date_UTC_to: Operation period for station 
#


REF_SITES <- NULL

REF_SITES <- rbind(REF_SITES,data.frame(name="DUEBSL",x=250902,y=688680,h=432, canton="ZH", status=1,Date_UTC_from = strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))

REF_SITES <- rbind(REF_SITES,data.frame(name="DUE",   x=250902,y=688680,h=432, canton="ZH", status=1,Date_UTC_from = strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="PAY",   x=184776,y=562285,h=488, canton="VD", status=1,Date_UTC_from = strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))

REF_SITES <- rbind(REF_SITES,data.frame(name="RIG",   x=213437,y=677834,h=1030, canton="SZ", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="BRM",   x=226777,y=655840,h=797,  canton="LU", status=1,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="LAEG",  x=259462,y=672251,h=855,  canton="ZH", status=1,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="HAE",   x=240180,y=628874,h=430,  canton="SO", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="GIMM",  x=211397,y=585510,h=478,  canton="BE", status=1,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))

REF_SITES <- rbind(REF_SITES,data.frame(name="MAGN",  x=113199,y=715497,h=203, canton="TI", status=1,Date_UTC_from = strptime("20180816160000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="ZUE",   x=247989,y=682448,h=409, canton="ZH", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="ZHBR",  x=248471,y=685132,h=626, canton="ZH", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="ZSCH",  x=247242,y=681942,h=413, canton="ZH", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="ALBS",  x=240700,y=680699,h=810, canton="ZH", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="BNTG",  x=202974,y=606849,h=938, canton="BE", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="FROB",  x=248105,y=634732,h=867, canton="SO", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="CHRI",  x=269035,y=618695,h=493, canton="BS", status=1,Date_UTC_from = strptime("20181120000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="TAEN",  x=259812,y=710499,h=538, canton="TG", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="SEMP",  x=218512,y=658235,h=580, canton="LU", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="ESMO",  x=263906,y=685350,h=556, canton="ZH", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="SAVE",  x=121297,y=593583,h=767, canton="VS", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="RECK",  x=253563,y=681394,h=443, canton="ZH", status=0,Date_UTC_from = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="SSAL",  x= 92824,y=716877,h=902, canton="TI", status=1,Date_UTC_from = strptime("20190405170000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))
REF_SITES <- rbind(REF_SITES,data.frame(name="SOTT",  x=167442,y=546245,h=775, canton="VD", status=0,Date_UTC_from = strptime("20190719140000","%Y%m%d%H%M%S",tz="UTC"), Date_UTC_to = strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"),stringsAsFactors = F))


REF_SITES$timestamp_from <- as.numeric(difftime(time1=REF_SITES$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
REF_SITES$timestamp_to   <- as.numeric(difftime(time1=REF_SITES$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

### ----------------------------------------------------------------------------------------------------------------------------

## Database queries : metadata

# Table "Deployment"

if(!COMP_DUE){
  query_str       <- paste("SELECT * FROM Deployment WHERE SensorUnit_ID BETWEEN 1010 and 1334 and LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1') and Date_UTC_from >= '2017-07-01 00:00:00';",sep="")
}
if(COMP_DUE){
  query_str       <- paste("SELECT * FROM Deployment WHERE SensorUnit_ID BETWEEN 1010 and 1334 and LocationName = 'DUE1' and Date_UTC_to >= '2017-12-01 00:00:00';",sep="")
}
drv             <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_deployment  <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_deployment$Date_UTC_from <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to   <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

id <- which(tbl_deployment$Date_UTC_to==strptime("2100-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"))

if(length(id)>0){
  date_now <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
  for(ii in 1:length(id)){
    tbl_deployment$Date_UTC_to[id[ii]] <- date_now
  }
}

if(COMP_DUE){
  id <- which(tbl_deployment$Date_UTC_from<strptime("2017-12-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"))
  if(length(id)>0){
    for(ii in 1:length(id)){
      tbl_deployment$Date_UTC_from[id[ii]] <- strptime("2017-12-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC")
    }
  }
}

tbl_deployment$timestamp_from <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_from, time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
tbl_deployment$timestamp_to   <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_to,   time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))


# Table "Location"

tmp              <- paste("'",gsub(pattern = ",",replacement = "','",x = paste(sort(unique(tbl_deployment$LocationName)),collapse=",")),"'",sep="")
query_str        <- paste("SELECT * FROM Location WHERE LocationName IN (",tmp,");",sep="")
drv              <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res              <- dbSendQuery(con, query_str)
tbl_location     <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

#

query_str        <- paste("SELECT * FROM Location WHERE Network = 'METEOSWISS';",sep="")
drv              <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res              <- dbSendQuery(con, query_str)
tbl_location_MCH <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

# 

tbl_location_LP8 <- tbl_location[which(tbl_location$LocationName%in%tbl_deployment$LocationName[which(tbl_deployment$SensorUnit_ID%in%c(1010:1334))]),]


### ----------------------------------------------------------------------------------------------------------------------------

## Import of MCH measurements
#
#  (start of automatic data export from MCH: 2018-04-15 (timestamp: 1523750400))

query_str <- paste("SELECT DISTINCT LocationName FROM METEOSWISS_Measurements WHERE timestamp > 1523750400 and Windspeed != -999;",sep="")
drv       <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res       <- dbSendQuery(con, query_str)
MCH_SITES <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

MCH_SITES <- as.vector(MCH_SITES[,1])

MCH_WIND  <- NULL

for(MCH_SITE in MCH_SITES){
  
  query_str <- paste("SELECT timestamp,Winddirection,Windspeed,Sunshine FROM METEOSWISS_Measurements WHERE timestamp >= ",REF_data_timestamp_from," and timestamp < ",REF_data_timestamp_to," and LocationName = '",MCH_SITE,"';",sep="")
  drv       <- dbDriver("MySQL")
  con<-carboutil::get_conn(group="CarboSense_MySQL")
  res       <- dbSendQuery(con, query_str)
  tmp       <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  if(dim(tmp)[1]==0){
    next
  }
  
  tmp <- tmp[order(tmp$timestamp),]
  
  for(ith_col in c(2,3,4)){
    id_setToNA    <- which(tmp[,ith_col]==-999)
    if(length(id_setToNA)>0){
      tmp[id_setToNA,ith_col] <- NA
    }
  }
  
  colnames(tmp) <- c("timestamp",paste(toupper(MCH_SITE),"_WDIR",sep=""),paste(toupper(MCH_SITE),"_WSPEED",sep=""),paste(toupper(MCH_SITE),"_SUNSHINE",sep=""))
  
  if(is.null(MCH_WIND)){
    MCH_WIND <- tmp
  }else{
    MCH_WIND <- merge(MCH_WIND,tmp,by="timestamp",all=T)
  }
}

MCH_WIND$date    <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + MCH_WIND$timestamp

tbl_location_MCH <- tbl_location_MCH[which(tbl_location_MCH$LocationName%in%MCH_SITES),]

rm(tmp)
gc()


# Plot winds

if(F){
  MCH_SITES_TMP <- c("ABO","AEG","ALP","ARH","BAS","BER","BEZ","BHF","BIE","BIZ","BOL","BRL","BRZ","BUS","CDF","CDM","CGI","CHB","CHD","CHM","CHZ","COY","CRM","DEM","DIT","DUB","EBK","EGO","EIN","ELM","EMM","ENG","FAH","FLU","FRE","FRU","GES","GIH","GLA","GRA","GUB","GUT","GVE","HAI","HLL","HOE","INT","KLO","KOP","LAC","LAE","LAG","LCK","LEI","LUZ","MAH","MAS","MER","MOA","MOE","MSK","MUB","MUR","NAP","NEE","NEU","OBR","OED","OPF","ORO","PAA","PAY","PLF","PSI","PUY","QUI","RAG","REH","RUE","SAG","SCM","SHA","SMA","SMA","SPF","STG","STK","TAE","THU","UEB","VEV","VIT","WAE","WYN","ZWK")
  
  id_COL_WSPEED <- which(colnames(MCH_WIND)%in%paste(toupper(MCH_SITES_TMP),"_WSPEED",sep=""))
  yyy           <- as.matrix(MCH_WIND[,id_COL_WSPEED])
  yyy_FLAG      <- matrix(0,ncol=dim(yyy)[2],nrow=dim(yyy)[1])
  yyy_FLAG[which(!is.na(yyy))] <- 1
  
  xlabString <- "Date" 
  ylabString <- expression(paste("Wind speed [m/s]"))
  figname    <- paste(resultdir,"/MCH_WINDS_NORTHALPS.pdf",sep="")
  legend_str <- gsub(pattern = "_WSPEED",replacement = "",x=colnames(MCH_WIND)[id_COL_WSPEED])
  plot_ts_NETWORK(figname,MCH_WIND$date,yyy,yyy_FLAG,"week",NULL,c(0,20),xlabString,ylabString,legend_str,15*60)
  
  #
  
  MCH_SITES_TMP <- c("MAG","LUG","COM","CEV","OTL","PIO","SBO")
  
  id_COL_WSPEED <- which(colnames(MCH_WIND)%in%paste(toupper(MCH_SITES_TMP),"_WSPEED",sep=""))
  yyy           <- as.matrix(MCH_WIND[,id_COL_WSPEED])
  yyy_FLAG      <- matrix(0,ncol=dim(yyy)[2],nrow=dim(yyy)[1])
  yyy_FLAG[which(!is.na(yyy))] <- 1
  
  xlabString <- "Date" 
  ylabString <- expression(paste("Wind speed [m/s]"))
  figname    <- paste(resultdir,"/MCH_WINDS_SOUTHALPS.pdf",sep="")
  legend_str <- gsub(pattern = "_WSPEED",replacement = "",x=colnames(MCH_WIND)[id_COL_WSPEED])
  plot_ts_NETWORK(figname,MCH_WIND$date,yyy,yyy_FLAG,"week",NULL,c(0,20),xlabString,ylabString,legend_str,15*60)
}

# Plot Sunshine

if(F){
  MCH_SITES_TMP <- c("ABO","AEG","ALP","ARH","BAS","BER","BEZ","BHF","BIE","BIZ","BOL","BRL","BRZ","BUS","CDF","CDM","CGI","CHB","CHD","CHM","CHZ","COY","CRM","DEM","DIT","DUB","EBK","EGO","EIN","ELM","EMM","ENG","FAH","FLU","FRE","FRU","GES","GIH","GLA","GRA","GUB","GUT","GVE","HAI","HLL","HOE","INT","KLO","KOP","LAC","LAE","LAG","LCK","LEI","LUZ","MAH","MAS","MER","MOA","MOE","MSK","MUB","MUR","NAP","NEE","NEU","OBR","OED","OPF","ORO","PAA","PAY","PLF","PSI","PUY","QUI","RAG","REH","RUE","SAG","SCM","SHA","SMA","SMA","SPF","STG","STK","TAE","THU","UEB","VEV","VIT","WAE","WYN","ZWK")
  
  id_COL_WSPEED <- which(colnames(MCH_WIND)%in%paste(toupper(MCH_SITES_TMP),"_SUNSHINE",sep=""))
  yyy           <- as.matrix(MCH_WIND[,id_COL_WSPEED])
  yyy_FLAG      <- matrix(0,ncol=dim(yyy)[2],nrow=dim(yyy)[1])
  yyy_FLAG[which(!is.na(yyy))] <- 1
  
  xlabString <- "Date" 
  ylabString <- expression(paste("Sunshine [min]"))
  figname    <- paste(resultdir,"/MCH_SUNSHINE_NORTHALPS.pdf",sep="")
  legend_str <- gsub(pattern = "_WSPEED",replacement = "",x=colnames(MCH_WIND)[id_COL_WSPEED])
  plot_ts_NETWORK(figname,MCH_WIND$date,yyy,yyy_FLAG,"week",NULL,c(0,11),xlabString,ylabString,legend_str,15*60)
  
  #
  
  MCH_SITES_TMP <- c("MAG","LUG","COM","CEV","OTL","PIO","SBO")
  
  id_COL_WSPEED <- which(colnames(MCH_WIND)%in%paste(toupper(MCH_SITES_TMP),"_SUNSHINE",sep=""))
  yyy           <- as.matrix(MCH_WIND[,id_COL_WSPEED])
  yyy_FLAG      <- matrix(0,ncol=dim(yyy)[2],nrow=dim(yyy)[1])
  yyy_FLAG[which(!is.na(yyy))] <- 1
  
  xlabString <- "Date" 
  ylabString <- expression(paste("Sunshine [min]"))
  figname    <- paste(resultdir,"/MCH_SUNSHINE_SOUTHALPS.pdf",sep="")
  legend_str <- gsub(pattern = "_WSPEED",replacement = "",x=colnames(MCH_WIND)[id_COL_WSPEED])
  plot_ts_NETWORK(figname,MCH_WIND$date,yyy,yyy_FLAG,"week",NULL,c(0,11),xlabString,ylabString,legend_str,15*60)
}


### ----------------------------------------------------------------------------------------------------------------------------

## Import of CO2 data (+) from NABEL DUE/RIG/HAE/PAY + UNIBE BRM + EMPA LAEG + UNIBE GIMM

query_str <- paste("SELECT timestamp,CO2_DRY_CAL,CO2_DRY_F,H2O,H2O_F FROM NABEL_DUE WHERE CO2_DRY_CAL != -999 and H2O != -999 and H2O_F=1 and timestamp >= ",REF_data_timestamp_from," and timestamp < ",REF_data_timestamp_to,";",sep="")
drv       <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res       <- dbSendQuery(con, query_str)
data      <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

colnames(data)[which(colnames(data)=="CO2_DRY_CAL")] <- "CO2_DRY"

data       <- data[order(data$timestamp),]

data$CO2   <- data$CO2_DRY * (1 - data$H2O/100)
data$CO2_F <- data$CO2_DRY_F * data$H2O_F

colnames(data)[which(colnames(data)=="CO2")]   <- "DUE_CO2"
colnames(data)[which(colnames(data)=="CO2_F")] <- "DUE_CO2_F"

data       <- data[,c(which(colnames(data)=="timestamp"),which(colnames(data)=="DUE_CO2"),which(colnames(data)=="DUE_CO2_F"))]

# 

query_str <- paste("SELECT timestamp,CO2_WET_COMP FROM NABEL_PAY WHERE CO2_WET_COMP != -999 and timestamp >= ",REF_data_timestamp_from," and timestamp < ",REF_data_timestamp_to,";",sep="")
drv       <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res       <- dbSendQuery(con, query_str)
tmp       <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

colnames(tmp)[which(colnames(tmp)=="CO2_WET_COMP")] <- "CO2"
tmp$CO2_F     <- rep(1,dim(tmp)[1])
tmp           <- tmp[,c(which(colnames(tmp)=="timestamp"),which(colnames(tmp)=="CO2"),which(colnames(tmp)=="CO2_F"))]
tmp           <- tmp[order(tmp$timestamp),]
colnames(tmp) <- c("timestamp","PAY_CO2","PAY_CO2_F")
data          <- merge(data,tmp,all=T)

#

query_str <- paste("SELECT timestamp,CO2_WET_COMP FROM NABEL_RIG WHERE CO2_WET_COMP != -999 and timestamp >= ",REF_data_timestamp_from," and timestamp < ",REF_data_timestamp_to,";",sep="")
drv       <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res       <- dbSendQuery(con, query_str)
tmp       <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

colnames(tmp)[which(colnames(tmp)=="CO2_WET_COMP")] <- "CO2"
tmp$CO2_F     <- rep(1,dim(tmp)[1])
tmp           <- tmp[,c(which(colnames(tmp)=="timestamp"),which(colnames(tmp)=="CO2"),which(colnames(tmp)=="CO2_F"))]
tmp           <- tmp[order(tmp$timestamp),]
colnames(tmp) <- c("timestamp","RIG_CO2","RIG_CO2_F")
data          <- merge(data,tmp,all=T)

#

query_str <- paste("SELECT timestamp,CO2_WET_COMP FROM NABEL_HAE WHERE CO2_WET_COMP != -999 and timestamp >= ",REF_data_timestamp_from," and timestamp < ",REF_data_timestamp_to,";",sep="")
drv       <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res       <- dbSendQuery(con, query_str)
tmp       <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

colnames(tmp)[which(colnames(tmp)=="CO2_WET_COMP")] <- "CO2"
tmp$CO2_F     <- rep(1,dim(tmp)[1])
tmp           <- tmp[,c(which(colnames(tmp)=="timestamp"),which(colnames(tmp)=="CO2"),which(colnames(tmp)=="CO2_F"))]
tmp           <- tmp[order(tmp$timestamp),]
colnames(tmp) <- c("timestamp","HAE_CO2","HAE_CO2_F")
data          <- merge(data,tmp,all=T)

#

query_str <- paste("SELECT timestamp,CO2,CO2_F FROM EMPA_LAEG WHERE CO2 != -999 and CO2_F = 1 and timestamp >= ",REF_data_timestamp_from," and timestamp < ",REF_data_timestamp_to,";",sep="")
drv       <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res       <- dbSendQuery(con, query_str)
tmp       <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tmp           <- tmp[order(tmp$timestamp),]
colnames(tmp) <- c("timestamp","LAEG_CO2","LAEG_CO2_F")
data          <- merge(data,tmp,all=T)

#

query_str <- paste("SELECT timestamp,CO2,CO2_F FROM UNIBE_GIMM WHERE CO2 != -999 and CO2_F = 1 and timestamp >= ",REF_data_timestamp_from," and timestamp < ",REF_data_timestamp_to,";",sep="")
drv       <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res       <- dbSendQuery(con, query_str)
tmp       <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tmp           <- tmp[order(tmp$timestamp),]
colnames(tmp) <- c("timestamp","GIMM_CO2","GIMM_CO2_F")
data          <- merge(data,tmp,all=T)

#

query_str <- paste("SELECT timestamp,CO2,CO2_F FROM UNIBE_BRM WHERE CO2 != -999 and CO2_F = 1 and timestamp >= ",REF_data_timestamp_from," and timestamp < ",REF_data_timestamp_to,";",sep="")
drv       <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res       <- dbSendQuery(con, query_str)
tmp       <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tmp           <- tmp[order(tmp$timestamp),]
colnames(tmp) <- c("timestamp","BRM_CO2","BRM_CO2_F")
data          <- merge(data,tmp,all=T)

#

rm(tmp)
gc()

#

id_setToNA <- which(data$DUE_CO2 == -999 | data$DUE_CO2_F == 0)
if(length(id_setToNA)>0){
  data$DUE_CO2[id_setToNA] <- NA
}

id_setToNA <- which(data$RIG_CO2 == -999 | data$RIG_CO2_F == 0)
if(length(id_setToNA)>0){
  data$RIG_CO2[id_setToNA] <- NA
}

id_setToNA <- which(data$PAY_CO2 == -999 | data$PAY_CO2_F == 0)
if(length(id_setToNA)>0){
  data$PAY_CO2[id_setToNA] <- NA
}

id_setToNA <- which(data$HAE_CO2 == -999 | data$HAE_CO2_F == 0)
if(length(id_setToNA)>0){
  data$HAE_CO2[id_setToNA] <- NA
}

id_setToNA <- which(data$LAEG_CO2 == -999 | data$LAEG_CO2_F == 0)
if(length(id_setToNA)>0){
  data$LAEG_CO2[id_setToNA] <- NA
}

id_setToNA <- which(data$GIMM_CO2 == -999 | data$GIMM_CO2_F == 0)
if(length(id_setToNA)>0){
  data$GIMM_CO2[id_setToNA] <- NA
}

id_setToNA <- which(data$BRM_CO2 == -999 | data$BRM_CO2_F == 0)
if(length(id_setToNA)>0){
  data$BRM_CO2[id_setToNA] <- NA
}

data <- data[,which(!colnames(data)%in%c("DUE_CO2_F","RIG_CO2_F","PAY_CO2_F","GIMM_CO2_F","BRM_CO2_F","HAE_CO2_F","LAEG_CO2_F"))]

rm(id_setToNA)
gc()

#

data$date      <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
data$date      <- as.POSIXct(data$date,tz="UTC")
data           <- timeAverage(data,avg.time = "10 min",statistic="mean",start.date = strftime(strptime(min(data$date),"%Y%m%d%H0000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
data           <- as.data.frame(data,stringsAsFactors=F)
data$timestamp <- as.numeric(difftime(time1=data$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

#

tmp              <- timeAverage(mydata = data.frame(date=data$date,CO2=data$DUE_CO2,stringsAsFactors = F),
                                avg.time = "week",
                                data.thresh = 50,
                                statistic = "percentile",
                                percentile = 20,
                                start.date = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"))
tmp              <- approx(x=tmp$date+3.5*86400,y=tmp$CO2,xout=data$date,method="linear",rule = 2)
data$DUE_CO2_BSL <- tmp$y

#


id_COL_CO2     <- which(colnames(data)%in%paste(c("DUE","PAY","RIG","HAE","BRM","LAEG","GIMM"),"_CO2",sep=""))

data$CO2_RANGE <- apply(data[,id_COL_CO2],1,max,na.rm=T) - apply(data[,id_COL_CO2],1,min,na.rm=T)
data$N_CO2     <- apply(!is.na(data[,id_COL_CO2]),1,sum)

### ----------------------------------------------------------------------------------------------------------------------------

## Import of CO2 data (+) from deployed HPPs

query_str <- paste("SELECT SensorUnit_ID, LocationName FROM Deployment WHERE SensorUnit_ID BETWEEN 426 and 445 and LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1');",sep="")
drv       <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res       <- dbSendQuery(con, query_str)
HPP_SU_LOC<- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)


# 

HPP_data <- NULL

for(ith_HPP_SU_LOC in 1:dim(HPP_SU_LOC)[1]){
  
  query_str <- paste("SELECT timestamp,CO2_CAL_ADJ FROM CarboSense_HPP_CO2 WHERE LocationName = '",HPP_SU_LOC$LocationName[ith_HPP_SU_LOC],"' and SensorUnit_ID = ",HPP_SU_LOC$SensorUnit_ID[ith_HPP_SU_LOC]," and CO2_CAL_ADJ != -999 and Valve = 0 and timestamp >= ",REF_data_timestamp_from," and timestamp < ",REF_data_timestamp_to,";",sep="")
  drv       <- dbDriver("MySQL")
  con<-carboutil::get_conn(group="CarboSense_MySQL")
  res       <- dbSendQuery(con, query_str)
  tmp       <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  if(dim(tmp)[1]==0){
    next
  }
  
  colnames(tmp)[which(colnames(tmp)=="CO2_CAL_ADJ")] <- "CO2"
  
  tmp           <- tmp[order(tmp$timestamp),]
  
  colnames(tmp) <- c("timestamp",
                     paste("HPP_",HPP_SU_LOC$LocationName[ith_HPP_SU_LOC],"_",HPP_SU_LOC$SensorUnit_ID[ith_HPP_SU_LOC],"_CO2",  sep=""))
  
  if(ith_HPP_SU_LOC==1){
    HPP_data <- tmp
  }else{
    HPP_data <- merge(HPP_data,tmp,all=T)
  }
}

#

HPP_data$date      <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + HPP_data$timestamp
HPP_data$date      <- as.POSIXct(HPP_data$date,tz="UTC")
HPP_data           <- timeAverage(HPP_data,avg.time = "10 min",statistic="mean",start.date = strftime(strptime(min(HPP_data$date),"%Y%m%d%H0000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
HPP_data           <- as.data.frame(HPP_data,stringsAsFactors=F)
HPP_data$timestamp <- as.numeric(difftime(time1=HPP_data$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

#

HPP_data           <- HPP_data[,which(!colnames(HPP_data)%in%c("date"))]

data               <- data[,which(!colnames(data)%in%c("date"))]

data               <- merge(data,HPP_data,all=T,by="timestamp")

#


rm(HPP_data)
gc()

### ----------------------------------------------------------------------------------------------------------------------------

## ADJUST data structures "MCH_WIND" and "data"

min_timestamp  <- max(c(min(data$timestamp),min(MCH_WIND$timestamp)))
max_timestamp  <- min(c(max(data$timestamp),max(MCH_WIND$timestamp)))

min_date       <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + min_timestamp
max_date       <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + max_timestamp

all_timestamps <- seq(min_timestamp,max_timestamp,600)

data           <- data[data$timestamp         >= min_timestamp & data$timestamp     <= max_timestamp,]
MCH_WIND       <- MCH_WIND[MCH_WIND$timestamp >= min_timestamp & MCH_WIND$timestamp <= max_timestamp,]

data           <- merge(x = data.frame(timestamp=all_timestamps,stringsAsFactors = F),y = data,    all.x=T)
MCH_WIND       <- merge(x = data.frame(timestamp=all_timestamps,stringsAsFactors = F),y = MCH_WIND,all.x=T)

MCH_WIND$date  <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + MCH_WIND$timestamp
data$date      <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp

if(any(data$timestamp!=MCH_WIND$timestamp)){
  stop("data / MCH_WIND : timestamps do not match.")
}

if(any(as.POSIXct(data$date)!=as.POSIXct(MCH_WIND$date))){
  stop("data / MCH_WIND : dates do not match.")
}

if(dim(data)[1]!=dim(MCH_WIND)[1]){
  stop("data / MCH_WIND : dimensions do not match.")
}

# Additional columns for time

MCH_WIND$hour      <- as.numeric(strftime(MCH_WIND$date,"%H",tz="UTC"))

data$hour          <- as.numeric(strftime(data$date,"%H",tz="UTC"))
data$month         <- as.numeric(strftime(data$date,"%m",tz="UTC"))
data$dow           <- as.numeric(strftime(data$date,"%w",tz="UTC"))

### ----------------------------------------------------------------------------------------------------------------------------

## Import SU event file

if(apply_SU_events){
  SU_events                <- read.table(SU_events_fn,header=T,sep=";")
  SU_events$Date_UTC_from  <- strptime(SU_events$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  SU_events$Date_UTC_to    <- strptime(SU_events$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  SU_events$timestamp_from <- as.numeric(difftime(time1=SU_events$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
  SU_events$timestamp_to   <- as.numeric(difftime(time1=SU_events$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
}

### ----------------------------------------------------------------------------------------------------------------------------

## Drift correction
#
#  Loop over all sensor units 
#

if(T){
  
  anchor_events_SU_ALL       <- NULL
  
  for(SU_id in 1010:1334){
    
    #
    
    print(SU_id)
    
    # if(SU_id < 1069){
    #   next
    # }
    
    
    # Get deployment information for sensor unit
    
    if(!COMP_DUE){
      id_depl   <- which(tbl_deployment$SensorUnit_ID==SU_id
                         & !tbl_deployment$LocationName%in%c("DUE1","DUE2","DUE3","DUE4","DUE5","MET1"))
    }
    if(COMP_DUE){
      id_depl   <- which(tbl_deployment$SensorUnit_ID==SU_id
                         & tbl_deployment$LocationName%in%c("DUE1"))
    }
    
    n_id_depl <- length(id_depl)
    
    if(n_id_depl==0){
      next
    }
    
    #
    
    for(ith_depl in 1:n_id_depl){
      
      # Apply SU events or not
      
      if(apply_SU_events){
        
        id_SU_events   <- which(SU_events$SensorUnit_ID    == SU_id
                                & SU_events$LocationName   == tbl_deployment$LocationName[id_depl[ith_depl]]
                                & SU_events$timestamp_from >= tbl_deployment$timestamp_from[id_depl[ith_depl]]
                                & SU_events$timestamp_to   <= tbl_deployment$timestamp_to[id_depl[ith_depl]])
        
        n_id_SU_events <- length(id_SU_events)
        
        if(n_id_SU_events>0){
          processing_blocks <- data.frame(timestamp_from=c(tbl_deployment$timestamp_from[id_depl[ith_depl]],SU_events$timestamp_to[id_SU_events]),
                                          timestamp_to  =c(SU_events$timestamp_from[id_SU_events],tbl_deployment$timestamp_to[id_depl[ith_depl]]),
                                          stringsAsFactors = F)
        }else{
          processing_blocks <- data.frame(timestamp_from=c(tbl_deployment$timestamp_from[id_depl[ith_depl]]),
                                          timestamp_to  =c(tbl_deployment$timestamp_to[id_depl[ith_depl]]),
                                          stringsAsFactors = F)
        }
      }else{
        processing_blocks <- data.frame(timestamp_from=c(tbl_deployment$timestamp_from[id_depl[ith_depl]]),
                                        timestamp_to  =c(tbl_deployment$timestamp_to[id_depl[ith_depl]]),
                                        stringsAsFactors = F)
      }
      
      id_keep_pb <- which(!(processing_blocks$timestamp_from   >= SENSOR_data_ANALYSE_timestamp_to 
                            & processing_blocks$timestamp_to   >= SENSOR_data_ANALYSE_timestamp_to)
                          & !(processing_blocks$timestamp_from <= SENSOR_data_ANALYSE_timestamp_from 
                              & processing_blocks$timestamp_to <= SENSOR_data_ANALYSE_timestamp_from))
      
      if(length(id_keep_pb)>0){
        processing_blocks <- processing_blocks[id_keep_pb,]
      }else{
        next
      }
      
      id_adjDate <- which(processing_blocks$timestamp_from < SENSOR_data_ANALYSE_timestamp_from)
      if(length(id_adjDate)>0){
        processing_blocks$timestamp_from[id_adjDate] <- SENSOR_data_ANALYSE_timestamp_from
      }
      
      id_adjDate <- which(processing_blocks$timestamp_to > SENSOR_data_ANALYSE_timestamp_to)
      if(length(id_adjDate)>0){
        processing_blocks$timestamp_to[id_adjDate] <- SENSOR_data_ANALYSE_timestamp_to
      }
      
      n_processing_blocks <- dim(processing_blocks)[1]
      
      rm(id_adjDate,id_keep_pb)
      gc()
      
      
      # Get location information for deployment
      
      id_loc    <- which(tbl_location$LocationName==tbl_deployment$LocationName[id_depl[ith_depl]])
      
      ALTITUDE  <- tbl_location$h[id_loc]
      CANTON    <- tbl_location$Canton[id_loc]
      
      # Selection of MCH sites around sensor unit
      
      MCH_SITES_DEPL   <- tbl_location_MCH$LocationName[which(sqrt((tbl_location$Y_LV03[id_loc]-tbl_location_MCH$Y_LV03)^2+(tbl_location$X_LV03[id_loc]-tbl_location_MCH$X_LV03)^2)<40000)]
      n_MCH_SITES_DEPL <- length(MCH_SITES_DEPL)
      
      if((n_MCH_SITES_DEPL < 4 & !tbl_deployment$LocationName[id_depl[ith_depl]]%in%c("FAH","CHRI","CASP"))
         | (tbl_deployment$LocationName[id_depl[ith_depl]]=="FAH"  & n_MCH_SITES_DEPL<2)
         | (tbl_deployment$LocationName[id_depl[ith_depl]]=="CHRI" & n_MCH_SITES_DEPL<3)
         | (tbl_deployment$LocationName[id_depl[ith_depl]]=="CASP" & n_MCH_SITES_DEPL<3)){
        print(paste("FAIL",tbl_location$LocationName[id_loc],n_MCH_SITES_DEPL))
        next
      }
      
      col_MCH_SITES_DEPL <- which(colnames(MCH_WIND)%in%paste(MCH_SITES_DEPL,"_WSPEED",sep=""))
      
      # Select windy periods / well mixed atmosphere
      
      WIND_SITUATION        <- rep(F,dim(MCH_WIND)[1])
      MIXED_ATMOS_SITUATION <- rep(F,dim(MCH_WIND)[1])
      
      id_test_situation     <- which(MCH_WIND$timestamp   >= tbl_deployment$timestamp_from[id_depl[ith_depl]] 
                                     & MCH_WIND$timestamp <  tbl_deployment$timestamp_to[id_depl[ith_depl]])
      
      if(length(id_test_situation)==0){
        next
      }
      
      # Version: v > 2 m/s
      if(F){
        WIND_SITUATION[id_test_situation] <- (apply(MCH_WIND[id_test_situation,col_MCH_SITES_DEPL]>2 | is.na(MCH_WIND[id_test_situation,col_MCH_SITES_DEPL]),1,sum) == n_MCH_SITES_DEPL)
      }
      
      # Version: (v > 2 m/s) | (v > 0.75 & median(v) > 3 m/s)
      if(T){
        WIND_SITUATION[id_test_situation] <- (apply(MCH_WIND[id_test_situation,col_MCH_SITES_DEPL]>2 | is.na(MCH_WIND[id_test_situation,col_MCH_SITES_DEPL]),1,sum) == n_MCH_SITES_DEPL
                                              | (apply(MCH_WIND[id_test_situation,col_MCH_SITES_DEPL]>0.75 | is.na(MCH_WIND[id_test_situation,col_MCH_SITES_DEPL]),1,sum) == n_MCH_SITES_DEPL
                                                 & apply(MCH_WIND[id_test_situation,col_MCH_SITES_DEPL],1,median,na.rm=T) > 3))
      }
      
      
      col_MCH_SITES_DEPL_SUNSHINE <- which(colnames(MCH_WIND)%in%paste(MCH_SITES_DEPL,"_SUNSHINE",sep=""))
      
      # Version: sunshine >= 8 minutes & wind speed > 1 m/s & time between 11:00 UTC and 15:00 UTC
      MIXED_ATMOS_SITUATION[id_test_situation]   <- (apply(MCH_WIND[id_test_situation,col_MCH_SITES_DEPL_SUNSHINE]>=8 | is.na(MCH_WIND[id_test_situation,col_MCH_SITES_DEPL_SUNSHINE]),1,sum) == n_MCH_SITES_DEPL
                                                     & apply(MCH_WIND[id_test_situation,col_MCH_SITES_DEPL] >= 1.0    | is.na(MCH_WIND[id_test_situation,col_MCH_SITES_DEPL]),1,sum) == n_MCH_SITES_DEPL
                                                     & MCH_WIND$hour[id_test_situation] %in% c(11,12,13,14))
      
      # Limitation of correction for traffic sites to 00 UTC - 5 UTC 
      if(tbl_deployment$LocationName[id_depl[ith_depl]]%in%c("ZMAN","ZSBS")){
        WIND_SITUATION[id_test_situation] <- WIND_SITUATION[id_test_situation] & (MCH_WIND$hour[id_test_situation] %in% c(0,1,2,3,4)) & MCH_WIND$date[id_test_situation] < strptime("20200307000000","%Y%m%d%H%M%S",tz="UTC")
      }
      if(tbl_deployment$LocationName[id_depl[ith_depl]]%in%c("ZSCH")){
        WIND_SITUATION[id_test_situation] <- WIND_SITUATION[id_test_situation] & (MCH_WIND$hour[id_test_situation] %in% c(0,1,2,3,4))
      }
      
      if(all(WIND_SITUATION==F)){
        print("AAA")
        next
      }
      
      
      ## WIND ONLY / WIND + mixed atmosphere
      
      if(F){
        dist2DUE <- sqrt((tbl_location$Y_LV03[id_loc]-688680)^2+(tbl_location$X_LV03[id_loc]-250902)^2)
        
        if(dist2DUE>20000 & !(tbl_deployment$LocationName[id_depl[ith_depl]]%in%c("PAY","PAYN","RIG","HAE","BRM","LAEG","ZUE","ZSCH","ZHBR","ALBS","FROB","CHRI","TAEN","SEMP","ESMO","REH","BNTG","SAVE","SSAL","MAGN"))){
          if(any(MIXED_ATMOS_SITUATION==T)){
            WIND_SITUATION <- WIND_SITUATION | MIXED_ATMOS_SITUATION
          }
        }
      }
      
      # Time series of wind speeds of neighbouring sites
      
      if(F){
        
        if(tbl_deployment$LocationName[id_depl[ith_depl]] %in% c("ZHRZ","PAY","HAE","RIG","LAEG","BRM","CHRI","FROB","ALBS","BNTG","SAVE","RECK","ZUE","ZSCH","ZHBR","ESMO","MAGN","TAEN","SSAL")){
          
          tmp_ws <- rep(NA,length(WIND_SITUATION))
          tmp_ws[WIND_SITUATION] <- 0.0
          
          id_COL_WSPEED <- which(colnames(MCH_WIND)%in%paste(toupper(MCH_SITES_DEPL),"_WSPEED",sep=""))
          yyy           <- as.matrix(cbind(MCH_WIND[,id_COL_WSPEED],rep(2,dim(data)[1]),rep(1,dim(data)[1]),tmp_ws))
          yyy_FLAG      <- matrix(0,ncol=dim(yyy)[2]+1,nrow=dim(yyy)[1])
          yyy_FLAG[which(!is.na(yyy))] <- 1
          
          xlabString <- "Date" 
          ylabString <- expression(paste("Wind speed [m/s]"))
          figname    <- paste(resultdir,"/",tbl_deployment$LocationName[id_depl[ith_depl]],"_",tbl_deployment$SensorUnit_ID[id_depl[ith_depl]],"_MCH_WINDS.pdf",sep="")
          legend_str <- c(gsub(pattern = "_WSPEED",replacement = "",x=colnames(MCH_WIND)[id_COL_WSPEED]),"LIMIT_2","LIMIT_1","WSIT")
          plot_ts_NETWORK(figname,MCH_WIND$date,yyy,yyy_FLAG,"week",NULL,c(0,20),xlabString,ylabString,legend_str,15*60)
        }
      }
      
      ## Adjustment events (split of these events in case they last longer than 4 h)
      
      ADJUSTMENT_EVENT_NO                 <- rep(-999,dim(data)[1])
      ADJUSTMENT_EVENT_NO[WIND_SITUATION] <- c(0,cumsum(diff(MCH_WIND$timestamp[WIND_SITUATION])>700))
      u_ADJUSTMENT_EVENT_NO               <- sort(unique(ADJUSTMENT_EVENT_NO[ADJUSTMENT_EVENT_NO != -999]))
      n_u_ADJUSTMENT_EVENT_NO             <- length(u_ADJUSTMENT_EVENT_NO)
      
      for(ith_uAENO in 1:n_u_ADJUSTMENT_EVENT_NO){
        
        id_WIND         <- which(ADJUSTMENT_EVENT_NO==u_ADJUSTMENT_EVENT_NO[ith_uAENO])
        n_id_WIND       <- length(id_WIND)
        
        n_id_WIND_parts <- (n_id_WIND%/%12)
        
        if(n_id_WIND_parts>1){
          ADJUSTMENT_EVENT_NO[id_WIND] <- ADJUSTMENT_EVENT_NO[id_WIND] + (((0:(n_id_WIND-1))%/%(n_id_WIND/n_id_WIND_parts))/n_id_WIND_parts)
        }
      }
      
      u_ADJUSTMENT_EVENT_NO               <- sort(unique(ADJUSTMENT_EVENT_NO[which(ADJUSTMENT_EVENT_NO != -999)]))
      n_u_ADJUSTMENT_EVENT_NO             <- length(u_ADJUSTMENT_EVENT_NO)
      
      
      ## Selection of anchor REF site for each windy period
      #
      #  Depends on location of LP8 sensor and the availability of reference measurements in specific region 
      #
      
      for(ith_uAENO in 1:n_u_ADJUSTMENT_EVENT_NO){
        
        id_WIND   <- which(ADJUSTMENT_EVENT_NO==u_ADJUSTMENT_EVENT_NO[ith_uAENO])
        n_id_WIND <- length(id_WIND)
        
        # n_id_WIND: 6/9/12
        if(n_id_WIND<9){
          next
        }
        
        # id_wind_ts_from <- as.numeric(difftime(time1=min(data$date[id_WIND]), time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
        # id_wind_ts_to   <- as.numeric(difftime(time1=max(data$date[id_WIND]), time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
        
        id_wind_ts_from <- min(data$timestamp[id_WIND])
        id_wind_ts_to   <- max(data$timestamp[id_WIND])
        
        id_REF_options           <- NULL
        multiple_refsite_options <- F
        
        if(CANTON=="TI" | tbl_deployment$LocationName[id_depl[ith_depl]]=="GRO"){
          id_REF_options         <- which(REF_SITES$status==1
                                          & REF_SITES$timestamp_from < id_wind_ts_from
                                          & REF_SITES$timestamp_to   > id_wind_ts_to
                                          & REF_SITES$canton=="TI")
          
          if(length(id_REF_options)==0){
            id_REF_options <- which(REF_SITES$name=="DUEBSL")
          }
          
          multiple_refsite_options <- T
        }
        if(CANTON=="VS"){
          id_REF_options         <- which(REF_SITES$status==1
                                          & REF_SITES$timestamp_from < id_wind_ts_from
                                          & REF_SITES$timestamp_to   > id_wind_ts_to
                                          & REF_SITES$canton=="VS")
          if(length(id_REF_options)==0){
            id_REF_options <- which(REF_SITES$name=="DUEBSL")
          }
          
          multiple_refsite_options <- F
        }
        if(CANTON=="GR" & tbl_deployment$LocationName[id_depl[ith_depl]]!="GRO"){
          id_REF_options         <- which(REF_SITES$status==1
                                          & REF_SITES$timestamp_from < id_wind_ts_from
                                          & REF_SITES$timestamp_to   > id_wind_ts_to
                                          & REF_SITES$canton=="GR")
          if(length(id_REF_options)==0){
            id_REF_options <- which(REF_SITES$name=="DUEBSL")
          }
          
          multiple_refsite_options <- F
        }
        
        if(!CANTON%in%c("TI","GR","VS") | tbl_deployment$LocationName[id_depl[ith_depl]]=="SAVE"){
          id_REF_options         <- which(REF_SITES$status==1
                                          & REF_SITES$timestamp_from < id_wind_ts_from
                                          & REF_SITES$timestamp_to   > id_wind_ts_to
                                          & !REF_SITES$canton%in%c("TI","GR","VS")
                                          & !REF_SITES$name%in%c("DUEBSL","GIMM","CHRI","LAEG","BRM"))
          
          multiple_refsite_options <- F
        }
        
        # if(tbl_deployment$LocationName[id_depl[ith_depl]]%in%c("CHRI","BAS","BASN","MOE")){
        #   id_REF_options         <- which(REF_SITES$status==1
        #                                   & REF_SITES$timestamp_from < id_wind_ts_from
        #                                   & REF_SITES$timestamp_to   > id_wind_ts_to
        #                                   & REF_SITES$name%in%c("CHRI","DUE"))
        # }
        
        if(tbl_deployment$LocationName[id_depl[ith_depl]]%in%c("PAY","PAYN")){
          id_REF_options         <- which(REF_SITES$status==1
                                          & REF_SITES$timestamp_from < id_wind_ts_from
                                          & REF_SITES$timestamp_to   > id_wind_ts_to
                                          & REF_SITES$name%in%c("GIMM"))
          
          multiple_refsite_options <- F
        }
        
        if(tbl_deployment$LocationName[id_depl[ith_depl]]=="DUE1"){
          id_REF_options         <- which(REF_SITES$status==1
                                          & REF_SITES$timestamp_from < id_wind_ts_from
                                          & REF_SITES$timestamp_to   > id_wind_ts_to
                                          & REF_SITES$name%in%c("LAEG","BRM"))
          
          multiple_refsite_options <- T
        }
        
        if(tbl_deployment$SensorUnit_ID[id_depl[ith_depl]]==1222 & tbl_deployment$LocationName[id_depl[ith_depl]]=="MAGN"){
          id_REF_options         <- which(REF_SITES$status==1
                                          & REF_SITES$timestamp_from < id_wind_ts_from
                                          & REF_SITES$timestamp_to   > id_wind_ts_to
                                          & REF_SITES$name%in%c("SSAL"))
          
          multiple_refsite_options <- F
        }
        
        if(tbl_deployment$LocationName[id_depl[ith_depl]]=="SSAL"){
          id_REF_options         <- which(REF_SITES$status==1
                                          & REF_SITES$timestamp_from < id_wind_ts_from
                                          & REF_SITES$timestamp_to   > id_wind_ts_to
                                          & REF_SITES$canton=="TI"
                                          & !REF_SITES$name%in%c("SSAL"))
          
          if(length(id_REF_options)==0){
            id_REF_options <- which(REF_SITES$name=="DUEBSL")
          }
          
          multiple_refsite_options <- T
        }
        
        
        if(is.null(id_REF_options) | length(id_REF_options)==0){
          stop("IS.NULL(id_REF_options)/length(id_REF_options)=0: 'id_REF_options'")
        }
        
        dist2REF                <- sqrt((tbl_location$Y_LV03[id_loc]-REF_SITES$y)^2+(tbl_location$X_LV03[id_loc]-REF_SITES$x)^2)
        hdiff2REF               <- tbl_location$h[id_loc]-REF_SITES$h
        
        id_REF_selected         <- id_REF_options[order(dist2REF[id_REF_options],decreasing = F)]
        
        REF_site_selection_done <- F
        
        for(ith_id_REF_selected in 1:length(id_REF_selected)){
          
          if(REF_site_selection_done){
            next
          }
          
          if(multiple_refsite_options==F & ith_id_REF_selected>1){
            next
          }
          
          REF_site_selected      <- REF_SITES$name[id_REF_selected[ith_id_REF_selected]]
          dist2REF_site_selected <- dist2REF[id_REF_selected[ith_id_REF_selected]]
          
          if(REF_site_selected=="DUEBSL"){
            pos_REF_CO2 <- which(colnames(data)==paste("DUE_CO2_BSL",sep=""))
          }else{
            pos_REF_CO2 <- grep(pattern = paste(REF_site_selected,"[[:print:]]{1,5}","CO2$",sep=""),x = colnames(data))
          }
          
          if(length(pos_REF_CO2)!=1){
            stop("'pos_REF_CO2' not unique!")
          }
          
          ###
          
          id_REF   <- id_WIND[which(!is.na(data[id_WIND,pos_REF_CO2]))]
          n_id_REF <- length(id_REF)
          
          if(n_id_REF<6){
            next
          }
          
          ###
          
          REF_site_selection_done <- T
        }
        
        if(!REF_site_selection_done){
          next
        }
        
        CO2_REF_median <- median(data[id_REF,pos_REF_CO2])
        CO2_REF_mean   <- mean(  data[id_REF,pos_REF_CO2])
        CO2_REF_sd     <- sd(    data[id_REF,pos_REF_CO2])
        CO2_REF_mad    <- mad(   data[id_REF,pos_REF_CO2])
        CO2_REF_rng    <- diff(range( data[id_REF,pos_REF_CO2]))
        
        if(CO2_REF_sd>4){
          next
        }
        
        
        anchor_events_SU_ALL <- rbind(anchor_events_SU_ALL,data.frame(SensorUnit_ID  = tbl_deployment$SensorUnit_ID[id_depl[ith_depl]],
                                                                      LocationName   = tbl_deployment$LocationName[id_depl[ith_depl]],
                                                                      ith_depl       = ith_depl,
                                                                      Date_UTC_from  = min(data$date[id_WIND]),
                                                                      Date_UTC_to    = max(data$date[id_WIND]),
                                                                      timestamp_from = as.numeric(difftime(time1=min(data$date[id_WIND]), time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                                                      timestamp_to   = as.numeric(difftime(time1=max(data$date[id_WIND]), time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                                                      Duration       = as.numeric(difftime(time1=max(data$date[id_WIND]), time2=min(data$date[id_WIND]), units="mins",tz="UTC")),
                                                                      CO2_REF_SITE   = REF_site_selected,
                                                                      DIST2REF       = dist2REF_site_selected,
                                                                      CO2_REF_median = CO2_REF_median,
                                                                      CO2_REF_mean   = CO2_REF_mean,
                                                                      CO2_REF_sd     = CO2_REF_sd,
                                                                      CO2_REF_mad    = CO2_REF_mad,
                                                                      CO2_REF_rng    = CO2_REF_rng,
                                                                      CO2_REF_N      = n_id_REF,
                                                                      CO2_RANGE      = max(data$CO2_RANGE[id_WIND]),
                                                                      CO2_SU_median  = NA,
                                                                      CO2_SU_mean    = NA,
                                                                      CO2_SU_sd      = NA,
                                                                      CO2_SU_mad     = NA,
                                                                      CO2_SU_N       = NA,
                                                                      SU_CO2_corr    = NA,
                                                                      WIND_SITUATION = as.integer(all(WIND_SITUATION[id_WIND]==T)),
                                                                      MIXED_ATMOS_SITUATION = as.integer(all(MIXED_ATMOS_SITUATION[id_WIND]==T)),
                                                                      stringsAsFactors = F))
        
      }
      
      
      #
      
      TS_UPDATE_FROM <- max(c(SENSOR_data_DBINSERT_timestamp_from,tbl_deployment$timestamp_from[id_depl[ith_depl]]))
      TS_UPDATE_TO   <- min(c(SENSOR_data_DBINSERT_timestamp_to  ,tbl_deployment$timestamp_to[id_depl[ith_depl]]))
      
      query_str <- paste("UPDATE ",ProcMeasDBTableName," SET CO2_A = -999 WHERE SensorUnit_ID = ",tbl_deployment$SensorUnit_ID[id_depl[ith_depl]]," and LocationName = '",tbl_deployment$LocationName[id_depl[ith_depl]],"' and SensorUnit_ID = ",SU_id," and timestamp >= ",TS_UPDATE_FROM," and timestamp <= ",TS_UPDATE_TO,"; ",sep="")
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn(group="CarboSense_MySQL")
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
      
      
      #
      
      
      for(ith_processing_block in 1:n_processing_blocks){
        
        query_str <- paste("SELECT timestamp, CO2, FLAG FROM ",ProcMeasDBTableName," WHERE SensorUnit_ID = ",SU_id," and LocationName = '",tbl_deployment$LocationName[id_depl[ith_depl]],"' and timestamp >= ",processing_blocks$timestamp_from[ith_processing_block]," and timestamp <= ",processing_blocks$timestamp_to[ith_processing_block],";",sep="")
        drv       <- dbDriver("MySQL")
        con<-carboutil::get_conn(group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        data_SU   <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        data_SU$date  <- data_SU$timestamp + strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")
        
        
        # Selection of anchor events
        
        id_AE   <- which(anchor_events_SU_ALL$SensorUnit_ID    == tbl_deployment$SensorUnit_ID[id_depl[ith_depl]]
                         & anchor_events_SU_ALL$LocationName   == tbl_deployment$LocationName[id_depl[ith_depl]]
                         & anchor_events_SU_ALL$timestamp_from >  processing_blocks$timestamp_from[ith_processing_block]
                         & anchor_events_SU_ALL$timestamp_to   <  processing_blocks$timestamp_to[ith_processing_block])
        
        n_id_AE <- length(id_AE)
        
        if(n_id_AE<1){
          next
        }
        
        AE_ok <- rep(F,n_id_AE)
        
        for(ith_AE in 1:n_id_AE){
          
          id_SU <- which(data_SU$timestamp   >= anchor_events_SU_ALL$timestamp_from[id_AE[ith_AE]]
                         & data_SU$timestamp <= anchor_events_SU_ALL$timestamp_to[id_AE[ith_AE]]
                         & data_SU$CO2       != -999
                         & data_SU$FLAG      == 1)
          
          n_id_SU <- length(id_SU)
          
          if(n_id_SU<6){
            next
          }
          
          CO2_SU_median  <- median(data_SU$CO2[id_SU])
          CO2_SU_mean    <- mean(  data_SU$CO2[id_SU])
          CO2_SU_sd      <- sd(    data_SU$CO2[id_SU])
          CO2_SU_mad     <- mad(   data_SU$CO2[id_SU])
          
          if(CO2_SU_sd>15){
            next
          }
          
          AE_ok[ith_AE] <- T
          
          anchor_events_SU_ALL$CO2_SU_median[id_AE[ith_AE]]  <- CO2_SU_median
          anchor_events_SU_ALL$CO2_SU_mean[id_AE[ith_AE]]    <- CO2_SU_mean
          anchor_events_SU_ALL$CO2_SU_sd[id_AE[ith_AE]]      <- CO2_SU_sd
          anchor_events_SU_ALL$CO2_SU_mad[id_AE[ith_AE]]     <- CO2_SU_mad
          anchor_events_SU_ALL$CO2_SU_N[id_AE[ith_AE]]       <- n_id_SU
          
          anchor_events_SU_ALL$SU_CO2_corr[id_AE[ith_AE]]    <- anchor_events_SU_ALL$CO2_REF_median[id_AE[ith_AE]] - anchor_events_SU_ALL$CO2_SU_median[id_AE[ith_AE]]
          
        }
        
        if(sum(AE_ok)==0){
          next
        }
        
        if(sum(AE_ok)>1){
          correction <- approx(x = 0.5*(anchor_events_SU_ALL$timestamp_from[id_AE[AE_ok]]+anchor_events_SU_ALL$timestamp_to[id_AE[AE_ok]]),
                               y = anchor_events_SU_ALL$SU_CO2_corr[id_AE[AE_ok]],data_SU$timestamp,rule = 2)
          correction <- correction$y
        }
        
        if(sum(AE_ok)==1){
          correction <- rep(anchor_events_SU_ALL$SU_CO2_corr[id_AE[AE_ok]],dim(data_SU)[1])
        }
        
        
        #
        
        id_insert <- which(data_SU$CO2!=-999
                           & !is.na(correction)
                           & data_SU$timestamp >= SENSOR_data_DBINSERT_timestamp_from
                           & data_SU$timestamp <= SENSOR_data_DBINSERT_timestamp_to)
        
        
        if(length(id_insert)>0){
          
          
          query_str <- paste("INSERT INTO ",ProcMeasDBTableName," (timestamp,SensorUnit_ID,LocationName,CO2_A) ",sep="")
          query_str <- paste(query_str,"VALUES ")
          query_str <- paste(query_str,
                             paste("(",paste(data_SU$timestamp[id_insert],",",
                                             rep(SU_id,length(id_insert)),",'",
                                             rep(tbl_deployment$LocationName[id_depl[ith_depl]],length(id_insert)),"',",
                                             data_SU$CO2[id_insert]+correction[id_insert],
                                             collapse = "),(",sep=""),")",sep=""),
                             paste(" ON DUPLICATE KEY UPDATE "))
          
          query_str       <- paste(query_str,paste("CO2_A=VALUES(CO2_A);",    sep=""))
          
          
          
          drv             <- dbDriver("MySQL")
          con<-carboutil::get_conn(group="CarboSense_MySQL")
          res             <- dbSendQuery(con, query_str)
          dbClearResult(res)
          dbDisconnect(con)
        }
      }
    }
  }
  
  #
  
  anchor_events_SU_ALL$Date_UTC_from <- strftime(anchor_events_SU_ALL$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  anchor_events_SU_ALL$Date_UTC_to   <- strftime(anchor_events_SU_ALL$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
  if(!COMP_DUE){
    write.table(x = anchor_events_SU_ALL,file = paste(resultdir,"/anchor_events_SU_ALL.csv",sep=""),col.names = T,row.names = F,sep=";")
  }
  if(COMP_DUE){
    write.table(x = anchor_events_SU_ALL,file = paste(resultdir,"/anchor_events_SU_ALL_DUE1.csv",sep=""),col.names = T,row.names = F,sep=";")
  }
  
}

if(COMP_DUE){
  stop()
}

### ----------------------------------------------------------------------------------------------------------------------------

anchor_events_SU_ALL <- read.table(paste(resultdir,"/anchor_events_SU_ALL.csv",sep=""),sep=";",header=T,as.is=T)

anchor_events_SU_ALL$Date_UTC_from  <- strptime(anchor_events_SU_ALL$Date_UTC_from,"%Y-%m-%d %H:%M:%S", tz="UTC")
anchor_events_SU_ALL$Date_UTC_to    <- strptime(anchor_events_SU_ALL$Date_UTC_to,  "%Y-%m-%d %H:%M:%S", tz="UTC")
anchor_events_SU_ALL$Date_UTC_mid   <- anchor_events_SU_ALL$Date_UTC_from + anchor_events_SU_ALL$Duration*60/2


### ----------------------------------------------------------------------------------------------------------------------------

u_SU_ID   <- sort(unique(anchor_events_SU_ALL$SensorUnit_ID))
n_u_SU_ID <- length(u_SU_ID)

min_date  <- min(anchor_events_SU_ALL$Date_UTC_mid)
max_date  <- max(anchor_events_SU_ALL$Date_UTC_mid)

xaxis_dates     <- seq(strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20190101000000","%Y%m%d%H%M%S",tz="UTC"),"month") 
xaxis_dates_str <- strftime(xaxis_dates,"%m/%y",tz="UTC")


figname <- paste(resultdir,"/CO2_adjustment_ZH.pdf",sep="")

def_par <- par()
pdf(file = figname, width=12, height=8, onefile=T, pointsize=12, colormodel="srgb")
par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))

plot(c(min_date,max_date),c(-1e3,-1e3),ylim=c(-250,250),
     xlab="Date [MM/YY]",ylab=expression(paste("CO"[2]*" adjustment [ppb]")),
     xaxt="n",
     cex.axis=1.25,
     cex.lab=1.25)
axis(side = 1,at = xaxis_dates,labels = xaxis_dates_str,cex.axis=1.25,cex.lab=1.25)

for(ll in seq(-500,500,100)){
  lines(c(min_date-86400*50,max_date+86400*50),c(ll,ll),col="gray40",lwd=1,lty=5)
}

counter <- 0

for(SU_ID in u_SU_ID){
  
  id   <- which(anchor_events_SU_ALL$SensorUnit_ID==SU_ID
                & !is.na(anchor_events_SU_ALL$SU_CO2_corr)
                & anchor_events_SU_ALL$DIST2REF < 500000)
  
  # & anchor_events_SU_ALL$CO2_REF_SITE   %in% c("DUE","PAY")
  
  n_id <- length(id)
  
  # print(SU_ID)
  
  if(n_id>=2){
    
    counter <- counter + 1
    
    for(ii in 2:n_id){
      
      if(anchor_events_SU_ALL$ith_depl[id[ii]]==anchor_events_SU_ALL$ith_depl[id[ii-1]]){
        lines(c(anchor_events_SU_ALL$Date_UTC_mid[id[ii-1]],anchor_events_SU_ALL$Date_UTC_mid[id[ii]]),
              c(anchor_events_SU_ALL$SU_CO2_corr[id[ii-1]],anchor_events_SU_ALL$SU_CO2_corr[id[ii]]),
              col="black",
              lwd=1)
        
        points(anchor_events_SU_ALL$Date_UTC_mid[id[ii]],anchor_events_SU_ALL$SU_CO2_corr[id[ii]],
               pch=16,
               col=2,
               cex=0.75)
        
        points(anchor_events_SU_ALL$Date_UTC_mid[id[ii-1]],anchor_events_SU_ALL$SU_CO2_corr[id[ii-1]],
               pch=16,
               col=2,
               cex=0.75)
      }
    }
  }
}

str_00 <- paste("N sensors:",sprintf("%.0f",counter))

legend("topleft",legend = c(str_00),bg="white",cex=1.25)

par(def_par)
dev.off()

### ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---   ---

if(T){
  
  ### EGU 2018
  
  # PIC 01
  
  figname <- paste(resultdir,"/CO2_adjustment_ZH_PIC01.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=12, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
  
  plot(c(min_date,max_date),c(-1e3,-1e3),ylim=c(-250,250),
       xlab="Date [MM/YY]",ylab=expression(paste("CO"[2]*" adjustment [ppb]")),
       xaxt="n",
       cex.axis=1.25,
       cex.lab=1.25)
  axis(side = 1,at = xaxis_dates,labels = xaxis_dates_str,cex.axis=1.25,cex.lab=1.25)
  
  for(ll in seq(-500,500,100)){
    lines(c(min_date-86400*50,max_date+86400*50),c(ll,ll),col="gray40",lwd=1,lty=5)
  }
  
  counter <- 0
  
  for(SU_ID in u_SU_ID){
    
    id   <- which(anchor_events_SU_ALL$SensorUnit_ID==SU_ID
                  & anchor_events_SU_ALL$DIST2REF <= 20000
                  & anchor_events_SU_ALL$CO2_REF_SITE=="DUE"
                  & anchor_events_SU_ALL$LocationName!="DUE1"
                  & !is.na(anchor_events_SU_ALL$SU_CO2_corr))
    
    n_id <- length(id)
    
    if(any(anchor_events_SU_ALL$SU_CO2_corr[id] < -100 | anchor_events_SU_ALL$SU_CO2_corr[id]> 75)){
      next
    }
    
    if(n_id>=2){
      
      counter <- counter + 1
      
      for(ii in 2:n_id){
        
        if(anchor_events_SU_ALL$ith_depl[id[ii]]==anchor_events_SU_ALL$ith_depl[id[ii-1]]){
          lines(c(anchor_events_SU_ALL$Date_UTC_mid[id[ii-1]],anchor_events_SU_ALL$Date_UTC_mid[id[ii]]),
                c(anchor_events_SU_ALL$SU_CO2_corr[id[ii-1]],anchor_events_SU_ALL$SU_CO2_corr[id[ii]]),
                col="black",
                lwd=1)
          
          xx1 <- anchor_events_SU_ALL$Date_UTC_mid[id[ii]]
          xx2 <- anchor_events_SU_ALL$Date_UTC_mid[id[ii]] + 7200
          yy1 <- -1e9
          yy2 <-  1e9
          
          polygon(c(xx1,xx2,xx2,xx1),c(yy1,yy1,yy2,yy2),col=2,border=NA)
          
          xx1 <- anchor_events_SU_ALL$Date_UTC_mid[id[ii-1]]
          xx2 <- anchor_events_SU_ALL$Date_UTC_mid[id[ii-1]] + 7200
          yy1 <- -1e9
          yy2 <-  1e9
          
          polygon(c(xx1,xx2,xx2,xx1),c(yy1,yy1,yy2,yy2),col=2,border=NA)
        }
      }
    }
  }
  
  str_00 <- paste("N sensors:",sprintf("%.0f",counter))
  
  legend("topleft",legend = c(str_00),bg="white",cex=1.25)
  
  par(def_par)
  dev.off()
  
  
  # PIC 02
  
  figname <- paste(resultdir,"/CO2_adjustment_ZH_PIC02.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=12, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
  
  plot(c(min_date,max_date),c(-1e3,-1e3),ylim=c(-250,250),
       xlab="Date [MM/YY]",ylab=expression(paste("CO"[2]*" adjustment [ppb]")),
       xaxt="n",
       cex.axis=1.25,
       cex.lab=1.25)
  axis(side = 1,at = xaxis_dates,labels = xaxis_dates_str,cex.axis=1.25,cex.lab=1.25)
  
  for(ll in seq(-500,500,100)){
    lines(c(min_date-86400*50,max_date+86400*50),c(ll,ll),col="gray40",lwd=1,lty=5)
  }
  
  counter <- 0
  
  for(SU_ID in u_SU_ID){
    
    id   <- which(anchor_events_SU_ALL$SensorUnit_ID==SU_ID
                  & anchor_events_SU_ALL$DIST2REF <= 20000
                  & anchor_events_SU_ALL$CO2_REF_SITE=="DUE"
                  & anchor_events_SU_ALL$LocationName!="DUE1"
                  & !is.na(anchor_events_SU_ALL$SU_CO2_corr))
    
    n_id <- length(id)
    
    
    
    if(n_id>=2){
      
      counter <- counter + 1
      
      for(ii in 2:n_id){
        
        if(anchor_events_SU_ALL$ith_depl[id[ii]]==anchor_events_SU_ALL$ith_depl[id[ii-1]]){
          lines(c(anchor_events_SU_ALL$Date_UTC_mid[id[ii-1]],anchor_events_SU_ALL$Date_UTC_mid[id[ii]]),
                c(anchor_events_SU_ALL$SU_CO2_corr[id[ii-1]],anchor_events_SU_ALL$SU_CO2_corr[id[ii]]),
                col="black",
                lwd=1)
          
          xx1 <- anchor_events_SU_ALL$Date_UTC_mid[id[ii]]
          xx2 <- anchor_events_SU_ALL$Date_UTC_mid[id[ii]] + 7200
          yy1 <- -1e9
          yy2 <-  1e9
          
          polygon(c(xx1,xx2,xx2,xx1),c(yy1,yy1,yy2,yy2),col=2,border=NA)
          
          xx1 <- anchor_events_SU_ALL$Date_UTC_mid[id[ii-1]]
          xx2 <- anchor_events_SU_ALL$Date_UTC_mid[id[ii-1]] + 7200
          yy1 <- -1e9
          yy2 <-  1e9
          
          polygon(c(xx1,xx2,xx2,xx1),c(yy1,yy1,yy2,yy2),col=2,border=NA)
        }
      }
    }
  }
  
  str_00 <- paste("N sensors:",sprintf("%.0f",counter))
  
  legend("topleft",legend = c(str_00),bg="white",cex=1.25)
  
  par(def_par)
  dev.off()
  
}


### ----------------------------------------------------------------------------------------------------------------------------

## Test of concept

ProofOfConcept_statistics <- NULL

for(mode in 2:1){
  
  figname <- paste(resultdir,"/DIFF_CO2_TESTSITE_REFSITE_HIST_MODE_",sprintf("%02.0f",mode),".pdf",sep="")
  
  
  def_par <- par()
  pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.5,0.1),mfrow=c(1,1))
  
  for(ref_site in c("DUE","PAY","GIMM","MAGN")){
    
    for(test_site in c("BRM","LAEG","HAE","RIG","GIMM","PAY","DUE","ZUE","MAGN","ZHBR","ZSCH","ALBS","BNTG_00","BNTG_01","FROB_00","FROB_01","CHRI","TAEN_00","TAEN_01","SEMP_00","SEMP_01","SEMP_02","ESMO","SAVE","RECK","SSAL","SOTT","HAE_MW","HAE_CA","HAE_TF","ZUE_TF","ZSCH_TF")){
      
      if(ref_site == test_site){
        next
      }
      
      winddir_ok <- rep(T,dim(MCH_WIND)[1])
      
      #
      
      if(test_site == "HAE_MW"){
        test_site  = "HAE"
        winddir_ok = MCH_WIND$NABHAE_WDIR >= 80 & MCH_WIND$NABHAE_WDIR <= 280
      }
      if(test_site == "HAE_CA"){
        test_site  = "HAE"
        winddir_ok = MCH_WIND$NABHAE_WDIR < 80 | MCH_WIND$NABHAE_WDIR > 280
      }
      if(test_site == "HAE_TF"){
        test_site  = "HAE"
        winddir_ok = MCH_WIND$hour %in% c(22,23,0,1,2,3)
      }
      if(test_site == "ZUE_TF"){
        test_site  = "ZUE"
        winddir_ok = MCH_WIND$hour %in% c(22,23,0,1,2,3)
      }
      if(test_site == "ZSCH_TF"){
        test_site  = "ZSCH"
        winddir_ok = MCH_WIND$hour %in% c(22,23,0,1,2,3)
      }
      
      #
      
      if(!ref_site%in%c("MAGN","ZUE","ZHBR","ZSCH","ALBS","FROB","BNTG","CHRI","TAEN","SEMP","ESMO","SAVE","RECK","SSAL","SOTT")){
        ref_pos <- which(colnames(data)==paste(ref_site, "_CO2",  sep=""))
      }
      if(ref_site=="MAGN"){
        ref_pos <- which(colnames(data)==paste("HPP_MAGN_438_CO2",  sep=""))
      }
      
      #
      
      
      if(!test_site%in%c("MAGN","ZUE","ZHBR","ZSCH","ALBS","FROB","BNTG","CHRI","TAEN","SEMP","ESMO","SAVE","RECK","SSAL","SOTT")){
        pos   <- which(colnames(data)==paste(test_site,"_CO2",  sep=""))
      }
      if(test_site=="ZUE"){
        pos   <- which(colnames(data)==paste("HPP_ZUE_426_CO2",  sep=""))
      }
      if(test_site=="MAGN"){
        pos   <- which(colnames(data)==paste("HPP_MAGN_438_CO2",  sep=""))
      }
      if(test_site=="ZHBR"){
        pos   <- which(colnames(data)==paste("HPP_ZHBR_428_CO2",  sep=""))
      }
      if(test_site=="ZSCH"){
        pos   <- which(colnames(data)==paste("HPP_ZSCH_437_CO2",  sep=""))
      }
      if(test_site=="ALBS"){
        pos   <- which(colnames(data)==paste("HPP_ALBS_429_CO2",  sep=""))
      }
      if(test_site=="BNTG_00"){
        pos       <- which(colnames(data)==paste("HPP_BNTG_431_CO2",  sep=""))
        test_site <- "BNTG"
      }
      if(test_site=="BNTG_01"){
        pos       <- which(colnames(data)==paste("HPP_BNTG_434_CO2",  sep=""))
        test_site <- "BNTG"
      }
      if(test_site=="FROB_00"){
        pos       <- which(colnames(data)==paste("HPP_FROB_435_CO2",  sep=""))
        test_site <- "FROB"
      }
      if(test_site=="FROB_01"){
        pos       <- which(colnames(data)==paste("HPP_FROB_439_CO2",  sep=""))
        test_site <- "FROB"
      }
      if(test_site=="CHRI"){
        pos   <- which(colnames(data)==paste("HPP_CHRI_433_CO2",  sep=""))
      }
      if(test_site=="TAEN_00"){
        pos       <- which(colnames(data)==paste("HPP_TAEN_436_CO2",  sep=""))
        test_site <- "TAEN"
      }
      if(test_site=="TAEN_01"){
        pos       <- which(colnames(data)==paste("HPP_TAEN_443_CO2",  sep=""))
        test_site <- "TAEN"
      }
      if(test_site=="SEMP_00"){
        pos       <- which(colnames(data)==paste("HPP_SEMP_444_CO2",  sep=""))
        test_site <- "SEMP"
      }
      if(test_site=="SEMP_01"){
        pos       <- which(colnames(data)==paste("HPP_SEMP_439_CO2",  sep=""))
        test_site <- "SEMP"
      }
      if(test_site=="SEMP_02"){
        pos       <- which(colnames(data)==paste("HPP_SEMP_427_CO2",  sep=""))
        test_site <- "SEMP"
      }
      if(test_site=="ESMO"){
        pos   <- which(colnames(data)==paste("HPP_ESMO_445_CO2",  sep=""))
      }
      if(test_site=="SAVE"){
        pos   <- which(colnames(data)==paste("HPP_SAVE_440_CO2",  sep=""))
      }
      if(test_site=="RECK"){
        pos   <- which(colnames(data)==paste("HPP_RECK_441_CO2",  sep=""))
      }
      if(test_site=="SSAL"){
        pos   <- which(colnames(data)==paste("HPP_SSAL_432_CO2",  sep=""))
      }
      if(test_site=="SOTT"){
        pos   <- which(colnames(data)==paste("HPP_SOTT_442_CO2",  sep=""))
      }
      
      #      
      
      pos_REF_SITES_rs <- which(REF_SITES$name==ref_site)
      pos_REF_SITES_ts <- which(REF_SITES$name==test_site)
      
      
      #
      
      dist_ts_rs <- sqrt((REF_SITES$x[pos_REF_SITES_rs]-REF_SITES$x[pos_REF_SITES_ts])^2 +(REF_SITES$y[pos_REF_SITES_rs]-REF_SITES$y[pos_REF_SITES_ts])^2)
      
      #
      
      if(mode==2){
        
        tmp             <- sqrt((tbl_location_LP8$Y_LV03-REF_SITES$y[pos_REF_SITES_ts])^2 + (tbl_location_LP8$X_LV03-REF_SITES$x[pos_REF_SITES_ts])^2)
        id_closest_site <- which(tmp==min(tmp,na.rm=T))[1]
        
        
        if(length(id_closest_site)>0){
          CLOSEST_SITE <- tbl_location_LP8$LocationName[id_closest_site]
          
          id_windy     <- which(anchor_events_SU_ALL$CO2_REF_SITE==ref_site & anchor_events_SU_ALL$LocationName==CLOSEST_SITE)
        }else{
          next
        }
        
      }
      
      if(mode==1){
        id_windy <- which(anchor_events_SU_ALL$CO2_REF_SITE==ref_site & anchor_events_SU_ALL$DIST2REF<=dist_ts_rs)
      }
      
      if(length(id_windy)==0){
        next
      }
      
      #
      
      ok_windy   <- rep(F,dim(data)[1])
      
      minTS      <- min(data$timestamp)
      index_from <- (anchor_events_SU_ALL$timestamp_from[id_windy]-minTS)/600+1
      index_to   <- (anchor_events_SU_ALL$timestamp_to[id_windy]  -minTS)/600+1
      
      for(i in 1:length(id_windy)){
        ok_windy[index_from[i]:index_to[i]] <- T
      }
      
      #
      
      if(mode==1){
        data_period <- rep(T,dim(data)[1]) 
      }
      if(mode==2){
        data_period <- data$timestamp>=min(anchor_events_SU_ALL$timestamp_from[id_windy]) & data$timestamp<=max(anchor_events_SU_ALL$timestamp_to[id_windy])
      }
      
      n_LP8_sites <- length(unique(anchor_events_SU_ALL$LocationName[id_windy]))
      
      id_00    <- which(data_period & !is.na(data[,ref_pos])&!is.na(data[,pos]))
      id_01    <- which(data_period & !is.na(data[,ref_pos])&!is.na(data[,pos]) & ok_windy & winddir_ok)
      
      n_id_00  <- length(id_00)
      n_id_01  <- length(id_01)
      
      if(n_id_00==0 | n_id_01==0){
        next
      }
      
      diff_CO2 <- matrix(NA,ncol=2,nrow=dim(data)[1])
      
      diff_CO2[1:n_id_00,1] <- data[id_00,pos] - data[id_00,ref_pos]
      diff_CO2[1:n_id_01,2] <- data[id_01,pos] - data[id_01,ref_pos]
      
      RMSE_COR    <- sqrt(sum(diff_CO2[,2]^2,na.rm=T)/n_id_01)
      SD_A        <- sd(diff_CO2[,1], na.rm=T)
      SD_W        <- sd(diff_CO2[,2], na.rm=T)
      MAD_A       <- mad(diff_CO2[,1],na.rm=T)
      MAD_W       <- mad(diff_CO2[,2],na.rm=T)
      
      DIFF_Q005_A <- quantile(diff_CO2[,1],probs=0.05,na.rm=T)
      DIFF_Q050_A <- quantile(diff_CO2[,1],probs=0.50,na.rm=T)
      DIFF_Q095_A <- quantile(diff_CO2[,1],probs=0.95,na.rm=T)
      DIFF_MEAN_A <- mean(diff_CO2[,1],na.rm=T)
      
      DIFF_Q005_W <- quantile(diff_CO2[,2],probs=0.05,na.rm=T)
      DIFF_Q050_W <- quantile(diff_CO2[,2],probs=0.50,na.rm=T)
      DIFF_Q095_W <- quantile(diff_CO2[,2],probs=0.95,na.rm=T)
      DIFF_MEAN_W <- mean(diff_CO2[,2],na.rm=T)
      
      ProofOfConcept_statistics <- rbind(ProofOfConcept_statistics,data.frame(TEST_SITE   = test_site,
                                                                              REF_SITE    = ref_site,
                                                                              mode        = mode,
                                                                              DIST_km     = dist_ts_rs / 1e3,
                                                                              n_LP8_sites = n_LP8_sites,
                                                                              N_ALL       = n_id_00,
                                                                              N_WIND      = n_id_01,
                                                                              DIFF_Q005_A = DIFF_Q005_A,
                                                                              DIFF_Q050_A = DIFF_Q050_A,
                                                                              DIFF_Q095_A = DIFF_Q095_A,
                                                                              DIFF_MEAN_A = DIFF_MEAN_A,
                                                                              DIFF_SD_A   = SD_A,
                                                                              DIFF_MAD_A  = MAD_A,
                                                                              DIFF_Q005_W = DIFF_Q005_W,
                                                                              DIFF_Q050_W = DIFF_Q050_W,
                                                                              DIFF_Q095_W = DIFF_Q095_W,
                                                                              DIFF_MEAN_W = DIFF_MEAN_W,
                                                                              DIFF_SD_W   = SD_W,
                                                                              DIFF_MAD_W  = MAD_W,
                                                                              RMSE_COR    = RMSE_COR,
                                                                              stringsAsFactors = F))
      
      
      xlabString <- paste("CO2 ",test_site," - CO2 ",ref_site," [ppm]")
      
      if(ref_site=="DUE" & test_site=="BRM"){
        xlabString <- expression(paste("CO"[2]*" BRM - CO"[2]*" DUE [ppm]"))
      }
      if(ref_site=="DUE" & test_site=="RIG"){
        xlabString <- expression(paste("CO"[2]*" RIG - CO"[2]*" DUE [ppm]"))
      }
      if(ref_site=="DUE" & test_site=="LAEG"){
        xlabString <- expression(paste("CO"[2]*" LAEG - CO"[2]*" DUE [ppm]"))
      }
      if(ref_site=="DUE" & test_site=="HAE"){
        xlabString <- expression(paste("CO"[2]*" HAE - CO"[2]*" DUE [ppm]"))
      }
      if(ref_site=="GIMM" & test_site=="PAY"){
        xlabString <- expression(paste("CO"[2]*" PAY - CO"[2]*" GIMM [ppm]"))
      }
      if(ref_site=="PAY" & test_site=="GIMM"){
        xlabString <- expression(paste("CO"[2]*" GIMM - CO"[2]*" PAY [ppm]"))
      }
      
      subStr     <- paste("Data:",strftime(min(data$date[id_00]),"%Y-%m-%d",tz="UTC"),"-",strftime(max(data$date[id_00]),"%Y-%m-%d",tz="UTC"))
      
      str_00     <- paste("N ALL: ",sprintf("%6.0f",n_id_00))
      str_01     <- paste("N WIND:",sprintf("%6.0f",n_id_01))
      str_02     <- paste("Q005 WP:",sprintf("%5.1f",quantile(diff_CO2[,2],probs=0.05,na.rm=T)))
      str_03     <- paste("Q050 WP:",sprintf("%5.1f",quantile(diff_CO2[,2],probs=0.50,na.rm=T)))
      str_04     <- paste("Q095 WP:",sprintf("%5.1f",quantile(diff_CO2[,2],probs=0.95,na.rm=T)))
      str_05     <- paste("MAD  WP:",sprintf("%5.1f",MAD_W))
      str_06     <- paste("RMSE WP:",sprintf("%5.1f",RMSE_COR))
      str_07     <- paste("DIST:   ",sprintf("%5.0f",dist_ts_rs/1000))
      str_08     <- paste("N LP8s: ",sprintf("%5.0f",n_LP8_sites))
      str_09     <- paste("SD   WP:",sprintf("%5.1f",SD_W))
      
      str_H1     <- paste("H1:     ",sprintf("%5.0f",REF_SITES$h[pos_REF_SITES_rs]))
      str_H2     <- paste("H2:     ",sprintf("%5.0f",REF_SITES$h[pos_REF_SITES_ts]))
      
      h1 <- hist(diff_CO2[,1],seq(-1e3-1,1e3+1,2),plot=F)
      h2 <- hist(diff_CO2[,2],seq(-1e3-1,1e3+1,2),plot=F)
      
      hist_ymax <- max(c(h1$counts,h2$counts))
      
      hist(diff_CO2[,1],seq(-1e3-1,1e3+1,2),xlim=c(-50,50),ylim=c(-0.1*hist_ymax,hist_ymax),col="gray70", main="",sub=subStr,xlab=xlabString,cex.lab=1.5,cex.axis=1.5,cex.sub=1.25)
      hist(diff_CO2[,2],seq(-1e3-1,1e3+1,2),xlim=c(-50,50),ylim=c(-0.1*hist_ymax,hist_ymax),col="gray30", main="",sub=subStr,xlab=xlabString,cex.lab=1.5,cex.axis=1.5,cex.sub=1.25,add=T)
      
      lines(c(-10,-10),c(-1e9,1e9),col=1,lty=5,lwd=2)
      lines(c( 10, 10),c(-1e9,1e9),col=1,lty=5,lwd=2)
      lines(c(  0,  0),c(-1e9,1e9),col=1,lty=5,lwd=2)
      
      lines(c(DIFF_Q050_W,DIFF_Q050_W), c(0,1e9),col=2,lty=1,lwd=2)
      
      boxplot(diff_CO2[,1],at=-1/3*0.1*hist_ymax,horizontal=T,ylim=c(-50,50),xlim=c(-0.1*hist_ymax,hist_ymax),col="gray70", boxwex=0.5*0.1*hist_ymax,add=T,xaxt="n",yaxt="n",outline = F)
      boxplot(diff_CO2[,2],at=-2/3*0.1*hist_ymax,horizontal=T,ylim=c(-50,50),xlim=c(-0.1*hist_ymax,hist_ymax),col="gray30", boxwex=0.5*0.1*hist_ymax,add=T,xaxt="n",yaxt="n",outline = F)
      
      par(family="mono")
      legend("topright",legend=c(str_07,str_H1,str_H2,"",str_00,str_01,str_02,str_03,str_04,str_05,str_09,str_06),bg="white",cex=1.5)
      par(family="")
      
      par(family="mono")
      legend("topleft",legend=c("All","Windy periods"),pch=15,col=c("gray70","gray30"),bg="white",cex=1.5)
      par(family="")
      
      
      if(T){
        if(mode==2){
          par(family="mono")
          legend("left",legend=c(CLOSEST_SITE),bg="white",cex=1.5)
          par(family="")
        }
      }
      
    }
  }
  
  dev.off()
  par(def_par)
  
  
}

rm(winddir_ok)
gc()


##

write.table(x = ProofOfConcept_statistics,file = paste(resultdir,"/ProofOfConcept_statistics.csv",sep=""),col.names = T,row.names = F,sep=";")

### ----------------------------------------------------------------------------------------------------------------------------

adjustment_statistics <- NULL

for(cor_mode in c("W","T","WT")){
  
  print(cor_mode)
  
  if(cor_mode=="W"){
    cor_mode_ok <- anchor_events_SU_ALL$WIND_SITUATION == T 
  }
  if(cor_mode=="T"){
    cor_mode_ok <- anchor_events_SU_ALL$MIXED_ATMOS_SITUATION == T 
  }
  if(cor_mode=="WT"){
    cor_mode_ok <- anchor_events_SU_ALL$WIND_SITUATION == T | anchor_events_SU_ALL$MIXED_ATMOS_SITUATION == T
  }
  
  for(use_mode in c("POTENTIAL","USED")){
    
    print(use_mode)
    
    if(use_mode=="POTENTIAL"){
      use_mode_ok <- rep(T,dim(anchor_events_SU_ALL)[1])
    }
    if(use_mode=="USED"){
      use_mode_ok <- !is.na(anchor_events_SU_ALL$SU_CO2_corr)
    }
    
    
    
    for(ith_depl in 1:dim(tbl_deployment)[1]){
      
      if(!tbl_deployment$SensorUnit_ID[ith_depl]%in%c(1010:1334)){
        next
      }
      if(tbl_deployment$LocationName[ith_depl]%in%c("DUE1","DUE2","DUE3","DUE4","DUE5","MET1")){
        next
      }
      if(tbl_deployment$Date_UTC_from[ith_depl]<strptime("19700101000000","%Y%m%d%H%M%S", tz="UTC")){
        next
      }
      
      id_loc <- which(tbl_location$LocationName == tbl_deployment$LocationName[ith_depl])
      
      DurationOfDeployment <- tbl_deployment$timestamp_to[ith_depl] - tbl_deployment$timestamp_from[ith_depl]
      
      
      id   <- which(anchor_events_SU_ALL$SensorUnit_ID   == tbl_deployment$SensorUnit_ID[ith_depl]
                    & anchor_events_SU_ALL$LocationName  == tbl_deployment$LocationName[ith_depl]
                    & anchor_events_SU_ALL$Date_UTC_from >= tbl_deployment$Date_UTC_from[ith_depl]
                    & anchor_events_SU_ALL$Date_UTC_to   <= tbl_deployment$Date_UTC_to[ith_depl]
                    & cor_mode_ok
                    & use_mode_ok)
      
      n_id <- length(id)
      
      #
      
      max_dT       <- NA
      indep_events <- NA
      
      if(n_id>0){
        indep_events <- rep(F,n_id)
        
        for(ith_event in 1:n_id){
          if(ith_event==1){
            indep_events[ith_event] <- T
          }else{
            if((anchor_events_SU_ALL$timestamp_from[id[ith_event]] - anchor_events_SU_ALL$timestamp_to[id[ith_event-1]]) > 72000){
              indep_events[ith_event] <- T
            }
          }
        }
        
        #
        
        if(n_id>1){
          max_dT <- max(as.numeric(difftime(time1=anchor_events_SU_ALL$Date_UTC_from[id[2:n_id]],
                                            time2=anchor_events_SU_ALL$Date_UTC_to[id[1:(n_id-1)]],units="secs",tz="UTC")))
        }
      }
      
      
      
      
      
      #
      
      adjustment_statistics <- rbind(adjustment_statistics,
                                     data.frame(LocationName         = tbl_deployment$LocationName[ith_depl],
                                                SensorUnit_ID        = tbl_deployment$SensorUnit_ID[ith_depl],
                                                Date_UTC_from        = strftime(tbl_deployment$Date_UTC_from[ith_depl],"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                Date_UTC_to          = strftime(tbl_deployment$Date_UTC_to[ith_depl],  "%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                DurationOfDeployment = DurationOfDeployment,
                                                Canton               = tbl_location$Canton[id_loc],
                                                Altitude             = tbl_location$h[id_loc],
                                                X_LV03               = tbl_location$X_LV03[id_loc],
                                                Y_LV03               = tbl_location$Y_LV03[id_loc],
                                                N_events             = n_id,
                                                N_indep_events       = sum(indep_events),
                                                Indep_events_per_month = sum(indep_events)/(DurationOfDeployment/(30*86400)),
                                                max_dT_days          = max_dT/86400,
                                                cor_mode             = cor_mode,
                                                use_mode             = use_mode,
                                                stringsAsFactors     = F))
      
    }
    
    #
    
  }
}

#

for(cor_mode in c("W","T","WT")){
  
  for(use_mode in c("POTENTIAL","USED")){
    
    id   <- which(adjustment_statistics$cor_mode == cor_mode & adjustment_statistics$use_mode == use_mode)
    n_id <- length(id)
    
    if(n_id==0){
      next
    }
    
    #
    
    figname <- paste(resultdir,"/MAP_MONTHLY_FREQUENCY_ADJUSTMENT_COR_MODE_",sprintf("%s",cor_mode),"_",use_mode,".pdf",sep="")
    
    def_par <- par()
    pdf(file = figname, width=10, height=10, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1,1,0.5,0.1),mfrow=c(1,1))
    
    colP <- ((adjustment_statistics$Indep_events_per_month[id] %/% 2)+1)
    colP[which(colP>7)] <- 7
    colP <- rev(rainbow(7))[colP]
    
    cex <- rep(1.25,n_id)
    cex[adjustment_statistics$DurationOfDeployment[id]<(365*86400)] <- 0.75
    
    plot(adjustment_statistics$Y_LV03[id]/1000,adjustment_statistics$X_LV03[id]/1000,col=colP,pch=16,cex=cex,
         xlab="Easting [km]",ylab="Northing [km]",cex.axis=1.5,cex.lab=1.5)
    
    par(family="mono")
    legend("topleft",legend=c("1/m","2/m","3/m","4/m","5/m","6/m","7/m"),col=rev(rainbow(7)),pch=16,cex=1,bg="white")
    par(family="")
    
    dev.off()
    par(def_par)
    
    #
    
    figname <- paste(resultdir,"/HIST_MONTHLY_FREQUENCY_ADJUSTMENT_COR_MODE_",sprintf("%s",cor_mode),"_",use_mode,".pdf",sep="")
    
    def_par <- par()
    pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1,1,0.5,0.1),mfrow=c(1,1))
    
    leg_str_00 <- paste("N depl: ",sprintf("%4.0f",n_id))
    leg_str_01 <- paste("Min:    ",sprintf("%4.1f",min(adjustment_statistics$Events_per_month[id],na.rm=T)))
    leg_str_02 <- paste("Max:    ",sprintf("%4.1f",max(adjustment_statistics$Events_per_month[id],na.rm=T)))
    
    maxIEPM <- ceiling(max(adjustment_statistics$Indep_events_per_month[id],na.rm=T))
    hist(adjustment_statistics$Indep_events_per_month[id],seq(0,maxIEPM,1),xlim=c(0,15),col="slategray",
         xlab="Independent events per month",main="",cex.axis=1.5,cex.lab=1.5)
    
    par(family="mono")
    legend("topright",legend=c(leg_str_00,leg_str_01,leg_str_02),cex=1,bg="white")
    par(family="")
    
    dev.off()
    par(def_par)
    
    #
    
    figname <- paste(resultdir,"/HIST_MAX_TIME_BETWEEN_ADJUSTMENTS_COR_MODE_",sprintf("%s",cor_mode),"_",use_mode,".pdf",sep="")
    
    def_par <- par()
    pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1,1,0.5,0.1),mfrow=c(1,1))
    
    leg_str_00 <- paste("N depl: ",sprintf("%4.0f",n_id))
    leg_str_01 <- paste("Min:    ",sprintf("%4.1f",min(adjustment_statistics$max_dT_days[id],na.rm=T)))
    leg_str_02 <- paste("Max:    ",sprintf("%4.1f",max(adjustment_statistics$max_dT_days[id],na.rm=T)))
    
    maxIEPM <- ceiling(max(adjustment_statistics$max_dT_days[id],na.rm=T))
    hist(adjustment_statistics$max_dT_days[id],seq(0,maxIEPM+3.5,3.5),xlim=c(0,90),col="slategray",
         xlab="Maximum duration between adjustments [days]",main="",cex.axis=1.5,cex.lab=1.5)
    
    par(family="mono")
    legend("topright",legend=c(leg_str_00,leg_str_01,leg_str_02),cex=1,bg="white")
    par(family="")
    
    dev.off()
    par(def_par)
    
  }
}

#

write.table(x = adjustment_statistics,file = paste(resultdir,"/adjustment_statistics.csv",sep=""),col.names = T,row.names = F,sep=";")

### ----------------------------------------------------------------------------------------------------------------------------


SU_CORRECTIONS_LARGE <- NULL

figname <- paste(resultdir,"/SU_CORRECTIONS.pdf",sep="")


def_par <- par()
pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
par(mai=c(1,1,0.5,0.1),mfrow=c(1,1))

u_SU_LOC <- unique(data.frame(SensorUnit_ID= anchor_events_SU_ALL$SensorUnit_ID,
                              LocationName = anchor_events_SU_ALL$LocationName,
                              stringsAsFactors = F))

for(ith_u_SU_LOC in 1:dim(u_SU_LOC)[1]){
  
  id <- which(anchor_events_SU_ALL$LocationName    == u_SU_LOC$LocationName[ith_u_SU_LOC]
              & anchor_events_SU_ALL$SensorUnit_ID == u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC]
              & !is.na(anchor_events_SU_ALL$SU_CO2_corr)
              & anchor_events_SU_ALL$WIND_SITUATION==T)
  
  n_id <- length(id)
  
  if(n_id>0){
    
    if(any(abs(anchor_events_SU_ALL$SU_CO2_corr[id])>200)){
      SU_CORRECTIONS_LARGE <- rbind(SU_CORRECTIONS_LARGE,data.frame(LocationName     = u_SU_LOC$LocationName[ith_u_SU_LOC],
                                                                    SensorUnit_ID    = u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC],
                                                                    MIN_SU_CO2_corr  = min(anchor_events_SU_ALL$SU_CO2_corr[id]),
                                                                    MAX_SU_CO2_corr  = max(anchor_events_SU_ALL$SU_CO2_corr[id]),
                                                                    LAST_SU_CO2_corr = anchor_events_SU_ALL$SU_CO2_corr[id[n_id]],
                                                                    LAST_DATE_UTC    = strftime(anchor_events_SU_ALL$Date_UTC_mid[id[n_id]],"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                                    stringsAsFactors = F))
    }
    
    #
    
    min_corr <- min(anchor_events_SU_ALL$SU_CO2_corr[id])
    max_corr <- max(anchor_events_SU_ALL$SU_CO2_corr[id])
    
    mainString <- paste(u_SU_LOC$LocationName[ith_u_SU_LOC],"/",u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC])
    ylabString <- "Correction [ppm]"
    xlabString <- "Date"
    
    plot(anchor_events_SU_ALL$Date_UTC_mid[id],rep(NA,length(id)),ylim=c(-500,500),lty=5,lwd=1,col=1,cex.lab=1.25,cex.axis=1.25,xlab=xlabString,ylab=ylabString,main=mainString)
    
    for(ith_id in 1:n_id){
      polygon(x = c(anchor_events_SU_ALL$Date_UTC_from[id[ith_id]],
                    anchor_events_SU_ALL$Date_UTC_to[id[ith_id]],
                    anchor_events_SU_ALL$Date_UTC_to[id[ith_id]],
                    anchor_events_SU_ALL$Date_UTC_from[id[ith_id]]),
              y = c(-1e4,-1e4,1e4,1e4),
              col="orange",
              border=NA)
    }
    
    for(ll in seq(-500,500,100)){
      lines(anchor_events_SU_ALL$Date_UTC_mid[id],rep(ll,length(id)),lwd=1,lty=5,col=1)
    }
    
    lines( anchor_events_SU_ALL$Date_UTC_mid[id],anchor_events_SU_ALL$SU_CO2_corr[id],lwd=2,col=2)  
    points(anchor_events_SU_ALL$Date_UTC_mid[id],anchor_events_SU_ALL$SU_CO2_corr[id],cex=0.25,col=2,pch=16)
  }
}

dev.off()
par(def_par)

#

write.table(x = SU_CORRECTIONS_LARGE,file = paste(resultdir,"/SU_CORRECTIONS_LARGE.csv",sep=""),col.names = T,row.names = F,sep=";")


### ----------------------------------------------------------------------------------------------------------------------------








