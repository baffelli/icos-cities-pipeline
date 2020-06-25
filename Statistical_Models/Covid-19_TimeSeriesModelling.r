# CO2_Covid-19.r
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
library(rpart)
library(randomForest)
library(maptools)
library(suncalc)
library(ncdf4)

source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")



### ----------------------------------------------------------------------------------------------------------------------------

resultdir  <- "/project/CarboSense/Statistical_Models/Covid-19_ZH/Results/CO2/"

if(!dir.exists(resultdir)){
  dir.create(resultdir)
}

### ----------------------------------------------------------------------------------------------------------------------------

## Import table "Locations"

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_loc         <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

### ----------------------------------------------------------------------------------------------------------------------------

## Import of HPP measurements [1 min data --> 1 hour]

data <- NULL

for(ZH_SITE in c("ZSCH","ZHBR","ZUE","RECK","ALBS","ESMO")){
  
  query_str       <- paste("SELECT timestamp, CO2_CAL_ADJ FROM CarboSense_HPP_CO2 ",sep="")
  query_str       <- paste(query_str, "WHERE LocationName = '",ZH_SITE,"' and Valve = 0 and CO2_CAL_ADJ != -999;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tmp             <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  colnames(tmp) <- c("timestamp", paste(ZH_SITE,"_CO2",sep=""))
  
  if(is.null(data)){
    data <- tmp
  }else{
    data <- merge(data,tmp,all=T,by="timestamp")
  }
}

colnames(data)[which(colnames(data)=="timestamp")] <- "date"

data$date      <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$date

data           <- timeAverage(mydata = data,avg.time = "1 hour",statistic = "mean",data.thresh = 50,
                              start.date = strptime("20180801000000","%Y%m%d%H%M%S",tz="UTC"))
data           <- as.data.frame(data)


## Import of LP8 measurements [10 min data --> 1 hour]

data_LP8 <- NULL

for(ZH_SITE in c("ZMAN","ZSBS","ZBLG","ZUE","ZDLT","ZSCH","ZBAD","ZGLA","LUG","LUGN","ZHRG","ZGHD","ZPRD")){
  
  query_str       <- paste("SELECT SensorUnit_ID, LocationName, Date_UTC_from, Date_UTC_to FROM Deployment ",sep="")
  query_str       <- paste(query_str, "WHERE LocationName = '",ZH_SITE,"' AND SensorUnit_ID >= 1010 and SensorUnit_ID <= 1334;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tbl_depl        <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  if(ZH_SITE=="ZHRG"){
    tbl_depl <- tbl_depl[which(tbl_depl$SensorUnit_ID==1128),]
  }
  if(ZH_SITE=="ZGHD"){
    tbl_depl <- tbl_depl[which(tbl_depl$SensorUnit_ID==1032),]
  }
  
  tbl_depl$Date_UTC_from <- strptime(tbl_depl$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_depl$Date_UTC_to   <- strptime(tbl_depl$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
  tbl_depl$timestamp_from <- as.numeric(difftime(time1=tbl_depl$Date_UTC_from,time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
  tbl_depl$timestamp_to   <- as.numeric(difftime(time1=tbl_depl$Date_UTC_to,  time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
  
  query_str       <- paste("SELECT timestamp, CO2_A FROM CarboSense_CO2_TEST01 ",sep="")
  query_str       <- paste(query_str, "WHERE LocationName = '",ZH_SITE,"' AND CO2_A != -999 AND O_FLAG = 1 ",sep="")
  query_str       <- paste(query_str, "AND SensorUnit_ID=",tbl_depl$SensorUnit_ID," ",sep="")
  query_str       <- paste(query_str, "AND timestamp>=",tbl_depl$timestamp_from," AND timestamp<",tbl_depl$timestamp_to,";",sep="")
  
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tmp             <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  colnames(tmp) <- c("timestamp", paste(ZH_SITE,"_LP8_CO2",sep=""))
  
  if(is.null(data_LP8)){
    data_LP8 <- tmp
  }else{
    data_LP8 <- merge(data_LP8,tmp,all=T,by="timestamp")
  }
}

colnames(data_LP8)[which(colnames(data_LP8)=="timestamp")] <- "date"

data_LP8$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_LP8$date

data_LP8      <- timeAverage(mydata = data_LP8,avg.time = "1 hour",statistic = "mean",
                             start.date = strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC"))
data_LP8      <- as.data.frame(data_LP8)


## Import of NABEL CO2 measurements [1 min data --> 1 hour]

data_NABEL <- NULL

for(SITE in c("PAY","HAE","RIG","DUE","LAEG")){
  
  if(SITE == "PAY"){
    table <- "NABEL_PAY"
  }
  if(SITE == "DUE"){
    table <- "NABEL_DUE"
  }
  if(SITE == "RIG"){
    table <- "NABEL_RIG"
  }
  if(SITE == "HAE"){
    table <- "NABEL_HAE"
  }
  if(SITE == "LAEG"){
    table <- "EMPA_LAEG"
  }
  
  if(SITE%in%c("PAY","DUE","RIG","HAE")){
    query_str       <- paste("SELECT timestamp, CO2_WET_COMP FROM ",table," ",sep="")
    query_str       <- paste(query_str, "WHERE CO2_WET_COMP != -999 AND timestamp > 1483228800;",sep="")
  }
  if(SITE%in%c("LAEG")){
    query_str       <- paste("SELECT timestamp, CO2 FROM ",table," ",sep="")
    query_str       <- paste(query_str, "WHERE CO2 != -999 AND timestamp > 1483228800;",sep="")
  }
  
  
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tmp             <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  if(SITE%in%c("PAY","DUE","RIG","HAE")){
    colnames(tmp) <- c("timestamp", paste(SITE,"_NABEL_CO2",sep=""))
  }
  if(SITE%in%c("LAEG")){
    colnames(tmp) <- c("timestamp", paste(SITE,"_EMPA_CO2",sep=""))
  }
  
  # Exclude data while Picarro was at METAS
  if(SITE=="DUE"){
    id <- which(tmp$timestamp>=1495065600 & tmp$timestamp<=1495670400)
    if(length(id)>0){
      tmp[id,2] <- NA
    }
    rm(id)
    gc()
  }
  
  if(is.null(data_NABEL)){
    data_NABEL <- tmp
  }else{
    data_NABEL <- merge(data_NABEL,tmp,all=T,by="timestamp")
  }
}

colnames(data_NABEL)[which(colnames(data_NABEL)=="timestamp")] <- "date"

data_NABEL$date      <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_NABEL$date

data_NABEL           <- timeAverage(mydata = data_NABEL,avg.time = "1 hour",statistic = "mean",data.thresh = 50,
                                    start.date = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"))
data_NABEL           <- as.data.frame(data_NABEL)


## Import of NABEL NO, NO2 measurements [1 min data --> 1 hour]

data_NABEL_Poll <- NULL

for(SITE in c("HAE","DUE","PAY")){
  
  
  if(SITE == "HAE"){
    table <- "NABEL_HAE"
  }
  if(SITE == "DUE"){
    table <- "NABEL_DUE"
  }
  if(SITE == "PAY"){
    table <- "NABEL_PAY"
  }
  
  if(SITE%in%c("HAE","DUE","PAY")){
    query_str       <- paste("SELECT timestamp, NO, NO2 FROM ",table," ",sep="")
    query_str       <- paste(query_str, "WHERE NO != -999 AND NO2 != -999 AND timestamp > 1483228800;",sep="")
  }
  
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tmp             <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  if(SITE%in%c("HAE","DUE","PAY")){
    tmp$NewCol    <- tmp[,2] + tmp[,3]
    colnames(tmp) <- c("timestamp", paste(SITE,"_NABEL_NO",sep=""),
                       paste(SITE,"_NABEL_NO2",sep=""),paste(SITE,"_NABEL_NOX",sep=""))
  }
  
  if(is.null(data_NABEL_Poll)){
    data_NABEL_Poll <- tmp
  }else{
    data_NABEL_Poll <- merge(data_NABEL_Poll,tmp,all=T,by="timestamp")
  }
}

colnames(data_NABEL_Poll)[which(colnames(data_NABEL_Poll)=="timestamp")] <- "date"

data_NABEL_Poll$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_NABEL_Poll$date

data_NABEL_Poll      <- timeAverage(mydata = data_NABEL_Poll,avg.time = "1 hour",statistic = "mean",data.thresh = 50,
                                    start.date = strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"))
data_NABEL_Poll      <- as.data.frame(data_NABEL_Poll)



## Import of MeteoSwiss temperature, wind speed and pressure measurements  [10 min data --> 1 hour]

METEO_SITES <- c("KLO","NABZUE","NABHAE","NABRIG","PAY","NABDUE","CHZ","CHM","LAE","REH","SMA","KOP","GRA","LUG","GEN")

data_METEO <- NULL

for(METEO_SITE in METEO_SITES){
  
  if(!METEO_SITE%in%c("LAE","KLO","LUG")){
    query_str <- paste("SELECT timestamp, Temperature, Windspeed, Winddirection, RH FROM METEOSWISS_Measurements ",sep="")
    query_str <- paste(query_str,"WHERE LocationName = '",METEO_SITE,"' and timestamp >= 1483228800 ",sep="")
    query_str <- paste(query_str,"AND Temperature != -999 AND Windspeed != -999 AND Winddirection!=0 AND Winddirection != -999 AND RH != -999;",sep="")
  }
  if(METEO_SITE=="LAE"){
    query_str <- paste("SELECT timestamp, Temperature, RH FROM METEOSWISS_Measurements ",sep="")
    query_str <- paste(query_str,"WHERE LocationName = '",METEO_SITE,"' and timestamp >= 1483228800 AND Temperature != -999 AND RH != -999;",sep="")
  }
  if(METEO_SITE%in%c("KLO","LUG")){
    query_str <- paste("SELECT timestamp, Temperature, Windspeed, Winddirection,Pressure, RH FROM METEOSWISS_Measurements ",sep="")
    query_str <- paste(query_str,"WHERE LocationName = '",METEO_SITE,"' and timestamp >= 1483228800 AND Temperature != -999 ",sep="")
    query_str <- paste(query_str,"AND Windspeed != -999 AND Winddirection != 0 AND Winddirection != -999 AND Pressure != -999 AND RH != -999;",sep="")
  }
  drv       <- dbDriver("MySQL")
  con       <- dbConnect(drv, group="CarboSense_MySQL")
  res       <- dbSendQuery(con, query_str)
  tmp       <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  if(METEO_SITE=="LAE"){
    colnames(tmp) <- c("timestamp",
                       paste(METEO_SITE,"_T",sep=""),
                       paste(METEO_SITE,"_RH",sep=""))
  }
  if(METEO_SITE%in%c("KLO","LUG")){
    colnames(tmp) <- c("timestamp",
                       paste(METEO_SITE,"_T",sep=""),
                       paste(METEO_SITE,"_WS",sep=""),
                       paste(METEO_SITE,"_WD",sep=""),
                       paste(METEO_SITE,"_P",sep=""),
                       paste(METEO_SITE,"_RH",sep=""))
  }
  if(!METEO_SITE%in%c("LAE","KLO","LUG")){
    colnames(tmp) <- c("timestamp",
                       paste(METEO_SITE,"_T",sep=""),
                       paste(METEO_SITE,"_WS",sep=""),
                       paste(METEO_SITE,"_WD",sep=""),
                       paste(METEO_SITE,"_RH",sep=""))
  }
  
  if(is.null(data_METEO)){
    data_METEO <- tmp
  }else{
    data_METEO <- merge(data_METEO,tmp,all=T,by="timestamp")
  }
}

colnames(data_METEO)[which(colnames(data_METEO)=="timestamp")] <- "date"

data_METEO$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_METEO$date

data_METEO <- timeAverage(mydata = data_METEO,avg.time = "1 hour",statistic = "mean",start.date = strptime("20180801000000","%Y%m%d%H%M%S",tz="UTC")-3600)
data_METEO <- as.data.frame(data_METEO)

#

data_METEO$dT_NABZUE_KLO    <- data_METEO$NABZUE_T - data_METEO$KLO_T
data_METEO$dT_NABZUE_LAE    <- data_METEO$NABZUE_T - data_METEO$LAE_T
data_METEO$dT_REH_KLO       <- data_METEO$REH_T    - data_METEO$KLO_T
data_METEO$dT_REH_LAE       <- data_METEO$REH_T    - data_METEO$LAE_T
data_METEO$dT_KLO_LAE       <- data_METEO$KLO_T    - data_METEO$LAE_T

data_METEO$dT_NABDUE_KLO    <- data_METEO$NABDUE_T - data_METEO$KLO_T
data_METEO$dT_NABDUE_LAE    <- data_METEO$NABDUE_T - data_METEO$LAE_T
data_METEO$dT_NABRIG_CHZ    <- data_METEO$NABRIG_T - data_METEO$CHZ_T
data_METEO$dT_NABRIG_NABHAE <- data_METEO$NABRIG_T - data_METEO$NABHAE_T
data_METEO$dT_PAY_CHM       <- data_METEO$PAY_T    - data_METEO$CHM_T
data_METEO$dT_PAY_GRA       <- data_METEO$PAY_T    - data_METEO$GRA_T
data_METEO$dT_NABHAE_KOP    <- data_METEO$NABHAE_T - data_METEO$KOP_T

data_METEO$dT_LUG_GEN       <- data_METEO$LUG_T    - data_METEO$GEN_T

### ----------------------------------------------------------------------------------------------------------------------------

## Import of MeteoSwiss temperature and radiance measurements from KLO/LUG  [10 min data --> seasonal]

query_str <- paste("SELECT timestamp, Temperature, Radiance FROM METEOSWISS_Measurements ", sep="")
query_str <- paste(query_str, "WHERE LocationName = 'KLO' and timestamp >= 1483228800 ",sep="")
query_str <- paste(query_str, "AND Radiance != -999 AND Temperature != -999;",sep="")
drv       <- dbDriver("MySQL")
con       <- dbConnect(drv, group="CarboSense_MySQL")
res       <- dbSendQuery(con, query_str)
METEO_DAY <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

colnames(METEO_DAY)[which(colnames(METEO_DAY)=="timestamp")] <- "date"

METEO_DAY$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + METEO_DAY$date

METEO_DAY <- timeAverage(mydata = METEO_DAY,avg.time = "day",statistic = "mean",start.date = strptime("20161231000000","%Y%m%d%H%M%S",tz="UTC")-3600)
METEO_DAY <- as.data.frame(METEO_DAY)

METEO_DAY$timestamp <- as.numeric(difftime(time1=METEO_DAY$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

METEO_DAY$Temperature_SEASON <- NA
METEO_DAY$Radiance_SEASON    <- NA

for(ith_day in 1:dim(METEO_DAY)[1]){
  
  t1 <- METEO_DAY$timestamp[ith_day] - 15 * 86400
  t2 <- METEO_DAY$timestamp[ith_day]
  
  id <- which(METEO_DAY$timestamp>=t1 & METEO_DAY$timestamp<t2)
  
  if(length(id)>0){
    METEO_DAY$Temperature_SEASON[ith_day] <- mean(METEO_DAY$Temperature[id],na.rm=T)
    METEO_DAY$Radiance_SEASON[ith_day]    <- mean(METEO_DAY$Radiance[id],na.rm=T)
  }
}

#

figname <- paste(resultdir,"T_KLO_TS.pdf",sep="")

def_par <- par()
pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
par(mai=c(1,1,0.75,0.1),mfrow=c(1,1))

xlabStr <- "Date"
ylabStr <- expression(paste("T [deg C]"))

lab_dates <- seq(strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),
                 strptime("20210101000000","%Y%m%d%H%M%S",tz="UTC"),by="month")

lab_dates_str <- strftime(lab_dates, "%m/%y",tz="UTC")


plot(c(min(data$date),max(data$date)),c(NA,NA),ylim=c(-10,40),
     t="l",main="",xlab=xlabStr, ylab=ylabStr,
     cex.main=1.5,cex.lab=1.5,cex.axis=1.5,xaxt="n")


pos <- which(colnames(data_METEO)=="KLO_T")
lines(data_METEO$date, data_METEO[,pos], col=1, lwd=0.5)

lines(METEO_DAY$date, METEO_DAY$Temperature_SEASON, col=2, lwd=2)

axis(side = 1,at = lab_dates,labels = lab_dates_str,cex.lab=1.5,cex.axis=1.5)

par(family="mono")
legend("topright",legend=c("MEAS","SEAS"),col=c(1,2),lwd=1,lty=1,bg="white")
par(family="")


dev.off()
par(def_par)



#

query_str <- paste("SELECT timestamp, Temperature, Radiance FROM METEOSWISS_Measurements ", sep="")
query_str <- paste(query_str, "WHERE LocationName = 'LUG' and timestamp >= 1483228800 ",sep="")
query_str <- paste(query_str, "AND Radiance != -999 AND Temperature != -999;",sep="")
drv       <- dbDriver("MySQL")
con       <- dbConnect(drv, group="CarboSense_MySQL")
res       <- dbSendQuery(con, query_str)
METEO_DAY_TI <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

colnames(METEO_DAY_TI)[which(colnames(METEO_DAY_TI)=="timestamp")] <- "date"

METEO_DAY_TI$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + METEO_DAY_TI$date

METEO_DAY_TI <- timeAverage(mydata = METEO_DAY_TI,avg.time = "day",statistic = "mean",start.date = strptime("20161231000000","%Y%m%d%H%M%S",tz="UTC")-3600)
METEO_DAY_TI <- as.data.frame(METEO_DAY_TI)

METEO_DAY_TI$timestamp <- as.numeric(difftime(time1=METEO_DAY_TI$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

METEO_DAY_TI$Temperature_SEASON <- NA
METEO_DAY_TI$Radiance_SEASON    <- NA

for(ith_day in 1:dim(METEO_DAY)[1]){
  
  t1 <- METEO_DAY_TI$timestamp[ith_day] - 15 * 86400
  t2 <- METEO_DAY_TI$timestamp[ith_day]
  
  id <- which(METEO_DAY_TI$timestamp>=t1 & METEO_DAY_TI$timestamp<t2)
  
  if(length(id)>0){
    METEO_DAY_TI$Temperature_SEASON[ith_day] <- mean(METEO_DAY_TI$Temperature[id],na.rm=T)
    METEO_DAY_TI$Radiance_SEASON[ith_day]    <- mean(METEO_DAY_TI$Radiance[id],na.rm=T)
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

# VPRM

ncdf_vprm        <- nc_open("/project/jae/zurich_tree/VPRM/VPRM_ZH_20180101-20200329.nc")
ncdf_vprm_lat    <- ncvar_get(ncdf_vprm, "lat")
ncdf_vprm_lon    <- ncvar_get(ncdf_vprm, "lon")
ncdf_vprm_time   <- ncvar_get(ncdf_vprm, "time")

n_ncdf_vprm_lat  <- length(ncdf_vprm_lat)
n_ncdf_vprm_lon  <- length(ncdf_vprm_lon)
n_ncdf_vprm_time <- length(ncdf_vprm_time)

sites   <- c("LAE","ALBS","ESMO","RECK","DUE","ZUE")
n_sites <- length(sites)

data_VPRN <- NULL

for(ith_site in 1:n_sites){
  
  if(sites[ith_site]=="DUE"){
    id_loc <- which(tbl_loc$LocationName == "DUE1")
  }else{
    id_loc <- which(tbl_loc$LocationName == sites[ith_site])
  }
  
  id_lat <- which(abs(tbl_loc$LAT_WGS84[id_loc]-ncdf_vprm_lat) == min(abs(tbl_loc$LAT_WGS84[id_loc]-ncdf_vprm_lat)))[1]
  id_lon <- which(abs(tbl_loc$LON_WGS84[id_loc]-ncdf_vprm_lon) == min(abs(tbl_loc$LON_WGS84[id_loc]-ncdf_vprm_lon)))[1]
  
  RA  <- ncvar_get(nc = ncdf_vprm, varid = "RA", start = c(id_lon,id_lat,1),count = c(1,1,-1))
  GPP <- ncvar_get(nc = ncdf_vprm, varid = "GPP",start = c(id_lon,id_lat,1),count = c(1,1,-1))
  
  tmp <- data.frame(time = ncdf_vprm_time,
                    RA   = RA,
                    GPP  = GPP,
                    stringsAsFactors = F)
  
  colnames(tmp) <- c("time",paste(sites[ith_site],"_RA",sep=""),paste(sites[ith_site],"_GPP",sep=""))
  
  if(is.null(data_VPRN)){
    data_VPRN <- tmp
  }else{
    data_VPRN <- merge(data_VPRN,tmp,by="time",all=T)
  }
}

data_VPRN$date <- strptime("20180101000000","%Y%m%d%H%M%S",tz="UTC") + data_VPRN$time*3600

data_VPRN      <- data_VPRN[,which(colnames(data_VPRN)!="time")]

nc_close(ncdf_vprm)

#

for(subject in c("GPP","RA")){
  
  figname <- paste(resultdir,subject,"_TS.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.75,0.1),mfrow=c(1,1))
  
  pos     <- grep(pattern = paste("_",subject,sep=""),x = colnames(data_VPRN))
  n_pos   <- length(pos)
  
  min_y   <- min(data_VPRN[,pos])
  max_y   <- max(data_VPRN[,pos])
  
  xlabStr <- "date"
  ylabStr <- paste(subject,"[umol m-2 s-1]")
  
  lab_dates <- seq(strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),
                   strptime("20210101000000","%Y%m%d%H%M%S",tz="UTC"),by="3 months")
  
  lab_dates_str <- strftime(lab_dates, "%m/%y",tz="UTC")
  
  plot(c(min(data_VPRN$date),max(data_VPRN$date)),c(NA,NA),ylim=c(min_y,max_y),xlab=xlabStr, ylab=ylabStr,main="",xaxt="n",
       cex.axis=1.5,cex.lab=1.5)
  axis(side = 1,at = lab_dates,labels = lab_dates_str,cex.lab=1.5,cex.axis=1.5)
  
  for(ii in 1:n_pos){
    lines(data_VPRN$date,data_VPRN[,pos[ii]],col=ii,lty=1,lwd=1)
  }
  
  par(family="mono")
  legend("topright",legend=colnames(data_VPRN)[pos],col=1:n_pos,bg="white",lty=1)
  par(family="")
  
  dev.off()
  par(def_par)
}



# Antropogenic emissions

ncdf_ae        <- nc_open("/project/jae/zurich_tree/VPRM/co2_zh_2015.nc")

ncdf_ae_lat    <- ncvar_get(ncdf_ae, "lat")
ncdf_ae_lon    <- ncvar_get(ncdf_ae, "lon")
ncdf_ae_time   <- ncvar_get(ncdf_ae, "time")

n_ncdf_ae_lat  <- length(ncdf_ae_lat)
n_ncdf_ae_lon  <- length(ncdf_ae_lon)
n_ncdf_ae_time <- length(ncdf_ae_time)

sites   <- c("RECK","ESMO","ZUE","ALBS","LAE","DUE")
n_sites <- length(sites)

data_ae <- NULL

for(ith_site in 1:n_sites){
  
  if(sites[ith_site]=="DUE"){
    id_loc <- which(tbl_loc$LocationName == "DUE1")
  }else{
    id_loc <- which(tbl_loc$LocationName == sites[ith_site])
  }
  
  id_lat <- which(abs(tbl_loc$LAT_WGS84[id_loc]-ncdf_ae_lat) == min(abs(tbl_loc$LAT_WGS84[id_loc]-ncdf_ae_lat)))[1]
  id_lon <- which(abs(tbl_loc$LON_WGS84[id_loc]-ncdf_ae_lon) == min(abs(tbl_loc$LON_WGS84[id_loc]-ncdf_ae_lon)))[1]
  
  AE  <- ncvar_get(nc = ncdf_ae, varid = "CO2_EMIS", start = c(id_lon,id_lat,1),count = c(1,1,-1))
  
  tmp <- data.frame(time = ncdf_ae_time,
                    AE   = AE * 1000 * 1e6 / 44.01,
                    stringsAsFactors = F)
  
  colnames(tmp) <- c("time",paste(sites[ith_site],"_AE",sep=""))
  
  if(is.null(data_ae)){
    data_ae <- tmp
  }else{
    data_ae <- merge(data_ae,tmp,by="time",all=T)
  }
}

data_ae$date <- strptime("20150101000000","%Y%m%d%H%M%S",tz="UTC") + data_ae$time*3600

data_ae      <- data_ae[,which(colnames(data_ae)!="time")]

nc_close(ncdf_ae)

#

figname <- paste(resultdir,"AE_TS.pdf",sep="")

def_par <- par()
pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
par(mai=c(1,1,0.75,0.1),mfrow=c(1,1))

for(ith_site in 1:n_sites){
  
  pos     <- which(colnames(data_ae)==paste(sites[ith_site],"_AE",sep=""))
  n_pos   <- length(pos)
  
  min_y   <- min(data_ae[,pos])
  max_y   <- max(data_ae[,pos])
  
  xlabStr <- "date"
  ylabStr <- "Ant. emissions [umol m-2 s-1]"
  
  lab_dates <- seq(strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),
                   strptime("20210101000000","%Y%m%d%H%M%S",tz="UTC"),by="3 months")
  
  lab_dates_str <- strftime(lab_dates, "%m/%y",tz="UTC")
  
  plot(c(min(data_ae$date),max(data_ae$date)),c(NA,NA),ylim=c(min_y,max_y),xlab=xlabStr, ylab=ylabStr,main=sites[ith_site],xaxt="n",
       cex.axis=1.5,cex.lab=1.5)
  axis(side = 1,at = lab_dates,labels = lab_dates_str,cex.lab=1.5,cex.axis=1.5)
  
  lines(data_ae$date,data_ae[,pos],col=1,lty=1,lwd=1)
  
}

dev.off()
par(def_par)

#

figname <- paste(resultdir,"AE_TS_WEEK.pdf",sep="")
pos     <- grep(pattern = paste("_AE",sep=""),x = colnames(data_ae))
yyy     <- cbind(as.matrix(data_ae[,pos]))

xlabString <- "Date" 
ylabString <- expression(paste("CO2 flux [umol m-2 s-1]"))
legend_str <- colnames(data_ae)[pos]
plot_ts(figname,data_ae$date,yyy,"week",NULL,NULL,xlabString,ylabString,legend_str)

##

em_tot <- NULL

ncdf_ae        <- nc_open("/project/jae/zurich_tree/VPRM/co2_categories_total_zh_500m.nc")

ncdf_ae_lat    <- ncvar_get(ncdf_ae, "lat")
ncdf_ae_lon    <- ncvar_get(ncdf_ae, "lon")

n_ncdf_ae_lat  <- length(ncdf_ae_lat)
n_ncdf_ae_lon  <- length(ncdf_ae_lon)

sites   <- c("RECK","ESMO","ZUE","ALBS","LAE","DUE")
n_sites <- length(sites)

data_ae <- NULL

for(ith_site in 1:n_sites){
  
  if(sites[ith_site]=="DUE"){
    id_loc <- which(tbl_loc$LocationName == "DUE1")
  }else{
    id_loc <- which(tbl_loc$LocationName == sites[ith_site])
  }
  
  id_lat <- which(abs(tbl_loc$LAT_WGS84[id_loc]-ncdf_ae_lat) == min(abs(tbl_loc$LAT_WGS84[id_loc]-ncdf_ae_lat)))[1]
  id_lon <- which(abs(tbl_loc$LON_WGS84[id_loc]-ncdf_ae_lon) == min(abs(tbl_loc$LON_WGS84[id_loc]-ncdf_ae_lon)))[1]
  
  CO2_B  <- ncvar_get(nc = ncdf_ae, varid = "CO2_B", start = c(id_lon,id_lat),count = c(1,1))
  CO2_C  <- ncvar_get(nc = ncdf_ae, varid = "CO2_C", start = c(id_lon,id_lat),count = c(1,1))
  CO2_J  <- ncvar_get(nc = ncdf_ae, varid = "CO2_J", start = c(id_lon,id_lat),count = c(1,1))
  CO2_L  <- ncvar_get(nc = ncdf_ae, varid = "CO2_L", start = c(id_lon,id_lat),count = c(1,1))
  CO2_F  <- ncvar_get(nc = ncdf_ae, varid = "CO2_F", start = c(id_lon,id_lat),count = c(1,1))
  CO2    <- ncvar_get(nc = ncdf_ae, varid = "CO2",   start = c(id_lon,id_lat),count = c(1,1))
  
  em_tot <- rbind(em_tot,data.frame(site  = sites[ith_site],
                                    lat   = ncdf_ae_lat[id_lat],
                                    lon   = ncdf_ae_lon[id_lon],
                                    CO2_B = CO2_B,
                                    CO2_C = CO2_C,
                                    CO2_J = CO2_J,
                                    CO2_F = CO2_F,
                                    CO2   = CO2,
                                    stringsAsFactors = F))
}

write.table(x = em_tot,file = paste(resultdir,"em_tot.csv",sep=""),sep=";",col.names = T,row.names = F)

nc_close(ncdf_ae)


## Import of traffic data [hourly data, pick for selected locations]

fn_traffic_2018 <- "/project/CarboSense/Statistical_Models/Covid-19_ZH/Data/Traffic/sid_dav_verkehrszaehlung_miv_od2031_2018.csv"
fn_traffic_2019 <- "/project/CarboSense/Statistical_Models/Covid-19_ZH/Data/Traffic/sid_dav_verkehrszaehlung_miv_od2031_2019.csv"
fn_traffic_2020 <- "/project/CarboSense/Statistical_Models/Covid-19_ZH/Data/Traffic/sid_dav_verkehrszaehlung_miv_od2031_2020.csv"

traffic_data <- NULL
traffic_data <- rbind(traffic_data, read.table(file = fn_traffic_2018,header = T,sep = ",",as.is = T))
traffic_data <- rbind(traffic_data, read.table(file = fn_traffic_2019,header = T,sep = ",",as.is = T))
traffic_data <- rbind(traffic_data, read.table(file = fn_traffic_2020,header = T,sep = ",",as.is = T))

traffic_data$date <- strptime(traffic_data$MessungDatZeit,"%Y-%m-%dT%H:%M:%S",tz="UTC")
traffic_data$AnzFahrzeugeStatus_TF <- traffic_data$AnzFahrzeugeStatus== "Gemessen"

id_dst_17      <- which(traffic_data$date  >=strptime("20170326020000","%Y%m%d%H%M%S",tz="UTC")
                        & traffic_data$date<=strptime("20171029030000","%Y%m%d%H%M%S",tz="UTC"))
id_dst_18      <- which(traffic_data$date  >=strptime("20180325020000","%Y%m%d%H%M%S",tz="UTC")
                        & traffic_data$date<=strptime("20181028030000","%Y%m%d%H%M%S",tz="UTC"))
id_dst_19      <- which(traffic_data$date  >=strptime("20190331020000","%Y%m%d%H%M%S",tz="UTC")
                        & traffic_data$date<=strptime("20191027030000","%Y%m%d%H%M%S",tz="UTC"))
id_dst_20      <- which(traffic_data$date  >=strptime("20200329020000","%Y%m%d%H%M%S",tz="UTC")
                        & traffic_data$date<=strptime("20201025030000","%Y%m%d%H%M%S",tz="UTC"))

if(length(id_dst_17)>0){
  traffic_data$date[id_dst_17] <- traffic_data$date[id_dst_17] - 3600
}
if(length(id_dst_18)>0){
  traffic_data$date[id_dst_18] <- traffic_data$date[id_dst_18] - 3600
}
if(length(id_dst_19)>0){
  traffic_data$date[id_dst_19] <- traffic_data$date[id_dst_19] - 3600
}
if(length(id_dst_20)>0){
  traffic_data$date[id_dst_20] <- traffic_data$date[id_dst_20] - 3600
}

traffic_data <- traffic_data[traffic_data$AnzFahrzeugeStatus_TF,]
traffic_data <- traffic_data[order(traffic_data$date),]


traffic_df <- NULL

for(ms in 1:3){
  
  if(ms == 1){
    ZSName <- "Schimmelstrasse (Werdstrasse)"
    cn     <- "AnzFahrzeuge_Schimmelstrasse"
  }
  if(ms == 2){
    ZSName <- "Manessestrasse (Weststrasse)"
    cn     <- "AnzFahrzeuge_Manessestrasse"
  }
  if(ms == 3){
    ZSName <- "Seebahnstrasse (Stauffacherstrasse)"
    cn     <- "AnzFahrzeuge_Seebahnstrasse"
  }
  
  id    <- which(traffic_data$ZSName     == ZSName
                 & traffic_data$AnzFahrzeugeStatus_T)
  
  Richtungen <- sort(unique(traffic_data$Richtung[id]))
  
  id_R1 <- id[which(traffic_data$Richtung[id] == Richtungen[1])]
  id_R2 <- id[which(traffic_data$Richtung[id] == Richtungen[2])]
  
  id_R1 <- id_R1[!duplicated(traffic_data$date[id_R1])]
  id_R2 <- id_R2[!duplicated(traffic_data$date[id_R2])]
  
  tmp <- merge(data.frame(date             = as.POSIXct(traffic_data$date[id_R1]),
                          AnzFahrzeuge_R1  = traffic_data$AnzFahrzeuge[id_R1],
                          stringsAsFactors = F),
               data.frame(date             = as.POSIXct(traffic_data$date[id_R2]),
                          AnzFahrzeuge_R2  = traffic_data$AnzFahrzeuge[id_R2],
                          stringsAsFactors = F))
  
  
  tmp <- data.frame(date   = tmp$date, 
                    NewCol = tmp$AnzFahrzeuge_R1 + tmp$AnzFahrzeuge_R2,
                    stringsAsFactors = F)
  
  colnames(tmp) <- c("date",cn)
  
  if(ms == 1){
    traffic_df <- tmp
  }else{
    traffic_df <- merge(traffic_df,tmp,all=T)
  }
}

rm(tmp,traffic_data,id,id_R1,id_R2,Richtungen)
gc()


## Merge data frames

data <- merge(data,data_METEO,      by="date", all = T)

data <- merge(data,data_LP8,        by="date", all = T)

data <- merge(data,data_NABEL,      by="date", all = T)

data <- merge(data,data_NABEL_Poll, by="date", all = T)

data <- merge(data,data_VPRN,       by="date", all = T)

# (UTC --> CET)
data$date <- data$date + 3600

data <- merge(data,traffic_df,by="date", all = T)

rm(data_METEO,data_LP8,data_NABEL,traffic_df,data_VPRN)
gc()


## Additional time proxies

data$timestamp <- as.numeric(difftime(time1=data$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
data$weektime  <- (data$timestamp-1483315200)%%(86400*7)
data$dow       <- as.numeric(strftime(data$date,"%w",tz="UTC"))
data$hour      <- as.numeric(strftime(data$date,"%H",tz="UTC"))
data$doy       <- as.numeric(strftime(data$date,"%j",tz="UTC"))
data$woy       <- as.numeric(strftime(data$date,"%V",tz="UTC"))


SunPos      <- getSunlightPosition(date = as.POSIXct(data$date-3600),lat = 47.37784,lon = 8.54015)
data$SunAlt <- SunPos$altitude
# data$SunAz  <- SunPos$azimuth

SunPos      <- getSunlightPosition(date = as.POSIXct(strptime(strftime(data$date-3600,"%Y%m%d12000000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")),
                                   lat = 47.37784,lon = 8.54015)

data$MaxSunAltDay <- SunPos$altitude

rm(SunPos)
gc()

hels         <- matrix(c(8.54015,47.37784), nrow=1)
Hels         <- SpatialPoints(hels, proj4string=CRS("+proj=longlat +datum=WGS84"))
sunrise_date <- sunriset(Hels, as.POSIXct(strptime(strftime(data$date-3600,"%Y%m%d12000000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")),
                         direction="sunrise", POSIXct.out=TRUE) + 3600
sunrise_date <- sunriset(Hels, as.POSIXct(strptime(strftime(data$date-3600,"%Y%m%d12000000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")),
                         direction="sunrise",POSIXct.out=TRUE) + 3600

data$timeRelSunrise <- as.numeric(difftime(time1=data$date,time2=as.POSIXct(sunrise_date$time),unit="secs",tz="UTC"))




# "traffic time" in CET

data$date_TT   <- data$date

id_dst_17      <- which(data$date_TT  >=strptime("20170326020000","%Y%m%d%H%M%S",tz="UTC")
                        & data$date_TT<=strptime("20171029030000","%Y%m%d%H%M%S",tz="UTC"))
id_dst_18      <- which(data$date_TT  >=strptime("20180325020000","%Y%m%d%H%M%S",tz="UTC")
                        & data$date_TT<=strptime("20181028030000","%Y%m%d%H%M%S",tz="UTC"))
id_dst_19      <- which(data$date_TT  >=strptime("20190331020000","%Y%m%d%H%M%S",tz="UTC")
                        & data$date_TT<=strptime("20191027030000","%Y%m%d%H%M%S",tz="UTC"))
id_dst_20      <- which(data$date_TT  >=strptime("20200329020000","%Y%m%d%H%M%S",tz="UTC")
                        & data$date_TT<=strptime("20201025030000","%Y%m%d%H%M%S",tz="UTC"))

if(length(id_dst_17)>0){
  data$date_TT[id_dst_17] <- data$date_TT[id_dst_17] + 3600
}
if(length(id_dst_18)>0){
  data$date_TT[id_dst_18] <- data$date_TT[id_dst_18] + 3600
}
if(length(id_dst_19)>0){
  data$date_TT[id_dst_19] <- data$date_TT[id_dst_19] + 3600
}
if(length(id_dst_20)>0){
  data$date_TT[id_dst_20] <- data$date_TT[id_dst_20] + 3600
}

data$hour_TT <- as.numeric(strftime(data$date_TT,"%H",tz="UTC"))

rm(id_dst_17,id_dst_18,id_dst_19,id_dst_20)
rm()


### Public holidays as Sundays; Set non-typical days to NA: "Sächseläuten", "24./31.12.",...

public_holidays <- NULL

public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.01.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("02.01.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("14.04.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("16.04.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("17.04.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("24.04.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.05.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("25.05.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("05.06.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.08.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("11.09.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("24.12.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("25.12.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("26.12.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("31.12.2017 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))

public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.01.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("02.01.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("30.03.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.04.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("02.04.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("16.04.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.05.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("10.05.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("21.05.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.08.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("10.09.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("24.12.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("25.12.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("26.12.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("31.12.2018 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))

public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.01.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("02.01.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("08.04.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("19.04.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("21.04.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("22.04.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.05.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("30.05.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("10.06.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.08.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("09.09.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("24.12.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("25.12.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("26.12.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("31.12.2019 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))

public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.01.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("02.01.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("03.01.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("10.04.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("13.04.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.05.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("21.05.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.06.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("01.08.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("14.09.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("24.12.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("25.12.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("26.12.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow= 0,stringsAsFactors = F))
public_holidays <- rbind(public_holidays,data.frame(date=strptime("31.12.2020 00:00:00","%d.%m.%Y %H:%M:%S"),dow=NA,stringsAsFactors = F))


for(ith_ph in 1:dim(public_holidays)[1]){
  
  id <- which(data$date >= public_holidays$date[ith_ph]
              & data$date < public_holidays$date[ith_ph] + 86400)
  
  if(length(id)>0){
    data$dow[id] <- public_holidays$dow[ith_ph]
  }
}

data$dow2 <- data$dow
data$dow2[which(data$dow2==0)] <- 7


## CO2 Baselines

BSL_SITES   <- c("ALBS_CO2","RIG_NABEL_CO2","LAEG_EMPA_CO2")
n_BSL_SITES <- length(BSL_SITES)

for(ith_BSL_SITE in 1:n_BSL_SITES){
  
  pos     <- which(colnames(data)==BSL_SITES[ith_BSL_SITE])
  
  
  start_date <- strptime("20161231000000","%Y%m%d%H%M%S",tz="UTC")
  
  CO2_BSL <- NULL
  
  for(ii in 0:14){
    tmp     <- timeAverage(mydata = data.frame(date=data$date,CO2=data[,pos],stringsAsFactors = F),
                           avg.time = "15 day",statistic = "median",data.thresh = 75,
                           start.date = start_date-ii*86400)
    
    tmp     <- as.data.frame(tmp)
    
    CO2_BSL <- rbind(CO2_BSL,tmp)
  }
  
  CO2_BSL      <- CO2_BSL[order(CO2_BSL$date),]
  CO2_BSL      <- CO2_BSL[which(!is.na(CO2_BSL$CO2)),]
  CO2_BSL$date <- CO2_BSL$date + 7.5 *86400
  
  
  tmp     <- approx(x      = CO2_BSL$date,
                    y      = CO2_BSL$CO2,
                    xout   = data$date,
                    method = "linear",
                    rule   = 1)
  
  data$NewCol <- tmp$y
  
  colnames(data)[which(colnames(data)=="NewCol")] <- paste(BSL_SITES[ith_BSL_SITE],"_BSL",sep="")
  
}

rm(CO2_BSL,tmp)
gc()


#

for(ith_BSL_SITE in 1:n_BSL_SITES){
  
  figname <- paste(resultdir,"CO2_BSL_",BSL_SITES[ith_BSL_SITE],"_TS.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.75,0.1),mfrow=c(1,1))
  
  xlabStr <- "Date"
  ylabStr <- expression(paste("CO"[2]*" [ppm]"))
  
  lab_dates <- seq(strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),
                   strptime("20210101000000","%Y%m%d%H%M%S",tz="UTC"),by="month")
  
  lab_dates_str <- strftime(lab_dates, "%m/%y",tz="UTC")
  
  
  plot(c(min(data$date),max(data$date)),c(NA,NA),ylim=c(380,450),
       t="l",main="",xlab=xlabStr, ylab=ylabStr,
       cex.main=1.5,cex.lab=1.5,cex.axis=1.5,xaxt="n")
  
  
  pos <- which(colnames(data)==BSL_SITES[ith_BSL_SITE])
  lines(data$date, data[,pos], col=1, lwd=0.5)
  
  pos <- which(colnames(data)==paste(BSL_SITES[ith_BSL_SITE],"_BSL",sep=""))
  lines(data$date, data[,pos], col=2, lwd=2)
  
  
  axis(side = 1,at = lab_dates,labels = lab_dates_str,cex.lab=1.5,cex.axis=1.5)
  
  par(family="mono")
  legend("topright",legend=c("MEAS","BSL"),col=c(1,2),lwd=1,lty=1,bg="white")
  par(family="")
  
  
  dev.off()
  par(def_par)
  
}


##

data$CO2_ZSCH_ZUE             <- data$ZSCH_CO2     - data$ZUE_CO2
data$CO2_ZSCH_ZUE_TRAFFIC     <- data$CO2_ZSCH_ZUE

data$CO2_ZMAN_LP8_ZUE         <- data$ZMAN_LP8_CO2 - data$ZUE_CO2
data$CO2_ZMAN_LP8_ZUE_TRAFFIC <- data$CO2_ZMAN_LP8_ZUE

data$CO2_ZSBS_LP8_ZUE         <- data$ZSBS_LP8_CO2 - data$ZUE_CO2
data$CO2_ZSBS_LP8_ZUE_TRAFFIC <- data$CO2_ZSBS_LP8_ZUE

data$CO2_ZBLG_LP8_ZUE         <- data$ZBLG_LP8_CO2 - data$ZUE_CO2
data$CO2_ZHRG_LP8_ZUE         <- data$ZHRG_LP8_CO2 - data$ZUE_CO2
data$CO2_ZGHD_LP8_ZUE         <- data$ZGHD_LP8_CO2 - data$ZUE_CO2
data$CO2_ZPRD_LP8_ZUE         <- data$ZPRD_LP8_CO2 - data$ZUE_CO2

data$ZUE_CO2_BSL       <- data$ZUE_CO2       - data$RIG_NABEL_CO2
data$RECK_CO2_BSL      <- data$RECK_CO2      - data$RIG_NABEL_CO2
data$ZSCH_CO2_BSL      <- data$ZSCH_CO2      - data$RIG_NABEL_CO2
data$ZHBR_CO2_BSL      <- data$ZHBR_CO2      - data$RIG_NABEL_CO2
data$ESMO_CO2_BSL      <- data$ESMO_CO2      - data$RIG_NABEL_CO2
data$ALBS_CO2_BSL      <- data$ALBS_CO2      - data$RIG_NABEL_CO2
data$ZMAN_LP8_CO2_BSL  <- data$ZMAN_LP8_CO2  - data$RIG_NABEL_CO2
data$ZSBS_LP8_CO2_BSL  <- data$ZSBS_LP8_CO2  - data$RIG_NABEL_CO2
data$ZBLG_LP8_CO2_BSL  <- data$ZBLG_LP8_CO2  - data$RIG_NABEL_CO2
data$ZUE_LP8_CO2_BSL   <- data$ZUE_LP8_CO2   - data$RIG_NABEL_CO2
data$ZDLT_LP8_CO2_BSL  <- data$ZDLT_LP8_CO2  - data$RIG_NABEL_CO2
data$ZSCH_LP8_CO2_BSL  <- data$ZSCH_LP8_CO2  - data$RIG_NABEL_CO2
data$ZBAD_LP8_CO2_BSL  <- data$ZBAD_LP8_CO2  - data$RIG_NABEL_CO2
data$ZGLA_LP8_CO2_BSL  <- data$ZGLA_LP8_CO2  - data$RIG_NABEL_CO2
data$ZHRG_LP8_CO2_BSL  <- data$ZHRG_LP8_CO2  - data$RIG_NABEL_CO2
data$ZGHD_LP8_CO2_BSL  <- data$ZGHD_LP8_CO2  - data$RIG_NABEL_CO2
data$ZPRD_LP8_CO2_BSL  <- data$ZPRD_LP8_CO2  - data$RIG_NABEL_CO2

data$DUE_NABEL_CO2_BSL <- data$DUE_NABEL_CO2 - data$RIG_NABEL_CO2
data$HAE_NABEL_CO2_BSL <- data$HAE_NABEL_CO2 - data$RIG_NABEL_CO2
data$PAY_NABEL_CO2_BSL <- data$PAY_NABEL_CO2 - data$RIG_NABEL_CO2

data$LAEG_EMPA_CO2_BSL <- data$LAEG_EMPA_CO2 - data$RIG_NABEL_CO2

###

tmp <- approx(x=METEO_DAY$timestamp,y=METEO_DAY$Temperature_SEASON,xout = data$timestamp,method = "linear")
data$Temperature_SEASON <- tmp$y

tmp <- approx(x=METEO_DAY$timestamp,y=METEO_DAY$Radiance_SEASON,xout = data$timestamp,method = "linear")
data$Radiance_SEASON <- tmp$y

tmp <- approx(x=METEO_DAY_TI$timestamp,y=METEO_DAY_TI$Temperature_SEASON,xout = data$timestamp,method = "linear")
data$Temperature_SEASON_TI <- tmp$y

tmp <- approx(x=METEO_DAY_TI$timestamp,y=METEO_DAY_TI$Radiance_SEASON,xout = data$timestamp,method = "linear")
data$Radiance_SEASON_TI <- tmp$y


###

pos_1   <- grep(pattern = "_WS",x = colnames(data))
n_pos_1 <- length(pos_1)
pos_2   <- NULL
pos_3   <- NULL
pos_4   <- NULL

for(ii in 1:n_pos_1){
  data$NewCol <- NA
  colnames(data)[which(colnames(data)=="NewCol")] <- paste(colnames(data)[pos_1[ii]],"_9h",sep="")
  pos_2       <- c(pos_2,which(colnames(data)==paste(colnames(data)[pos_1[ii]],"_9h",sep="")))
  
  data$NewCol <- NA
  colnames(data)[which(colnames(data)=="NewCol")] <- paste(colnames(data)[pos_1[ii]],"_6h",sep="")
  pos_3       <- c(pos_3,which(colnames(data)==paste(colnames(data)[pos_1[ii]],"_6h",sep="")))
  
  data$NewCol <- NA
  colnames(data)[which(colnames(data)=="NewCol")] <- paste(colnames(data)[pos_1[ii]],"_3h",sep="")
  pos_4       <- c(pos_4,which(colnames(data)==paste(colnames(data)[pos_1[ii]],"_3h",sep="")))
}

n_pos_2 <- length(pos_2)
n_pos_3 <- length(pos_3)
n_pos_4 <- length(pos_4)

for(ii in 9:dim(data)[1]){
  index <- (ii-8):(ii)
  for(jj in 1:n_pos_2){
    data[ii,pos_2[jj]] <- sum(data[index,pos_1[jj]])
  }
  
  index <- (ii-5):(ii)
  for(jj in 1:n_pos_3){
    data[ii,pos_3[jj]] <- sum(data[index,pos_1[jj]])
  }
  
  index <- (ii-2):(ii)
  for(jj in 1:n_pos_3){
    data[ii,pos_4[jj]] <- sum(data[index,pos_1[jj]])
  }
}

rm(pos_1,pos_2,pos_3,n_pos_1,n_pos_2,n_pos_3)
gc()


###
# 
# cn <- colnames(data)[grep(pattern = "^dT_",colnames(data))]
# 
# for(ith_cn in 1:length(cn)){
#   
#   pos   <- which(colnames(data)==cn[ith_cn])
#   
#   tmp_1 <- c(0,diff(data[,pos]))
#   
#   #
#   
#   tmp_2 <- rep(NA, dim(data)[1])
#   
#   for(ii in 3:dim(data)[1]){
#     index     <- (ii-2):ii
#     tmp_2[ii] <- sum(tmp_1[index])
#   }
#   
#   data$NewCol <- tmp_2
#   colnames(data)[which(colnames(data)=="NewCol")] <- paste(cn[ith_cn],"_3h",sep="")
#   
#   #
#   
#   tmp_2 <- rep(NA, dim(data)[1])
#   
#   for(ii in 6:dim(data)[1]){
#     index     <- (ii-5):ii
#     tmp_2[ii] <- sum(tmp_1[index])
#   }
#   
#   data$NewCol <- tmp_2
#   colnames(data)[which(colnames(data)=="NewCol")] <- paste(cn[ith_cn],"_6h",sep="")
#   
#   #
#   
#   tmp_2 <- rep(NA, dim(data)[1])
#   
#   for(ii in 9:dim(data)[1]){
#     index     <- (ii-8):ii
#     tmp_2[ii] <- sum(tmp_1[index])
#   }
#   
#   data$NewCol <- tmp_2
#   colnames(data)[which(colnames(data)=="NewCol")] <- paste(cn[ith_cn],"_9h",sep="")
# }
# 
# rm(tmp_1,tmp_2,index,cn)
# gc()
# 
# 
#
###

tmp_df <- NULL
tmp_df <- rbind(tmp_df,data.frame(CO2_name         = "ZMAN_LP8_CO2_BSL",
                                  Traffic_name     = "AnzFahrzeuge_Manessestrasse",
                                  Figname          = "AA_ZMAN_LP8_CO2_BSL_vs_TRAFFIC.pdf",
                                  stringsAsFactors = F))

tmp_df <- rbind(tmp_df,data.frame(CO2_name         = "CO2_ZMAN_LP8_ZUE",
                                  Traffic_name     = "AnzFahrzeuge_Manessestrasse",
                                  Figname          = "AA_CO2_ZMAN_LP8_ZUE_vs_TRAFFIC.pdf",
                                  stringsAsFactors = F))

tmp_df <- rbind(tmp_df,data.frame(CO2_name         = "ZSBS_LP8_CO2_BSL",
                                  Traffic_name     = "AnzFahrzeuge_Seebahnstrasse",
                                  Figname          = "AA_ZSBS_LP8_CO2_BSL_vs_TRAFFIC.pdf",
                                  stringsAsFactors = F))

tmp_df <- rbind(tmp_df,data.frame(CO2_name         = "CO2_ZSBS_LP8_ZUE",
                                  Traffic_name     = "AnzFahrzeuge_Seebahnstrasse",
                                  Figname          = "AA_CO2_ZSBS_LP8_ZUE_vs_TRAFFIC.pdf",
                                  stringsAsFactors = F))

tmp_df <- rbind(tmp_df,data.frame(CO2_name         = "ZSCH_CO2_BSL",
                                  Traffic_name     = "AnzFahrzeuge_Schimmelstrasse",
                                  Figname          = "AA_ZSCH_CO2_BSL_vs_TRAFFIC.pdf",
                                  stringsAsFactors = F))

tmp_df <- rbind(tmp_df,data.frame(CO2_name         = "CO2_ZSCH_ZUE",
                                  Traffic_name     = "AnzFahrzeuge_Schimmelstrasse",
                                  Figname          = "AA_CO2_ZSCH_ZUE_vs_TRAFFIC.pdf",
                                  stringsAsFactors = F))


for(ith_entry in 1:dim(tmp_df)[1]){
  
  figname <- paste(resultdir,tmp_df$Figname[ith_entry],sep="")
  
  pos_01   <- which(colnames(data)==tmp_df$CO2_name[ith_entry])
  pos_02   <- which(colnames(data)==tmp_df$Traffic_name[ith_entry])
  
  Nboxes      <- 10
  max_CO2     <- max(data[,pos_01],na.rm=T)
  min_CO2     <- min(data[,pos_01],na.rm=T)
  max_traffic <- max(data[,pos_02],na.rm=T)
  
  tmp_01 <- matrix(NA,ncol=Nboxes,nrow=dim(data)[1])
  tmp_02 <- matrix(NA,ncol=Nboxes,nrow=dim(data)[1])
  
  for(ii in 1:Nboxes){
    
    id <- which(data[,pos_02]   >= (ii-1)*max_traffic/Nboxes
                & data[,pos_02] <=  ii*max_traffic/Nboxes
                & !is.na(data[,pos_01])
                & !is.na(data[,pos_02])
                & data$date < strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC"))
    
    n_id <- length(id)
    
    if(n_id>0){
      tmp_01[1:n_id,ii] <-  data[id,pos_01]
    }
    
    id <- which(data[,pos_02]   >= (ii-1)*max_traffic/Nboxes
                & data[,pos_02] <=  ii*max_traffic/Nboxes
                & !is.na(data[,pos_01])
                & !is.na(data[,pos_02])
                & data$date >= strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC"))
    
    n_id <- length(id)
    
    if(n_id>0){
      tmp_02[1:n_id,ii] <-  data[id,pos_01]
    }
  }
  
  #
  
  def_par <- par()
  pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.75,0.75),mfrow=c(1,2))
  
  id_00 <- which(data$date< strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC") & !is.na(data[,pos_01]) & !is.na(data[,pos_02]))
  id_01 <- which(data$date>=strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC") & !is.na(data[,pos_01]) & !is.na(data[,pos_02]))
  
  Q025        <- quantile(data[c(id_00,id_01),pos_01],probs=0.25)
  Q050        <- quantile(data[c(id_00,id_01),pos_01],probs=0.50)
  Q075        <- quantile(data[c(id_00,id_01),pos_01],probs=0.75)
  
  min_CO2     <- Q050 - 4 * (Q075-Q025)
  max_CO2     <- Q050 + 4 * (Q075-Q025)
  max_traffic <- max(data[c(id_00,id_01),pos_02],na.rm=T)
  
  mainStr_00 <- paste("Zeitraum:",strftime(min(data$date[id_00]),"%Y-%m-%d",tz="UTC"),"-",strftime(max(data$date[id_00]),"%Y-%m-%d",tz="UTC"))
  mainStr_01 <- paste("Zeitraum:",strftime(min(data$date[id_01]),"%Y-%m-%d",tz="UTC"),"-",strftime(max(data$date[id_01]),"%Y-%m-%d",tz="UTC"))
  
  plot(data[id_00,pos_02],data[id_00,pos_01],pch=16,cex=0.5,cex.lab=1.5,cex.axis=1.5,cex.main=1.5,
       xlim=c(0,max_traffic),ylim=c(min_CO2,max_CO2),
       xlab="Anzahl Fahrzeuge / Stunde",ylab=expression(paste("CO"[2]*" [ppm]")),main=mainStr_00)

  lines(c(-1e3,1e9),c(0,0),col="gray50",lwd=2,lty=1)
  
  for(qq in seq(0.25,0.75,0.25)){
    tmp <- quantile(data[id_00,pos_02],probs=qq,na.rm=T)
    if(qq==0.50){
      lines(c(tmp,tmp),c(-1e5,1e5),col="dodgerblue",lwd=2,lty=1)
    }else{
      lines(c(tmp,tmp),c(-1e5,1e5),col="dodgerblue",lwd=2,lty=5)
    }
  }
  
  lines(seq(0+(max_traffic/Nboxes)/2,max_traffic-(max_traffic/Nboxes)/2,max_traffic/Nboxes),
        apply(tmp_01,2,median,na.rm=T),col=2,lwd=2)
  lines(seq(0+(max_traffic/Nboxes)/2,max_traffic-(max_traffic/Nboxes)/2,max_traffic/Nboxes),
        apply(tmp_01,2,quantile,0.25,na.rm=T),col=2,lwd=2,lty=5)
  lines(seq(0+(max_traffic/Nboxes)/2,max_traffic-(max_traffic/Nboxes)/2,max_traffic/Nboxes),
        apply(tmp_01,2,quantile,0.75,na.rm=T),col=2,lwd=2,lty=5)

  par(family="mono")
  legend("topright",legend=c("25%-Perzentil","50%-Perzentil","75%-Perzentil"),bg="white",col=1,lty=c(5,1,5),lwd=2,cex=1.25)
  par(family="")
  
  #
  
  plot(data[id_01,pos_02],data[id_01,pos_01],pch=16,cex=0.5,cex.lab=1.5,cex.axis=1.5,cex.main=1.5,
       xlim=c(0,max_traffic),ylim=c(min_CO2,max_CO2),
       xlab="Anzahl Fahrzeuge / Stunde",ylab=expression(paste("CO"[2]*" [ppm]")),main=mainStr_01)
  
  lines(c(-1e3,1e9),c(0,0),col="gray50",lwd=2,lty=1)
  
  for(qq in seq(0.25,0.75,0.25)){
    tmp <- quantile(data[id_01,pos_02],probs=qq,na.rm=T)
    if(qq==0.50){
      lines(c(tmp,tmp),c(-1e5,1e5),col="dodgerblue",lwd=2,lty=1)
    }else{
      lines(c(tmp,tmp),c(-1e5,1e5),col="dodgerblue",lwd=2,lty=5)
    }
  }
  
  lines(seq(0+(max_traffic/Nboxes)/2,max_traffic-(max_traffic/Nboxes)/2,max_traffic/Nboxes),
        apply(tmp_02,2,median,na.rm=T),col=2,lwd=2)
  lines(seq(0+(max_traffic/Nboxes)/2,max_traffic-(max_traffic/Nboxes)/2,max_traffic/Nboxes),
        apply(tmp_02,2,quantile,0.25,na.rm=T),col=2,lwd=2,lty=5)
  lines(seq(0+(max_traffic/Nboxes)/2,max_traffic-(max_traffic/Nboxes)/2,max_traffic/Nboxes),
        apply(tmp_02,2,quantile,0.75,na.rm=T),col=2,lwd=2,lty=5)
  
  par(family="mono")
  legend("topright",legend=c("25%-Perzentil","50%-Perzentil","75%-Perzentil"),bg="white",col=1,lty=c(5,1,5),lwd=2,cex=1.25)
  par(family="")
  
  dev.off()
  par(def_par)
  
  rm(tmp_01,tmp_02,tmp,mainStr_00,mainStr_01,Q025,Q050,Q075)
  gc()
}



###

print("RF")

target_v <- c("LAEG_EMPA_CO2","LAEG_EMPA_CO2_BSL",
              "LUG_LP8_CO2","LUGN_LP8_CO2",
              "ZUE_CO2","ZSCH_CO2","ZHBR_CO2","RECK_CO2","ALBS_CO2","ESMO_CO2",
              "CO2_ZSCH_ZUE","CO2_ZMAN_LP8_ZUE","CO2_ZSBS_LP8_ZUE","CO2_ZBLG_LP8_ZUE","CO2_ZHRG_LP8_ZUE","CO2_ZGHD_LP8_ZUE",
              "CO2_ZSCH_ZUE_TRAFFIC","CO2_ZMAN_LP8_ZUE_TRAFFIC","CO2_ZSBS_LP8_ZUE_TRAFFIC",
              "ZUE_CO2_BSL","ZSCH_CO2_BSL","ZHBR_CO2_BSL","RECK_CO2_BSL","ALBS_CO2_BSL","ESMO_CO2_BSL",
              "ZMAN_LP8_CO2","ZSBS_LP8_CO2","ZBLG_LP8_CO2","ZUE_LP8_CO2","ZDLT_LP8_CO2","ZSCH_LP8_CO2",
              "ZBAD_LP8_CO2","ZGLA_LP8_CO2","ZGHD_LP8_CO2","ZHRG_LP8_CO2","ZPRD_LP8_CO2",
              "ZMAN_LP8_CO2_BSL","ZSBS_LP8_CO2_BSL","ZBLG_LP8_CO2_BSL","ZUE_LP8_CO2_BSL","ZDLT_LP8_CO2_BSL","ZSCH_LP8_CO2_BSL",
              "ZBAD_LP8_CO2_BSL","ZGLA_LP8_CO2_BSL","ZGHD_LP8_CO2_BSL","ZHRG_LP8_CO2_BSL","ZPRD_LP8_CO2_BSL",
              "DUE_NABEL_CO2","HAE_NABEL_CO2","RIG_NABEL_CO2","PAY_NABEL_CO2",
              "DUE_NABEL_CO2_BSL","HAE_NABEL_CO2_BSL","PAY_NABEL_CO2_BSL",
              "HAE_NABEL_NOX","DUE_NABEL_NOX","PAY_NABEL_NOX")

# target_v <- c("CO2_ZMAN_LP8_ZUE_TRAFFIC","ZMAN_LP8_CO2_BSL","CO2_ZMAN_LP8_ZUE",
#               "ZSBS_LP8_CO2_BSL","CO2_ZSBS_LP8_ZUE","CO2_ZSBS_LP8_ZUE_TRAFFIC",
#               "ZSCH_LP8_CO2_BSL","CO2_ZSCH_LP8_ZUE","CO2_ZSCH_LP8_ZUE_TRAFFIC",
#               "ZSCH_CO2_BSL","CO2_ZSCH_ZUE","CO2_ZSCH_ZUE_TRAFFIC",
#               "ZUE_CO2_BSL")
# 
# target_v <- c("ZSCH_CO2","ZSCH_CO2_BSL","CO2_ZSCH_ZUE","CO2_ZSCH_ZUE_TRAFFIC")

# target_v <- c("LAEG_EMPA_CO2","LAEG_EMPA_CO2_BSL")
# 
# target_v <- c("DUE_NABEL_CO2","HAE_NABEL_CO2","RIG_NABEL_CO2","PAY_NABEL_CO2",
#               "DUE_NABEL_CO2_BSL","HAE_NABEL_CO2_BSL","PAY_NABEL_CO2_BSL")

# target_v <- c("ZUE_CO2_BSL","ZSCH_CO2_BSL","ZMAN_LP8_CO2_BSL","ZSBS_LP8_CO2_BSL","ZBLG_LP8_CO2_BSL","ZUE_LP8_CO2_BSL","ZDLT_LP8_CO2_BSL","ZSCH_LP8_CO2_BSL",
#             "ZBAD_LP8_CO2_BSL","ZGLA_LP8_CO2_BSL","ZGHD_LP8_CO2_BSL","ZHRG_LP8_CO2_BSL","ZPRD_LP8_CO2_BSL")

target_v <- c("HAE_NABEL_NOX","DUE_NABEL_NOX","PAY_NABEL_NOX","RECK_CO2_BSL","DUE_NABEL_CO2_BSL","HAE_NABEL_CO2_BSL","PAY_NABEL_CO2_BSL")


target_v <- c("LAEG_EMPA_CO2_BSL",
              "CO2_ZSCH_ZUE","CO2_ZMAN_LP8_ZUE","CO2_ZSBS_LP8_ZUE","CO2_ZBLG_LP8_ZUE","CO2_ZHRG_LP8_ZUE","CO2_ZGHD_LP8_ZUE",
              "CO2_ZSCH_ZUE_TRAFFIC","CO2_ZMAN_LP8_ZUE_TRAFFIC","CO2_ZSBS_LP8_ZUE_TRAFFIC",
              "ZUE_CO2_BSL","ZSCH_CO2_BSL","ZHBR_CO2_BSL","RECK_CO2_BSL","ALBS_CO2_BSL","ESMO_CO2_BSL",
              "ZMAN_LP8_CO2_BSL","ZSBS_LP8_CO2_BSL","ZBLG_LP8_CO2_BSL","ZUE_LP8_CO2_BSL","ZDLT_LP8_CO2_BSL","ZSCH_LP8_CO2_BSL",
              "ZBAD_LP8_CO2_BSL","ZGLA_LP8_CO2_BSL","ZGHD_LP8_CO2_BSL","ZHRG_LP8_CO2_BSL","ZPRD_LP8_CO2_BSL",
              "DUE_NABEL_CO2_BSL","HAE_NABEL_CO2_BSL","PAY_NABEL_CO2_BSL",
              "HAE_NABEL_NOX","DUE_NABEL_NOX","PAY_NABEL_NOX")

statistics <- NULL


for(target in target_v){
  
  print(target)
  
  cn <- NULL
  
  if(target %in% c("ZUE_CO2","ZUE_CO2_BSL")){
    cn <- c("hour","dow2","hour_TT",
            "NABZUE_T","NABZUE_WS","NABZUE_WD",
            "dT_NABZUE_KLO","dT_NABZUE_LAE",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  # ZSCH and (NAB)ZUE very close
  if(target %in% c("ZSCH_CO2","ZSCH_CO2_BSL")){
    cn <- c("hour","dow2","hour_TT",
            "NABZUE_T","NABZUE_WS","NABZUE_WD",
            "dT_NABZUE_KLO","dT_NABZUE_LAE",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  if(target %in% c("CO2_ZSCH_ZUE",
                   "CO2_ZMAN_LP8_ZUE","CO2_ZSBS_LP8_ZUE","CO2_ZBLG_LP8_ZUE","CO2_ZHRG_LP8_ZUE","CO2_ZGHD_LP8_ZUE")){
    cn <- c("hour","dow2","hour_TT",
            "NABZUE_T","NABZUE_WS","NABZUE_WD",
            "dT_NABZUE_KLO","dT_NABZUE_LAE")
  }
  
  
  
  if(target %in% c("CO2_ZSCH_ZUE_TRAFFIC")){
    cn <- c("hour","timeRelSunrise",
            "NABZUE_T","NABZUE_WS","NABZUE_WD",
            "dT_NABZUE_KLO","dT_NABZUE_LAE",
            "AnzFahrzeuge_Schimmelstrasse")
  }
  
  if(target %in% c("CO2_ZMAN_LP8_ZUE_TRAFFIC")){
    cn <- c("hour","timeRelSunrise",
            "NABZUE_T","NABZUE_WS","NABZUE_WD",
            "dT_NABZUE_KLO","dT_NABZUE_LAE",
            "AnzFahrzeuge_Manessestrasse")
  }
  
  if(target %in% c("CO2_ZSBS_LP8_ZUE_TRAFFIC")){
    cn <- c("hour",
            "NABZUE_T","NABZUE_WS","NABZUE_WD",
            "dT_NABZUE_KLO","dT_NABZUE_LAE",
            "AnzFahrzeuge_Seebahnstrasse")
  }
  
  # SMA and ZHBR very close
  if(target %in% c("ZHBR_CO2","ZHBR_CO2_BSL")){
    cn <- c("hour","dow2","hour_TT",
            "SMA_T","SMA_WS","SMA_WD",
            "dT_NABZUE_KLO","dT_NABZUE_LAE",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  # REH and RECK same location
  if(target %in% c("RECK_CO2","RECK_CO2_BSL")){
    cn <- c("hour","dow2","hour_TT",
            "REH_T","REH_WS","REH_WD",
            "dT_REH_KLO","dT_REH_LAE",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  if(target %in% c("RECK_CO2","RECK_CO2_BSL")){
    cn <- c("dow2",
            "REH_T","REH_WS","REH_WD",
            "dT_REH_LAE",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  # !!! NOT OPTIMAL
  if(target %in% c("ALBS_CO2","ALBS_CO2_BSL")){
    cn <- c("hour","dow2","hour_TT",
            "SMA_T","SMA_WS",
            "dT_NABZUE_LAE",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  # !!! NOT OPTIMAL
  if(target %in% c("ESMO_CO2","ESMO_CO2_BSL")){
    cn <- c("hour","dow2","hour_TT",
            "KLO_T","KLO_WS",
            "dT_KLO_LAE",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  # ZSBS, ZMAN and (NAB)ZUE very close
  if(target %in% c("ZMAN_LP8_CO2","ZSBS_LP8_CO2","ZBLG_LP8_CO2","ZUE_LP8_CO2","ZDLT_LP8_CO2","ZSCH_LP8_CO2",
                   "ZGLA_LP8_CO2","ZBAD_LP8_CO2","ZGHD_LP8_CO2","ZHRG_LP8_CO2","ZPRD_LP8_CO2",
                   "ZMAN_LP8_CO2_BSL","ZSBS_LP8_CO2_BSL","ZBLG_LP8_CO2_BSL","ZUE_LP8_CO2_BSL","ZDLT_LP8_CO2_BSL","ZSCH_LP8_CO2_BSL",
                   "ZGLA_LP8_CO2_BSL","ZBAD_LP8_CO2_BSL","ZGHD_LP8_CO2_BSL","ZHRG_LP8_CO2_BSL","ZPRD_LP8_CO2_BSL")){
    cn <- c("hour","dow2","hour_TT",
            "NABZUE_T","NABZUE_WS","NABZUE_WD",
            "dT_NABZUE_KLO","dT_NABZUE_LAE",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  #
  
  if(target %in% c("LUG_LP8_CO2","LUGN_LP8_CO2")){
    cn <- c("hour","dow2","hour_TT",
            "LUG_T","LUG_WS","LUG_WD",
            "dT_LUG_GEN",
            "Temperature_SEASON_TI","Radiance_SEASON_TI")
  }
  
  #
  
  if(target %in% c("DUE_NABEL_CO2","DUE_NABEL_CO2_BSL","DUE_NABEL_NOX")){
    cn <- c("hour","dow2","hour_TT",
            "NABDUE_T","NABDUE_WS","NABDUE_WD",
            "dT_NABDUE_KLO","dT_NABDUE_LAE",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  
  if(target %in% c("HAE_NABEL_CO2","HAE_NABEL_CO2_BSL","HAE_NABEL_NOX")){
    cn <- c("hour","dow2","hour_TT",
            "NABHAE_T","NABHAE_WS","NABHAE_WD",
            "dT_NABRIG_NABHAE","dT_NABHAE_KOP",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  if(target %in% c("RIG_NABEL_CO2","RIG_NABEL_NOX")){
    cn <- c("hour","dow2",
            "NABRIG_T","NABRIG_WS",
            "dT_NABRIG_CHZ",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  if(target %in% c("PAY_NABEL_CO2","PAY_NABEL_CO2_BSL","PAY_NABEL_NOX")){
    cn <- c("hour","dow2","hour_TT",
            "PAY_T","PAY_WS","PAY_WD",
            "dT_PAY_CHM","dT_PAY_GRA",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  # !!! NOT OPTIMAL
  if(target %in% c("LAEG_EMPA_CO2","LAEG_EMPA_CO2_BSL")){
    cn <- c("hour","dow2",
            "KLO_WS","LAE_T",
            "dT_KLO_LAE",
            "Temperature_SEASON","Radiance_SEASON")
  }
  
  
  if(is.null(cn)){
    print(paste(target,"not defined!"))
    next
  }
  
  # if(length(grep(pattern = "_BSL",x = target))){
  #   cn <- cn[!cn%in%c("Temperature_SEASON","Radiance_SEASON","Temperature_SEASON_TI","Radiance_SEASON_TI")]
  # }
  # if(length(grep(pattern = "ZSCH_ZUE",x = target))){
  #   cn <- cn[!cn%in%c("Temperature_SEASON","Radiance_SEASON","Temperature_SEASON_TI","Radiance_SEASON_TI")]
  # }
  
  pos_ws   <- grep(pattern = "_WS$",x = cn)
  n_pos_ws <- length(pos_ws)
  if(n_pos_ws>0){
    for(ii in 1:n_pos_ws){
      cn <- c(cn,paste(cn[pos_ws[ii]],"_3h",sep=""),
              paste(cn[pos_ws[ii]],"_6h",sep=""),
              paste(cn[pos_ws[ii]],"_9h",sep=""))
    }
  }
  
  #
  
  # pos_ws   <- grep(pattern = "^dT_",x = cn)
  # n_pos_ws <- length(pos_ws)
  # if(n_pos_ws>0){
  #   for(ii in 1:n_pos_ws){
  #     cn <- c(cn,paste(cn[pos_ws[ii]],"_3h",sep=""),
  #             paste(cn[pos_ws[ii]],"_6h",sep=""),
  #             paste(cn[pos_ws[ii]],"_9h",sep=""))
  #   }
  # }
  
  #
  
  pos_ws   <- grep(pattern = "_T$",x = cn)
  n_pos_ws <- length(pos_ws)
  if(n_pos_ws>0){
    for(ii in 1:n_pos_ws){
      cn <- c(cn,gsub(pattern = "_T",replacement = "_RH",x = cn[pos_ws[ii]]))
    }
  }
  
  #
  
  if(length(grep(pattern = "LUG",x = target))>0){
    cn <- c(cn,"LUG_P")
  }
  if(length(grep(pattern = "LUG",x = target,invert=T))>0){
    cn <- c(cn,"KLO_P")
  }
  
  cn    <- unique(c(cn,"timeRelSunrise","MaxSunAltDay"))
  
  cn    <- unique(c(cn,"doy","woy"))
  
  cn    <- cn[which(cn%in%colnames(data))]
  
  
  id_cn <- which(colnames(data)%in%c(target,cn))
  
  
  formula     <- as.formula(paste(target,"~",paste(cn,collapse = "+"),sep=""))
  
  
  id_train    <- which(data$date  >=strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC")
                       & data$date< strptime("20200215000000","%Y%m%d%H%M%S",tz="UTC")
                       & complete.cases(data[,id_cn]))
  
  id_test     <- which(data$date  >=strptime("20200215000000","%Y%m%d%H%M%S",tz="UTC")
                       & data$date< strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC")
                       & complete.cases(data[,id_cn]))
  
  id_test_00  <- which(data$date  >=strptime("20200215000000","%Y%m%d%H%M%S",tz="UTC")
                       & data$date< strptime("20200314000000","%Y%m%d%H%M%S",tz="UTC")
                       & complete.cases(data[,id_cn]))
  
  id_test_01  <- which(data$date  >=strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC")
                       & data$date< strptime("20200427000000","%Y%m%d%H%M%S",tz="UTC")
                       & complete.cases(data[,id_cn]))
  
  id_test_02  <- which(data$date  >=strptime("20200427000000","%Y%m%d%H%M%S",tz="UTC")
                       & data$date< strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC")
                       & complete.cases(data[,id_cn]))
  
  id_test_03  <- which(data$date  >=strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC")
                       & data$date< strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC")
                       & complete.cases(data[,id_cn]))
  
  
  ntree    <- 250
  nodesize <- 5
  
  
  if(T){
    rf_obj <- randomForest(formula    = formula,
                           data       = data[id_train,id_cn],
                           ntree      = ntree,
                           nodesize   = nodesize,
                           mtry       = ceiling(3/4*(length(id_cn))),
                           importance = TRUE)
  }
  
  if(F){
    rpart_control <- rpart.control(minsplit = 12,minbucket = 4,cp = 1e-8)
    
    rf_obj        <- rpart(formula = formula,
                           data    = data[id_train,id_cn],
                           control = rpart_control)
  }
  
  CO2_pred <- predict(rf_obj,newdata = data)
  
  data$NewCol <- CO2_pred
  
  colnames(data)[which(colnames(data)=="NewCol")] <- paste(target,"_PRED",sep="")
  
  #
  
  if(T){
    
    figname <- paste(resultdir,target,"_VarImpPlot.pdf",sep="")
    
    def_par <- par()
    pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1,1,0.75,0.1),mfrow=c(1,1))
    
    varImpPlot(rf_obj)
    
    par(def_par)
    dev.off()
  }
  
  #
  
  pos      <- which(colnames(data)==target)
  
  # Statistics
  
  N_TEST       <- length(id_test)
  N_TEST_00    <- length(id_test_00)
  N_TEST_01    <- length(id_test_01)
  N_TEST_02    <- length(id_test_02)
  N_TEST_03    <- length(id_test_03)
  N_TRAIN      <- length(id_train)
  
  RMSE_TEST    <- sqrt(sum((data[id_test,    pos]-CO2_pred[id_test]   )^2) /length(id_test))
  RMSE_TEST_00 <- sqrt(sum((data[id_test_00, pos]-CO2_pred[id_test_00])^2) /length(id_test_00))
  RMSE_TEST_01 <- sqrt(sum((data[id_test_01, pos]-CO2_pred[id_test_01])^2) /length(id_test_01))
  RMSE_TEST_02 <- sqrt(sum((data[id_test_02, pos]-CO2_pred[id_test_02])^2) /length(id_test_02))
  RMSE_TEST_03 <- sqrt(sum((data[id_test_03, pos]-CO2_pred[id_test_03])^2) /length(id_test_03))
  RMSE_TRAIN   <- sqrt(sum((data[id_train,   pos]-CO2_pred[id_train]  )^2) /length(id_train))
  
  COR_TEST     <- cor(x = data[id_test,     pos], y = CO2_pred[id_test],    method="pearson",use = "complete.obs")
  COR_TEST_00  <- cor(x = data[id_test_00,  pos], y = CO2_pred[id_test_00], method="pearson",use = "complete.obs")
  COR_TEST_01  <- cor(x = data[id_test_01,  pos], y = CO2_pred[id_test_01], method="pearson",use = "complete.obs")
  COR_TEST_02  <- cor(x = data[id_test_02,  pos], y = CO2_pred[id_test_02], method="pearson",use = "complete.obs")
  COR_TEST_03  <- cor(x = data[id_test_03,  pos], y = CO2_pred[id_test_03], method="pearson",use = "complete.obs")
  COR_TRAIN    <- cor(x = data[id_train,    pos], y = CO2_pred[id_train],   method="pearson",use = "complete.obs")
  
  diff       <- CO2_pred-data[,pos]
  
  statistics <- rbind(statistics,
                      data.frame(Target                 = target,
                                 TRAIN_Date_UTC_from    = strftime(min(data$date[id_train]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                 TRAIN_Date_UTC_to      = strftime(max(data$date[id_train]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                 N_TRAIN                = N_TRAIN,
                                 RMSE_TRAIN             = RMSE_TRAIN,
                                 COR_TRAIN              = COR_TRAIN,
                                 TEST_Date_UTC_from     = strftime(min(data$date[id_test]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                 TEST_Date_UTC_to       = strftime(max(data$date[id_test]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                 N_TEST                 = N_TEST,
                                 RMSE_TEST              = RMSE_TEST,
                                 COR_TEST               = COR_TEST,
                                 TEST_00_Date_UTC_from  = strftime(min(data$date[id_test_00]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                 TEST_00_Date_UTC_to    = strftime(max(data$date[id_test_00]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                 N_TEST_00              = N_TEST_00,
                                 RMSE_TEST_00           = RMSE_TEST_00,
                                 COR_TEST_00            = COR_TEST_00,
                                 TEST_01_Date_UTC_from  = strftime(min(data$date[id_test_01]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                 TEST_01_Date_UTC_to    = strftime(max(data$date[id_test_01]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                 N_TEST_01              = N_TEST_01,
                                 RMSE_TEST_01           = RMSE_TEST_01,
                                 COR_TEST_01            = COR_TEST_01,
                                 TEST_02_Date_UTC_from  = strftime(min(data$date[id_test_02]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                 TEST_02_Date_UTC_to    = strftime(max(data$date[id_test_02]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                 N_TEST_02              = N_TEST_02,
                                 RMSE_TEST_02           = RMSE_TEST_02,
                                 COR_TEST_02            = COR_TEST_02,
                                 TEST_03_Date_UTC_from  = strftime(min(data$date[id_test_03]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                 TEST_03_Date_UTC_to    = strftime(max(data$date[id_test_03]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                 N_TEST_03              = N_TEST_03,
                                 RMSE_TEST_03           = RMSE_TEST_03,
                                 COR_TEST_03            = COR_TEST_03,
                                 DIFF_TRAINING_Q000     = quantile(x = diff[id_train],probs = 0.00),
                                 DIFF_TRAINING_Q005     = quantile(x = diff[id_train],probs = 0.05),
                                 DIFF_TRAINING_Q050     = quantile(x = diff[id_train],probs = 0.50),
                                 DIFF_TRAINING_Q095     = quantile(x = diff[id_train],probs = 0.95),
                                 DIFF_TRAINING_Q100     = quantile(x = diff[id_train],probs = 1.00),
                                 DIFF_TESTING_Q000      = quantile(x = diff[id_test], probs = 0.00),
                                 DIFF_TESTING_Q005      = quantile(x = diff[id_test], probs = 0.05),
                                 DIFF_TESTING_Q050      = quantile(x = diff[id_test], probs = 0.50),
                                 DIFF_TESTING_Q095      = quantile(x = diff[id_test], probs = 0.95),
                                 DIFF_TESTING_Q100      = quantile(x = diff[id_test], probs = 1.00),
                                 stringsAsFactors = F))
  
  
  # Zeitreihe
  
  figname <- paste(resultdir,target,"_TS.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.75,0.1),mfrow=c(1,1))
  
  mainStr <- target
  xlabStr <- "Date"
  ylabStr <- expression(paste("CO"[2]*" [ppm]"))
  
  lab_dates <- seq(strptime("20191230000000","%Y%m%d%H%M%S",tz="UTC"),
                   strptime("20201230000000","%Y%m%d%H%M%S",tz="UTC"),by="week")
  
  lab_dates_str <- strftime(lab_dates, "%d/%m",tz="UTC")
  
  plot(data$date[id_test],data[id_test,pos],t="l",main=mainStr,xlab=xlabStr, ylab=ylabStr,
       cex.main=1.5,cex.lab=1.5,cex.axis=1.5,xaxt="n")
  lines(data$date[id_test],CO2_pred[id_test],col=2)
  
  axis(side = 1,at = lab_dates,labels = lab_dates_str,cex.lab=1.5,cex.axis=1.5)
  
  lines(c(strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC")),
        c(-1e9,1e9),col=4,lwd=4)
  
  par(family="mono")
  legend("topright",legend=c("Measurement","Testing"),col=c(1,2),lty=1,cex=1.25,bg="white")
  par(family="")
  
  dev.off()
  par(def_par)
  
  #
  
  figname    <- paste(resultdir,target,"_TS_WEEK.pdf",sep="")
  
  pos_pred   <- paste(target,"_PRED",sep="")
  
  yyy        <- cbind(data[,pos],data[,pos_pred])
  
  xlabString <- "Date" 
  ylabString <- expression(paste("CO2 [ppm]"))
  legend_str <- c(target,paste(target,"_PRED",sep=""))
  plot_ts(figname,data$date,yyy,"week",NULL,NULL,xlabString,ylabString,legend_str)
  
  
  # Zeitreihe Residuen
  
  figname <- paste(resultdir,target,"_RESIDUALS_TS.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.75,0.1),mfrow=c(1,1))
  
  mainStr <- target
  xlabStr <- "Date"
  ylabStr <- expression(paste("Meas minus pred CO"[2]*" [ppm]"))
  
  lab_dates <- seq(strptime("20191230000000","%Y%m%d%H%M%S",tz="UTC"),
                   strptime("20201230000000","%Y%m%d%H%M%S",tz="UTC"),by="week")
  
  lab_dates_str <- strftime(lab_dates, "%d/%m",tz="UTC")
  
  plot(data$date[id_test],data[id_test,pos] - CO2_pred[id_test] ,t="l",main=mainStr,xlab=xlabStr, ylab=ylabStr,
       cex.main=1.5,cex.lab=1.5,cex.axis=1.5,xaxt="n")
  
  axis(side = 1,at = lab_dates,labels = lab_dates_str,cex.lab=1.5,cex.axis=1.5)
  
  lines(c(strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC")),
        c(-1e9,1e9),col=4,lwd=4)
  
  lines(c(strptime("20000101000000","%Y%m%d%H%M%S",tz="UTC"),strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC")),
        c(0,0),col=2,lwd=1)
  
  dev.off()
  par(def_par)
  
  #
  
  figname    <- paste(resultdir,target,"_RESIDUALS_TS_WEEK.pdf",sep="")
  
  pos_pred   <- paste(target,"_PRED",sep="")
  
  yyy        <- cbind(data[,pos] - CO2_pred)
  
  xlabString <- "Date" 
  ylabString <- expression(paste("CO2 [ppm]"))
  legend_str <- c(paste(target," Residuals",sep=""))
  plot_ts(figname,data$date,yyy,"week",NULL,NULL,xlabString,ylabString,legend_str)
  
  
  # Scatter
  
  str_01 <- paste("RMSE T00:",sprintf("%5.1f",RMSE_TEST_00),sep="")
  str_02 <- paste("COR  T00:",sprintf("%5.2f",COR_TEST_00), sep="")
  str_03 <- paste("N    T00:",sprintf("%5.0f",N_TEST_00),   sep="")
  
  str_04 <- paste("RMSE T03:",sprintf("%5.1f",RMSE_TEST_03),sep="")
  str_05 <- paste("COR  T03:",sprintf("%5.2f",COR_TEST_03), sep="")
  str_06 <- paste("N    T03:",sprintf("%5.0f",N_TEST_03),   sep="")
  
  Q000_T00 <- quantile(diff[id_test_00],probs=0.00)
  Q005_T00 <- quantile(diff[id_test_00],probs=0.05)
  Q050_T00 <- quantile(diff[id_test_00],probs=0.50)
  Q095_T00 <- quantile(diff[id_test_00],probs=0.95)
  Q100_T00 <- quantile(diff[id_test_00],probs=1.00)
  
  Q000_T03 <- quantile(diff[id_test_03],probs=0.00)
  Q005_T03 <- quantile(diff[id_test_03],probs=0.05)
  Q050_T03 <- quantile(diff[id_test_03],probs=0.50)
  Q095_T03 <- quantile(diff[id_test_03],probs=0.95)
  Q100_T03 <- quantile(diff[id_test_03],probs=1.00)
  
  quantile(diff[id_test_01],probs=0.00)
  
  str_11 <- paste("Q000:",sprintf("%6.1f",Q000_T00),
                  sprintf("%6.1f",Q000_T03),sep="")
  str_12 <- paste("Q005:",sprintf("%6.1f",Q005_T00),
                  sprintf("%6.1f",Q005_T03),sep="")
  str_13 <- paste("Q050:",sprintf("%6.1f",Q050_T00),
                  sprintf("%6.1f",Q050_T03),sep="")
  str_14 <- paste("Q095:",sprintf("%6.1f",Q095_T00),
                  sprintf("%6.1f",Q095_T03),sep="")
  str_15 <- paste("Q100:",sprintf("%6.1f",Q100_T00),
                  sprintf("%6.1f",Q100_T03),sep="")
  
  figname <- paste(resultdir,target,"_SP.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.75,0.1),mfrow=c(1,2))
  
  mainStr <- target
  xlabStr <- "Measurement CO2 [ppm]"
  ylabStr <- "Training / Testing CO2 [ppm]"
  
  xyrange <-c(c(min(c(data[,pos],CO2_pred),na.rm=T)),max(c(data[,pos],CO2_pred),na.rm=T))
  
  plot(data[,pos],CO2_pred,pch=16,cex=0.5,xlim=xyrange,ylim=xyrange,
       main=mainStr,xlab=xlabStr, ylab=ylabStr,
       cex.main=1.5,cex.lab=1.5,cex.axis=1.5)
  points(data[id_test_00,pos],CO2_pred[id_test_00],pch=16,cex=0.5,col=3)
  points(data[id_test_03,pos],CO2_pred[id_test_03],pch=16,cex=0.5,col=2)
  lines(c(-1e4,1e4),c(-1e4,1e4),col=2)
  
  par(family="mono")
  legend("bottomright",legend=c("Training","Test before LD","Test after LD"),col=c(1,3,2),pch=16,bg="white",cex=1.25)
  legend("topleft",legend=c(str_01,str_02,str_03,str_04,str_05,str_06),bg="white",cex=1.25)
  par(family="")
  
  #
  
  mainStr <- target
  xlabStr <- "Testing - Measurement CO2 [ppm]"
  
  hist(CO2_pred[id_test]-data[id_test,pos],seq(-1e3,1e3,2),xlim=c(-50,50),col="slategray",
       main=mainStr,xlab=xlabStr,
       cex.main=1.5,cex.lab=1.5,cex.axis=1.5)
  
  lines(c(0,0),c(-1e3,1e3),col=1,lwd=2)
  
  lines(c(Q005_T00,Q005_T00),c(-1e3,1e3),col=3,lwd=2,lty=5)
  lines(c(Q050_T00,Q050_T00),c(-1e3,1e3),col=3,lwd=2,lty=1)
  lines(c(Q095_T00,Q095_T00),c(-1e3,1e3),col=3,lwd=2,lty=5)
  
  lines(c(Q005_T03,Q005_T03),c(-1e3,1e3),col=2,lwd=2,lty=5)
  lines(c(Q050_T03,Q050_T03),c(-1e3,1e3),col=2,lwd=2,lty=1)
  lines(c(Q095_T03,Q095_T03),c(-1e3,1e3),col=2,lwd=2,lty=5)
  
  
  par(family="mono")
  legend("topleft",legend=c(str_11,str_12,str_13,str_14,str_15),bg="white",cex=1.25)
  par(family="")
  
  
  dev.off()
  par(def_par)
  
  
  
  # Boxplots residuals
  
  figname <- paste(resultdir,target,"_BP_res.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=6, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.75,0.1),mfrow=c(1,1))
  
  tmp                <- matrix(NA,nrow = max(c(N_TEST_00,N_TEST_01,N_TEST_02)),ncol = 3)
  
  tmp[1:N_TEST_00,1] <- data[id_test_00,pos] - CO2_pred[id_test_00]
  tmp[1:N_TEST_01,2] <- data[id_test_01,pos] - CO2_pred[id_test_01]
  tmp[1:N_TEST_02,3] <- data[id_test_02,pos] - CO2_pred[id_test_02]
  
  id_ok  <- id_test_00[which(!is.na(data[id_test_00,pos] - CO2_pred[id_test_00]))]
  txt_00 <- paste(strftime(min(data$date[id_ok]),"%d %b",tz="UTC")," - ",
                  strftime(max(data$date[id_ok]),"%d %b",tz="UTC"),sep="")
  
  id_ok  <- id_test_01[which(!is.na(data[id_test_01,pos] - CO2_pred[id_test_01]))]
  txt_01 <- paste(strftime(min(data$date[id_ok]),"%d %b",tz="UTC")," - ",
                  strftime(max(data$date[id_ok]),"%d %b",tz="UTC"),sep="")
  
  id_ok  <- id_test_02[which(!is.na(data[id_test_02,pos] - CO2_pred[id_test_02]))]
  txt_02 <- paste(strftime(min(data$date[id_ok]),"%d %b",tz="UTC")," - ",
                  strftime(max(data$date[id_ok]),"%d %b",tz="UTC"),sep="")
  
  mainStr <- target
  
  if(target == "ZBLG_LP8_CO2_BSL"){
    mainStr <- "Bullingerhof 5 [LP8]"
  }
  if(target == "ZMAN_LP8_CO2_BSL"){
    mainStr <- "Manessestrasse 34 [LP8]"
  }
  if(target == "ZSBS_LP8_CO2_BSL"){
    mainStr <- "Seebahnstrasse 229 [LP8]"
  }
  if(target == "ZSCH_LP8_CO2_BSL"){
    mainStr <- "Schimmelstrasse [LP8]"
  }
  if(target == "ZUE_LP8_CO2_BSL"){
    mainStr <- "Kaserne [LP8]"
  }
  if(target == "ZUE_CO2_BSL"){
    mainStr <- "Kaserne [HPP]"
  }
  if(target == "ZSCH_CO2_BSL"){
    mainStr <- "Schimmelstrasse [HPP]"
  }
  
  ylabStr <- expression(paste("Measurement - Prediction CO"[2]*" [ppm]"))
  ylabStr <- expression(paste("Messung CO"[2]*" - Vorhersage CO"[2]*" [ppm]"))
  
  boxplot(tmp,names=c("Before LD","LD Ph 1","LD Ph 2"),col="slategray",ylab=ylabStr,main=mainStr,
          pch=16,cex=0.5,cex.lab=1.5,cex.axis=1.5,cex.main=1.5,ylim=c(-40,40))
  
  lines(c(-5.0,5.0),c(0   ,0),  lwd=2,col=2)
  lines(c( 1.5,1.5),c(-100,100),lwd=2,col=2,lty=5)
  lines(c( 2.5,2.5),c(-100,100),lwd=2,col=2,lty=5)
  
  text(x = 1,y = -35,labels = txt_00,cex=1.0)
  text(x = 2,y = -35,labels = txt_01,cex=1.0)
  text(x = 3,y = -35,labels = txt_02,cex=1.0)
  
  dev.off()
  par(def_par)
  
  rm(tmp,id_ok,txt_00,txt_01)
  gc()
  
  
  # Boxplots residuals Ext
  
  figname <- paste(resultdir,target,"_BP_res_EXT.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=12, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.75,0.1),mfrow=c(1,1))
  
  tmp     <- matrix(NA,nrow = max(c(N_TEST_00,N_TEST_01,N_TEST_02)),ncol = 12)
  
  id_MoFr <- id_test_00[which(data$dow2[id_test_00]%in%c(1:5))]
  id_Sat  <- id_test_00[which(data$dow2[id_test_00]==6)]
  id_Sun  <- id_test_00[which(data$dow2[id_test_00]==7)]
  
  tmp[1:N_TEST_00,      1] <- data[id_test_00,pos] - CO2_pred[id_test_00]
  tmp[1:length(id_MoFr),2] <- data[id_MoFr,pos]    - CO2_pred[id_MoFr]
  tmp[1:length(id_Sat), 3] <- data[id_Sat,pos]     - CO2_pred[id_Sat]
  tmp[1:length(id_Sun), 4] <- data[id_Sun,pos]     - CO2_pred[id_Sun]
  
  id_MoFr <- id_test_01[which(data$dow2[id_test_01]%in%c(1:5))]
  id_Sat  <- id_test_01[which(data$dow2[id_test_01]==6)]
  id_Sun  <- id_test_01[which(data$dow2[id_test_01]==7)]
  
  tmp[1:N_TEST_01,      5] <- data[id_test_01,pos] - CO2_pred[id_test_01]
  tmp[1:length(id_MoFr),6] <- data[id_MoFr,pos]    - CO2_pred[id_MoFr]
  tmp[1:length(id_Sat), 7] <- data[id_Sat,pos]     - CO2_pred[id_Sat]
  tmp[1:length(id_Sun), 8] <- data[id_Sun,pos]     - CO2_pred[id_Sun]
  
  id_MoFr <- id_test_02[which(data$dow2[id_test_02]%in%c(1:5))]
  id_Sat  <- id_test_02[which(data$dow2[id_test_02]==6)]
  id_Sun  <- id_test_02[which(data$dow2[id_test_02]==7)]
  
  tmp[1:N_TEST_02,      9]  <- data[id_test_02,pos] - CO2_pred[id_test_02]
  tmp[1:length(id_MoFr),10] <- data[id_MoFr,pos]    - CO2_pred[id_MoFr]
  tmp[1:length(id_Sat), 11] <- data[id_Sat,pos]     - CO2_pred[id_Sat]
  tmp[1:length(id_Sun), 12] <- data[id_Sun,pos]     - CO2_pred[id_Sun]
  
  txt_00 <- paste("N=",sprintf("%.0f",N_TEST_00),sep="")
  txt_01 <- paste("N=",sprintf("%.0f",N_TEST_01),sep="")
  txt_02 <- paste("N=",sprintf("%.0f",N_TEST_02),sep="")
  
  mainStr <- target
  ylabStr <- expression(paste("Measurement - Prediction CO"[2]*" [ppm]"))
  
  boxplot(tmp,names=c("NL ALL","NL MoFr","NL Sat","NL Sun",
                      "LD1 ALL","LD1 MoFr","LD1 Sat","LD1 Sun",
                      "LD2 ALL","LD2 MoFr","LD2 Sat","LD2 Sun"),col="slategray",ylab=ylabStr,main=mainStr,
          pch=16,cex=0.5,cex.lab=1.5,cex.axis=1.5,cex.main=1.5,ylim=c(-40,40))
  
  lines(c( -5,15), c(0,0),lwd=2,col=2)
  lines(c(4.5,4.5),c(-100,100),lwd=2,col=2,lty=5)
  lines(c(8.5,8.5),c(-100,100),lwd=2,col=2,lty=5)
  
  # text(x = 2.5,y = -35,labels = txt_00,cex=1.5)
  # text(x = 6.5,y = -35,labels = txt_01,cex=1.5)
  
  dev.off()
  par(def_par)
  
  rm(tmp,id_MoFr,id_Sat,id_Sun,txt_00,txt_01)
  gc()
  
  
  # Boxplots
  
  for(mode in c("ABS","DIFF")){
    for(dow_set in c("MoFr","Sat","Sun")){
      if(dow_set == "MoFr"){
        selected_dows <- 1:5
      }
      if(dow_set == "Sat"){
        selected_dows <- 6
      }
      if(dow_set == "Sun"){
        selected_dows <- 7
      }
      
      tmp_00 <- matrix(NA,ncol=24,nrow=dim(data)[1])
      tmp_01 <- matrix(NA,ncol=24,nrow=dim(data)[1])
      
      for(hh in 0:23){
        
        id <- id_test_00[which(data$dow2[id_test_00] %in% selected_dows
                               & data$hour[id_test_00] == hh
                               & data$date[id_test_00]< strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC"))]
        
        if(length(id)>0){
          if(mode=="ABS"){
            tmp_00[1:length(id),hh+1] <- data[id,pos]
          }
          if(mode=="DIFF"){
            tmp_00[1:length(id),hh+1] <- diff[id]
          }
        }
        
        id <- id_test_03[which(data$dow2[id_test_03] %in% selected_dows
                               & data$hour[id_test_03] == hh
                               & data$date[id_test_03]>=strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC"))]
        
        if(length(id)>0){
          if(mode=="ABS"){
            tmp_01[1:length(id),hh+1] <- data[id,pos]
          }
          if(mode=="DIFF"){
            tmp_01[1:length(id),hh+1] <- diff[id]
          }
        }
      }
      
      #
      
      figname <- paste(resultdir,target,"_",mode,"_",dow_set,"_BP.pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1,1,0.75,0.1),mfrow=c(1,2))
      
      if(mode=="DIFF"){
        min_y <- min(c(min(rbind(tmp_00,tmp_01),na.rm=T),-15))
        max_y <- max(c(max(rbind(tmp_00,tmp_01),na.rm=T), 15))
        min_y <- -30
        max_y <-  30 
      }
      if(mode=="ABS"){
        min_y <- min(c(min(rbind(tmp_00,tmp_01),na.rm=T),400))
        max_y <- max(c(max(rbind(tmp_00,tmp_01),na.rm=T)))
      }
      
      
      mainStr <- target
      xlabStr <- "Hour of day [CET]"
      
      if(mode=="DIFF"){
        ylabStr <- "Test - Measurement CO2 [ppm]"
      }
      if(mode=="ABS"){
        ylabStr <- "Measurement CO2 [ppm]"
      }
      
      if(dow_set == "MoFr"){
        
        boxplot(tmp_00,at=0:23,names=0:23,main=mainStr,ylim=c(min_y,max_y),xlab=xlabStr, ylab=ylabStr,
                col="orange",pch=16,cex=0.5,xaxt="n",cex.lab=1.5,cex.axis=1.5,cex.main=1.5)
        axis(side = 1,at = seq(0,23,6),labels = seq(0,23,6),cex.lab=1.5,cex.axis=1.5)
        
        if(any(!is.na(tmp_01))){
          boxplot(tmp_01,at=0:23,names=0:23,main=mainStr,ylim=c(min_y,max_y),xlab=xlabStr, ylab=ylabStr,
                  col="green",pch=16,cex=0.5,xaxt="n",cex.lab=1.5,cex.axis=1.5,cex.main=1.5)
          axis(side = 1,at = seq(0,23,6),labels = seq(0,23,6),cex.lab=1.5,cex.axis=1.5)
        }else{
          plot.new()
        }
        
      }else{
        
        x <- matrix(rep(0:23,each=dim(tmp_00)[1]),nrow =dim(tmp_00)[1])
        
        plot(x,tmp_00,main=mainStr,ylim=c(min_y,max_y),xlab=xlabStr, ylab=ylabStr,
             col="orange",pch=16,cex=1,xaxt="n",cex.lab=1.5,cex.axis=1.5,cex.main=1.5)
        
        for(ii in 1:24){
          lines(c(ii-1,ii-1),c(min(tmp_00[,ii],na.rm=T),max(tmp_00[,ii],na.rm=T)),col="orange",lwd=2,lty=1)
        }
        
        axis(side = 1,at = seq(0,23,6),labels = seq(0,23,6),cex.lab=1.5,cex.axis=1.5)
        
        if(any(!is.na(tmp_01))){
          x <- matrix(rep(0:23,each=dim(tmp_01)[1]),nrow =dim(tmp_01)[1])+0.25
          
          plot(x,tmp_01,main=mainStr,ylim=c(min_y,max_y),xlab=xlabStr, ylab=ylabStr,
               col="green",pch=16,cex=1,xaxt="n",cex.lab=1.5,cex.axis=1.5,cex.main=1.5)
          
          for(ii in 1:24){
            lines(c(ii-1,ii-1)+0.25,c(min(tmp_01[,ii],na.rm=T),max(tmp_01[,ii],na.rm=T)),col="green",lwd=2,lty=1)
          }
          
          axis(side = 1,at = seq(0,23,6),labels = seq(0,23,6),cex.lab=1.5,cex.axis=1.5)
          
        }else{
          plot.new()
        }
      }
      
      
      
      
      if(mode=="DIFF"){
        lines(c(-10,30),c(0,0),lwd=2,col=2)
      }
      
      par(family="mono")
      legend("bottomright",legend=c("Before LD","After LD"),col=c("orange","green"),pch=15,bg="white",cex=1.5)
      par(family="")
      
      dev.off()
      par(def_par)
      
    }
  }
}

###


data$date <- strftime(data$date,"%Y-%m-%d %H:%M:%S",tz="UTC")
write.table(x = data,file = paste(resultdir,"data.csv",sep=""),col.names = T,row.names = F,sep=";")

# stop()

###

write.table(x = statistics,file = paste(resultdir,"statistics.csv",sep=""),col.names = T,row.names = F,sep=";")

###

data      <- read.table(file = paste(resultdir,"data.csv",sep=""), sep=";",header=T,as.is=T)

data$date <- strptime(data$date,"%Y-%m-%d %H:%M:%S",tz="UTC")


#


for(ith_SensorSet in 1:4){
  
  if(ith_SensorSet == 1){
    SensorSet <- c("HAE_NABEL_CO2","RIG_NABEL_CO2","PAY_NABEL_CO2","DUE_NABEL_CO2")
  }
  
  if(ith_SensorSet == 2){
    SensorSet <- c("ZUE_CO2","ZSCH_CO2","ZHBR_CO2","RECK_CO2","ALBS_CO2","ESMO_CO2","DUE_NABEL_CO2")
  }
  
  if(ith_SensorSet == 3){
    SensorSet <- c("ZUE_CO2","ZUE_LP8_CO2","ZSCH_CO2","ZSCH_LP8_CO2",
                   "ZMAN_LP8_CO2","ZSBS_LP8_CO2","ZBLG_LP8_CO2","ZDLT_LP8_CO2")
  }
  
  if(ith_SensorSet == 4){
    SensorSet <- c("ZUE_CO2_BSL","ZUE_LP8_CO2_BSL","ZSCH_CO2_BSL","ZSCH_LP8_CO2_BSL",
                   "ZMAN_LP8_CO2_BSL","ZSBS_LP8_CO2_BSL","ZBLG_LP8_CO2_BSL","ZDLT_LP8_CO2_BSL")
  }
  
  n_SensorSet <- length(SensorSet)
  
  pos_meas <- NULL
  pos_pred <- NULL
  
  for(ith_sensor in 1:n_SensorSet){
    id <- which(colnames(data)==paste(SensorSet[ith_sensor],sep=""))
    if(length(id)==1){
      pos_meas <- c(pos_meas,id)
    }else{
      stop()
    }
    
    id <- which(colnames(data)==paste(SensorSet[ith_sensor],"_PRED",sep=""))
    if(length(id)==1){
      pos_pred <- c(pos_pred,id)
    }else{
      stop()
    }
  }
  
  # Measurements
  
  figname <- paste(resultdir,"SensorSet_",sprintf("%02.0f",ith_SensorSet),"_MEASUREMENTS_TS.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
  
  xlabStr <- "Date"
  ylabStr <- expression(paste("CO"[2]*" [ppm]"))
  
  
  lab_dates <- seq(strptime("20191230000000","%Y%m%d%H%M%S",tz="UTC"),
                   strptime("20201230000000","%Y%m%d%H%M%S",tz="UTC"),by="week")
  
  lab_dates_str <- strftime(lab_dates, "%d/%m",tz="UTC")
  
  yrange <- c(390,600)
  
  if(ith_SensorSet == 4){
    yrange <- c(-50,150)
  }
  
  plot(c(strptime("20200301000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20200410000000","%Y%m%d%H%M%S",tz="UTC")),
       c(NA,NA),ylim=yrange,main="",xlab=xlabStr, ylab=ylabStr,
       cex.main=1.5,cex.lab=1.5,cex.axis=1.5,xaxt="n")
  
  axis(side = 1,at = lab_dates,labels = lab_dates_str,cex.lab=1.5,cex.axis=1.5)
  
  id_test <- which(data$date>=strptime("20200301000000","%Y%m%d%H%M%S",tz="UTC"))
  
  for(ith_sensor in 1:n_SensorSet){
    lines(data$date[id_test],data[id_test,pos_meas[ith_sensor]],
          col=rainbow(n_SensorSet)[ith_sensor])
  }
  
  lines(c(strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC")),
        c(-1e9,1e9),col=1,lwd=2,lty=5)
  
  leg_str <- gsub(pattern = "_CO2",replacement = "",x = SensorSet)
  
  par(family="mono")
  legend("topright",legend=leg_str,col=rainbow(n_SensorSet),lwd=1,lty=1,bg="white",cex=0.75)
  par(family="")
  
  dev.off()
  par(def_par)
  
  
  # Residuals
  
  figname <- paste(resultdir,"SensorSet_",sprintf("%02.0f",ith_SensorSet),"_RESIDUALS_TS.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
  
  xlabStr <- "Date"
  ylabStr <- expression(paste("Meas minus pred CO"[2]*" [ppm]"))
  
  
  lab_dates <- seq(strptime("20191230000000","%Y%m%d%H%M%S",tz="UTC"),
                   strptime("20201230000000","%Y%m%d%H%M%S",tz="UTC"),by="week")
  
  lab_dates_str <- strftime(lab_dates, "%d/%m",tz="UTC")
  
  plot(c(strptime("20200301000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20200410000000","%Y%m%d%H%M%S",tz="UTC")),
       c(NA,NA),ylim=c(-50,50),main="",xlab=xlabStr, ylab=ylabStr,
       cex.main=1.5,cex.lab=1.5,cex.axis=1.5,xaxt="n")
  
  axis(side = 1,at = lab_dates,labels = lab_dates_str,cex.lab=1.5,cex.axis=1.5)
  
  id_test <- which(data$date>=strptime("20200301000000","%Y%m%d%H%M%S",tz="UTC"))
  
  for(ith_sensor in 1:n_SensorSet){
    lines(data$date[id_test],data[id_test,pos_meas[ith_sensor]]-data[id_test,pos_pred[ith_sensor]],
          col=rainbow(n_SensorSet)[ith_sensor])
  }
  
  lines(c(strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC")),
        c(-1e9,1e9),col=1,lwd=2,lty=5)
  
  lines(c(strptime("20000101000000","%Y%m%d%H%M%S",tz="UTC"),strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC")),
        c(0,0),col=1,lwd=2,lty=1)
  
  leg_str <- gsub(pattern = "_CO2",replacement = "",x = SensorSet)
  
  par(family="mono")
  legend("topright",legend=leg_str,col=rainbow(n_SensorSet),lwd=1,lty=1,bg="white",cex=0.75)
  par(family="")
  
  dev.off()
  par(def_par)
  
}




# tmp <- matrix(NA,ncol=24,nrow=dim(data)[1])
# 
# for(i in 1:24){
#   id <- which(!is.na(data$AnzFahrzeuge_Schimmelstrasse) & data$hour==(i-1) & data$dow2%in%c(1) & as.numeric(strftime(data$date,"%Y",tz="UTC"))==2018)
#   n_id <- length(id)
#   
#   tmp[1:n_id,i] <- data$AnzFahrzeuge_Schimmelstrasse[id]
# }
# 
# boxplot(tmp)
# 
# id <- which(data$AnzFahrzeuge_Schimmelstrasse<1250 & data$hour==11 & data$dow2==1)

# ok  <- (!is.na(data$DUE_NABEL_CO2_BSL)
#         & !is.na(data$DUE_RA)
#         & !is.na(data$NABDUE_WS_3h)
#         & !is.na(data$RECK_CO2_BSL)
#         & !is.na(data$RECK_RA)
#         & !is.na(data$REH_WS_3h))
# 
# def_par <- par()
# pdf(file = "H:/RA_CO2.pdf", width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
# par(mai=c(1,1,0.75,0.1),mfrow=c(1,2))
# 
# ccol  <- (data$NABDUE_WS_3h %/% 0.5)+1
# ccol[ccol>10] <- 10
# plot(data$DUE_RA[ok],data$DUE_NABEL_CO2[ok],pch=16,cex=0.5,cex.lab=1.5,cex.axis=1.5,
#      ylab="DUE CO2 [ppm]",xlab="Respiration",xlim=c(0,10),ylim=c(380,650),col=heat.colors(10)[ccol[ok]])
# 
# par(family="mono")
# legend("topright",legend=paste(sprintf("%.2f",seq(0.5,10*.5,.5)/3),"m/s"),title="MEAN WS 3h",
#        bg="white",col=heat.colors(10)[1:10],pch=16,cex=1)
# par(family="")
# 
# ccol  <- (data$REH_WS_3h %/% 0.5)+1
# ccol[ccol>10] <- 10
# plot(data$RECK_RA[ok],data$RECK_CO2[ok],pch=16,cex=0.5,cex.lab=1.5,cex.axis=1.5,
#      ylab="RECK CO2 [ppm]",xlab="Respiration",xlim=c(0,10),ylim=c(380,650),col=heat.colors(10)[ccol[ok]])
# 
# par(family="mono")
# legend("topright",legend=paste(sprintf("%.2f",seq(0.25,10*.5,.5)/3),"m/s"),title="MEAN WS 3h",
#        bg="white",col=heat.colors(10)[1:10],pch=16,cex=1)
# par(family="")
# 
# dev.off()
# par(def_par)

# 
# ccol <- heat.colors(9)
# cc   <- 0
# for(i in seq(0.5,4.0,0.5)){
#   id <- which(data$NABDUE_WS_3h>=(i-0.5) & data$NABDUE_WS_3h <= i & ok)
#   cc <- cc + 1
#   points(data$DUE_NABEL_CO2_BSL[id], data$DUE_RA[id],pch=16,cex=0.5,col=ccol[cc])
# }
# 
# points(data$DUE_NABEL_CO2_BSL[id_02], data$DUE_RA[id_02],pch=16,cex=0.5,col=4)
# # 
# # 
# # 
# # id_01 <- which(data$REH_WS_3h<=2.5 & ok)
# # id_02 <- which(data$REH_WS_3h >2.5 & ok)
# # 
# # plot(data$RECK_CO2_BSL[ok],          data$RECK_RA[ok],    pch=16,cex=0.5,xlim=c(-20,300))
# # points(data$RECK_CO2_BSL[id_01], data$RECK_RA[id_01],pch=16,cex=0.5,col=2)
# # points(data$RECK_CO2_BSL[id_02], data$RECK_RA[id_02],pch=16,cex=0.5,col=4)
