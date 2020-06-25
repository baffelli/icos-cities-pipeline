# Plot_HPP_CO2.r
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

## source


source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

### ------------------------------------------------------------------------------------------------------------------------------

## directories

resultdir <- "/project/CarboSense/Carbosense_Network/TS_CO2_HPP_Measurements/"

### ------------------------------------------------------------------------------------------------------------------------------

# Table with processed CO2 measurements

ProcMeasDBTableName <- "CarboSense_HPP_CO2"

### ------------------------------------------------------------------------------------------------------------------------------

if(!dir.exists(resultdir)){
  dir.create((gsub(pattern = "/$",replacement = "",resultdir)))
}

### ------------------------------------------------------------------------------------------------------------------------------

query_str       <- paste("SELECT DISTINCT SensorUnit_ID FROM ",ProcMeasDBTableName,";",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_CO2_SU      <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

#

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_location    <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

#

query_str       <- paste("SELECT * FROM Deployment WHERE SensorUnit_ID BETWEEN 426 AND 445 AND LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1');",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_deployment  <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_deployment$Date_UTC_from  <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to    <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

tbl_deployment$timestamp_from <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units = "secs",tz="UTC"))
tbl_deployment$timestamp_to   <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units = "secs",tz="UTC"))

### ------------------------------------------------------------------------------------------------------------------------------

# Combined time series of several HPPs

if(T){
  for(ith_siteSet in 1:4){
    
    ## Site sets 
    
    if(ith_siteSet == 1){
      siteSet_name    <- "CH"
      siteSet_cantons <- c("BE","BS","LU","SO","TG","TI","VS","ZH","TG")
      add_sites       <- c("AAA")
      rm_sites        <- c("DUE1","DUE2","DUE3","DUE4","DUE5")
    }
    
    if(ith_siteSet == 2){
      siteSet_name    <- "TI"
      siteSet_cantons <- c("TI")
      add_sites       <- c("AAA")
      rm_sites        <- c("DUE1","DUE2","DUE3","DUE4","DUE5")
    }
    
    if(ith_siteSet == 3){
      siteSet_name    <- "CH-PL"
      siteSet_cantons <- c("BE","BS","LU","SO","TG","VS","ZH","TG")
      add_sites       <- c("AAA")
      rm_sites        <- c("DUE1","DUE2","DUE3","DUE4","DUE5")
    }
    if(ith_siteSet == 4){
      siteSet_name    <- "CH-PL-ALT"
      siteSet_cantons <- c("ZH")
      add_sites       <- c("BNTG","CHRI","SEMP","FROB","ESMO","SAVE","ALBS")
      rm_sites        <- c("DUE1","DUE2","DUE3","DUE4","DUE5","ZHBR","ZUE","ZSCH","RECK")
    }
    
    SiteSet      <- sort(unique(tbl_location$LocationName[which((tbl_location$Canton%in%siteSet_cantons
                                                                 & !tbl_location$LocationName%in%rm_sites)
                                                                | tbl_location$LocationName%in%add_sites)]))
    figname_base <- paste(resultdir,"/0NET_",siteSet_name,sep="")
    
    
    # Data import
    
    query_str       <- paste("SELECT * FROM ",ProcMeasDBTableName," WHERE LocationName IN ",paste("('",paste(SiteSet,collapse = "','"),"') and Valve=0",sep=""),";",sep="")
    drv             <- dbDriver("MySQL")
    con             <- dbConnect(drv, group="CarboSense_MySQL")
    res             <- dbSendQuery(con, query_str)
    tbl_CO2         <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    # sort data corresponding to timestamp
    
    tbl_CO2 <- tbl_CO2[order(tbl_CO2$timestamp),]
    
    # set -999 to NA
    
    id_setToNA <- which(tbl_CO2$CO2_CAL == -999)
    if(length(id_setToNA)>0){
      tbl_CO2$CO2_CAL[id_setToNA] <- NA
    }
    
    id_setToNA <- which(tbl_CO2$CO2_CAL_DRY == -999)
    if(length(id_setToNA)>0){
      tbl_CO2$CO2_CAL_DRY[id_setToNA] <- NA
    }
    
    id_setToNA <- which(tbl_CO2$CO2_CAL_ADJ_DRY == -999)
    if(length(id_setToNA)>0){
      tbl_CO2$CO2_CAL_ADJ[id_setToNA] <- NA
    }
    
    id_setToNA <- which(tbl_CO2$CO2_CAL_ADJ == -999)
    if(length(id_setToNA)>0){
      tbl_CO2$CO2_CAL_ADJ_DRY[id_setToNA] <- NA
    }
    
    rm(id_setToNA)
    gc()
    
    # Get site information
    
    u_sensorAtLocation   <- unique(data.frame(SensorUnit_ID=tbl_CO2$SensorUnit_ID,LocationName=tbl_CO2$LocationName,Height=NA,leg_str=NA,color=NA))
    n_u_sensorAtLocation <- dim(u_sensorAtLocation)[1]
    
    for(ith_SenAtLoc in 1:n_u_sensorAtLocation){
      
      id <- which(u_sensorAtLocation$LocationName[ith_SenAtLoc]==tbl_location$LocationName)
      
      u_sensorAtLocation$Height[ith_SenAtLoc]  <- tbl_location$h[id]
      u_sensorAtLocation$leg_str[ith_SenAtLoc] <- paste(sprintf("%-6s",tbl_location$LocationName[id])," ",sprintf("%4.0f",tbl_location$h[id]),"m")
    }
    
    u_sensorAtLocation       <- u_sensorAtLocation[order(u_sensorAtLocation$Height),]
    
    #
    
    u_timestamps        <- sort(unique(tbl_CO2$timestamp))
    n_u_timestamps      <- length(u_timestamps)
    
    u_dates             <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + u_timestamps
    
    #
    
    yyy_CO2_CAL         <- matrix(NA,ncol=n_u_sensorAtLocation,nrow=n_u_timestamps)
    yyy_CO2_CAL_DRY     <- matrix(NA,ncol=n_u_sensorAtLocation,nrow=n_u_timestamps)
    yyy_CO2_CAL_ADJ     <- matrix(NA,ncol=n_u_sensorAtLocation,nrow=n_u_timestamps)
    yyy_CO2_CAL_ADJ_DRY <- matrix(NA,ncol=n_u_sensorAtLocation,nrow=n_u_timestamps)
    yyy_H2O             <- matrix(NA,ncol=n_u_sensorAtLocation,nrow=n_u_timestamps)
    yyy_T               <- matrix(NA,ncol=n_u_sensorAtLocation,nrow=n_u_timestamps)
    yyy_RH              <- matrix(NA,ncol=n_u_sensorAtLocation,nrow=n_u_timestamps)
    
    #
    
    for(ith_SenAtLoc in 1:n_u_sensorAtLocation){
      
      id_location  <- which(tbl_location$LocationName==u_sensorAtLocation$LocationName[ith_SenAtLoc])
      
      id_loc       <- which(tbl_CO2$LocationName==u_sensorAtLocation$LocationName[ith_SenAtLoc] & tbl_CO2$SensorUnit_ID==u_sensorAtLocation$SensorUnit_ID[ith_SenAtLoc])
      n_id_loc     <- length(id_loc)
      
      CO2_CAL_loc         <- tbl_CO2$CO2_CAL[id_loc]
      CO2_CAL_DRY_loc     <- tbl_CO2$CO2_CAL_DRY[id_loc]
      CO2_CAL_ADJ_loc     <- tbl_CO2$CO2_CAL_ADJ[id_loc]
      CO2_CAL_ADJ_DRY_loc <- tbl_CO2$CO2_CAL_ADJ_DRY[id_loc]
      H2O_loc             <- tbl_CO2$H2O[id_loc]
      T_loc               <- tbl_CO2$T[id_loc]
      RH_loc              <- tbl_CO2$RH[id_loc]
      
      id_AB <- which(u_timestamps%in%tbl_CO2$timestamp[id_loc])
      
      yyy_CO2_CAL[id_AB,ith_SenAtLoc]         <- CO2_CAL_loc
      yyy_CO2_CAL_DRY[id_AB,ith_SenAtLoc]     <- CO2_CAL_DRY_loc
      yyy_CO2_CAL_ADJ[id_AB,ith_SenAtLoc]     <- CO2_CAL_ADJ_loc
      yyy_CO2_CAL_ADJ_DRY[id_AB,ith_SenAtLoc] <- CO2_CAL_ADJ_DRY_loc
      yyy_H2O[id_AB,ith_SenAtLoc]             <- H2O_loc
      yyy_T[id_AB,ith_SenAtLoc]               <- T_loc
      yyy_RH[id_AB,ith_SenAtLoc]              <- RH_loc
    }
    
    # 
    
    xlabString <- "Date" 
    
    figname    <- paste(figname_base,"_TS_CO2_CAL.pdf",sep="")
    ylabString <- expression(paste("CO"[2]*" [ppm]"))
    plot_ts_NETWORK(figname,u_dates,yyy_CO2_CAL,matrix(1,ncol=dim(yyy_CO2_CAL)[2],nrow=dim(yyy_CO2_CAL)[1]),"week",NULL,c(350,650),xlabString,ylabString,u_sensorAtLocation$leg_str,5*60)
    
    figname    <- paste(figname_base,"_TS_CO2_CAL_DRY.pdf",sep="")
    ylabString <- expression(paste("CO"[2]*" DRY [ppm]"))
    plot_ts_NETWORK(figname,u_dates,yyy_CO2_CAL_DRY,matrix(1,ncol=dim(yyy_CO2_CAL_DRY)[2],nrow=dim(yyy_CO2_CAL_DRY)[1]),"week",NULL,c(350,650),xlabString,ylabString,u_sensorAtLocation$leg_str,5*60)
    
    figname    <- paste(figname_base,"_TS_CO2_CAL_ADJ.pdf",sep="")
    ylabString <- expression(paste("CO"[2]*" [ppm] (ADJUSTED)"))
    plot_ts_NETWORK(figname,u_dates,yyy_CO2_CAL_ADJ,matrix(1,ncol=dim(yyy_CO2_CAL_ADJ)[2],nrow=dim(yyy_CO2_CAL_ADJ)[1]),"week",NULL,c(350,650),xlabString,ylabString,u_sensorAtLocation$leg_str,5*60)
    
    figname    <- paste(figname_base,"_TS_CO2_CAL_ADJ_DRY.pdf",sep="")
    ylabString <- expression(paste("CO"[2]*" DRY [ppm] (ADJUSTED)"))
    plot_ts_NETWORK(figname,u_dates,yyy_CO2_CAL_ADJ_DRY,matrix(1,ncol=dim(yyy_CO2_CAL_ADJ_DRY)[2],nrow=dim(yyy_CO2_CAL_ADJ_DRY)[1]),"week",NULL,c(350,650),xlabString,ylabString,u_sensorAtLocation$leg_str,5*60)
    
    figname    <- paste(figname_base,"_TS_H2O.pdf",sep="")
    ylabString <- expression(paste("H2O [VOL-%]"))
    plot_ts_NETWORK(figname,u_dates,yyy_H2O,matrix(1,ncol=dim(yyy_H2O)[2],nrow=dim(yyy_H2O)[1]),"week",NULL,c(0,3),xlabString,ylabString,u_sensorAtLocation$leg_str,5*60)
    
    figname    <- paste(figname_base,"_TS_T.pdf",sep="")
    ylabString <- expression(paste("T [deg C]"))
    plot_ts_NETWORK(figname,u_dates,yyy_T,matrix(1,ncol=dim(yyy_T)[2],nrow=dim(yyy_T)[1]),"week",NULL,c(-20,50),xlabString,ylabString,u_sensorAtLocation$leg_str,5*60)
    
    figname    <- paste(figname_base,"_TS_RH.pdf",sep="")
    ylabString <- expression(paste("RH [%]"))
    plot_ts_NETWORK(figname,u_dates,yyy_RH,matrix(1,ncol=dim(yyy_RH)[2],nrow=dim(yyy_RH)[1]),"week",NULL,c(-5,105),xlabString,ylabString,u_sensorAtLocation$leg_str,5*60)
    
  }
}

### ------------------------------------------------------------------------------------------------------------------------------

# Individual time series


for(ith_depl in 1:dim(tbl_deployment)[1]){
  
  #
  
  DESC <- paste(tbl_deployment$LocationName[ith_depl],"_",tbl_deployment$SensorUnit_ID[ith_depl],"_",strftime(tbl_deployment$Date_UTC_from[ith_depl],"%y%m%d",sep=""),sep="")
  
  #
  
  query_str       <- paste("SELECT * FROM ",ProcMeasDBTableName," WHERE SensorUnit_ID=",tbl_deployment$SensorUnit_ID[ith_depl]," and LocationName = '",tbl_deployment$LocationName[ith_depl],"' and timestamp >= ",tbl_deployment$timestamp_from[ith_depl]," and timestamp <= ",tbl_deployment$timestamp_to[ith_depl],";",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  data            <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  if(dim(data)[1]==0){
    next
  }
  
  data      <- data[order(data$timestamp),]
  data$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
  
  # Compare interpolated pressure / HPP pressure
  
  query_str       <- paste("SELECT * FROM PressureInterpolation WHERE LocationName='",tbl_deployment$LocationName[ith_depl],"' and timestamp >= ",tbl_deployment$timestamp_from[ith_depl]," and timestamp <= ",tbl_deployment$timestamp_to[ith_depl],";",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tbl_pressure    <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  query_str        <- paste("SELECT * FROM PressureParameter WHERE timestamp >= ",tbl_deployment$timestamp_from[ith_depl]," and timestamp <= ",tbl_deployment$timestamp_to[ith_depl],";",sep="")
  drv              <- dbDriver("MySQL")
  con              <- dbConnect(drv, group="CarboSense_MySQL")
  res              <- dbSendQuery(con, query_str)
  tbl_pressure_INT <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  #
  
  tbl_pressure          <- merge(tbl_pressure,tbl_pressure_INT,by="timestamp")
  tbl_pressure          <- tbl_pressure[order(tbl_pressure$timestamp),]
  tbl_pressure$pressure <- tbl_pressure$pressure*exp(-tbl_deployment$HeightAboveGround[ith_depl]/tbl_pressure$height)
  tbl_pressure$date     <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_pressure$timestamp
  
  #
  
  tmp               <- data.frame(date         = data$date,
                                  hpp_pressure = data$Pressure,
                                  stringsAsFactors = F)
  
  tmp               <- timeAverage(mydata = tmp,
                                   avg.time = "10 min",
                                   statistic = "mean",
                                   start.date = strptime(strftime(min(tmp$date),"%Y%m%d%H0000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
  
  tbl_pressure      <- merge(x = tbl_pressure,y = tmp,all=T,by="date")
  
  #
  
  rm(tmp,tbl_pressure_INT)
  gc()
  
  # --- 
  
  figname    <- paste(resultdir,"/",DESC,"_TS_CO2.pdf",sep="")
  
  yyy        <- cbind(data$CO2_CAL,
                      data$CO2_CAL_DRY,
                      data$CO2_CAL_ADJ,
                      data$CO2_CAL_ADJ_DRY)
  
  xlabString <- "Date" 
  ylabString <- expression(paste("CO"[2]*" [ppm]"))
  legend_str <- c("CO2_CAL","CO2_CAL_DRY","CO2_CAL_ADJ","CO2_CAL_ADJ_DRY")
  plot_ts(figname,data$date,yyy,"week",NULL,c(350,650),xlabString,ylabString,legend_str)
  
  # --- 
  
  figname    <- paste(resultdir,"/",DESC,"_TS_CO2_CAL_ADJ.pdf",sep="")
  
  yyy        <- cbind(data$CO2_CAL_ADJ)
  
  xlabString <- "Date" 
  ylabString <- expression(paste("CO"[2]*" [ppm]"))
  legend_str <- c("CO2_CAL_ADJ")
  plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(350,650),xlabString,ylabString,legend_str)
  
  
  figname    <- paste(resultdir,"/",DESC,"_TS_CO2_CAL_ADJ_WEEK.pdf",sep="")
  
  yyy        <- cbind(data$CO2_CAL_ADJ)
  
  xlabString <- "Date" 
  ylabString <- expression(paste("CO"[2]*" [ppm]"))
  legend_str <- c("CO2_CAL_ADJ")
  plot_ts(figname,data$date,yyy,"week",NULL,c(350,650),xlabString,ylabString,legend_str)
  
  
  figname    <- paste(resultdir,"/",DESC,"_TS_CO2_CAL_ADJ_DAY.pdf",sep="")
  
  yyy        <- cbind(data$CO2_CAL_ADJ)
  
  xlabString <- "Date" 
  ylabString <- expression(paste("CO"[2]*" [ppm]"))
  legend_str <- c("CO2_CAL_ADJ")
  plot_ts(figname,data$date,yyy,"day",NULL,c(350,650),xlabString,ylabString,legend_str)
  
  # --- 
  
  figname    <- paste(resultdir,"/",DESC,"_TS_H2O.pdf",sep="")
  
  yyy        <- cbind(data$H2O)
  
  xlabString <- "Date" 
  ylabString <- expression(paste("H2O [Vol-%]"))
  legend_str <- c("H2O")
  plot_ts(figname,data$date,yyy,"week",NULL,c(0,3),xlabString,ylabString,legend_str)
  
  # ---  
  
  figname    <- paste(resultdir,"/",DESC,"_TS_RH.pdf",sep="")
  
  yyy        <- cbind(data$RH)
  
  xlabString <- "Date" 
  ylabString <- expression(paste("RH [%]"))
  legend_str <- c("RH")
  plot_ts(figname,data$date,yyy,"week",NULL,c(0,100),xlabString,ylabString,legend_str)
  
  # ---  
  
  figname    <- paste(resultdir,"/",DESC,"_TS_T.pdf",sep="")
  
  yyy        <- cbind(data$T)
  
  yrange     <- c(min(data$T), max(data$T))
  xlabString <- "Date" 
  ylabString <- expression(paste("T [deg C]"))
  legend_str <- c("T")
  plot_ts(figname,data$date,yyy,"week",NULL,yrange,xlabString,ylabString,legend_str)
  
  
  # ---  
  
  figname    <- paste(resultdir,"/",DESC,"_TS_pressure.pdf",sep="")
  
  yyy        <- cbind(tbl_pressure$hpp_pressure,
                      tbl_pressure$pressure)
  
  yrange     <- c(min(tbl_pressure$hpp_pressure,tbl_pressure$pressure,na.rm=T), max(tbl_pressure$hpp_pressure,tbl_pressure$pressure,na.rm=T))
  xlabString <- "Date" 
  ylabString <- expression(paste("Presure [hPa]"))
  legend_str <- c("HPP pressure","Interpolated pressure")
  plot_ts(figname,tbl_pressure$date,yyy,"week",NULL,yrange,xlabString,ylabString,legend_str)
  
}



