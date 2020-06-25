# Plot_CarboSense_CO2_TS.r
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

## source

source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

### ------------------------------------------------------------------------------------------------------------------------------

## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

# 

if(!as.integer(args[1])%in%c(0,1,2,3,20,30)){
  stop("MODEL: 1, 2, 3, 20, 30 or 0 (= FINAL)!")
}else{
  
  # Standard 
  if(as.integer(args[1])==0){
    # Directories
    resultdir           <- "/project/CarboSense/Carbosense_Network/TS_CS_SITES_FINAL/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_FINAL"
  }
  
  # Standard 
  if(as.integer(args[1])==1){
    # Directories
    resultdir           <- "/project/CarboSense/Carbosense_Network/TS_CS_SITES/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2"
  }
  
  # Test 00
  if(as.integer(args[1])==2){
    # Directories
    resultdir           <- "/project/CarboSense/Carbosense_Network/TS_CS_SITES_TEST00/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST00"
  }
  
  # Test 01
  if(as.integer(args[1])==3){
    # Directories
    resultdir           <- "/project/CarboSense/Carbosense_Network/TS_CS_SITES_TEST01/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST01"
  }
  
  # Test 00 AMT
  if(as.integer(args[1])==20){
    # Directories
    resultdir           <- "/project/CarboSense/Carbosense_Network/TS_CS_SITES_TEST00_AMT/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST00_AMT"
  }
  
  # Test 01 AMT
  if(as.integer(args[1])==30){
    # Directories
    resultdir           <- "/project/CarboSense/Carbosense_Network/TS_CS_SITES_TEST01_AMT/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST01_AMT"
  }
}

### ------------------------------------------------------------------------------------------------------------------------------

if(!dir.exists(resultdir)){
  dir.create((gsub(pattern = "/$",replacement = "",resultdir)))
}

### ------------------------------------------------------------------------------------------------------------------------------

query_str       <- paste("SELECT SensorUnit_ID FROM ",ProcMeasDBTableName,";",sep="")
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

query_str       <- paste("SELECT * FROM Deployment ",sep="")
query_str       <- paste(query_str, "WHERE LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1') ",sep="")
query_str       <- paste(query_str, "AND SensorUnit_ID BETWEEN 1010 AND 1334;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_depl        <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_depl$Date_UTC_from  <- strptime(tbl_depl$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_depl$Date_UTC_to    <- strptime(tbl_depl$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

tbl_depl$timestamp_from <- as.numeric(difftime(time1=tbl_depl$Date_UTC_from, time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"), units="secs",tz="UTC"))
tbl_depl$timestamp_to   <- as.numeric(difftime(time1=tbl_depl$Date_UTC_to,   time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"), units="secs",tz="UTC"))

tbl_depl$h              <- NA
tbl_depl$leg_str        <- NA
tbl_depl$ok             <- T

for(i in 1:dim(tbl_depl)[1]){
  id                  <- which(tbl_location$LocationName == tbl_depl$LocationName[i])
  tbl_depl$h[i]       <- tbl_location$h[id]
  tbl_depl$leg_str[i] <- paste(sprintf("%-6s",tbl_depl$LocationName[i])," ",sprintf("%4.0f",tbl_depl$h[i]),"m")
}



### ------------------------------------------------------------------------------------------------------------------------------

## Time series of multiple sensor units (grouped by different regions) 


if(F){
  
  for(year in c(2017,2018,2019,2020)){
    
    timestamp_year_from <- as.numeric(difftime(time1=strptime(paste(year,  "0101000000",sep=""),"%Y%m%d%H%M%S",tz="UTC"),
                                               time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                                               units="secs",tz="UTC"))
    
    timestamp_year_to   <- as.numeric(difftime(time1=strptime(paste(year+1,"0101000000",sep=""),"%Y%m%d%H%M%S",tz="UTC"),
                                               time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                                               units="secs",tz="UTC"))
    
    tbl_depl_year       <- tbl_depl[which(tbl_depl$timestamp_from <= timestamp_year_to & tbl_depl$timestamp_to >= timestamp_year_from),]
    
    
    #
    
    
    for(ith_siteSet in 1:9){
      
      ## Site sets 
      
      if(ith_siteSet == 1){
        siteSet_name    <- "GR"
        siteSet_cantons <- c("GR")
        add_sites       <- c("AAA")
        rm_sites        <- c("GRO")
      }
      if(ith_siteSet == 2){
        siteSet_name    <- "TI"
        siteSet_cantons <- c("TI")
        add_sites       <- c("GRO")
        rm_sites        <- c("AAA")
      }
      if(ith_siteSet == 3){
        siteSet_name    <- "VS"
        siteSet_cantons <- c("VS")
        add_sites       <- c("AAA")
        rm_sites        <- c("AAA")
      }
      if(ith_siteSet == 4){
        siteSet_name    <- "EastCH"
        siteSet_cantons <- c("SG","TG","GLA","SH","AI","AR")
        add_sites       <- c("AAA")
        rm_sites        <- c("AAA")
      }
      if(ith_siteSet == 5){
        siteSet_name    <- "ZH"
        siteSet_cantons <- c("ZH")
        add_sites       <- c("AAA")
        rm_sites        <- c("DUE1","DUE2","DUE3","DUE4","DUE5")
      }
      if(ith_siteSet == 6){
        siteSet_name    <- "InCH"
        siteSet_cantons <- c("ZG","LU","NW","OW","SZ","UR")
        add_sites       <- c("AAA")
        rm_sites        <- c("AAA")
      }
      if(ith_siteSet == 7){
        siteSet_name    <- "NCH"
        siteSet_cantons <- c("BS","BL","AG","JU")
        add_sites       <- c("AAA")
        rm_sites        <- c("AAA")
      }
      if(ith_siteSet == 8){
        siteSet_name    <- "MCH"
        siteSet_cantons <- c("BE","SO")
        add_sites       <- c("AAA")
        rm_sites        <- c("MET1")
      }
      if(ith_siteSet == 9){
        siteSet_name    <- "WCH"
        siteSet_cantons <- c("GE","VD","NE","FR")
        add_sites       <- c("AAA")
        rm_sites        <- c("AAA")
      }
      
      #
      
      SiteSet      <- sort(unique(tbl_location$LocationName[which((tbl_location$Canton%in%siteSet_cantons
                                                                   & !tbl_location$LocationName%in%rm_sites)
                                                                  | tbl_location$LocationName%in%add_sites)]))
      
      tbl_depl_year_loc <- tbl_depl_year[which(tbl_depl_year$LocationName%in%SiteSet),]
      tbl_depl_year_loc <- tbl_depl_year_loc[order(tbl_depl_year_loc$h),]
       
      
      # Data import
      
      query_str       <- paste("SELECT timestamp, SensorUnit_ID, LocationName, CO2, CO2_A, LP8_T, SHT21_T, SHT21_RH, FLAG, O_FLAG FROM ",ProcMeasDBTableName," ",sep="") 
      query_str       <- paste(query_str, "WHERE timestamp >= ",timestamp_year_from," AND timestamp <= ",timestamp_year_to," ",sep="")
      query_str       <- paste(query_str,"AND SensorUnit_ID IN ",paste("(",paste(unique(tbl_depl_year_loc$SensorUnit_ID),collapse = ","),")",sep="")," ",sep="")
      query_str       <- paste(query_str,"AND LocationName IN ",paste("('",paste(SiteSet,collapse = "','"),"')",sep=""),";",sep="")
      drv             <- dbDriver("MySQL")
      con             <- dbConnect(drv, group="CarboSense_MySQL")
      res             <- dbSendQuery(con, query_str)
      tbl_CO2         <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      # sort data corresponding to timestamp
      
      tbl_CO2$timestamp <- (tbl_CO2$timestamp%/%300)*300
      
      tbl_CO2 <- tbl_CO2[order(tbl_CO2$timestamp),]
      
      # set -999 to NA
      
      id_setToNA <- which(tbl_CO2$CO2_A == -999)
      if(length(id_setToNA)>0){
        tbl_CO2$CO2_A[id_setToNA] <- NA
      }
      
      #
      
      u_timestamps        <- sort(unique(tbl_CO2$timestamp))
      n_u_timestamps      <- length(u_timestamps)
      
      u_dates             <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + u_timestamps
      
      #
      
      yyy          <- matrix(NA,ncol=dim(tbl_depl_year_loc)[1],nrow=n_u_timestamps)
      yyy_imp      <- matrix(NA,ncol=dim(tbl_depl_year_loc)[1],nrow=n_u_timestamps)
      yyy_FLAG     <- matrix(NA,ncol=dim(tbl_depl_year_loc)[1],nrow=n_u_timestamps)
      yyy_LP8_T    <- matrix(NA,ncol=dim(tbl_depl_year_loc)[1],nrow=n_u_timestamps)
      yyy_SHT21_T  <- matrix(NA,ncol=dim(tbl_depl_year_loc)[1],nrow=n_u_timestamps)
      yyy_SHT21_RH <- matrix(NA,ncol=dim(tbl_depl_year_loc)[1],nrow=n_u_timestamps)
      
      #
      
      for(ith_depl in 1:dim(tbl_depl_year_loc)[1]){
        
        id_loc       <- which(tbl_CO2$LocationName==tbl_depl_year_loc$LocationName[ith_depl] & tbl_CO2$SensorUnit_ID==tbl_depl_year_loc$SensorUnit_ID[ith_depl])
        n_id_loc     <- length(id_loc)
        
        if(n_id_loc==0){
          tbl_depl_year_loc$ok[ith_depl] <- F
          next
        }
        
        CO2_loc      <- tbl_CO2$CO2[id_loc]
        CO2_imp_loc  <- tbl_CO2$CO2_A[id_loc]
        LP8_T_loc    <- tbl_CO2$LP8_T[id_loc]
        SHT21_RH_loc <- tbl_CO2$SHT21_RH[id_loc]
        SHT21_T_loc  <- tbl_CO2$SHT21_T[id_loc]
        FLAG_loc     <- tbl_CO2$O_FLAG[id_loc]
        
        id_AB <- which(u_timestamps%in%tbl_CO2$timestamp[id_loc])
        
        yyy[id_AB,ith_depl]          <- CO2_loc
        yyy_imp[id_AB,ith_depl]      <- CO2_imp_loc
        yyy_LP8_T[id_AB,ith_depl]    <- LP8_T_loc
        yyy_SHT21_RH[id_AB,ith_depl] <- SHT21_RH_loc
        yyy_SHT21_T[id_AB,ith_depl]  <- SHT21_T_loc
        yyy_FLAG[id_AB,ith_depl]     <- FLAG_loc
        
      }
      
      tbl_depl_year_loc <- tbl_depl_year_loc[tbl_depl_year_loc$ok,]
      
      yyy          <- yyy[,tbl_depl_year_loc$ok]
      yyy_imp      <- yyy_imp[,tbl_depl_year_loc$ok]
      yyy_FLAG     <- yyy_FLAG[,tbl_depl_year_loc$ok]
      yyy_LP8_T    <- yyy_LP8_T[,tbl_depl_year_loc$ok]
      yyy_SHT21_T  <- yyy_SHT21_T[,tbl_depl_year_loc$ok]
      yyy_SHT21_RH <- yyy_SHT21_RH[,tbl_depl_year_loc$ok]
      
      # 
      
      figname_base <- paste(resultdir,"/0NET_",year,"_",siteSet_name,sep="")
      
      #
      
      xlabString <- "Date" 
      ylabString <- expression(paste("CO"[2]*" [ppm]"))
      figname    <- paste(figname_base,"_TS.pdf",sep="")
      plot_ts_NETWORK(figname,u_dates,yyy,yyy_FLAG,"week",NULL,c(350,650),xlabString,ylabString,tbl_depl_year_loc$leg_str,15*60)
      
      figname    <- paste(figname_base,"_TS_ALL.pdf",sep="")
      plot_ts_NETWORK(figname,u_dates,yyy,matrix(1,ncol=dim(yyy)[2],nrow=dim(yyy)[1]),"week",NULL,c(350,650),xlabString,ylabString,tbl_depl_year_loc$leg_str,15*60)
      
      figname    <- paste(figname_base,"_TS_SHT21_RH.pdf",sep="")
      ylabString <- expression(paste("SHT21 RH [%]"))
      plot_ts_NETWORK(figname,u_dates,yyy_SHT21_RH,matrix(1,ncol=dim(yyy)[2],nrow=dim(yyy)[1]),"week",NULL,c(0,105),xlabString,ylabString,tbl_depl_year_loc$leg_str,15*60)
      
      figname    <- paste(figname_base,"_TS_LP8_T.pdf",sep="")
      ylabString <- expression(paste("LP8 T [deg C]"))
      plot_ts_NETWORK(figname,u_dates,yyy_LP8_T,matrix(1,ncol=dim(yyy)[2],nrow=dim(yyy)[1]),"week",NULL,c(-10,50),xlabString,ylabString,tbl_depl_year_loc$leg_str,15*60)
      
      figname    <- paste(figname_base,"_TS_SHT21_T.pdf",sep="")
      ylabString <- expression(paste("SHT21 T [deg C]"))
      plot_ts_NETWORK(figname,u_dates,yyy_SHT21_T,matrix(1,ncol=dim(yyy)[2],nrow=dim(yyy)[1]),"week",NULL,c(-20,50),xlabString,ylabString,tbl_depl_year_loc$leg_str,15*60)
      
      
      # 
      
      xlabString <- "Date" 
      ylabString <- expression(paste("CO"[2]*" [ppm]"))
      figname    <- paste(figname_base,"_TS_IMP.pdf",sep="")
      plot_ts_NETWORK(figname,u_dates,yyy_imp,yyy_FLAG,"week",NULL,c(350,650),xlabString,ylabString,tbl_depl_year_loc$leg_str,15*60)
      
      figname    <- paste(figname_base,"_TS_IMP_ALL.pdf",sep="")
      plot_ts_NETWORK(figname,u_dates,yyy_imp,matrix(1,ncol=dim(yyy)[2],nrow=dim(yyy)[1]),"week",NULL,c(350,650),xlabString,ylabString,tbl_depl_year_loc$leg_str,15*60)
    }
  }
}

### ------------------------------------------------------------------------------------------------------------------------------

## Time series of individual sensor units

if(T){
  
  u_SensorUnit_ID   <- sort(unique(tbl_CO2_SU$SensorUnit_ID))
  n_u_SensorUnit_ID <- length(u_SensorUnit_ID)
  
  for(ith_SUID in 1:n_u_SensorUnit_ID){
    
    query_str       <- paste("SELECT  timestamp, SensorUnit_ID, LocationName, CO2, CO2_A, SHT21_T, SHT21_RH, FLAG, O_FLAG FROM ",ProcMeasDBTableName," ",sep="")
    query_str       <- paste(query_str, "WHERE SensorUnit_ID=",u_SensorUnit_ID[ith_SUID],";",sep="")
    drv             <- dbDriver("MySQL")
    con             <- dbConnect(drv, group="CarboSense_MySQL")
    res             <- dbSendQuery(con, query_str)
    tbl_CO2         <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    tbl_CO2      <- tbl_CO2[order(tbl_CO2$timestamp),]
    tbl_CO2$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_CO2$timestamp
    
    #
    
    LocationNames   <- sort(unique(tbl_CO2$LocationName))  
    n_LocationNames <- length(LocationNames)
    
    #
    
    
    for(ith_LOC in 1:n_LocationNames){
      
      CO2_ok       <- rep(NA,dim(tbl_CO2)[1])
      CO2_nok      <- rep(NA,dim(tbl_CO2)[1])
      
      CO2_A_ok     <- rep(NA,dim(tbl_CO2)[1])
      CO2_A_nok    <- rep(NA,dim(tbl_CO2)[1])
      
      loc          <- tbl_CO2$LocationName==LocationNames[ith_LOC]
      
      #
      
      id_ok        <- which(tbl_CO2$O_FLAG==1 & loc)
      id_nok       <- which(tbl_CO2$O_FLAG==0 & loc)
      
      if(length(id_ok)>0){
        CO2_ok[id_ok]     <- tbl_CO2$CO2[id_ok]
        CO2_A_ok[id_ok]   <- tbl_CO2$CO2_A[id_ok]
      }
      if(length(id_nok)>0){
        CO2_nok[id_nok]   <- tbl_CO2$CO2[id_nok]
        CO2_A_nok[id_nok] <- tbl_CO2$CO2_A[id_nok]
      }
      
      # 
      
      figname <- paste(resultdir,"/",LocationNames[ith_LOC],"_",u_SensorUnit_ID[ith_SUID],"_TS.pdf",sep="")
      
      if(length(id_nok)>0){
        yyy     <- cbind(CO2_ok[loc],
                         CO2_nok[loc],
                         CO2_A_ok[loc],
                         CO2_A_nok[loc])
      }else{
        yyy     <- cbind(CO2_ok[loc],
                         CO2_A_ok[loc])
      }
      
      xlabString <- "Date" 
      ylabString <- expression(paste("CO"[2]*" [ppm]"))
      legend_str <- c("CO2","CO2 NOK","CO2_A","CO2_A NOK")
      plot_ts(figname,tbl_CO2$date[loc],yyy,"week",NULL,c(350,650),xlabString,ylabString,legend_str)
      
      # 
      
      figname <- paste(resultdir,"/",LocationNames[ith_LOC],"_",u_SensorUnit_ID[ith_SUID],"_TS_OSCALE.pdf",sep="")
      plot_ts(figname,tbl_CO2$date[loc],yyy,"week",NULL,range(yyy[which(!is.na(yyy) & yyy != -999)]),ylabString,ylabString,legend_str)
      
    }
  }
}

### ------------------------------------------------------------------------------------------------------------------------------

## Time series of individual sensor units plus reference measurements

if(T & args[1] %in% c(2,3)){
  
  if(as.integer(args[1])==1){
    anchor_events   <- read.table(file = "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/anchor_events_SU_ALL.csv",sep=";",header=T,as.is=T)
  }
  if(as.integer(args[1])==2){
    anchor_events   <- read.table(file = "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/anchor_events_SU_ALL.csv",sep=";",header=T,as.is=T)
  }
  if(as.integer(args[1])==3){
    anchor_events   <- read.table(file = "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/anchor_events_SU_ALL.csv",sep=";",header=T,as.is=T)
  }
  
  anchor_events$Date_UTC_from <- strptime(anchor_events$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  anchor_events$Date_UTC_to   <- strptime(anchor_events$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
  #
  
  query_str       <- paste("SELECT timestamp,CO2_DRY_CAL,H2O FROM NABEL_DUE WHERE timestamp>=1498867200 AND CO2_DRY_CAL!=-999 and H2O_F=1;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  data_DUE        <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  data_DUE$CO2    <- data_DUE$CO2_DRY_CAL * (1-data_DUE$H2O/100)
  
  data_DUE$date   <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_DUE$timestamp
  
  data_DUE        <- timeAverage(mydata = data_DUE,avg.time = "10 min",statistic = "mean",start.date = strptime("20170630235000","%Y%m%d%H%M%S",tz="UTC"))
  
  data_DUE        <- data_DUE[,c(which(colnames(data_DUE)=="date"),which(colnames(data_DUE)=="CO2"))]
  
  colnames(data_DUE) <- c("date","DUE_CO2")
  
  #
  
  query_str       <- paste("SELECT timestamp,CO2_DRY_CAL,H2O FROM NABEL_RIG WHERE timestamp>=1498867200 AND CO2_DRY_CAL!=-999 and H2O_F=1;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  data_RIG        <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  data_RIG$CO2    <- data_RIG$CO2_DRY_CAL * (1-data_RIG$H2O/100)
  
  data_RIG$date   <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_RIG$timestamp
  
  data_RIG        <- timeAverage(mydata = data_RIG,avg.time = "10 min",statistic = "mean",start.date = strptime("20170630235000","%Y%m%d%H%M%S",tz="UTC"))
  
  data_RIG        <- data_RIG[,c(which(colnames(data_RIG)=="date"),which(colnames(data_RIG)=="CO2"))]
  
  colnames(data_RIG) <- c("date","RIG_CO2")
  
  #
  
  query_str       <- paste("SELECT timestamp,CO2_DRY_CAL,H2O FROM NABEL_PAY WHERE timestamp>=1498867200 AND CO2_DRY_CAL!=-999 and H2O_F=1;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  data_PAY        <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  data_PAY$CO2    <- data_PAY$CO2_DRY_CAL * (1-data_PAY$H2O/100)
  
  data_PAY$date   <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_PAY$timestamp
  
  data_PAY        <- timeAverage(mydata = data_PAY,avg.time = "10 min",statistic = "mean",start.date = strptime("20170630235000","%Y%m%d%H%M%S",tz="UTC"))
  
  data_PAY        <- data_PAY[,c(which(colnames(data_PAY)=="date"),which(colnames(data_PAY)=="CO2"))]
  
  colnames(data_PAY) <- c("date","PAY_CO2")
  
  #
  
  query_str       <- paste("SELECT timestamp,CO2 FROM NABEL_HAE WHERE timestamp>=1498867200 AND CO2!=-999;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  data_HAE        <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  data_HAE$date   <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_HAE$timestamp
  
  data_HAE        <- timeAverage(mydata = data_HAE,avg.time = "10 min",statistic = "mean",start.date = strptime("20170630235000","%Y%m%d%H%M%S",tz="UTC"))
  
  data_HAE        <- data_HAE[,c(which(colnames(data_HAE)=="date"),which(colnames(data_HAE)=="CO2"))]
  
  colnames(data_HAE) <- c("date","HAE_CO2")
  
  #
  
  query_str       <- paste("SELECT timestamp,CO2 FROM EMPA_LAEG WHERE timestamp>=1498867200 AND CO2!=-999;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  data_LAEG       <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  data_LAEG$date   <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_LAEG$timestamp
  
  data_LAEG        <- timeAverage(mydata = data_LAEG,avg.time = "10 min",statistic = "mean",start.date = strptime("20170630235000","%Y%m%d%H%M%S",tz="UTC"))
  
  data_LAEG        <- data_LAEG[,c(which(colnames(data_LAEG)=="date"),which(colnames(data_LAEG)=="CO2"))]
  
  colnames(data_LAEG) <- c("date","LAEG_CO2")
  
  #
  
  query_str       <- paste("SELECT timestamp,CO2 FROM UNIBE_GIMM WHERE timestamp>=1498867200 AND CO2!=-999;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  data_GIMM       <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  data_GIMM$date   <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_GIMM$timestamp
  
  data_GIMM        <- timeAverage(mydata = data_GIMM,avg.time = "10 min",statistic = "mean",start.date = strptime("20170630235000","%Y%m%d%H%M%S",tz="UTC"))
  
  data_GIMM        <- data_GIMM[,c(which(colnames(data_GIMM)=="date"),which(colnames(data_GIMM)=="CO2"))]
  
  colnames(data_GIMM) <- c("date","GIMM_CO2")
  
  #
  
  query_str       <- paste("SELECT timestamp,CO2_CAL_ADJ FROM CarboSense_HPP_CO2 WHERE LocationName='ZUE' and SensorUnit_ID = 426 and timestamp>=1498867200 AND CO2_CAL_ADJ!=-999 and Valve=0;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  data_ZUE        <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  data_ZUE$date   <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_ZUE$timestamp
  
  data_ZUE        <- timeAverage(mydata = data_ZUE,avg.time = "10 min",statistic = "mean",start.date = strptime("20170630235000","%Y%m%d%H%M%S",tz="UTC"))
  
  data_ZUE        <- data_ZUE[,c(which(colnames(data_ZUE)=="date"),which(colnames(data_ZUE)=="CO2_CAL_ADJ"))]
  
  colnames(data_ZUE) <- c("date","ZUE_CO2")
  
  #
  
  query_str       <- paste("SELECT timestamp,CO2_CAL_ADJ FROM CarboSense_HPP_CO2 WHERE LocationName='MAGN' and SensorUnit_ID = 438 and timestamp>=1498867200 AND CO2_CAL_ADJ!=-999 and Valve=0;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  data_MAGN       <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  data_MAGN$date   <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_MAGN$timestamp
  
  data_MAGN        <- timeAverage(mydata = data_MAGN,avg.time = "10 min",statistic = "mean",start.date = strptime("20170630235000","%Y%m%d%H%M%S",tz="UTC"))
  
  data_MAGN        <- data_MAGN[,c(which(colnames(data_MAGN)=="date"),which(colnames(data_MAGN)=="CO2_CAL_ADJ"))]
  
  colnames(data_MAGN) <- c("date","MAGN_CO2")
  
  #
  
  query_str       <- paste("SELECT timestamp,CO2_CAL_ADJ FROM CarboSense_HPP_CO2 WHERE LocationName='SSAL' and SensorUnit_ID = 432 and timestamp>=1498867200 AND CO2_CAL_ADJ!=-999 and Valve=0;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  data_SSAL       <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  data_SSAL$date   <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_SSAL$timestamp
  
  data_SSAL        <- timeAverage(mydata = data_SSAL,avg.time = "10 min",statistic = "mean",start.date = strptime("20170630235000","%Y%m%d%H%M%S",tz="UTC"))
  
  data_SSAL        <- data_SSAL[,c(which(colnames(data_SSAL)=="date"),which(colnames(data_SSAL)=="CO2_CAL_ADJ"))]
  
  colnames(data_SSAL) <- c("date","SSAL_CO2")
  
  #
  
  data_REF <- merge(data_DUE,data_ZUE,  all=T)
  data_REF <- merge(data_REF,data_LAEG, all=T)
  data_REF <- merge(data_REF,data_GIMM, all=T)
  data_REF <- merge(data_REF,data_RIG,  all=T)
  data_REF <- merge(data_REF,data_PAY,  all=T)
  data_REF <- merge(data_REF,data_HAE,  all=T)
  data_REF <- merge(data_REF,data_MAGN, all=T)
  data_REF <- merge(data_REF,data_SSAL, all=T)
  
  rm(data_DUE,data_ZUE,data_LAEG,data_RIG,data_PAY,data_HAE,data_GIMM,data_SSAL,data_MAGN)
  gc()
  
  
  #
  
  query_str       <- paste("SELECT DISTINCT LocationName, SensorUnit_ID FROM CarboSense_CO2 WHERE LocationName != 'DUE1';",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  SU_LOC          <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  
  #
  
  print("LOOP")
  
  for(ith_SU_LOC in 1:dim(SU_LOC)){
    
    #
    
    CANTON <- tbl_location$Canton[which(tbl_location$LocationName == SU_LOC$LocationName[ith_SU_LOC])]
    
    #
    
    # if(!SU_LOC$LocationName[ith_SU_LOC]%in%c("RIG","HAE","PAY","PAYN") & CANTON!="ZH"){
    #   next
    # }
    # 
    # if(!SU_LOC$LocationName[ith_SU_LOC]%in%c("RIG","HAE","PAY","PAYN")){
    #   next
    # }
    
    # if(!CANTON%in%c("TI","GR")){
    #   next
    # }
    
    print(paste(SU_LOC$LocationName[ith_SU_LOC],CANTON))
    
    #
    
    query_str       <- paste("SELECT timestamp, CO2, CO2_A, SHT21_T, SHT21_RH, FLAG, O_FLAG FROM ",ProcMeasDBTableName," ",sep="")
    query_str       <- paste(query_str, " WHERE LocationName = '",SU_LOC$LocationName[ith_SU_LOC],"' AND SensorUnit_ID = ",SU_LOC$SensorUnit_ID[ith_SU_LOC],";",sep="")
    drv             <- dbDriver("MySQL")
    con             <- dbConnect(drv, group="CarboSense_MySQL")
    res             <- dbSendQuery(con, query_str)
    data_SU_LOC     <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    #
    
    data_SU_LOC$timestamp <- (data_SU_LOC$timestamp%/%600) * 600
    
    
    colnames(data_01) <- c("timestamp","CO2")
    
    ts_sync <- (data_SU_LOC$timestamp%/%600)*600
    id_1    <- which((data_SU_LOC$timestamp-ts_sync)<=300)
    id_2    <- which((data_SU_LOC$timestamp-ts_sync) >300)
    
    if(length(id_1)>0){
      data_SU_LOC$timestamp[id_1] <- ts_sync[id_1] 
    }
    if(length(id_2)>0){
      data_SU_LOC$timestamp[id_2] <- ts_sync[id_2] + 600
    }
    
    data_SU_LOC      <- data_SU_LOC[!duplicated(data_SU_LOC$timestamp),]
    
    data_SU_LOC$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_SU_LOC$timestamp
    
    #
    
    data <- merge(data_REF,data_SU_LOC,all=T)
    
    #
    
    
    id_anchor_events   <- which(  anchor_events$LocationName==SU_LOC$LocationName[ith_SU_LOC] 
                                  & anchor_events$SensorUnit_ID==SU_LOC$SensorUnit_ID[ith_SU_LOC]
                                  & !is.na(anchor_events$SU_CO2_corr))
    
    n_id_anchor_events <- length(id_anchor_events)
    
    anchor_events_ts   <- rep(NA,dim(data)[1])
    
    if(n_id_anchor_events>0){
      for(ith_ae in 1:n_id_anchor_events){
        
        anchor_events_ts[data$date>=anchor_events$Date_UTC_from[id_anchor_events[ith_ae]] & data$date<=anchor_events$Date_UTC_to[id_anchor_events[ith_ae]]] <- 350
        
      }
    }
    
    #
    
    use <- !is.na(data$CO2_A) & data$O_FLAG == 1
    
    figname <- paste(resultdir,"/ZZ_",CANTON,"_",SU_LOC$LocationName[ith_SU_LOC],"_",SU_LOC$SensorUnit_ID[ith_SU_LOC],"_TS.pdf",sep="")
    
    if(!SU_LOC$LocationName[ith_SU_LOC]%in%c("HAE","PAY","PAYN","RIG","GRO") & !CANTON%in%c("ZUE","TI")){
      yyy        <- matrix(NA,ncol=6,nrow=dim(data)[1])
      yyy[,1]    <- data$CO2_A
      yyy[use,2] <- data$CO2_A[use]
      yyy[,3]    <- data$DUE_CO2
      yyy[,4]    <- data$PAY_CO2
      yyy[,5]    <- data$RIG_CO2
      yyy[,6]    <- anchor_events_ts
      
      legend_str <- c("SU CO2 A","SU CO2 USE","DUE CO2","PAY CO2","RIG CO2","CORRECTION")
    }
    
    if(SU_LOC$LocationName[ith_SU_LOC]=="HAE"){
      yyy        <- matrix(NA,ncol=5,nrow=dim(data)[1])
      yyy[,1]    <- data$CO2_A
      yyy[use,2] <- data$CO2_A[use]
      yyy[,3]    <- data$DUE_CO2
      yyy[,4]    <- data$HAE_CO2
      yyy[,5]    <- anchor_events_ts
      
      legend_str <- c("SU CO2 A","SU CO2 USE","DUE CO2","HAE CO2","CORRECTION")
    }
    
    if(SU_LOC$LocationName[ith_SU_LOC]%in%c("PAY","PAYN")){
      yyy        <- matrix(NA,ncol=5,nrow=dim(data)[1])
      yyy[,1]    <- data$CO2_A
      yyy[use,2] <- data$CO2_A[use]
      yyy[,3]    <- data$GIMM_CO2
      yyy[,4]    <- data$PAY_CO2
      yyy[,5]    <- anchor_events_ts
      
      legend_str <- c("SU CO2 A","SU CO2 USE","GIMM CO2","PAY CO2","CORRECTION")
    }
    
    if(SU_LOC$LocationName[ith_SU_LOC]=="RIG"){
      yyy        <- matrix(NA,ncol=5,nrow=dim(data)[1])
      yyy[,1]    <- data$CO2_A
      yyy[use,2] <- data$CO2_A[use]
      yyy[,3]    <- data$DUE_CO2
      yyy[,4]    <- data$RIG_CO2
      yyy[,5]    <- anchor_events_ts
      
      legend_str <- c("SU CO2 A","SU CO2 USE","DUE CO2","RIG CO2","CORRECTION")
    }
    
    if(CANTON=="ZH"){
      yyy        <- matrix(NA,ncol=6,nrow=dim(data)[1])
      yyy[,1]    <- data$CO2_A
      yyy[use,2] <- data$CO2_A[use]
      yyy[,3]    <- data$DUE_CO2
      yyy[,4]    <- data$LAEG_CO2
      yyy[,5]    <- data$ZUE_CO2
      yyy[,6]    <- anchor_events_ts
      
      legend_str <- c("SU CO2 A","SU CO2 USE","DUE CO2","LAEG CO2","ZUE CO2","CORRECTION")
    }
    
    if(CANTON=="TI" | SU_LOC$LocationName[ith_SU_LOC]=="GRO"){
      yyy        <- matrix(NA,ncol=6,nrow=dim(data)[1])
      yyy[,1]    <- data$CO2_A
      yyy[use,2] <- data$CO2_A[use]
      yyy[,3]    <- data$MAGN_CO2
      yyy[,4]    <- data$SSAL_CO2
      yyy[,5]    <- anchor_events_ts
      
      legend_str <- c("SU CO2 A","SU CO2 USE","MAGN CO2","SSAL CO2","CORRECTION")
    }
    
    
    xlabString <- "Date" 
    ylabString <- expression(paste("CO"[2]*" [ppm]"))
    plot_ts(figname,data$date,yyy,"week",NULL,c(350,650),xlabString,ylabString,legend_str)
    
  }
  
}
