# LP8_measurement_analysis.r
# --------------------------------------------------
#
# Author: Michael Mueller
#
# --------------------------------------------------
#
# Remarks:
# - Computations refer to UTC.
#
# --------------------------------------------------
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

### ----------------------------------------------------------------------------------------------------------------------------

## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

# 

if(!as.integer(args[1])%in%c(1,2,3,20,30)){
  stop("MODEL: 1, 2, 3, 20 or 30!")
}else{
  
  # Standard 
  if(as.integer(args[1])==1){
    # Directories and filenames
    resultdir           <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2"
  }
  
  # Test 00
  if(as.integer(args[1])==2){
    # Directories and filenames
    resultdir           <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST00"
  }
  
  # Test 01
  if(as.integer(args[1])==3){
    # Directories and filenames
    resultdir           <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST01"
  }
  
  # Test 00 AMT
  if(as.integer(args[1])==20){
    # Directories and filenames
    resultdir           <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST00_AMT"
  }
  
  # Test 01
  if(as.integer(args[1])==30){
    # Directories and filenames
    resultdir           <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST01_AMT"
  }
}

#

COMP_DUE <- F

if(length(args)==2){
  if(args[2]=="DUE"){
    COMP_DUE <- T
  }else{
    stop("ARGS[2] must be DUE or must be not specified!")
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

# CO2 reference sites

ref_sites    <- c("DUE","PAY","HAE","RIG")
n_ref_sites  <- length(ref_sites)

### ----------------------------------------------------------------------------------------------------------------------------

# Parameters

QQ           <- 0.2

### ----------------------------------------------------------------------------------------------------------------------------

# Database queries : metadata

# Table "Deployment"

if(!COMP_DUE){
  query_str <- paste("SELECT * FROM Deployment WHERE LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1') AND Date_UTC_from >= '2017-06-28 00:00:00' ",sep="")
  query_str <- paste(query_str, "AND SensorUnit_ID BETWEEN 1010 AND 1334;",sep="")
}else{
  query_str <- paste("SELECT * FROM Deployment WHERE LocationName = 'DUE1' AND Date_UTC_to >= '2017-12-01 00:00:00' ",sep="")
  query_str <- paste(query_str, "AND SensorUnit_ID BETWEEN 1010 AND 1334;",sep="")
}
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
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
  id <- which(tbl_deployment$Date_UTC_from < strptime("2017-12-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"))
  if(length(id)>0){
    for(ii in 1:length(id)){
      tbl_deployment$Date_UTC_from[id[ii]] <- strptime("2017-12-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC")
    }
  }
}

tbl_deployment$duration_days     <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_to,time2=tbl_deployment$Date_UTC_from,units="days",tz="UTC"))
tbl_deployment$Date_UTC_from_str <- strftime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to_str   <- strftime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

tbl_deployment$timestamp_from    <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
tbl_deployment$timestamp_to      <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

# ----------------------------------------------------------------------------------------------------------------------

# Create directories

if(!dir.exists(resultdir)){
  dir.create((gsub(pattern = "/$",replacement = "",resultdir)))
}

# ----------------------------------------------------------------------------------------------------------------------

# Definition of variables

SU_events  <- NULL
df_REF     <- NULL

# Open PDF figure

if(!COMP_DUE){
  figname <- paste(resultdir,"LP8_measurement_analysis.pdf",sep="")
}
if(COMP_DUE){
  figname <- paste(resultdir,"LP8_measurement_analysis_DUE1.pdf",sep="")
}

def_par <- par()
pdf(file = figname, width=16, height=16, onefile=T, pointsize=12, colormodel="srgb")
par(mai=c(1,1,0.5,0.5),mfrow=c(2,1))

# Loop over REF + LP8

for(ith_depl in (1-n_ref_sites):dim(tbl_deployment)[1]){
  
  
  # import LP8 measurements
  
  if(ith_depl>0){
    query_str <- paste("SELECT timestamp, CO2, SHT21_RH FROM ",ProcMeasDBTableName," WHERE SensorUnit_ID = ",tbl_deployment$SensorUnit_ID[ith_depl]," and LocationName = '",tbl_deployment$LocationName[ith_depl],"' and timestamp >= ",tbl_deployment$timestamp_from[ith_depl]," and timestamp <= ",tbl_deployment$timestamp_to[ith_depl],";",sep="")
    drv       <- dbDriver("MySQL")
    con       <- dbConnect(drv, group="CarboSense_MySQL")
    res       <- dbSendQuery(con, query_str)
    data      <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    if(dim(data)[1]==0){
      next
    }
    
    data$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
    
    #
    
    descriptor <- paste("SU",tbl_deployment$SensorUnit_ID[ith_depl]," @ ",tbl_deployment$LocationName[ith_depl],sep="")
    
    #
    
    rm(id)
    gc()
  }
  
  # import REF measurements
  
  if(ith_depl<=0){
    
    ith_ref_site <- ith_depl + n_ref_sites
    
    if(ref_sites[ith_ref_site]%in%c("DUE")){
      query_str <- paste("SELECT timestamp,CO2_DRY_CAL,H2O FROM ",paste("NABEL_",ref_sites[ith_ref_site],sep="")," WHERE CO2_DRY_CAL != -999 and CO2_DRY != -999 and H2O != -999 and timestamp > 1496275200;",sep="")
      drv       <- dbDriver("MySQL")
      con       <- dbConnect(drv, group="CarboSense_MySQL")
      res       <- dbSendQuery(con, query_str)
      data      <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      colnames(data)[which(colnames(data)=="CO2_DRY_CAL")] <- "CO2_DRY"
      
      data$CO2 <- data$CO2_DRY * (1 - data$H2O/100)
      
      data     <- data[,c(which(colnames(data)=="timestamp"),which(colnames(data)=="CO2"))]
    }
    
    
    if(ref_sites[ith_ref_site]%in%c("RIG","PAY","HAE")){
      query_str <- paste("SELECT timestamp,CO2_WET_COMP FROM ",paste("NABEL_",ref_sites[ith_ref_site],sep="")," WHERE CO2_WET_COMP != -999 and timestamp > 1496275200;",sep="")
      drv       <- dbDriver("MySQL")
      con       <- dbConnect(drv, group="CarboSense_MySQL")
      res       <- dbSendQuery(con, query_str)
      data      <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)

      colnames(data)[which(colnames(data)=="CO2_WET_COMP")] <- "CO2"

      data     <- data[,c(which(colnames(data)=="timestamp"),which(colnames(data)=="CO2"))]
    }
    
    # if(ref_sites[ith_ref_site]==c("PAY")){
    #   query_str <- paste("SELECT timestamp,CO2_DRY_CAL,H2O_CAL FROM ",paste("NABEL_",ref_sites[ith_ref_site],sep="")," WHERE CO2_DRY_CAL != -999 and CO2_DRY != -999 and H2O_CAL != -999 and timestamp > 1496275200;",sep="")
    #   drv       <- dbDriver("MySQL")
    #   con       <- dbConnect(drv, group="CarboSense_MySQL")
    #   res       <- dbSendQuery(con, query_str)
    #   data      <- fetch(res, n=-1)
    #   dbClearResult(res)
    #   dbDisconnect(con)
    #   
    #   colnames(data)[which(colnames(data)=="CO2_DRY_CAL")] <- "CO2_DRY"
    #   colnames(data)[which(colnames(data)=="H2O_CAL")]     <- "H2O"
    #   
    #   data$CO2 <- data$CO2_DRY * (1 - data$H2O/100)
    #   
    #   data     <- data[,c(which(colnames(data)=="timestamp"),which(colnames(data)=="CO2"))]
    # }
    # 
    # if(ref_sites[ith_ref_site]%in%c("HAE")){
    #   query_str <- paste("SELECT timestamp,CO2 FROM ",paste("NABEL_",ref_sites[ith_ref_site],sep="")," WHERE CO2 != -999 and CO2_F=1 and timestamp > 1496275200;",sep="")
    #   drv       <- dbDriver("MySQL")
    #   con       <- dbConnect(drv, group="CarboSense_MySQL")
    #   res       <- dbSendQuery(con, query_str)
    #   data      <- fetch(res, n=-1)
    #   dbClearResult(res)
    #   dbDisconnect(con)
    # }
    
    
    data$date     <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
    data$SHT21_RH <- 0
    
    #
    
    data          <- timeAverage(mydata = data,avg.time = "10 min",statistic = "mean",start.date = strptime("20170601000000","%Y%m%d%H%M%S",tz="UTC"))
    data          <- as.data.frame(data, stringsAsFactors=T)
    data          <- data[which(!is.na(data$CO2)),]
    
    #
    
    descriptor <- paste("SU: REF -",ref_sites[ith_ref_site])
    
  }
  
  # Adding additional variables to data.frame "data"
  
  data$hour          <- as.numeric(strftime(data$date,"%H",tz="UTC"))
  data$days          <- as.numeric(difftime(time1=data$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="days",tz="UTC"))
  data$days_00       <- floor(data$days)
  
  # Analysis period
  
  min_day_00         <- min(data$days_00)
  max_day_00         <- max(data$days_00)
  n_days             <- max_day_00 - min_day_00 + 1
  
  # Colnames for data frame "df"
  
  cn <- c("date",
          "day_00",
          "QQ_CO2_p007",
          "QQ_CO2_n007",
          "QQ_CO2_p015",
          "QQ_CO2_n015",
          "QQ_CO2_today",
          "QQ_CO2_yesterday",
          "QQ_CO2_tommorow",
          "QQ_CO2_eq_tp015",
          "corCoef",
          "slope_CO2_eq_tp007",
          "slope_CO2_eq_tp015",
          "rng_QQ_CO2_eq_tp015",
          "trsh")
  
  df           <- as.data.frame(matrix(NA,ncol=length(cn),nrow=n_days))
  colnames(df) <- cn
  
  df$day_00    <- min_day_00:max_day_00
  df$date      <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + df$day_00 * 86400
  
  
  # Loop over all days
  
  cc <- 0
  
  hoursOfDay <- c(13,14,15,16)
  
  for(dayNow_00 in min_day_00:max_day_00){
    
    cc <- cc + 1
    
    # time filters
    
    id_p007          <- which(data$days_00 %in% (dayNow_00-7):(dayNow_00-1) & data$hour%in%hoursOfDay & data$CO2!=-999 & data$SHT21_RH < 95)
    id_n007          <- which(data$days_00 %in% (dayNow_00+7):(dayNow_00+1) & data$hour%in%hoursOfDay & data$CO2!=-999 & data$SHT21_RH < 95)
    
    id_p015          <- which(data$days_00 %in% (dayNow_00-15):(dayNow_00-1) & data$hour%in%hoursOfDay & data$CO2!=-999 & data$SHT21_RH < 95)
    id_n015          <- which(data$days_00 %in% (dayNow_00+15):(dayNow_00+1) & data$hour%in%hoursOfDay & data$CO2!=-999 & data$SHT21_RH < 95)
    
    id_yesterday     <- which(data$days_00 %in%  (dayNow_00-1)              & data$hour%in%hoursOfDay & data$CO2!=-999 & data$SHT21_RH < 95)
    id_tomorrow      <- which(data$days_00 %in%  (dayNow_00+1)              & data$hour%in%hoursOfDay & data$CO2!=-999 & data$SHT21_RH < 95)
    
    id_today         <- which(data$days_00 %in%  dayNow_00                  & data$hour%in%hoursOfDay & data$CO2!=-999 & data$SHT21_RH < 95)
    
    id_eq_tp015      <- which(data$days_00 %in% (dayNow_00-7):(dayNow_00+7) & data$hour%in%hoursOfDay & data$CO2!=-999 & data$SHT21_RH < 95)
    id_eq_tp007      <- which(data$days_00 %in% (dayNow_00-3):(dayNow_00+3) & data$hour%in%hoursOfDay & data$CO2!=-999 & data$SHT21_RH < 95)
    
    id_yesterday_all <- which(data$days_00 %in%  (dayNow_00-1)                                        & data$CO2!=-999 & data$SHT21_RH < 95)
    id_tomorrow_all  <- which(data$days_00 %in%  (dayNow_00+1)                                        & data$CO2!=-999 & data$SHT21_RH < 95)
    id_today_all     <- which(data$days_00 %in%  dayNow_00                                            & data$CO2!=-999 & data$SHT21_RH < 95)
    
    # 
    
    n_id_p007          <- length(id_p007)
    n_id_n007          <- length(id_n007)
    
    n_id_p015          <- length(id_p015)
    n_id_n015          <- length(id_n015)
    
    n_id_yesterday     <- length(id_yesterday)
    n_id_tomorrow      <- length(id_tomorrow)
    n_id_today         <- length(id_today)
    
    n_id_eq_tp015      <- length(id_eq_tp015)
    n_id_eq_tp007      <- length(id_eq_tp007)
    
    n_id_yesterday_all <- length(id_yesterday_all)
    n_id_tomorrow_all  <- length(id_tomorrow_all)
    n_id_today_all     <- length(id_today_all)
    
    #
    
    if(n_id_p007<10){
      next
    }
    if(n_id_today<5){
      next
    }
    if(n_id_n007<10){
      next
    }
    
    u_days_p007   <- unique(data$days_00[id_p007])
    n_u_days_p007 <- length(u_days_p007)
    
    if(n_u_days_p007<=3){
      next
    }
    
    u_days_n007   <- unique(data$days_00[id_n007])
    n_u_days_n007 <- length(u_days_n007)
    
    if(n_u_days_n007<=3){
      next
    }
    
    # computation of CO2 measures
    
    df$QQ_CO2_p007[cc]      <- as.numeric(quantile(data$CO2[id_p007],      probs=QQ))
    df$QQ_CO2_n007[cc]      <- as.numeric(quantile(data$CO2[id_n007],      probs=QQ))
    df$QQ_CO2_p015[cc]      <- as.numeric(quantile(data$CO2[id_p015],      probs=QQ))
    df$QQ_CO2_n015[cc]      <- as.numeric(quantile(data$CO2[id_n015],      probs=QQ))
    df$QQ_CO2_today[cc]     <- as.numeric(quantile(data$CO2[id_today],     probs=QQ))
    df$QQ_CO2_yesterday[cc] <- as.numeric(quantile(data$CO2[id_yesterday], probs=QQ))
    df$QQ_CO2_tommorow[cc]  <- as.numeric(quantile(data$CO2[id_tomorrow],  probs=QQ))
    df$QQ_CO2_eq_tp015[cc]  <- as.numeric(quantile(data$CO2[id_eq_tp015],  probs=QQ))
  }
  
  #
  
  max_cc <- dim(df)[1]
  
  for(cc in 1:max_cc){
    
    id    <- max(c(cc-7,1)):min(c(cc+7,max_cc))
    id_id <- which(!is.na(df$QQ_CO2_eq_tp015[id]))
    
    if(length(id_id)>10){
      # fit                       <- rlm(y~x,data.frame(x=df$day_00[id[id_id]],y=df$QQ_CO2_eq_tp015[id[id_id]],stringsAsFactors = F),psi=psi.huber,k=1.345)
      fit                       <- lm(y~x,data.frame(x=df$day_00[id[id_id]],y=df$QQ_CO2_eq_tp015[id[id_id]],stringsAsFactors = F))
      df$slope_CO2_eq_tp015[cc] <- fit$coefficients[2]
    }
    
    
    id    <- max(c(cc-3,1)):min(c(cc+3,dim(df)[1]))
    id_id <- which(!is.na(df$QQ_CO2_eq_tp007[id]))
    
    if(length(id_id)>10){
      # fit                       <- rlm(y~x,data.frame(x=df$day_00[id[id_id]],y=df$QQ_CO2_eq_tp007[id[id_id]],stringsAsFactors = F),psi=psi.huber,k=1.345)
      fit                       <- lm(y~x,data.frame(x=df$day_00[id[id_id]],y=df$QQ_CO2_eq_tp007[id[id_id]],stringsAsFactors = F))
      df$slope_CO2_eq_tp007[cc] <- fit$coefficients[2]
    }
  }
  
  
  # Treshold discontinuity
  
  diff_QQ_CO2_n007_p007 <- df$QQ_CO2_n007-df$QQ_CO2_p007
  df$trsh               <- median(diff_QQ_CO2_n007_p007,na.rm=T) + 5*mad(diff_QQ_CO2_n007_p007,na.rm=T)
  
  
  # Detection of discontinuities
  
  discontinuity_day_00 <- NULL
  discontinuity_date   <- NULL
  
  cc <- 0
  
  for(dayNow_00 in min_day_00:max_day_00){
    
    cc <- cc + 1
    
    if(!is.na(df$QQ_CO2_p007[cc])&!is.na(df$QQ_CO2_n007[cc])&!is.na(df$QQ_CO2_yesterday[cc])&!is.na(df$QQ_CO2_today[cc])&!is.na(df$QQ_CO2_tommorow[cc])){
      
      if(abs(df$QQ_CO2_p007[cc]-df$QQ_CO2_n007[cc])>min(c(100,df$trsh[cc]))){
        discontinuity_day_00 <- c(discontinuity_day_00,df$day_00[cc])
      }
    }
  }
  
  if(!is.null(discontinuity_day_00)){
    discontinuity_date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + discontinuity_day_00*86400
  }
  
  #
  
  slope_date   <- NULL
  slope_day_00 <- NULL
  
  
  if(T){
    
    # 2 / 20 ??
    # 3 / 40 ??
    
    id <- which(abs(df$slope_CO2_eq_tp015)>3 & abs(df$QQ_CO2_p015-df$QQ_CO2_n015)>40)
    if(length(id)>0){
      slope_day_00 <- df$day_00[id]
      slope_date   <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + slope_day_00*86400
    }
  }
  
  
  if(all(is.na(df$QQ_CO2_p007)) | all(is.na(df$QQ_CO2_n007))){
    next
  }
  
  
  ### ---------------------------------
  
  ## Figures
  
  # Part 1
  
  yrange <- range(c(df$QQ_CO2_today,350,650),na.rm=T)
  
  plot(  df$date,rep(-999,dim(df)[1]),ylim=yrange,main=descriptor,xlab="Date", ylab=expression(paste("CO"[2]*" [ppm]")),cex.lab=2.00,cex.axis=2.00,cex.main=2.00)
  
  if(!is.null(slope_date)){
    for(i in 1:length(slope_date)){
      polygon(c(slope_date[i],slope_date[i]+86400,slope_date[i]+86400,slope_date[i]),c(-1e5,-1e5,1e5,1e5),border=NA,col="gray75")
    }
  }
  
  points(df$date,df$QQ_CO2_today, main=descriptor,pch=16,cex=1,col=1)
  lines( df$date,df$QQ_CO2_eq_tp015, col=2,lwd=2)
  lines( df$date,df$QQ_CO2_p007, col=1,lwd=2,lty=5)
  lines( df$date,df$QQ_CO2_n007, col=2,lwd=2,lty=5)
  
  if(!is.null(discontinuity_date)){
    for(i in 1:length(discontinuity_date)){
      lines(c(discontinuity_date[i],discontinuity_date[i]),c(-1e5,1e5),col=2,lwd=2)
    }
  }
  
  if(ith_depl>0){
    for(i in 1:n_ref_sites){
      pos <- which(colnames(df_REF)==paste(ref_sites[i],"_QQ_CO2_eq_tp015",sep=""))
      lines(df_REF$date,df_REF[,pos],col=i+3)
    }
    
    par(family="mono")
    legend("right",legend=ref_sites,lty=1,lwd=2,col=3+(1:n_ref_sites),bg="white",cex=2.00)
    par(family="")
  }
  
  # str_01 <- expression(paste("CO"[2]*" td"))
  # str_02 <- expression(paste("CO"[2]*" pW"))
  # str_03 <- expression(paste("CO"[2]*" nW"))
  # str_04 <- expression(paste("CO"[2]*" 2W"))
  
  str_01 <- expression(paste("Q"[d]*""))
  str_02 <- expression(paste("Q"[prev7d]*""))
  str_03 <- expression(paste("Q"[next7d]*""))
  str_04 <- expression(paste("Q"[15]*""[d]*""))
  
  par(family="mono")
  legend("topleft",legend=c(str_01,str_02,str_03,str_04),lty=c(NA,5,5,1),lwd=c(NA,2,2,2),pch=c(16,NA,NA,NA),col=c(1,1,2,2),bg="white",cex=2.00)
  par(family="")
  
  # Part 2
  
  plot(  df$date,rep(-999,dim(df)[1]),ylim=c(-75,75),main=descriptor,xlab="Date",ylab=expression(paste("CO"[2]*" [ppm]")),cex.lab=2.00,cex.axis=2.00,cex.main=2.00)
  
  if(!is.null(slope_date)){
    for(i in 1:length(slope_date)){
      polygon(c(slope_date[i],slope_date[i]+86400,slope_date[i]+86400,slope_date[i]),c(-1e5,-1e5,1e5,1e5),border=NA,col="gray75")
    }
  }
  
  # if(ith_depl>0){
  #   for(i in 1:n_ref_sites){
  #     pos01 <- which(colnames(df_REF)==paste(ref_sites[i],"_QQ_CO2_n007",sep=""))
  #     pos02 <- which(colnames(df_REF)==paste(ref_sites[i],"_QQ_CO2_p007",sep=""))
  #     lines(df_REF$date,df_REF[,pos01]-df_REF[,pos02],col=i+3,lwd=1)
  #   }
  # }
  
  lines( df$date,df$QQ_CO2_n007-df$QQ_CO2_p007,col=1,lwd=3)
  points(df$date,df$QQ_CO2_tommorow-df$QQ_CO2_yesterday,pch=16,col=2,cex=1)
  lines( df$date,+df$trsh,col="gray50",lwd=3)
  lines( df$date,-df$trsh,col="gray50",lwd=3)
  
  
  # str_01 <- expression(paste("CO"[2]*": p7d - n7d"))
  # str_02 <- expression(paste("CO"[2]*": p1d - n1d"))
  
  str_01 <- expression(paste("Q"[next7d]*" - Q"[prev7d]*""))
  str_02 <- expression(paste("Q"[next1d]*" - Q"[prev1d]*""))
  
  par(family="mono")
  legend("topleft",legend=c(str_01,str_02),lty=c(1,NA),pch=c(NA,16),lwd=c(2,NA),col=c(1,2),bg="white",cex=2.00)
  par(family="")
  
  # if(ith_depl>0){
  #   par(family="mono")
  #   legend("topright",legend=ref_sites,lty=1,col=3+(1:n_ref_sites),bg="white",cex=2.00)
  #   par(family="")
  # }
  
  
  ### ---------------------------------
  
  
  if(ith_depl<=0){
    if(is.null(df_REF)){
      colnames(df) <- c(colnames(df)[1:2],paste(paste(ref_sites[ith_ref_site],"_",sep=""),colnames(df)[3:dim(df)[2]],sep=""))
      df_REF       <- df
    }else{
      colnames(df) <- c(colnames(df)[1:2],paste(paste(ref_sites[ith_ref_site],"_",sep=""),colnames(df)[3:dim(df)[2]],sep=""))
      df           <- df[,which(colnames(df)!="date")]
      df_REF       <- merge(df_REF,df,by="day_00")
    }
  }
  
  
  ### ---------------------------------
  
  ## extension of data frame "SU_events" -> transform single entries to records
  
  if(ith_depl>0 & (!is.null(discontinuity_day_00)|!is.null(slope_day_00)) ){
    
    dates_combined <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + sort(unique(c(discontinuity_day_00,slope_day_00)))*86400
    
    for(i in 1:length(dates_combined)){
      
      newEntry <- T
      
      if(i>1){
        if(SU_events$Date_UTC_to[dim(SU_events)[1]]==dates_combined[i]){
          SU_events$Date_UTC_to[dim(SU_events)[1]] <- SU_events$Date_UTC_to[dim(SU_events)[1]] + 86400
          newEntry <- F
        }
      }
      
      if(newEntry){
        
        SU_events <- rbind(SU_events,data.frame(SensorUnit_ID = tbl_deployment$SensorUnit_ID[ith_depl],
                                                LocationName  = tbl_deployment$LocationName[ith_depl],
                                                Date_UTC_from = dates_combined[i],
                                                Date_UTC_to   = dates_combined[i]+86400,
                                                stringsAsFactors = T))
        
      }
    }
  }
}

dev.off()
par(def_par)


# ----------------------------------------------------------------------------------------------------------------------

## Data export

if(dim(SU_events)[1]>0){
  SU_events$Date_UTC_from <- strftime(SU_events$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  SU_events$Date_UTC_to   <- strftime(SU_events$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
}

#

if(!COMP_DUE){
  write.table(df_REF,   paste(resultdir,"df_REF.csv",sep="")   , sep=";",row.names=F,col.names=T)
  write.table(SU_events,paste(resultdir,"SU_events.csv",sep=""), sep=";",row.names=F,col.names=T)
}

if(COMP_DUE){
  write.table(df_REF,   paste(resultdir,"df_REF_DUE1.csv",sep="")   , sep=";",row.names=F,col.names=T)
  write.table(SU_events,paste(resultdir,"SU_events_DUE1.csv",sep=""), sep=";",row.names=F,col.names=T)
}

#





