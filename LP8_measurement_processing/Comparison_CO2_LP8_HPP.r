# Comparison_CO2_LP8_HPP.r
# ------------------------

# Remarks:
# - Comparison of CO2 data from LP8 and HPP instrument.
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

source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

### ----------------------------------------------------------------------------------------------------------------------------

ma <- function(x,n=10){filter(x,rep(1/n,n), sides=1)}

### ------------------------------------------------------------------------------------------------------------------------------

## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

# 

if(!as.integer(args[1])%in%c(0,1,2,3,20,30)){
  stop("MODEL: 1, 2, 3, 20, 30 or 0 (= FINAL)!")
}else{
  
  # Final 
  if(as.integer(args[1])==0){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_FINAL/Comparison_LP8_HPP/"
    # Table with processed CO2 measurements
    LP8_ProcDataTblName <- "CarboSense_CO2_FINAL"
    HPP_ProcDataTblName <- "CarboSense_HPP_CO2"
    eq_label            <- "(FINAL)"
  }
  
  # Standard 
  if(as.integer(args[1])==1){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/Comparison_LP8_HPP/" 
    # Table with processed CO2 measurements
    LP8_ProcDataTblName <- "CarboSense_CO2"
    HPP_ProcDataTblName <- "CarboSense_HPP_CO2"
    eq_label            <- "(Eq. 4)"
  }
  
  # Test 00
  if(as.integer(args[1])==2){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/Comparison_LP8_HPP/" 
    # Table with processed CO2 measurements
    LP8_ProcDataTblName <- "CarboSense_CO2_TEST00"
    HPP_ProcDataTblName <- "CarboSense_HPP_CO2"
    eq_label            <- "(Eq. 5)"
  }
  
  # Test 01
  if(as.integer(args[1])==3){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/Comparison_LP8_HPP/" 
    # Table with processed CO2 measurements
    LP8_ProcDataTblName <- "CarboSense_CO2_TEST01"
    HPP_ProcDataTblName <- "CarboSense_HPP_CO2"
    eq_label            <- "(Eq. 4)"
  }
  
  # Test 00 AMT
  if(as.integer(args[1])==20){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT/Comparison_LP8_HPP/" 
    # Table with processed CO2 measurements
    LP8_ProcDataTblName <- "CarboSense_CO2_TEST00_AMT"
    HPP_ProcDataTblName <- "CarboSense_HPP_CO2"
    eq_label            <- "(Eq. 5)"
  }
  
  # Test 01 AMT
  if(as.integer(args[1])==30){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT/Comparison_LP8_HPP/" 
    # Table with processed CO2 measurements
    LP8_ProcDataTblName <- "CarboSense_CO2_TEST01_AMT"
    HPP_ProcDataTblName <- "CarboSense_HPP_CO2"
    eq_label            <- "(Eq. 4)"
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

query_str         <- paste("SELECT * FROM Deployment WHERE SensorUnit_ID BETWEEN 426 AND 445 and LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','MET1','DUE5');",sep="")

drv               <- dbDriver("MySQL")
con               <- dbConnect(drv, group="CarboSense_MySQL")
res               <- dbSendQuery(con, query_str)
tbl_depl          <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_depl$Date_UTC_from  <- strptime(tbl_depl$Date_UTC_from, "%Y-%m-%d %H:%M:%S", tz="UTC")
tbl_depl$Date_UTC_to    <- strptime(tbl_depl$Date_UTC_to,   "%Y-%m-%d %H:%M:%S", tz="UTC")

tbl_depl$timestamp_from <- as.numeric(difftime(time1=tbl_depl$Date_UTC_from,time2=strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S", tz="UTC"),units="secs",tz="UTC"))
tbl_depl$timestamp_to   <- as.numeric(difftime(time1=tbl_depl$Date_UTC_to,  time2=strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S", tz="UTC"),units="secs",tz="UTC"))

### ----------------------------------------------------------------------------------------------------------------------------

statistics        <- NULL

weekly_statistics <- NULL

### ----------------------------------------------------------------------------------------------------------------------------

for(ith_depl in 1:dim(tbl_depl)[1]){
  
  depl_date_string <- paste(strftime(tbl_depl$Date_UTC_from[ith_depl],"%d%m%Y",tz="UTC"),"-",strftime(tbl_depl$Date_UTC_to[ith_depl],"%d%m%Y",tz="UTC"),sep="")
  
  # Compare pressure
  # ----------------
  
  DESC <- paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_",depl_date_string,sep="")
  
  # Import HPP pressure
  
  query_str         <- paste("SELECT timestamp, Pressure FROM ",HPP_ProcDataTblName," WHERE LocationName = '",tbl_depl$LocationName[ith_depl],"' and SensorUnit_ID = ",tbl_depl$SensorUnit_ID[ith_depl]," and timestamp >= ",tbl_depl$timestamp_from[ith_depl]," and timestamp <= ",tbl_depl$timestamp_to[ith_depl],";",sep="")
  drv               <- dbDriver("MySQL")
  con               <- dbConnect(drv, group="CarboSense_MySQL")
  res               <- dbSendQuery(con, query_str)
  tbl_HPP           <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  colnames(tbl_HPP) <- c("timestamp","HPP_pressure")
  
  minTimestamp      <- min(tbl_HPP$timestamp)
  maxTimestamp      <- max(tbl_HPP$timestamp)
  
  # Import interpolated pressure
  
  query_str         <- paste("SELECT timestamp, pressure FROM PressureInterpolation WHERE LocationName='",tbl_depl$LocationName[ith_depl],"' and timestamp >= ",minTimestamp," and timestamp <= ",maxTimestamp,";",sep="")
  drv               <- dbDriver("MySQL")
  con               <- dbConnect(drv, group="CarboSense_MySQL")
  res               <- dbSendQuery(con, query_str)
  tbl_pressure      <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  query_str         <- paste("SELECT timestamp, height FROM PressureParameter WHERE timestamp >= ",minTimestamp," and timestamp <= ",maxTimestamp,";",sep="")
  drv               <- dbDriver("MySQL")
  con               <- dbConnect(drv, group="CarboSense_MySQL")
  res               <- dbSendQuery(con, query_str)
  tbl_pressure_par  <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  
  colnames(tbl_pressure)     <- c("timestamp","INT_pressure")
  colnames(tbl_pressure_par) <- c("timestamp","INT_pressure_par")
  
  tbl_pressure               <- merge(tbl_pressure,tbl_pressure_par,by="timestamp")
  
  if(tbl_depl$HeightAboveGround[ith_depl] != -999){ 
    tbl_pressure$INT_pressure  <- tbl_pressure$INT_pressure * exp(-tbl_depl$HeightAboveGround[ith_depl]/tbl_pressure$INT_pressure_par)
  }
  
  # merge data
  
  data      <- merge(tbl_HPP,tbl_pressure)
  data$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
  
  # ANALYSIS
  
  residuals   <- data$HPP_pressure-data$INT_pressure
  
  ok          <- !is.na(data$HPP_pressure) & !is.na(data$INT_pressure)
  
  id_FLAG_0   <- which(ok)
  n_id_FLAG_0 <- length(id_FLAG_0)
  
  RMSE_0      <- sqrt(sum((residuals[id_FLAG_0])^2)/n_id_FLAG_0)
  COR_0       <- cor(data$INT_pressure[id_FLAG_0],data$HPP_pressure[id_FLAG_0],method="pearson",use="complete.obs")
  
  fit_0       <- lm(HPP_pressure~INT_pressure,data[id_FLAG_0,])
  
  # PLOT: SCATTER/HIST
  
  str_L_00 <- paste("SLOPE:",sprintf("%6.2f",fit_0$coefficients[2]))
  str_L_01 <- paste("INTER:",sprintf("%6.2f",fit_0$coefficients[1]))
  str_L_02 <- paste("RMSE :",sprintf("%6.1f",RMSE_0))
  str_L_03 <- paste("COR  :",sprintf("%6.1f",COR_0))
  str_L_04 <- paste("N    :",sprintf("%6.0f",n_id_FLAG_0))
  
  
  str_R_00 <- paste("MEAN A:",sprintf("%6.1f",mean(residuals[id_FLAG_0])))
  str_R_01 <- paste("Q001 A:",sprintf("%6.1f",quantile(residuals[id_FLAG_0],probs=0.01)))
  str_R_02 <- paste("Q050 A:",sprintf("%6.1f",quantile(residuals[id_FLAG_0],probs=0.50)))
  str_R_03 <- paste("Q099 A:",sprintf("%6.1f",quantile(residuals[id_FLAG_0],probs=0.99)))
  
  #
  
  figname <- paste(resultdir,DESC,"_pressure.pdf",sep="")
  
  mainStr <- paste(DESC,sep="")
  subStr  <- paste("Data: ",
                   strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+min(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),
                   "-",
                   strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+max(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),sep="")
  
  def_par <- par()
  pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,2))
  
  yrange <- range(c(data$HPP_pressure,data$INT_pressure),na.rm=T)
  xrange <- yrange
  
  plot(0,0,xlim=xrange,ylim=yrange,main=mainStr,xlab="Pressure INT [hPa]",ylab="Pressure HPP [hPa]",sub=subStr,cex.axis=1.25,cex.lab=1.25,cex.main=1.25)
  
  if(n_id_FLAG_0>0){
    points(data$INT_pressure[id_FLAG_0],data$HPP_pressure[id_FLAG_0],col="black",pch=16,cex=0.5)
  }
  
  lines(c(0,1e4),c(fit_0$coefficients[1],fit_0$coefficients[1]+1e4*fit_0$coefficients[2]),col=2)
  
  lines(c(0,1e4),c(0,1e4),col=1)
  
  
  par(family="mono")
  legend("bottomright",legend=c(str_L_00,str_L_01,"",str_L_02,str_L_03,str_L_04),bg="white",cex=1.25)
  par(family="")
  
  #
  
  Q050_S <- quantile(residuals[id_FLAG_0],probs=0.50)
  MEAN_S <- mean(residuals[id_FLAG_0])
  
  hist(residuals[id_FLAG_0],seq(floor(min(residuals[id_FLAG_0])),ceiling(max(residuals[id_FLAG_0])),0.5),xlim=c(Q050_S-10,Q050_S+10),col="slategray",main="",xlab="Pressure interp - pressure HPP [hPa]",ylab="Number",cex.axis=1.25,cex.lab=1.25)
  lines(c(MEAN_S,MEAN_S),c(-1e9,1e9),col=2,lwd=2)
  par(family="mono")
  legend("topright",legend=c(str_R_00,str_R_01,str_R_02,str_R_03),bg="white",cex=1.25)
  par(family="")
  
  #
  
  dev.off()
  par(def_par)
  
  
  #
  
  figname <- paste(resultdir,DESC,"_pressure_TS.pdf",sep="")
  
  yyy        <- cbind(data$HPP_pressure,
                      data$INT_pressure)
  
  xlabString <- "Date"
  ylabString <- expression(paste("Pressure [hPa]"))
  legend_str <- c("HPP pressure","INT pressure")
  plot_ts(figname,data$date,yyy,"week",NULL,NULL,xlabString,ylabString,legend_str)
  
  #
  
  rm(tbl_pressure,tbl_pressure_par,residuals,ok,id_FLAG_0,n_id_FLAG_0,RMSE_0,COR_0,fit_0)
  rm(str_L_00,str_L_01,str_L_02,str_L_03,str_L_04)
  rm(str_R_00,str_R_01,str_R_02,str_R_03)
  rm(Q050_S,MEAN_S)
  gc()
  
  
  
  ### ---------------------------------------------------
  
  # Compare CO2 measurements
  # ------------------------
  
  Corresponding_LP8_LocationName <- tbl_depl$LocationName[ith_depl]
  
  if(tbl_depl$LocationName[ith_depl]=="RECK"){
    Corresponding_LP8_LocationName <- "REH"
  }
  
  # Select SensorUnit_IDs
  
  query_str         <- paste("SELECT SensorUnit_ID FROM ",LP8_ProcDataTblName," WHERE LocationName='",Corresponding_LP8_LocationName,"' ",sep="")
  query_str         <- paste(query_str,"AND timestamp >= ",tbl_depl$timestamp_from[ith_depl]," AND timestamp <= ",tbl_depl$timestamp_to[ith_depl],";",sep="")
  drv               <- dbDriver("MySQL")
  con               <- dbConnect(drv, group="CarboSense_MySQL")
  res               <- dbSendQuery(con, query_str)
  tbl_CO2_SU        <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  if(dim(tbl_CO2_SU)[1]==0){
    next
  }
  
  u_SensorUnit_ID   <- sort(unique(tbl_CO2_SU$SensorUnit_ID))
  n_u_SensorUnit_ID <- length(u_SensorUnit_ID)
  
  rm(tbl_CO2_SU)
  gc()
  
  
  for(ith_SU in 1:n_u_SensorUnit_ID){
    
    # Import processed LP8 data
    
    query_str         <- paste("SELECT * FROM ",LP8_ProcDataTblName," WHERE LocationName='",Corresponding_LP8_LocationName,"' and SensorUnit_ID=",u_SensorUnit_ID[ith_SU]," ",sep="")
    query_str         <- paste(query_str,"AND timestamp >= ",tbl_depl$timestamp_from[ith_depl]," AND timestamp <= ",tbl_depl$timestamp_to[ith_depl],";",sep="")
    drv               <- dbDriver("MySQL")
    con               <- dbConnect(drv, group="CarboSense_MySQL")
    res               <- dbSendQuery(con, query_str)
    tbl_CO2           <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    if(dim(tbl_CO2)[1]==0){
      next
    }
    
    tbl_CO2      <- tbl_CO2[order(tbl_CO2$timestamp),]
    minTimestamp <- min(tbl_CO2$timestamp)
    maxTimestamp <- max(tbl_CO2$timestamp)
    
    # Import reference data
    
    query_str         <- paste("SELECT timestamp, CO2_CAL_ADJ FROM ",HPP_ProcDataTblName," WHERE LocationName = '",tbl_depl$LocationName[ith_depl],"' and SensorUnit_ID = ",tbl_depl$SensorUnit_ID[ith_depl]," and CO2_CAL_ADJ != -999 and Valve = 0 and timestamp >= ",minTimestamp," and timestamp <= ",maxTimestamp,";",sep="")
    drv               <- dbDriver("MySQL")
    con               <- dbConnect(drv, group="CarboSense_MySQL")
    res               <- dbSendQuery(con, query_str)
    tbl_HPP           <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    if(dim(tbl_HPP)[1]==0){
      next
    }
    
    tbl_HPP      <- tbl_HPP[order(tbl_HPP$timestamp),]
    tbl_HPP$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_HPP$timestamp
    
    date_00      <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + min(tbl_HPP$timestamp)
    date_00      <- strptime(strftime(date_00,"%Y%m%d%H0000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
    
    tmp <- NULL
    
    for(ii in 0:9){
      tmp <- rbind(tmp,timeAverage(mydata = tbl_HPP,avg.time = "10 min",statistic = "mean",start.date = (date_00+ii*60)-3600))
    }
    
    tmp           <- tmp[which(!is.na(tmp$CO2_CAL_ADJ)),]
    tmp$timestamp <- as.numeric(difftime(time1=tmp$date,time2=strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S", tz="UTC"),units="secs",tz="UTC"))
    tmp           <- tmp[order(tmp$timestamp),]
    
    tbl_HPP       <- data.frame(timestamp=tmp$timestamp,CO2_CAL_ADJ = tmp$CO2_CAL_ADJ,stringsAsFactors = F)
    
    data          <- merge(tbl_HPP,tbl_CO2)
    
    #
    
    data$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
    
    # Aggregation
    
    weeks     <- floor(as.numeric(difftime(time1=data$date,time2=strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC"),units="weeks",tz="UTC")))
    u_weeks   <- sort(unique(weeks))
    n_u_weeks <- length(u_weeks)
    
    
    # ANALYSIS
    
    for(mode in c(1,2,3,4,5,6)){
      
      for(ith_week in 0:n_u_weeks){
        
        if(ith_week==0){
          week_ok   <- rep(T,dim(data)[1])
          week_mode <- 0
        }else{
          week_ok   <- weeks == u_weeks[ith_week]
          week_mode <- u_weeks[ith_week]
        }
        
        
        if(mode==1){
          data$measurements <- data$CO2
          residuals         <- data$CO2-data$CO2_CAL_ADJ
          ok                <- !is.na(data$CO2_CAL_ADJ) & !is.na(data$CO2) & week_ok
        }
        if(mode%in%c(2,3,4,5,6)){
          data$measurements <- data$CO2_A
          residuals         <- data$CO2_A-data$CO2_CAL_ADJ
          ok                <- !is.na(data$CO2_CAL_ADJ) & !is.na(data$CO2_A) & data$CO2_A != -999 & week_ok
        }
        
        
        id_FLAG_0   <- which(ok)
        
        if(mode%in%c(1,2)){
          id_FLAG_1   <- which(ok & data$FLAG==1)
        }
        if(mode==3){
          id_FLAG_1   <- which(ok & data$Q_FLAG==1)
        }
        if(mode==4){
          id_FLAG_1   <- which(ok & data$O_FLAG==1)
        }
        if(mode==5){
          id_FLAG_1   <- which(ok & data$O_FLAG==1 & data$L_FLAG==1)
        }
        if(mode==6){
          id_FLAG_1   <- which(ok & data$FLAG==1 & data$SHT21_RH<=90)
        }
        
        n_id_FLAG_0 <- length(id_FLAG_0)
        n_id_FLAG_1 <- length(id_FLAG_1)
        
        if(n_id_FLAG_0==0 | n_id_FLAG_1==0){
          next
        }
        
        
        RMSE_0 <- NA
        RMSE_1 <- NA
        RMSE_C <- NA
        COR_0  <- NA
        COR_1  <- NA
        fit_1  <- NULL
        c1     <- NA
        c2     <- NA
        
        if(n_id_FLAG_0>2){
          RMSE_0 <- sqrt(sum((residuals[id_FLAG_0])^2)/n_id_FLAG_0)
          COR_0  <- cor(x = data$measurements[id_FLAG_0],y = data$CO2_CAL_ADJ[id_FLAG_0],method = "pearson",use = "complete.obs")
          
          Q000_A <- quantile(residuals[id_FLAG_0],probs=0.00)
          Q001_A <- quantile(residuals[id_FLAG_0],probs=0.01)
          Q005_A <- quantile(residuals[id_FLAG_0],probs=0.05)
          Q050_A <- quantile(residuals[id_FLAG_0],probs=0.50)
          Q095_A <- quantile(residuals[id_FLAG_0],probs=0.95)
          Q099_A <- quantile(residuals[id_FLAG_0],probs=0.99)
          Q100_A <- quantile(residuals[id_FLAG_0],probs=1.00)
        }
        
        if(n_id_FLAG_1>2){
          RMSE_1 <- sqrt(sum((residuals[id_FLAG_1])^2)/n_id_FLAG_1)
          COR_1  <- cor(x = data$measurements[id_FLAG_1],y = data$CO2_CAL_ADJ[id_FLAG_1],method = "pearson",use = "complete.obs")
          fit_1  <- lm(measurements~CO2_CAL_ADJ,data[id_FLAG_1,])
          c1     <- fit_1$coefficients[1]
          c2     <- fit_1$coefficients[2]
          
          residuals_centered <- data$measurements[id_FLAG_1] - (data$CO2_CAL_ADJ[id_FLAG_1] - (mean(data$CO2_CAL_ADJ[id_FLAG_1]) - mean(data$measurements[id_FLAG_1])))
          
          RMSE_C <- sqrt(sum((residuals_centered)^2)/n_id_FLAG_1)
          
          Q000 <- quantile(residuals[id_FLAG_1],probs=0.00)
          Q001 <- quantile(residuals[id_FLAG_1],probs=0.01)
          Q005 <- quantile(residuals[id_FLAG_1],probs=0.05)
          Q050 <- quantile(residuals[id_FLAG_1],probs=0.50)
          Q095 <- quantile(residuals[id_FLAG_1],probs=0.95)
          Q099 <- quantile(residuals[id_FLAG_1],probs=0.99)
          Q100 <- quantile(residuals[id_FLAG_1],probs=1.00)
        }
        
        statistics <- rbind(statistics,data.frame(LocationName      = tbl_depl$LocationName[ith_depl],
                                                  HPP_SensorUnit_ID = tbl_depl$SensorUnit_ID[ith_depl],
                                                  SensorUnit_ID = u_SensorUnit_ID[ith_SU],
                                                  MODE          = mode,
                                                  WEEK          = week_mode,
                                                  Date_UTC_from = strftime(min(data$date[id_FLAG_0]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                  Date_UTC_to   = strftime(max(data$date[id_FLAG_0]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                  N             = n_id_FLAG_1,
                                                  N_ALL         = n_id_FLAG_0,
                                                  FRACTION_OK   = n_id_FLAG_1/n_id_FLAG_0*1e2,
                                                  RMSE          = RMSE_1,
                                                  RMSE_ALL      = RMSE_0,
                                                  RMSE_P        = RMSE_C,
                                                  COR           = COR_1,
                                                  COR_ALL       = COR_0,
                                                  SLOPE         = c2,
                                                  INTERCEPT     = c1,
                                                  Q000          = Q000,
                                                  Q001          = Q001,
                                                  Q005          = Q005,
                                                  Q050          = Q050,
                                                  Q095          = Q095,
                                                  Q099          = Q099,
                                                  Q100          = Q100,
                                                  Q000_ALL      = Q000_A,
                                                  Q001_ALL      = Q001_A,
                                                  Q005_ALL      = Q005_A,
                                                  Q050_ALL      = Q050_A,
                                                  Q095_ALL      = Q095_A,
                                                  Q099_ALL      = Q099_A,
                                                  Q100_ALL      = Q100_A,
                                                  stringsAsFactors=F))
        
        
        if(week_mode!=0){
          next
        }
        
        # PLOT: SCATTER/HIST
        
        str_L_00 <- paste("SLOPE:    ",sprintf("%6.2f",c2))
        str_L_01 <- paste("INTERCEPT:",sprintf("%6.2f",c1))
        str_L_02 <- paste("RMSE:     ",sprintf("%6.1f",RMSE_1))
        str_L_03 <- paste("RMSE P:   ",sprintf("%6.1f",RMSE_C))
        str_L_04 <- paste("COR:      ",sprintf("%6.1f",COR_1))
        str_L_05 <- paste("N:        ",sprintf("%6.0f",n_id_FLAG_1))
        str_L_06 <- paste("N ALL:    ",sprintf("%6.0f",n_id_FLAG_0))
        
        # str_L_05 <- paste("RMSE  A:",sprintf("%6.1f",RMSE_0))
        # str_L_06 <- paste("COR   A:",sprintf("%6.1f",COR_0))
        # str_L_07 <- paste("N     A:",sprintf("%6.0f",n_id_FLAG_0))
        
        
        str_R_00 <- paste("MEAN:",sprintf("%6.1f",mean(residuals[id_FLAG_1])))
        str_R_01 <- paste("Q000:",sprintf("%6.1f",quantile(residuals[id_FLAG_1],probs=0.00)))
        str_R_02 <- paste("Q001:",sprintf("%6.1f",quantile(residuals[id_FLAG_1],probs=0.01)))
        str_R_03 <- paste("Q050:",sprintf("%6.1f",quantile(residuals[id_FLAG_1],probs=0.50)))
        str_R_04 <- paste("Q099:",sprintf("%6.1f",quantile(residuals[id_FLAG_1],probs=0.99)))
        str_R_05 <- paste("Q100:",sprintf("%6.1f",quantile(residuals[id_FLAG_1],probs=1.00)))
        str_R_06 <- paste("N:   ",sprintf("%6.0f",n_id_FLAG_1))
        str_R_07 <- paste("RMSE:",sprintf("%6.1f",RMSE_1))
        
        
        # SCATTER + HIST
        
        figname <- paste(resultdir,DESC,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),".pdf",sep="")
        
        mainStr <- paste(DESC," ",u_SensorUnit_ID[ith_SU],sep="")
        subStr  <- paste("Data: ",
                         strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+min(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),
                         "-",
                         strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+max(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),sep="")
        
        def_par <- par()
        pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
        par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,2))
        
        plot(0,0,xlim=c(350,700),ylim=c(350,700),main=mainStr,xlab=expression(paste("CO"[2]*" HPP [ppm]")),ylab=expression(paste("CO"[2]*" LP8 [ppm]")),sub=subStr,cex.axis=1.25,cex.lab=1.25,cex.main=1.25)
        
        if(any(data$FLAG==0)){
          points(data$CO2_CAL_ADJ[id_FLAG_0],data$measurements[id_FLAG_0],col="gray70",pch=16,cex=0.5)
        }
        if(any(data$FLAG==1)){
          points(data$CO2_CAL_ADJ[id_FLAG_1],data$measurements[id_FLAG_1],col="black",pch=16,cex=0.5)
        }
        
        lines(c(0,1e4),c(c1,c1+1e4*c2),col=2)
        
        if(F){
          lines(c(0,1e4),c(0,1e4),   col=1)
          lines(c(0,1e4),c(0,1e4)+20,col=1,lty=50)
          lines(c(0,1e4),c(0,1e4)-20,col=1,lty=50)
        }else{
          lines(c(0,1e4),c(0,1e4),   col=1)
          lines(c(0,1e4),c(0,1e4)+RMSE_1,col=1,lty=50)
          lines(c(0,1e4),c(0,1e4)-RMSE_1,col=1,lty=50)
        }
        par(family="mono")
        legend("bottomright",legend=c(str_L_00,str_L_01,"",str_L_02,str_L_03,str_L_04,str_L_05,str_L_06),bg="white",cex=1.25)
        par(family="")
        
        #
        
        Q050_S <- quantile(residuals[id_FLAG_1],probs=0.50)
        MEAN_S <- mean(residuals[id_FLAG_1])
        
        if(n_id_FLAG_1>2){
          hist(residuals[id_FLAG_1],seq(floor(min(residuals[id_FLAG_1])),ceiling(max(residuals[id_FLAG_1]))+2,2),xlim=c(Q050_S-40,Q050_S+40),col="slategray",main="",xlab=expression(paste("CO"[2]*" LP8 - CO"[2]*" HPP [ppm]")),ylab="Number",cex.axis=1.25,cex.lab=1.25)
          
          
          lines(c(MEAN_S,MEAN_S),c(-1e9,1e9),col=2,lwd=3)
          if(F){
            lines(c(MEAN_S+20,MEAN_S+20),c(-1e9,1e9),col=2,lwd=3,lty=5)
            lines(c(MEAN_S-20,MEAN_S-20),c(-1e9,1e9),col=2,lwd=3,lty=5)
          }else{
            lines(c(MEAN_S+RMSE_1,MEAN_S+RMSE_1),c(-1e9,1e9),col=2,lwd=3,lty=5)
            lines(c(MEAN_S-RMSE_1,MEAN_S-RMSE_1),c(-1e9,1e9),col=2,lwd=3,lty=5)
          }
          if(F){
            par(family="mono")
            legend("topright",legend=c(str_R_00,str_R_01,str_R_02,str_R_03,str_R_04,str_R_05),bg="white",cex=1.25)
            par(family="")
          }else{
            par(family="mono")
            legend("topright",legend=c(str_R_00,str_R_03,str_R_07),bg="white",cex=1.25)
            par(family="")
          }
        }else{
          plot.new()
        }
        
        #
        
        dev.off()
        par(def_par)
        
        
        # SCATTER
        
        figname <- paste(resultdir,DESC,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_SCATTER.pdf",sep="")
        
        mainStr <- paste(DESC," ",u_SensorUnit_ID[ith_SU],sep="")
        subStr  <- paste("Data: ",
                         strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+min(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),
                         "-",
                         strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+max(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),sep="")
        
        def_par <- par()
        pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
        par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,1))
        
        plot(0,0,xlim=c(350,700),ylim=c(350,700),main=mainStr,xlab=expression(paste("CO"[2]*" HPP [ppm]")),ylab=expression(paste("CO"[2]*" LP8 [ppm]")),sub=subStr,cex.axis=1.25,cex.lab=1.25,cex.main=1.25)
        
        if(any(data$FLAG==0)){
          points(data$CO2_CAL_ADJ[id_FLAG_0],data$measurements[id_FLAG_0],col="gray70",pch=16,cex=0.5)
        }
        if(any(data$FLAG==1)){
          points(data$CO2_CAL_ADJ[id_FLAG_1],data$measurements[id_FLAG_1],col="black",pch=16,cex=0.5)
        }
        
        lines(c(0,1e4),c(c1,c1+1e4*c2),col=2)
        
        if(F){
          lines(c(0,1e4),c(0,1e4),   col=1)
          lines(c(0,1e4),c(0,1e4)+20,col=1,lty=50)
          lines(c(0,1e4),c(0,1e4)-20,col=1,lty=50)
        }else{
          lines(c(0,1e4),c(0,1e4),   col=1)
          lines(c(0,1e4),c(0,1e4)+RMSE_1,col=1,lty=50)
          lines(c(0,1e4),c(0,1e4)-RMSE_1,col=1,lty=50)
        }
        par(family="mono")
        legend("bottomright",legend=c(str_L_00,str_L_01,"",str_L_02,str_L_03,str_L_04,str_L_05,str_L_06),bg="white",cex=1.25)
        par(family="")
        
        dev.off()
        par(def_par)
        
        
        # HIST
        
        figname <- paste(resultdir,DESC,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_HIST.pdf",sep="")
        
        mainStr <- paste(DESC," ",u_SensorUnit_ID[ith_SU],sep="")
        subStr  <- paste("Data: ",
                         strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+min(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),
                         "-",
                         strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+max(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),sep="")
        
        def_par <- par()
        pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
        par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,1))
        
        Q050_S <- quantile(residuals[id_FLAG_1],probs=0.50)
        MEAN_S <- mean(residuals[id_FLAG_1])
        
        if(n_id_FLAG_1>2){
          hist(residuals[id_FLAG_1],seq(floor(min(residuals[id_FLAG_1])),ceiling(max(residuals[id_FLAG_1]))+2,2),xlim=c(Q050_S-40,Q050_S+40),col="slategray",main="",xlab=expression(paste("CO"[2]*" LP8 - CO"[2]*" HPP [ppm]")),ylab="Number",cex.axis=1.25,cex.lab=1.25)
          
          
          lines(c(MEAN_S,MEAN_S),c(-1e9,1e9),col=2,lwd=3)
          if(F){
            lines(c(MEAN_S+20,MEAN_S+20),c(-1e9,1e9),col=2,lwd=3,lty=5)
            lines(c(MEAN_S-20,MEAN_S-20),c(-1e9,1e9),col=2,lwd=3,lty=5)
          }else{
            lines(c(MEAN_S+RMSE_1,MEAN_S+RMSE_1),c(-1e9,1e9),col=2,lwd=3,lty=5)
            lines(c(MEAN_S-RMSE_1,MEAN_S-RMSE_1),c(-1e9,1e9),col=2,lwd=3,lty=5)
          }
          if(F){
            par(family="mono")
            legend("topright",legend=c(str_R_00,str_R_01,str_R_02,str_R_03,str_R_04,str_R_05),bg="white",cex=1.25)
            par(family="")
          }else{
            par(family="mono")
            legend("topright",legend=c(str_R_00,str_R_03,str_R_07),bg="white",cex=1.25)
            par(family="")
          }
        }else{
          plot.new()
        }
        
        dev.off()
        par(def_par)
        
        # PLOT: TS
        
        figname    <- paste(resultdir,DESC,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_TS.pdf",sep="")
        
        tmp1            <- rep(NA,dim(data)[1])
        tmp1[id_FLAG_1] <- data$measurements[id_FLAG_1]
        
        yyy        <- cbind(data$CO2_CAL_ADJ,
                            data$measurements,
                            tmp1)
        
        xlabString <- "Date"
        ylabString <- expression(paste("CO"[2]*" [ppm]"))
        legend_str <- c("HPP CO2","LP8 CO2 [ALL]","LP8 CO2 [USE]")
        plot_ts(figname,data$date,yyy,"week",NULL,c(350,700),xlabString,ylabString,legend_str)
        
        
        if(mode==1){
          
          # PLOT: RH TS
          
          figname    <- paste(resultdir,DESC,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_RH_TS.pdf",sep="")
          
          yyy        <- cbind(data$SHT21_RH)
          
          xlabString <- "Date"
          ylabString <- expression(paste("RH [%]"))
          legend_str <- c("SHT21 RH")
          plot_ts(figname,data$date,yyy,"week",NULL,c(0,102),xlabString,ylabString,legend_str)
          
          # PLOT: T TS
          
          figname    <- paste(resultdir,DESC,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_T_TS.pdf",sep="")
          
          yyy        <- cbind(data$SHT21_T)
          
          xlabString <- "Date"
          ylabString <- expression(paste("T [deg C]"))
          legend_str <- c("SHT21 T")
          plot_ts(figname,data$date,yyy,"week",NULL,c(0,102),xlabString,ylabString,legend_str)
        }
        
        ## RESIDUALS
        
        # RESIDUAL - SHT21 T 
        
        bin_delta<- 2 
        bins     <- seq(-50,50,bin_delta)
        n_bins   <- length(bins)
        Q025_bin <- rep(NA,n_bins)
        Q050_bin <- rep(NA,n_bins)
        Q075_bin <- rep(NA,n_bins)
        MID_bin  <- rep(NA,n_bins)
        N_bin    <- rep(NA,n_bins)
        
        for(ith_bin in 1:n_bins){
          id_bin         <- which(data$SHT21_T[id_FLAG_1]>=bins[ith_bin] & data$SHT21_T[id_FLAG_1]<(bins[ith_bin]+bin_delta))
          N_bin[ith_bin] <- length(id_bin)
          
          if(N_bin[ith_bin]>10){
            Q025_bin[ith_bin] <- quantile(x = (data$measurements[id_FLAG_1]-data$CO2_CAL_ADJ[id_FLAG_1])[id_bin],probs=0.25,na.rm=T)
            Q050_bin[ith_bin] <- quantile(x = (data$measurements[id_FLAG_1]-data$CO2_CAL_ADJ[id_FLAG_1])[id_bin],probs=0.50,na.rm=T)
            Q075_bin[ith_bin] <- quantile(x = (data$measurements[id_FLAG_1]-data$CO2_CAL_ADJ[id_FLAG_1])[id_bin],probs=0.75,na.rm=T)
            MID_bin[ith_bin]  <- bins[ith_bin] + 1/2*bin_delta
          }
        }
        
        figname <- paste(resultdir,DESC,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_RESIUDAL_SHT21T.pdf",sep="")
        
        mainStr <- paste(u_SensorUnit_ID[ith_SU]," @ ",DESC," ",eq_label,sep="")
        
        subStr  <- paste("Data: ",
                         strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+min(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),
                         "-",
                         strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+max(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),sep="")
        
        
        def_par <- par()
        pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
        par(mai=c(1.25,1,1.0,1.0),mfrow=c(1,1))
        
        plot(0,0,xlim=c(-15,45),ylim=c(-50,50),main=mainStr,ylab=expression(paste("CO"[2]*" LP8 - CO"[2]*" HPP [ppm]")),xlab=expression(paste("SHT21 T [deg C]")),cex.axis=1.75,cex.lab=1.75,cex.main=1.75)
        
        if(any(data$FLAG==0)){
          points(data$SHT21_T[id_FLAG_0],data$measurements[id_FLAG_0]-data$CO2_CAL_ADJ[id_FLAG_0],col="gray70",pch=16,cex=0.5)
        }
        if(any(data$FLAG==1)){
          points(data$SHT21_T[id_FLAG_1],data$measurements[id_FLAG_1]-data$CO2_CAL_ADJ[id_FLAG_1],col="black",pch=16,cex=0.5)
        }
        
        lines(c(-1e3,1e3),c(0,0),col="grey70",lwd=2,lty=1)
        
        lines(MID_bin,Q025_bin,lwd=2,lty=5,col=2)
        lines(MID_bin,Q050_bin,lwd=2,lty=5,col=2)
        lines(MID_bin,Q075_bin,lwd=2,lty=5,col=2)
        
        par(new=T)
        
        plot(MID_bin,N_bin,xlim=c(-15,45),pch=15,cex=1.5,col=2,xaxt="n",yaxt="n",xlab="",ylab="")
        axis(side = 4,cex.axis=1.75,cex.lab=1.75)
        mtext(text="Number of measurements",side = 4,line = 3.5,cex=1.75)
        
        mtext(text = subStr,side = 1,line = 4.5,cex=1.5)
        
        par(family="mono")
        legend("bottomright",legend=c(str_L_02,str_L_05,str_L_06),bg="white",cex=1.75)
        par(family="")
        
        dev.off()
        par(def_par)
        
        
        
        # RESIDUAL - SHT21 RH 
        
        bin_delta<- 2.5 
        bins     <- seq(0,100,bin_delta)
        n_bins   <- length(bins)
        Q025_bin <- rep(NA,n_bins)
        Q050_bin <- rep(NA,n_bins)
        Q075_bin <- rep(NA,n_bins)
        MID_bin  <- rep(NA,n_bins)
        N_bin    <- rep(NA,n_bins)
        
        for(ith_bin in 1:n_bins){
          id_bin         <- which(data$SHT21_RH[id_FLAG_1]>=bins[ith_bin] & data$SHT21_RH[id_FLAG_1]<(bins[ith_bin]+bin_delta))
          N_bin[ith_bin] <- length(id_bin)
          
          if(N_bin[ith_bin]>10){
            Q025_bin[ith_bin] <- quantile(x = (data$measurements[id_FLAG_1]-data$CO2_CAL_ADJ[id_FLAG_1])[id_bin],probs=0.25,na.rm=T)
            Q050_bin[ith_bin] <- quantile(x = (data$measurements[id_FLAG_1]-data$CO2_CAL_ADJ[id_FLAG_1])[id_bin],probs=0.50,na.rm=T)
            Q075_bin[ith_bin] <- quantile(x = (data$measurements[id_FLAG_1]-data$CO2_CAL_ADJ[id_FLAG_1])[id_bin],probs=0.75,na.rm=T)
            MID_bin[ith_bin]  <- bins[ith_bin] + 1/2*bin_delta
          }
        }
        
        figname <- paste(resultdir,DESC,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_RESIUDAL_SHT21RH.pdf",sep="")
        
        mainStr <- paste(u_SensorUnit_ID[ith_SU]," @ ",DESC," ",eq_label,sep="")
        
        
        subStr  <- paste("Data: ",
                         strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+min(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),
                         "-",
                         strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+max(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),sep="")
        
        def_par <- par()
        pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
        par(mai=c(1.25,1,1.0,1.0),mfrow=c(1,1))
        
        plot(-100,0,xlim=c(0,105),ylim=c(-50,50),main=mainStr,ylab=expression(paste("CO"[2]*" LP8 - CO"[2]*" HPP [ppm]")),xlab=expression(paste("SHT21 RH [%]")),cex.axis=1.75,cex.lab=1.75,cex.main=1.75)
        
        if(any(data$FLAG==0)){
          points(data$SHT21_RH[id_FLAG_0],data$measurements[id_FLAG_0]-data$CO2_CAL_ADJ[id_FLAG_0],col="gray70",pch=16,cex=0.5)
        }
        if(any(data$FLAG==1)){
          points(data$SHT21_RH[id_FLAG_1],data$measurements[id_FLAG_1]-data$CO2_CAL_ADJ[id_FLAG_1],col="black",pch=16,cex=0.5)
        }
        
        lines(c(-1e3,1e3),c(0,0),col="grey70",lwd=2,lty=1)
        
        lines(MID_bin,Q025_bin,lwd=2,lty=5,col=2)
        lines(MID_bin,Q050_bin,lwd=2,lty=5,col=2)
        lines(MID_bin,Q075_bin,lwd=2,lty=5,col=2)
        
        par(new=T)
        
        plot(MID_bin,N_bin,xlim=c(0,105),pch=15,cex=1.5,col=2,xaxt="n",yaxt="n",xlab="",ylab="")
        axis(side = 4,cex.axis=1.75,cex.lab=1.75)
        mtext(text="Number of measurements",side = 4,line = 3.5,cex=1.75)
        
        mtext(text = subStr,side = 1,line = 4.5,cex=1.5)
        
        par(family="mono")
        legend("bottomright",legend=c(str_L_02,str_L_05,str_L_06),bg="white",cex=1.75)
        par(family="")
        
        
        dev.off()
        par(def_par)
        
      }
    }
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

write.table(statistics,paste(resultdir,"statistics.csv",sep=""),col.names = T,row.names = F,sep=";")



