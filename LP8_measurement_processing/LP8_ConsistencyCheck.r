# LP8_ConsistencyCheck.r
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

source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

### ------------------------------------------------------------------------------------------------------------------------------

## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

# 

if(!as.integer(args[1])%in%c(1,2,3,20,30)){
  stop("MODEL: 1, 2, 3, 20, 30!")
}else{
  
  # Standard 
  if(as.integer(args[1])==1){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/"
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2"
  }
  
  # Test 00
  if(as.integer(args[1])==2){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/"
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_TEST00"
  }
  
  # Test 01
  if(as.integer(args[1])==3){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/"
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_TEST01"
  }
  
  # Test 00 AMT
  if(as.integer(args[1])==20){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT/"
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_TEST00_AMT"
  }
  
  # Test 01 AMT
  if(as.integer(args[1])==30){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT/"
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_TEST01_AMT"
  }
}

if(length(args)==2 & args[2]=="DUE"){
  COMP_DUE <- T
}else{
  COMP_DUE <- F
}


### ----------------------------------------------------------------------------------------------------------------------------

# Classification file

SSC <- read.table(file = "/project/CarboSense/Carbosense_Network/SpatialSiteClassification/SpatialSiteClassification.csv",
                  sep=";",header=T,as.is=T)

### ----------------------------------------------------------------------------------------------------------------------------

# Validation period

Date_UTC_from  <- strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC")
Date_UTC_to    <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")

timestamp_from <- as.numeric(difftime(time1=Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
timestamp_to   <- as.numeric(difftime(time1=Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

all_timestamps <- seq(timestamp_from,timestamp_to,6*3600)
REF_HPP        <- data.frame(timestamp=all_timestamps)

### ----------------------------------------------------------------------------------------------------------------------------

## Database queries

# Location

query_str         <- paste("SELECT * from Location;",sep="")
drv               <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res               <- dbSendQuery(con, query_str)
tbl_location      <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)


# Deployment

query_str         <- paste("SELECT * from Deployment;",sep="")
drv               <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res               <- dbSendQuery(con, query_str)
tbl_deployment    <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_deployment$Date_UTC_from <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to   <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

tbl_deployment$timestamp_from <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
tbl_deployment$timestamp_to   <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))


# HPPs (available data in table "CarboSense_HPP_CO2")

query_str         <- paste("SELECT DISTINCT SensorUnit_ID, LocationName from CarboSense_HPP_CO2 WHERE LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1');",sep="")
drv               <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res               <- dbSendQuery(con, query_str)
HPP_SU_SITES      <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

HPP_SU_SITES$desc <- paste(HPP_SU_SITES$LocationName,"_",HPP_SU_SITES$SensorUnit_ID,sep="")

n_HPPs <- dim(HPP_SU_SITES)[1]

#

REF_sites      <- c("DUE","PAY","LAEG","RIG","BRM","HAE","GIMM")
n_REF_sites    <- length(REF_sites)
n_REF_HPP      <- n_HPPs + n_REF_sites

### ----------------------------------------------------------------------------------------------------------------------------

# Import REF+HPP data / computation of percentiles

for(ref_site in c(REF_sites,HPP_SU_SITES$desc)){
  
  if(ref_site%in%HPP_SU_SITES$desc){
    
    id_HPP <- which(ref_site==HPP_SU_SITES$desc)
    
    query_str    <- paste("SELECT timestamp, CO2_CAL_ADJ FROM CarboSense_HPP_CO2 WHERE CO2_CAL_ADJ != -999 AND Valve=0 and LocationName = '",HPP_SU_SITES$LocationName[id_HPP],"' ",sep="")
    query_str    <- paste(query_str, "AND SensorUnit_ID = ",HPP_SU_SITES$SensorUnit_ID[id_HPP]," AND timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to,";",sep="")
    drv          <- dbDriver("MySQL")
    con<-carboutil::get_conn(group="CarboSense_MySQL")
    res          <- dbSendQuery(con, query_str)
    tbl_REF      <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    colnames(tbl_REF)[which(colnames(tbl_REF)=="CO2_CAL_ADJ")] <- "CO2"
    
  }
  
  #
  
  if(ref_site %in% c("DUE")){
    query_str   <- paste("SELECT timestamp, CO2_DRY_CAL, H2O FROM ",paste("NABEL_",ref_site,sep="")," WHERE CO2_DRY_CAL != -999 AND CO2_DRY != -999 AND H2O != -999 ",sep="")
    query_str   <- paste(query_str," AND timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to,";",sep="")
    drv         <- dbDriver("MySQL")
    con<-carboutil::get_conn(group="CarboSense_MySQL")
    res         <- dbSendQuery(con, query_str)
    tbl_REF     <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    colnames(tbl_REF)[which(colnames(tbl_REF)=="CO2_DRY_CAL")] <- "CO2_DRY"
    
    tbl_REF     <- tbl_REF[order(tbl_REF$timestamp),]
    tbl_REF$CO2 <- tbl_REF$CO2_DRY * (1 - tbl_REF$H2O/100)
    tbl_REF     <- tbl_REF[,c(which(colnames(tbl_REF)=="timestamp"),which(colnames(tbl_REF)=="CO2"))]
  }
  
  if(ref_site %in% c("RIG","PAY","HAE")){
    query_str   <- paste("SELECT timestamp, CO2_WET_COMP FROM ",paste("NABEL_",ref_site,sep="")," WHERE CO2_WET_COMP != -999 AND CO2>350 ",sep="")
    query_str   <- paste(query_str, " AND timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to,";",sep="")
    drv         <- dbDriver("MySQL")
    con<-carboutil::get_conn(group="CarboSense_MySQL")
    res         <- dbSendQuery(con, query_str)
    tbl_REF     <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    colnames(tbl_REF)[which(colnames(tbl_REF)=="CO2_WET_COMP")] <- "CO2"

    tbl_REF      <- tbl_REF[order(tbl_REF$timestamp),]
  }
  
  # if(ref_site == "PAY"){
  #   query_str   <- paste("SELECT timestamp, CO2_DRY_CAL, H2O_CAL FROM ",paste("NABEL_",ref_site,sep="")," WHERE CO2_DRY_CAL != -999 AND CO2_DRY != -999 ",sep="")
  #   query_str   <- paste(query_str, "AND H2O_CAL != -999 AND timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to,";",sep="")
  #   drv         <- dbDriver("MySQL")
  #   con<-carboutil::get_conn(group="CarboSense_MySQL")
  #   res         <- dbSendQuery(con, query_str)
  #   tbl_REF     <- fetch(res, n=-1)
  #   dbClearResult(res)
  #   dbDisconnect(con)
  #   
  #   colnames(tbl_REF)[which(colnames(tbl_REF)=="CO2_DRY_CAL")] <- "CO2_DRY"
  #   colnames(tbl_REF)[which(colnames(tbl_REF)=="H2O_CAL")]     <- "H2O"
  #   
  #   tbl_REF     <- tbl_REF[order(tbl_REF$timestamp),]
  #   tbl_REF$CO2 <- tbl_REF$CO2_DRY * (1 - tbl_REF$H2O/100)
  #   tbl_REF     <- tbl_REF[,c(which(colnames(tbl_REF)=="timestamp"),which(colnames(tbl_REF)=="CO2"))]
  # }
  # 
  # if(ref_site %in% c("HAE")){
  #   query_str   <- paste("SELECT timestamp, CO2 FROM ",paste("NABEL_",ref_site,sep="")," WHERE CO2 != -999 AND CO2>350 ",sep="")
  #   query_str   <- paste(query_str, " AND timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to,";",sep="")
  #   drv         <- dbDriver("MySQL")
  #   con<-carboutil::get_conn(group="CarboSense_MySQL")
  #   res         <- dbSendQuery(con, query_str)
  #   tbl_REF     <- fetch(res, n=-1)
  #   dbClearResult(res)
  #   dbDisconnect(con)
  #   
  #   tbl_REF      <- tbl_REF[order(tbl_REF$timestamp),]
  # }
  
  if(ref_site == "LAEG"){
    query_str    <- paste("SELECT timestamp, CO2 FROM ",paste("EMPA_",ref_site,sep="")," WHERE CO2 != -999 ",sep="")
    query_str    <- paste(query_str,"AND timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to,";",sep="")
    drv          <- dbDriver("MySQL")
    con<-carboutil::get_conn(group="CarboSense_MySQL")
    res          <- dbSendQuery(con, query_str)
    tbl_REF      <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    tbl_REF      <- tbl_REF[order(tbl_REF$timestamp),]
  }
  
  if(ref_site == "BRM"){
    query_str    <- paste("SELECT timestamp, CO2 FROM ",paste("UNIBE_",ref_site,sep="")," WHERE MEAS_HEIGHT = 12 AND CO2_DRY_N > 5 AND CO2 != -999 ",sep="")
    query_str    <- paste(query_str,"AND timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to,";",sep="")
    drv          <- dbDriver("MySQL")
    con<-carboutil::get_conn(group="CarboSense_MySQL")
    res          <- dbSendQuery(con, query_str)
    tbl_REF      <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    tbl_REF      <- tbl_REF[order(tbl_REF$timestamp),]
  }
  
  if(ref_site == "GIMM"){
    query_str     <- paste("SELECT timestamp, CO2 FROM ",paste("UNIBE_",ref_site,sep="")," WHERE CO2 != -999 ",sep="")
    query_str     <- paste(query_str, "AND timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to,";",sep="")
    drv           <- dbDriver("MySQL")
    con<-carboutil::get_conn(group="CarboSense_MySQL")
    res           <- dbSendQuery(con, query_str)
    tbl_REF       <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    tbl_REF      <- tbl_REF[order(tbl_REF$timestamp),]
  }
  
  if(dim(tbl_REF)[1]==0){
    next
  }
  
  
  tbl_REF$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_REF$timestamp
  
  # Average to 10 min intervals
  
  tbl_REF <- timeAverage(mydata = tbl_REF,avg.time = "10 min",statistic = "mean",start.date = Date_UTC_from)
  tbl_REF <- as.data.frame(tbl_REF)
  
  # Compute percentiles
  
  tbl_REF        <- data.frame(date = tbl_REF$date,
                               CO2  = tbl_REF$CO2,
                               stringsAsFactors = F)
  
  tbl_REF_n      <- data.frame(date = tbl_REF$date,
                               n    = as.numeric(!is.na(tbl_REF$CO2)),
                               stringsAsFactors = F)
  
  tbl_REF$date   <- as.POSIXct(tbl_REF$date)
  tbl_REF_n$date <- as.POSIXct(tbl_REF_n$date)
  
  df_00   <- timeAverage(mydata = tbl_REF,avg.time = "24 hour", statistic = "percentile",percentile = 10, start.date = as.POSIXct(Date_UTC_from - 86400))
  df_06   <- timeAverage(mydata = tbl_REF,avg.time = "24 hour", statistic = "percentile",percentile = 10, start.date = as.POSIXct(Date_UTC_from - 64800))
  df_12   <- timeAverage(mydata = tbl_REF,avg.time = "24 hour", statistic = "percentile",percentile = 10, start.date = as.POSIXct(Date_UTC_from - 43200))
  df_18   <- timeAverage(mydata = tbl_REF,avg.time = "24 hour", statistic = "percentile",percentile = 10, start.date = as.POSIXct(Date_UTC_from - 21600))
  
  
  df_00_n <- timeAverage(mydata = tbl_REF_n,avg.time = "24 hour",statistic = "sum",start.date = as.POSIXct(Date_UTC_from - 86400))
  df_06_n <- timeAverage(mydata = tbl_REF_n,avg.time = "24 hour",statistic = "sum",start.date = as.POSIXct(Date_UTC_from - 64800))
  df_12_n <- timeAverage(mydata = tbl_REF_n,avg.time = "24 hour",statistic = "sum",start.date = as.POSIXct(Date_UTC_from - 43200))
  df_18_n <- timeAverage(mydata = tbl_REF_n,avg.time = "24 hour",statistic = "sum",start.date = as.POSIXct(Date_UTC_from - 21600))
  
  df      <- rbind(df_00,  df_06,  df_12,  df_18)
  df_n    <- rbind(df_00_n,df_06_n,df_12_n,df_18_n)
  
  df      <- df[  order(df$date),]
  df_n    <- df_n[order(df_n$date),]
  
  if(any(df$date!=df_n$date)){
    stop("Error in correspondence between data frames df and df_n.")
  }
  
  df$timestamp <- as.numeric(difftime(time1=df$date,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")) + 86400

  #

  median_n <- median(df_n$n,na.rm=T)
  
  #
  
  if(ref_site%in%HPP_SU_SITES$desc){
    id_ok <- which(df_n$n>=22.5/24*median_n)
  }else{
    id_ok <- which(df_n$n>=23/24*median_n)
  }
  
  df    <- df[id_ok,]
  
  #
  
  id_A_B <- which(REF_HPP$timestamp %in% df$timestamp)
  id_B_A <- which(df$timestamp %in% REF_HPP$timestamp)
  
  REF_HPP$newCol         <- NA
  REF_HPP$newCol[id_A_B] <- df$CO2[id_B_A]
  
  colnames(REF_HPP) <- c(colnames(REF_HPP)[1:(dim(REF_HPP)[2]-1)],ref_site)
  
  #
  
  rm(df,df_00,df_06,df_12,df_18,df_00_n,df_06_n,df_12_n,df_18_n,median_n,tbl_REF,tbl_REF_n)
  gc()
}

### ----------------------------------------------------------------------------------------------------------------------------

REF_HPP$date  <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + REF_HPP$timestamp

### ----------------------------------------------------------------------------------------------------------------------------

# Spatial classification of REF / HPP sites

REF_HPP_CANTON   <- rep(NA,dim(REF_HPP)[2])
REF_HPP_ALTITUDE <- rep(NA,dim(REF_HPP)[2])
REF_HPP_SSC      <- rep(NA,dim(REF_HPP)[2])

for(ith_col in 1:dim(REF_HPP)[2]){
  
  if(colnames(REF_HPP)[ith_col] %in% c("date","timestamp")){
    next
  }
  
  ref_site <- colnames(REF_HPP)[ith_col]
  
  if(ref_site%in%HPP_SU_SITES$desc){
    id_HPP                    <- which(HPP_SU_SITES$desc==ref_site)
    id_loc                    <- which(tbl_location$LocationName==HPP_SU_SITES$LocationName[id_HPP])
    REF_HPP_CANTON[ith_col]   <- tbl_location$Canton[id_loc]
    REF_HPP_ALTITUDE[ith_col] <- tbl_location$h[id_loc]
    
    #
    
    id_SSC_CH                 <- which(HPP_SU_SITES$SensorUnit_ID[id_HPP]  == SSC$SensorUnit_ID
                                       & HPP_SU_SITES$LocationName[id_HPP] == SSC$LocationName
                                       & min(REF_HPP$timestamp[which(!is.na(REF_HPP[,ith_col]))]) >= SSC$timestamp_from
                                       & max(REF_HPP$timestamp[which(!is.na(REF_HPP[,ith_col]))]) <= SSC$timestamp_to)
    
    if(length(id_SSC_CH)==1){
      REF_HPP_SSC[ith_col] <- SSC$CH[id_SSC_CH]
    }else{
      REF_HPP_SSC[ith_col] <- "NOT_DEFINED"
    }
    
    print(paste(ref_site,REF_HPP_SSC[ith_col]))
    
  }else{
    
    ref_site_locationName   <- ref_site
    if(ref_site=="DUE"){
      ref_site_locationName <- "DUE1"
    }
    if(ref_site=="PAY"){
      ref_site_locationName <- "PAYN"
    }
    
    id_loc                    <- which(tbl_location$LocationName==ref_site_locationName)
    REF_HPP_CANTON[ith_col]   <- tbl_location$Canton[id_loc]
    REF_HPP_ALTITUDE[ith_col] <- tbl_location$h[id_loc]
    
    #
    
    id_SSC_CH                 <- which(is.na(SSC$HeightAboveGround) & ref_site_locationName == SSC$LocationName)
    
    if(length(id_SSC_CH)==1){
      REF_HPP_SSC[ith_col] <- SSC$CH[id_SSC_CH]
    }else{
      REF_HPP_SSC[ith_col] <- "NOT_DEFINED"
    }
    
    print(paste(ref_site,REF_HPP_SSC[ith_col]))
  }
}


### ----------------------------------------------------------------------------------------------------------------------------

# Plot daily CO2 mimimum time series

if(T){
  if(!COMP_DUE){
    figname <- paste(resultdir,"CO2_REF_LOW.pdf",sep="")
  }
  if(COMP_DUE){
    figname <- paste(resultdir,"CO2_REF_LOW_DUE1.pdf",sep="")
  }
  
  id      <- which(!colnames(REF_HPP)%in%c("date","timestamp"))
  yyy     <- as.matrix(REF_HPP[,id])
  
  
  xlabString <- "Date" 
  ylabString <- expression(paste("CO"[2]*" [ppm]"))
  legend_str <- c(colnames(REF_HPP)[id])
  plot_ts(figname,REF_HPP$date,yyy,"week",NULL,NULL,xlabString,ylabString,legend_str)
}

### ----------------------------------------------------------------------------------------------------------------------------

# Compute daily CO2 minimum bands for the groups

n_groups <- 3

mean_CO2    <- matrix(NA,ncol=n_groups,nrow=dim(REF_HPP)[1])
median_CO2  <- matrix(NA,ncol=n_groups,nrow=dim(REF_HPP)[1])
rng_CO2     <- matrix(NA,ncol=n_groups,nrow=dim(REF_HPP)[1])
min_CO2_bsl <- matrix(NA,ncol=n_groups,nrow=dim(REF_HPP)[1])
max_CO2_bsl <- matrix(NA,ncol=n_groups,nrow=dim(REF_HPP)[1])
n_sites_grp <- matrix(NA,ncol=n_groups,nrow=dim(REF_HPP)[1])

for(ith_group in 1:n_groups){
  
  if(ith_group==1){
    id_REF_HPP         <- which(REF_HPP_CANTON=="TI")
    rng_CO2_OneRefSite <- 50
    rng_CO2_minimum    <- 15
  }
  
  if(ith_group==2){
    id_REF_HPP         <- which(REF_HPP_CANTON!="TI" & REF_HPP_SSC != "HILLTOP")
    rng_CO2_OneRefSite <- 50
    rng_CO2_minimum    <- 15
  }
  
  if(ith_group==3){
    id_REF_HPP         <- which(REF_HPP_CANTON!="TI" & REF_HPP_SSC == "HILLTOP")
    rng_CO2_OneRefSite <- 20
    rng_CO2_minimum    <- 15
  }
  
  n_sites_grp[,ith_group] <- apply(!is.na(REF_HPP[,id_REF_HPP]),1,sum)
  
  id <- which(n_sites_grp[,ith_group]==1)
  if(length(id)>0){
    mean_CO2[   id,ith_group] <- apply(REF_HPP[id,id_REF_HPP],1,mean,  na.rm=T)
    median_CO2[ id,ith_group] <- apply(REF_HPP[id,id_REF_HPP],1,median,na.rm=T)
    rng_CO2[    id,ith_group] <- rep(rng_CO2_OneRefSite,length(id))
    min_CO2_bsl[id,ith_group] <- median_CO2[id,ith_group] - rng_CO2[id,ith_group]
    max_CO2_bsl[id,ith_group] <- median_CO2[id,ith_group] + rng_CO2[id,ith_group]
  }
  
  id <- which(n_sites_grp[,ith_group]>1)
  if(length(id)>0){
    mean_CO2[   id,ith_group] <- apply(REF_HPP[id,id_REF_HPP],1,mean,  na.rm=T)
    median_CO2[ id,ith_group] <- apply(REF_HPP[id,id_REF_HPP],1,median,na.rm=T)
    rng_CO2[    id,ith_group] <- 2.0*(apply(REF_HPP[id,id_REF_HPP],1,max, na.rm=T) -  apply(REF_HPP[id,id_REF_HPP],1,min, na.rm=T))
    
    id_id <- which(rng_CO2[id,ith_group]<rng_CO2_minimum)
    if(length(id_id)>0){
      rng_CO2[id[id_id],ith_group] <- rng_CO2_minimum
    }
    
    min_CO2_bsl[id,ith_group] <- median_CO2[id,ith_group] - rng_CO2[id,ith_group]
    max_CO2_bsl[id,ith_group] <- median_CO2[id,ith_group] + rng_CO2[id,ith_group]
  }
  
  rm(id,id_id)
  gc()
  
}

# Export limits

if(T){
  for(ith_group in 1:n_groups){
    
    df_tmp <- data.frame(date        = strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+REF_HPP$timestamp,"%Y-%m-%d %H:%M:%S",tz="UTC"),
                         median_CO2  = median_CO2[,ith_group],
                         rng_CO2     = rng_CO2[,ith_group],
                         min_CO2_bsl = min_CO2_bsl[,ith_group],
                         max_CO2_bsl = max_CO2_bsl[,ith_group],
                         stringsAsFactors = F)
    
    
    write.table(df_tmp,file = paste(resultdir,"CO2_REF_LOW_GROUP_",sprintf("%02.0f",ith_group),".csv",sep=""),col.names=T,row.names = F,sep=";")
    
    rm(df_tmp)
    gc()
  }
}


### ----------------------------------------------------------------------------------------------------------------------------

## Validation of LP8 CO2 measurements

# get relevant deployments

if(!COMP_DUE){
  tbl_deployment_LP8 <- tbl_deployment[which(tbl_deployment$SensorUnit_ID >= 1010 & tbl_deployment$SensorUnit_ID <= 1334
                                             & !tbl_deployment$LocationName %in% c('DUE1','DUE2','DUE3','DUE4','DUE5','MET1')
                                             & tbl_deployment$Date_UTC_to   > Date_UTC_from
                                             & tbl_deployment$Date_UTC_from < Date_UTC_to),]
}
if(COMP_DUE){
  tbl_deployment_LP8 <- tbl_deployment[which(tbl_deployment$SensorUnit_ID >= 1010 & tbl_deployment$SensorUnit_ID <= 1334
                                             & tbl_deployment$LocationName   == c('DUE1')
                                             & tbl_deployment$Date_UTC_to   > Date_UTC_from
                                             & tbl_deployment$Date_UTC_from < Date_UTC_to),]
}

id <- which(tbl_deployment_LP8$Date_UTC_from < Date_UTC_from)
if(length(id)>0){
  tbl_deployment_LP8$Date_UTC_from[id] <- Date_UTC_from
}

id <- which(tbl_deployment_LP8$Date_UTC_to > Date_UTC_to)
if(length(id)>0){
  tbl_deployment_LP8$Date_UTC_to[id] <- Date_UTC_to
}

n_depl <- dim(tbl_deployment_LP8)[1]

# Loop over all LP8 deployments

for(ith_depl in 1:n_depl){
  
  # import of LP8 data
  query_str         <- paste("SELECT timestamp, CO2_A, SHT21_RH FROM ",ProcDataTblName," WHERE CO2_A != -999 AND LocationName = '",tbl_deployment_LP8$LocationName[ith_depl],"' ",sep="")
  query_str         <- paste(query_str, " and SensorUnit_ID = ",tbl_deployment_LP8$SensorUnit_ID[ith_depl]," AND timestamp >= ",tbl_deployment_LP8$timestamp_from[ith_depl]," AND timestamp <= ",tbl_deployment_LP8$timestamp_to[ith_depl],";",sep="")
  drv               <- dbDriver("MySQL")
  con<-carboutil::get_conn(group="CarboSense_MySQL")
  res               <- dbSendQuery(con, query_str)
  data              <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  #
  
  if(dim(data)[1]==0){
    next
  }
  
  #
  
  colnames(data)[which(colnames(data)=="CO2_A")] <- "CO2"
  
  # Get site classification / corresponding group
  
  if(tbl_deployment_LP8$LocationName[ith_depl] %in% c("KAST")){
    SSC_SITE <- "NOT_DEFINED"
  }else{
    id_SSC_CH <- which(tbl_deployment_LP8$SensorUnit_ID[ith_depl]  == SSC$SensorUnit_ID
                       & tbl_deployment_LP8$LocationName[ith_depl] == SSC$LocationName
                       & min(data$timestamp) >= SSC$timestamp_from
                       & max(data$timestamp) <= SSC$timestamp_to)
    
    if(length(id_SSC_CH)==1){
      SSC_SITE <- SSC$CH[id_SSC_CH]
    }else{
      SSC_SITE <- "NOT_DEFINED"
    }
  }
  
  #
  
  id_loc <- which(tbl_deployment_LP8$LocationName[ith_depl]==tbl_location$LocationName)
  
  #
  
  if(tbl_location$Canton[id_loc]=="TI"){
    group <- 1
  }
  
  if(tbl_location$Canton[id_loc]!="TI" & SSC_SITE != "HILLTOP"){
    group <- 2
  }
  
  if(tbl_location$Canton[id_loc]!="TI" & SSC_SITE == "HILLTOP"){
    group <- 3
  }
  
  print(paste(tbl_deployment_LP8$LocationName[ith_depl],tbl_deployment_LP8$SensorUnit_ID[ith_depl],SSC_SITE,group))
  
  
  # Compute daily CO2 minimums for the LP8
  
  all_LP8_timestamps<- data$timestamp
  
  #
  
  data$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
  
  
  data_n   <- data.frame(date=data$date,n=rep(1,dim(data)[1]), stringsAsFactors = F)
  data_RH  <- data.frame(date=data$date,SHT21_RH=data$SHT21_RH,stringsAsFactors = F)
  data     <- data.frame(date=data$date,CO2=data$CO2,          stringsAsFactors = F)
  
  df_A     <- timeAverage(mydata = data,   avg.time = "6 hour",statistic = "percentile",percentile = 10,start.date = (Date_UTC_from - 86400),end.date = Date_UTC_to)
  df_A_n   <- timeAverage(mydata = data_n, avg.time = "6 hour",statistic = "sum",start.date = (Date_UTC_from - 86400),end.date = Date_UTC_to)
  df_A_RH  <- timeAverage(mydata = data_RH,avg.time = "6 hour",statistic = "min",start.date = (Date_UTC_from - 86400),end.date = Date_UTC_to)
  
  nn       <- dim(df_A_n)[1]
  
  df_A     <- apply(cbind(c(df_A$CO2[1:(nn)]),
                          c(df_A$CO2[2:(nn)],NA),
                          c(df_A$CO2[3:(nn)],NA,NA),
                          c(df_A$CO2[4:(nn)],NA,NA,NA)),1,min,na.rm=T)
  
  df_A_n   <- apply(cbind(c(df_A_n$n[1:(nn)]),
                          c(df_A_n$n[2:(nn)],NA),
                          c(df_A_n$n[3:(nn)],NA,NA),
                          c(df_A_n$n[4:(nn)],NA,NA,NA)),1,sum,na.rm=T)
  
  df_A_RH  <- apply(cbind(c(df_A_RH$SHT21_RH[1:(nn)]),
                          c(df_A_RH$SHT21_RH[2:(nn)],NA),
                          c(df_A_RH$SHT21_RH[3:(nn)],NA,NA),
                          c(df_A_RH$SHT21_RH[4:(nn)],NA,NA,NA)),1,min,na.rm=T)
  
  df_A_ts  <- as.numeric(difftime(time1=seq(Date_UTC_from-86400,Date_UTC_to,"6 hour"), time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")) + 86400
  
  #
  
  median_n <- median(df_A_n,na.rm=T)
  
  #
  
  id_keep <- which(df_A_n>=22.5/24*median_n)
  id_keep <- which(df_A_n>=18.0/24*median_n)
  
  df_A    <- df_A[   id_keep]
  df_A_n  <- df_A_n[ id_keep]
  df_A_RH <- df_A_RH[id_keep]
  df_A_ts <- df_A_ts[id_keep]
  
  id_A_B <- which(REF_HPP$timestamp %in% df_A_ts)
  id_B_A <- which(df_A_ts           %in% REF_HPP$timestamp)
  
  CO2_LOW_LP8         <- rep(NA,dim(REF_HPP)[1])
  RH_MIN              <- rep(NA,dim(REF_HPP)[1])
  
  CO2_LOW_LP8[id_A_B] <- df_A[id_B_A]
  RH_MIN[id_A_B]      <- df_A_RH[id_B_A]
  
  
  # Validate against reference measurements
  
  id_CO2_LOW_not_trusty_RH_dep     <- which((CO2_LOW_LP8<min_CO2_bsl[,group] | CO2_LOW_LP8>max_CO2_bsl[,group]) & RH_MIN>=85)
  id_CO2_LOW_not_trusty_RH_indep   <- which((CO2_LOW_LP8<min_CO2_bsl[,group] | CO2_LOW_LP8>max_CO2_bsl[,group]))
  n_id_CO2_LOW_not_trusty_RH_dep   <- length(id_CO2_LOW_not_trusty_RH_dep)
  n_id_CO2_LOW_not_trusty_RH_indep <- length(id_CO2_LOW_not_trusty_RH_indep)
  
  timestamp_flag_L   <- rep(1,length(all_LP8_timestamps))
  timestamp_flag_LRH <- rep(1,length(all_LP8_timestamps))
  
  
  if(n_id_CO2_LOW_not_trusty_RH_indep>0){
    for(ith_event in 1:n_id_CO2_LOW_not_trusty_RH_indep){
      id <- which(all_LP8_timestamps<=REF_HPP$timestamp[id_CO2_LOW_not_trusty_RH_indep[ith_event]]
                  & all_LP8_timestamps>=(REF_HPP$timestamp[id_CO2_LOW_not_trusty_RH_indep[ith_event]]-86400))
      
      if(length(id)>0){
        timestamp_flag_L[id] <- 0
      }
    }
  }
  
  if(n_id_CO2_LOW_not_trusty_RH_dep>0){
    for(ith_event in 1:n_id_CO2_LOW_not_trusty_RH_dep){
      id <- which(all_LP8_timestamps<=REF_HPP$timestamp[id_CO2_LOW_not_trusty_RH_dep[ith_event]]
                  & all_LP8_timestamps>=(REF_HPP$timestamp[id_CO2_LOW_not_trusty_RH_dep[ith_event]]-86400))
      
      if(length(id)>0){
        timestamp_flag_LRH[id] <- 0
      }
    }
  }
  
  
  ##
  
  # Export analysis for each sensor
  
  if(T){
    tmp <- data.frame(date               = strftime(REF_HPP$date,"%Y-%m-%d %H:%M:%S",tz="UTC"),
                      timestamp          = REF_HPP$timestamp,
                      CO2_LOW_LP8        = CO2_LOW_LP8,
                      RH_MIN             = RH_MIN,
                      min_CO2_bsl        = min_CO2_bsl[,group],
                      max_CO2_bsl        = max_CO2_bsl[,group],
                      stringsAsFactors = F)
    
    write.table(x = tmp,file = paste(resultdir,"CO2_DAILY_MIN_",tbl_deployment_LP8$LocationName[ith_depl],"_",tbl_deployment_LP8$SensorUnit_ID[ith_depl],".csv",sep=""),col.names=T,row.names=F,sep=";")
  }
  
  
  # write flags into database
  
  query_str <- paste("INSERT INTO ",ProcDataTblName," (timestamp,SensorUnit_ID,LocationName,L_FLAG,LRH_FLAG)",sep="")
  query_str <- paste(query_str,"VALUES" )
  query_str <- paste(query_str,
                     paste("(",paste(all_LP8_timestamps,",",
                                     rep(tbl_deployment_LP8$SensorUnit_ID[ith_depl],length(all_LP8_timestamps)),",'",
                                     rep(tbl_deployment_LP8$LocationName[ith_depl], length(all_LP8_timestamps)),"',",
                                     timestamp_flag_L,",",
                                     timestamp_flag_LRH,
                                     collapse = "),(",sep=""),")",sep=""),
                     paste(" ON DUPLICATE KEY UPDATE "))
  
  query_str <- paste(query_str,paste("L_FLAG=VALUES(L_FLAG),LRH_FLAG=VALUES(LRH_FLAG);",    sep=""))
  
  drv             <- dbDriver("MySQL")
  con<-carboutil::get_conn(group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  dbClearResult(res)
  dbDisconnect(con)
  
  #
  
  rm(data,data_n,df_00,df_15,df_30,df_45,df_00_n,df_15_n,df_30_n,df_45_n,df,df_n)
  gc()
}

### ----------------------------------------------------------------------------------------------------------------------------





