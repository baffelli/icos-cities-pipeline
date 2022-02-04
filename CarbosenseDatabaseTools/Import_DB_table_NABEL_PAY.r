# Import_DB_table_NABEL_PAY.r
# ---------------------------
#
# Author: Michael Mueller
#
#
# ---------------------------


## clear variables
rm(list=ls(all=TRUE))
gc()

## ----------------------------------------------------------------------------------------------------------------------

## libraries
library(DBI)
require(RMySQL)
require(chron)
library(dplyr)
library(purrr)

## ----------------------------------------------------------------------------------------------------------------------

## DB information

DB_group <- "CarboSense_MySQL"

## ----------------------------------------------------------------------------------------------------------------------

## Directories

fdirectory <- "K:/Nabel/Daten/Stationen/PAY/"
fdirectory <- "/mnt/basi/Nabel/Daten/Stationen/PAY/"

## ----------------------------------------------------------------------------------------------------------------------

query_str               <- "SELECT * FROM RefMeasExclusionPeriods WHERE LocationName = 'PAY';";
drv                     <- dbDriver("MySQL")
con<-carboutil::get_conn(group=DB_group)
res                     <- dbSendQuery(con, query_str)
RefMeasExclusionPeriods <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

RefMeasExclusionPeriods$Date_UTC_from  <- strptime(RefMeasExclusionPeriods$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
RefMeasExclusionPeriods$Date_UTC_to    <- strptime(RefMeasExclusionPeriods$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

RefMeasExclusionPeriods$timestamp_from <- as.numeric(difftime(time1=RefMeasExclusionPeriods$Date_UTC_from,time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC")) 
RefMeasExclusionPeriods$timestamp_to   <- as.numeric(difftime(time1=RefMeasExclusionPeriods$Date_UTC_to,  time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC")) 

## ----------------------------------------------------------------------------------------------------------------------

## NABEL PAY Data

# - automatically downloaded data files [directory: "K:/Nabel/Daten/Stationen/PAY/"]
# - data files contain one minute data [date, values of NABEL measurement programm at PAY plus respective flags]
# - date refers to CET

if(T){
  
  files1  <- list.files(path=fdirectory,pattern = "NO NO2 O3 CO2 H2O PAY",full.names = T)
  files2  <- list.files(path=fdirectory,pattern = "NO NO2 O3 CO2 H2O MM1",full.names = T)
  files   <- c(files1,files2)
  n_files <- length(files)


  #Find which data is already in db and compute set difference to determine which files to load
  con <- carboutil::get_conn(group=DB_group)
  dates_in_db <- collect(tbl(con, sql("SELECT DISTINCT DATE(FROM_UNIXTIME(timestamp)) AS date FROM NABEL_PAY WHERE CO2 <> -999")))$date
  dates_in_files <- map(files, function(x) lubridate::as_date(stringr::str_extract(x,"\\d{6}")))
  dbDisconnect(con)
  files_to_load <-  match(setdiff(dates_in_files, dates_in_db), dates_in_files)

  print(files_to_load)
  
  #  No flags for CO2 available; Calibration periods of the Picarro instrument every 25 hours)
  
  CO2_cal_00_timestamp <- as.numeric(difftime(time1=strptime("2016-12-30 17:48:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                              time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                                              tz="UTC",
                                              units="secs"))
  CO2_cal_timestamps   <- NULL
  
  for(ith_cal in 0:500){
    CO2_cal_timestamps <- c(CO2_cal_timestamps,CO2_cal_00_timestamp + ith_cal*25*3600 + (0:31)*60)
  }
  
  #
  
  for(ith_file in files_to_load){
    
    data <- read.table(file = files[ith_file],header = F,sep = ";",skip = 4)
    
    if(!dim(data)[2]%in%c(12,14,16)){
      stop("Unexpected number of columns in file!")
    }
    if(dim(data)[2]==14){
      data <- cbind(data[,1:13],
                    data.frame(rep(-999,dim(data)[1]),rep(-999,dim(data)[1]),stringsAsFactors=F))
    }
    if(dim(data)[2]==12){
      data <- cbind(data[,1:11],
                    data.frame(rep(-999,dim(data)[1]),rep(-999,dim(data)[1]),rep(-999,dim(data)[1]),rep(-999,dim(data)[1]),stringsAsFactors=F))
    }
    
    # ! Order of col in imported files does not correspond to the order in the database !!!
    colnames(data) <- c("timestamp","NO","NO_F","NO2","NO2_F","O3","O3_F","CO2","CO2_F","H2O","H2O_F","CO","CO_F","CO2_DRY","CO2_DRY_F")
    data           <- cbind(data[,1:7],data[,12:13],data[,8:11],data[,14:15])
    
    
    # Date (CET -> UTC; end of Interval -> begin of interval)
    data$timestamp <- strptime(data$timestamp,"%d.%m.%Y %H:%M",tz="UTC") - 3600 - 60
    data$timestamp <- as.numeric(difftime(time1=data$timestamp,time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
    
    
    # Check data
    u_cn   <- colnames(data)
    u_cn   <- gsub(pattern = "_F",replacement = "",u_cn)
    u_cn   <- sort(unique(u_cn[which(u_cn!="timestamp")]))
    n_u_cn <- length(u_cn)
    
    
    for(ith_u_cn in 1:n_u_cn){
      pos   <- which(colnames(data)==u_cn[ith_u_cn])
      pos_F <- which(colnames(data)==paste(u_cn[ith_u_cn],"_F",sep=""))
      
      if(length(pos)==0 | length(pos_F)==0){
        stop()
      }
      
      if(u_cn[ith_u_cn]!="CO2"){
        id_ok <- which(data[,pos]!=""
                       & data[,pos]!= -999
                       & !is.na(data[,pos])
                       & (data[,pos_F]=="" | is.na(data[,pos_F])))
      }else{
        
        if(any(data$CO!=-999)){
          id_CO_nok <- which(data$CO_F!="" & !is.na(data$CO_F) & data$CO_F!=1)
          if(length(id_CO_nok)>0){
            CO2_cal_timestamps <- c(CO2_cal_timestamps,data$timestamp[id_CO_nok])
          }
        }
        
        if(T){
          id_CO_nok <- which((data$NO_F!="" & !is.na(data$NO_F) & data$NO_F!=1)
                             & (data$NO2_F!="" & !is.na(data$NO2_F) & data$NO2_F!=1)
                             & (data$O3_F!="" & !is.na(data$O3_F) & data$O3_F!=1))

          if(length(id_CO_nok)>0){
            CO2_cal_timestamps <- c(CO2_cal_timestamps,data$timestamp[id_CO_nok])
          }
        }
        
        id_ok <- which(!data$timestamp %in% CO2_cal_timestamps
                       & data[,pos]!=""
                       & data[,pos]!= -999
                       & data[,pos] >  350
                       & !is.na(data[,pos])
                       & (data[,pos_F]=="" | is.na(data[,pos_F])))
      }
      
      data[,pos_F]      <- 0
      data[id_ok,pos_F] <- 1
      
      data[!(1:dim(data)[1])%in%id_ok,pos] <- -999
      
      #
      
      id_excl   <- which(RefMeasExclusionPeriods$Measurement==u_cn[ith_u_cn])
      n_id_excl <- length(id_excl)
      
      if(n_id_excl>0){
        
        for(ith_excl in 1:n_id_excl){
          
          id_id_excl <- which(data$timestamp>=RefMeasExclusionPeriods$timestamp_from[id_excl[ith_excl]] & data$timestamp<=RefMeasExclusionPeriods$timestamp_to[id_excl[ith_excl]])
          
          if(length(id_id_excl)>0){
            data[id_id_excl,pos]   <- -999
            data[id_id_excl,pos_F] <- 0
          }
        }
      }
      
      #
      
      if(length(which(is.na(data[,pos])))>0){
        print(paste(u_cn[ith_u_cn],": Import error 1!"))
      }
      if(length(which(is.na(data[,pos_F])))>0){
        print(paste(u_cn[ith_u_cn],": Import error 2!"))
      }
      if(length(which(!data[,pos_F]%in%c(0,1)))>0){
        print(paste(u_cn[ith_u_cn],": Import error 3!"))
      }
      if(length(which(data[,pos]==-999 & data[,pos_F]!=0))>0){
        print(paste(u_cn[ith_u_cn],": Import error 4!"))
      }
    }
    
    #  
    
    id_PICnok <- which(  data$CO2   == -999
                       | data$CO2_F == 0
                       | data$H2O   == -999
                       | data$H2O_F == 0)
    
    if(length(id_PICnok)>0){
      data$CO2[id_PICnok]       <- -999
      data$CO2_F[id_PICnok]     <-  0
      data$H2O[id_PICnok]       <- -999
      data$H2O_F[id_PICnok]     <-  0
      data$CO2_DRY[id_PICnok]   <- -999
      data$CO2_DRY_F[id_PICnok] <-  0
    }
    
    #
    
    data <- as.data.frame(data,stringsAsFactors=F)
    
    
    # insert data into DB
    
    query_str <- "SELECT MAX(timestamp) AS MAX_timestamp FROM NABEL_PAY WHERE CO2!=-999 or NO2!=-999 or O3!=-999 or CO!=0;";
    drv             <- dbDriver("MySQL")
    con<-carboutil::get_conn(group=DB_group)
    res             <- dbSendQuery(con, query_str)
    MAX_timestamp   <- dbFetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    MAX_timestamp <- as.integer(MAX_timestamp)
    
    if(is.na(MAX_timestamp)){
      MAX_timestamp <- 0
    }
    
    #
    
    id_insert <- which(data$timestamp>MAX_timestamp)
    
    if(length(id_insert)>0){
      
      query_str <- paste("INSERT INTO NABEL_PAY (",paste(colnames(data)[1:15],collapse = ","),") ",sep="")
      query_str <- paste(query_str,"VALUES" )
      query_str <- paste(query_str,
                         paste("(",paste(data[id_insert,01],",",
                                         data[id_insert,02],",",
                                         data[id_insert,03],",",
                                         data[id_insert,04],",",
                                         data[id_insert,05],",",
                                         data[id_insert,06],",",
                                         data[id_insert,07],",",
                                         data[id_insert,08],",",
                                         data[id_insert,09],",",
                                         data[id_insert,10],",",
                                         data[id_insert,11],",",
                                         data[id_insert,12],",",
                                         data[id_insert,13],",",
                                         data[id_insert,14],",",
                                         data[id_insert,15],
                                         collapse = "),(",sep=""),")",sep=""),
                         paste(" ON DUPLICATE KEY UPDATE "))
      
      for(ii in 2:13){
        if(ii==13){
          query_str <- paste(query_str,paste(colnames(data)[ii],"=VALUES(",colnames(data)[ii],"); ",sep=""))
        }else{
          query_str <- paste(query_str,paste(colnames(data)[ii],"=VALUES(",colnames(data)[ii],"), ",sep=""))
        }
      }
      
      
      
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn(group=DB_group)
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
  }
  
  rm(data,query_str,id_insert)
  gc()
  
}

