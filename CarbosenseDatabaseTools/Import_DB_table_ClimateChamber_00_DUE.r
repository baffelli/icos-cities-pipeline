# Import_DB_table_ClimateChamber_00_DUE.r
# ---------------------------------------
#
# Author: Michael Mueller
#
#
# ---------------------------------------


## clear variables
rm(list=ls(all=TRUE))
gc()

## ----------------------------------------------------------------------------------------------------------------------

## libraries
library(openair)
library(DBI)
require(RMySQL)
require(chron)
library(MASS)


## ----------------------------------------------------------------------------------------------------------------------

## Directories

fdirectory <- "K:/Carbosense/Data/Klimakammer_Versuche_27022017_XXXXXXXX"

### ----------------------------------------------------------------------------------------------------------------------------

# EMPA CarboSense DB information

CS_DB_dbname <- "CarboSense"
CS_DB_user   <- ""
CS_DB_pass   <- ""

## ----------------------------------------------------------------------------------------------------------------------

## Picarro data

# - manually downloaded data files [directory: "K:/Carbosense/Data/Klimakammer_Versuche_27022017_XXXXXXXX/"]
# - data files contain one minute data [date from, date to, CO2, H2o]
# - date refers to CET or UTC depending on Picarro-Computer-Settings/Picarro-Type [CET->UTC]

if(T){
  
  files   <- list.files(path=fdirectory,pattern = "_MM1.csv",full.names = T)
  n_files <- length(files)
  
  #
  
  for(ith_file in 1:n_files){
    
    data <- read.table(file = files[ith_file], header = T,sep = ";")
    
    if(dim(data)[2]==4){
      colnames(data) <- c("timestamp","timestamp_EI","CO2","H2O")
      data$CO2_DRY   <- -999
    }else{
      colnames(data) <- c("timestamp","timestamp_EI","CO2","H2O","CO2_DRY")
    }
    
    
    # Date (UTC; begin of interval)
    
    data$timestamp <- strptime(data$timestamp,"%d.%m.%Y %H:%M",tz="UTC")
    
    id_adj_TZ <- rep(3600,dim(data)[1])
    if(all(data$timestamp>=strptime("20170410132000","%Y%m%d%H%M%S",tz="UTC") & data$timestamp<=strptime("20190530000000","%Y%m%d%H%M%S",tz="UTC"))){
      id_adj_TZ <- rep(0,dim(data)[1])
      print(files[ith_file])
    }
    
    data$timestamp <- data$timestamp - id_adj_TZ
    data$timestamp <- as.numeric(difftime(time1=data$timestamp,time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
    
    data <- as.data.frame(data,stringsAsFactors=F)
    
    
    # insert data into DB
    
    id_insert <- which(data$CO2!=-999
                       & data$H2O!=-999
                       & !is.na(data$CO2)
                       & !is.na(data$H2O)
                       & !is.na(data$CO2_DRY))
    
    if(length(id_insert)>0){
      
      query_str <- paste("INSERT INTO ClimateChamber_00_DUE (timestamp,CO2,CO2_DRY,CO2_F,H2O,H2O_F)",sep="")
      query_str <- paste(query_str,"VALUES" )
      query_str <- paste(query_str,
                         paste("(",paste(data[id_insert,01],",",
                                         data[id_insert,03],",",
                                         data[id_insert,05],",",
                                         rep(1,length(id_insert)),",",
                                         data[id_insert,04],",",
                                         rep(1,length(id_insert)),
                                         collapse = "),(",sep=""),")",sep=""),
                         paste(" ON DUPLICATE KEY UPDATE "))
      
      query_str <- paste(query_str,paste("CO2=VALUES(CO2),",         sep=""))
      query_str <- paste(query_str,paste("CO2_DRY=VALUES(CO2_DRY),", sep=""))
      query_str <- paste(query_str,paste("CO2_F=VALUES(CO2_F),",     sep=""))
      query_str <- paste(query_str,paste("H2O=VALUES(H2O),",         sep=""))
      query_str <- paste(query_str,paste("H2O_F=VALUES(H2O_F);",     sep=""))
      
      
      
      
      drv             <- dbDriver("MySQL")
      con             <- dbConnect(drv, dbname=CS_DB_dbname, host="du-gsn1", port=3306, user=CS_DB_user, pass=CS_DB_pass)
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
  }
  
  rm(data,query_str,id_insert)
  gc()
  
}



## ----------------------------------------------------------------------------------------------------------------------

## Climate chamber data

# - manually downloaded data files [directory: "K:/Carbosense/Data/Klimakammer_Versuche_27022017_XXXXXXXX/"]
# - data files contain one minute data [date of measurement, T soll, T ist, RH soll, RH ist, ...]
# - date refers to CET/CEST [CET/CEST -> UTC]

if(T){
  
  files   <- list.files(path=fdirectory,pattern = "Klimakammerdaten_",full.names = T)
  n_files <- length(files)
  
  #
  
  for(ith_file in 1:n_files){
    
    data <- read.table(file = files[ith_file], header = T,sep = ";",skip=3)
    data <- data[,1:5]
    colnames(data) <- c("timestamp","T_Soll","T","RH_Soll","RH")
    data <- as.data.frame(data,stringsAsFactors=F)
    
    # Date (UTC; begin of interval)
    
    # Format with/without seconds
    data$timestamp    <- as.character(data$timestamp)
    id_only_minutes   <- which(nchar(as.character(data$timestamp),type = "chars")==16)
    n_id_only_minutes <- length(id_only_minutes)
    
    if(n_id_only_minutes>0){
      data$timestamp[id_only_minutes] <- paste(data$timestamp[id_only_minutes],":00",sep="")
    }
    
    data$timestamp <- strptime(data$timestamp,"%d.%m.%Y %H:%M:%S",tz="UTC")
    
    # Daylight saving time
    
    df_daylightSavingTime   <- NULL
    df_daylightSavingTime   <- rbind(df_daylightSavingTime,data.frame(from = strptime("26.03.2017 02:00","%d.%m.%Y %H:%M",tz="UTC"),
                                                                      to   = strptime("29.10.2017 03:00","%d.%m.%Y %H:%M",tz="UTC"),
                                                                      stringsAsFactors = F))
    df_daylightSavingTime   <- rbind(df_daylightSavingTime,data.frame(from = strptime("25.03.2018 02:00","%d.%m.%Y %H:%M",tz="UTC"),
                                                                      to   = strptime("28.10.2018 03:00","%d.%m.%Y %H:%M",tz="UTC"),
                                                                      stringsAsFactors = F))
    df_daylightSavingTime   <- rbind(df_daylightSavingTime,data.frame(from = strptime("30.03.2019 02:00","%d.%m.%Y %H:%M",tz="UTC"),
                                                                      to   = strptime("26.10.2019 03:00","%d.%m.%Y %H:%M",tz="UTC"),
                                                                      stringsAsFactors = F))
    

    for(ith_dst in 1:dim(df_daylightSavingTime)[1]){
      id_daylightSavingTime   <- which(data$timestamp>=df_daylightSavingTime$from[ith_dst] & data$timestamp<=df_daylightSavingTime$to[ith_dst])
      n_id_daylightSavingTime <- length(id_daylightSavingTime)
      
      if(n_id_daylightSavingTime>0){
        data$timestamp[id_daylightSavingTime] <- data$timestamp[id_daylightSavingTime] - 3600
      }
    }
    
    
    # Force to full minute / keep only one measurement per minute 
    data$timestamp <- strptime(strftime(data$timestamp,"%d.%m.%Y %H:%M:00",tz="UTC"),"%d.%m.%Y %H:%M:%S",tz="UTC")
    duplicated_ts  <- duplicated(data$timestamp)
    data           <- data[!duplicated_ts,]
    
    # CET -> UTC
    data$timestamp <- data$timestamp - 3600
    
    data$timestamp <- as.numeric(difftime(time1=data$timestamp,time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
    
    data <- as.data.frame(data,stringsAsFactors=F)
    
    # NO RH between 9 Mar 2017 and 14 Mar 2017
    
    if(length(grep(pattern = "Klimakammerdaten_09032017_14032017.csv",x = files[ith_file]))!=0){
      data$RH      <- -999
      data$RH_Soll <- -999
    }
    
    # Set flags
    
    data$T_F         <- 0
    id_ok            <- which(!is.na(data$T)
                              & !is.na(data$T_Soll)
                              & data$T!= -999)
    data$T_F[id_ok]  <- 1
    
    data$RH_F        <- 0
    id_ok            <- which(!is.na(data$RH)
                              & !is.na(data$RH_Soll)
                              & data$RH!= -999)
    data$RH_F[id_ok] <- 1
    
    
    # insert data into DB
    
    id_insert <- which(!is.na(data$T)
                       & !is.na(data$T_Soll)
                       & !is.na(data$RH)
                       & !is.na(data$RH_Soll))
    
    if(length(id_insert)>0){
      
      query_str <- paste("INSERT INTO ClimateChamber_00_DUE (timestamp,T,T_F,T_Soll,RH,RH_F,RH_Soll)",sep="")
      query_str <- paste(query_str,"VALUES" )
      query_str <- paste(query_str,
                         paste("(",paste(data$timestamp[id_insert],",",
                                         data$T[id_insert],",",
                                         data$T_F[id_insert],",",
                                         data$T_Soll[id_insert],",",
                                         data$RH[id_insert],",",
                                         data$RH_F[id_insert],",",
                                         data$RH_Soll[id_insert],
                                         collapse = "),(",sep=""),")",sep=""),
                         paste(" ON DUPLICATE KEY UPDATE "))
      
      query_str <- paste(query_str,paste("T=VALUES(T),",    sep=""))
      query_str <- paste(query_str,paste("T_F=VALUES(T_F),",sep=""))
      query_str <- paste(query_str,paste("T_Soll=VALUES(T_Soll),",sep=""))
      query_str <- paste(query_str,paste("RH=VALUES(RH),",    sep=""))
      query_str <- paste(query_str,paste("RH_F=VALUES(RH_F),",  sep=""))
      query_str <- paste(query_str,paste("RH_Soll=VALUES(RH_Soll);",  sep=""))
      
      
      
      drv             <- dbDriver("MySQL")
      con             <- dbConnect(drv, dbname=CS_DB_dbname, host="du-gsn1", port=3306, user=CS_DB_user, pass=CS_DB_pass)
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
  }
  
  rm(data,query_str,id_insert)
  gc()
  
}


## ----------------------------------------------------------------------------------------------------------------------

## Pressure data

# - manually downloaded data files [directory: "K:/Carbosense/Data/Klimakammer_Versuche_27022017_XXXXXXXX/"]
# - data files contain one minute data [date, pressure]
# - date refers to CET/CEST [CET/CEST -> UTC; End of interval -> Begin of interval]
# - Computer of "Kalibrierlabor" was replaced in Aug/2017. Since then, date refers constantly to CET.

if(T){
  
  files   <- list.files(path=fdirectory,pattern = "DRUCK_GF_",full.names = T)
  n_files <- length(files)
  
  #
  
  for(ith_file in 1:n_files){
    
    print(files[ith_file])
    
    tmp  <- read.table(file = files[ith_file], header = T,sep = ";",skip=3)
    data <- data.frame(timestamp=tmp[,1],
                       pressure=tmp[,2],
                       stringsAsFactors=F)
    
    # Date (begin of interval, CET/CEST-->UTC)
    data$timestamp <- strptime(data$timestamp,"%d.%m.%y %H:%M",tz="UTC")
    
    # Daylight saving time (computer since Aug/2017 in CET, as it is normal in NABEL)
    id_daylightSavingTime   <- which(data$timestamp>=strptime("26.03.2017 02:00","%d.%m.%Y %H:%M",tz="UTC") & data$timestamp<=strptime("10.08.2017 15:45","%d.%m.%Y %H:%M",tz="UTC"))
    n_id_daylightSavingTime <- length(id_daylightSavingTime)
    
    if(n_id_daylightSavingTime>0){
      data$timestamp[id_daylightSavingTime] <- data$timestamp[id_daylightSavingTime] - 3600
    }
    
    data$timestamp <- data$timestamp - 3600 - 60
    
    data$timestamp <- as.numeric(difftime(time1=data$timestamp,time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
    
    data <- as.data.frame(data,stringsAsFactors=F)
    
    
    # insert data into DB
    
    id_insert <- which(data$pressure!=-999
                       & !is.na(data$pressure))
    
    if(length(id_insert)>0){
      
      query_str <- paste("INSERT INTO ClimateChamber_00_DUE (timestamp,pressure,pressure_F)",sep="")
      query_str <- paste(query_str,"VALUES" )
      query_str <- paste(query_str,
                         paste("(",paste(data$timestamp[id_insert],",",
                                         data$pressure[id_insert],",",
                                         rep(1,length(id_insert)),
                                         collapse = "),(",sep=""),")",sep=""),
                         paste(" ON DUPLICATE KEY UPDATE "))
      
      query_str <- paste(query_str,paste("pressure=VALUES(pressure),",    sep=""))
      query_str <- paste(query_str,paste("pressure_F=VALUES(pressure_F);",sep=""))
      
      
      
      
      drv             <- dbDriver("MySQL")
      con             <- dbConnect(drv, dbname=CS_DB_dbname, host="du-gsn1", port=3306, user=CS_DB_user, pass=CS_DB_pass)
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
  }
  
  rm(data,query_str,id_insert)
  gc()
  
}

## ----------------------------------------------------------------------------------------------------------------------

## Pressure data

# - pressure data from NABEL DUE for specific time periods (failures of barometer in the NABEL calibration lab)


if(T){
  
  periods <- NULL
  periods <- rbind(periods, data.frame(date_UTC_from_str = "2017-10-02 00:00:00",
                                       date_UTC_to_str   = "2017-10-12 00:00:00"))
  periods <- rbind(periods, data.frame(date_UTC_from_str = "2018-10-01 00:00:00",
                                       date_UTC_to_str   = "2018-10-13 00:00:00"))
  
  periods$date_UTC_from  <- strptime(periods$date_UTC_from_str,"%Y-%m-%d %H:%M:%S",tz="UTC")
  periods$date_UTC_to    <- strptime(periods$date_UTC_to_str,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
  periods$timestamp_from <- as.numeric(difftime(time1=periods$date_UTC_from,time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
  periods$timestamp_to   <- as.numeric(difftime(time1=periods$date_UTC_to,  time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
  
  if(!is.null(periods)){
    for(ith_period in 1:dim(periods)[1]){
      
      # get pressure data from table NABEL_DUE
      
      query_str       <- paste("SELECT timestamp, pressure, pressure_F FROM NABEL_DUE WHERE timestamp >= ",periods$timestamp_from[ith_period]," and timestamp <= ",periods$timestamp_to[ith_period],";")
      drv             <- dbDriver("MySQL")
      con             <- dbConnect(drv, dbname=CS_DB_dbname, host="du-gsn1", port=3306, user=CS_DB_user, pass=CS_DB_pass)
      res             <- dbSendQuery(con, query_str)
      data            <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      # write pressure data into table ClimateChamber_00_DUE
      
      id_insert <- which(data$pressure!=-999
                         & !is.na(data$pressure))
      
      if(length(id_insert)>0){
        
        query_str <- paste("INSERT INTO ClimateChamber_00_DUE (timestamp,pressure,pressure_F)",sep="")
        query_str <- paste(query_str,"VALUES" )
        query_str <- paste(query_str,
                           paste("(",paste(data$timestamp[id_insert],",",
                                           data$pressure[id_insert],",",
                                           rep(1,length(id_insert)),
                                           collapse = "),(",sep=""),")",sep=""),
                           paste(" ON DUPLICATE KEY UPDATE "))
        
        query_str <- paste(query_str,paste("pressure=VALUES(pressure),",    sep=""))
        query_str <- paste(query_str,paste("pressure_F=VALUES(pressure_F);",sep=""))
        
        drv             <- dbDriver("MySQL")
        con             <- dbConnect(drv, dbname=CS_DB_dbname, host="du-gsn1", port=3306, user=CS_DB_user, pass=CS_DB_pass)
        res             <- dbSendQuery(con, query_str)
        dbClearResult(res)
        dbDisconnect(con)
      }
    }
  }
}

