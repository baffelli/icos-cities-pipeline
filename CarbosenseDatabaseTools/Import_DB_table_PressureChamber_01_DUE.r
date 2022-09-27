# Import_DB_table_PressureChamber_01_DUE.r
# ----------------------------------------
#
# Author: Michael Mueller
#
#
# ------------------------------------------


## clear variables
rm(list=ls(all=TRUE))
gc()

### ----------------------------------------------------------------------------------------------------------------------

## libraries
library(DBI)
require(RMySQL)
require(chron)

### ----------------------------------------------------------------------------------------------------------------------

# directory

fdirectory <- "K:/Carbosense/Data//Druckkammer_Versuche_Empa_LA003/"

### ----------------------------------------------------------------------------------------------------------------------------

# EMPA CarboSense DB information

CS_DB_dbname <- "CarboSense"
CS_DB_user   <- ""
CS_DB_pass   <- ""


## ----------------------------------------------------------------------------------------------------------------------

## Picarro data

# - manually downloaded data files
# - data files contain one minute data [date from, date to, CO2, H2o, CO2_DRY]
# - date refers to UTC


if(T){
  
  files   <- list.files(path=fdirectory,pattern = "_MM1.csv",full.names = T)
  n_files <- length(files)
  
  #
  
  for(ith_file in 1:n_files){
    
    data <- read.table(file = files[ith_file], header = T,sep = ";")
    
    colnames(data) <- c("timestamp","timestamp_EI","CO2","H2O","CO2_DRY")
    
    
    # Date (UTC; begin of interval)
    data$timestamp <- strptime(data$timestamp,"%d.%m.%Y %H:%M",tz="UTC")
    data$timestamp <- as.numeric(difftime(time1=data$timestamp,time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
    
    data <- as.data.frame(data,stringsAsFactors=F)
    
    
    # insert data into DB
    
    id_insert <- which(data$CO2!=-999
                       & data$H2O!=-999
                       & data$CO2_DRY!=-999
                       & !is.na(data$CO2)
                       & !is.na(data$H2O)
                       & !is.na(data$CO2_DRY))
    
    if(length(id_insert)>0){
      
      query_str <- paste("INSERT INTO PressureChamber_01_DUE (timestamp,CO2,CO2_F,CO2_DRY,CO2_DRY_F,H2O,H2O_F)",sep="")
      query_str <- paste(query_str,"VALUES" )
      query_str <- paste(query_str,
                         paste("(",paste(data$timestamp[id_insert],",",
                                         data$CO2[id_insert],",",
                                         rep(1,length(id_insert)),",",
                                         data$CO2_DRY[id_insert],",",
                                         rep(1,length(id_insert)),",",
                                         data$H2O[id_insert],",",
                                         rep(1,length(id_insert)),
                                         collapse = "),(",sep=""),")",sep=""),
                         paste(" ON DUPLICATE KEY UPDATE "))
      
      query_str <- paste(query_str,paste("CO2=VALUES(CO2),",             sep=""))
      query_str <- paste(query_str,paste("CO2_F=VALUES(CO2_F),",         sep=""))
      query_str <- paste(query_str,paste("CO2_DRY=VALUES(CO2_DRY),",     sep=""))
      query_str <- paste(query_str,paste("CO2_DRY_F=VALUES(CO2_DRY_F),", sep=""))
      query_str <- paste(query_str,paste("H2O=VALUES(H2O),",             sep=""))
      query_str <- paste(query_str,paste("H2O_F=VALUES(H2O_F);",         sep=""))
      
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn( dbname=CS_DB_dbname, host="du-gsn1", port=3306, user=CS_DB_user, pass=CS_DB_pass)
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

# - manually downloaded data files 
# - data files contain one minute data [date, pressure]
# - date refers to CEST/CET

if(T){
  
  files   <- list.files(path=fdirectory,pattern = "Druckmessungen_[[:digit:]]{8}_[[:digit:]]{8}.csv",full.names = T)
  n_files <- length(files)
  
  #
  
  for(ith_file in 1:n_files){
    
    print(files[ith_file])
    
    data <- read.table(file = files[ith_file], header = T,sep = ";")
    
    colnames(data) <- c("ID","timestamp","pressure","unit","T")
    
    # Date (UTC; begin of interval)
    data$timestamp <- strptime(data$timestamp,"%d.%m.%Y %H:%M:%S",tz="UTC")
    
    id_CEST2UTC     <- which( (data$timestamp>=strptime("17.08.2017 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC")
                             & data$timestamp<=strptime("22.08.2017 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"))
                            | (data$timestamp>=strptime("10.06.2018 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC")
                             & data$timestamp<=strptime("28.10.2018 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"))
                            | (data$timestamp>=strptime("31.03.2019 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC")
                             & data$timestamp<=strptime("27.10.2019 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"))
                            | (data$timestamp>=strptime("29.03.2020 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC")
                             & data$timestamp<=strptime("25.10.2020 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC")))
    
    if(length(id_CEST2UTC)>0){
      data$timestamp[id_CEST2UTC] <- data$timestamp[id_CEST2UTC] - 7200
      rm(id_CEST2UTC)
      gc()
    }
    
    id_CET2UTC     <- which( (data$timestamp>=strptime("11.12.2017 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC")
                            & data$timestamp<=strptime("15.12.2017 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"))
                            |(data$timestamp>=strptime("01.01.2019 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC")
                            & data$timestamp<=strptime("24.01.2019 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"))
                            |(data$timestamp>=strptime("01.01.2020 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC")
                            & data$timestamp<=strptime("28.01.2020 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC")))
    
    if(length(id_CET2UTC)>0){
      data$timestamp[id_CET2UTC] <- data$timestamp[id_CET2UTC] - 3600
      rm(id_CET2UTC)
      gc()
    }
    
    data$timestamp <- strptime(strftime(data$timestamp,"%d.%m.%Y %H:%M:00",tz="UTC"),"%d.%m.%Y %H:%M:%S",tz="UTC")
    
    data           <- data[order(data$timestamp),]
    data           <- data[!duplicated(data$timestamp),]
    
    data$timestamp <- as.numeric(difftime(time1=data$timestamp,time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
    
    data           <- as.data.frame(data,stringsAsFactors=F)
    
    # Pressure correction
    
    # 2019-06-03 - 2019-06-13: Lab instrument (NABEL LAB) minus Additel 681 = -0.7  
    id_Pcorr <- which(data$timestamp>=1559520000 & data$timestamp<=1560384000)
    if(length(id_Pcorr)>0){
      data$pressure[id_Pcorr] <- data$pressure[id_Pcorr] - 0.7
    }
    # 2020-01-17 - 2020-01-28: Lab instrument (NABEL LAB) minus Additel 681 = -0.3
    id_Pcorr <- which(data$timestamp>=1579219200 & data$timestamp<=1589500800)
    if(length(id_Pcorr)>0){
      data$pressure[id_Pcorr] <- data$pressure[id_Pcorr] - 0.3
    }
    
    # insert data into DB
    
    id_insert <- which(!is.na(data$timestamp) & !is.na(data$T) & !is.na(data$pressure))
    
    if(length(id_insert)>0){
      
      query_str <- paste("INSERT INTO PressureChamber_01_DUE (timestamp,T,T_F,pressure,pressure_F)",sep="")
      query_str <- paste(query_str,"VALUES" )
      query_str <- paste(query_str,
                         paste("(",paste(data$timestamp[id_insert],",",
                                         data$T[id_insert],",",
                                         rep(1,length(id_insert)),",",
                                         data$pressure[id_insert],",",
                                         rep(1,length(id_insert)),
                                         collapse = "),(",sep=""),")",sep=""),
                         paste(" ON DUPLICATE KEY UPDATE "))
      
      query_str <- paste(query_str,paste("T=VALUES(T),",                    sep=""))
      query_str <- paste(query_str,paste("T_F=VALUES(T_F),",                sep=""))
      query_str <- paste(query_str,paste("pressure=VALUES(pressure),",      sep=""))
      query_str <- paste(query_str,paste("pressure_F=VALUES(pressure_F);",  sep=""))
      
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn( dbname=CS_DB_dbname, host="du-gsn1", port=3306, user=CS_DB_user, pass=CS_DB_pass)
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
      
    }
  }
  
  rm(data,query_str,id_insert)
  gc()
}

