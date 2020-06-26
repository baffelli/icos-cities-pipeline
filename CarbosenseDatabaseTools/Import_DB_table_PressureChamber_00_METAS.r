# Import_DB_table_PressureChamber_00_METAS.r
# ------------------------------------------
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
library(openair)
library(DBI)
require(RMySQL)
require(chron)
library(MASS)

### ----------------------------------------------------------------------------------------------------------------------

# directory

fdirectory <- "K:/Carbosense/Data/Druckkammer_Versuche_Metas/Data/"

### ----------------------------------------------------------------------------------------------------------------------------

# EMPA CarboSense DB information

CS_DB_dbname <- "CarboSense"
CS_DB_user   <- ""
CS_DB_pass   <- ""


## ----------------------------------------------------------------------------------------------------------------------

## Picarro data

# - manually downloaded data files
# - data files contain one minute data [date from, date to, CO2, H2o, CO2_DRY]
# - date refers to CET [CET->UTC] // time synchronisation of Picarro on 18 May 2017


if(T){
  
  files   <- list.files(path=fdirectory,pattern = "_MM10.csv",full.names = T)
  n_files <- length(files)
  
  #
  
  for(ith_file in 1:n_files){
    
    data <- read.table(file = files[ith_file], header = T,sep = ";")

    colnames(data) <- c("timestamp","timestamp_EI","CO2","H2O","CO2_DRY")


    # Date (UTC; begin of interval)
    data$timestamp <- strptime(data$timestamp,"%d.%m.%Y %H:%M",tz="UTC") - 3600
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
      
      query_str <- paste("INSERT INTO PressureChamber_00_METAS (timestamp,CO2,CO2_F,CO2_DRY,CO2_DRY_F,H2O,H2O_F)",sep="")
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

## Almemo FDA612SA pressure data

# - manually downloaded data files (METAS)
# - data files contain one two minute data [date, pressure]
# - date refers to UTC / CEST (Time synchronisation on 22 May 2017 in the middle of the experiment)

if(T){
  
  data <- read.table(file = "K:/Carbosense/Data/Druckkammer_Versuche_Metas/Almemo_FDA612SA/Pressure_FDA612SA_formatted.csv", header = T,sep = ";")
  
  colnames(data) <- c("timestamp","pressure")
  
  
  
  # Date (UTC; begin of interval)
  data$timestamp <- strptime(data$timestamp,"%d.%m.%Y %H:%M:%S",tz="UTC")
  data$timestamp <- strptime(strftime(data$timestamp,"%d.%m.%Y %H:%M:00",tz="UTC"),"%d.%m.%Y %H:%M:%S",tz="UTC")
  
  # correct date
  
  id_case01  <- which(data$timestamp <  strptime("22.05.2017 14:03:19","%d.%m.%Y %H:%M:%S",tz="UTC"))
  id_case02  <- which(data$timestamp >= strptime("22.05.2017 14:03:19","%d.%m.%Y %H:%M:%S",tz="UTC"))
  
  if(length(id_case01)>0){
    data$timestamp[id_case01] <- data$timestamp[id_case01] - 9*60
  }

  if(length(id_case02)>0){
    data$timestamp[id_case02] <- data$timestamp[id_case02] - 7200
  }
  
  data           <- data[order(data$timestamp),]
  data           <- data[!duplicated(data$timestamp),]
  
  
  data$timestamp <- as.numeric(difftime(time1=data$timestamp,time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
  
  data           <- as.data.frame(data,stringsAsFactors=F)
  
  # insert data into DB
  
  id_insert <- which(!is.na(data$timestamp) & !is.na(data$pressure))
  
  if(length(id_insert)>0){
    
    query_str <- paste("INSERT INTO PressureChamber_00_METAS (timestamp,pressure,pressure_F)",sep="")
    query_str <- paste(query_str,"VALUES" )
    query_str <- paste(query_str,
                       paste("(",paste(data$timestamp[id_insert],",",
                                       data$pressure[id_insert],",",
                                       rep(1,length(id_insert)),
                                       collapse = "),(",sep=""),")",sep=""),
                       paste(" ON DUPLICATE KEY UPDATE "))
    
    query_str <- paste(query_str,paste("pressure=VALUES(pressure),",             sep=""))
    query_str <- paste(query_str,paste("pressure_F=VALUES(pressure_F);",         sep=""))
    
    drv             <- dbDriver("MySQL")
    con             <- dbConnect(drv, dbname=CS_DB_dbname, host="du-gsn1", port=3306, user=CS_DB_user, pass=CS_DB_pass)
    res             <- dbSendQuery(con, query_str)
    dbClearResult(res)
    dbDisconnect(con)
    
  }
  
  rm(data,query_str,id_insert)
  gc()
}

