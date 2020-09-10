# Import_DB_table_NABEL_RIG.r
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

## ----------------------------------------------------------------------------------------------------------------------

## DB information

DB_group <- "CarboSense_MySQL"

## ----------------------------------------------------------------------------------------------------------------------

## Directories

fdirectory <- "K:/Nabel/Daten/Stationen/RIG/"
fdirectory <- "/project/CarboSense/Win_K/Daten/Stationen/RIG/"

## ----------------------------------------------------------------------------------------------------------------------

## NABEL RIG Data

# - automatically downloaded data files [directory: "K:/Nabel/Daten/Stationen/RIG/"]
# - data files contain one minute data [date, values of NABEL measurement programm at RIG plus respective flags]
# - date refers to CET

if(T){
  
  files   <- list.files(path=fdirectory,pattern = "RIG Gase und Meteo_",full.names = T)
  n_files <- length(files)
  
  #
  
  for(ith_file in 1:n_files){
    
    data <- read.table(file = files[ith_file],header = F,sep = ";",skip = 4)
    
    
    if(!dim(data)[2]%in%c(20,22)){
      stop("Unexpected number of columns in file!")
    }
    if(dim(data)[2]==20){
      data <- cbind(data[,1:19],
                    data.frame(rep(-999,dim(data)[1]),rep(-999,dim(data)[1]),stringsAsFactors=F))
    }
    
    # ! Order of col in imported files does not correspond to the order in the database !!!
    colnames(data) <- c("timestamp","NO","NO_F","NO2","NO2_F","O3","O3_F","CO2","CO2_F","CO2_DRY","CO2_DRY_F","H2O","H2O_F","T","T_F","RH","RH_F","pressure","pressure_F","CO","CO_F")
    data           <- cbind(data[,1:7],data[,20:21],data[,8:19])
    
    
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
        id_ok <- which(data[,pos]!=""
                       & data[,pos]!= -999
                       & data[,pos] >  350
                       & !is.na(data[,pos])
                       & (data[,pos_F]=="" | is.na(data[,pos_F])))
      }
      
      data[,pos_F]      <- 0
      data[id_ok,pos_F] <- 1
      
      data[!(1:dim(data)[1])%in%id_ok,pos] <- -999
      
      
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
    
    #
    
    data <- as.data.frame(data,stringsAsFactors=F)
    
    # insert data into DB
    
    query_str <- "SELECT MAX(timestamp) AS MAX_timestamp FROM NABEL_RIG WHERE CO2!=-999 or NO2!=-999 or O3!=-999;";
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
      
      query_str <- paste("INSERT INTO NABEL_RIG (",paste(colnames(data)[1:21],collapse = ","),") ",sep="")
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
                                         data[id_insert,15],",",
                                         data[id_insert,16],",",
                                         data[id_insert,17],",",
                                         data[id_insert,18],",",
                                         data[id_insert,19],",",
                                         data[id_insert,20],",",
                                         data[id_insert,21],
                                         collapse = "),(",sep=""),")",sep=""),
                         paste(" ON DUPLICATE KEY UPDATE "))
      
      for(ii in 2:21){
        if(ii==21){
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

