# Import_DB_table_METEOSWISS_Pressure_MAN_DOWNLOADED_FILES.r
# ----------------------------------------------------------
#
# Author: Michael Mueller
#
#
# ----------------------------------------------------------

# Remarks:
# - Computations refer to UTC.
#
#

## clear variables
rm(list=ls(all=TRUE))
gc()

## libraries
library(DBI)
require(RMySQL)
require(chron)

### ----------------------------------------------------------------------------------------------------------------------------

## DB information

CarboSense_DB_group <- "CarboSense_MySQL"

### ----------------------------------------------------------------------------------------------------------------------------

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con<-carboutil::get_conn(group=CarboSense_DB_group)
res             <- dbSendQuery(con, query_str)
tbl_location    <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

### ----------------------------------------------------------------------------------------------------------------------------

dirs <- NULL
dirs <- c(dirs, "/project/CarboSense/Data/METEO/PRESSURE_prestas0/prestas0_01012017-28032018/")
dirs <- c(dirs, "/project/CarboSense/Data/METEO/PRECIPITATION_rre150z0/")
dirs <- c(dirs, "/project/CarboSense/Data/METEO/RADIANCE_gre000z0/")
dirs <- c(dirs, "/project/CarboSense/Data/METEO/RH_ure200s0/")
dirs <- c(dirs, "/project/CarboSense/Data/METEO/WIND_dkl010z0/")
dirs <- c(dirs, "/project/CarboSense/Data/METEO/WIND_fkl010z0/")
dirs <- c(dirs, "/project/CarboSense/Data/METEO/Sunshine_sre000z0/")
dirs <- c(dirs, "/project/CarboSense/Data/METEO/Temperature_tre200s0/")

files <- NULL

for(ith_dir in 1:length(dirs)){
  files <- c(files,list.files(path=dirs[ith_dir],pattern = "order_[[:digit:]]*_data.txt",full.names = T))
  
}
n_files <- length(files)


for(ith_file in 1:n_files){
  
  data    <- read.table(files[ith_file],sep=";",skip = 2,header=T,as.is=T)
  colName <- NULL
  
  if(colnames(data)[3]=="gre000z0"){
    colName <- "Radiance"
  }
  if(colnames(data)[3]=="fkl010z0"){
    colName <- "Windspeed"
  }
  if(colnames(data)[3]=="dkl010z0"){
    colName <- "Winddirection"
  }
  if(colnames(data)[3]=="prestas0"){
    colName <- "Pressure"
  }
  if(colnames(data)[3]=="rre150z0"){
    colName <- "Rain"
  }
  if(colnames(data)[3]=="tre200s0"){
    colName <- "Temperature"
  }
  if(colnames(data)[3]=="ure200s0"){
    colName <- "RH"
  }
  if(colnames(data)[3]=="sre000z0"){
    colName <- "Sunshine"
  }

  if(is.null(colName)){
    stop()
  }else{
    colnames(data) <- c("LocationName","date_str",colName)
  }
  
  
  # Date : change of timestamp : end of interval --> begin of interval
  data$date      <- strptime(data$date_str,"%Y%m%d%H%M",tz="UTC") - 600
  data$timestamp <- as.numeric(difftime(time1=data$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
  
  data           <- data[data[,3]!="-",]
  data           <- data[data$LocationName!="",]
  data[,3]       <- as.numeric(data[,3])
  data           <- data[!is.na(data[,3]),]
  
  data           <- data[order(data$timestamp),]
  
  #
  
  u_LocationName   <- sort(unique(data$LocationName))
  n_u_LocationName <- length(u_LocationName)
  
  for(ith_loc in 1:n_u_LocationName){
    
    print(paste(colName,u_LocationName[ith_loc]))
    
    id_loc <- which(tbl_location$LocationName==u_LocationName[ith_loc])
    
    if(length(id_loc)==1){
      
      id   <- which(data$LocationName==u_LocationName[ith_loc])
      n_id <- length(id)
      
      median_ts_diff <- median(diff(data$timestamp[id]))
      
      if(median_ts_diff!=600){
        next
      }
      
      if(tbl_location$Network[id_loc]!="METEOSWISS"){
        next
      }
      
      # insert data into database
      
      id_insert <- id[which(!is.na(data[id,3])
                            & is.numeric(data[id,3])
                            & !is.na(data$timestamp[id])
                            & is.numeric(data$timestamp[id]))]
      
      n_id_insert <- length(id_insert)
      
      if(n_id_insert>0){
        
        data2insert <- data.frame(timestamp=data$timestamp[id_insert],
                                  Placeholder=data[id_insert,3],
                                  stringsAsFactors = F)
        
        colnames(data2insert) <- c("timestamp",colName)
        
      }else{
        print("a")
        next
      }
      
      
      # select existing site data from database
      
      query_str       <- paste("SELECT * FROM METEOSWISS_Measurements WHERE LocationName='",u_LocationName[ith_loc],"';",sep="")
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn(group=CarboSense_DB_group)
      res             <- dbSendQuery(con, query_str)
      existing_data   <- dbFetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      # merge existing and new data from database
      
      if(dim(existing_data)[1]==0){
        
        data2insert$LocationName <- rep(u_LocationName[ith_loc],n_id_insert)
        
        if(!("Radiance" %in% colnames(data2insert))){
          data2insert$Radiance      <- -999
        }
        if(!("Windspeed" %in% colnames(data2insert))){
          data2insert$Windspeed     <- -999
        }
        if(!("Winddirection" %in% colnames(data2insert))){
          data2insert$Winddirection <- -999
        }
        if(!("Pressure" %in% colnames(data2insert))){
          data2insert$Pressure      <- -999
        }
        if(!("Rain" %in% colnames(data2insert))){
          data2insert$Rain          <- -999
        }
        if(!("Temperature" %in% colnames(data2insert))){
          data2insert$Temperature   <- -999
        }
        if(!("RH" %in% colnames(data2insert))){
          data2insert$RH            <- -999
        }
        if(!("Sunshine" %in% colnames(data2insert))){
          data2insert$Sunshine      <- -999
        }
        
      }else{
        pos         <- which(colnames(existing_data)!=colName)
        data2insert <- merge(existing_data[,pos],data2insert,all=T,by="timestamp")
        
        for(ith_col in 1:dim(data2insert)[2]){
          id_setToNa  <- which(is.na(data2insert[,ith_col]))
          if(length(id_setToNa)>0){
            data2insert[id_setToNa,ith_col] <- -999
          }
        }
        data2insert$LocationName <- u_LocationName[ith_loc]
      }
      
      # delete existing data from database
      
      query_str       <- paste("DELETE FROM METEOSWISS_Measurements WHERE LocationName='",u_LocationName[ith_loc],"';",sep="")
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn(group=CarboSense_DB_group)
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
      
      # insert all data into database
      
      query_str <- paste("INSERT INTO METEOSWISS_Measurements (LocationName,timestamp,Radiance,Windspeed,Winddirection,Pressure,Rain,Temperature,RH,Sunshine)",sep="")
      query_str <- paste(query_str,"VALUES" )
      query_str <- paste(query_str,
                         paste("(",paste("'",data2insert$LocationName,"',",
                                         data2insert$timestamp,",",
                                         data2insert$Radiance,",",
                                         data2insert$Windspeed,",",
                                         data2insert$Winddirection,",",
                                         data2insert$Pressure,",",
                                         data2insert$Rain,",",
                                         data2insert$Temperature,",",
                                         data2insert$RH,",",
                                         data2insert$Sunshine,
                                         collapse = "),(",sep=""),")",sep=""))
      
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn(group=CarboSense_DB_group)
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
  }
}
