# Import_DB_table_METEOSWISS_Measurements.r
# -----------------------------------------
#
# Author: Michael Mueller
#
# - Imports meteo measurements from the files obtained by MeteoSwiss
# - Imports meteo measurements from selected NABEL sites
#
# ------------------------------------------------------
#

## clear variables
rm(list=ls(all=TRUE))
gc()

## ----------------------------------------------------------------------------------------------------------------------

## libraries
library(openair)
library(DBI)
require(RMySQL)
require(chron)

## ----------------------------------------------------------------------------------------------------------------------

COMPLETE_IMPORT    <- F

NABEL_METEO_IMPORT <- T

## ----------------------------------------------------------------------------------------------------------------------

## DB information

CarboSense_DB_group <- "CarboSense_MySQL"
NabelGsn_DB_group   <- "NabelGsn"

## ----------------------------------------------------------------------------------------------------------------------

## Directories

fdirectory   <- "/project/CarboSense/Data/METEO/MCH_DAILY_DATA_DUMP"
logdirectory <- "/project/CarboSense/Software/LOGs"

## ----------------------------------------------------------------------------------------------------------------------

## tbl Location

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con<-carboutil::get_conn(group=CarboSense_DB_group)
res             <- dbSendQuery(con, query_str)
tbl_location    <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

## tbl MCH_EP

query_str       <- paste("SELECT * FROM MCHMeasExclusionPeriods;",sep="")
drv             <- dbDriver("MySQL")
con<-carboutil::get_conn(group=CarboSense_DB_group)
res             <- dbSendQuery(con, query_str)
tbl_MCH_EP      <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_MCH_EP$Date_UTC_from  <- strptime(tbl_MCH_EP$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_MCH_EP$Date_UTC_to    <- strptime(tbl_MCH_EP$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

tbl_MCH_EP$timestamp_from <- as.numeric(difftime(time1=tbl_MCH_EP$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
tbl_MCH_EP$timestamp_to   <- as.numeric(difftime(time1=tbl_MCH_EP$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

## ----------------------------------------------------------------------------------------------------------------------

date_now <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")

## ----------------------------------------------------------------------------------------------------------------------

# Get names of files in directory

files             <- list.files(path = fdirectory,pattern = "VQEA33.[[:digit:]]{12}.csv",full.names = T)
n_files           <- length(files)

# Computation of date of files from file name

tmp               <- gsub(pattern = paste(fdirectory,"/VQEA33.",sep=""),replacement = "",x = files)
tmp               <- gsub(pattern = paste(".csv",sep=""),              replacement = "",x = tmp)
datesOfFiles      <- strptime(tmp,"%Y%m%d%H%M",tz="UTC")

# Import of all files / last file

if(!COMPLETE_IMPORT){
  id_files2import <- which(datesOfFiles>=(date_now-2*86400))
}else{
  id_files2import <- 1:n_files
}
n_id_files2import <- length(id_files2import)


# Loop over all files

vec_header        <- c("stn","time","","gre000z0","fkl010z0","dkl010z0","prestas0","rre150z0","tre200s0","ure200s0","sre000z0")

if(n_id_files2import>0){
  for(ith_file in 1:n_id_files2import){
    
    
    data <- read.table(file = files[id_files2import[ith_file]],header = F,skip=2,sep=";",as.is=T)
    
    if(!all(data[1,1:dim(data)[2]]==vec_header)){
      
      mess <- NULL
      mess <- rbind(mess,vec_header)
      mess <- rbind(mess,data[1,1:dim(data)[2]])
      
      write.table(x = mess,file = paste(logdirectory,"/VQEA33_",strftime(datesOfFiles[id_files2import[ith_file]],"%Y%m%d%H%M%S",tz="UTC"),".log",sep=""),
                  col.names=F,row.names=F,sep=";")
      
      stop()
    }
    
    data           <- data[3:dim(data)[1],]
    colnames(data) <- vec_header
    
    # Date : change of timestamp : end of interval --> begin of interval
    data$date      <- strptime(paste(data$time," ",data[,3],":00",sep=""),"%Y%m%d %H:%M:%S",tz="UTC")  - 600
    data$timestamp <- as.numeric(difftime(time1=data$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    
    
    date_UTC_from  <- min(data$date)
    date_UTC_to    <- max(data$date)
    
    timestamp_from <- as.numeric(difftime(time1=date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    timestamp_to   <- as.numeric(difftime(time1=date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
    
    u_sites        <- sort(unique(data$stn))
    n_u_sites      <- length(u_sites)
    
    # Convert data to numeric values (change "-" to -999)
    
    for(ith_col in 4:11){
      tmp        <- data[,ith_col]
      id_setToNA <- which(data[,ith_col] == "-")
      id_num     <- which(data[,ith_col] != "-")
      
      n_id_setToNA <- length(id_setToNA)
      n_id_num     <- length(id_num)
      
      if((n_id_setToNA+n_id_num) != dim(data)[1]){
        stop()
      }
      
      data[,ith_col]       <- -999
      data[id_num,ith_col] <- as.numeric(tmp[id_num])
    }
    
    
    
    # check if all sites are listed in tbl Location
    
    check_sites  <- NULL
    import_sites <- rep(F,n_u_sites)
    
    for(ith_site in 1:n_u_sites){
      
      id_loc <- which(tbl_location$LocationName==u_sites[ith_site])
      
      if(length(id_loc)!=1){
        check_sites <- rbind(check_sites,data.frame(LocationName=u_sites[ith_site],Issue="Not included in tbl Location.",stringsAsFactors = F))
        next
      }
      
      if(tbl_location$Network[id_loc]!="METEOSWISS"){
        check_sites <- rbind(check_sites,data.frame(LocationName=u_sites[ith_site],Issue="Network not Meteoswiss.",stringsAsFactors = F))
        next
      }
      
      import_sites[ith_site] <- T
    }
    
    if(!is.null(check_sites)){
      write.table(x = check_sites,file = paste(logdirectory,"/VQEA33_",strftime(datesOfFiles[id_files2import[ith_file]],"%Y%m%d%H%M%S",tz="UTC"),".log",sep=""),
                  col.names=T,row.names=F,sep=";")
    }
    
    # Import meteo measurements
    
    for(ith_site in 1:n_u_sites){
      
      if(!import_sites[ith_site]){
        next
      }
      
      # Check for entries in MCHMesExclusionPeriods
      
      for(cn in c("gre000z0","fkl010z0","dkl010z0","prestas0","rre150z0","tre200s0","ure200s0","sre000z0")){
        
        if(cn=="gre000z0"){
          cn_match <- "Radiance"
        }
        if(cn=="fkl010z0"){
          cn_match <- "Windspeed"
        }
        if(cn=="dkl010z0"){
          cn_match <- "Winddirection"
        }
        if(cn=="prestas0"){
          cn_match <- "Pressure"
        }
        if(cn=="rre150z0"){
          cn_match <- "Rain"
        }
        if(cn=="tre200s0"){
          cn_match <- "Temperature"
        }
        if(cn=="ure200s0"){
          cn_match <- "RH"
        }
        if(cn=="sre000z0"){
          cn_match <- "Sunshine"
        }
        
        id_MCH_EP   <- which(tbl_MCH_EP$LocationName == u_sites[ith_site] & tbl_MCH_EP$Measurement == cn_match)
        n_id_MCH_EP <- length(id_MCH_EP)
        
        if(n_id_MCH_EP>0){
          for(ith_MCH_EP in 1:n_id_MCH_EP){
            pos <- which(colnames(data)==cn)
            
            id_setToNA <- which(    data$timestamp >= tbl_MCH_EP$timestamp_from[id_MCH_EP[ith_MCH_EP]]
                                  & data$timestamp <= tbl_MCH_EP$timestamp_to[  id_MCH_EP[ith_MCH_EP]]
                                  & data$stn       == u_sites[ith_site])
            
            if(length(id_setToNA)>0){
              data[id_setToNA,pos] <- -999
            }
          }
        }
      }
      
      #
      
      id_data_site   <- which(data$stn==u_sites[ith_site])
      n_id_data_site <- length(id_data_site)
      
      if(n_id_data_site>0){
        
        query_str       <- paste("DELETE FROM METEOSWISS_Measurements WHERE LocationName = '",u_sites[ith_site],"' and timestamp>=",timestamp_from," and timestamp<=",timestamp_to,";",sep="")
        drv             <- dbDriver("MySQL")
        con<-carboutil::get_conn(group=CarboSense_DB_group)
        res             <- dbSendQuery(con, query_str)
        dbClearResult(res)
        dbDisconnect(con)
        
        #
        
        query_str <- paste("INSERT INTO METEOSWISS_Measurements (LocationName,timestamp,Radiance,Windspeed,Winddirection,Pressure,Rain,Temperature,RH,Sunshine)",sep="")
        query_str <- paste(query_str,"VALUES" )
        query_str <- paste(query_str,
                           paste("(",paste("'",data$stn[id_data_site],"',",
                                           data$timestamp[id_data_site],",",
                                           data$gre000z0[id_data_site],",",
                                           data$fkl010z0[id_data_site],",",
                                           data$dkl010z0[id_data_site],",",
                                           data$prestas0[id_data_site],",",
                                           data$rre150z0[id_data_site],",",
                                           data$tre200s0[id_data_site],",",
                                           data$ure200s0[id_data_site],",",
                                           data$sre000z0[id_data_site],
                                           collapse = "),(",sep=""),")",sep=""))
        
        drv             <- dbDriver("MySQL")
        con<-carboutil::get_conn(group=CarboSense_DB_group)
        res             <- dbSendQuery(con, query_str)
        dbClearResult(res)
        dbDisconnect(con)
      }
      
    }
  }
}

## ----------------------------------------------------------------------------------------------------------------------

# Import NABEL meteo data

if(NABEL_METEO_IMPORT){
  
  for(NABEL_SITE in c("NABRIG","NABHAE","NABZUE","NABDUE")){
    
    #
    
    if(NABEL_SITE == "NABRIG"){
      NabelGSN_tbl_name <- "nabelnrt_rig"
    }
    if(NABEL_SITE == "NABHAE"){
      NabelGSN_tbl_name <- "nabelnrt_hae"
    }
    if(NABEL_SITE == "NABZUE"){
      NabelGSN_tbl_name <- "nabelnrt_zue"
    }
    if(NABEL_SITE == "NABDUE"){
      NabelGSN_tbl_name <- "nabelnrt_due"
    }
    
    # Retrieve timestamp of last entry in CarboSense.METEOSWISS_Measurements
    
    query_str       <- paste("SELECT MAX(timestamp) as TS_LAST_ENTRY FROM METEOSWISS_Measurements WHERE LocationName = '",NABEL_SITE,"' and (Radiance != -999 or Windspeed != -999 or Winddirection != -999 or Pressure != -999 or Rain != -999 or Temperature != -999 or RH != -999);",sep="")
    drv             <- dbDriver("MySQL")
    con<-carboutil::get_conn(group=CarboSense_DB_group)
    res             <- dbSendQuery(con, query_str)
    TS_LAST_ENTRY   <- as.integer(dbFetch(res, n=-1))
    dbClearResult(res)
    dbDisconnect(con)
    
    # Select data in NabelGsn.table
    
    TS_LAST_ENTRY <- (TS_LAST_ENTRY + 1200)*1e3
    
    query_str       <- paste("SELECT timed,PRESSURE,RELATIVEHUMIDITY,TEMPERATURE,WINDDIRECTION,WINDSPEED,GLOBALRADIATION,PRECIPITATION FROM ",NabelGSN_tbl_name," WHERE timed >= ",TS_LAST_ENTRY,";",sep="")
    drv             <- dbDriver("MySQL")
    con<-carboutil::get_conn(group=NabelGsn_DB_group)
    res             <- dbSendQuery(con, query_str)
    NABEL_data      <- dbFetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    # Check for entries in MCHMesExclusionPeriods
    
    for(cn in c("PRESSURE","RELATIVEHUMIDITY","TEMPERATURE","WINDDIRECTION","WINDSPEED","GLOBALRADIATION","PRECIPITATION")){
      
      if(cn=="PRESSURE"){
        cn_match <- "Pressure"
      }
      if(cn=="RELATIVEHUMIDITY"){
        cn_match <- "RH"
      }
      if(cn=="TEMPERATURE"){
        cn_match <- "Temperature"
      }
      if(cn=="WINDDIRECTION"){
        cn_match <- "Winddirection"
      }
      if(cn=="WINDSPEED"){
        cn_match <- "Windspeed"
      }
      if(cn=="GLOBALRADIATION"){
        cn_match <- "Radiance"
      }
      if(cn=="PRECIPITATION"){
        cn_match <- "Rain"
      }
      
      id_MCH_EP   <- which(tbl_MCH_EP$LocationName == NABEL_SITE & tbl_MCH_EP$Measurement == cn_match)
      n_id_MCH_EP <- length(id_MCH_EP)
      
      if(n_id_MCH_EP>0){
        for(ith_MCH_EP in 1:n_id_MCH_EP){
          pos <- which(colnames(NABEL_data)==cn)
          
          id_setToNA <- which(  NABEL_data$timed >= 1e3*tbl_MCH_EP$timestamp_from[id_MCH_EP[ith_MCH_EP]]
                                & NABEL_data$timed <= 1e3*tbl_MCH_EP$timestamp_to[  id_MCH_EP[ith_MCH_EP]])
          
          if(length(id_setToNA)>0){
            NABEL_data[id_setToNA,pos] <- NA
          }
        }
      }
    }
    
    # Additional data checking
    
    insert <- rep(F,dim(NABEL_data)[1])
    
    for(cn in c("PRESSURE","RELATIVEHUMIDITY","TEMPERATURE","WINDDIRECTION","WINDSPEED","GLOBALRADIATION","PRECIPITATION")){
      
      pos <- which(colnames(NABEL_data)==cn)
      
      if(length(pos)!=1){
        stop()
      }
      
      id   <- which(is.na(NABEL_data[,pos]) | is.null(NABEL_data[,pos]) | !is.numeric(NABEL_data[,pos]))
      n_id <- length(id)
      
      if(length(id)>0){
        NABEL_data[id,pos] <- rep(-999,n_id)
      }
      
      insert <- insert | NABEL_data[,pos] != -999
    }

    
    # Insert NABEL data into table CarboSense.METEOSWISS_Measurements
    
    id_insert   <- which(insert==T)
    n_id_insert <- length(id_insert)
    
    if(n_id_insert>0){
      
      query_str <- paste("INSERT INTO METEOSWISS_Measurements (LocationName,timestamp,Radiance,Windspeed,Winddirection,Pressure,Rain,Temperature,RH,Sunshine)",sep="")
      query_str <- paste(query_str,"VALUES" )
      query_str <- paste(query_str,
                         paste("(",paste("'",NABEL_SITE,"',",
                                         NABEL_data$timed[id_insert]/1e3-600,",",
                                         NABEL_data$GLOBALRADIATION[id_insert],",",
                                         NABEL_data$WINDSPEED[id_insert],",",
                                         NABEL_data$WINDDIRECTION[id_insert],",",
                                         NABEL_data$PRESSURE[id_insert],",",
                                         NABEL_data$PRECIPITATION[id_insert],",",
                                         NABEL_data$TEMPERATURE[id_insert],",",
                                         NABEL_data$RELATIVEHUMIDITY[id_insert],",",
                                         rep(-999,n_id_insert),
                                         collapse = "),(",sep=""),")",sep=""))
      
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn(group=CarboSense_DB_group)
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
  }
}