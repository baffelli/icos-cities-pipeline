# Compute_CarboSense_CO2_values_FINAL.r
# -------------------------------------
#
# Author: Michael Mueller
#
#
# -------------------------------------

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

# Anchor events file of simplest processing version

anchor_events_fn <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/anchor_events_SU_ALL.csv"

# Resultdir

resultdir        <- "/project/CarboSense/Carbosense_Network/PROCESSING/"

# Date now

data_now         <- strptime(strftime(Sys.time(),"%Y-%m-%d %H:%M:%S",tz="UTC"),"%Y-%m-%d %H:%M:%S",tz="UTC") 

### ----------------------------------------------------------------------------------------------------------------------------

# DB queries

query_str       <- paste("SELECT * FROM Deployment WHERE LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1') and Date_UTC_to > '2017-07-01 00:00:00' and SensorUnit_ID BETWEEN 1010 AND 1334;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_deployment  <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_deployment$Date_UTC_from  <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to    <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

tbl_deployment$timestamp_from <- as.numeric(difftime(time1 = tbl_deployment$Date_UTC_from ,time2=strptime("19700101000000","%Y%m%d%H%M%S"), units="secs",tz="UTC"))
tbl_deployment$timestamp_to   <- as.numeric(difftime(time1 = tbl_deployment$Date_UTC_to,   time2=strptime("19700101000000","%Y%m%d%H%M%S"), units="secs",tz="UTC"))

##

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_location    <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

### ----------------------------------------------------------------------------------------------------------------------------

anchor_events <- read.table(anchor_events_fn,header=T,sep=";")

### ----------------------------------------------------------------------------------------------------------------------------

statistics <- NULL


for(ith_depl in 1:dim(tbl_deployment)[1]){
  
  # Altitude
  
  id_loc   <- which(tbl_location$LocationName==tbl_deployment$LocationName[ith_depl])
  
  Altitude <- tbl_location$h[id_loc] + tbl_deployment$HeightAboveGround[ith_depl]
  
  # Drift correction information
  
  id_anchor_events <- which(anchor_events$SensorUnit_ID    == tbl_deployment$SensorUnit_ID[ith_depl]
                            & anchor_events$LocationName   == tbl_deployment$LocationName[ith_depl]
                            & anchor_events$timestamp_from >= tbl_deployment$timestamp_from[ith_depl]
                            & anchor_events$timestamp_to   <= tbl_deployment$timestamp_to[ith_depl]
                            & !is.na(anchor_events$SU_CO2_corr))
  
  n_id_anchor_events <- length(id_anchor_events)
  
  if(n_id_anchor_events==0){
    SU_CORR <- 0
  }else{
    if(n_id_anchor_events > 3){
      SU_CORR <- median(anchor_events$SU_CO2_corr[id_anchor_events[(n_id_anchor_events-2):(n_id_anchor_events)]])
    }else{
      SU_CORR <- median(anchor_events$SU_CO2_corr[id_anchor_events])
    }
  }
  
  #
  
  PreferredCalModel <- "CarboSense_CO2_TEST01"
  
  if(Altitude>1200){
    PreferredCalModel <- "CarboSense_CO2_TEST00"
  }
  
  if(abs(SU_CORR)>300){
    PreferredCalModel <- "CarboSense_CO2_TEST00"
  }
  
  #
  
  statistics <- rbind(statistics, data.frame(SensorUnit_ID     = tbl_deployment$SensorUnit_ID[ith_depl],
                                             LocationName      = tbl_deployment$LocationName[ith_depl],
                                             DateOfProcessing  = strftime(data_now,"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                             Date_UTC_from     = strftime(tbl_deployment$Date_UTC_from[ith_depl],"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                             Date_UTC_to       = strftime(tbl_deployment$Date_UTC_to[ith_depl],  "%Y-%m-%d %H:%M:%S",tz="UTC"),
                                             timestamp_from    = tbl_deployment$timestamp_from[ith_depl],
                                             timestamp_to      = tbl_deployment$timestamp_to[ith_depl],
                                             Altitude          = Altitude,
                                             SU_CORR           = SU_CORR,
                                             PreferredCalModel = PreferredCalModel,
                                             stringsAsFactors  = F))
}

### ----------------------------------------------------------------------------------------------------------------------------


# Copy processed from particular sources into table "CarboSense_CO2_FINAL"

for(ith_row in 1:dim(statistics)[1]){
  
  # Delete existing data in table CarboSense_CO2_FINAL
  
  query_str       <- paste("DELETE FROM CarboSense_CO2_FINAL WHERE LocationName = '",statistics$LocationName[ith_row],"' and SensorUnit_ID = ",statistics$SensorUnit_ID[ith_row]," and timestamp >= ",statistics$timestamp_from[ith_row]," and timestamp <= ",statistics$timestamp_to[ith_row],";",sep="")
  drv             <- dbDriver("MySQL")
  con <-carboutil::get_conn()
  res             <- dbSendQuery(con, query_str)
  tbl_deployment  <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  # Insert data into table CarboSense_CO2_FINAL (copy) 
  
  query_str       <- paste("INSERT INTO CarboSense_CO2_FINAL (LocationName,SensorUnit_ID,timestamp,CO2,CO2_L,CO2_A,H2O,LP8_IR,LP8_IR_L,LP8_T,SHT21_T,SHT21_RH,FLAG,Q_FLAG,O_FLAG,L_FLAG,LRH_FLAG) ",sep="")
  query_str       <- paste(query_str, "SELECT LocationName,SensorUnit_ID,timestamp,CO2,CO2_L,CO2_A,H2O,LP8_IR,LP8_IR_L,LP8_T,SHT21_T,SHT21_RH,FLAG,Q_FLAG,O_FLAG,L_FLAG,LRH_FLAG FROM ",statistics$PreferredCalModel[ith_row]," ",sep="")
  query_str       <- paste(query_str, "WHERE LocationName = '",statistics$LocationName[ith_row],"' and SensorUnit_ID = ",statistics$SensorUnit_ID[ith_row]," and timestamp >= ",statistics$timestamp_from[ith_row]," and timestamp <= ",statistics$timestamp_to[ith_row],";",sep="")
  drv             <- dbDriver("MySQL")
  con <-carboutil::get_conn()
  res             <- dbSendQuery(con, query_str)
  tbl_deployment  <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
}

### ----------------------------------------------------------------------------------------------------------------------------

# store statistics file

write.table(statistics,paste(resultdir,"PreferredCalModel.csv",sep=""),row.names = F,col.names = T, sep=";",quote = F)

### ----------------------------------------------------------------------------------------------------------------------------
