# Import_DB_table_EMPA_BRM.r
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
library(openair)
library(DBI)
require(RMySQL)
require(chron)
library(MASS)

## ----------------------------------------------------------------------------------------------------------------------

## DB information

DB_group_in  <- "empaGSN"
DB_group_out <- "CarboSense_MySQL"

## ----------------------------------------------------------------------------------------------------------------------

## Read data from Database "empaGSN"

measurement_heights <- c(12,45,72,132,212)
VALVEPOS_occupancy  <- c(9,8,7,6,5)

data                <- NULL

for(ith_measurement_height in 1:length(measurement_heights)){
  
  table           <- paste("beromuenster_",sprintf("%.0f",measurement_heights[ith_measurement_height]),"m_1min_cal",sep="")
  query_str       <- paste("SELECT * from ",table," WHERE timed >= 1483228800000;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- carboutil::get_conn(group=DB_group_in)
  res             <- dbSendQuery(con, query_str)
  tmp             <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  cn_required     <- c("timed","VALVEPOS","CO2_DRY","CO2_DRY_N","CO2_DRY_SE","CO2_DRY_UNCERT","CO2_DRY_TOT_UNC","CO2_DRY_ERR","CO2_DRY_FLAG","H2O","H2O_N","H2O_SE","H2O_FLAG")
  tmp             <- tmp[,which(colnames(tmp)%in%cn_required)]
  tmp$MEAS_HEIGHT <- rep(measurement_heights[ith_measurement_height],dim(tmp)[1])
  
  # Check valvepos
  
  id <- which(!is.na(tmp$VALVEPOS) & tmp$VALVEPOS==VALVEPOS_occupancy[ith_measurement_height])
  
  if(length(id)>0){
    tmp <- tmp[id,]
  }else{
    stop()
  }
  
  # Check CO2_DRY
  
  id <- which(!(tmp$CO2_DRY>200 | tmp$CO2_DRY < 2000) | is.na(tmp$CO2_DRY))
  
  if(length(id)>0){
    tmp$CO2_DRY[id]         <- -999
    tmp$CO2_DRY_N[id]       <- -999
    tmp$CO2_DRY_SE[id]      <- -999
    tmp$CO2_DRY_UNCERT[id]  <- -999
    tmp$CO2_DRY_TOT_UNC[id] <- -999
    tmp$CO2_DRY_ERR[id]     <- -999
    tmp$CO2_DRY_FLAG[id]    <- -999
  }
  
  id <- which(is.na(tmp$CO2_DRY_N))
  if(length(id)>0){
    tmp$CO2_DRY_N[id] <- -999
  }
  
  id <- which(is.na(tmp$CO2_DRY_SE))
  if(length(id)>0){
    tmp$CO2_DRY_SE[id] <- -999
  }
  
  id <- which(is.na(tmp$CO2_DRY_UNCERT))
  if(length(id)>0){
    tmp$CO2_DRY_UNCERT[id] <- -999
  }
  
  id <- which(is.na(tmp$CO2_DRY_TOT_UNC))
  if(length(id)>0){
    tmp$CO2_DRY_TOT_UNC[id] <- -999
  }
  
  id <- which(is.na(tmp$CO2_DRY_ERR))
  if(length(id)>0){
    tmp$CO2_DRY_ERR[id] <- -999
  }
  
  id <- which(is.na(tmp$CO2_DRY_FLAG))
  if(length(id)>0){
    tmp$CO2_DRY_FLAG[id] <- -999
  }
  
  # Check H2O
  
  id <- which(!(tmp$H2O>0 | tmp$H2O < 100) | is.na(tmp$H2O))
  
  if(length(id)>0){
    tmp$H2O[id]      <- -999
    tmp$H2O_N[id]    <- -999
    tmp$H2O_SE[id]   <- -999
    tmp$H2O_FLAG[id] <- -999
  }
  
  id <- which(is.na(tmp$H2O_N))
  if(length(id)>0){
    tmp$H2O_N[id] <- -999
  }
  
  id <- which(is.na(tmp$H2O_SE))
  if(length(id)>0){
    tmp$H2O_SE[id] <- -999
  }
  
  id <- which(is.na(tmp$H2O_FLAG))
  if(length(id)>0){
    tmp$H2O_FLAG[id] <- -999
  }
  
  #
  
  if(is.null(data)){
    data <- tmp
  }else{
    data <- rbind(data,tmp)
  }
}

data$timed <- data$timed/1e3
data$CO2   <- data$CO2_DRY * ( 1 - data$H2O/100)
data$CO2_F <- 1

#

id <- which(data$CO2_DRY == -999 | data$H2O == -999)

if(length(id)>0){
  data$CO2[id]   <- -999
  data$CO2_F[id] <- 0
}

# Change colname timed to timestamp

colnames(data)[which(colnames(data)=="timed")] <- "timestamp"

# sort data

data <- data[order(data$timestamp),]


## Write data into Database "CarboSense_MySQL"

#
query_str       <- paste("DELETE from UNIBE_BRM;",sep="")
drv             <- dbDriver("MySQL")
con             <- carboutil::get_conn(group=DB_group_out)
res             <- dbSendQuery(con, query_str)
dbClearResult(res)
dbDisconnect(con)

#

N_dataPackets2insert <- 1e4

for(row_A in seq(1,dim(data)[1],N_dataPackets2insert)){
  
  row_B     <- min(c(row_A+N_dataPackets2insert-1,dim(data)[1])) 
  
  id_insert <- (row_A:row_B)

  query_str <- paste("INSERT INTO UNIBE_BRM (",paste(colnames(data)[1:16],collapse = ","),") ",sep="")
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
                                     data[id_insert,16],
                                     collapse = "),(",sep=""),")",sep=""),";")
  
  
  drv             <- dbDriver("MySQL")
  con             <- carboutil::get_conn(,group=DB_group_out)
  res             <- dbSendQuery(con, query_str)
  dbClearResult(res)
  dbDisconnect(con)
}



