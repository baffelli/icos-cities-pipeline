# Import_DB_table_EMPA_LAEG.r
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

## ----------------------------------------------------------------------------------------------------------------------

## DB information

DB_group_in  <- "empaGSN"
DB_group_out <- "CarboSense_MySQL"

## ----------------------------------------------------------------------------------------------------------------------

query_str               <- "SELECT * FROM RefMeasExclusionPeriods WHERE LocationName = 'LAEG';";
drv                     <- dbDriver("MySQL")
con                     <- dbConnect(drv,group=DB_group_out)
res                     <- dbSendQuery(con, query_str)
RefMeasExclusionPeriods <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

RefMeasExclusionPeriods$Date_UTC_from  <- strptime(RefMeasExclusionPeriods$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
RefMeasExclusionPeriods$Date_UTC_to    <- strptime(RefMeasExclusionPeriods$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

RefMeasExclusionPeriods$timestamp_from <- as.numeric(difftime(time1=RefMeasExclusionPeriods$Date_UTC_from,time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC")) 
RefMeasExclusionPeriods$timestamp_to   <- as.numeric(difftime(time1=RefMeasExclusionPeriods$Date_UTC_to,  time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC")) 


## ----------------------------------------------------------------------------------------------------------------------

## Read data from Database "empaGSN"


query_str       <- paste("SELECT * from laegern_1min_cal WHERE timed >= 1483228800000;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv,group=DB_group_in)
res             <- dbSendQuery(con, query_str)
data            <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

cn_required     <- c("timed","VALVEPOS","CO2_DRY","CO2_DRY_N","CO2_DRY_SE","CO2_DRY_UNCERT","CO2_DRY_TOT_UNC","CO2_DRY_ERR","CO2_DRY_FLAG","H2O","H2O_N","H2O_SE","H2O_FLAG")
data            <- data[,which(colnames(data)%in%cn_required)]


# Check valvepos (VALVEPOS must equal 0)

id <- which(!is.na(data$VALVEPOS) & data$VALVEPOS==0)

if(length(id)>0){
  data <- data[id,]
}else{
  stop()
}

# Check CO2_DRY

id <- which(!(data$CO2_DRY>200 | data$CO2_DRY < 2000) | is.na(data$CO2_DRY))

if(length(id)>0){
  data$CO2_DRY[id]         <- -999
  data$CO2_DRY_N[id]       <- -999
  data$CO2_DRY_SE[id]      <- -999
  data$CO2_DRY_UNCERT[id]  <- -999
  data$CO2_DRY_TOT_UNC[id] <- -999
  data$CO2_DRY_ERR[id]     <- -999
  data$CO2_DRY_FLAG[id]    <- -999
}

id <- which(is.na(data$CO2_DRY_N))
if(length(id)>0){
  data$CO2_DRY_N[id] <- -999
}

id <- which(is.na(data$CO2_DRY_SE))
if(length(id)>0){
  data$CO2_DRY_SE[id] <- -999
}

id <- which(is.na(data$CO2_DRY_UNCERT))
if(length(id)>0){
  data$CO2_DRY_UNCERT[id] <- -999
}

id <- which(is.na(data$CO2_DRY_TOT_UNC))
if(length(id)>0){
  data$CO2_DRY_TOT_UNC[id] <- -999
}

id <- which(is.na(data$CO2_DRY_ERR))
if(length(id)>0){
  data$CO2_DRY_ERR[id] <- -999
}

id <- which(is.na(data$CO2_DRY_FLAG))
if(length(id)>0){
  data$CO2_DRY_FLAG[id] <- -999
}

# Check H2O

id <- which(!(data$H2O>0 | data$H2O < 100) | is.na(data$H2O))

if(length(id)>0){
  data$H2O[id]      <- -999
  data$H2O_N[id]    <- -999
  data$H2O_SE[id]   <- -999
  data$H2O_FLAG[id] <- -999
}

id <- which(is.na(data$H2O_N))
if(length(id)>0){
  data$H2O_N[id] <- -999
}

id <- which(is.na(data$H2O_SE))
if(length(id)>0){
  data$H2O_SE[id] <- -999
}

id <- which(is.na(data$H2O_FLAG))
if(length(id)>0){
  data$H2O_FLAG[id] <- -999
}

#

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


## Known station problems / issues concurring with less reliable data

id_excl   <- which(RefMeasExclusionPeriods$Measurement=="CO2")
n_id_excl <- length(id_excl)

if(n_id_excl>0){
  
  for(ith_excl in 1:n_id_excl){
    
    id_id_excl <- which(data$timestamp>=RefMeasExclusionPeriods$timestamp_from[id_excl[ith_excl]] & data$timestamp<=RefMeasExclusionPeriods$timestamp_to[id_excl[ith_excl]])
    
    if(length(id_id_excl)>0){
      data$CO2[id_id_excl]   <- -999
      data$CO2_F[id_id_excl] <- 0
    }
  }
}


## Write data into Database "CarboSense_MySQL"

query_str       <- paste("DELETE from EMPA_LAEG;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv,group=DB_group_out)
res             <- dbSendQuery(con, query_str)
dbClearResult(res)
dbDisconnect(con)

#

N_dataPackets2insert <- 1e4

for(row_A in seq(1,dim(data)[1],N_dataPackets2insert)){
  
  row_B     <- min(c(row_A+N_dataPackets2insert-1,dim(data)[1])) 
  
  id_insert <- (row_A:row_B)
  
  query_str <- paste("INSERT INTO EMPA_LAEG (",paste(colnames(data)[1:15],collapse = ","),") ",sep="")
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
                                     collapse = "),(",sep=""),")",sep=""),";")
  
  
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv,group=DB_group_out)
  res             <- dbSendQuery(con, query_str)
  dbClearResult(res)
  dbDisconnect(con)
}



