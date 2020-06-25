# MCH_MEAS_TimeSeries.r
# -----------------------------------
#
# Author: Michael Mueller
#
#
# -----------------------------------


# Remarks:
# - Computations refer to UTC.
#
#

## clear variables
rm(list=ls(all=TRUE))
gc()

## libraries
library(openair)
library(DBI)
require(RMySQL)
require(chron)
library(MASS)
library(rpart)

## source

source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

### ----------------------------------------------------------------------------------------------------------------------------

resultdir <- "/project/CarboSense/Carbosense_Network/METEOSWISS/"

### ----------------------------------------------------------------------------------------------------------------------------

# MCH locations

query_str       <- paste("SELECT DISTINCT LocationName FROM METEOSWISS_Measurements;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tmp             <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

u_locations   <- tmp$LocationName
n_u_locations <- length(u_locations)


# Analysis of measurements from each site

for(ith_location in 1:n_u_locations){
  
  query_str       <- paste("SELECT * FROM METEOSWISS_Measurements WHERE LocationName='",u_locations[ith_location],"';",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  data            <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  # 
  
  data       <- data[order(data$timestamp),]
  
  data$date  <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
  
  
  for(cn in c("Pressure","Temperature")){
    
    pos <- which(colnames(data)==cn)
    
    if(all(data[,pos]==-999)){
      next
    }
    
    #
    
    id_setToNA <- which(data[,pos] == -999)
    if(length(id_setToNA)>0){
      data[id_setToNA,pos] <- NA
    }
    
    #
    
    if(cn=="Pressure"){
      figname    <- paste(resultdir,u_locations[ith_location],"_Pressure_TS.pdf",sep="")
      ylabString <- expression(paste("Pressure [hPa]"))
      legend_str <- c("MCH pressure")
    }
    if(cn=="Temperature"){
      figname    <- paste(resultdir,u_locations[ith_location],"_Temperature_TS.pdf",sep="")
      ylabString <- expression(paste("Temperature [deg C]"))
      legend_str <- c("MCH temperature")
    }
    
    yyy        <- cbind(data[,pos])
    
    xlabString <- "Date"
    plot_ts(figname,data$date,yyy,"week",NULL,NULL,xlabString,ylabString,legend_str)
    
  }
  
}