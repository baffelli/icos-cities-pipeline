# MCH_MEAS_DiurnalVariatonsPerMonth.r
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
  data$hour  <- as.numeric(strftime(data$date,"%H",tz="UTC"))
  data$month <- as.numeric(strftime(data$date,"%m",tz="UTC"))
  data$year  <- as.numeric(strftime(data$date,"%Y",tz="UTC"))
  
  #
  
  if(all(data$RH==-999) | all(data$Temperature==-999)){
    next
  }
  
  #
  
  figname <- paste(resultdir,"/",u_locations[ith_location],"_T_RH_DIURNAL_VAR.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.5,0.1),mfrow=c(1,2))
  
  
  u_m_y   <- unique(cbind(data$year,data$month))
  n_u_m_y <- dim(u_m_y)[1]
  
  for(ith_m_y in 1:n_u_m_y){
    
    tmp_T  <- matrix(NA,ncol=24,nrow=dim(data)[1])
    tmp_RH <- matrix(NA,ncol=24,nrow=dim(data)[1])
    
    make_plot_T  <- F
    make_plot_RH <- F
    
    for(i in 0:23){
      sel <- (data$year==u_m_y[ith_m_y,1] & data$month == u_m_y[ith_m_y,2] & data$hour==i)
      
      id   <- which(sel & data$RH != -999)
      n_id <- length(id)
      
      if(n_id>10){
        tmp_RH[1:n_id,i+1] <- data$RH[id]
        make_plot_RH       <- T
      }
      
      id   <- which(sel & data$Temperature != -999)
      n_id <- length(id)
      
      if(n_id>10){
        tmp_T[1:n_id,i+1] <- data$Temperature[id]
        make_plot_T       <- T
      }
    }
    
    
    if(!make_plot_T & !make_plot_RH){
      next
    }
    
    if(make_plot_T){
      
      mainStr <- paste(u_locations[ith_location]," ",sprintf("%02.0f",u_m_y[ith_m_y,1]),"/",sprintf("%02.0f",u_m_y[ith_m_y,2]),sep="")
      
      boxplot(tmp_T, ylim = c(-15,40), outline = T, cex=0.5, pch=16 ,col="slategray",xlab="Hour of day",ylab="T [deg C]", main=mainStr,cex.lab=1.25,cex.main=1.25,cex.axis=1.25)
    }else(
      plot.new()
    )
    
    if(make_plot_RH){
      
      mainStr <- paste(u_locations[ith_location]," ",sprintf("%02.0f",u_m_y[ith_m_y,1]),"/",sprintf("%02.0f",u_m_y[ith_m_y,2]),sep="")
      
      boxplot(tmp_RH, ylim = c(0,100),outline = T, cex=0.5, pch=16 ,col="slategray",xlab="Hour of day",ylab="RH [%]", main=mainStr,cex.lab=1.25,cex.main=1.25,cex.axis=1.25)
    }else(
      plot.new()
    )
  }
  
  dev.off()
  par(def_par)
  
}