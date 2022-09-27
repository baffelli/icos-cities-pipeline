# CO2_ZH_RPART.r
# --------------------------------------------------
#
# Author: Michael Mueller
#
# --------------------------------------------------
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
library(raster)
library(mgcv)
library(fields)
library(randomForest)

source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

### ----------------------------------------------------------------------------------------------------------------------------

args = commandArgs(trailingOnly=TRUE)

selected_version <- args[1]

if(!selected_version%in%c(10,11,20,21,30)){
  stop()
}

### ----------------------------------------------------------------------------------------------------------------------------

## Files and directories

resultdir <- "/project/CarboSense/Statistical_Models/CO2_MAPPING/"
grid_fn   <- "/project/CarboSense/Carbosense_Network/SpatialSiteClassification/SpatialSiteClassification_GRID.csv"
DTV1_fn   <- "/project/CarboSense/Statistical_Models/Data/Geodata/ZH_traffic_DTV1_sum_expD_000030m_r2000m_r_10x10.tif"
DTV2_fn   <- "/project/CarboSense/Statistical_Models/Data/Geodata/ZH_traffic_DTV2_sum_expD_000030m_r2000m_r_10x10.tif"

## Control parameters

if(selected_version==10){
  approach      <- "RPART"
  model_name    <- "RPART_V1"
  rpart_control <- rpart.control(minsplit = 6,minbucket = 3,cp = 0.01)
}
if(selected_version==11){
  approach      <- "RPART"
  model_name    <- "RPART_V2"
  rpart_control <- rpart.control(minsplit = 4,minbucket = 2,cp = 0.01)
}

if(selected_version==20){
  approach   <- "RF"
  model_name <- "RF_V1"
  nodesize   <- 3
  ntree      <- 30
}
if(selected_version==21){
  approach   <- "RF"
  model_name <- "RF_V2"
  nodesize   <- 2
  ntree      <- 30
}

if(selected_version==30){
  approach   <- "GAM"
  model_name <- "GAM"
}


DO_MAP_PLOT    <- F
DO_GRID_EXPORT <- T

# 


##

if(!dir.exists(paste(resultdir,model_name,sep=""))){
  dir.create(paste(resultdir,model_name,sep=""))
}

if(!dir.exists(paste(resultdir,model_name,"/DATA",sep=""))){
  dir.create(paste(resultdir,model_name,"/DATA",sep=""))
}

if(!dir.exists(paste(resultdir,model_name,"/MODEL_PICS",sep=""))){
  dir.create(paste(resultdir,model_name,"/MODEL_PICS",sep=""))
}

if(!dir.exists(paste(resultdir,model_name,"/MOVIE_PICS",sep=""))){
  dir.create(paste(resultdir,model_name,"/MOVIE_PICS",sep=""))
}

if(!dir.exists(paste(resultdir,model_name,"/MODEL_STATS",sep=""))){
  dir.create(paste(resultdir,model_name,"/MODEL_STATS",sep=""))
}


### ----------------------------------------------------------------------------------------------------------------------------

## Specification of time period

Date_UTC_from  <- strptime("20190101000000","%Y%m%d%H%M%S",tz="UTC")
Date_UTC_to    <- strptime("20200426000000","%Y%m%d%H%M%S",tz="UTC")

timestamp_from <- as.numeric(difftime(time1=Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
timestamp_to   <- as.numeric(difftime(time1=Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

### ----------------------------------------------------------------------------------------------------------------------------

# Table Deployment

query_str       <- paste("SELECT * FROM Deployment;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_deployment  <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_deployment$Date_UTC_from  <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to    <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

tbl_deployment$timestamp_from <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
tbl_deployment$timestamp_to   <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))


# Table Location

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_location    <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

ZH_locations_LP8 <- tbl_location$LocationName[which((tbl_location$X_LV03-248000)^2 + (tbl_location$Y_LV03-683200)^2 < 20000^2 & !tbl_location$LocationName%in%c('DUE1','DUE2','DUE3','DUE4','DUE5','MET1'))]
ZH_locations_LP8 <- ZH_locations_LP8[!ZH_locations_LP8%in%c("UTLI")]


# LP8 measurements

query_str       <- paste("SELECT timestamp, LocationName, SensorUnit_ID, CO2_A, O_FLAG FROM CarboSense_CO2_TEST01 ",sep="")
query_str       <- paste(query_str, "WHERE timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to," AND CO2_A != -999 AND O_FLAG = 1 ",sep="")
query_str       <- paste(query_str, "AND LocationName IN ('",paste(ZH_locations_LP8,collapse="','",sep=""),"');",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_CO2         <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

colnames(tbl_CO2)[which(colnames(tbl_CO2)=="CO2_A")] <- "CO2"

tbl_CO2$weight   <- 1

if(T){
  
  u_SU_LOC <- unique(data.frame(LocationName = tbl_CO2$LocationName,
                                SensorUnit_ID= tbl_CO2$SensorUnit_ID,
                                stringsAsFactors = F))
  
  n_u_SU_LOC      <- dim(u_SU_LOC)[1]
  
  tbl_CO2_tmp <- tbl_CO2
  tbl_CO2     <- NULL
  
  for(ith_u_SU_LOC in 1:n_u_SU_LOC){
    
    id <- which(tbl_CO2_tmp$LocationName    == u_SU_LOC$LocationName[ith_u_SU_LOC]
                & tbl_CO2_tmp$SensorUnit_ID == u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC])
    
    tmp <- data.frame(date = strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_CO2_tmp$timestamp[id],
                      CO2  = tbl_CO2_tmp$CO2[id],
                      stringsAsFactors = F)
    
    tmp <- tmp[order(tmp$date),]
    
    tmp <- timeAverage(mydata = tmp,avg.time = "1 hour",statistic = "median",
                       start.date = strptime(strftime(min(tmp$date)-3600,"%Y%m%d%H0000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
    
    tmp <- tmp[which(!is.na(tmp$CO2)),]
    
    tbl_CO2 <- rbind(tbl_CO2,data.frame(timestamp     = as.numeric(difftime(time1=tmp$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                        LocationName  = rep(u_SU_LOC$LocationName[ith_u_SU_LOC],dim(tmp)[1]),
                                        SensorUnit_ID = rep(u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC],dim(tmp)[1]),
                                        CO2           = tmp$CO2,
                                        O_FLAG        = rep(1,dim(tmp)[1]),
                                        weight        = rep(1,dim(tmp)[1]),
                                        stringsAsFactors = F))
  }
  
  rm(u_SU_LOC,n_u_SU_LOC,tbl_CO2_tmp,tmp,id)
  gc()
  
}


# HPP measurements

query_str       <- paste("SELECT timestamp, LocationName, SensorUnit_ID, CO2_CAL_ADJ as CO2 FROM CarboSense_HPP_CO2 ",sep="")
query_str       <- paste(query_str,"WHERE timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to," AND Valve = 0 ",sep="")
query_str       <- paste(query_str,"AND LocationName IN ('ZHBR','ZSCH','ZUE','ALBS','RECK','ESMO');",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_CO2_HPP     <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

if(T){
  
  u_SU_LOC <- unique(data.frame(LocationName = tbl_CO2_HPP$LocationName,
                                SensorUnit_ID= tbl_CO2_HPP$SensorUnit_ID,
                                stringsAsFactors = F))
  
  n_u_SU_LOC      <- dim(u_SU_LOC)[1]
  
  tbl_CO2_HPP_tmp <- tbl_CO2_HPP
  tbl_CO2_HPP     <- NULL
  
  for(ith_u_SU_LOC in 1:n_u_SU_LOC){
    
    id <- which(tbl_CO2_HPP_tmp$LocationName    == u_SU_LOC$LocationName[ith_u_SU_LOC]
                & tbl_CO2_HPP_tmp$SensorUnit_ID == u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC])
    
    tmp <- data.frame(date = strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_CO2_HPP_tmp$timestamp[id],
                      CO2  = tbl_CO2_HPP_tmp$CO2[id],
                      stringsAsFactors = F)
    
    tmp <- tmp[order(tmp$date),]
    
    tmp <- timeAverage(mydata = tmp,avg.time = "1 hour",statistic = "median",
                       start.date = strptime(strftime(min(tmp$date)-3600,"%Y%m%d%H0000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
    
    tmp <- tmp[which(!is.na(tmp$CO2)),]
    
    tbl_CO2_HPP <- rbind(tbl_CO2_HPP,data.frame(timestamp     = as.numeric(difftime(time1=tmp$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                                LocationName  = rep(u_SU_LOC$LocationName[ith_u_SU_LOC],dim(tmp)[1]),
                                                SensorUnit_ID = rep(u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC],dim(tmp)[1]),
                                                CO2           = tmp$CO2,
                                                stringsAsFactors = F))
  }
  
  rm(u_SU_LOC,n_u_SU_LOC,tbl_CO2_HPP_tmp,tmp,id)
  gc()
  
}


# Picarro measurements DUE

query_str       <- paste("SELECT timestamp, CO2_WET_COMP FROM NABEL_DUE ",sep="")
query_str       <- paste(query_str,"WHERE timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to," and CO2_WET_COMP != -999;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_CO2_DUE     <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

colnames(tbl_CO2_DUE)[which(colnames(data)=="CO2_WET_COMP")] <- "CO2"

if(T){
  
  tmp <- data.frame(date = strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_CO2_DUE$timestamp,
                    CO2  = tbl_CO2_DUE$CO2,
                    stringsAsFactors = F)
  
  tmp <- tmp[order(tmp$date),]
  
  tmp <- timeAverage(mydata = tmp,avg.time = "1 hour",statistic = "median",
                     start.date = strptime(strftime(min(tmp$date)-3600,"%Y%m%d%H0000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
  
  tmp <- as.data.frame(tmp)
  
  tmp <- tmp[which(!is.na(tmp$CO2)),]
  
  tbl_CO2_DUE <- data.frame(timestamp  = as.numeric(difftime(time1=tmp$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                            CO2        = tmp$CO2,
                            stringsAsFactors = F)
  
  rm(tmp)
  gc()
}


# Picarro measurements LAEG

query_str       <- paste("SELECT timestamp, CO2 FROM EMPA_LAEG WHERE timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to," and CO2 != -999;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_CO2_LAEG    <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)


if(T){
  tmp <- data.frame(date = strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_CO2_LAEG$timestamp,
                    CO2  = tbl_CO2_LAEG$CO2,
                    stringsAsFactors = F)
  
  tmp <- tmp[order(tmp$date),]
  
  tmp <- timeAverage(mydata = tmp,avg.time = "1 hour",statistic = "mean",
                     start.date = strptime(strftime(min(tmp$date)-3600,"%Y%m%d%H0000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
  
  tmp <- tmp[which(!is.na(tmp$CO2)),]
  
  tbl_CO2_LAEG <- data.frame(timestamp  = as.numeric(difftime(time1=tmp$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                             CO2        = tmp$CO2,
                             stringsAsFactors = F)
  
  rm(tmp)
  gc()
}

#

if(dim(tbl_CO2_HPP)[1]>0){
  tbl_CO2 <- rbind(tbl_CO2,data.frame(timestamp    = tbl_CO2_HPP$timestamp,
                                      LocationName = tbl_CO2_HPP$LocationName,
                                      SensorUnit_ID= tbl_CO2_HPP$SensorUnit_ID,
                                      CO2          = tbl_CO2_HPP$CO2,
                                      O_FLAG       = rep(1,dim(tbl_CO2_HPP)[1]),
                                      weight       = 1,
                                      stringsAsFactors = F))
}

if(dim(tbl_CO2_DUE)[1]>0){
  tbl_CO2 <- rbind(tbl_CO2,data.frame(timestamp    = tbl_CO2_DUE$timestamp,
                                      LocationName = rep("DUE1",dim(tbl_CO2_DUE)[1]),
                                      SensorUnit_ID= rep(999,dim(tbl_CO2_DUE)[1]),
                                      CO2          = tbl_CO2_DUE$CO2,
                                      O_FLAG       = rep(1,dim(tbl_CO2_DUE)[1]),
                                      weight       = 1,
                                      stringsAsFactors = F))
}

if(dim(tbl_CO2_LAEG)[1]>0){
  tbl_CO2 <- rbind(tbl_CO2,data.frame(timestamp    = tbl_CO2_LAEG$timestamp,
                                      LocationName = rep("LAEG",dim(tbl_CO2_LAEG)[1]),
                                      SensorUnit_ID= rep(999,dim(tbl_CO2_LAEG)[1]),
                                      CO2          = tbl_CO2_LAEG$CO2,
                                      O_FLAG       = rep(1,dim(tbl_CO2_LAEG)[1]),
                                      weight       = 1,
                                      stringsAsFactors = F))
}

rm(tbl_CO2_DUE,tbl_CO2_LAEG,tbl_CO2_HPP)
gc()

#

tbl_CO2$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_CO2$timestamp

### ----------------------------------------------------------------------------------------------------------------------------

## Add coordinates and heights to CO2 observation data

id_depl   <- which(!(tbl_deployment$timestamp_to <= timestamp_from 
                     | tbl_deployment$timestamp_from >= timestamp_to 
                     | tbl_deployment$LocationName%in%c("DUE1","DUE2","DUE3","DUE4","DUE5","MET1")))


n_id_depl <- length(id_depl)

##

tbl_CO2$X_LV03 <- NA
tbl_CO2$Y_LV03 <- NA
tbl_CO2$h      <- NA
tbl_CO2$hdepl  <- NA

for(ith_depl in c(-2,-1,1:n_id_depl)){
  
  if(ith_depl < 0){
    if(ith_depl == -1){
      LocationNameRef <- "DUE1"
    }
    if(ith_depl == -2){
      LocationNameRef <- "LAEG"
    }
    
    id_loc <- which(tbl_location$LocationName  == LocationNameRef)
    
    id_CO2 <- which(tbl_CO2$LocationName    == LocationNameRef
                    & tbl_CO2$SensorUnit_ID == 999)
    
    HeightAboveGround <- 0
    
  }else{
    
    id_loc <- which(tbl_location$LocationName  == tbl_deployment$LocationName[id_depl[ith_depl]])
    
    id_CO2 <- which(tbl_CO2$LocationName    == tbl_deployment$LocationName[id_depl[ith_depl]]
                    & tbl_CO2$SensorUnit_ID == tbl_deployment$SensorUnit_ID[id_depl[ith_depl]]
                    & tbl_CO2$timestamp     >= tbl_deployment$timestamp_from[id_depl[ith_depl]]
                    & tbl_CO2$timestamp     <= tbl_deployment$timestamp_to[id_depl[ith_depl]])
    
    HeightAboveGround <- tbl_deployment$HeightAboveGround[id_depl[ith_depl]]
  }
  
  if(length(id_CO2)>0){
    tbl_CO2$X_LV03[id_CO2] <- tbl_location$X_LV03[id_loc]
    tbl_CO2$Y_LV03[id_CO2] <- tbl_location$Y_LV03[id_loc]
    tbl_CO2$h[id_CO2]      <- tbl_location$h[id_loc]
    tbl_CO2$hdepl[id_CO2]  <- tbl_location$h[id_loc] + HeightAboveGround
  }
}

tbl_CO2          <- tbl_CO2[which(!is.na(tbl_CO2$X_LV03) & !is.na(tbl_CO2$Y_LV03)),]


### ----------------------------------------------------------------------------------------------------------------------------

# Import traffic intensities

TrafficInt_1 <- raster(DTV1_fn)
TrafficInt_2 <- raster(DTV2_fn)
TrafficInt   <- TrafficInt_1 + 5*TrafficInt_2

### ----------------------------------------------------------------------------------------------------------------------------

## Import SpatialSiteClassification_GRID

grid <- read.table(grid_fn,header=T,sep=";")

grid_max_x <- max(grid$X_LV03)
grid_min_x <- min(grid$X_LV03)
grid_max_y <- max(grid$Y_LV03)
grid_min_y <- min(grid$Y_LV03)

grid$rel_h_1000 <- (grid$h-grid$TOPO_MIN_01000)
grid$rel_h_2500 <- (grid$h-grid$TOPO_MIN_02500)

cn_grid_classifier <- colnames(grid)

### ----------------------------------------------------------------------------------------------------------------------------

## SPATIAL ATTRIBUTES


cn_model_func       <- c("LCOV_FRAC_SEALED_AREAS_01000",
                         "LCOV_FRAC_SEALED_AREAS_02000",
                         "LCOV_FRAC_BUILDINGS_01000",
                         "LCOV_FRAC_BUILDINGS_02000",
                         "LCOV_FRAC_FOREST_02000",
                         "LCOV_FRAC_WATER_02000",
                         "rel_h_1000",
                         "rel_h_2500",
                         "DTV",
                         "DTV1",
                         "DTV2",
                         "h")



cn_model_func_add   <- cn_model_func[!cn_model_func%in%colnames(tbl_CO2)]
cn_model_func_add   <- cn_model_func_add[!cn_model_func_add%in%c("DTV","DTV1","DTV2")]
n_cn_model_func_add <- length(cn_model_func_add)


if(n_cn_model_func_add>0){
  
  #
  
  for(i in 1:n_cn_model_func_add){
    tbl_CO2$NCol      <- NA
    colnames(tbl_CO2) <- c(colnames(tbl_CO2)[1:(dim(tbl_CO2)[2]-1)],cn_model_func_add[i])
  }
  
  
  #
  
  u_SU_LOC   <- unique(data.frame(SensorUnit_ID = tbl_CO2$SensorUnit_ID, 
                                  LocationName  = tbl_CO2$LocationName,
                                  Y_LV03        = tbl_CO2$Y_LV03,
                                  X_LV03        = tbl_CO2$X_LV03,
                                  stringsAsFactors = F))
  n_u_SU_LOC <- dim(u_SU_LOC)[1]
  
  
  
  for(ith_u_SU_LOC in 1:n_u_SU_LOC){
    
    id_CO2_SU_LOC <- which(tbl_CO2$SensorUnit_ID   == u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC]
                           & tbl_CO2$LocationName  == u_SU_LOC$LocationName[ith_u_SU_LOC]
                           & tbl_CO2$Y_LV03        == u_SU_LOC$Y_LV03[ith_u_SU_LOC]
                           & tbl_CO2$X_LV03        == u_SU_LOC$X_LV03[ith_u_SU_LOC])
    
    dist          <- sqrt((grid$Y_LV03-u_SU_LOC$Y_LV03[ith_u_SU_LOC])^2+(grid$X_LV03-u_SU_LOC$X_LV03[ith_u_SU_LOC])^2)
    id_grid       <- which(dist==min(dist))[1]
    
    for(i in 1:n_cn_model_func_add){
      pos_grid <- which(colnames(grid)   ==cn_model_func_add[i])
      pos_co2  <- which(colnames(tbl_CO2)==cn_model_func_add[i])
      
      tbl_CO2[id_CO2_SU_LOC,pos_co2] <- rep(grid[id_grid,pos_grid],length(id_CO2_SU_LOC))
    }
  }
}

##

tbl_CO2$DTV  <- 0
tbl_CO2$DTV1 <- 0
tbl_CO2$DTV2 <- 0
cell         <- cellFromXY(TrafficInt,xy = matrix(c(tbl_CO2$Y_LV03,tbl_CO2$X_LV03),ncol=2))
id_cell_ok   <- which(!is.na(cell))

if(length(id_cell_ok)>0){
  tbl_CO2$DTV[id_cell_ok]  <- as.numeric(TrafficInt[cell[id_cell_ok]])
  tbl_CO2$DTV1[id_cell_ok] <- as.numeric(TrafficInt_1[cell[id_cell_ok]])
  tbl_CO2$DTV2[id_cell_ok] <- as.numeric(TrafficInt_2[cell[id_cell_ok]])
}

rm(cell,id_cell_ok)
gc()

### ----------------------------------------------------------------------------------------------------------------------------

## Prediction grid (corresponds to dimensions of traffic input data) 

pred_grid_export_xmin <- 676200 
pred_grid_export_xmax <- 689700 
pred_grid_export_ymin <- 241560 
pred_grid_export_ymax <- 254320
pred_grid_export_res  <- 20

pred_grid_export_nx   <- (pred_grid_export_xmax - pred_grid_export_xmin)/pred_grid_export_res + 1
pred_grid_export_ny   <- (pred_grid_export_ymax - pred_grid_export_ymin)/pred_grid_export_res + 1

pred_grid_export      <- data.frame(Y_LV03 = rep(seq(pred_grid_export_xmin,pred_grid_export_xmax,pred_grid_export_res),pred_grid_export_ny),
                                    X_LV03 = rep(seq(pred_grid_export_ymin,pred_grid_export_ymax,pred_grid_export_res),each=pred_grid_export_nx),
                                    stringsAsFactors = F)

##

tmp_Y_LV03  <- (pred_grid_export$Y_LV03%/%100)*100
tmp_X_LV03  <- (pred_grid_export$X_LV03%/%100)*100
id_A_B      <- rep(0,dim(pred_grid_export)[1])

for(ith_cell in 1:dim(pred_grid_export)[1]){
  
  if(id_A_B[ith_cell]!=0){
    next
  }
  
  id_range <- ith_cell + 0:(pred_grid_export_nx*4)
  id_range <- id_range[which(id_range<=dim(pred_grid_export)[1])]
  
  id_1 <- which(grid$Y_LV03          == tmp_Y_LV03[ith_cell] & grid$X_LV03          == tmp_X_LV03[ith_cell])
  id_2 <- which(tmp_Y_LV03[id_range] == tmp_Y_LV03[ith_cell] & tmp_X_LV03[id_range] == tmp_X_LV03[ith_cell])
  
  id_A_B[id_range[id_2]] <- rep(id_1,length(id_2))
}

for(ith_cn_model_func in 1:length(cn_model_func)){
  
  pred_grid_export$NCOL <- grid[id_A_B,which(colnames(grid)==cn_model_func[ith_cn_model_func])]
  
  colnames(pred_grid_export)[which(colnames(pred_grid_export)=="NCOL")] <- cn_model_func[ith_cn_model_func]
  
}

if(any(cn_model_func%in%c("DTV","DTV1","DTV2"))){
  id_A_B <- cellFromXY(object = TrafficInt, xy = data.frame(x=pred_grid_export$Y_LV03,
                                                            y=pred_grid_export$X_LV03,
                                                            stringsAsFactors = F))
  
  pred_grid_export$DTV  <- TrafficInt[id_A_B]
  pred_grid_export$DTV1 <- TrafficInt_1[id_A_B]
  pred_grid_export$DTV2 <- TrafficInt_2[id_A_B]
}

rm(pred_grid_export_nx,pred_grid_export_ny,pred_grid_export_xmin,pred_grid_export_xmax,pred_grid_export_ymin,pred_grid_export_ymax,pred_grid_export_res)
rm(tmp_Y_LV03,tmp_X_LV03,id_A_B,id_range,id_1,id_2,id_A_B)
rm(TrafficInt_1,TrafficInt_2,TrafficInt)
gc()


### ----------------------------------------------------------------------------------------------------------------------------

## Modelling

modelling_period_length <- 3600

tbl_CO2$CO2_LOG      <- log(tbl_CO2$CO2)
tbl_CO2$CO2_model    <- NA
tbl_CO2$CO2_pred     <- NA
tbl_CO2$CO2_pred_LOO <- NA


for(timestamp_now in seq(timestamp_from,timestamp_to,modelling_period_length)){
  
  date_now      <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + timestamp_now
  date_now_hour <- as.numeric(strftime(date_now,"%H",tz="UTC"))
  date_str      <- strftime(date_now, "%Y%m%d_%H%M%S",tz="UTC")
  
  print(paste(date_now," -- ST ",Sys.time()))
  
  
  # Data selection
  
  id_tp    <- which(tbl_CO2$timestamp>=timestamp_now & tbl_CO2$timestamp<(timestamp_now+modelling_period_length))
  n_id_tp  <- length(id_tp)
  
  if(n_id_tp==0){
    next
  }
  
  id_CO2   <- id_tp[which(tbl_CO2$CO2[id_tp]>=350 & tbl_CO2$CO2[id_tp]<=800 & tbl_CO2$O_FLAG[id_tp]==1)]
  n_id_CO2 <- length(id_CO2)
  
  if(n_id_tp != n_id_CO2){
    print(paste("n_id_tp",n_id_tp,"n_id_CO2",n_id_CO2))
  }
  
  #
  
  u_SU_LOC <- unique(data.frame(LocationName  = tbl_CO2$LocationName[id_CO2],
                                SensorUnit_ID = tbl_CO2$SensorUnit_ID[id_CO2],
                                Y_LV03        = tbl_CO2$Y_LV03[id_CO2],
                                X_LV03        = tbl_CO2$X_LV03[id_CO2],
                                stringsAsFactors = F))
  n_u_SU_LOC <- dim(u_SU_LOC)[1]
  
  #
  
  if(approach=="GAM"){
    if(length(id_CO2)==0 | n_u_SU_LOC<13){
      next
    }
  }
  
  if(approach=="RPART"){
    if(length(id_CO2)==0 | n_u_SU_LOC<2){
      next
    }
  }
  
  
  if(approach=="GAM"){
    
    formula                   <- as.formula(paste("CO2_LOG~",paste(paste("s(",cn_model_func,")",sep=""),collapse = "+"),sep=""))
    
    gam_obj                   <- gam(formula,data=tbl_CO2[id_CO2,],weights = tbl_CO2$weight[id_CO2])
    
    tbl_CO2$CO2_model[id_CO2] <- exp(predict(object=gam_obj,newdata = tbl_CO2[id_CO2,]))
    
    tbl_CO2$CO2_pred[id_tp]   <- exp(predict(object=gam_obj,newdata = tbl_CO2[id_tp,]))
    
    pred_grid_export$CO2      <- exp(predict(object=gam_obj,newdata = pred_grid_export))
    
  }
  
  if(approach=="RPART"){
    
    formula                   <- as.formula(paste("CO2~",paste(cn_model_func,collapse = "+"),sep=""))
    
    rpart_obj                 <- rpart(formula = formula,data = tbl_CO2[id_CO2,],control = rpart_control,weights = tbl_CO2$weight[id_CO2])
    
    tbl_CO2$CO2_model[id_CO2] <- predict(object=rpart_obj,newdata = tbl_CO2[id_CO2,])
    
    tbl_CO2$CO2_pred[id_tp]   <- predict(object=rpart_obj,newdata = tbl_CO2[id_tp,])
    
    pred_grid_export$CO2      <- predict(object=rpart_obj,newdata = pred_grid_export)
  }
  
  if(approach=="RF"){
    
    formula                   <- as.formula(paste("CO2~",paste(cn_model_func,collapse = "+"),sep=""))
    
    rf_obj                    <- randomForest(formula = formula,data=tbl_CO2[id_CO2,],
                                              ntree=ntree,nodesize=nodesize, 
                                              weights = tbl_CO2$weight[id_CO2])
    
    tbl_CO2$CO2_model[id_CO2] <- predict(object=rf_obj,newdata = tbl_CO2[id_CO2,])
    
    tbl_CO2$CO2_pred[id_tp]   <- predict(object=rf_obj,newdata = tbl_CO2[id_tp,])
    
    pred_grid_export$CO2      <- predict(object=rf_obj,newdata = pred_grid_export)
  }
  
  
  #
  
  if(DO_GRID_EXPORT){
    
    grid_export_fn  <- paste(resultdir,model_name,"/DATA/","PRED_GRID_",date_str,".csv",sep="")
    
    write.table(x = data.frame(Y_LV03 = pred_grid_export$Y_LV03,
                               X_LV03 = pred_grid_export$X_LV03,
                               CO2    = pred_grid_export$CO2,
                               stringsAsFactors = F),
                file = grid_export_fn,sep = ";",row.names = F,col.names = T,quote = F)
    
    meas_export_fn  <- paste(resultdir,model_name,"/DATA/","MEAS_",date_str,".csv",sep="")
    
    write.table(x = data.frame(LocationName  = tbl_CO2$LocationName[id_CO2],
                               SensorUnit_ID = tbl_CO2$SensorUnit_ID[id_CO2],
                               timestamp     = tbl_CO2$timestamp[id_CO2],
                               Y_LV03        = tbl_CO2$Y_LV03[id_CO2],
                               X_LV03        = tbl_CO2$X_LV03[id_CO2],
                               CO2           = tbl_CO2$CO2[id_CO2],
                               stringsAsFactors = F),
                file = meas_export_fn,sep = ";",row.names = F,col.names = T,quote = F)
    
  }
  
  
  
  
  # L-O-O cross-validation
  
  for(ith_SU_LOC in 1:n_u_SU_LOC){
    
    id_CO2_LOO_TRAINING <- id_CO2[which(tbl_CO2$LocationName[id_CO2] != u_SU_LOC$LocationName[ith_SU_LOC])]
    
    if(u_SU_LOC$LocationName[ith_SU_LOC] %in% c("REH","RECK")){
      id_CO2_LOO_TRAINING <- id_CO2[which(!tbl_CO2$LocationName[id_CO2]%in%c("REH","RECK"))]
    }
    
    id_CO2_LOO_TEST <- id_CO2[which(tbl_CO2$LocationName[id_CO2]    == u_SU_LOC$LocationName[ith_SU_LOC]
                                    & tbl_CO2$SensorUnit_ID[id_CO2] == u_SU_LOC$SensorUnit_ID[ith_SU_LOC])]
    
    
    if(approach=="GAM"){
      gam_obj_LOO <- gam(formula,data=tbl_CO2[id_CO2_LOO_TRAINING,],  weights = tbl_CO2$weight[id_CO2_LOO_TRAINING])
      
      tbl_CO2$CO2_pred_LOO[id_CO2_LOO_TEST] <- exp(predict(object=gam_obj_LOO,newdata = tbl_CO2[id_CO2_LOO_TEST,]))
    }
    
    
    if(approach=="RPART"){
      
      rpart_obj_LOO <- rpart(formula = formula,data = tbl_CO2[id_CO2_LOO_TRAINING,],control = rpart_control)
      
      tbl_CO2$CO2_pred_LOO[id_CO2_LOO_TEST] <- predict(object=rpart_obj_LOO,newdata = tbl_CO2[id_CO2_LOO_TEST,])
    }
    
    if(approach=="RF"){
      
      rf_obj   <- randomForest(formula = formula,data=tbl_CO2[id_CO2_LOO_TRAINING,],ntree=ntree,nodesize=nodesize)
      
      tbl_CO2$CO2_pred_LOO[id_CO2_LOO_TEST] <- predict(object=rf_obj,newdata = tbl_CO2[id_CO2_LOO_TEST,])
    }
  }
  
  if(any(is.na(tbl_CO2$CO2_pred_LOO[id_CO2]) & !is.na(tbl_CO2$CO2_pred[id_CO2]))){
    print("FF")
    stop()
  }
  
  ###
  
  if(DO_MAP_PLOT){
    
    #
    
    figname  <- paste(resultdir,model_name,"/MODEL_PICS/",date_str,".jpg",sep="")
    
    #
    
    def_par <- par()
    jpeg(file = figname, width=960, height=960, pointsize=12)
    par(mai=c(1,1,0.5,0.5))
    
    #
    
    id_CO2_tooHigh <- which(pred_grid_export$CO2>650)
    if(length(id_CO2_tooHigh)>0){
      pred_grid_export$CO2[id_CO2_tooHigh] <- 650
    }
    id_CO2_tooLow <- which(pred_grid_export$CO2<370)
    if(length(id_CO2_tooLow)>0){
      pred_grid_export$CO2[id_CO2_tooLow] <- 370
    }
    
    # x <- round((x-370)/(650-370)*255)+1
    
    u_pred_grid_yy   <- sort(unique(pred_grid_export$Y_LV03))
    u_pred_grid_xx   <- rev(sort(unique(pred_grid_export$X_LV03)))
    
    n_u_pred_grid_yy <- length(u_pred_grid_yy)
    n_u_pred_grid_xx <- length(u_pred_grid_xx)
    
    GRID_pred_grid   <- matrix(NA,ncol=n_u_pred_grid_yy,nrow=n_u_pred_grid_xx)
    
    cc <- -1
    for(ith_row in 1:n_u_pred_grid_xx){
      cc <- cc+1
      GRID_pred_grid[ith_row,1:n_u_pred_grid_yy] <- pred_grid_export$CO2[cc*n_u_pred_grid_yy+(1:n_u_pred_grid_yy)]
    }
    
    #
    
    u_SU_LOC$CO2_mean <- NA
    u_SU_LOC$xx       <- NA
    
    for(ith_SU_LOC in 1:dim(u_SU_LOC)[1]){
      id_meas <- which(  tbl_CO2$LocationName[id_CO2]  == u_SU_LOC$LocationName[ith_SU_LOC]
                         & tbl_CO2$SensorUnit_ID[id_CO2] == u_SU_LOC$SensorUnit_ID[ith_SU_LOC])
      
      u_SU_LOC$CO2_mean[ith_SU_LOC] <- mean(tbl_CO2$CO2[id_CO2[id_meas]])
      
    }
    
    id_CO2_tooHigh <- which(u_SU_LOC$CO2_mean>650)
    if(length(id_CO2_tooHigh)>0){
      u_SU_LOC$CO2_mean[id_CO2_tooHigh] <- 650
    }
    id_CO2_tooLow <- which(u_SU_LOC$CO2_mean<370)
    if(length(id_CO2_tooLow)>0){
      u_SU_LOC$CO2_mean[id_CO2_tooLow] <- 370
    }
    
    xx <- round((u_SU_LOC$CO2_mean-370)/(650-370)*255)+1
    
    #
    
    image.plot(u_pred_grid_yy,rev(u_pred_grid_xx),t(GRID_pred_grid),col=rev(heat.colors(256)),zlim=c(370,650),xlab="Easting [m]", ylab="Norhting [m]",main="",cex.lab=1.5,cex.axis=1.5,cex.main=1.5,legend.args=list(text="CO2 [ppm]",col=1, cex=1.0, side=1, line=2))
    
    if(any(u_SU_LOC$SensorUnit_ID%in%c(1010:1334))){
      id <- which(u_SU_LOC$SensorUnit_ID%in%c(1010:1334))
      points(u_SU_LOC$Y_LV03[id],u_SU_LOC$X_LV03[id],col=rep(1,length(xx[id])),bg=rev(heat.colors(256))[xx[id]],pch=21,cex=3,lwd=2)
    }
    
    if(any(u_SU_LOC$SensorUnit_ID%in%c(999,426:445))){
      id <- which(u_SU_LOC$SensorUnit_ID%in%c(999,426:445))
      points(u_SU_LOC$Y_LV03[id],u_SU_LOC$X_LV03[id],col=rep(1,length(xx[id])),bg=rev(heat.colors(256))[xx[id]],pch=22,cex=5,lwd=2)
    }
    
    #
    
    dev.off()
    par(def_par)
    
    #
    
    rm(x,xx,id_CO2_tooHigh,id_CO2_tooLow,x,u_grid_yy,u_grid_xx,GRID_grid_pred)
    gc()
  }
  
  ###
}


### ----------------------------------------------------------------------------------------------------------------------------

# 
# tbl_CO2_tmp <- NULL
# 
# u_SU_LOC <- unique(data.frame(LocationName  = tbl_CO2$LocationName,
#                               SensorUnit_ID = tbl_CO2$SensorUnit_ID,
#                               stringsAsFactors = F))
# 
# for(ith_u_SU_LOC in 1:dim(u_SU_LOC)[1]){
#   id_ok <- which(  tbl_CO2$LocationName    == u_SU_LOC$LocationName[ith_u_SU_LOC]
#                    & tbl_CO2$SensorUnit_ID == u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC]
#                    & tbl_CO2$O_FLAG==1)
#   
#   tmp <- timeAverage(mydata = tbl_CO2[id_ok,],avg.time = "hour",statistic = "mean",start.date = Date_UTC_from)
#   tmp$LocationName  <- rep(u_SU_LOC$LocationName[ith_u_SU_LOC], dim(tmp)[1])
#   tmp$SensorUnit_ID <- rep(u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC],dim(tmp)[1])
#   
#   id_ok <- which(!is.na(tmp$CO2))
#   
#   if(length(id_ok)>0){
#     tbl_CO2_tmp <- rbind(tbl_CO2_tmp,tmp[id_ok,])
#   }
# }
# 
# tbl_CO2 <- tbl_CO2_tmp
# 
# rm(id_ok,tbl_CO2_tmp,tmp,u_SU_LOC)
# gc()


###


statistics <- NULL

u_SU_LOC    <- unique(data.frame(LocationName  = tbl_CO2$LocationName,
                                 SensorUnit_ID = tbl_CO2$SensorUnit_ID,
                                 stringsAsFactors = F))
n_u_SU_LOC  <- dim(u_SU_LOC)[1]


for(ith_SU_LOC in 0:n_u_SU_LOC){
  
  if(ith_SU_LOC==0){
    id <- 1:dim(tbl_CO2)[1]
    
    u_SU_LOC_LocationName  <- "ALL"
    u_SU_LOC_SensorUnit_ID <- 0
  }else{
    id <- which(tbl_CO2$LocationName    == u_SU_LOC$LocationName[ith_SU_LOC] 
                & tbl_CO2$SensorUnit_ID == u_SU_LOC$SensorUnit_ID[ith_SU_LOC])
    
    u_SU_LOC_LocationName  <- u_SU_LOC$LocationName[ith_SU_LOC]
    u_SU_LOC_SensorUnit_ID <- u_SU_LOC$SensorUnit_ID[ith_SU_LOC]
  }
  
  minDate <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + min(tbl_CO2$timestamp[id])
  maxDate <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + max(tbl_CO2$timestamp[id])
  
  subStr <- paste("Data:",strftime(minDate,"%Y/%m/%d",tz="UTC"),"-",strftime(maxDate,"%Y/%m/%d",tz="UTC"))
  
  
  # PLOT: Time series
  
  if(ith_SU_LOC != 0){
    figname <- paste(resultdir,model_name,"/MODEL_STATS/",u_SU_LOC_LocationName,"_",u_SU_LOC_SensorUnit_ID,"_TS.pdf",sep="")
    
    id_gap  <- which(diff(as.numeric(difftime(time1=tbl_CO2$date[id],time2=tbl_CO2$date[id[1]],units="secs",tz="UTC")))>3600)
    
    if(length(id_gap)>0){
      tbl_CO2_tmp <- data.frame(date         = tbl_CO2$date[id],
                                CO2          = tbl_CO2$CO2[id],
                                CO2_model    = tbl_CO2$CO2_model[id],
                                CO2_pred     = tbl_CO2$CO2_pred[id],
                                CO2_pred_LOO = tbl_CO2$CO2_pred_LOO[id],
                                stringsAsFactors = F)
      
      tbl_CO2_tmp <- rbind(tbl_CO2_tmp,data.frame(date         = tbl_CO2$date[id[id_gap]]+3600,
                                                  CO2          = rep(NA,length(id_gap)),
                                                  CO2_model    = rep(NA,length(id_gap)),
                                                  CO2_pred     = rep(NA,length(id_gap)),
                                                  CO2_pred_LOO = rep(NA,length(id_gap)),
                                                  stringsAsFactors = F))
      
      tbl_CO2_tmp <- tbl_CO2_tmp[order(tbl_CO2_tmp$date),]
      
    }else{
      tbl_CO2_tmp <- tbl_CO2[id,]
    }
    
    yyy     <- cbind(tbl_CO2_tmp$CO2,
                     tbl_CO2_tmp$CO2_model,
                     tbl_CO2_tmp$CO2_pred_LOO)
    
    
    xlabString <- "Date" 
    ylabString <- expression(paste("CO2 [ppm]"))
    legend_str <- c("CO2","CO2_model","CO2_pred_LOO")
    plot_ts(figname,tbl_CO2_tmp$date,yyy,"week",NULL,c(350,600),xlabString,ylabString,legend_str)
    
    rm(tbl_CO2_tmp,id_gap)
    gc()
  }
  
  
  # PLOT: Scatter
  
  figname <- paste(resultdir,model_name,"/MODEL_STATS/",u_SU_LOC_LocationName,"_",u_SU_LOC_SensorUnit_ID,"_SCATTER.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=16, height=16, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1.0,1,0.5,0.1),mfrow=c(2,2))
  
  # F1
  
  id_ok     <- id[which(!is.na(tbl_CO2$CO2[id]) & !is.na(tbl_CO2$CO2_model[id]))]
  n_id_ok   <- length(id_ok)
  N_P       <- n_id_ok
  
  xx        <- tbl_CO2$CO2[id_ok]
  yy        <- tbl_CO2$CO2_model[id_ok]
  RES       <- yy-xx
  
  RMSE_P    <- sqrt(sum((RES)^2)/n_id_ok)
  corCoef_P <- cor(x=xx,y=yy,method="pearson",use="complete.obs")
  
  
  str01     <- paste("RMSE:  ", sprintf("%6.2f",RMSE_P))
  str02     <- paste("COR:   ", sprintf("%6.2f",corCoef_P))
  str03     <- paste("N:     ", sprintf("%6.0f",N_P))
  
  xyrange   <- range(c(xx,yy))
  
  mainStr   <- paste(u_SU_LOC_LocationName," ",u_SU_LOC_SensorUnit_ID,sep="")
  xlabStr01 <- expression(paste("CO"[2]*" measurement [ppm]"))
  ylabStr01 <- expression(paste("CO"[2]*" model [ppm]"))
  xlabStr02 <- expression(paste("CO"[2]*" model - CO"[2]*" measurement [ppm]"))
  
  if(xyrange[1]>800){
    xyrange[1] <- 380
  }
  if(xyrange[2]>800){
    xyrange[2] <- 800
  }
  
  
  plot(  xx,  yy,  pch=16,cex=0.75,cex.main=1.5,cex.lab=1.5,cex.axis=1.5,cex.sub=1.25,xlab=xlabStr01,ylab=ylabStr01,main=mainStr,sub=subStr,xlim=xyrange,ylim=xyrange)
  
  lines(c(0,999),c(0,999),col=2,lwd=1)
  
  par(family="mono")
  legend("topleft",legend=c(str01,str02,str03),bg="white",cex=1.5)
  par(family="")
  
  
  # F2
  
  Q000_P  <- quantile(RES,  probs=0.00)
  Q005_P  <- quantile(RES,  probs=0.05)
  Q050_P  <- quantile(RES,  probs=0.50)
  Q095_P  <- quantile(RES,  probs=0.95)
  Q100_P  <- quantile(RES,  probs=1.00)
  
  
  str01 <- paste("Q000:", sprintf("%7.1f",Q000_P))
  str02 <- paste("Q005:", sprintf("%7.1f",Q005_P))
  str03 <- paste("Q050:", sprintf("%7.1f",Q050_P))
  str04 <- paste("Q095:", sprintf("%7.1f",Q095_P))
  str05 <- paste("Q100:", sprintf("%7.1f",Q100_P))
  
  
  hist_vv <- ((max(abs(RES))%/%2)+1)*2
  hist(RES,  seq(-hist_vv,hist_vv,2),xlim=c(-50,50),col="slategray",xlab=xlabStr02,main=mainStr,cex.main=1.5,cex.lab=1.5,cex.axis=1.5)
  
  
  lines(c(0,0),c(-1e6,1e6),col=2,lwd=1)
  
  par(family="mono")
  legend("topleft",legend=c(str01,str02,str03,str04,str05),bg="white",cex=1.5)
  par(family="")
  
  
  # F3
  
  id_ok   <- id[which(!is.na(tbl_CO2$CO2[id]) & !is.na(tbl_CO2$CO2_pred_LOO[id]))]
  n_id_ok <- length(id_ok)
  N_CV    <- n_id_ok
  
  if(N_CV==0){
    Q000_CV    <- NA
    Q005_CV    <- NA
    Q050_CV    <- NA
    Q095_CV    <- NA
    Q100_CV    <- NA
    RMSE_CV    <- NA
    corCoef_CV <- NA
    plot.new()
    plot.new()
  }else{
    
    xx        <- tbl_CO2$CO2[id_ok]
    yy        <- tbl_CO2$CO2_pred_LOO[id_ok]
    RES       <- yy-xx
    
    RMSE_CV    <- sqrt(sum((RES)^2)/n_id_ok)
    corCoef_CV <- cor(x=xx,y=yy,method="pearson",use="complete.obs")
    
    str01   <- paste("RMSE:  ", sprintf("%6.2f",RMSE_CV))
    str02   <- paste("COR:   ", sprintf("%6.2f",corCoef_CV))
    str03   <- paste("N:     ", sprintf("%6.0f",N_CV))
    xyrange <- range(c(xx,yy))
    
    mainStr   <- paste(u_SU_LOC_LocationName," ",u_SU_LOC_SensorUnit_ID,sep="")
    xlabStr01 <- expression(paste("CO"[2]*" measurement [ppm]"))
    ylabStr01 <- expression(paste("CO"[2]*" prediction LOO [ppm]"))
    xlabStr02 <- expression(paste("CO"[2]*" prediction LOO - CO"[2]*" measurement [ppm]"))
    
    if(xyrange[1]>800){
      xyrange[1] <- 380
    }
    if(xyrange[2]>800){
      xyrange[2] <- 800
    }
    
    plot(  xx,  yy,  pch=16,cex=0.75,cex.main=1.5,cex.lab=1.5,cex.axis=1.5,cex.sub=1.25,xlab=xlabStr01,ylab=ylabStr01,main=mainStr,sub=subStr,xlim=xyrange,ylim=xyrange)
    
    lines(c(0,999),c(0,999),col=2,lwd=1)
    
    par(family="mono")
    legend("topleft",legend=c(str01,str02,str03),bg="white",cex=1.5)
    par(family="")
    
  }
  
  # F4
  
  if(N_CV!=0){
    
    Q000_CV  <- quantile(RES,probs=0.00)
    Q005_CV  <- quantile(RES,probs=0.05)
    Q050_CV  <- quantile(RES,probs=0.50)
    Q095_CV  <- quantile(RES,probs=0.95)
    Q100_CV  <- quantile(RES,probs=1.00)
    
    str01 <- paste("Q000:  ", sprintf("%7.1f",Q000_CV))
    str02 <- paste("Q005:  ", sprintf("%7.1f",Q005_CV))
    str03 <- paste("Q050:  ", sprintf("%7.1f",Q050_CV))
    str04 <- paste("Q095:  ", sprintf("%7.1f",Q095_CV))
    str05 <- paste("Q100:  ", sprintf("%7.1f",Q100_CV))
    
    
    hist_vv <- ((max(abs(RES))%/%2)+1)*2
    hist(RES,  seq(-hist_vv,hist_vv,2),xlim=c(-50,50),col="slategray",xlab=xlabStr02,main=mainStr,cex.main=1.5,cex.lab=1.5,cex.axis=1.5)
    
    lines(c(0,0),c(-1e6,1e6),col=2,lwd=1)
    
    par(family="mono")
    legend("topleft",legend=c(str01,str02,str03,str04,str05),bg="white",cex=1.5)
    par(family="")
    
  }
  
  #
  
  dev.off()
  par(def_par)
  
  ###
  
  statistics <- rbind(statistics,data.frame(LocationName  = u_SU_LOC_LocationName,
                                            SensorUnit_ID = u_SU_LOC_SensorUnit_ID,
                                            RMSE_P        = sprintf("%.2f",RMSE_P),
                                            corCoef_P     = sprintf("%.2f",corCoef_P),
                                            N_P           = N_P,
                                            Q000_P        = sprintf("%.2f",Q000_P),
                                            Q005_P        = sprintf("%.2f",Q005_P),
                                            Q050_P        = sprintf("%.2f",Q050_P),
                                            Q095_P        = sprintf("%.2f",Q095_P),
                                            Q100_P        = sprintf("%.2f",Q100_P),
                                            RMSE_CV       = sprintf("%.2f",RMSE_CV),
                                            corCoef_CV    = sprintf("%.2f",corCoef_CV),
                                            N_CV          = N_CV,
                                            Q000_CV       = sprintf("%.2f",Q000_CV),
                                            Q005_CV       = sprintf("%.2f",Q005_CV),
                                            Q050_CV       = sprintf("%.2f",Q050_CV),
                                            Q095_CV       = sprintf("%.2f",Q095_CV),
                                            Q100_CV       = sprintf("%.2f",Q100_CV),
                                            stringsAsFactors = F))
  
}

write.table(statistics,paste(resultdir,model_name,"/MODEL_STATS/","statistics_",approach,".csv",sep=""),col.names=T,row.names=F,sep=";",quote=F)



