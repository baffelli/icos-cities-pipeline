# Temp_ZH_RPART.r
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
library(maptools)

source("/project/muem/CarboSense/Software/CarboSenseFunctions.r")

### ----------------------------------------------------------------------------------------------------------------------------

args = commandArgs(trailingOnly=TRUE)

selected_version <- args[1]

if(!selected_version%in%c(10,11,20,21,22,23,30)){
  stop()
}

### ----------------------------------------------------------------------------------------------------------------------------

## Files and directories

resultdir <- "/project/CarboSense/Statistical_Models/T_MAPPING/"
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
if(selected_version==22){
  approach   <- "RF"
  model_name <- "RF_V1_noDTV"
  nodesize   <- 3
  ntree      <- 30
}
if(selected_version==23){
  approach   <- "RF"
  model_name <- "RF_V2_noDTV"
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

Date_UTC_from  <- strptime("20171221000000","%Y%m%d%H%M%S",tz="UTC")
Date_UTC_to    <- strptime("20180104000000","%Y%m%d%H%M%S",tz="UTC")

Date_UTC_from  <- strptime("20171120000000","%Y%m%d%H%M%S",tz="UTC")
Date_UTC_to    <- strptime("20171127000000","%Y%m%d%H%M%S",tz="UTC")

Date_UTC_from  <- strptime("20190612000000","%Y%m%d%H%M%S",tz="UTC")
Date_UTC_to    <- strptime("20190617000000","%Y%m%d%H%M%S",tz="UTC")

Date_UTC_from  <- strptime("20181201000000","%Y%m%d%H%M%S",tz="UTC")
Date_UTC_to    <- strptime("20190617000000","%Y%m%d%H%M%S",tz="UTC")
# Date_UTC_to    <- strptime("20181205000000","%Y%m%d%H%M%S",tz="UTC")

Date_UTC_from  <- strptime("20190624000000","%Y%m%d%H%M%S",tz="UTC")
Date_UTC_to    <- strptime("20190626000000","%Y%m%d%H%M%S",tz="UTC")

# Date_UTC_from  <- strptime("20190830000000","%Y%m%d%H%M%S",tz="UTC")
# Date_UTC_to    <- strptime("20190903000000","%Y%m%d%H%M%S",tz="UTC")

# Date_UTC_from  <- strptime("20190101000000","%Y%m%d%H%M%S",tz="UTC")
# Date_UTC_to    <- strptime("20200101000000","%Y%m%d%H%M%S",tz="UTC")

Date_UTC_from  <- strptime("20170901000000","%Y%m%d%H%M%S",tz="UTC")
Date_UTC_to    <- strptime("20200611000000","%Y%m%d%H%M%S",tz="UTC")

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



# SHT21 measurements

query_str       <- paste("SELECT timestamp, LocationName, SensorUnit_ID, SHT21_T FROM CarboSense_T_RH ",sep="")
query_str       <- paste(query_str, "WHERE timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to," ",sep="")
query_str       <- paste(query_str, "AND LocationName IN ('",paste(ZH_locations_LP8,collapse="','",sep=""),"');",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_TEMP        <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_TEMP$weight   <- 1

if(T){
  
  u_SU_LOC <- unique(data.frame(LocationName = tbl_TEMP$LocationName,
                                SensorUnit_ID= tbl_TEMP$SensorUnit_ID,
                                stringsAsFactors = F))
  
  n_u_SU_LOC   <- dim(u_SU_LOC)[1]
  
  tbl_TEMP_tmp <- tbl_TEMP
  tbl_TEMP     <- NULL
  
  for(ith_u_SU_LOC in 1:n_u_SU_LOC){
    
    id <- which(tbl_TEMP_tmp$LocationName    == u_SU_LOC$LocationName[ith_u_SU_LOC]
                & tbl_TEMP_tmp$SensorUnit_ID == u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC])
    
    tmp <- data.frame(date     = strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_TEMP_tmp$timestamp[id],
                      SHT21_T  = tbl_TEMP_tmp$SHT21_T[id],
                      stringsAsFactors = F)
    
    tmp <- tmp[order(tmp$date),]
    
    tmp <- timeAverage(mydata = tmp,avg.time = "1 hour",statistic = "mean",
                       start.date = strptime(strftime(min(tmp$date)-3600,"%Y%m%d%H0000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
    
    tmp <- tmp[which(!is.na(tmp$SHT21_T)),]
    
    tbl_TEMP <- rbind(tbl_TEMP,data.frame(timestamp     = as.numeric(difftime(time1=tmp$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                          LocationName  = rep(u_SU_LOC$LocationName[ith_u_SU_LOC],dim(tmp)[1]),
                                          SensorUnit_ID = rep(u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC],dim(tmp)[1]),
                                          SHT21_T       = tmp$SHT21_T,
                                          weight        = rep(1,dim(tmp)[1]),
                                          stringsAsFactors = F))
  }
  
  rm(u_SU_LOC,n_u_SU_LOC,tbl_TEMP_tmp,tmp,id)
  gc()
  
}



# MeteoSwiss measurements

query_str       <- paste("SELECT LocationName, timestamp,Temperature from METEOSWISS_Measurements ",sep="")
query_str       <- paste(query_str, "WHERE timestamp >= ",timestamp_from," AND timestamp <= ",timestamp_to," and Temperature != -999 ",sep="")
query_str       <- paste(query_str, "AND LocationName IN ('LAE','KLO','SMA','REH','UEB','NABZUE','NABDUE') ",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_MCH_TEMP    <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

colnames(tbl_MCH_TEMP)[which(colnames(tbl_MCH_TEMP)=="Temperature")] <- "SHT21_T"

tbl_MCH_TEMP$weight   <- 1

if(T){
  
  u_LOC         <- unique(tbl_MCH_TEMP$LocationName)
  n_u_LOC       <- length(u_LOC)
  
  tbl_MCH_TEMP_tmp <- tbl_MCH_TEMP
  tbl_MCH_TEMP     <- NULL
  
  for(ith_u_LOC in 1:n_u_LOC){
    
    id <- which(tbl_MCH_TEMP_tmp$LocationName == u_LOC[ith_u_LOC])
    
    tmp <- data.frame(date     = strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_MCH_TEMP_tmp$timestamp[id],
                      SHT21_T  = tbl_MCH_TEMP_tmp$SHT21_T[id],
                      stringsAsFactors = F)
    
    tmp <- tmp[order(tmp$date),]
    
    tmp <- timeAverage(mydata = tmp,avg.time = "1 hour",statistic = "mean",
                       start.date = strptime(strftime(min(tmp$date)-3600,"%Y%m%d%H0000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
    
    tmp <- tmp[which(!is.na(tmp$SHT21_T)),]
    
    tbl_MCH_TEMP <- rbind(tbl_MCH_TEMP,data.frame(timestamp     = as.numeric(difftime(time1=tmp$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                                  LocationName  = rep(u_LOC[ith_u_LOC],dim(tmp)[1]),
                                                  SensorUnit_ID = rep(ith_u_LOC,dim(tmp)[1]),
                                                  SHT21_T       = tmp$SHT21_T,
                                                  weight        = rep(1,dim(tmp)[1]),
                                                  stringsAsFactors = F))
  }
  
  rm(u_SU_LOC,n_u_SU_LOC,tbl_MCH_TEMP_tmp,tmp,id)
  gc()
  
}


#

if(dim(tbl_TEMP)[1]>0){
  tbl_TEMP <- rbind(tbl_TEMP,data.frame(timestamp     = tbl_MCH_TEMP$timestamp,
                                        LocationName  = tbl_MCH_TEMP$LocationName,
                                        SensorUnit_ID = tbl_MCH_TEMP$SensorUnit_ID,
                                        SHT21_T       = tbl_MCH_TEMP$SHT21_T,
                                        weight        = 1,
                                        stringsAsFactors = F))
}

rm(tbl_MCH_TEMP)
gc()

#

tbl_TEMP$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_TEMP$timestamp

### ----------------------------------------------------------------------------------------------------------------------------

## Add coordinates and heights to temperature observation data


u_SU_LOC    <- unique(data.frame(LocationName  = tbl_TEMP$LocationName,
                                 SensorUnit_ID = tbl_TEMP$SensorUnit_ID,
                                 stringsAsFactors = F))
n_u_SU_LOC  <- dim(u_SU_LOC)[1] 

tbl_TEMP$X_LV03 <- NA
tbl_TEMP$Y_LV03 <- NA
tbl_TEMP$h      <- NA
tbl_TEMP$hdepl  <- NA


for(ith_SU_LOC in 1:n_u_SU_LOC){
  
  id_loc  <- which(tbl_location$LocationName == u_SU_LOC$LocationName[ith_SU_LOC])
  
  if(u_SU_LOC$LocationName[ith_SU_LOC] %in% c('KLO','LAE','REH','NABZUE','NABDUE') & u_SU_LOC$SensorUnit_ID[ith_SU_LOC] < 1000){
    
    id_meas <- which(tbl_TEMP$LocationName == u_SU_LOC$LocationName[ith_SU_LOC]
                     & tbl_TEMP$SensorUnit_ID == u_SU_LOC$SensorUnit_ID[ith_SU_LOC])
    
    tbl_TEMP$X_LV03[id_meas] <- tbl_location$X_LV03[id_loc]
    tbl_TEMP$Y_LV03[id_meas] <- tbl_location$Y_LV03[id_loc]
    tbl_TEMP$h[id_meas]      <- tbl_location$h[id_loc]
    tbl_TEMP$hdepl[id_meas]  <- tbl_location$h[id_loc] + 2
    
    
  }else{
    
    id_depl <- which(tbl_deployment$LocationName == u_SU_LOC$LocationName[ith_SU_LOC]
                     & tbl_deployment$SensorUnit_ID == u_SU_LOC$SensorUnit_ID[ith_SU_LOC])
    
    for(ith_depl in 1:length(id_depl)){
      
      id_meas <- which(tbl_TEMP$LocationName == u_SU_LOC$LocationName[ith_SU_LOC]
                       & tbl_TEMP$SensorUnit_ID == u_SU_LOC$SensorUnit_ID[ith_SU_LOC]
                       & tbl_TEMP$timestamp >= tbl_deployment$timestamp_from[id_depl[ith_depl]]
                       & tbl_TEMP$timestamp <= tbl_deployment$timestamp_to[id_depl[ith_depl]])
      
      tbl_TEMP$X_LV03[id_meas] <- tbl_location$X_LV03[id_loc]
      tbl_TEMP$Y_LV03[id_meas] <- tbl_location$Y_LV03[id_loc]
      tbl_TEMP$h[id_meas]      <- tbl_location$h[id_loc]
      tbl_TEMP$hdepl[id_meas]  <- tbl_location$h[id_loc] + tbl_deployment$HeightAboveGround[id_depl[ith_depl]]
      
    }
  }
}

tbl_TEMP <- tbl_TEMP[which(!is.na(tbl_TEMP$X_LV03) & !is.na(tbl_TEMP$Y_LV03)),]


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

if(selected_version%in%c(22,23)){
  cn_model_func       <- c("LCOV_FRAC_SEALED_AREAS_01000",
                           "LCOV_FRAC_SEALED_AREAS_02000",
                           "LCOV_FRAC_BUILDINGS_01000",
                           "LCOV_FRAC_BUILDINGS_02000",
                           "LCOV_FRAC_FOREST_02000",
                           "LCOV_FRAC_WATER_02000",
                           "rel_h_1000",
                           "rel_h_2500",
                           "h")
}



cn_model_func_add   <- cn_model_func[!cn_model_func%in%colnames(tbl_TEMP)]
cn_model_func_add   <- cn_model_func_add[!cn_model_func_add%in%c("DTV","DTV1","DTV2")]
n_cn_model_func_add <- length(cn_model_func_add)


if(n_cn_model_func_add>0){
  
  #
  
  for(i in 1:n_cn_model_func_add){
    tbl_TEMP$NCol      <- NA
    colnames(tbl_TEMP) <- c(colnames(tbl_TEMP)[1:(dim(tbl_TEMP)[2]-1)],cn_model_func_add[i])
  }
  
  
  #
  
  u_SU_LOC   <- unique(data.frame(SensorUnit_ID = tbl_TEMP$SensorUnit_ID, 
                                  LocationName  = tbl_TEMP$LocationName,
                                  Y_LV03        = tbl_TEMP$Y_LV03,
                                  X_LV03        = tbl_TEMP$X_LV03,
                                  stringsAsFactors = F))
  n_u_SU_LOC <- dim(u_SU_LOC)[1]
  
  
  
  for(ith_u_SU_LOC in 1:n_u_SU_LOC){
    
    id_SHT21_T_SU_LOC <- which(tbl_TEMP$SensorUnit_ID   == u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC]
                               & tbl_TEMP$LocationName  == u_SU_LOC$LocationName[ith_u_SU_LOC]
                               & tbl_TEMP$Y_LV03        == u_SU_LOC$Y_LV03[ith_u_SU_LOC]
                               & tbl_TEMP$X_LV03        == u_SU_LOC$X_LV03[ith_u_SU_LOC])
    
    dist          <- sqrt((grid$Y_LV03-u_SU_LOC$Y_LV03[ith_u_SU_LOC])^2+(grid$X_LV03-u_SU_LOC$X_LV03[ith_u_SU_LOC])^2)
    id_grid       <- which(dist==min(dist))[1]
    
    for(i in 1:n_cn_model_func_add){
      pos_grid <- which(colnames(grid)   ==cn_model_func_add[i])
      pos_co2  <- which(colnames(tbl_TEMP)==cn_model_func_add[i])
      
      tbl_TEMP[id_SHT21_T_SU_LOC,pos_co2] <- rep(grid[id_grid,pos_grid],length(id_SHT21_T_SU_LOC))
    }
  }
}

##

tbl_TEMP$DTV  <- 0
tbl_TEMP$DTV1 <- 0
tbl_TEMP$DTV2 <- 0
cell         <- cellFromXY(TrafficInt,xy = matrix(c(tbl_TEMP$Y_LV03,tbl_TEMP$X_LV03),ncol=2))
id_cell_ok   <- which(!is.na(cell))

if(length(id_cell_ok)>0){
  tbl_TEMP$DTV[id_cell_ok]  <- as.numeric(TrafficInt[cell[id_cell_ok]])
  tbl_TEMP$DTV1[id_cell_ok] <- as.numeric(TrafficInt_1[cell[id_cell_ok]])
  tbl_TEMP$DTV2[id_cell_ok] <- as.numeric(TrafficInt_2[cell[id_cell_ok]])
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

if(selected_version%in%c(22,23)){
  pred_grid_export_xmin <- 671000 + 1e3
  pred_grid_export_xmax <- 696000 - 4e3
  pred_grid_export_ymin <- 238000 + 2e3
  pred_grid_export_ymax <- 263000 - 3e3
  pred_grid_export_res  <- 100
}



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

tbl_TEMP$SHT21_T_LOG      <- log(tbl_TEMP$SHT21_T)
tbl_TEMP$SHT21_T_model    <- NA
tbl_TEMP$SHT21_T_pred     <- NA
tbl_TEMP$SHT21_T_pred_LOO <- NA


for(timestamp_now in seq(timestamp_from,timestamp_to,modelling_period_length)){
  
  date_now      <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + timestamp_now
  date_now_hour <- as.numeric(strftime(date_now,"%H",tz="UTC"))
  date_str      <- strftime(date_now, "%Y%m%d_%H%M%S",tz="UTC")
  
  print(paste(date_now," -- ST ",Sys.time()))
  
  # Sunrise / sunset
  
  hels <- matrix(c(8.54015,47.37784), nrow=1)
  Hels <- SpatialPoints(hels, proj4string=CRS("+proj=longlat +datum=WGS84"))
  
  sunset_date  <- sunriset(Hels, as.POSIXct(date_now),direction="sunset",   POSIXct.out=TRUE)
  sunrise_date <- sunriset(Hels, as.POSIXct(date_now),direction="sunrise",  POSIXct.out=TRUE)
  
  if((date_now+3600) > (sunrise_date$time) & date_now < (sunset_date$time+7200)){
    next
  }

  
  # Data selection
  
  id_tp    <- which(tbl_TEMP$timestamp>=timestamp_now & tbl_TEMP$timestamp<(timestamp_now+modelling_period_length))
  n_id_tp  <- length(id_tp)
  
  if(n_id_tp==0){
    next
  }
  
  id_SHT21_T   <- id_tp[which(tbl_TEMP$SHT21_T[id_tp]>=-50 & tbl_TEMP$SHT21_T[id_tp]<=50)]
  n_id_SHT21_T <- length(id_SHT21_T)
  
  if(n_id_tp != n_id_SHT21_T){
    print(paste("n_id_tp",n_id_tp,"n_id_SHT21_T",n_id_SHT21_T))
  }
  
  #
  
  u_SU_LOC <- unique(data.frame(LocationName  = tbl_TEMP$LocationName[id_SHT21_T],
                                SensorUnit_ID = tbl_TEMP$SensorUnit_ID[id_SHT21_T],
                                Y_LV03        = tbl_TEMP$Y_LV03[id_SHT21_T],
                                X_LV03        = tbl_TEMP$X_LV03[id_SHT21_T],
                                stringsAsFactors = F))
  n_u_SU_LOC <- dim(u_SU_LOC)[1]
  
  #
  
  if(approach=="GAM"){
    if(length(id_SHT21_T)==0 | n_u_SU_LOC<13){
      next
    }
  }
  
  if(approach=="RPART"){
    if(length(id_SHT21_T)==0 | n_u_SU_LOC<2){
      next
    }
  }
  
  
  if(approach=="GAM"){
    
    formula                            <- as.formula(paste("SHT21_T_LOG~",paste(paste("s(",cn_model_func,")",sep=""),collapse = "+"),sep=""))
    
    gam_obj                            <- gam(formula,data=tbl_TEMP[id_SHT21_T,],weights = tbl_TEMP$weight[id_SHT21_T])
    
    tbl_TEMP$SHT21_T_model[id_SHT21_T] <- exp(predict(object=gam_obj,newdata = tbl_TEMP[id_SHT21_T,]))
    
    tbl_TEMP$SHT21_T_pred[id_tp]       <- exp(predict(object=gam_obj,newdata = tbl_TEMP[id_tp,]))
    
    pred_grid_export$SHT21_T           <- exp(predict(object=gam_obj,newdata = pred_grid_export))
    
  }
  
  if(approach=="RPART"){
    
    formula                            <- as.formula(paste("SHT21_T~",paste(cn_model_func,collapse = "+"),sep=""))
    
    rpart_obj                          <- rpart(formula = formula,data = tbl_TEMP[id_SHT21_T,],control = rpart_control,weights = tbl_TEMP$weight[id_SHT21_T])
    
    tbl_TEMP$SHT21_T_model[id_SHT21_T] <- predict(object=rpart_obj,newdata = tbl_TEMP[id_SHT21_T,])
    
    tbl_TEMP$SHT21_T_pred[id_tp]       <- predict(object=rpart_obj,newdata = tbl_TEMP[id_tp,])
    
    pred_grid_export$SHT21_T           <- predict(object=rpart_obj,newdata = pred_grid_export)
  }
  
  if(approach=="RF"){
    
    formula                            <- as.formula(paste("SHT21_T~",paste(cn_model_func,collapse = "+"),sep=""))
    
    rf_obj                             <- randomForest(formula = formula,data=tbl_TEMP[id_SHT21_T,],
                                                       ntree=ntree,nodesize=nodesize, 
                                                       weights = tbl_TEMP$weight[id_SHT21_T])
    
    tbl_TEMP$SHT21_T_model[id_SHT21_T] <- predict(object=rf_obj,newdata = tbl_TEMP[id_SHT21_T,])
    
    tbl_TEMP$SHT21_T_pred[id_tp]       <- predict(object=rf_obj,newdata = tbl_TEMP[id_tp,])
    
    pred_grid_export$SHT21_T           <- predict(object=rf_obj,newdata = pred_grid_export)
  }
  
  
  #
  
  if(DO_GRID_EXPORT){
    
    grid_export_fn  <- paste(resultdir,model_name,"/DATA/","PRED_GRID_",date_str,".csv",sep="")
    
    write.table(x = data.frame(Y_LV03  = pred_grid_export$Y_LV03,
                               X_LV03  = pred_grid_export$X_LV03,
                               SHT21_T = pred_grid_export$SHT21_T,
                               stringsAsFactors = F),
                file = grid_export_fn,sep = ";",row.names = F,col.names = T,quote = F)
    
    meas_export_fn  <- paste(resultdir,model_name,"/DATA/","MEAS_",date_str,".csv",sep="")
    
    write.table(x = data.frame(LocationName  = tbl_TEMP$LocationName[id_SHT21_T],
                               SensorUnit_ID = tbl_TEMP$SensorUnit_ID[id_SHT21_T],
                               timestamp     = tbl_TEMP$timestamp[id_SHT21_T],
                               Y_LV03        = tbl_TEMP$Y_LV03[id_SHT21_T],
                               X_LV03        = tbl_TEMP$X_LV03[id_SHT21_T],
                               SHT21_T       = tbl_TEMP$SHT21_T[id_SHT21_T],
                               stringsAsFactors = F),
                file = meas_export_fn,sep = ";",row.names = F,col.names = T,quote = F)
    
  }
  
  
  
  
  # L-O-O cross-validation
  
  for(ith_SU_LOC in 1:n_u_SU_LOC){
    
    id_SHT21_T_LOO_TRAINING <- id_SHT21_T[which(tbl_TEMP$LocationName[id_SHT21_T] != u_SU_LOC$LocationName[ith_SU_LOC])]
    
    if(u_SU_LOC$LocationName[ith_SU_LOC] %in% c("REH","RECK")){
      id_SHT21_T_LOO_TRAINING <- id_SHT21_T[which(!tbl_TEMP$LocationName[id_SHT21_T]%in%c("REH","RECK"))]
    }
    
    if(u_SU_LOC$LocationName[ith_SU_LOC] %in% c("NABZUE","ZUE")){
      id_SHT21_T_LOO_TRAINING <- id_SHT21_T[which(!tbl_TEMP$LocationName[id_SHT21_T]%in%c("NABZUE","ZUE"))]
    }
    
    if(u_SU_LOC$LocationName[ith_SU_LOC] %in% c("NABDUE","DUE")){
      id_SHT21_T_LOO_TRAINING <- id_SHT21_T[which(!tbl_TEMP$LocationName[id_SHT21_T]%in%c("NABDUE","DUE"))]
    }
    
    if(u_SU_LOC$LocationName[ith_SU_LOC] %in% c("LAE","LAEG")){
      id_SHT21_T_LOO_TRAINING <- id_SHT21_T[which(!tbl_TEMP$LocationName[id_SHT21_T]%in%c("NABDUE","DUE"))]
    }
    
    id_SHT21_T_LOO_TEST <- id_SHT21_T[which(tbl_TEMP$LocationName[id_SHT21_T]    == u_SU_LOC$LocationName[ith_SU_LOC]
                                            & tbl_TEMP$SensorUnit_ID[id_SHT21_T] == u_SU_LOC$SensorUnit_ID[ith_SU_LOC])]
    
    
    if(approach=="GAM"){
      gam_obj_LOO <- gam(formula,data=tbl_TEMP[id_SHT21_T_LOO_TRAINING,],  weights = tbl_TEMP$weight[id_SHT21_T_LOO_TRAINING])
      
      tbl_TEMP$SHT21_T_pred_LOO[id_SHT21_T_LOO_TEST] <- exp(predict(object=gam_obj_LOO,newdata = tbl_TEMP[id_SHT21_T_LOO_TEST,]))
    }
    
    
    if(approach=="RPART"){
      
      rpart_obj_LOO <- rpart(formula = formula,data = tbl_TEMP[id_SHT21_T_LOO_TRAINING,],control = rpart_control)
      
      tbl_TEMP$SHT21_T_pred_LOO[id_SHT21_T_LOO_TEST] <- predict(object=rpart_obj_LOO,newdata = tbl_TEMP[id_SHT21_T_LOO_TEST,])
    }
    
    if(approach=="RF"){
      
      rf_obj   <- randomForest(formula = formula,data=tbl_TEMP[id_SHT21_T_LOO_TRAINING,],ntree=ntree,nodesize=nodesize)
      
      tbl_TEMP$SHT21_T_pred_LOO[id_SHT21_T_LOO_TEST] <- predict(object=rf_obj,newdata = tbl_TEMP[id_SHT21_T_LOO_TEST,])
    }
  }
  
  if(any(is.na(tbl_TEMP$SHT21_T_pred_LOO[id_SHT21_T]) & !is.na(tbl_TEMP$SHT21_T_pred[id_SHT21_T]))){
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
    
    id_SHT21_T_tooHigh <- which(pred_grid_export$SHT21_T > 30)
    if(length(id_SHT21_T_tooHigh)>0){
      pred_grid_export$SHT21_T[id_SHT21_T_tooHigh] <- 30
    }
    id_SHT21_T_tooLow <- which(pred_grid_export$SHT21_T< -15)
    if(length(id_SHT21_T_tooLow)>0){
      pred_grid_export$SHT21_T[id_SHT21_T_tooLow] <- -15
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
      GRID_pred_grid[ith_row,1:n_u_pred_grid_yy] <- pred_grid_export$SHT21_T[cc*n_u_pred_grid_yy+(1:n_u_pred_grid_yy)]
    }
    
    #
    
    u_SU_LOC$SHT21_T_mean <- NA
    u_SU_LOC$xx           <- NA
    
    for(ith_SU_LOC in 1:dim(u_SU_LOC)[1]){
      id_meas <- which(  tbl_TEMP$LocationName[id_SHT21_T]  == u_SU_LOC$LocationName[ith_SU_LOC]
                         & tbl_TEMP$SensorUnit_ID[id_SHT21_T] == u_SU_LOC$SensorUnit_ID[ith_SU_LOC])
      
      u_SU_LOC$SHT21_T_mean[ith_SU_LOC] <- mean(tbl_TEMP$SHT21_T[id_SHT21_T[id_meas]])
      
    }
    
    id_SHT21_T_tooHigh <- which(u_SU_LOC$SHT21_T_mean > 30)
    if(length(id_SHT21_T_tooHigh)>0){
      u_SU_LOC$SHT21_T_mean[id_SHT21_T_tooHigh] <- 30
    }
    id_SHT21_T_tooLow <- which(u_SU_LOC$SHT21_T_mean < -15)
    if(length(id_SHT21_T_tooLow)>0){
      u_SU_LOC$SHT21_T_mean[id_SHT21_T_tooLow] <- -15
    }
    
    xx <- round((u_SU_LOC$SHT21_T_mean-370)/(650-370)*255)+1
    
    #
    
    image.plot(u_pred_grid_yy,rev(u_pred_grid_xx),t(GRID_pred_grid),col=rev(heat.colors(256)),zlim=c(370,650),xlab="Easting [m]", ylab="Norhting [m]",main="",cex.lab=1.5,cex.axis=1.5,cex.main=1.5,legend.args=list(text="T [deg C]",col=1, cex=1.0, side=1, line=2))
    
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
    
    rm(x,xx,id_SHT21_T_tooHigh,id_SHT21_T_tooLow,x,u_grid_yy,u_grid_xx,GRID_grid_pred)
    gc()
  }
  
  ###
}


### ----------------------------------------------------------------------------------------------------------------------------

# 
# tbl_TEMP_tmp <- NULL
# 
# u_SU_LOC <- unique(data.frame(LocationName  = tbl_TEMP$LocationName,
#                               SensorUnit_ID = tbl_TEMP$SensorUnit_ID,
#                               stringsAsFactors = F))
# 
# for(ith_u_SU_LOC in 1:dim(u_SU_LOC)[1]){
#   id_ok <- which(  tbl_TEMP$LocationName    == u_SU_LOC$LocationName[ith_u_SU_LOC]
#                    & tbl_TEMP$SensorUnit_ID == u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC]
#                    & tbl_TEMP$O_FLAG==1)
#   
#   tmp <- timeAverage(mydata = tbl_TEMP[id_ok,],avg.time = "hour",statistic = "mean",start.date = Date_UTC_from)
#   tmp$LocationName  <- rep(u_SU_LOC$LocationName[ith_u_SU_LOC], dim(tmp)[1])
#   tmp$SensorUnit_ID <- rep(u_SU_LOC$SensorUnit_ID[ith_u_SU_LOC],dim(tmp)[1])
#   
#   id_ok <- which(!is.na(tmp$CO2))
#   
#   if(length(id_ok)>0){
#     tbl_TEMP_tmp <- rbind(tbl_TEMP_tmp,tmp[id_ok,])
#   }
# }
# 
# tbl_TEMP <- tbl_TEMP_tmp
# 
# rm(id_ok,tbl_TEMP_tmp,tmp,u_SU_LOC)
# gc()


###


statistics <- NULL

u_SU_LOC    <- unique(data.frame(LocationName  = tbl_TEMP$LocationName,
                                 SensorUnit_ID = tbl_TEMP$SensorUnit_ID,
                                 stringsAsFactors = F))
n_u_SU_LOC  <- dim(u_SU_LOC)[1]


for(ith_SU_LOC in 0:n_u_SU_LOC){
  
  if(ith_SU_LOC==0){
    id <- 1:dim(tbl_TEMP)[1]
    
    u_SU_LOC_LocationName  <- "ALL"
    u_SU_LOC_SensorUnit_ID <- 0
  }else{
    id <- which(tbl_TEMP$LocationName    == u_SU_LOC$LocationName[ith_SU_LOC] 
                & tbl_TEMP$SensorUnit_ID == u_SU_LOC$SensorUnit_ID[ith_SU_LOC])
    
    u_SU_LOC_LocationName  <- u_SU_LOC$LocationName[ith_SU_LOC]
    u_SU_LOC_SensorUnit_ID <- u_SU_LOC$SensorUnit_ID[ith_SU_LOC]
  }
  
  minDate <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + min(tbl_TEMP$timestamp[id])
  maxDate <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + max(tbl_TEMP$timestamp[id])
  
  subStr <- paste("Data:",strftime(minDate,"%Y/%m/%d",tz="UTC"),"-",strftime(maxDate,"%Y/%m/%d",tz="UTC"))
  
  
  # PLOT: Time series
  
  if(ith_SU_LOC != 0){
    figname <- paste(resultdir,model_name,"/MODEL_STATS/",u_SU_LOC_LocationName,"_",u_SU_LOC_SensorUnit_ID,"_TS.pdf",sep="")
    
    id_gap  <- which(diff(as.numeric(difftime(time1=tbl_TEMP$date[id],time2=tbl_TEMP$date[id[1]],units="secs",tz="UTC")))>3600)
    
    if(length(id_gap)>0){
      tbl_TEMP_tmp <- data.frame(date             = tbl_TEMP$date[id],
                                 SHT21_T          = tbl_TEMP$SHT21_T[id],
                                 SHT21_T_model    = tbl_TEMP$SHT21_T_model[id],
                                 SHT21_T_pred     = tbl_TEMP$SHT21_T_pred[id],
                                 SHT21_T_pred_LOO = tbl_TEMP$SHT21_T_pred_LOO[id],
                                 stringsAsFactors = F)
      
      tbl_TEMP_tmp <- rbind(tbl_TEMP_tmp,data.frame(date             = tbl_TEMP$date[id[id_gap]]+3600,
                                                    SHT21_T          = rep(NA,length(id_gap)),
                                                    SHT21_T_model    = rep(NA,length(id_gap)),
                                                    SHT21_T_pred     = rep(NA,length(id_gap)),
                                                    SHT21_T_pred_LOO = rep(NA,length(id_gap)),
                                                    stringsAsFactors = F))
      
      tbl_TEMP_tmp <- tbl_TEMP_tmp[order(tbl_TEMP_tmp$date),]
      
    }else{
      tbl_TEMP_tmp <- tbl_TEMP[id,]
    }
    
    yyy     <- cbind(tbl_TEMP_tmp$SHT21_T,
                     tbl_TEMP_tmp$SHT21_T_model,
                     tbl_TEMP_tmp$SHT21_T_pred_LOO)
    
    
    xlabString <- "Date" 
    ylabString <- expression(paste("T [deg C]"))
    legend_str <- c("SHT21_T","SHT21_T_model","SHT21_T_pred_LOO")
    plot_ts(figname,tbl_TEMP_tmp$date,yyy,"week",NULL,c(-15,30),xlabString,ylabString,legend_str)
    
    rm(tbl_TEMP_tmp,id_gap)
    gc()
  }
  
  
  # PLOT: Scatter
  
  figname <- paste(resultdir,model_name,"/MODEL_STATS/",u_SU_LOC_LocationName,"_",u_SU_LOC_SensorUnit_ID,"_SCATTER.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=16, height=16, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1.0,1,0.5,0.1),mfrow=c(2,2))
  
  # F1
  
  id_ok     <- id[which(!is.na(tbl_TEMP$SHT21_T[id]) & !is.na(tbl_TEMP$SHT21_T_model[id]))]
  n_id_ok   <- length(id_ok)
  N_P       <- n_id_ok
  
  xx        <- tbl_TEMP$SHT21_T[id_ok]
  yy        <- tbl_TEMP$SHT21_T_model[id_ok]
  RES       <- yy-xx
  
  RMSE_P    <- sqrt(sum((RES)^2)/n_id_ok)
  corCoef_P <- cor(x=xx,y=yy,method="pearson",use="complete.obs")
  
  
  str01     <- paste("RMSE:  ", sprintf("%6.2f",RMSE_P))
  str02     <- paste("COR:   ", sprintf("%6.2f",corCoef_P))
  str03     <- paste("N:     ", sprintf("%6.0f",N_P))
  
  xyrange   <- range(c(xx,yy))
  
  mainStr   <- paste(u_SU_LOC_LocationName," ",u_SU_LOC_SensorUnit_ID,sep="")
  xlabStr01 <- expression(paste("T measurement [deg C]"))
  ylabStr01 <- expression(paste("T prediction LOO [deg C]"))
  xlabStr02 <- expression(paste("T prediction LOO - T measurement [deg C]"))
  
  if(xyrange[1]>800){
    xyrange[1] <- 380
  }
  if(xyrange[2]>800){
    xyrange[2] <- 800
  }
  
  
  plot(  xx,  yy,  pch=16,cex=0.75,cex.main=1.5,cex.lab=1.5,cex.axis=1.5,cex.sub=1.25,xlab=xlabStr01,ylab=ylabStr01,main=mainStr,sub=subStr,xlim=xyrange,ylim=xyrange)
  
  lines(c(-999,999),c(-999,999),col=2,lwd=1)
  
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
  
  
  hist_vv <- ((max(abs(RES))%/%0.25)+1)*0.25
  hist(RES,  seq(-hist_vv,hist_vv,0.25),xlim=c(-10,10),col="slategray",xlab=xlabStr02,main=mainStr,cex.main=1.5,cex.lab=1.5,cex.axis=1.5)
  
  
  lines(c(0,0),c(-1e6,1e6),col=2,lwd=1)
  
  par(family="mono")
  legend("topleft",legend=c(str01,str02,str03,str04,str05),bg="white",cex=1.5)
  par(family="")
  
  
  # F3
  
  id_ok   <- id[which(!is.na(tbl_TEMP$SHT21_T[id]) & !is.na(tbl_TEMP$SHT21_T_pred_LOO[id]))]
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
    
    xx        <- tbl_TEMP$SHT21_T[id_ok]
    yy        <- tbl_TEMP$SHT21_T_pred_LOO[id_ok]
    RES       <- yy-xx
    
    RMSE_CV    <- sqrt(sum((RES)^2)/n_id_ok)
    corCoef_CV <- cor(x=xx,y=yy,method="pearson",use="complete.obs")
    
    str01   <- paste("RMSE:  ", sprintf("%6.2f",RMSE_CV))
    str02   <- paste("COR:   ", sprintf("%6.2f",corCoef_CV))
    str03   <- paste("N:     ", sprintf("%6.0f",N_CV))
    xyrange <- range(c(xx,yy))
    
    mainStr   <- paste(u_SU_LOC_LocationName," ",u_SU_LOC_SensorUnit_ID,sep="")
    xlabStr01 <- expression(paste("T measurement [deg C]"))
    ylabStr01 <- expression(paste("T prediction LOO [deg C]"))
    xlabStr02 <- expression(paste("T prediction LOO - T measurement [deg C]"))
    
    if(xyrange[1]>800){
      xyrange[1] <- 380
    }
    if(xyrange[2]>800){
      xyrange[2] <- 800
    }
    
    plot(  xx,  yy,  pch=16,cex=0.75,cex.main=1.5,cex.lab=1.5,cex.axis=1.5,cex.sub=1.25,xlab=xlabStr01,ylab=ylabStr01,main=mainStr,sub=subStr,xlim=xyrange,ylim=xyrange)
    
    lines(c(-999,999),c(-999,999),col=2,lwd=1)
    
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
    
    
    hist_vv <- ((max(abs(RES))%/%0.25)+1)*0.25
    hist(RES,  seq(-hist_vv,hist_vv,0.25),xlim=c(-10,10),col="slategray",xlab=xlabStr02,main=mainStr,cex.main=1.5,cex.lab=1.5,cex.axis=1.5)
    
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



