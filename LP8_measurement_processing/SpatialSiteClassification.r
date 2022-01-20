# SpatialSiteClassification.r
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


## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

#

comp_mode <- args[1]

if(!comp_mode%in%c("DEPLOYMENT","GRID")){
  stop("comp_mode: DEPLOYMENT GRID")
}

### ----------------------------------------------------------------------------------------------------------------------------

print(paste(Sys.time(),"--> START"))

### ----------------------------------------------------------------------------------------------------------------------------

## directories

resultdir    <- "/project/CarboSense/Carbosense_Network/SpatialSiteClassification/"

if(comp_mode=="DEPLOYMENT"){
  result_fn <- paste(resultdir,"SpatialSiteClassification.csv",sep="")
}
if(comp_mode=="GRID"){
  result_fn <- paste(resultdir,"SpatialSiteClassification_GRID.csv",sep="")
}

### --------------------------------------------------------------------------------------------------------------------------

# file names, directories

topo_data_fn <- "/project/CarboSense/Data/Federal_Statistical_Office/gd-b-00.03-99-topotxt/Gelaendedaten.CSV"
lcov_data_fn <- "/project/CarboSense/Data/Federal_Statistical_Office/gd-b-00.03-37-nolc04P/AREA_NOLC04_27_161114.csv"

### --------------------------------------------------------------------------------------------------------------------------

if(comp_mode == "DEPLOYMENT"){
  
  ## Locations
  
  query_str       <- paste("SELECT * FROM Location;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tbl_location    <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  tbl_location <- tbl_location[tbl_location$Canton!="NA",]
  
  
  ##
  
  classification <- data.frame(LocationName      = tbl_location$LocationName,
                               Y_LV03            = tbl_location$Y_LV03,
                               X_LV03            = tbl_location$X_LV03,
                               h                 = tbl_location$h,
                               Canton            = tbl_location$Canton,
                               SensorUnit_ID     = NA,
                               HeightAboveGround = NA,
                               Inlet_HeightAboveGround = NA,
                               timestamp_from    = NA,
                               timestamp_to      = NA,
                               stringsAsFactors  = F)
  
  
  ## Actual Deployment
  
  query_str       <- paste("SELECT DISTINCT LocationName, SensorUnit_ID, HeightAboveGround,Inlet_HeightAboveGround, Date_UTC_from, Date_UTC_to FROM Deployment WHERE HeightAboveGround != -999;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tbl_deployment  <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  tbl_deployment$Date_UTC_from  <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_deployment$Date_UTC_to    <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_deployment$timestamp_from <- difftime(time1=tbl_deployment$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")
  tbl_deployment$timestamp_to   <- difftime(time1=tbl_deployment$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")
  
  for(ii in 1:dim(tbl_deployment)[1]){
    
    id <- which(tbl_deployment$LocationName[ii]==classification$LocationName 
                & is.na(classification$HeightAboveGround)
                & is.na(classification$Inlet_HeightAboveGround))
    
    if(tbl_deployment$SensorUnit_ID[ii] %in% c(426:445)){
      hh <- tbl_deployment$Inlet_HeightAboveGround[ii]
    }else{
      hh <- tbl_deployment$HeightAboveGround[ii]
    }
    
    classification <- rbind(classification,data.frame(LocationName      = classification$LocationName[id],
                                                      Y_LV03            = classification$Y_LV03[id],
                                                      X_LV03            = classification$X_LV03[id],
                                                      h                 = classification$h[id] + hh,
                                                      Canton            = classification$Canton[id],
                                                      SensorUnit_ID     = tbl_deployment$SensorUnit_ID[ii],
                                                      HeightAboveGround = tbl_deployment$HeightAboveGround[ii],
                                                      Inlet_HeightAboveGround = tbl_deployment$Inlet_HeightAboveGround[ii],
                                                      timestamp_from    = tbl_deployment$timestamp_from[ii],
                                                      timestamp_to      = tbl_deployment$timestamp_to[ii],
                                                      stringsAsFactors  = F))
  }
  
  print(paste(Sys.time(),"--> Deployment data structure completed."))
  
}

if(comp_mode == "GRID"){
  
  ## GRID - Locations
  
  ymin <- 673000
  ymax <- 692000
  xmin <- 238000
  xmax <- 256000
  
  xmax <- 248000 + 25000
  xmin <- 248000 - 25000
  ymax <- 683000 + 25000
  ymin <- 683000 - 25000
  
  # xmax <- 260000 + 10000
  # xmin <- 260000 - 10000
  # ymax <- 695000 + 10000
  # ymin <- 695000 - 10000
  
  nx   <- (xmax - xmin) / 100 + 1
  ny   <- (ymax - ymin) / 100 + 1
  
  
  
  classification <- data.frame(Y_LV03 = rep(seq(ymin,ymax,length.out=ny),each=nx),
                               X_LV03 = rep(seq(xmin,xmax,length.out=nx),ny),
                               h      = NA,
                               Canton = "XX",
                               stringsAsFactors = F)
  
}

##

n_sites  <- dim(classification)[1]

### --------------------------------------------------------------------------------------------------------------------------

## Add height to grid data

topo_data <- read.table(topo_data_fn,header=T,sep=";")

if(comp_mode == "GRID"){
  
  u_Y_LV03   <- sort(unique(classification$Y_LV03))
  n_u_Y_LV03 <- length(u_Y_LV03)
  
  min_X_LV03 <- min(classification$X_LV03)
  max_X_LV03 <- max(classification$X_LV03)
  
  for(ith_uc in 1:n_u_Y_LV03){
    
    id_topo_00 <- which(topo_data$KX+000==u_Y_LV03[ith_uc] & topo_data$KY-000 >= min_X_LV03 & topo_data$KY-000 <= max_X_LV03)
    # id_topo_01 <- which(topo_data$KX+000==u_Y_LV03[ith_uc] & topo_data$KY-100 >= min_X_LV03 & topo_data$KY-100 <= max_X_LV03) 
    # id_topo_02 <- which(topo_data$KX-100==u_Y_LV03[ith_uc] & topo_data$KY-000 >= min_X_LV03 & topo_data$KY-000 <= max_X_LV03) 
    # id_topo_03 <- which(topo_data$KX-100==u_Y_LV03[ith_uc] & topo_data$KY-100 >= min_X_LV03 & topo_data$KY-100 <= max_X_LV03) 
    
    id_topo_00 <- id_topo_00[order(topo_data$KY[id_topo_00])]
    # id_topo_01 <- id_topo_01[order(topo_data$KY[id_topo_01])]
    # id_topo_02 <- id_topo_02[order(topo_data$KY[id_topo_02])]
    # id_topo_03 <- id_topo_03[order(topo_data$KY[id_topo_03])]
    
    id_class <- which(classification$Y_LV03 == u_Y_LV03[ith_uc])
    id_class <- id_class[order(classification$X_LV03[id_class])]
    
    if(any(classification$Y_LV03[id_class]!=topo_data$KX[id_topo_00] | classification$X_LV03[id_class]!=topo_data$KY[id_topo_00] )){
      stop("ERROR: GRID, TOPO <-> CLASSIFICATION")
    }
    
    # classification$h[id_class] <- 0.25*(topo_data$HOEHE[id_topo_00]+topo_data$HOEHE[id_topo_01]+topo_data$HOEHE[id_topo_02]+topo_data$HOEHE[id_topo_03])
    
    classification$h[id_class] <- topo_data$HOEHE[id_topo_00]
  }
  
  print(paste(Sys.time(),"--> GRID data structure completed."))
  
}


### --------------------------------------------------------------------------------------------------------------------------

## Site classification due to topography

classification$TOPO_MIN_01000 <- NA
classification$TOPO_MAX_01000 <- NA
classification$TOPO_RNG_01000 <- NA
classification$TOPO_FRR_01000 <- NA
classification$TOPO_FAR_01000 <- NA

classification$TOPO_MIN_01750 <- NA
classification$TOPO_MAX_01750 <- NA
classification$TOPO_RNG_01750 <- NA
classification$TOPO_FRR_01750 <- NA
classification$TOPO_FAR_01750 <- NA

classification$TOPO_MIN_02500 <- NA
classification$TOPO_MAX_02500 <- NA
classification$TOPO_RNG_02500 <- NA
classification$TOPO_FRR_02500 <- NA
classification$TOPO_FAR_02500 <- NA

classification$TOPO_MIN_10000 <- NA
classification$TOPO_MAX_10000 <- NA
classification$TOPO_RNG_10000 <- NA
classification$TOPO_FRR_10000 <- NA
classification$TOPO_FAR_10000 <- NA

HH_max_01000 <- rep(NA,n_sites)
HH_max_01750 <- rep(NA,n_sites)
HH_max_02500 <- rep(NA,n_sites)
HH_max_10000 <- rep(NA,n_sites)


for(ith_site in 1:n_sites){
  
  dist <- (topo_data$KX-classification$Y_LV03[ith_site])^2 + (topo_data$KY-classification$X_LV03[ith_site])^2
  
  #
  
  id_closest <- which(dist==min(dist))
  
  #
  
  id   <- which(dist<=10000^2)
  n_id <- length(id)
  
  if(n_id>10){
    
    if(comp_mode == "DEPLOYMENT"){
      HH_max_10000[ith_site] <- max(c(max(topo_data$HOEHE[id_closest]),classification$h[ith_site]))
    }
    if(comp_mode == "GRID"){
      HH_max_10000[ith_site] <- classification$h[ith_site]
    }
    
    classification$TOPO_MIN_10000[ith_site] <- min(topo_data$HOEHE[id])
    classification$TOPO_MAX_10000[ith_site] <- max(topo_data$HOEHE[id])
    classification$TOPO_FAR_10000[ith_site] <- length(which(topo_data$HOEHE[id]<=classification$h[ith_site]))/n_id
  }
  
  #
  
  id   <- id[which(dist[id]<=2500^2)]
  n_id <- length(id)
  
  if(n_id>10){
    
    if(comp_mode == "DEPLOYMENT"){
      HH_max_02500[ith_site] <- max(c(max(topo_data$HOEHE[id_closest]),classification$h[ith_site]))
    }
    if(comp_mode == "GRID"){
      HH_max_02500[ith_site] <- classification$h[ith_site]
    }
    
    classification$TOPO_MIN_02500[ith_site] <- min(topo_data$HOEHE[id])
    classification$TOPO_MAX_02500[ith_site] <- max(topo_data$HOEHE[id])
    classification$TOPO_FAR_02500[ith_site] <- length(which(topo_data$HOEHE[id]<=classification$h[ith_site]))/n_id
  }
  
  #
  
  id   <- id[which(dist[id]<=1750^2)]
  n_id <- length(id)
  
  if(n_id>10){
    
    if(comp_mode == "DEPLOYMENT"){
      HH_max_01750[ith_site] <- max(c(max(topo_data$HOEHE[id_closest]),classification$h[ith_site]))
    }
    if(comp_mode == "GRID"){
      HH_max_01750[ith_site] <- classification$h[ith_site]
    }
    
    classification$TOPO_MIN_01750[ith_site] <- min(topo_data$HOEHE[id])
    classification$TOPO_MAX_01750[ith_site] <- max(topo_data$HOEHE[id])
    classification$TOPO_FAR_01750[ith_site] <- length(which(topo_data$HOEHE[id]<=classification$h[ith_site]))/n_id
  }
  
  #
  
  id   <- id[which(dist[id]<=1000^2)]
  n_id <- length(id)
  
  if(n_id>10){
    
    if(comp_mode == "DEPLOYMENT"){
      HH_max_01000[ith_site] <- max(c(max(topo_data$HOEHE[id_closest]),classification$h[ith_site]))
    }
    if(comp_mode == "GRID"){
      HH_max_01000[ith_site] <- classification$h[ith_site]
    }
    
    classification$TOPO_MIN_01000[ith_site] <- min(topo_data$HOEHE[id])
    classification$TOPO_MAX_01000[ith_site] <- max(topo_data$HOEHE[id])
    classification$TOPO_FAR_01000[ith_site] <- length(which(topo_data$HOEHE[id]<=classification$h[ith_site]))/n_id
  }
}

classification$TOPO_RNG_10000 <- classification$TOPO_MAX_10000 - classification$TOPO_MIN_10000
classification$TOPO_FRR_10000 <- (HH_max_10000 - classification$TOPO_MIN_10000)/classification$TOPO_RNG_10000
classification$TOPO_RNG_02500 <- classification$TOPO_MAX_02500 - classification$TOPO_MIN_02500
classification$TOPO_FRR_02500 <- (HH_max_02500 - classification$TOPO_MIN_02500)/classification$TOPO_RNG_02500
classification$TOPO_RNG_01750 <- classification$TOPO_MAX_01750 - classification$TOPO_MIN_01750
classification$TOPO_FRR_01750 <- (HH_max_01750 - classification$TOPO_MIN_01750)/classification$TOPO_RNG_01750
classification$TOPO_RNG_01000 <- classification$TOPO_MAX_01000 - classification$TOPO_MIN_01000
classification$TOPO_FRR_01000 <- (HH_max_01000 - classification$TOPO_MIN_01000)/classification$TOPO_RNG_01000


rm(topo_data,dist)
gc()

print(paste(Sys.time(),"--> Topography completed."))

### --------------------------------------------------------------------------------------------------------------------------

## Site classification due to land coverage

lcov_data <- read.table(lcov_data_fn,header=T,sep=",")

classification$LCOV_FRAC_SEALED_AREAS_02000 <- NA
classification$LCOV_FRAC_BUILDINGS_02000    <- NA
classification$LCOV_FRAC_WATER_02000        <- NA
classification$LCOV_FRAC_FOREST_02000       <- NA

classification$LCOV_FRAC_SEALED_AREAS_01000 <- NA
classification$LCOV_FRAC_BUILDINGS_01000    <- NA
classification$LCOV_FRAC_WATER_01000        <- NA
classification$LCOV_FRAC_FOREST_01000       <- NA

classification$LCOV_FRAC_SEALED_AREAS_00500 <- NA
classification$LCOV_FRAC_BUILDINGS_00500    <- NA
classification$LCOV_FRAC_WATER_00500        <- NA
classification$LCOV_FRAC_FOREST_00500       <- NA

for(ith_site in 1:n_sites){
  
  dist <- (lcov_data$X-classification$Y_LV03[ith_site])^2 + (lcov_data$Y-classification$X_LV03[ith_site])^2
  
  #
  
  id   <- which(dist<=2000^2)
  n_id <- length(id)
  
  if(n_id>10){
    classification$LCOV_FRAC_SEALED_AREAS_02000[ith_site] <- length(which(lcov_data$LC09_27[id]%in%c(11,12,13)))/n_id
    classification$LCOV_FRAC_BUILDINGS_02000[ith_site]    <- length(which(lcov_data$LC09_27[id]%in%c(12)))/n_id
    classification$LCOV_FRAC_WATER_02000[ith_site]        <- length(which(lcov_data$LC09_27[id]%in%c(61,63,64)))/n_id
    classification$LCOV_FRAC_FOREST_02000[ith_site]       <- length(which(lcov_data$LC09_27[id]%in%c(41:45)))/n_id
  }
  
  #
  
  id   <- id[which(dist[id]<=1000^2)]
  n_id <- length(id)
  
  if(n_id>10){
    classification$LCOV_FRAC_SEALED_AREAS_01000[ith_site] <- length(which(lcov_data$LC09_27[id]%in%c(11,12,13)))/n_id
    classification$LCOV_FRAC_BUILDINGS_01000[ith_site]    <- length(which(lcov_data$LC09_27[id]%in%c(12)))/n_id
    classification$LCOV_FRAC_WATER_01000[ith_site]        <- length(which(lcov_data$LC09_27[id]%in%c(61,63,64)))/n_id
    classification$LCOV_FRAC_FOREST_01000[ith_site]       <- length(which(lcov_data$LC09_27[id]%in%c(41:45)))/n_id
  }
  
  #
  
  id   <- id[which(dist[id]<=500^2)]
  n_id <- length(id)
  
  if(n_id>10){
    classification$LCOV_FRAC_SEALED_AREAS_00500[ith_site] <- length(which(lcov_data$LC09_27[id]%in%c(11,12,13)))/n_id
    classification$LCOV_FRAC_BUILDINGS_00500[ith_site]    <- length(which(lcov_data$LC09_27[id]%in%c(12)))/n_id
    classification$LCOV_FRAC_WATER_00500[ith_site]        <- length(which(lcov_data$LC09_27[id]%in%c(61,63,64)))/n_id
    classification$LCOV_FRAC_FOREST_00500[ith_site]       <- length(which(lcov_data$LC09_27[id]%in%c(41:45)))/n_id
  }
}

rm(lcov_data,dist)
gc()


print(paste(Sys.time(),"--> Land use completed."))

### --------------------------------------------------------------------------------------------------------------------------

## ZH site classes

SITE_IN_ZH_AREA <- classification$Canton=="ZH"

classification$ZH <- NA

for(i in 1:dim(classification)[1]){
  
  if(is.na(classification$TOPO_FAR_02500[i]) 
     | is.na(classification$TOPO_RNG_02500[i]) 
     | is.na(classification$LCOV_FRAC_SEALED_AREAS_00500[i])){
    next
  }
  
  if(SITE_IN_ZH_AREA[i] & classification$TOPO_FAR_02500[i] > 0.9 & classification$TOPO_RNG_02500[i] > 200){
    classification$ZH[i] <- "HILLTOP"
    next
  }
  if(SITE_IN_ZH_AREA[i] & classification$LCOV_FRAC_SEALED_AREAS_00500[i] >= 0.6){
    classification$ZH[i] <- "URBAN"
    next
  }
  if(SITE_IN_ZH_AREA[i] & classification$LCOV_FRAC_SEALED_AREAS_00500[i]  < 0.6 & classification$LCOV_FRAC_SEALED_AREAS_00500[i] >= 0.3){
    classification$ZH[i] <- "SUBURBAN"
    next
  }
  if(SITE_IN_ZH_AREA[i] & classification$LCOV_FRAC_SEALED_AREAS_00500[i]  < 0.3){
    classification$ZH[i] <- "RURAL"
    next
  }
}

## CH site classes

classification$CH <- NA

for(i in 1:dim(classification)[1]){
  
  if(is.na(classification$TOPO_FAR_02500[i]) 
     | is.na(classification$TOPO_RNG_02500[i]) 
     | is.na(classification$TOPO_MIN_02500[i])
     | is.na(classification$LCOV_FRAC_SEALED_AREAS_00500[i])){
    next
  }
  
  # if(!(classification$TOPO_FAR_02500[i] > 0.9 & classification$TOPO_RNG_02500[i] > 300) & (classification$h[i] - classification$TOPO_MIN_02500[i] > 400)){
  #   print(classification$LocationName[i])
  # }
  
  if((classification$TOPO_FAR_02500[i] > 0.9 & classification$TOPO_RNG_02500[i] > 300) | (classification$h[i] - classification$TOPO_MIN_02500[i] > 400)){
    classification$CH[i] <- "HILLTOP"
    next
  }
  if(classification$h[i] - classification$TOPO_MIN_02500[i] > 150){
    classification$CH[i] <- "ELEVATED"
    next
  }
  if(classification$TOPO_FAR_02500[i] < 0.1 & classification$TOPO_RNG_02500[i] > 300){
    classification$CH[i] <- "VALLEY"
    next
  }
  
  #
  
  if(classification$LCOV_FRAC_SEALED_AREAS_00500[i] >= 0.6){
    classification$CH[i] <- "URBAN"
    next
  }
  if(classification$LCOV_FRAC_SEALED_AREAS_00500[i]  < 0.6 & classification$LCOV_FRAC_SEALED_AREAS_00500[i] >= 0.3){
    classification$CH[i] <- "SUBURBAN"
    next
  }
  if(classification$LCOV_FRAC_SEALED_AREAS_00500[i]  < 0.3){
    classification$CH[i] <- "RURAL"
    next
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

write.table(x = classification,file = result_fn,col.names = T,row.names=F, sep=";")

### ----------------------------------------------------------------------------------------------------------------------------

if(comp_mode == "GRID"){
  
  data <- read.table(file = result_fn,header=T,sep=";")
  
  # MAP altitude / relative altitude
  
  id       <- which(data$Y_LV03>=670000 & data$Y_LV03<=690000 & data$X_LV03>=240000 & data$X_LV03 <= 260000 )
  data_tmp <- data[id,]
  
  figname  <- paste(resultdir,"MAP_Altitude_RelativeAltitude.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.5,0.5))
  
  for(i in 1:5){
    if(i==1){
      x       <- data_tmp$h
      mainStr <- paste("A [",sprintf("%.0f",min(x)),"m...",sprintf("%.0f",max(x)),"m]",sep="")
    }
    if(i==2){
      next
      x       <- data_tmp$h - data_tmp$TOPO_MIN_10000
      mainStr <- paste("A-MIN_R10000 [",sprintf("%.0f",min(x)),"m...",sprintf("%.0f",max(x)),"m]",sep="")
    }
    if(i==3){
      x       <- data_tmp$h - data_tmp$TOPO_MIN_02500
      mainStr <- paste("A-MIN_R2500 [",sprintf("%.0f",min(x)),"m...",sprintf("%.0f",max(x)),"m]",sep="")
    }
    if(i==4){
      x       <- data_tmp$h - data_tmp$TOPO_MIN_01750
      mainStr <- paste("A-MIN_R1750 [",sprintf("%.0f",min(x)),"m...",sprintf("%.0f",max(x)),"m]",sep="")
    }
    if(i==5){
      x       <- data_tmp$h - data_tmp$TOPO_MIN_01000
      mainStr <- paste("A-MIN_R1000 [",sprintf("%.0f",min(x)),"m...",sprintf("%.0f",max(x)),"m]",sep="")
    }
    
    x <- round((x-min(x))/(max(x)-min(x))*255)+1

    plot(data_tmp$Y_LV03,data_tmp$X_LV03,col=rainbow(256)[x],pch=16,cex=0.5,xlab="Easting [m]", ylab="Norhting [m]",main=mainStr,cex.lab=1.5,cex.axis=1.5,cex.main=1.5)
    
  }
  
  dev.off()
  par(def_par)
  
  
  
  # Transect
  
  Y_LV03   <- 680000
  id       <- which(data$Y_LV03>=Y_LV03 & data$Y_LV03<=Y_LV03 & data$X_LV03>=240000 & data$X_LV03 <= 260000 )
  data_tmp <- data[id,]
  
  figname  <- paste(resultdir,"Transect_Altitude_RelativeAltitude.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.1,0.1))
  
  plot(data_tmp$X_LV03,data_tmp$h,t="l",lty=1,col=1,ylim=c(0,max(data$h)),xlab="Northing [m] (@ easting=680000m)",ylab="Altitude [m]",cex.lab=1.5,cex.axis=1.5)
  
  lines(data_tmp$X_LV03,data_tmp$h-data_tmp$TOPO_MIN_01000,lty=1,col=2)
  lines(data_tmp$X_LV03,data_tmp$h-data_tmp$TOPO_MIN_01750,lty=1,col=3)
  lines(data_tmp$X_LV03,data_tmp$h-data_tmp$TOPO_MIN_02500,lty=1,col=4)
  lines(data_tmp$X_LV03,data_tmp$h-data_tmp$TOPO_MIN_10000,lty=1,col=5)
  
  lines(data_tmp$X_LV03,data_tmp$TOPO_MIN_01000,lty=5,col=2)
  lines(data_tmp$X_LV03,data_tmp$TOPO_MIN_01750,lty=5,col=3)
  lines(data_tmp$X_LV03,data_tmp$TOPO_MIN_02500,lty=5,col=4)
  lines(data_tmp$X_LV03,data_tmp$TOPO_MIN_10000,lty=5,col=5)
  
  par(family="mono")
  legend("topright",legend=c("Altitude","A-MIN_R1000","A-MIN_R1750","A-MIN_R2500","A-MIN_R10000"),bg="white",col=c(1,2,3,4,5),lty=1,cex=1.5)
  par(family="")
  
  dev.off()
  par(def_par)
  
  #
  
  figname  <- paste(resultdir,"Transect_FAR_FRR.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.1,0.1))
  
  plot( data_tmp$X_LV03, data_tmp$TOPO_FAR_02500,t="l",lty=1,col=1,ylim=c(0,1),xlab="Northing [m] (@ easting=680000m)",ylab="Altitude [m]",cex.lab=1.5,cex.axis=1.5)
  lines(data_tmp$X_LV03, data_tmp$TOPO_FRR_02500,lty=1,col=2)
  
  par(family="mono")
  legend("topright",legend=c("FAR 2500","FRR 2500"),bg="white",col=c(1,2),lty=1,cex=1.5)
  par(family="")
  
  dev.off()
  par(def_par)
  
  # MAP LCOV_FRAC_SEALED_AREAS_00500
  
  id       <- which(data$Y_LV03>=670000 & data$Y_LV03<=690000 & data$X_LV03>=240000 & data$X_LV03 <= 260000 )
  data_tmp <- data[id,]
  
  figname  <- paste(resultdir,"MAP_LCOV.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.5,0.5))
  
  for(i in 1:4){
    if(i==1){
      x       <- data_tmp$LCOV_FRAC_SEALED_AREAS_00500
      mainStr <- paste("LCOV_FRAC_SEALED_AREAS_00500",sep="")
    }
    if(i==2){
      x       <- data_tmp$LCOV_FRAC_BUILDINGS_00500
      mainStr <- paste("LCOV_FRAC_BUILDINGS_00500",sep="")
    }
    if(i==3){
      x       <- data_tmp$LCOV_FRAC_WATER_00500
      mainStr <- paste("LCOV_FRAC_WATER_00500",sep="")
    }
    if(i==4){
      x       <- data_tmp$LCOV_FRAC_FOREST_00500
      mainStr <- paste("LCOV_FRAC_FOREST_00500",sep="")
    }
    
    x <- round((x-min(x))/(max(x)-min(x))*255)+1
    
    plot(data_tmp$Y_LV03,data_tmp$X_LV03,col=heat.colors(256)[x],pch=16,cex=0.5,xlab="Easting [m]", ylab="Norhting [m]",main=mainStr,cex.lab=1.5,cex.axis=1.5,cex.main=1.5)
    
  }
  
  dev.off()
  par(def_par)
}

#

     