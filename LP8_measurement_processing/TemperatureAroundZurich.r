# TemperatureAroundZurich.r
# --------------------------------
#
# Author: Michael Mueller
#
# Task: 
# - Daily map of temperature distribution of SHT21 sensors and MCH instruments in the early morning.
# - Daily time series of temperature of SHT21 sensors and MCH instruments
# 
# Remarks:
# - Computations refer to UTC.
#
# --------------------------------

## clear variables
rm(list=ls(all=TRUE))
gc()

## libraries
library(DBI)
require(RMySQL)
require(chron)
library(raster)
library(rgdal)
library(maptools)
library(fields)

### ----------------------------------------------------------------------------------------------------------------------------

# EMPA CarboSense DB information

CS_DB_dbname <- "CarboSense"
CS_DB_user   <- ""
CS_DB_pass   <- ""

### ----------------------------------------------------------------------------------------------------------------------------

# directories

plot_directory <- "/project/CarboSense/Carbosense_Network/TemperatureAroundZurich/"

### ----------------------------------------------------------------------------------------------------------------------------

# Date interval, time of day

Date_from       <- strptime("20170701000000","%Y%m%d%H%M%S",tz="UTC")
Date_to         <- strptime("20200501000000","%Y%m%d%H%M%S",tz="UTC")

# Date_from       <- strptime("20190901000000","%Y%m%d%H%M%S",tz="UTC")
# Date_to         <- strptime("20191001000000","%Y%m%d%H%M%S",tz="UTC")

timestamp_from  <- as.numeric(difftime(time1=Date_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
timestamp_to    <- as.numeric(difftime(time1=Date_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

timestamp_seq   <- seq(timestamp_from,timestamp_to,86400)
n_timestamp_seq <- length(timestamp_seq)


### ----------------------------------------------------------------------------------------------------------------------------

# Geodata for map 

# Extent

X_min <- 237000
X_max <- 262000
Y_min <- 669500
Y_max <- 694500

bb    <- extent(x=c(Y_min-1e4,Y_max+1e4,X_min-1e4,X_max+1e4))

# DHM

proj_string     <- "+proj=somerc +lat_0=46.95240555555556 +lon_0=7.439583333333333 +k_0=1 +x_0=600000 +y_0=200000 +ellps=bessel +units=m +no_defs"

tmp             <- read.table("/project/CarboSense/Data/Federal_Statistical_Office/gd-b-00.03-99-topotxt/Gelaendedaten.CSV",sep=";",header=T)

dhm             <- rasterFromXYZ(tmp[,1:3])

projection(dhm) <- proj_string

# CH
ch_area           <- readOGR("/project/CarboSense/Data/Swisstopo/swissBOUNDARIES3D_1_1_TLM_HOHEITSGEBIET_SHAPEFILE_LV03_LN02","swissBOUNDARIES3D_1_1_TLM_HOHEITSGEBIET")
ch_cantons        <- readOGR("/project/CarboSense/Data/Swisstopo/BOUNDARIES_2013/swissBOUNDARIES3D/SHAPEFILE_LV03_LN02","swissBOUNDARIES3D_1_1_TLM_KANTONSGEBIET")
ch_communities    <- readOGR("/project/CarboSense/Data/Swisstopo/BOUNDARIES_2013/swissBOUNDARIES3D/SHAPEFILE_LV03_LN02","swissBOUNDARIES3D_1_1_TLM_BEZIRKSGEBIET")
ch_communities_zh <- ch_communities[ch_communities$BEZIRKSNUM==0112,]

# Lakes
ZH_PRI_AREA        <- readOGR("/project/CarboSense/Data/Swisstopo/Vector25/Vector25_ZH","pri_25_a")
ZH_PRI_LINES       <- readOGR("/project/CarboSense/Data/Swisstopo/Vector25/Vector25_ZH","pri_25_l")

ZH_PRI_AREA_LAKES  <-  ZH_PRI_AREA[ZH_PRI_AREA$OBJECTVAL=="Z_See",]
ZH_PRI_AREA_FOREST <-  ZH_PRI_AREA[ZH_PRI_AREA$OBJECTVAL=="Z_Wald",]
ZH_PRI_AREA_SEALED <-  ZH_PRI_AREA[ZH_PRI_AREA$OBJECTVAL=="Z_Siedl",]

ZH_PRI_LINES_RIVERS <-  ZH_PRI_LINES[ZH_PRI_LINES$OBJECTVAL=="Fluss_Li",]                  

# DHM 
dhm             <- crop(x=dhm,y=bb)
slope           <- terrain(x=dhm,opt="slope", unit="radians")
aspect          <- terrain(x=dhm,opt="aspect",unit="radians")
hs              <- hillShade(slope, aspect, angle=45, direction=-45)

ch_area_raster <- rasterize(x=ch_area,dhm)
dhm[which(is.na(ch_area_raster[1:length(ch_area_raster)]))] <- NA
hs[which(is.na(ch_area_raster[1:length(ch_area_raster)]))] <- NA

rm(slope,aspec,tmp)
gc()

### ----------------------------------------------------------------------------------------------------------------------------

# Locations in Zurich

query_str       <- paste("SELECT * FROM Location WHERE Canton = 'ZH';",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_location    <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

LocationNamesZH_SQL_STR <- paste("('",paste(tbl_location$LocationName,collapse = "','",sep=""),"')",sep="")
LocationNamesZH         <- tbl_location$LocationName

### ----------------------------------------------------------------------------------------------------------------------------

# Loop

for(ith_timestamp in 1:n_timestamp_seq){
  
  
  # interval timestamps
  
  timestamp_int_24h_from     <- timestamp_seq[ith_timestamp] + 2*3600 - 12 * 3600
  timestamp_int_24h_to       <- timestamp_seq[ith_timestamp] + 2*3600 + 12 * 3600
  
  timestamp_LP8_int_24h_from <- timestamp_seq[ith_timestamp] - 2*3600
  timestamp_LP8_int_24h_to   <- timestamp_seq[ith_timestamp] + 4*3600 
  
  date_int_24h_from          <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + timestamp_int_24h_from
  date_int_24h_to            <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + timestamp_int_24h_to
  
  timestamp_int_av_from      <- timestamp_seq[ith_timestamp] + 2*3600
  timestamp_int_av_to        <- timestamp_seq[ith_timestamp] + 2*3600 + 600
  
  ### ---------------------------------
  
  # DST
  
  DST_2017_timestamp_from <- as.numeric(difftime(time1=strptime("26.03.2017 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
  DST_2017_timestamp_to   <- as.numeric(difftime(time1=strptime("29.10.2017 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
  
  DST_2018_timestamp_from <- as.numeric(difftime(time1=strptime("25.03.2018 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
  DST_2018_timestamp_to   <- as.numeric(difftime(time1=strptime("28.10.2018 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
  
  DST_2019_timestamp_from <- as.numeric(difftime(time1=strptime("31.03.2019 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
  DST_2019_timestamp_to   <- as.numeric(difftime(time1=strptime("27.10.2019 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
  
  DST_2020_timestamp_from <- as.numeric(difftime(time1=strptime("29.03.2020 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
  DST_2020_timestamp_to   <- as.numeric(difftime(time1=strptime("25.10.2019 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
  
  DST <- 0
  
  if(timestamp_seq[ith_timestamp]>=DST_2017_timestamp_from & timestamp_seq[ith_timestamp]<DST_2017_timestamp_to){
    DST <- 1
  }
  if(timestamp_seq[ith_timestamp]>=DST_2018_timestamp_from & timestamp_seq[ith_timestamp]<DST_2018_timestamp_to){
    DST <- 1
  }
  if(timestamp_seq[ith_timestamp]>=DST_2019_timestamp_from & timestamp_seq[ith_timestamp]<DST_2019_timestamp_to){
    DST <- 1
  }
  if(timestamp_seq[ith_timestamp]>=DST_2020_timestamp_from & timestamp_seq[ith_timestamp]<DST_2020_timestamp_to){
    DST <- 1
  }
  
  ### ---------------------------------
  
  
  # LP8 data
  
  query_str       <- paste("SELECT timestamp, SensorUnit_ID, LocationName, SHT21_T FROM CarboSense_T_RH ",sep="")
  query_str       <- paste(query_str,"WHERE timestamp >= ",timestamp_int_24h_from," AND timestamp <= ",timestamp_int_24h_to," ",sep="")
  query_str       <- paste(query_str,"AND SHT21_T != -999 ",sep="")
  query_str       <- paste(query_str,"AND LocationName IN ",LocationNamesZH_SQL_STR,";",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  LP8_data        <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  if(dim(LP8_data)[1]==0){
    next
  }
  
  u_LP8_SU_LOC    <- unique(data.frame(SensorUnit_ID = LP8_data$SensorUnit_ID,
                                       LocationName  = LP8_data$LocationName,
                                       stringsAsFactors = F))
  
  n_u_LP8_SU_LOC  <- dim(u_LP8_SU_LOC)[1]
  
  
  u_LP8_SU_LOC$Y_LV03       <- NA
  u_LP8_SU_LOC$X_LV03       <- NA
  u_LP8_SU_LOC$h            <- NA
  u_LP8_SU_LOC$SHT21_T_comp <- NA
  
  
  for(ith_LP8_SU_LOC in 1:n_u_LP8_SU_LOC){
    id_loc <- which(u_LP8_SU_LOC$LocationName[ith_LP8_SU_LOC]==tbl_location$LocationName)
    
    id_LP8_data <- which(LP8_data$LocationName    == u_LP8_SU_LOC$LocationName[ith_LP8_SU_LOC]
                         & LP8_data$SensorUnit_ID == u_LP8_SU_LOC$SensorUnit_ID[ith_LP8_SU_LOC]
                         & LP8_data$timestamp >= timestamp_int_av_from
                         & LP8_data$timestamp <  timestamp_int_av_to)
    
    u_LP8_SU_LOC$Y_LV03[ith_LP8_SU_LOC] <- tbl_location$Y_LV03[id_loc]
    u_LP8_SU_LOC$X_LV03[ith_LP8_SU_LOC] <- tbl_location$X_LV03[id_loc]
    u_LP8_SU_LOC$h[ith_LP8_SU_LOC]      <- tbl_location$h[id_loc]
    
    
    if(length(id_LP8_data)>=1){
      u_LP8_SU_LOC$SHT21_T_comp[ith_LP8_SU_LOC] <- mean(LP8_data$SHT21_T[id_LP8_data])
    }
  }
  
  ### ---------------------------------
  
  ## MCH data
  
  query_str       <- paste("SELECT timestamp, LocationName, Temperature FROM METEOSWISS_Measurements ",sep="")
  query_str       <- paste(query_str,"WHERE timestamp >= ",timestamp_int_24h_from," and timestamp <= ",timestamp_int_24h_to," ",sep="")
  query_str       <- paste(query_str,"AND LocationName IN ('KLO','REH','SMA','LAE','NABDUE','NABZUE') ",sep="")
  query_str       <- paste(query_str,"AND Temperature != -999; ",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  MCH_data        <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  
  u_MCH_LOC    <- data.frame(LocationName=sort(unique(MCH_data$LocationName)),stringsAsFactors = F)
  
  #
  
  u_MCH_LOC$LocationName_ED <- u_MCH_LOC$LocationName
  u_MCH_LOC$LocationName_ED[which(u_MCH_LOC$LocationName_ED=="NABZUE")] <- "ZUE"
  u_MCH_LOC$LocationName_ED[which(u_MCH_LOC$LocationName_ED=="NABDUE")] <- "DUE"
  
  u_MCH_LOC$col <-NA
  
  
  
  n_u_MCH_LOC  <- dim(u_MCH_LOC)[1]
  
  u_MCH_LOC$Y_LV03 <- NA
  u_MCH_LOC$X_LV03 <- NA
  u_MCH_LOC$h      <- NA
  u_MCH_LOC$Temperature_comp <- NA
  
  
  for(ith_MCH_LOC in 1:n_u_MCH_LOC){
    
    id_loc      <- which(u_MCH_LOC$LocationName[ith_MCH_LOC]==tbl_location$LocationName)
    
    id_MCH_data <- which(MCH_data$LocationName==u_MCH_LOC$LocationName[ith_MCH_LOC]
                         & MCH_data$timestamp >= timestamp_int_av_from
                         & MCH_data$timestamp <  timestamp_int_av_to)
    
    u_MCH_LOC$Y_LV03[ith_MCH_LOC] <- tbl_location$Y_LV03[id_loc]
    u_MCH_LOC$X_LV03[ith_MCH_LOC] <- tbl_location$X_LV03[id_loc]
    u_MCH_LOC$h[ith_MCH_LOC]      <- tbl_location$h[id_loc]
    
    
    if(length(id_MCH_data)>=1){
      u_MCH_LOC$Temperature_comp[ith_MCH_LOC] <- mean(MCH_data$Temperature[id_MCH_data])
    }
    
    if(u_MCH_LOC$LocationName[ith_MCH_LOC]=="KLO"){
      u_MCH_LOC$col[ith_MCH_LOC] <- 2
    }
    if(u_MCH_LOC$LocationName[ith_MCH_LOC]=="REH"){
      u_MCH_LOC$col[ith_MCH_LOC] <- 3
    }
    if(u_MCH_LOC$LocationName[ith_MCH_LOC]=="LAE"){
      u_MCH_LOC$col[ith_MCH_LOC] <- 1
    }
    if(u_MCH_LOC$LocationName[ith_MCH_LOC]=="SMA"){
      u_MCH_LOC$col[ith_MCH_LOC] <- 5
    }
    if(u_MCH_LOC$LocationName[ith_MCH_LOC]=="NABDUE"){
      u_MCH_LOC$col[ith_MCH_LOC] <- 6
    }
    if(u_MCH_LOC$LocationName[ith_MCH_LOC]=="NABZUE"){
      u_MCH_LOC$col[ith_MCH_LOC] <- 4
    }
  }
  

  ### ---------------------------------
  
  # Relative temperatures, colors, ...
  
  id_MCH_REF    <- which(u_MCH_LOC$LocationName == "KLO")
  id_MCH_notREF <- which(u_MCH_LOC$LocationName != "KLO")
  
  if(!is.na(u_MCH_LOC$Temperature_comp[id_MCH_REF])){
    
    u_MCH_LOC$Temperature_comp_relToREF <- u_MCH_LOC$Temperature_comp - u_MCH_LOC$Temperature_comp[id_MCH_REF]
    u_LP8_SU_LOC$SHT21_T_comp_relToREF  <- u_LP8_SU_LOC$SHT21_T_comp  - u_MCH_LOC$Temperature_comp[id_MCH_REF]
    
    Temperature_range     <- range(c(u_LP8_SU_LOC$SHT21_T_comp,u_MCH_LOC$Temperature_comp),na.rm=T) 
    Temperature_rel_range <- range(c(u_LP8_SU_LOC$SHT21_T_comp_relToREF,u_MCH_LOC$Temperature_comp_relToREF),na.rm=T) 
    
    cr <- colorRampPalette(c("blue","purple","white","yellow","red"))
    cr_colors <- cr(81)
    
    tmp <- u_LP8_SU_LOC$SHT21_T_comp_relToREF
    tmp[which(tmp < -10)] <- -10
    tmp[which(tmp >  10)] <-  10
    
    u_LP8_SU_LOC$Temperature_rel_color <- cr_colors[(10 + tmp)%/%0.25 + 1]
    
    
    tmp <- u_MCH_LOC$Temperature_comp_relToREF
    tmp[which(tmp < -10)] <- -10
    tmp[which(tmp >  10)] <-  10
    
    u_MCH_LOC$Temperature_rel_color <- cr_colors[(10 + tmp)%/%0.25 + 1]
    
  }else{
    next
  }
  
  ### ---------------------------------
  
  # Time series
  
  Date_map     <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+timestamp_seq[ith_timestamp] + 2*3600
  Date_map_str <- strftime(Date_map + 3600 + 3600*DST,"%Y-%m-%d %H:%M",tz="UTC") 
  Date_map_fn  <- strftime(Date_map,"%Y%m%d_%H%M",tz="UTC") 
  
  T_range      <- range(c(MCH_data$Temperature,LP8_data$SHT21_T)) + c(-5,10)
  
  fn           <- paste(plot_directory,"ZH_Temperature_",Date_map_fn,"_TS.pdf",sep="")
    
  def_par <- par()
  pdf(file = fn, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.5,0.1))
  
  plot(c(date_int_24h_from + 3600 + 3600*DST,date_int_24h_to + 3600 + 3600*DST),c(-999,-999),ylim=T_range,main=paste(Date_map_str,"[LT]"),xlab="Date [LT]", ylab=expression(paste("Temperature [",degree,"C]")),xaxt="n",cex.axis=1.5,cex.lab=1.5,cex.main=1.5)
  x_lab <- seq(date_int_24h_from + 3600 + 3600*DST,date_int_24h_to + 3600 + 3600*DST,"4 hours")
  x_lab_str <- strftime(x_lab,"%H:%M",tz="UTC")
  axis(side = 1,at = x_lab,labels = x_lab_str,cex.axis=1.5,cex.lab=1.5)
  
  hels <- matrix(c(8.54015,47.37784), nrow=1)
  Hels <- SpatialPoints(hels, proj4string=CRS("+proj=longlat +datum=WGS84"))
  
  sunset_date  <- sunriset(Hels, date_int_24h_from,direction="sunset", POSIXct.out=TRUE)
  sunrise_date <- sunriset(Hels, date_int_24h_to,  direction="sunrise",POSIXct.out=TRUE)

   
  polygon(c(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+timestamp_int_av_from + 3600 + 3600*DST,
            strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+timestamp_int_av_to + 3600 + 3600*DST,
            strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+timestamp_int_av_to + 3600 + 3600*DST,
            strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+timestamp_int_av_from + 3600 + 3600*DST),c(-1e3,-1e3,1e3,1e3),
          col="salmon",border=NA)
  
  polygon(c(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
            sunset_date$time + 3600 + 3600*DST,
            sunset_date$time + 3600 + 3600*DST,
            strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")),c(-1e3,-1e3,1e3,1e3),
          col="beige",border=NA)
  
  polygon(c(strptime("20700101000000","%Y%m%d%H%M%S",tz="UTC"),
            sunrise_date$time + 3600 + 3600*DST,
            sunrise_date$time + 3600 + 3600*DST,
            strptime("20700101000000","%Y%m%d%H%M%S",tz="UTC")),c(-1e3,-1e3,1e3,1e3),
          col="beige",border=NA)
  
  for(ith_LP8_SU_LOC in 1:n_u_LP8_SU_LOC){
    
    id_LP8_data <- which(LP8_data$LocationName    == u_LP8_SU_LOC$LocationName[ith_LP8_SU_LOC]
                         & LP8_data$SensorUnit_ID == u_LP8_SU_LOC$SensorUnit_ID[ith_LP8_SU_LOC]
                         & LP8_data$timestamp >= timestamp_int_24h_from
                         & LP8_data$timestamp <  timestamp_int_24h_to)
    
    xx <- LP8_data$timestamp[id_LP8_data] + 3600 + 3600*DST
    yy <- LP8_data$SHT21_T[id_LP8_data]
    
    id <- which(diff(xx)>900)
    if(length(id)>0){
      xx <- c(xx,xx[id]+900)
      yy <- c(yy,rep(NA,length(id)))
      oo <- order(xx)
      xx <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + xx[oo]
      yy <- yy[oo]
    }

    lines(xx,yy,col="gray50",lwd=1)
  }
  
  
  for(ith_MCH_LOC in 1:n_u_MCH_LOC){

    id_MCH_data <- which(MCH_data$LocationName==u_MCH_LOC$LocationName[ith_MCH_LOC]
                         & MCH_data$timestamp >= timestamp_int_24h_from
                         & MCH_data$timestamp <  timestamp_int_24h_to)
    
    xx <- MCH_data$timestamp[id_MCH_data] + 3600 + 3600*DST
    yy <- MCH_data$Temperature[id_MCH_data]
    
    id <- which(diff(xx)>900)
    if(length(id)>0){
      xx <- c(xx,xx[id]+900)
      yy <- c(yy,rep(NA,length(id)))
      oo <- order(xx)
      xx <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + xx[oo]
      yy <- yy[oo]
    }

    lines(xx,yy,col=u_MCH_LOC$col[ith_MCH_LOC],lwd=2)
  }
  
  par(family="mono")
  legend("topleft",legend=u_MCH_LOC$LocationName_ED,lty=1,lwd=2,col=u_MCH_LOC$col,bg="white",cex=1.5)
  par(family="")
  
  dev.off()
  par(def_par)
  
  ### ---------------------------------
  
  # Plot temperature versus altitude
  
  u_MCH_LOC$Temperature_comp_relToREF <- u_MCH_LOC$Temperature_comp - u_MCH_LOC$Temperature_comp[id_MCH_REF]
  u_LP8_SU_LOC$SHT21_T_comp_relToREF  <- u_LP8_SU_LOC$SHT21_T_comp  - u_MCH_LOC$Temperature_comp[id_MCH_REF]
  
  Date_map     <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+timestamp_seq[ith_timestamp] + 2*3600
  Date_map_str <- strftime(Date_map + 3600 + 3600*DST,"%Y-%m-%d %H:%M",tz="UTC") 
  Date_map_fn  <- strftime(Date_map,"%Y%m%d_%H%M",tz="UTC") 
  
  fn           <- paste(plot_directory,"ZH_Temperature_",Date_map_fn,"_T_vs_Alt.pdf",sep="")
  
  def_par <- par()
  pdf(file = fn, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.5,0.1))
  
  id_LP8 <- which(!is.na(u_LP8_SU_LOC$SHT21_T_comp))
  id_MCH <- which(!is.na(u_MCH_LOC$Temperature_comp))
  
  xx     <- c(u_MCH_LOC$h[id_MCH],u_LP8_SU_LOC$h[id_LP8])
  yy     <- c(u_MCH_LOC$Temperature_comp[id_MCH],u_LP8_SU_LOC$SHT21_T_comp[id_LP8])
  pch    <- c(rep(22,length(id_MCH)),rep(21,length(id_LP8)))
  pcol   <- c(u_MCH_LOC$Temperature_rel_color[id_MCH],u_LP8_SU_LOC$Temperature_rel_color[id_LP8])
  
  xrange <- range(xx)
  yrange <- c(u_MCH_LOC$Temperature_comp[id_MCH_REF]-10,u_MCH_LOC$Temperature_comp[id_MCH_REF]+10)
  
  plot(NA,NA,xlim=xrange,ylim=yrange, 
       main=paste(Date_map_str,"[local time]"),ylab=expression(paste("Temperature [",degree,"C]")),xlab="Altitude [m a.s.l.]",
       cex.lab=1.5,cex.axis=1.5,cex.main=1.5)
  
  lines(c(-1e4,1e4),c(u_MCH_LOC$Temperature_comp[id_MCH_REF],u_MCH_LOC$Temperature_comp[id_MCH_REF]),lwd=1,col=1)
  lines(c(u_MCH_LOC$h[id_MCH_REF],u_MCH_LOC$h[id_MCH_REF]),c(-1e4,1e4),lwd=1,col=1)
  
  points(xx,yy,xlim=xrange,ylim=yrange,pch=pch,col=1,bg=pcol,lwd=1,cex=1.5)
  
  par(family="mono")
  legend("bottomleft",legend=c("Carbosense","MeteoSwiss/NABEL"),pch=c(21,22),cex=1.5,col=c(1,1),pt.bg = c("white","white"),pt.cex = 1,bg="white")
  par(family="")
  
  dev.off()
  par(def_par)
  
  
  
  ### ---------------------------------
  
  # MAP 
  
  Date_map     <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+timestamp_seq[ith_timestamp] + 2*3600
  Date_map_str <- strftime(Date_map + 3600 + 3600*DST,"%Y-%m-%d %H:%M",tz="UTC") 
  Date_map_fn  <- strftime(Date_map,"%Y%m%d_%H%M",tz="UTC") 
  
  fn           <- paste(plot_directory,"ZH_Temperature_",Date_map_fn,"_map.png",sep="")
  
  def_par <- par()
  png(file = fn, width = 960, height=960, pointsize=12,units="px")
  
  par(mai=c(1.0,1.0,0.1,0.1))
  
  colfunc     <- colorRampPalette(c("gray20","gray100"))
  
  colors_used <- colfunc(100)
  
  # plot(hs, col=grey(0:100/100), legend=FALSE, xlim=c(Y_min,Y_max),ylim=c(X_min,X_max), main='',ylab="",xlab="",xaxt="n",yaxt="n",bty="n",axes=FALSE, box=FALSE)
  
  
  plot(NA,NA,xlim=c(Y_min+1e3,Y_max-1e3),ylim=c(X_min+1e3,X_max-1e3), main='',ylab="Northing [m]",xlab="Easting [m]",cex.lab=2.25,cex.axis=2.25)
  plot(hs, col=grey(0:100/100), legend=FALSE,add=T)
  plot(dhm, col=colors_used,alpha=0.6, xlim=c(Y_min,Y_max),ylim=c(X_min,X_max),legend=FALSE,add=T)
  # 
  plot(ZH_PRI_AREA_LAKES, add=T,col="lightskyblue",border=NA, xlim=c(Y_min,Y_max),ylim=c(X_min,X_max))

  plot(ch_communities_zh, add=T,col=NA,border="red", xlim=c(Y_min,Y_max),ylim=c(X_min,X_max),lwd=2)

  # MCH REF
  points(u_MCH_LOC$Y_LV03[id_MCH_REF],u_MCH_LOC$X_LV03[id_MCH_REF], cex=5,pch=22,col=2,lwd=3,bg=u_MCH_LOC$Temperature_rel_color[id_MCH_REF])

  # MCH data
  points(u_MCH_LOC$Y_LV03[id_MCH_notREF],u_MCH_LOC$X_LV03[id_MCH_notREF], cex=5,pch=22,col=1,lwd=1,bg=u_MCH_LOC$Temperature_rel_color[id_MCH_notREF])
  
  # LP8 data
  id <- which(!is.na(u_LP8_SU_LOC$SHT21_T_comp))
  points(u_LP8_SU_LOC$Y_LV03[id],u_LP8_SU_LOC$X_LV03[id],cex=4,pch=21, col=1,lwd=1,bg=u_LP8_SU_LOC$Temperature_rel_color[id])
  
  tmp1       <- sprintf("%4.1f",u_MCH_LOC$Temperature_comp[id_MCH_REF])
  tmp2       <- paste(sprintf("%.1f",Temperature_rel_range[1]))
  tmp3       <- paste(sprintf("%.1f",Temperature_rel_range[2]))
  
  leg_str_00 <- bquote(T~.("@")~KLO: ~ .(tmp1) ~ degree*C)
  leg_str_01 <- bquote(Delta ~ T: ~ .(tmp2) ~ degree*C ~ - ~ .(tmp3) ~ degree*C)

  polygon(c(Y_max-9.0e3,Y_max-0.3e3,Y_max-0.3e3,Y_max-9.0e3),
          c(X_min+3.4e3,X_min+3.4e3,X_min+0.3e3,X_min+0.3e3),col="white",border=1,lwd=1)
  
  par(family="mono")
  text(Y_max-8.8e3,X_min+2.8e3,paste(Date_map_str,"[LT]"),cex=2.0,pos=4)
  text(Y_max-8.8e3,X_min+1.8e3,leg_str_00,                cex=2.0,pos=4)
  text(Y_max-8.8e3,X_min+0.8e3,leg_str_01,                cex=2.0,pos=4)
  par(family="")
  
  
  polygon(c(Y_min+8.6e3,Y_min+0.3e3,Y_min+0.3e3,Y_min+8.6e3),
          c(X_min+3.4e3,X_min+3.4e3,X_min+0.3e3,X_min+0.3e3),col="white",border=1,lwd=1)
  
  colorbar.plot(Y_min+4.5e3, X_min+1e3,strip = 1:81,horizontal = T,col = cr(80),strip.width = 0.03, strip.length = 0.3)
  
  par(family="mono")
  text(Y_min+1.2e3, X_min+1.9e3,"-10", cex=2.0,col=1)
  text(Y_min+4.5e3, X_min+1.9e3,"0",   cex=2.0,col=1)
  text(Y_min+7.8e3, X_min+1.9e3,"10",  cex=2.0,col=1)
  text(Y_min+4.5e3, X_min+2.7e3,expression(paste(Delta,"T [",degree,"C]")), cex=2.0,col=1)
  par(family="")
  
  text(u_MCH_LOC$Y_LV03+400,u_MCH_LOC$X_LV03+300,u_MCH_LOC$LocationName_ED,cex=2.5,col=u_MCH_LOC$col,pos=4,font=2)
  
  par(family="mono")
  legend("topright",legend=c("Carbosense","MeteoSwiss/NABEL"),pch=c(21,22),cex=2,col=c(1,1),pt.bg = c("white","white"),pt.cex = 2.0,bg="white")
  par(family="")
  
  dev.off()
  par(def_par)

}