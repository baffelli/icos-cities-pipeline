# Plot_Traffic_Maps.r
# --------------------------------
#
# Author: Michael Mueller
#
#
# --------------------------------

# Remarks:
# - Computations refer to UTC.
#
#

## DO NOT USE WITHOUT REVIEW !
## - issues e.g. public holidays, local time

## clear variables
rm(list=ls(all=TRUE))
gc()

library(raster)
library(rgdal)

### ----------------------------------------------------------------------------------------------------------------------------

args = commandArgs(trailingOnly=TRUE)

selected_version <- args[1]

if(!selected_version%in%c(1)){
  stop()
}

### ----------------------------------------------------------------------------------------------------------------------------

# Directories, file names

if(selected_version==1){
  ref_traffic_fn <- "/project/CarboSense/Statistical_Models/Covid-19_ZH/Results/referencePeriodTraffic.csv"
  
  if(!file.exists(ref_traffic_fn)){
    stop()
  }
}

figure_dir       <- paste("/project/CarboSense/Statistical_Models/Covid-19_ZH/Results/TrafficCountMaps/",sep="")

swissimage_fn    <- "/project/CarboSense/Data/Swisstopo/SWISSIMAGE25m/SWISSIMAGE25m/SI25-2012-2013-2014.tif"

HOHEITSGEBIET_fn <- "/project/CarboSense/Data/Swisstopo/BOUNDARIES_2013/swissBOUNDARIES3D/SHAPEFILE_LV95_LN02"


if(!dir.exists(figure_dir)){
  dir.create(figure_dir)
}


### --------------------------------------------------------------------------------------------------------------

# MAP Parameters: Time period

timestamp_start <- as.numeric(difftime(time1=strptime("2020-03-17 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))

timestamp_end   <- as.numeric(difftime(time1=strptime("2020-03-22 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))


### --------------------------------------------------------------------------------------------------------------

fn_traffic_2020 <- "/project/CarboSense/Statistical_Models/Covid-19_ZH/Data/Traffic/sid_dav_verkehrszaehlung_miv_od2031_2020.csv"

traffic_data    <- NULL
traffic_data    <- rbind(traffic_data, read.table(file = fn_traffic_2020,header = T,sep = ",",as.is = T))

traffic_data$date      <- strptime(traffic_data$MessungDatZeit,"%Y-%m-%dT%H:%M:%S",tz="UTC")
traffic_data$year      <- as.numeric(strftime(traffic_data$date,"%Y",tz="UTC"))
traffic_data$month     <- as.numeric(strftime(traffic_data$date,"%m",tz="UTC"))
traffic_data$day       <- as.numeric(strftime(traffic_data$date,"%d",tz="UTC"))
traffic_data$hour      <- as.numeric(strftime(traffic_data$date,"%H",tz="UTC"))
traffic_data$dow       <- as.numeric(strftime(traffic_data$date,"%w",tz="UTC"))
traffic_data$timestamp <- as.numeric(difftime(time1=traffic_data$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units ="secs",tz="UTC"))

traffic_data$AnzFahrzeugeStatus_TF <- traffic_data$AnzFahrzeugeStatus== "Gemessen"

#

ref_traffic <- read.table(ref_traffic_fn,header=T,sep=";",as.is=T)

u_ZSName    <- sort(unique(ref_traffic$ZSName))
n_u_ZSName  <- length(u_ZSName)


###

timestamp_end <- max(traffic_data$timestamp)

### --------------------------------------------------------------------------------------------------------------

# base map

photo <- brick(x = swissimage_fn,
               xmn = 2484387.5,
               xmx = 2484387.5 + 14000*25,
               ymn = 1298987.5 -  9480*25,
               ymx = 1298987.5,
               crs = "+proj=somerc +lat_0=46.95240555555556 +lon_0=7.439583333333333 +k_0=1 +x_0=2600000 +y_0=1200000 +ellps=bessel +towgs84=674.374,15.056,405.346,0,0,0,0 +units=m +no_defs")

pct <- rgdal::SGDF2PCT(as(photo, "SpatialGridDataFrame"))

photoZH <- setValues(raster(photo), pct$idx-1)
colortable(photoZH) <- pct$ct


# Model domain


# City of Zurich

if(T){
  zh_extent <- extent(x = c(2676000,2676000+13000,1241000,1241000+13000))
}
if(F){
  zh_extent <- extent(x = c(2671000,2696000,1238000+1000,1263000+1000))
}


photoZH   <- crop(photoZH, zh_extent)


# border of the city of Zurich

zh_border <- readOGR(HOHEITSGEBIET_fn,"swissBOUNDARIES3D_1_1_TLM_HOHEITSGEBIET")

zh_border <- zh_border[which(zh_border$BFS_NUMMER==261),]


### --------------------------------------------------------------------------------------------------------------


# Loop over timestamps


for(timestamp_now in seq(timestamp_start,timestamp_end,3600)){
  
  date_now      <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + timestamp_now
  date_now_dow  <- strftime(date_now, "%w",tz="UTC")
  date_now_hour <- strftime(date_now, "%H",tz="UTC")
  
  date_str_ModelPeriod_MAP <- strftime(strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC") + timestamp_now+1800,"%A, %Y-%m-%d %H:%M",tz="UTC")
  date_str_ModelPeriod_FN  <- strftime(strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC") + timestamp_now+1800,"%Y%m%dT%H%M%S",tz="UTC")
  
  # MAP
  
  filename <- paste(figure_dir,date_str_ModelPeriod_FN,".jpg",sep="")
  
  jpeg(filename = filename,
       width = 992, height = 960, units = "px", pointsize = 12,
       quality = 75,
       bg = "white", res = NA, family = "",
       type = c("cairo"))
  
  # basemap
  plot(photoZH)
  
  # ref period traffic
  
  for(ith_ZS in 1:n_u_ZSName){
    
    id_r <- which(ref_traffic$ZSName == u_ZSName[ith_ZS]
                  & ref_traffic$DOW  == date_now_dow)
    
    
    pos <- which(colnames(ref_traffic)==paste("hh_",as.numeric(date_now_hour),sep=""))
    
    
    traffic_count_r <- sum(ref_traffic[id_r,pos])
    cex_point_r     <- sqrt((traffic_count_r/5000))*7
    
    
    
    #
    
    id_a  <- which(traffic_data$ZSName       == u_ZSName[ith_ZS]
                   & traffic_data$timestamp  == timestamp_now
                   & traffic_data$AnzFahrzeugeStatus_TF)
    
    
    traffic_count_a <- sum(traffic_data$AnzFahrzeuge[id_a])
    cex_point_a     <- sqrt((traffic_count_a/5000))*7
    
    #
    
    if(length(id_r)==2 & length(id_a)==2){
      
      points(ref_traffic$EKoord[id_r[1]]-50, ref_traffic$NKoord[id_r[1]], pch=21,cex=cex_point_r,col=1,bg="orange")
      points(traffic_data$EKoord[id_a[1]]+50,traffic_data$NKoord[id_a[1]],pch=21,cex=cex_point_a,col=1,bg="green")
      
      # if(traffic_count_r>=traffic_count_a){ 
      #   points(ref_traffic$EKoord[id_r[1]]-50, ref_traffic$NKoord[id_r[1]], pch=21,cex=cex_point_r,col=1,bg="orange")
      #   points(traffic_data$EKoord[id_a[1]]+50,traffic_data$NKoord[id_a[1]],pch=21,cex=cex_point_a,col=1,bg="green")
      # }else{
      #   points(traffic_data$EKoord[id_a[1]]+50,traffic_data$NKoord[id_a[1]],pch=21,cex=cex_point_a,col=1,bg="green")
      #   points(ref_traffic$EKoord[id_r[1]]-50, ref_traffic$NKoord[id_r[1]], pch=21,cex=cex_point_r,col=1,bg="orange")
      # }
    }
  }
  
  text(0.5*(zh_extent[1]+zh_extent[2]),zh_extent[4]-0.05*(zh_extent[4]-zh_extent[3]),
       labels=paste(date_str_ModelPeriod_MAP,"LT"),cex=3,col="azure",font=2)
  
  
  dx  <- (zh_extent[2]-zh_extent[1])
  dy  <- (zh_extent[4]-zh_extent[3])
  
  xx1 <- zh_extent[1]+0.68*dx
  xx2 <- zh_extent[1]+1.00*dx
  yy1 <- zh_extent[4]-0.75*dy
  yy2 <- zh_extent[4]-1.00*dy
  
  polygon(c(xx1,xx2,xx2,xx1),c(yy2,yy2,yy1,yy1),col="white")
  
  
  points(zh_extent[1]+0.71*dx,zh_extent[4]-0.81*dy,cex=sqrt(1000/5000)*7,pch=21,col=1,bg="orange")
  points(zh_extent[1]+0.71*dx,zh_extent[4]-0.85*dy,cex=sqrt(2000/5000)*7,pch=21,col=1,bg="orange")
  points(zh_extent[1]+0.71*dx,zh_extent[4]-0.89*dy,cex=sqrt(3000/5000)*7,pch=21,col=1,bg="orange")
  points(zh_extent[1]+0.71*dx,zh_extent[4]-0.93*dy,cex=sqrt(4000/5000)*7,pch=21,col=1,bg="orange")
  points(zh_extent[1]+0.71*dx,zh_extent[4]-0.97*dy,cex=sqrt(5000/5000)*7,pch=21,col=1,bg="orange")
  
  points(zh_extent[1]+0.76*dx,zh_extent[4]-0.81*dy,cex=sqrt(1000/5000)*7,pch=21,col=1,bg="green")
  points(zh_extent[1]+0.76*dx,zh_extent[4]-0.85*dy,cex=sqrt(2000/5000)*7,pch=21,col=1,bg="green")
  points(zh_extent[1]+0.76*dx,zh_extent[4]-0.89*dy,cex=sqrt(3000/5000)*7,pch=21,col=1,bg="green")
  points(zh_extent[1]+0.76*dx,zh_extent[4]-0.93*dy,cex=sqrt(4000/5000)*7,pch=21,col=1,bg="green")
  points(zh_extent[1]+0.76*dx,zh_extent[4]-0.97*dy,cex=sqrt(5000/5000)*7,pch=21,col=1,bg="green")
  
  
  par(family="mono")
  
  text(zh_extent[1]+0.71*dx,zh_extent[4]-0.77*dy,labels="REF",cex=2,col="black",font=1)
  text(zh_extent[1]+0.76*dx,zh_extent[4]-0.77*dy,labels="NOW",cex=2,col="black",font=1)
  
  text(zh_extent[1]+0.78*dx,zh_extent[4]-0.81*dy,labels="1000 veh/hour",cex=2,col="black",font=1,pos=4)
  text(zh_extent[1]+0.78*dx,zh_extent[4]-0.85*dy,labels="2000 veh/hour",cex=2,col="black",font=1,pos=4)
  text(zh_extent[1]+0.78*dx,zh_extent[4]-0.89*dy,labels="3000 veh/hour",cex=2,col="black",font=1,pos=4)
  text(zh_extent[1]+0.78*dx,zh_extent[4]-0.93*dy,labels="4000 veh/hour",cex=2,col="black",font=1,pos=4)
  text(zh_extent[1]+0.78*dx,zh_extent[4]-0.97*dy,labels="5000 veh/hour",cex=2,col="black",font=1,pos=4)
  par(family="")
  
  dev.off()
}


