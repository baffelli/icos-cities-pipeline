# Plot_RPART_Maps.r
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

## clear variables
rm(list=ls(all=TRUE))
gc()

library(raster)
library(rgdal)

### ----------------------------------------------------------------------------------------------------------------------------

args = commandArgs(trailingOnly=TRUE)

selected_version <- args[1]

if(!selected_version%in%c(10,11,20,21,30)){
  stop()
}

### ----------------------------------------------------------------------------------------------------------------------------

# Directories, file names

if(selected_version==10){
  base_dir <- "/project/CarboSense/Statistical_Models/CO2_MAPPING/RPART_V1/"
}

if(selected_version==11){
  base_dir <- "/project/CarboSense/Statistical_Models/CO2_MAPPING/RPART_V2/"
}

if(selected_version==20){
  base_dir <- "/project/CarboSense/Statistical_Models/CO2_MAPPING/RF_V1/"
}

if(selected_version==21){
  base_dir <- "/project/CarboSense/Statistical_Models/CO2_MAPPING/RF_V2/"
}

if(selected_version==30){
  base_dir <- "/project/CarboSense/Statistical_Models/CO2_MAPPING/GAM/"
}

datadir          <- paste(base_dir,"DATA/",sep="")

figure_dir       <- paste(base_dir,"MOVIE_PICS/",sep="")

swissimage_fn    <- "/project/CarboSense/Data/Swisstopo/SWISSIMAGE25m/SWISSIMAGE25m/SI25-2012-2013-2014.tif"

HOHEITSGEBIET_fn <- "/project/CarboSense/Data/Swisstopo/BOUNDARIES_2013/swissBOUNDARIES3D/SHAPEFILE_LV95_LN02"



### --------------------------------------------------------------------------------------------------------------

# MAP Parameters: Time period

timestamp_start <- as.numeric(difftime(time1=strptime("2019-06-24 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))

timestamp_end   <- as.numeric(difftime(time1=strptime("2019-07-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))


timestamp_start <- as.numeric(difftime(time1=strptime("2019-06-25 14:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))

timestamp_end   <- as.numeric(difftime(time1=strptime("2019-06-27 10:58:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))


timestamp_start <- as.numeric(difftime(time1=strptime("2020-01-13 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))

timestamp_end   <- as.numeric(difftime(time1=strptime("2020-01-20 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))


# Map parameters: CO2 concentration range

CO2_MAP_MIN <- 375
CO2_MAP_MAX <- 600
nColors     <-  50


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

# GP

if(F){
  zh_extent <- extent(x = c(2673500-1500,2689000+2500,1240000-2000,1255000+2000))
}

# RF

if(T){
  zh_extent <- extent(x = c(2673500-0500,2689000+3500,1240000-2000,1255000+2000))
}

# City of Zurich

if(F){
  zh_extent <- extent(x = c(2673500-1000,2689000+3000,1240000-2000,1255000+2000))
}


photoZH   <- crop(photoZH, zh_extent)


# border of the city of Zurich

zh_border <- readOGR(HOHEITSGEBIET_fn,"swissBOUNDARIES3D_1_1_TLM_HOHEITSGEBIET")

zh_border <- zh_border[which(zh_border$BFS_NUMMER==261),]


### --------------------------------------------------------------------------------------------------------------


# Loop over timestamps


for(timestamp_now in seq(timestamp_start,timestamp_end,3600)){
  
  zh_area_calc <- T
  
  date_now         <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + timestamp_now
  date_now_fn_str  <- strftime(date_now,     "%Y%m%d_%H%M%S",tz="UTC")
  date_next_fn_str <- strftime(date_now+3600,"%Y%m%d_%H%M%S",tz="UTC")
  date_prev_fn_str <- strftime(date_now-3600,"%Y%m%d_%H%M%S",tz="UTC")
  
  # Sensor data
  #
  # "TimePeriod" refers to UTC and indicates the beginning of the one hour modelling period
  #
  
  data_fn_0 <- paste(datadir,"MEAS_",date_now_fn_str,  ".csv",sep="")
  data_fn_1 <- paste(datadir,"MEAS_",date_next_fn_str, ".csv",sep="")
  
  if(file.exists(data_fn_0)){
    data_0 <- read.table(file = data_fn_0,sep=";",as.is=T,header=T)
  }else{
    data_0 <- NULL
  }
  
  if(file.exists(data_fn_1)){
    data_1 <- read.table(file = data_fn_1,sep=";",as.is=T,header=T)
  }else{
    data_1 <- NULL
  }
  
  data <- rbind(data_0,data_1)
  
  rm(data_0, data_1)
  gc()
  
  #
  
  data$CO2_adj <- data$CO2
  
  id <- which(data$CO2_adj>CO2_MAP_MAX)
  if(length(id)>0){
    data$CO2_adj[id] <- CO2_MAP_MAX
  }
  
  id <- which(data$CO2_adj<CO2_MAP_MIN)
  if(length(id)>0){
    data$CO2_adj[id] <- CO2_MAP_MIN
  }
  
  tmp_1 <- ceiling((data$CO2_adj-CO2_MAP_MIN)/(CO2_MAP_MAX-CO2_MAP_MIN)*nColors)
  
  
  id <- which(tmp_1==0)
  if(length(id)>0){
    tmp_1[id] <- 1
  }
  
  data$col <- rev(heat.colors(nColors,alpha=0.7))[tmp_1]
  
  rm(tmp_1)
  gc()
  
  
  # GRIDS
  
  fn_1   <- paste(datadir,"PRED_GRID_",date_now_fn_str, ".csv",sep="")
  fn_2   <- paste(datadir,"PRED_GRID_",date_next_fn_str,".csv",sep="")
  
  if(file.exists(fn_1)){
    grid_1 <- read.table(file = fn_1, header=T,sep = ";",as.is=T)
  }else{
    next
  }
  
  if(file.exists(fn_2)){
    grid_2 <- read.table(file = fn_2, header=T,sep = ";",as.is=T)
  }else{
    next
  }
  
  #
  
  xyz_1 <- data.frame(x = grid_1$Y_LV03+2e6,
                      y = grid_1$X_LV03+1e6,
                      z = grid_1$CO2,
                      stringsAsFactors=F)
  
  xyz_2 <- data.frame(x = grid_2$Y_LV03+2e6,
                      y = grid_2$X_LV03+1e6,
                      z = grid_2$CO2,
                      stringsAsFactors=F)
  
  id <- which(xyz_1$z < CO2_MAP_MIN)
  if(length(id)>0){
    xyz_1$z[id] <- CO2_MAP_MIN
  }
  
  id <- which(xyz_2$z < CO2_MAP_MIN)
  if(length(id)>0){
    xyz_2$z[id] <- CO2_MAP_MIN
  }
  
  id <- which(xyz_1$z > CO2_MAP_MAX)
  if(length(id)>0){
    xyz_1$z[id] <- CO2_MAP_MAX
  }
  
  id <- which(xyz_2$z > CO2_MAP_MAX)
  if(length(id)>0){
    xyz_2$z[id] <- CO2_MAP_MAX
  }
  
  
  CO2_conc_1 <- rasterFromXYZ(xyz = xyz_1,res=c(20,20),
                              crs = "+proj=somerc +lat_0=46.95240555555556 +lon_0=7.439583333333333 +k_0=1 +x_0=2600000 +y_0=1200000 +ellps=bessel +towgs84=674.374,15.056,405.346,0,0,0,0 +units=m +no_defs")
  
  CO2_conc_2 <- rasterFromXYZ(xyz = xyz_2,res=c(20,20),
                              crs = "+proj=somerc +lat_0=46.95240555555556 +lon_0=7.439583333333333 +k_0=1 +x_0=2600000 +y_0=1200000 +ellps=bessel +towgs84=674.374,15.056,405.346,0,0,0,0 +units=m +no_defs")
  
  
  CO2_conc <- CO2_conc_1
  
  
  for(ip in seq(0,29,1)){
    
    CO2_conc[1:length(CO2_conc)] <- ((30-ip) * CO2_conc_1[1:length(CO2_conc_1)] + ip * CO2_conc_2[1:length(CO2_conc_2)])/30
    
    #
    
    if(zh_area_calc & F){
      zh_area   <- rasterize(x = zh_border,y = CO2_conc,field="BFS_NUMMER",background=NA)
      zh_area[!is.na(zh_area[1:length(zh_area)])] <- 1
      
      zh_area_calc <- F
    }
    
    if(F){
      CO2_conc[1:length(CO2_conc)] <- CO2_conc[1:length(CO2_conc)] * zh_area[1:length(zh_area)]
    }
    
    # TimePerid: UTC, end of one hour interval
    # UTC:  TimePeriod  
    # CEST: TimePeriod + 7200 (center of one hour intervall, adjustment UTC-CEST)
    
    date_str_ModelPeriod_MAP <- strftime(strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC") + timestamp_now+3600+1800+2*ip*60,"%Y-%m-%d %H:%M",tz="UTC")
    date_str_ModelPeriod_FN  <- strftime(strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC") + timestamp_now     +1800+2*ip*60,"%Y%m%dT%H%M%SZ",tz="UTC")
    
    
    # MAP
    
    filename <- paste(figure_dir,date_str_ModelPeriod_FN,".jpg",sep="")
    
    jpeg(filename = filename,
         width = 992, height = 960, units = "px", pointsize = 12,
         quality = 75,
         bg = "white", res = NA, family = "",
         type = c("cairo"))
    
    # basemap
    plot(photoZH)
    
    # CO2 field: colors
    plot(CO2_conc,col=rev(heat.colors(nColors, alpha=0.7)),add=T,zlim=c(CO2_MAP_MIN,CO2_MAP_MAX),legend=F)
    
    # CO2 field: contours
    #contour(CO2_conc,add=T,col="white",drawlabels = F,levels=seq(400,600,25),lwd=1)
    
    # CO2 measurements
    
    if(2*ip < 30){
      id_data <- which(data$timestamp >= (timestamp_now) & data$timestamp < (timestamp_now + 3600))
    }
    if(2*ip >= 30){
      id_data <- which(data$timestamp >= (timestamp_now+3600) & data$timestamp < (timestamp_now + 7200))
    }
    if(length(id_data)>0){
      points(2e6+data$Y[id_data],1e6+data$X[id_data],pch=21,bg=data$col[id_data],col=1,lwd=1,cex=3)
    }
    
    # legend
    leg_title    <- bquote(CO[2])
    leg_unit     <- "[ppm]"
    leg_labels   <- seq(CO2_MAP_MIN,CO2_MAP_MAX,25)
    n_leg_labels <- length(leg_labels)
    
    xx_min <- zh_extent@xmin
    xx_max <- zh_extent@xmax
    yy_min <- zh_extent@ymin
    yy_max <- zh_extent@ymax
    
    xx_frac <- 0.925
    dx_frac <- 0.025
    yy_frac <- 0.5
    dy_frac <- 0.4
    
    leg_height <- (yy_max-yy_min)*dy_frac
    leg_width  <- (xx_max-xx_min)*dx_frac
    leg_ll_corner_yy <- yy_min + yy_frac*(yy_max-yy_min) - 0.5*dy_frac*(yy_max-yy_min)
    leg_ll_corner_xx <- xx_min + xx_frac*(xx_max-xx_min) - 0.5*dx_frac*(xx_max-xx_min)
    
    for(ii in 1:nColors){
      
      yy_tmp <- leg_ll_corner_yy + (ii-1)*(yy_max-yy_min)*dy_frac/nColors
      
      polygon(c(leg_ll_corner_xx,leg_ll_corner_xx+dx_frac*(xx_max-xx_min),leg_ll_corner_xx+dx_frac*(xx_max-xx_min),leg_ll_corner_xx),
              c(yy_tmp,yy_tmp,yy_tmp+leg_height/nColors,yy_tmp+leg_height/nColors),
              col=rev(heat.colors(nColors,alpha=0.7))[ii],
              border=F)
      
      if(ii==nColors){
        polygon(c(leg_ll_corner_xx,leg_ll_corner_xx+leg_width,leg_ll_corner_xx+leg_width,leg_ll_corner_xx),
                c(leg_ll_corner_yy,leg_ll_corner_yy,leg_ll_corner_yy+leg_height,leg_ll_corner_yy+leg_height),
                col=NA,
                border=1,lwd=1)
      }
    }
    
    for(ii in 1:n_leg_labels){
      text(leg_ll_corner_xx+leg_width,leg_ll_corner_yy+(ii-1)*leg_height/(n_leg_labels-1),labels=leg_labels[ii],pos=4,cex=2,col="azure",font=2)
    }
    
    text(leg_ll_corner_xx+leg_width/2+600,leg_ll_corner_yy+1.20*leg_height,labels=leg_title,pos=1,cex=2,col="azure",font=2)
    text(leg_ll_corner_xx+leg_width/2+600,leg_ll_corner_yy+1.14*leg_height,labels=leg_unit, pos=1,cex=2,col="azure",font=2)
    
    text(0.5*(xx_max+xx_min),yy_max-0.05*(yy_max-yy_min),labels=date_str_ModelPeriod_MAP,cex=3,col="azure",font=2)
    
    # lines(2e6+zh_border[1]@polygons[[1]]@Polygons[[1]]@coords[,1],1e6+zh_border[1]@polygons[[1]]@Polygons[[1]]@coords[,2], col=1,lwd=3,lty=1)
    
    text(xx_min + 0.90*(xx_max-xx_min),yy_max-0.97*(yy_max-yy_min),labels=quote("\uA9" ~ Empa),cex=2.0,col="azure",font=2)
    
    dev.off()
  }
}

