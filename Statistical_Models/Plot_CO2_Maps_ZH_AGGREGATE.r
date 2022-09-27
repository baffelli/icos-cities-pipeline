# Plot_RPART_Maps_AGGREGATE.r
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
  base_dir <- "/project/muem/CarboSense/CO2_ZH_RPART/RPART_V1/"
}

if(selected_version==11){
  base_dir <- "/project/muem/CarboSense/CO2_ZH_RPART/RPART_V2/"
}

if(selected_version==20){
  base_dir <- "/project/muem/CarboSense/CO2_ZH_RPART/RF_V1/"
}

if(selected_version==21){
  base_dir <- "/project/muem/CarboSense/CO2_ZH_RPART/RF_V2/"
}

if(selected_version==30){
  base_dir <- "/project/muem/CarboSense/CO2_ZH_RPART/GAM/"
}

datadir          <- paste(base_dir,"DATA/",sep="")

figure_dir       <- paste(base_dir,"MODEL_PICS_AGGREGATED/",sep="")

swissimage_fn    <- "/project/CarboSense/Data/Swisstopo/SWISSIMAGE25m/SWISSIMAGE25m/SI25-2012-2013-2014.tif"

HOHEITSGEBIET_fn <- "/project/CarboSense/Data/Swisstopo/BOUNDARIES_2013/swissBOUNDARIES3D/SHAPEFILE_LV95_LN02"

wetterlagen_fn   <- "/project/muem/CarboSense/CO2_ZH_RPART/Wetterlagen/Wetterlagen_data.txt"

statistics       <- NULL

### --------------------------------------------------------------------------------------------------------------

# Weather conditions

wetterlagen      <- read.table(file = wetterlagen_fn,header = T,sep=";")
wetterlagen$date <- strptime(wetterlagen$time,"%Y%m%d",tz="UTC")
wetterlagen$dow  <- as.numeric(strftime(wetterlagen$date,"%w",tz="UTC"))

id_ph <- which(wetterlagen$time %in% c("20190101","20190102","20190419","20190421","20190422","20190501","20190530","20190610","20190801","20191225","20191226",
                                       "20200101","20200102","20200410","20200413","20200501"))

id_na <- which(wetterlagen$time %in% c("20190408","20191224","20191231","20200103"))

if(length(id_ph)>0){
  wetterlagen$dow[id_ph] <- 0
}
if(length(id_na)>0){
  wetterlagen$dow[id_na] <- -999
}

rm(id_ph,id_na)
gc()

### --------------------------------------------------------------------------------------------------------------

# MAP Parameters: Time period

agg_periods <- NULL

agg_periods <- rbind(agg_periods,data.frame(Date_UTC_from = strptime("20190301000000","%Y%m%d%H%M%S",tz="UTC"),
                                            Date_UTC_to   = strptime("20190330000000","%Y%m%d%H%M%S",tz="UTC"),
                                            name          = "March_2019",
                                            title         = "March 2019",
                                            stringsAsFactors = F))

agg_periods <- rbind(agg_periods,data.frame(Date_UTC_from = strptime("20190401000000","%Y%m%d%H%M%S",tz="UTC"),
                                            Date_UTC_to   = strptime("20190430000000","%Y%m%d%H%M%S",tz="UTC"),
                                            name          = "April_2019",
                                            title         = "April 2019",
                                            stringsAsFactors = F))

agg_periods <- rbind(agg_periods,data.frame(Date_UTC_from = strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC"),
                                            Date_UTC_to   = strptime("20200328000000","%Y%m%d%H%M%S",tz="UTC"),
                                            name          = "March_2020",
                                            title         = "March 2020",
                                            stringsAsFactors = F))

agg_periods <- rbind(agg_periods,data.frame(Date_UTC_from = strptime("20200330000000","%Y%m%d%H%M%S",tz="UTC"),
                                            Date_UTC_to   = strptime("20200425000000","%Y%m%d%H%M%S",tz="UTC"),
                                            name          = "April_2020",
                                            title         = "April 2020",
                                            stringsAsFactors = F))

n_agg_periods <- dim(agg_periods)[1]



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


# Loop aggregation periods

for(ith_agg_period in 1:n_agg_periods){
  
  # Loop over week day set
  
  for(ith_dow_set in 1:3){
    
    if(ith_dow_set==1){
      dow_set      <- 1:5
      dow_set_name <- "Mo-Fr"
    }
    if(ith_dow_set==2){
      dow_set      <- 6
      dow_set_name <- "Sat"
    }
    if(ith_dow_set==3){
      dow_set      <- 0
      dow_set_name <- "Sun"
    }
    
    id_days   <- which(wetterlagen$date   >= agg_periods$Date_UTC_from[ith_agg_period]
                  & wetterlagen$date <= agg_periods$Date_UTC_to[ith_agg_period]
                  & wetterlagen$dow  %in% dow_set)
    
    n_id_days <- length(id_days)
    
    if(n_id_days==0){
      next
    }
    
    u_weather   <- sort(unique(wetterlagen$wkmowkd0[id_days]))
    n_u_weather <- length(u_weather)
    
    
    for(ith_u_weather in 1:n_u_weather){
      
      # MeteoSwiss GWTWS classification
      
      if(u_weather[ith_u_weather]== 1){
        weather_name <- "West"
        weather_desc <- "W"
      }
      if(u_weather[ith_u_weather]== 2){
        weather_name <- "SouthWest"
        weather_desc <- "SW"
      }
      if(u_weather[ith_u_weather]== 3){
        weather_name <- "NorthWest"
        weather_desc <- "NW"
      }
      if(u_weather[ith_u_weather]== 4){
        weather_name <- "North"
        weather_desc <- "N"
      }
      if(u_weather[ith_u_weather]== 5){
        weather_name <- "NorthEast"
        weather_desc <- "NE"
      }
      if(u_weather[ith_u_weather]== 6){
        weather_name <- "East"
        weather_desc <- "E"
      }
      if(u_weather[ith_u_weather]== 7){
        weather_name <- "SouthEast"
        weather_desc <- "SE"
      }
      if(u_weather[ith_u_weather]== 8){
        weather_name <- "South"
        weather_desc <- "S"
      }
      if(u_weather[ith_u_weather]== 9){
        weather_name <- "Low Pressure"
        weather_desc <- "LP"
      }
      if(u_weather[ith_u_weather]==10){
        weather_name <- "High Pressure"
        weather_desc <- "HP"
      }
      if(u_weather[ith_u_weather]==11){
        weather_name <- "Flat pressure"
        weather_desc <- "FP"
      }
      
      #
      
      dates2agg     <- wetterlagen$date[id_days[which(wetterlagen$wkmowkd0[id_days]==u_weather[ith_u_weather])]]
      n_dates2agg   <- length(dates2agg)
      dates2agg_str <- strftime(dates2agg,"%Y%m%d",tz="UTC")
      
      n_hours_agg <- 3
      
      for(hh in seq(0,24-n_hours_agg,n_hours_agg)){
        
        zh_area_calc <- T
        
        
        # Sensor data
        #
        # "TimePeriod" refers to UTC and indicates the beginning of the one hour modelling period
        #
        
        data_agg <- NULL
        
        for(ith_dates2agg in 1:n_dates2agg){
          for(hh_i in hh:(hh+n_hours_agg-1)){
            date_fn_str <- paste(dates2agg_str[ith_dates2agg],"_",sprintf("%02.0f",hh_i),"0000",sep="")
            data_fn     <- paste(datadir,"MEAS_",date_fn_str,".csv",sep="")
            
            # print(data_fn)
            # print(paste(ith_dates2agg,n_dates2agg))
            
            data_agg    <- rbind(data_agg,read.table(file = data_fn,sep=";",as.is=T,header=T))
          }
        }
        
        data <- unique(data.frame(SensorUnit_ID = data_agg$SensorUnit_ID,
                                  LocationName  = data_agg$LocationName,
                                  Y_LV03        = data_agg$Y_LV03,
                                  X_LV03        = data_agg$X_LV03,
                                  stringsAsFactors = F))
        
        n_data <- dim(data)[1]
        
        data$CO2 <- NA
        
        for(ith_SU_LOC in 1:n_data){
          id   <- which(data_agg$LocationName == data$LocationName[ith_SU_LOC]
                        & data_agg$SensorUnit_ID == data$SensorUnit_ID[ith_SU_LOC])
          
          n_id <- length(id)
          
          if(n_id < 3/4*n_dates2agg*n_hours_agg){
            next
          }
          
          data$CO2[ith_SU_LOC] <- mean(data_agg$CO2[id])
        }
        
        id_notNA <- which(!is.na(data$CO2))
        
        if(length(id_notNA)>0){
          data <- data[id_notNA,]
        }else{
          data <- NULL
        }
        rm(id,n_id,id_notNA,data_agg,data_fn)
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
        
        grid <- NULL
        
        for(ith_dates2agg in 1:n_dates2agg){
          for(hh_i in hh:(hh+n_hours_agg-1)){
            date_fn_str <- paste(dates2agg_str[ith_dates2agg],"_",sprintf("%02.0f",hh_i),"0000",sep="")
            fn          <- paste(datadir,"PRED_GRID_",date_fn_str, ".csv",sep="")

            grid_tmp    <- read.table(file = fn, header=T,sep = ";",as.is=T)
            
            #
            
            statistics  <- rbind(statistics,data.frame(AggPeriod    = agg_periods$name[ith_agg_period],
                                                       DOWs         = dow_set_name,
                                                       AggHour      = hh,
                                                       Weather      = weather_desc,
                                                       date         = dates2agg_str[ith_dates2agg],
                                                       hour_agg     = hh_i,
                                                       CO2_Q000     = quantile(grid_tmp$CO2,probs=0.00),
                                                       CO2_Q005     = quantile(grid_tmp$CO2,probs=0.05),
                                                       CO2_Q050     = quantile(grid_tmp$CO2,probs=0.50),
                                                       CO2_Q095     = quantile(grid_tmp$CO2,probs=0.95),
                                                       CO2_Q100     = quantile(grid_tmp$CO2,probs=1.00),
                                                       CO2_MEAN     = mean(grid_tmp$CO2),
                                                       stringsAsFactors = F))
            
            #
            
            if(ith_dates2agg==1 & hh_i==hh){
              grid <- grid_tmp
            }else{
              if(all(grid$Y_LV03==grid_tmp$Y_LV03) & all(grid$X_LV03==grid_tmp$X_LV03)){
                grid$CO2 <- grid$CO2 + grid_tmp$CO2
              }else{
                stop("Grids do not match!")
              }
            }
          }
        }
        
        grid$CO2 <- grid$CO2/(n_dates2agg*n_hours_agg)
        
        rm(grid_tmp)
        gc()
        
        #
        
        xyz   <- data.frame(x = grid$Y_LV03+2e6,
                            y = grid$X_LV03+1e6,
                            z = grid$CO2,
                            stringsAsFactors=F)
        
        id <- which(xyz$z < CO2_MAP_MIN)
        if(length(id)>0){
          xyz$z[id] <- CO2_MAP_MIN
        }
        
        id <- which(xyz$z > CO2_MAP_MAX)
        if(length(id)>0){
          xyz$z[id] <- CO2_MAP_MAX
        }
        
        
        CO2_conc <- rasterFromXYZ(xyz = xyz,res=c(20,20),
                                  crs = "+proj=somerc +lat_0=46.95240555555556 +lon_0=7.439583333333333 +k_0=1 +x_0=2600000 +y_0=1200000 +ellps=bessel +towgs84=674.374,15.056,405.346,0,0,0,0 +units=m +no_defs")
        
        
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
        
        date_str_ModelPeriod_MAP <- paste(agg_periods$title[ith_agg_period],", ",
                                          dow_set_name,", ",
                                          sprintf("%02.0f",hh),"-",sprintf("%02.0f",hh+n_hours_agg)," UTC",
                                          " (",n_dates2agg,"d),",
                                          " ",weather_desc,sep="")
        date_str_ModelPeriod_FN  <- paste(agg_periods$name[ith_agg_period],"_",
                                          dow_set_name,"_",
                                          sprintf("%02.0f",hh),"-",sprintf("%02.0f",hh+n_hours_agg),"_UTC_",
                                          weather_desc,sep="")   
        
        
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
        
        # CO2 measurements
        if(!is.null(data)){
          points(2e6+data$Y,1e6+data$X,pch=21,bg=data$col,col=1,lwd=1,cex=3)
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
  }
}

#

write.table(x = statistics,file = paste(figure_dir,"statistics.csv",sep=""),
            sep=";",col.names = T,row.names = F,quote=F)

