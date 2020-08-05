# CarboSenseDeployment2KML.r
# -------------------------
#
# Creates KML of CarboSense deployment
#
#


# libraries

library(sp)
library(rgdal)
library(plotKML)
library(DBI)
require(RMySQL)

### ----------------------------------------------------------------------------------------------------------------------------

KML_fn         <- "/project/CarboSense/Carbosense_Network/CarboSense_KML/CarboSenseNetwork.kml"
KML_fn_PR_Mode <- "/project/CarboSense/Carbosense_Network/CarboSense_KML/CarboSenseNetwork_PR_Mode.kml"
KML_fn_S_HAG   <- "/project/CarboSense/Carbosense_Network/CarboSense_KML/CarboSenseNetwork_SWISSCOM_HeightAboveGround.kml"
KML_fn_SSC     <- "/project/CarboSense/Carbosense_Network/CarboSense_KML/CarboSenseNetwork_SPATIAL_SITE_CLASSIFICATION.kml"

### ----------------------------------------------------------------------------------------------------------------------------

# Earliest date for considering deployments

mapdate0_str    <- "20170701000000"
mapdate0        <- strptime(mapdate0_str,"%Y%m%d%H%M%S",tz="UTC")
mapdate0_db_str <- strftime(mapdate0,"%Y-%m-%d 00:00:00", tz="UTC")

### ----------------------------------------------------------------------------------------------------------------------------

# Database queries

query_str       <- paste("SELECT * FROM Deployment WHERE Date_UTC_from >= '",mapdate0_db_str,"';",sep="")
drv             <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL", host='du-gsn1')
res             <- dbSendQuery(con, query_str)
tbl_deployment  <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_deployment$Date_UTC_from <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to   <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

id <- which(tbl_deployment$Date_UTC_to==strptime("2100-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"))

if(length(id)>0){
  date_now <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
  for(ii in 1:length(id)){
    tbl_deployment$Date_UTC_to[id[ii]] <- date_now
  }
}

tbl_deployment$timestamp_from <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

#

tmp             <- paste("'",gsub(pattern = ",",replacement = "','",x = paste(sort(unique(tbl_deployment$LocationName)),collapse=",")),"'",sep="")
query_str       <- paste("SELECT * FROM Location WHERE LocationName IN (",tmp,");",sep="")
drv             <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL",  host='du-gsn1')
res             <- dbSendQuery(con, query_str)
tbl_location    <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

#

if(file.exists("/project/CarboSense/Carbosense_Network/SpatialSiteClassification/SpatialSiteClassification.csv")){
  SSC <- read.table(file = "/project/CarboSense/Carbosense_Network/SpatialSiteClassification/SpatialSiteClassification.csv",
                    sep=";",header=T,as.is=T)
  
  tbl_deployment$SSC_CH <- NA
  
  for(ith_depl in 1:dim(tbl_deployment)[1]){
    id <- which(tbl_deployment$SensorUnit_ID[ith_depl]    == SSC$SensorUnit_ID
                & tbl_deployment$LocationName[ith_depl]   == SSC$LocationName
                & tbl_deployment$timestamp_from[ith_depl] == SSC$timestamp_from)
    
    if(length(id)!=1){
      next
    }else{
      tbl_deployment$SSC_CH[ith_depl] <- SSC$CH[id]
    }
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

# Merge deployment/location information

Sensors <- data.frame(SensorUnit_ID=tbl_deployment$SensorUnit_ID,
                      LocationName=tbl_deployment$LocationName,
                      Lat=NA,
                      Lon=NA,
                      HeightAboveGround <- NA,
                      Network=NA,
                      TimeSpan_begin=NA,
                      TimeSpan_end=NA,
                      SSC_CH=tbl_deployment$SSC_CH)

for(ith_sensor in 1:dim(Sensors)[1]){
  
  id_loc <- which(tbl_location$LocationName==tbl_deployment$LocationName[ith_sensor])
  
  if(length(id_loc)==0){
    next
  }
  
  Sensors$Lat[ith_sensor]               <- tbl_location$LAT_WGS84[id_loc]
  Sensors$Lon[ith_sensor]               <- tbl_location$LON_WGS84[id_loc]
  Sensors$Network[ith_sensor]           <- tbl_location$Network[id_loc]
  Sensors$TimeSpan_begin[ith_sensor]    <- strftime(tbl_deployment$Date_UTC_from[ith_sensor],"%Y-%m-%dT%H:%M:%SZ",tz="UTC")
  Sensors$TimeSpan_end[ith_sensor]      <- strftime(tbl_deployment$Date_UTC_to[ith_sensor]  ,"%Y-%m-%dT%H:%M:%SZ",tz="UTC")
  Sensors$HeightAboveGround[ith_sensor] <- tbl_deployment$HeightAboveGround[ith_sensor]
}

u_networks   <- unique(Sensors$Network)
n_u_networks <- length(u_networks)

u_SSC        <- unique(Sensors$SSC_CH)
n_u_SSC      <- length(u_SSC)

### ----------------------------------------------------------------------------------------------------------------------------

for(ith_mode in 1:4){
  
  # write KML
  
  if(ith_mode==2){
    KML_fn <- KML_fn_PR_Mode
  }
  if(ith_mode==3){
    KML_fn <- KML_fn_S_HAG
  }
  if(ith_mode==4){
    KML_fn <- KML_fn_SSC
  }
  
  kml_open(KML_fn, kml_open = TRUE,
           kml_visibility = TRUE, overwrite = TRUE, use.Google_gx = FALSE,
           kml_xsd = get("kml_xsd", envir = plotKML.opts),
           xmlns = get("kml_url", envir = plotKML.opts),
           xmlns_gx = get("kml_gx", envir = plotKML.opts))
  
  
  
  # Normal operation mode
  
  if(ith_mode==1){
    for(ith_network in 1:n_u_networks){
      
      id <- which(Sensors$Network==u_networks[ith_network] & Sensors$Lat != -999 & Sensors$Lon != -999 & !is.na(Sensors$Lat) & !is.na(Sensors$Lon))
      n_id <- length(id)
      
      if(n_id>0){
        SP <- SpatialPoints(matrix(c(Sensors$Lon[id],Sensors$Lat[id]),ncol=2,nrow=n_id),
                            proj4string=CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
        
        if(!u_networks[ith_network]%in%c("EMPA","NABEL","METEOSWISS","SWISSCOM")){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_networks[ith_network],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=1,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="black",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/wht-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        
        
        if(u_networks[ith_network]=="EMPA"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_networks[ith_network],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=1,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="blue",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/blu-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        if(u_networks[ith_network]=="NABEL"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_networks[ith_network],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=1,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="cyan",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/ltblu-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        if(u_networks[ith_network]=="SWISSCOM"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_networks[ith_network],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=1,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="red",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/red-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        if(u_networks[ith_network]=="METEOSWISS"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_networks[ith_network],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=1,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="green",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/grn-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
      }
    }
  }
  
  # Special mode for plots
  
  if(ith_mode==2){
    
    
    for(ith_network in 1:n_u_networks){
      
      id <- which(Sensors$Network==u_networks[ith_network] & Sensors$Lat != -999 & Sensors$Lon != -999 & !is.na(Sensors$Lat) & !is.na(Sensors$Lon))
      n_id <- length(id)
      
      if(n_id>0){
        SP <- SpatialPoints(matrix(c(Sensors$Lon[id],Sensors$Lat[id]),ncol=2,nrow=n_id),
                            proj4string=CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
        
        if(!u_networks[ith_network]%in%c("EMPA","NABEL","METEOSWISS","SWISSCOM")){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_networks[ith_network],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=0.75,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="orange",
                                  shape = "H:/circle.png",
                                  labels = rep(" ",n_id))
        }
        
        
        if(u_networks[ith_network]=="EMPA"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_networks[ith_network],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=0.75,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="orange",
                                  shape = "H:/circle.png",
                                  labels = rep(" ",n_id))
        }
        if(u_networks[ith_network]=="NABEL"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_networks[ith_network],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=0.75,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="orange",
                                  shape = "H:/circle.png",
                                  labels = rep(" ",n_id))
        }
        if(u_networks[ith_network]=="SWISSCOM"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_networks[ith_network],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=0.75,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="orange",
                                  shape = "H:/circle.png",
                                  labels = rep(" ",n_id))
        }
        if(u_networks[ith_network]=="METEOSWISS"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_networks[ith_network],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=0.75,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="orange",
                                  shape = "H:/circle.png",
                                  labels = rep(" ",n_id))
        }
      }
    }
    
  }
  
  
  if(ith_mode==3){
    
    
    for(cat in 1:3){
      
      if(cat==1){
        id <- which(Sensors$Network=="SWISSCOM" & Sensors$HeightAboveGround == -999  & Sensors$Lat != -999 & Sensors$Lon != -999 & !is.na(Sensors$Lat) & !is.na(Sensors$Lon)) 
      }
      if(cat==2){
        id <- which(Sensors$Network=="SWISSCOM" & Sensors$HeightAboveGround <= 10    & Sensors$HeightAboveGround != -999  & Sensors$Lat != -999 & Sensors$Lon != -999 & !is.na(Sensors$Lat) & !is.na(Sensors$Lon)) 
      }
      if(cat==3){
        id <- which(Sensors$Network=="SWISSCOM" & Sensors$HeightAboveGround  > 10    & Sensors$Lat != -999 & Sensors$Lon != -999 & !is.na(Sensors$Lat) & !is.na(Sensors$Lon)) 
      }
      
      
      n_id <- length(id)
      
      if(n_id>0){
        SP <- SpatialPoints(matrix(c(Sensors$Lon[id],Sensors$Lat[id]),ncol=2,nrow=n_id),
                            proj4string=CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
        
        if(cat==1){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = "Swisscom NOT DEFINED",
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=0.75,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="yellow",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/red-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        
        if(cat==2){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = "Swisscom <= 10 m",
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=0.75,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="red",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/red-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        
        if(cat==3){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = "Swisscom > 10 m",
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=0.75,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="green",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/red-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        
      }
    }
    
  }
  
  
  # SSC mode
  
  if(ith_mode==4){
    
    for(ith_SSC in 1:n_u_SSC){
      
      id <- which(Sensors$SSC_CH==u_SSC[ith_SSC] & Sensors$Lat != -999 & Sensors$Lon != -999 & !is.na(Sensors$Lat) & !is.na(Sensors$Lon))
      n_id <- length(id)
      
      if(n_id>0){
        SP <- SpatialPoints(matrix(c(Sensors$Lon[id],Sensors$Lat[id]),ncol=2,nrow=n_id),
                            proj4string=CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
        
        
        if(!u_SSC[ith_SSC]%in%c("HILLTOP","ELEVATED","URBAN","SUBURBAN","RURAL","VALLEY")){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_SSC[ith_SSC],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=1,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="black",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/wht-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        
        
        if(u_SSC[ith_SSC]=="HILLTOP"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_SSC[ith_SSC],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=1,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="yellow",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/wht-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        if(u_SSC[ith_SSC]=="ELEVATED"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_SSC[ith_SSC],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=1,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="cyan",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/ltblu-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        if(u_SSC[ith_SSC]=="URBAN"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_SSC[ith_SSC],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=1,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="red",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/red-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        if(u_SSC[ith_SSC]=="SUBURBAN"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_SSC[ith_SSC],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=1,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="blue",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/blu-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        if(u_SSC[ith_SSC]=="RURAL"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_SSC[ith_SSC],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=1,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="green",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/grn-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
        if(u_SSC[ith_SSC]=="VALLEY"){
          kml_layer.SpatialPoints(SP,
                                  subfolder.name = paste(u_SSC[ith_SSC],sep=""),
                                  extrude = TRUE, z.scale = 1,
                                  LabelScale = 1,
                                  size=1,
                                  metadata = NULL,
                                  html.table = NULL,
                                  TimeSpan.begin = Sensors$TimeSpan_begin[id],
                                  TimeSpan.end = Sensors$TimeSpan_end[id],
                                  colour="purple",
                                  shape = "http://maps.google.com/mapfiles/kml/paddle/purple-blank.png",
                                  labels=paste(Sensors$LocationName[id],"/",Sensors$SensorUnit_ID[id],sep=""))
        }
      }
    }
  }
  
  
  kml_close(KML_fn)
  
}
