# MeteoSwissNetwork2KML.r
# -----------------------
#
# Remarks:
# - Generates a KML-file including the availability of P,T and wind measurements from MeteoSwiss (table: METEOSWISS_Measurements)
#
#
#
# -----------------------
#
#

## clear variables
rm(list=ls(all=TRUE))
gc()

## libraries
library(DBI)
require(RMySQL)
require(chron)
library(sp)
library(rgdal)
library(plotKML)

### ----------------------------------------------------------------------------------------------------------------------------

KML_fn <- "/project/CarboSense/Carbosense_Network/CarboSense_KML/MeteoSwissNetwork.kml"

### ----------------------------------------------------------------------------------------------------------------------------

# MCH sites
query_str       <- paste("SELECT DISTINCT LocationName FROM METEOSWISS_Measurements;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_MCH_sites   <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

# 

tmp_IN <- paste("('",paste(tbl_MCH_sites$LocationName,collapse = "','"),"')",sep="")

query_str       <- paste("SELECT * FROM Location WHERE LocationName IN ",tmp_IN,";",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_MCH_sites   <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

n_MCH_sites <- dim(tbl_MCH_sites)[1]

#

tbl_MCH_sites$P_Date_UTC_from  <- NA
tbl_MCH_sites$P_Date_UTC_to    <- NA
tbl_MCH_sites$WS_Date_UTC_from <- NA
tbl_MCH_sites$WS_Date_UTC_to   <- NA
tbl_MCH_sites$T_Date_UTC_from  <- NA
tbl_MCH_sites$T_Date_UTC_to    <- NA



for(ith_MCH_site in 1:n_MCH_sites){
  
  query_str       <- paste("SELECT min(timestamp) as MIN_TS, max(timestamp) as MAX_TS FROM METEOSWISS_Measurements WHERE LocationName='",tbl_MCH_sites$LocationName[ith_MCH_site],"' and Pressure != -999;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tmp             <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  tbl_MCH_sites$P_Date_UTC_from[ith_MCH_site] <- strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp$MIN_TS,"%Y-%m-%dT%H:%M:%SZ",tz="UTC")
  tbl_MCH_sites$P_Date_UTC_to[ith_MCH_site]   <- strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp$MAX_TS,"%Y-%m-%dT%H:%M:%SZ",tz="UTC")
  
  #
  
  query_str       <- paste("SELECT min(timestamp) as MIN_TS, max(timestamp) as MAX_TS FROM METEOSWISS_Measurements WHERE LocationName='",tbl_MCH_sites$LocationName[ith_MCH_site],"' and Windspeed != -999;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tmp             <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  tbl_MCH_sites$WS_Date_UTC_from[ith_MCH_site] <- strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp$MIN_TS,"%Y-%m-%dT%H:%M:%SZ",tz="UTC")
  tbl_MCH_sites$WS_Date_UTC_to[ith_MCH_site]   <- strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp$MAX_TS,"%Y-%m-%dT%H:%M:%SZ",tz="UTC")
  
  #
  
  query_str       <- paste("SELECT min(timestamp) as MIN_TS, max(timestamp) as MAX_TS FROM METEOSWISS_Measurements WHERE LocationName='",tbl_MCH_sites$LocationName[ith_MCH_site],"' and Temperature != -999;",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tmp             <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  tbl_MCH_sites$T_Date_UTC_from[ith_MCH_site] <- strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp$MIN_TS,"%Y-%m-%dT%H:%M:%SZ",tz="UTC")
  tbl_MCH_sites$T_Date_UTC_to[ith_MCH_site]   <- strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp$MAX_TS,"%Y-%m-%dT%H:%M:%SZ",tz="UTC")
}



# write KML

kml_open(KML_fn, kml_open = TRUE,
         kml_visibility = TRUE, overwrite = TRUE, use.Google_gx = FALSE,
         kml_xsd = get("kml_xsd", envir = plotKML.opts),
         xmlns = get("kml_url", envir = plotKML.opts),
         xmlns_gx = get("kml_gx", envir = plotKML.opts))



# Normal operation mode

INCLUDE_LABELS <- F

if(T){
  
  id   <- which(!is.na(tbl_MCH_sites$P_Date_UTC_from) & tbl_MCH_sites$Canton!="NA")
  n_id <- length(id)
  
  if(INCLUDE_LABELS){
    lbls <- paste(tbl_MCH_sites$LocationName[id],sep="")
  }else{
    lbls <- rep(" ",n_id)
  }
  
  if(n_id>0){
    SP <- SpatialPoints(matrix(c(tbl_MCH_sites$LON_WGS84[id],tbl_MCH_sites$LAT_WGS84[id]),ncol=2,nrow=n_id),
                        proj4string=CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
    
    kml_layer.SpatialPoints(SP,
                            subfolder.name = "MCH Pressure",
                            extrude        = TRUE, z.scale = 1,
                            LabelScale     = 0.75,
                            size           = 0.75,
                            metadata       = NULL,
                            html.table     = NULL,
                            TimeSpan.begin = tbl_MCH_sites$P_Date_UTC_from[id],
                            TimeSpan.end   = tbl_MCH_sites$P_Date_UTC_to[id],
                            colour         = "yellow",
                            shape          = "http://maps.google.com/mapfiles/kml/paddle/wht-blank-lv.png",
                            labels         = as.vector(lbls)
                            )
  }
  
  #
  
  id   <- which(!is.na(tbl_MCH_sites$WS_Date_UTC_from) & tbl_MCH_sites$Canton!="NA")
  n_id <- length(id)
  
  if(INCLUDE_LABELS){
    lbls <- paste(tbl_MCH_sites$LocationName[id],sep="")
  }else{
    lbls <- rep(" ",n_id)
  }
  
  if(n_id>0){
    SP <- SpatialPoints(matrix(c(tbl_MCH_sites$LON_WGS84[id],tbl_MCH_sites$LAT_WGS84[id]),ncol=2,nrow=n_id),
                        proj4string=CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
    
    kml_layer.SpatialPoints(SP,
                            subfolder.name = "MCH Windspeed",
                            extrude        = TRUE, z.scale = 1,
                            LabelScale     = 0.75,
                            size           = 0.75,
                            metadata       = NULL,
                            html.table     = NULL,
                            TimeSpan.begin = tbl_MCH_sites$WS_Date_UTC_from[id],
                            TimeSpan.end   = tbl_MCH_sites$WS_Date_UTC_to[id],
                            colour         = "green",
                            shape          = "http://maps.google.com/mapfiles/kml/paddle/wht-blank-lv.png",
                            labels         = as.vector(lbls)
                            )
  }
  
  #
  
  id   <- which(!is.na(tbl_MCH_sites$T_Date_UTC_from) & tbl_MCH_sites$Canton!="NA")
  n_id <- length(id)
  
  if(INCLUDE_LABELS){
    lbls <- paste(tbl_MCH_sites$LocationName[id],sep="")
  }else{
    lbls <- rep(" ",n_id)
  }
  
  if(n_id>0){
    SP <- SpatialPoints(matrix(c(tbl_MCH_sites$LON_WGS84[id],tbl_MCH_sites$LAT_WGS84[id]),ncol=2,nrow=n_id),
                        proj4string=CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
    
    kml_layer.SpatialPoints(SP,
                            subfolder.name = "MCH Temperature",
                            extrude        = TRUE, z.scale = 1,
                            LabelScale     = 0.75,
                            size           = 0.75,
                            metadata       = NULL,
                            html.table     = NULL,
                            TimeSpan.begin = tbl_MCH_sites$T_Date_UTC_from[id],
                            TimeSpan.end   = tbl_MCH_sites$T_Date_UTC_to[id],
                            colour         = "red",
                            shape          = "http://maps.google.com/mapfiles/kml/paddle/wht-blank-lv.png",
                            labels         = as.vector(lbls)
                            )
  }
}



kml_close(KML_fn)