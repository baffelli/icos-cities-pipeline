# GEO_ADMIN_LocationNames.r
# --------------------------------
#
# Author: Michael Mueller
#
#
# --------------------------------

# Remarks:
# - Script generates GEO ADMIN LINK for each active sensor deployment
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
library(carboutil)

### ----------------------------------------------------------------------------------------------------------------------------

URL_str_base <- "https://map.geo.admin.ch/?lang=de&topic=ech&bgLayer=ch.swisstopo.pixelkarte-farbe&layers=ch.swisstopo.zeitreihen,ch.bfs.gebaeude_wohnungs_register,ch.bav.haltestellen-oev,ch.swisstopo.swisstlm3d-wanderwege&layers_visibility=false,false,false,false&layers_timestamp=18641231,,,&E=Y_COORD&N=X_COORD&zoom=10&crosshair=marker"

### ----------------------------------------------------------------------------------------------------------------------------
con <- carboutil::get_conn()
query_str  <- paste("SELECT * FROM Deployment WHERE Date_UTC_to = '2100-01-01 00:00:00' and LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','MET1');",sep="")
drv        <- dbDriver("MySQL")
#con        <- dbConnect(drv, group="CarboSense_MySQL")
res        <- dbSendQuery(con, query_str)
tbl_depl   <- fetch(res, n=-1)
dbClearResult(res)
#dbDisconnect(con)


query_str  <- paste("SELECT * FROM Location;",sep="")
drv        <- dbDriver("MySQL")
#con        <- dbConnect(drv, group="CarboSense_MySQL")
res        <- dbSendQuery(con, query_str)
tbl_loc    <- fetch(res, n=-1)
dbClearResult(res)
#bDisconnect(con)

### ----------------------------------------------------------------------------------------------------------------------------

tbl_depl$X_LV03  <- NA
tbl_depl$Y_LV03  <- NA
tbl_depl$Network <- NA
tbl_depl$Canton  <- NA
tbl_depl$URL     <- NA

for(ith_depl in 1:dim(tbl_depl)[1]){
  
  id_loc <- which(tbl_loc$LocationName==tbl_depl$LocationName[ith_depl])
  
  tbl_depl$X_LV03[ith_depl]  <- tbl_loc$X_LV03[id_loc]
  tbl_depl$Y_LV03[ith_depl]  <- tbl_loc$Y_LV03[id_loc]
 
  tbl_depl$Network[ith_depl] <- tbl_loc$Network[id_loc]
  tbl_depl$Canton[ith_depl]  <- tbl_loc$Canton[id_loc]
  
  #
  
  tmp <- gsub(pattern = "X_COORD",replacement = round(tbl_depl$X_LV03[ith_depl],0),URL_str_base)
  tmp <- gsub(pattern = "Y_COORD",replacement = round(tbl_depl$Y_LV03[ith_depl],0),tmp)
  
  tbl_depl$URL[ith_depl] <- tmp
   
}


### ----------------------------------------------------------------------------------------------------------------------------

df <- data.frame(SUID          = sprintf("%6s",tbl_depl$SensorUnit_ID),
                 LocationName  = sprintf("%6s",tbl_depl$LocationName),
                 Network       = sprintf("%12s",tbl_depl$Network),
                 Canton        = sprintf("%2s",tbl_depl$Canton),
                 Y_LV03        = round(tbl_depl$Y_LV03,0),
                 X_LV03        = round(tbl_depl$X_LV03,0),
                 HeightAboveGround = round(tbl_depl$HeightAboveGround,0),
                 URL           = tbl_depl$URL,
                 stringsAsFactors = F)

df <- df[order(df$Network),]

write.table(df,"/project/CarboSense/Carbosense_Network/CarboSense_KML/CarboSenseNetwork_GEO_ADMIN.txt",sep="\t",col.names = T,row.names = F,quote = F)

### ----------------------------------------------------------------------------------------------------------------------------