# AddCantonNameToTableLocation.r
# --------------------------------
#
# Author: Michael Mueller
#
#
# --------------------------------

# Tasks:
# - Identifies the canton for each site in table "Location"
#
#

## clear variables

rm(list=ls(all=TRUE))
gc()

## libraries

library(DBI)
require(RMySQL)
require(chron)
library(rgdal)
library(sp)
library(rgeos)

## source

source("/project/CarboSense/Software/CoordinateTransformations/WGS84_CH1903.R")


### ------------------------------------------------------------------------------------------------------------------------------

## directories

shape_dir <- "/project/CarboSense/Data/Swisstopo/BOUNDARIES_2013/swissBOUNDARIES3D/SHAPEFILE_LV03_LN02/"
shape_fn  <- "swissBOUNDARIES3D_1_1_TLM_KANTONSGEBIET"

### ------------------------------------------------------------------------------------------------------------------------------

NamesOfCantons <- data.frame(NAME=c("Aargau","Appenzell Ausserrhoden","Appenzell Innerrhoden","Basel-Landschaft","Basel-Stadt","Bern","Fribourg","Genève","Glarus","Graubünden","Jura","Luzern","Neuchâtel","Nidwalden","Obwalden","Schaffhausen","Schwyz","Solothurn","St. Gallen","Thurgau","Ticino","Uri","Valais","Vaud","Zug","Zürich"),
                             NAME_SHORT=c("AG","AR","AI","BL","BS","BE","FR","GE","GL","GR","JU","LU","NE","NW","OW","SH","SZ","SO","SG","TG","TI","UR","VS","VD","ZG","ZH"),
                             stringsAsFactors = F)

### ------------------------------------------------------------------------------------------------------------------------------

# Import table "Location"

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_Location    <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

### ------------------------------------------------------------------------------------------------------------------------------

# Import boundaries

shp   <- readOGR(dsn = shape_dir,layer = shape_fn,stringsAsFactors = F)
n_shp <- length(shp)

# Find canton for each site

LocationCantonName       <- rep("NA",dim(tbl_Location)[1])
LocationCantonName_SHORT <- rep("NA",dim(tbl_Location)[1])

for(ith_shp in 1:n_shp){
  
  LocationInCanton <- point.in.polygon(point.x = tbl_Location$Y_LV03,
                                       point.y = tbl_Location$X_LV03,
                                       pol.x = shp@polygons[[ith_shp]]@Polygons[[1]]@coords[,1],
                                       pol.y = shp@polygons[[ith_shp]]@Polygons[[1]]@coords[,2])
  
  id <- which(LocationInCanton==1)
  
  if(length(id)>0){
    LocationCantonName[id]       <- as.character(shp$NAME[ith_shp])
    LocationCantonName_SHORT[id] <- NamesOfCantons$NAME_SHORT[which(NamesOfCantons$NAME==as.character(shp$NAME[ith_shp]))]
  }
}

### ------------------------------------------------------------------------------------------------------------------------------

# Swisscom sites in D/F/I

id <- which(tbl_Location$LocationName == "KAST" & !is.na(LocationCantonName) & !is.na(LocationCantonName_SHORT))

if(length(id)==1){
  LocationCantonName_SHORT[id] <- "D"
}

id <- which(tbl_Location$LocationName == "SALE" & !is.na(LocationCantonName) & !is.na(LocationCantonName_SHORT))

if(length(id)==1){
  LocationCantonName_SHORT[id] <- "F"
}

### ------------------------------------------------------------------------------------------------------------------------------

# Update database concerning canton information

query_str <- paste("INSERT INTO Location (LocationName,Canton)",sep="")
query_str <- paste(query_str,"VALUES" )
query_str <- paste(query_str,
                   paste("(",paste("'",tbl_Location$LocationName,"',",
                                   "'",LocationCantonName_SHORT,"'",
                                   collapse = "),(",sep=""),")",sep=""),
                   paste(" ON DUPLICATE KEY UPDATE "))

query_str <- paste(query_str,paste("Canton=VALUES(Canton);",sep=""))

drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
dbClearResult(res)
dbDisconnect(con)

### ------------------------------------------------------------------------------------------------------------------------------

