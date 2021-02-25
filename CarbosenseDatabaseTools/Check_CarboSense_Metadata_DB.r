# Check_CarboSense_Metadata_DB.r
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

## libraries
library(DBI)
require(RMySQL)
require(chron)

dbFetch <- function(...) lubridate::with_tz(DBI::dbFetch(...), tz='UTC')

### ----------------------------------------------------------------------------------------------------------------------------


### ----------------------------------------------------------------------------------------------------------------------------

# Read tables

query_str       <- paste("SELECT * FROM Deployment;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_deployment  <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_deployment$Date_UTC_from <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to   <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

#

query_str       <- paste("SELECT * FROM Calibration;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_calibration <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_calibration$Date_UTC_from <- strptime(tbl_calibration$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_calibration$Date_UTC_to   <- strptime(tbl_calibration$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

#

query_str       <- paste("SELECT * FROM Sensors;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_sensors     <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_sensors$Date_UTC_from <- strptime(tbl_sensors$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_sensors$Date_UTC_to   <- strptime(tbl_sensors$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

#



query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_location    <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)


#

query_str       <- paste("SELECT * FROM SensorUnits;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_sensorUnits <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

#

query_str       <- paste("SELECT * FROM SensorUnits_Info;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_sensorUnitsInfo <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

#

query_str       <- paste("SELECT * FROM RefGasCylinder;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_refGasCylinder <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_refGasCylinder$Date_UTC_from <- strptime(tbl_refGasCylinder$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_refGasCylinder$Date_UTC_to   <- strptime(tbl_refGasCylinder$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

#

query_str       <- paste("SELECT * FROM RefGasCylinder_Deployment;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_refGasCylinderDepl <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_refGasCylinderDepl$Date_UTC_from <- strptime(tbl_refGasCylinderDepl$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_refGasCylinderDepl$Date_UTC_to   <- strptime(tbl_refGasCylinderDepl$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

#

query_str       <- paste("SELECT * FROM SensorExclusionPeriods;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_SEP         <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_SEP$Date_UTC_from <- strptime(tbl_SEP$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_SEP$Date_UTC_to   <- strptime(tbl_SEP$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

#

query_str       <- paste("SELECT * FROM CalibrationParameters;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_CalPar      <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)


### ----------------------------------------------------------------------------------------------------------------------------

### Table 'Location': CHECK BASIC INFORMATION

# Value of 'Network' must be in select list

if(any(!tbl_location$Network%in%c("EMPA","METEOSWISS","NABEL","METAS","SWISSCOM","UNIBE"))){
  id <- which(!tbl_location$Network%in%c("EMPA","METEOSWISS","NABEL","SWISSCOM","UNIBE"))
  stop(paste("TABLE LOCATION : NOT DEFINED Network:", sort(unique(tbl_location$Network[id]))))
}



### Table 'Deployment': CHECK BASIC INFORMATION

# 'LocationName' has to be defined in table 'Location'

if(any(!tbl_deployment$LocationName%in%tbl_location$LocationName)){
  id <- which(!tbl_deployment$LocationName%in%tbl_location$LocationName)
  stop(paste("TABLE DEPLOYMENT : NOT DEFINED IN TABLE LOCATION:", sort(unique(tbl_deployment$LocationName[id]))))
}

# 'SensorUnit_ID' has to be defined in table 'SensorUnits'

if(any(!tbl_deployment$SensorUnit_ID%in%tbl_sensorUnits$SensorUnit_ID)){
  id <- which(!tbl_deployment$SensorUnit_ID%in%tbl_sensorUnits$SensorUnit_ID)
  stop(paste("TABLE DEPLOYMENT : NOT DEFINED IN TABLE SensorUnits:", sort(unique(tbl_deployment$SensorUnit_ID[id]))))
}



### Table 'Calibration': CHECK BASIC INFORMATION

# 'LocationName' has to be defined in table 'Location'

if(any(!tbl_calibration$LocationName%in%tbl_location$LocationName)){
  id <- which(!tbl_calibration$LocationName%in%tbl_location$LocationName)
  stop(paste("TABLE CALIBRATION : NOT DEFINED IN TABLE LOCATION:", sort(unique(tbl_calibration$LocationName[id]))))
}

# 'SensorUnit_ID' has to be defined in table 'SensorUnits'

if(any(!tbl_calibration$SensorUnit_ID%in%tbl_sensorUnits$SensorUnit_ID)){
  id <- which(!tbl_calibration$SensorUnit_ID%in%tbl_sensorUnits$SensorUnit_ID)
  stop(paste("TABLE CALIBRATION : NOT DEFINED IN TABLE SensorUnits:", sort(unique(tbl_calibration$SensorUnit_ID[id]))))
}

# Value of 'CalMode' must be in select list

if(any(!tbl_calibration$CalMode%in%c(1,2,3,11,12,13,22,23,91))){
  id <- which(!tbl_calibration$CalMode%in%c(1,2,3,11,12,13,22,23,91))
  stop(paste("TABLE CALIBRATION : NOT DEFINED CalMode:", sort(unique(tbl_calibration$CalMode[id]))))
}


### ----------------------------------------------------------------------------------------------------------------------------

### Tables 'Deployment' and 'Calibration': CHECK TIME INFORMATION
###
### calibration entries must correspond to deployment entries

u_SensorUnit_ID   <- sort(unique(c(tbl_deployment$SensorUnit_ID,tbl_calibration$SensorUnit_ID)))
n_u_SensorUnit_ID <- length(u_SensorUnit_ID)


for(ith_SUID in 1:n_u_SensorUnit_ID){

  id_depl   <- which(tbl_deployment$SensorUnit_ID==u_SensorUnit_ID[ith_SUID])
  n_id_depl <- length(id_depl)

  id_cal    <- which(tbl_calibration$SensorUnit_ID==u_SensorUnit_ID[ith_SUID])
  n_id_cal  <- length(id_cal)

  if(n_id_cal!=0 & n_id_depl==0){
    stop(paste(u_SensorUnit_ID[ith_SUID])," >> Entry in tbl 'Calibration' but not in tbl 'Deployment'. <<")
  }

  if(n_id_depl == 0){
    next
  }

  id_depl   <- id_depl[order(tbl_deployment$Date_UTC_from[id_depl])]

  if(n_id_cal>0){
    id_cal   <- id_cal[order(tbl_calibration$Date_UTC_from[id_cal])]
  }

  #

  for(ith_depl in 1:n_id_depl){

    if(tbl_deployment$Date_UTC_from[id_depl[ith_depl]]>=tbl_deployment$Date_UTC_to[id_depl[ith_depl]]){
      stop(paste(u_SensorUnit_ID[ith_SUID])," >> DEPL Date_UTC_from >= Date_UTC_to <<",as.character(tbl_deployment[id_depl[ith_depl],]) )
    }

    if(ith_depl>1){
      if(tbl_deployment$Date_UTC_to[id_depl[ith_depl-1]]>=tbl_deployment$Date_UTC_from[id_depl[ith_depl]]){
        stop(paste(u_SensorUnit_ID[ith_SUID])," >> DEPL Date_UTC_to [t-1] >= Date_UTC_from [t] <<")
      }
    }
  }

  #

  if(n_id_cal>0){

    for(ith_cal in 1:n_id_cal){

      if(tbl_calibration$Date_UTC_from[id_cal[ith_cal]]>=tbl_calibration$Date_UTC_to[id_cal[ith_cal]]){
        stop(paste(u_SensorUnit_ID[ith_SUID])," >> CAL Date_UTC_from >= Date_UTC_to <<")
      }

      if(ith_cal>1){
        if(tbl_calibration$Date_UTC_to[id_cal[ith_cal-1]]>=tbl_calibration$Date_UTC_from[id_cal[ith_cal]]){
          stop(paste(u_SensorUnit_ID[ith_SUID])," >> CAL Date_UTC_to [t-1] >= Date_UTC_from [t] <<")

        }

        # dT <- as.numeric(difftime(time1=tbl_calibration$Date_UTC_from[id_cal[ith_cal]],time2=tbl_calibration$Date_UTC_from[id_cal[ith_cal-1]],units="days",tz="UTC"))
        #
        # if(tbl_calibration$CalMode[id_cal[ith_cal-1]]==2
        #    & tbl_calibration$CalMode[id_cal[ith_cal]]==2
        #    & dT>10 & dT<30){
        #   print(paste(tbl_calibration$SensorUnit_ID[id_cal[ith_cal]],dT))
        # }

      }

      #

      id_caldepl <- which(tbl_deployment$Date_UTC_from[id_depl]<=tbl_calibration$Date_UTC_from[id_cal[ith_cal]]
                          & tbl_deployment$Date_UTC_to[id_depl]>=tbl_calibration$Date_UTC_to[id_cal[ith_cal]]
                          & tbl_deployment$LocationName[id_depl]==tbl_calibration$LocationName[id_cal[ith_cal]])

      if(id_caldepl==0){
        stop(paste(u_SensorUnit_ID[ith_SUID])," >> DEPL and CAL periods do not agree <<")
      }
    }
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

### Tables 'Calibration' and 'Sensors': CHECK TIME INFORMATION
###
### calibration entries must correspond to sensor entries (no calibration period must refer to two sensors)

for(ith_cal in 1:dim(tbl_calibration)[1]){

  id_sensors   <- which(tbl_calibration$SensorUnit_ID[ith_cal] == tbl_sensors$SensorUnit_ID & tbl_sensors$Type%in%c("HPP","LP8"))
  n_id_sensors <- length(id_sensors)

  sensor_count <- 0

  for(ith_sensor in 1:n_id_sensors){

    if(tbl_calibration$Date_UTC_from[ith_cal] >= tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]] & tbl_calibration$Date_UTC_to[ith_cal] <= tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]]){
      sensor_count <- sensor_count + 1
    }
    
    if(tbl_calibration$Date_UTC_from[ith_cal] > tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]]
       & tbl_calibration$Date_UTC_from[ith_cal] < tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]]
       & tbl_calibration$Date_UTC_to[ith_cal] > tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]]){
      print(tbl_calibration[ith_cal,])
      print(tbl_sensors[id_sensors[ith_sensor],])
      print(tbl_calibration$Date_UTC_from[ith_cal] >= tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]])
      print(tbl_calibration$Date_UTC_from[ith_cal] < tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]])
      print(tbl_calibration$Date_UTC_to[ith_cal] >= tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]])
      stop(paste(tbl_calibration$SensorUnit_ID[ith_cal])," >>Calibration: CAL and SENSORS do not fit [1] <<")
    }

    if(tbl_calibration$Date_UTC_to[ith_cal] > tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]]
       & tbl_calibration$Date_UTC_to[ith_cal] <= tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]]
       & tbl_calibration$Date_UTC_from[ith_cal] < tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]]){
      print(tbl_calibration[ith_cal,])
      print(tbl_sensors[id_sensors[ith_sensor],])
      stop(paste(tbl_calibration$SensorUnit_ID[ith_cal])," >>Calibration: CAL and SENSORS do not fit [2] <<")
    }
  }

  if(sensor_count!=1){
    stop(paste(tbl_calibration$SensorUnit_ID[ith_cal])," >>Calibration: CAL and SENSORS do not fit [3] <<")
  }

}

### Tables 'Deployment' and 'Sensors': CHECK TIME INFORMATION
###
### deployment entries must correspond to sensor entries (no calibration period must refer to two sensors)

for(ith_depl in 1:dim(tbl_deployment)[1]){

  id_sensors   <- which(tbl_deployment$SensorUnit_ID[ith_depl] == tbl_sensors$SensorUnit_ID & tbl_sensors$Type%in%c("HPP","LP8"))
  n_id_sensors <- length(id_sensors)

  sensor_count <- 0

  for(ith_sensor in 1:n_id_sensors){

    if(tbl_deployment$Date_UTC_from[ith_depl] >= tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]] & tbl_deployment$Date_UTC_to[ith_depl] <= tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]]){
      sensor_count <- sensor_count + 1
    }

    if(tbl_deployment$Date_UTC_from[ith_depl] >= tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]]
       & tbl_deployment$Date_UTC_from[ith_depl] < tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]]
       & tbl_deployment$Date_UTC_to[ith_depl] > tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]]){
      stop(paste(tbl_deployment$SensorUnit_ID[ith_depl])," >> Deployment: CAL and SENSORS do not fit [1] <<")
    }

    if(tbl_deployment$Date_UTC_to[ith_depl] > tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]]
       & tbl_deployment$Date_UTC_to[ith_depl] <= tbl_sensors$Date_UTC_to[id_sensors[ith_sensor]]
       & tbl_deployment$Date_UTC_from[ith_depl] < tbl_sensors$Date_UTC_from[id_sensors[ith_sensor]]){
      stop(paste(tbl_deployment$SensorUnit_ID[ith_depl])," >>Deployment:  CAL and SENSORS do not fit [2] <<")
    }
  }

  if(sensor_count!=1){
    stop(paste(tbl_deployment$SensorUnit_ID[ith_depl])," >>Deployment:  CAL and SENSORS do not fit [3] <<")
  }

}


### ----------------------------------------------------------------------------------------------------------------------------

### Table 'SensorExclusionPeriod'
###
### SensorExclusionPeriod entries must correspond to sensor entries

for(ith_SEP in 1:dim(tbl_SEP)[1]){

  id_sensor <- which(  tbl_SEP$Type[ith_SEP]          == tbl_sensors$Type
                       & tbl_SEP$Serialnumber[ith_SEP]  == tbl_sensors$Serialnumber
                       & tbl_SEP$SensorUnit_ID[ith_SEP] == tbl_sensors$SensorUnit_ID
                       & tbl_SEP$Date_UTC_from[ith_SEP] >= tbl_sensors$Date_UTC_from
                       & tbl_SEP$Date_UTC_to[ith_SEP]   <= tbl_sensors$Date_UTC_to)

  if(length(id_sensor)!=1){
    stop(paste("Exclusion period:",tbl_SEP$Type[ith_SEP],tbl_SEP$Serialnumber[ith_SEP],tbl_SEP$SensorUnit_ID[ith_SEP],tbl_SEP$Date_UTC_from[ith_SEP],": No correspondence in tbl 'Sensors'."))
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

### Tables 'RefGasCylinder' and 'RefGasCylinder_Deployment'

cyl   <- sort(unique(tbl_refGasCylinder$CylinderID))
n_cyl <- length(cyl)

if(any(tbl_refGasCylinder$Date_UTC_from>=tbl_refGasCylinder$Date_UTC_to)){
  stop("Erroneous entry in table 'RefGasCylinder: Date_UTC_from >= Date_UTC_to'")
}

for(cyl in sort(unique(tbl_refGasCylinder$CylinderID))){
  id   <- which(tbl_refGasCylinder$CylinderID==cyl)
  id   <- id[order(tbl_refGasCylinder$Date_UTC_from[id])]
  n_id <- length(id)

  if(n_id>1){
    for(ith_cyl in 2:n_id){
      if(tbl_refGasCylinder$Date_UTC_to[id[ith_cyl-1]]>tbl_refGasCylinder$Date_UTC_from[id[ith_cyl]]){
        stop("Erroneous entry in table 'RefGasCylinder'")
      }
    }
  }
}

#

if(any(tbl_refGasCylinderDepl$Date_UTC_from>=tbl_refGasCylinderDepl$Date_UTC_to)){
  stop("Erroneous entry in table 'RefGasCylinder_Deployment: Date_UTC_from >= Date_UTC_to'")
}

for(cyl in sort(unique(tbl_refGasCylinderDepl$CylinderID))){
  id   <- which(tbl_refGasCylinderDepl$CylinderID==cyl)
  id   <- id[order(tbl_refGasCylinderDepl$Date_UTC_from[id])]
  n_id <- length(id)

  if(n_id>1){
    for(cyldep1 in 1:(n_id-1)){
      for(cyldep2 in 2:n_id){
        if(!(tbl_refGasCylinderDepl$Date_UTC_to[id[cyldep1]]   < tbl_refGasCylinderDepl$Date_UTC_from[id[cyldep2]]
             | tbl_refGasCylinderDepl$Date_UTC_from[id[cyldep1]] > tbl_refGasCylinderDepl$Date_UTC_to[id[cyldep2]])
           & tbl_refGasCylinderDepl$LocationName[id[cyldep1]] !=tbl_refGasCylinderDepl$LocationName[id[cyldep2]]){
          stop("Erroneous entry in table 'RefGasCylinder_Deployment'")
        }
      }
    }
  }
}

for(SUID in sort(unique(tbl_refGasCylinderDepl$SensorUnit_ID))){

  if(SUID==-999){
    next
  }

  id   <- which(tbl_refGasCylinderDepl$SensorUnit_ID==SUID)
  id   <- id[order(tbl_refGasCylinderDepl$Date_UTC_from[id])]
  n_id <- length(id)

  if(n_id>1){
    for(i in 2:n_id){
      if(tbl_refGasCylinderDepl$Date_UTC_to[id[i-1]]>tbl_refGasCylinderDepl$Date_UTC_from[id[i]]){
        stop("Erroneous entry in table 'RefGasCylinder_Deployment'")
      }
    }
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

### Table 'CalibrationParameters'

tmp <- data.frame(Type                 = tbl_CalPar$Type,
                  Serialnumber         = tbl_CalPar$Serialnumber,
                  CalibrationModelName = tbl_CalPar$CalibrationModelName,
                  Mode                 = tbl_CalPar$Mode,
                  stringsAsFactors     = T)

for(i in 1:dim(tmp)[1]){
  id <- which(tmp$Type                   == tbl_CalPar$Type[i]
              & tmp$Serialnumber         == tbl_CalPar$Serialnumber[i]
              & tmp$CalibrationModelName == tbl_CalPar$CalibrationModelName[i]
              & tmp$Mode                 == tbl_CalPar$Mode[i])

  if(length(id)!=1){
    stop("CalibrationParameters: Type, Serialnumber, CalibrationModelName, Mode not unique!")
  }
}

for(sensor in c("HPP","LP8")){

  id_sensor <- which(tbl_CalPar$Type==sensor)

  id_col   <- grep(pattern = paste("PAR_","[[:digit:]]{2,3}",sep=""), x = colnames(tbl_CalPar))
  n_id_col <- length(id_col)

  max_par <- 0
  for(i in 1:n_id_col){
    if(any(tbl_CalPar[id_sensor,id_col[i]]!=0)){
      max_par <- i
    }
  }

  print(paste(sensor,"max_par",max_par))
}


### ----------------------------------------------------------------------------------------------------------------------------