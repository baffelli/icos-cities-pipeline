# Compute_NABEL_Picarro_CO2_WET.r
# -----------------------------------------------------
#
# Author: Michael Mueller
#
# -----------------------------------------------------
#
# Remarks:
# - Computations refer to UTC.
#
# -----------------------------------------------------
#


## clear variables
rm(list=ls(all=TRUE))
gc()

## libraries
library(DBI)
require(RMySQL)
require(chron)

# ----------------------------------------------------------------------------------------------------------------------------------
if(T){
  
  for(site in c("RIG","PAY","HAE")){
    
    print(site)
    
    # RIG [from 2019-09-23 12:20:00 --> 1569241200]
    if(site == "RIG"){
      meteoSiteName              <- "NABRIG"
      DBtable                    <- "NABEL_RIG"
      timestamp_CO2_MEAS_WET2DRY <- 1569241200
    }
    
    # PAY [from 2019-10-09 12:00:00 --> 1570622400]
    if(site == "PAY"){
      meteoSiteName              <- "PAY"
      DBtable                    <- "NABEL_PAY"
      timestamp_CO2_MEAS_WET2DRY <- 1570622400
    }
    
    # HAE [from 2020-03-11 14:00:00 --> 1583935200]
    if(site == "HAE"){
      meteoSiteName              <- "NABHAE"
      DBtable                    <- "NABEL_HAE"
      timestamp_CO2_MEAS_WET2DRY <- 1583935200
    }
    
    
    # -----------------------------------------------------------------------------------------
    
    ## Compute H2O from NABEL/MCH measurements to be applied for computation of CO2_WET at PAY, RIG and HAE due to installation of dryer at the inlet of the Picarro instrument
    
    # abs humidity (sensor, reference) / H2O
    # [W. Wagner and A. Pru�: The IAPWS Formulation 1995 for the Thermodynamic Properties of Ordinary Water Substance for General and Scientific Use, Journal of Physical and Chemical Reference Data, June 2002 ,Volume 31, Issue 2, pp. 387535]
    
    coef_1 <-  -7.85951783
    coef_2 <-   1.84408259
    coef_3 <-  -11.7866497
    coef_4 <-   22.6807411
    coef_5 <-  -15.9618719
    coef_6 <-   1.80122502
    
    
    query_str    <- paste("SELECT timestamp,Temperature,RH,Pressure FROM METEOSWISS_Measurements ",sep="")
    query_str    <- paste(query_str, "WHERE timestamp >= ",timestamp_CO2_MEAS_WET2DRY," AND LocationName = '",meteoSiteName,"' ",sep="")
    query_str    <- paste(query_str, "AND Pressure != -999 AND RH != -999 AND Temperature != -999;",sep="")
    drv          <- dbDriver("MySQL")
    con<-carboutil::get_conn(group="CarboSense_MySQL")
    res          <- dbSendQuery(con, query_str)
    data_H2O     <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    
    
    data_H2O  <- data_H2O[order(data_H2O$timestamp),]
    
    theta     <- 1 - (273.15+data_H2O$Temperature)/647.096
    Pws       <- 220640 * exp( 647.096/(273.15+data_H2O$Temperature) * (coef_1*theta + coef_2*theta^1.5 + coef_3*theta^3 + coef_4*theta^3.5 + coef_5*theta^4 + coef_6*theta^7.5))
    Pw        <- data_H2O$RH*(Pws*100)/100
    data_H2O$H2O_COMP   <- Pw / (data_H2O$Pressure*1e2)*1e2
    
    data_H2O <- data.frame(timestamp = rep(data_H2O$timestamp,each=10) + rep(seq(0,540,60),dim(data_H2O)[1]),
                           H2O_COMP  = rep(data_H2O$H2O_COMP, each=10),
                           stringsAsFactors = F)
    
    data_H2O <- data_H2O[order(data_H2O$timestamp),]
    
    
    rm(theta,Pws,Pw)
    rm(coef_1,coef_2,coef_3,coef_4,coef_5,coef_6)
    gc()
    
    
    # -----------------------------------------------------------------------------------------
    
    query_str <- paste("SELECT timestamp,CO2_DRY_CAL FROM ",DBtable," ",sep="")
    query_str <- paste(query_str, "WHERE CO2_DRY_CAL != -999 AND timestamp >= ",timestamp_CO2_MEAS_WET2DRY,";",sep="")
    drv       <- dbDriver("MySQL")
    con<-carboutil::get_conn(group="CarboSense_MySQL")
    res       <- dbSendQuery(con, query_str)
    tmp       <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    if(dim(tmp)[1]==0){
      next
    }
    
    tmp            <- tmp[order(tmp$timestamp),]
    tmp$H2O        <- rep(-999,dim(tmp)[1])
    id_AB          <- which(tmp$timestamp      %in% data_H2O$timestamp)
    id_BA          <- which(data_H2O$timestamp %in% tmp$timestamp)
    tmp$H2O[id_AB] <- data_H2O$H2O_COMP[id_BA]
    tmp            <- tmp[which(tmp$H2O != -999),]
    
    if(dim(tmp)[1]==0){
      next
    }
    
    tmp$CO2_WET_COMP <- tmp$CO2_DRY_CAL * (1 - tmp$H2O/100)
    
    # -----------------------------------------------------------------------------------------
    
    # insert CO2_WET_COMP into database
    
    query_str    <- paste("UPDATE ",DBtable," SET CO2_WET_COMP = -999;",sep="")
    drv          <- dbDriver("MySQL")
    con<-carboutil::get_conn(group="CarboSense_MySQL")
    res          <- dbSendQuery(con, query_str)
    dbClearResult(res)
    dbDisconnect(con)
    
    if(site %in% c("RIG")){
      query_str    <- paste("UPDATE ",DBtable," SET CO2_WET_COMP = CO2_DRY_CAL*(1-H2O/100) ",sep="")
      query_str    <- paste(query_str, "WHERE timestamp < ",timestamp_CO2_MEAS_WET2DRY," ",sep="")
      query_str    <- paste(query_str, "AND CO2_DRY_CAL != -999 AND H2O != -999 and H2O_F = 1;",sep="")
      drv          <- dbDriver("MySQL")
      con<-carboutil::get_conn(group="CarboSense_MySQL")
      res          <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
    
    if(site %in% c("PAY")){
      query_str    <- paste("UPDATE ",DBtable," SET CO2_WET_COMP = CO2_DRY_CAL*(1-H2O_CAL/100) ",sep="")
      query_str    <- paste(query_str, "WHERE timestamp < ",timestamp_CO2_MEAS_WET2DRY," ",sep="")
      query_str    <- paste(query_str, "AND CO2_DRY_CAL != -999 AND H2O_CAL != -999 and H2O_F = 1;",sep="")
      drv          <- dbDriver("MySQL")
      con<-carboutil::get_conn(group="CarboSense_MySQL")
      res          <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
    
    if(site == "HAE"){
      query_str    <- paste("UPDATE ",DBtable," SET CO2_WET_COMP = CO2 ",sep="")
      query_str    <- paste(query_str, "WHERE CO2 != -999 AND timestamp < ",timestamp_CO2_MEAS_WET2DRY,";",sep="")
      drv          <- dbDriver("MySQL")
      con<-carboutil::get_conn(group="CarboSense_MySQL")
      res          <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
    
    
    query_str <- paste("INSERT INTO ",DBtable," (timestamp,CO2_WET_COMP) ",sep="")
    query_str <- paste(query_str,"VALUES ")
    query_str <- paste(query_str,
                       paste("(",paste(tmp$timestamp,",",
                                       tmp$CO2_WET_COMP,
                                       collapse = "),(",sep=""),")",sep=""),
                       paste(" ON DUPLICATE KEY UPDATE "))
    
    query_str       <- paste(query_str,paste("CO2_WET_COMP=VALUES(CO2_WET_COMP);",    sep=""))
    drv          <- dbDriver("MySQL")
    con<-carboutil::get_conn(group="CarboSense_MySQL")
    res          <- dbSendQuery(con, query_str)
    dbClearResult(res)
    dbDisconnect(con)
    
  }
}

#

# Process data of Picarro in DUE

print("DUE")

if(T){

query_str    <- paste("UPDATE NABEL_DUE SET CO2_WET_COMP = CO2_DRY_CAL*(1-H2O/100) ",sep="")
query_str    <- paste(query_str, "WHERE CO2_DRY_CAL != -999 AND H2O != -999 and H2O_F = 1;",sep="")
drv          <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res          <- dbSendQuery(con, query_str)
dbClearResult(res)
dbDisconnect(con)

}
