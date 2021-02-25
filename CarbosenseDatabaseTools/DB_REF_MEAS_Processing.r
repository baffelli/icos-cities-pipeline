# DB_REF_MEAS_Processing.r
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
library(openair)
library(DBI)
require(RMySQL)
require(chron)

### ----------------------------------------------------------------------------------------------------------------------------

# processing_mode <- "all"
processing_mode <- "last_measurements"

##

# Routine processing [in combination with "last_measurements"]
db_tables       <- c("NABEL_DUE","NABEL_HAE","NABEL_RIG","NABEL_PAY")

# Special processing [in combination with "all"]
# db_tables       <- c("ClimateChamber_00_DUE")
# db_tables       <- c("PressureChamber_01_DUE")
# db_tables       <- c("PressureChamber_00_METAS")


n_db_tables     <- length(db_tables)

### ----------------------------------------------------------------------------------------------------------------------------

# Calibration information for instruments

calibration_info <- NULL

calibration_info <- rbind(calibration_info,data.frame(Table         = "NABEL_DUE",
                                                      Species       = "CO2_DRY",
                                                      Species_CAL   = "CO2_DRY_CAL",
                                                      Date_UTC_from = '2017-01-01 00:00:00',
                                                      Date_UTC_to   = '2100-01-01 00:00:00',
                                                      par00         = 0.118555244,
                                                      par01         = 0.998324668,
                                                      stringsAsFactors = F))

calibration_info <- rbind(calibration_info,data.frame(Table         = "NABEL_RIG",
                                                      Species       = "CO2_DRY",
                                                      Species_CAL   = "CO2_DRY_CAL",
                                                      Date_UTC_from = '2017-01-01 00:00:00',
                                                      Date_UTC_to   = '2020-01-30 11:10:00',
                                                      par00         = 0.940081792,
                                                      par01         = 1.003035321,
                                                      stringsAsFactors = F))

calibration_info <- rbind(calibration_info,data.frame(Table         = "NABEL_RIG",
                                                      Species       = "CO2_DRY",
                                                      Species_CAL   = "CO2_DRY_CAL",
                                                      Date_UTC_from = '2020-01-30 11:11:00',
                                                      Date_UTC_to   = '2100-01-01 00:00:00',
                                                      par00         = 0,
                                                      par01         = 1.0,
                                                      stringsAsFactors = F))

calibration_info <- rbind(calibration_info,data.frame(Table         = "NABEL_PAY",
                                                      Species       = "CO2_DRY",
                                                      Species_CAL   = "CO2_DRY_CAL",
                                                      Date_UTC_from = '2017-01-01 00:00:00',
                                                      Date_UTC_to   = '2019-10-31 07:30:00',
                                                      par00         = 0.494701392,
                                                      par01         = 0.994069302,
                                                      stringsAsFactors = F))

calibration_info <- rbind(calibration_info,data.frame(Table         = "NABEL_PAY",
                                                      Species       = "CO2_DRY",
                                                      Species_CAL   = "CO2_DRY_CAL",
                                                      Date_UTC_from = '2019-10-31 07:30:00',
                                                      Date_UTC_to   = '2020-02-25 14:00:00',
                                                      par00         = 0.469709184,
                                                      par01         = 0.999378124,
                                                      stringsAsFactors = F))

calibration_info <- rbind(calibration_info,data.frame(Table         = "NABEL_PAY",
                                                      Species       = "CO2_DRY",
                                                      Species_CAL   = "CO2_DRY_CAL",
                                                      Date_UTC_from = '2020-02-25 14:00:00',
                                                      Date_UTC_to   = '2100-01-01 00:00:00',
                                                      par00         = 0,
                                                      par01         = 1,
                                                      stringsAsFactors = F))


calibration_info <- rbind(calibration_info,data.frame(Table         = "NABEL_PAY",
                                                      Species       = "H2O",
                                                      Species_CAL   = "H2O_CAL",
                                                      Date_UTC_from = '2017-01-01 00:00:00',
                                                      Date_UTC_to   = '2018-10-24 19:25:00',
                                                      par00         = 0,
                                                      par01         = 1,
                                                      stringsAsFactors = F))

calibration_info <- rbind(calibration_info,data.frame(Table         = "NABEL_PAY",
                                                      Species       = "H2O",
                                                      Species_CAL   = "H2O_CAL",
                                                      Date_UTC_from = '2018-10-24 19:25:00',
                                                      Date_UTC_to   = '2019-09-03 14:57:00',
                                                      par00         = -(-0.115/0.8786),
                                                      par01         = 1/0.8786,
                                                      stringsAsFactors = F))

calibration_info <- rbind(calibration_info,data.frame(Table         = "NABEL_PAY",
                                                      Species       = "H2O",
                                                      Species_CAL   = "H2O_CAL",
                                                      Date_UTC_from = '2019-09-03 14:57:00',
                                                      Date_UTC_to   = '2100-01-01 00:00:00',
                                                      par00         = 0,
                                                      par01         = 1,
                                                      stringsAsFactors = F))

calibration_info <- rbind(calibration_info,data.frame(Table         = "NABEL_HAE",
                                                      Species       = "CO2_DRY",
                                                      Species_CAL   = "CO2_DRY_CAL",
                                                      Date_UTC_from = '2020-03-11 13:00:00',
                                                      Date_UTC_to   = '2100-01-01 00:00:00',
                                                      par00         = 0,
                                                      par01         = 1.0,
                                                      stringsAsFactors = F))


calibration_info$timestamp_from <- as.numeric(difftime(time1=calibration_info$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
calibration_info$timestamp_to   <- as.numeric(difftime(time1=calibration_info$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

### ----------------------------------------------------------------------------------------------------------------------------

# EMPA CarboSense DB information

DB_group <- "CarboSense_MySQL"

### ----------------------------------------------------------------------------------------------------------------------------

# timestamp of processing

timestamp_processing <- ((as.numeric(difftime(time1=strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"),
                                              time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")))%/%60)*60

### ----------------------------------------------------------------------------------------------------------------------------

# Loop over all tables


for(ith_db_table in 1:n_db_tables){
  
  
  # Get first entry
  if(processing_mode=="all"){
    query_str       <- paste("SELECT MIN(timestamp) AS MIN_TIMESTAMP FROM ",db_tables[ith_db_table],";");
    drv             <- dbDriver("MySQL")
    con<-carboutil::get_conn(group=DB_group)
    res             <- dbSendQuery(con, query_str)
    MIN_TIMESTAMP   <- dbFetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    MIN_TIMESTAMP   <- as.numeric(MIN_TIMESTAMP)
  }
  
  if(processing_mode=="last_measurements"){
    con <-carboutil::get_conn(group=DB_group)
    query_res  <- carboutil::get_query_parametrized(con, "SELECT MAX(timestamp) AS MAX_TIMESTAMP FROM {`tb`}", tb=db_tables[ith_db_table]);
    MIN_TIMESTAMP <- query_res$MAX_TIMESTAMP
    #Process the last week of data
    MIN_TIMESTAMP <- MIN_TIMESTAMP - 7 * 86400
  }
  
  # data set length (amount of data)
  if(db_tables[ith_db_table]%in%c("ClimateChamber_00_DUE","PressureChamber_01_DUE","PressureChamber_00_METAS")){
    data_set_length <- 3650 * 86400
  }else{
    data_set_length <-   90 * 86400
  }
  
  # first data set
  data_set_timestamp_from           <- MIN_TIMESTAMP
  data_set_timestamp_to             <- MIN_TIMESTAMP + data_set_length
  data_set_timestamp_from_10min_av  <- MIN_TIMESTAMP - 10  * 60       # 10 minute average
  
  if(data_set_timestamp_to>timestamp_processing){
    data_set_timestamp_to <- timestamp_processing 
  }
  
  # -----

  # Loop over all data sets
  while(data_set_timestamp_from < timestamp_processing){
    
    # data import
    if(db_tables[ith_db_table]%in%c("PressureChamber_00_METAS")){
      query_str       <- paste("SELECT timestamp, CO2, CO2_F, CO2_DRY, CO2_DRY_F, H2O, H2O_F, pressure, pressure_F FROM ",db_tables[ith_db_table]," WHERE timestamp >= ",data_set_timestamp_from_10min_av," and timestamp < ",data_set_timestamp_to,";")
    }
    if(db_tables[ith_db_table]%in%c("PressureChamber_01_DUE")){
      query_str       <- paste("SELECT timestamp, CO2, CO2_F, CO2_DRY, CO2_DRY_F, H2O, H2O_F, T, T_F, pressure, pressure_F FROM ",db_tables[ith_db_table]," WHERE timestamp >= ",data_set_timestamp_from_10min_av," and timestamp < ",data_set_timestamp_to,";")
    }
    if(db_tables[ith_db_table]=="ClimateChamber_00_DUE"){
      query_str       <- paste("SELECT timestamp, CO2, CO2_F, CO2_DRY, H2O, H2O_F, T, T_F, RH, RH_F, pressure, pressure_F FROM ",db_tables[ith_db_table]," WHERE timestamp >= ",data_set_timestamp_from_10min_av," and timestamp < ",data_set_timestamp_to,";")
    }
    if(db_tables[ith_db_table]%in%c("NABEL_DUE","NABEL_RIG","NABEL_HAE")){
      query_str       <- paste("SELECT timestamp, CO2, CO2_F, CO2_DRY, CO2_DRY_F, H2O, H2O_F, T, T_F, RH, RH_F, pressure, pressure_F FROM ",db_tables[ith_db_table]," WHERE timestamp >= ",data_set_timestamp_from_10min_av," and timestamp < ",data_set_timestamp_to,";")
    }
    # if(db_tables[ith_db_table]%in%c("NABEL_HAE")){
    #   query_str       <- paste("SELECT timestamp, CO2, CO2_F, H2O, H2O_F, T, T_F, RH, RH_F, pressure, pressure_F FROM ",db_tables[ith_db_table]," WHERE timestamp >= ",data_set_timestamp_from_10min_av," and timestamp < ",data_set_timestamp_to,";")
    # }
    if(db_tables[ith_db_table]%in%c("NABEL_PAY")){
      query_str       <- paste("SELECT timestamp, CO2, CO2_F, CO2_DRY, CO2_DRY_F, H2O, H2O_F FROM ",db_tables[ith_db_table]," WHERE timestamp >= ",data_set_timestamp_from_10min_av," and timestamp < ",data_set_timestamp_to,";")
    }
    
    drv   <- dbDriver("MySQL")
    con<-carboutil::get_conn(group=DB_group)
    res   <- dbSendQuery(con, query_str)
    data  <- dbFetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    all_data_timestamps <- data$timestamp
    

    str(data)
    str(query_str, nchar.max = 1000)
    str(data_set_timestamp_from_10min_av)
    str(db_tables[ith_db_table])
    # -------
    
    all_timestamps   <- seq(data_set_timestamp_from_10min_av,data_set_timestamp_to,60)
    
    if(db_tables[ith_db_table]%in%c("ClimateChamber_00_DUE","PressureChamber_01_DUE","PressureChamber_00_METAS")){
      keep           <- which(floor(all_timestamps / 86400) %in% unique(floor(data$timestamp / 86400)) )
      all_timestamps <- all_timestamps[keep]
    }
    
    n_all_timestamps <- length(all_timestamps)
    
    # -------
    
    # Instrument calibrations

    CO2_DRY_CALIBRATION <- F
    H2O_CALIBRATION     <- F
    
    id_tab_cals   <- which(calibration_info$Table==db_tables[ith_db_table])
    n_id_tab_cals <- length(id_tab_cals)
    
    if(any(!calibration_info$Species%in%c("CO2_DRY","H2O"))){
      stop("Not defined calibration.")
    }
    
    if(n_id_tab_cals>0){
      
      u_tab_cals_species   <- sort(unique(calibration_info$Species[id_tab_cals]))
      n_u_tab_cals_species <- length(u_tab_cals_species)
      
      for(ith_u_tab_cals_species in 1:n_u_tab_cals_species){
        
        id_calsToApply <-  which(calibration_info$Table     == db_tables[ith_db_table]
                                 & calibration_info$Species == u_tab_cals_species[ith_u_tab_cals_species])
        
        n_id_calsToApply <- length(id_calsToApply)
        
        
        if(u_tab_cals_species[ith_u_tab_cals_species] == "CO2_DRY"){
          data$CO2_DRY_CAL <- -999
          pos_not_cal      <- which(colnames(data)=="CO2_DRY")
          pos_cal          <- which(colnames(data)=="CO2_DRY_CAL")
          if(any(data$CO2_DRY != -999)){
            CO2_DRY_CALIBRATION <- T
          }else{
            next
          }
        }
        if(u_tab_cals_species[ith_u_tab_cals_species] == "H2O"){
          data$H2O_CAL     <- -999
          pos_not_cal      <- which(colnames(data)=="H2O")
          pos_cal          <- which(colnames(data)=="H2O_CAL")
          H2O_CALIBRATION  <- T
        }
        
        for(ith_calsToApply in 1:n_id_calsToApply){
          
          id <- which(data[,pos_not_cal]     != -999 
                      & data$timestamp >= calibration_info$timestamp_from[id_calsToApply[ith_calsToApply]] 
                      & data$timestamp <  calibration_info$timestamp_to[  id_calsToApply[ith_calsToApply]])
          
          if(length(id)>0){
            data[id,pos_cal] <- calibration_info$par00[id_calsToApply[ith_calsToApply]] + calibration_info$par01[id_calsToApply[ith_calsToApply]]*data[id,pos_not_cal]
          }
        }
      }
    }
    
    # species to be processed
    
    species        <- c("CO2","CO2_DRY","CO2_DRY_CAL","H2O","H2O_CAL","T","RH","pressure")
    species2comp   <- c("CO2_10MIN_AV","CO2_DRY_10MIN_AV","CO2_DRY_CAL_10MIN_AV","H2O_10MIN_AV","H2O_CAL_10MIN_AV","T_10MIN_AV","RH_10MIN_AV","pressure_10MIN_AV") 
    
    n_species      <- length(species)
    n_species2comp <- length(species2comp)
    
    #
    
    speciesToBeProcessed <- rep(F,n_species)
    
    #
    
    mm                <- as.data.frame(matrix(NA,ncol=1+n_species,     nrow=dim(data)[1]),    stringsAsFactors=T)
    mm2comp           <- as.data.frame(matrix(NA,ncol=1+n_species2comp,nrow=n_all_timestamps),stringsAsFactors=T)
    colnames(mm)      <- c("timestamp",species)
    colnames(mm2comp) <- c("timestamp",species2comp)
    
    mm$timestamp      <- data$timestamp
    mm2comp$timestamp <- all_timestamps
    
    #
    
    
    for(ith_species in 1:n_species){
      
      pos <- which(colnames(data)==species[ith_species])
      
      if(length(pos)==0){
        next
      }else{
        speciesToBeProcessed[ith_species] <- T
      }
      
      id_setToNA <- which(data[,pos]==-999)
      
      mm[,ith_species+1]           <- data[,pos]
      mm[id_setToNA,ith_species+1] <- NA
    }
    
    mm <- merge(data.frame(timestamp=all_timestamps,stringsAsFactors = F),mm,by="timestamp",all.x=T)
    
    #
    
    rm(data)
    gc()
    
    # -------
    
    # Loop over all species
    
    ok <- rep(T,n_all_timestamps)
    
    for(ith_species in 1:n_species){
      
      #
      
      if(speciesToBeProcessed[ith_species] == F){
        next
      } 
      
      #
      
      pos      <- which(colnames(mm)     ==species[ith_species])
      pos2comp <- which(colnames(mm2comp)==species2comp[ith_species])
      
      if(length(pos)==0 | length(pos2comp)==0){
        next
      }
      
      #
      
      mm_av <- matrix(NA,ncol=10,nrow=n_all_timestamps)
      
      mm_av[,1] <- mm[,pos]
      for(ii in 2:10){
        mm_av[,ii] <- c(rep(NA,ii-1),mm[1:(n_all_timestamps-(ii-1)),pos]) 
      }
      
      INT_MIN  <- apply(mm_av,1,min,na.rm=T)
      INT_MAX  <- apply(mm_av,1,max,na.rm=T)
      INT_MEAN <- apply(mm_av,1,mean,na.rm=T)
      INT_N    <- apply(!is.na(mm_av),1,sum,na.rm=T)
      
      
      mm2comp[,pos2comp] <- INT_MEAN
      
      
      # Compute long term stability for climate chamber
      
      if(db_tables[ith_db_table]%in%c("ClimateChamber_00_DUE") & ith_species == 1){
        
        print("LONG_TERM")
        print(Sys.time())
        
        T_stable_conditions_range   <- rep(1e3,n_all_timestamps)
        CO2_stable_conditions_range <- rep(1e3,n_all_timestamps)
        
        for(ii in 1:n_all_timestamps){
          # id <- which(mm$timestamp>(mm$timestamp[ii]-7200) & mm$timestamp<=mm$timestamp[ii])
          
          id <- (ii-120):ii
          id <- id[id>0]
          id <- id[(mm$timestamp[ii]-mm$timestamp[id])<7200]
          
          if(length(id)>=60){
            id_CO2 <- id[which(!is.na(mm$CO2[id]))]
            id_T   <- id[which(!is.na(mm$T[id]))]
            
            if(length(id_CO2)>=60){
              CO2_stable_conditions_range[ii] <- max(mm$CO2[id_CO2]) - min(mm$CO2[id_CO2])
            }
            if(length(id_T)>=60){
              T_stable_conditions_range[ii] <- max(mm$T[id_T]) - min(mm$T[id_T])
            }
          }
        }
        
        print(Sys.time())
      }
      
      
      # CHECKS 
      
      ### ---
      
      if(db_tables[ith_db_table]%in%c("NABEL_DUE")){
        if(species[ith_species] == "CO2"){
          ok <- (ok & (INT_MAX - INT_MIN) < 20 & INT_N >=8)
        }
        # if(species[ith_species] == "CO2_DRY"){
        #   ok <- (ok & (((INT_MAX - INT_MIN) < 20 & INT_N >=8) | mm$timestamp <= 1492106400))
        # }
        if(species[ith_species] == "pressure"){
          ok <- (ok & (INT_MAX - INT_MIN) < 5  & INT_N >=8)
        }
        if(species[ith_species] == "T"){
          ok <- (ok & (((INT_MAX - INT_MIN) < 5  & INT_N >=8) | (mm$timestamp>=1528761600 & mm$timestamp<1529452800) ))
        }
        if(species[ith_species] == "RH"){
          ok <- (ok & (INT_N >=8 | (mm$timestamp>=1528761600 & mm$timestamp<1529452800)))
        }
        # if(species[ith_species] == "H2O"){
        #   ok <- (ok & INT_N >=8)
        # }
      }
      
      ### ---
      
      if(db_tables[ith_db_table]%in%c("NABEL_RIG")){
        if(species[ith_species] == "CO2"){
          ok <- (ok & (INT_MAX - INT_MIN) < 20 & INT_N >=8)
        }
        # if(species[ith_species] == "CO2_DRY"){
        #   ok <- (ok & (((INT_MAX - INT_MIN) < 20 & INT_N >=8) | mm$timestamp <= 1492106400))
        # }
        if(species[ith_species] == "pressure"){
          ok <- (ok & (INT_MAX - INT_MIN) < 5  & INT_N >=8)
        }
        if(species[ith_species] == "T"){
          ok <- (ok & (INT_MAX - INT_MIN) < 5  & INT_N >=8)
        }
        if(species[ith_species] == "RH"){
          ok <- (ok & INT_N >=8)
        }
        # if(species[ith_species] == "H2O"){
        #   ok <- (ok & INT_N >=8)
        # }
      }
      
      ### ---
      
      
      if(db_tables[ith_db_table]%in%c("NABEL_HAE")){
        if(species[ith_species] == "CO2"){
          ok <- (ok & (INT_MAX - INT_MIN) <= 150 & INT_N >=8)
        }
        if(species[ith_species] == "pressure"){
          ok <- (ok & (INT_MAX - INT_MIN) < 5  & INT_N >=8)
        }
        if(species[ith_species] == "T"){
          ok <- (ok & (INT_MAX - INT_MIN) < 5  & INT_N >=8)
        }
        if(species[ith_species] == "RH"){
          ok <- (ok & INT_N >=8)
        }
        # if(species[ith_species] == "H2O"){
        #   ok <- (ok & INT_N >=8)
        # }
      }
      
      ### ---
      
      if(db_tables[ith_db_table]%in%c("NABEL_PAY")){
        if(species[ith_species] == "CO2"){
          ok <- (ok & (INT_MAX - INT_MIN) < 20 & INT_N >=8)
        }
        # if(species[ith_species] == "CO2_DRY"){
        #   ok <- (ok & (INT_MAX - INT_MIN) < 20 & INT_N >=8)
        # }
      }
      
      ### ---
      
      if(db_tables[ith_db_table]%in%c("ClimateChamber_00_DUE")){
        
        criteria_set_2 <- mm$timestamp>=1506902400 & mm$timestamp<=1507734000
        criteria_set_3 <- mm$timestamp>=1511272443 & mm$timestamp<=1511355603
        criteria_set_4 <- mm$timestamp>=1557792000 & mm$timestamp<=1558396800
        criteria_set_1 <- !criteria_set_2 & !criteria_set_3 & !criteria_set_4
        
        if(species[ith_species] == "CO2"){
          ok <- (ok & (  (criteria_set_1 & (INT_MAX - INT_MIN) <= 2 & CO2_stable_conditions_range <=10)
                         | (criteria_set_2 & (INT_MAX - INT_MIN) <= 1) 
                         | (criteria_set_3 & (INT_MAX - INT_MIN) <= 1)
                         | (criteria_set_4 & (INT_MAX - INT_MIN) <= 2 & CO2_stable_conditions_range <=10)) & INT_N >=8)
        }
        if(species[ith_species] == "pressure"){
          ok <- (ok & ((  criteria_set_1 & (INT_MAX - INT_MIN) < 5) 
                       | (criteria_set_2 & (INT_MAX - INT_MIN) < 1) 
                       | (criteria_set_3 & (INT_MAX - INT_MIN) < 1)
                       | (criteria_set_4 & (INT_MAX - INT_MIN) < 1))  & INT_N >=8)
        }
        if(species[ith_species] == "T"){
          ok <- (ok & ((  criteria_set_1 & (INT_MAX - INT_MIN) < 2 & T_stable_conditions_range <= 7)
                       | (criteria_set_2 & (INT_MAX - INT_MIN) < 1)
                       |  criteria_set_3 
                       | (criteria_set_4 & (INT_MAX - INT_MIN) < 2 & T_stable_conditions_range <= 7)) & (INT_N >=8 | criteria_set_3 | criteria_set_4))
        }
        # if(species[ith_species] == "RH"){
        #   ok <- (ok & (INT_N >=8 | criteria_set_3 | criteria_set_4))
        # }
        if(species[ith_species] == "H2O"){
          ok <- (ok & INT_N >=8)
        }
        
        rm(criteria_set_1,criteria_set_2,criteria_set_3,criteria_set_4)
        gc()
      }
      
      ### ---
      
      if(db_tables[ith_db_table]%in%c("PressureChamber_01_DUE")){
        
        criteria_set_2 <- mm$timestamp>=1502668800 & mm$timestamp<=1503187200
        criteria_set_3 <- mm$timestamp>=1531630560 & mm$timestamp<=1531897920
        criteria_set_4 <- mm$timestamp>=1559995200 & mm$timestamp<=1560340800
        criteria_set_1 <- !criteria_set_2 & !criteria_set_3 & !criteria_set_4
        
        if(species[ith_species] == "CO2"){
          ok <- (ok & (  (  criteria_set_1 & (INT_MAX - INT_MIN) <= 0.3) 
                         | (criteria_set_2 & (INT_MAX - INT_MIN) <= 2.0) 
                         | (criteria_set_3 & (INT_MAX - INT_MIN) <= 0.3)
                         | (criteria_set_4 & (INT_MAX - INT_MIN) <= 0.3)) & INT_N >=8)
        }
        if(species[ith_species] == "pressure"){
          ok <- (ok & (((criteria_set_1 & (INT_MAX - INT_MIN) < 0.3)|(criteria_set_2 & (INT_MAX - INT_MIN) < 2.0)|(criteria_set_4 & (INT_MAX - INT_MIN) <= 1.0)) & INT_N >=4) | criteria_set_3)
        }
        
        rm(criteria_set_1,criteria_set_2,criteria_set_3)
        gc()
      }
      
      ### ---
      
      if(db_tables[ith_db_table]%in%c("PressureChamber_00_METAS")){
        
        if(species[ith_species] == "CO2"){
          ok <- (ok & (INT_MAX - INT_MIN) <= 2 & INT_N >=8)
        }
        if(species[ith_species] == "CO2_DRY"){
          ok <- (ok & INT_N >=8)
        }
        if(species[ith_species] == "H2O"){
          ok <- (ok & INT_N >=8)
        }
        if(species[ith_species] == "pressure"){
          ok <- (ok & (INT_MAX - INT_MIN) < 2 & INT_N >=4)
        }
        
        rm(criteria_set_1,criteria_set_2,criteria_set_3)
        gc()
      }
      
      ### ---
      
    }
    
    #
    
    ok <- ok & (all_timestamps%in%all_data_timestamps) & all_timestamps >=data_set_timestamp_from & all_timestamps < data_set_timestamp_to
    
    # TODO improve this by using proper parametrised queries
    # Insert into database
    print(db_tables[ith_db_table])
    cols_to_process <-species2comp[which(speciesToBeProcessed==T)]

    for(ith_species in 1:n_species){
      
      if(speciesToBeProcessed[ith_species]==T){
        
        # SET TO DEFAULT -999
        con<-carboutil::get_conn(group=DB_group)
        query_str_par <- glue::glue_sql("UPDATE {`db_tables[ith_db_table]`} SET {`species2comp[ith_species]`} = -999  WHERE timestamp BETWEEN {data_set_timestamp_from} AND {data_set_timestamp_to}", .con=con)
        #query_str <- paste("UPDATE ",db_tables[ith_db_table]," SET ",species2comp[ith_species]," = -999 WHERE timestamp >= ",data_set_timestamp_from," and timestamp < ",data_set_timestamp_to,";",sep="")
     
      
        res <- dbSendQuery(con, query_str_par)
        dbClearResult(res)
        dbDisconnect(con)
        
        #
        
        if(all(is.na(mm2comp[,1+ith_species]))){
          next
        }
        
        #
        
        id_insert <- which(ok & !is.na(mm2comp[,1+ith_species]))
        
        if(length(id_insert)==0){
          next
        }
        
        # INSERT
        
        # query_str <- paste("INSERT INTO ",db_tables[ith_db_table]," (timestamp,",species2comp[ith_species],")",sep="")
        # query_str <- paste(query_str,"VALUES" )
        
        # query_str <- paste(query_str,
        #                    paste("(",paste(mm2comp$timestamp[id_insert],",",
        #                                    mm2comp[id_insert,1+ith_species],
        #                                    collapse = "),(",sep=""),")",sep=""),
        #                    paste(" ON DUPLICATE KEY UPDATE "))
        
        # query_str <- paste(query_str,species2comp[ith_species],"=VALUES(",species2comp[ith_species],");",sep="")

        con<-carboutil::get_conn(group=DB_group)
        query_str_par <- glue::glue_sql("INSERT INTO {`db_tables[ith_db_table]`} (timestamp, {`species2comp[ith_species]`}) VALUES (?,?) ON DUPLICATE KEY UPDATE  {`species2comp[ith_species]`}=VALUES({`species2comp[ith_species]`});", .con=con)
        print(query_str_par)


        data_to_insert <- list(mm2comp$timestamp[id_insert],  mm2comp[id_insert,1+ith_species])
    
        res       <- dbSendQuery(con, query_str_par)
        dbBind(res, data_to_insert)
        dbClearResult(res)
        dbDisconnect(con)
      }
    }
    
    if(CO2_DRY_CALIBRATION){
      
      id_insert <- which(!is.na(mm$CO2_DRY_CAL) & (all_timestamps%in%all_data_timestamps) & all_timestamps >=data_set_timestamp_from & all_timestamps < data_set_timestamp_to)
      
      if(length(id_insert)>0){
        
        query_str <- paste("UPDATE ",db_tables[ith_db_table]," SET CO2_DRY_CAL = -999 WHERE timestamp >= ",data_set_timestamp_from," and timestamp < ",data_set_timestamp_to,";",sep="")
        drv       <- dbDriver("MySQL")
        con<-carboutil::get_conn(group=DB_group)
        res       <- dbSendQuery(con, query_str)
        dbClearResult(res)
        dbDisconnect(con)
        
        #
        
        query_str <- paste("INSERT INTO ",db_tables[ith_db_table]," (timestamp,CO2_DRY_CAL)",sep="")
        query_str <- paste(query_str,"VALUES" )
        
        query_str <- paste(query_str,
                           paste("(",paste(mm$timestamp[id_insert],",",
                                           mm$CO2_DRY_CAL[id_insert],
                                           collapse = "),(",sep=""),")",sep=""),
                           paste(" ON DUPLICATE KEY UPDATE "))
        
        query_str <- paste(query_str,"CO2_DRY_CAL=VALUES(CO2_DRY_CAL);",sep="")
        
        drv       <- dbDriver("MySQL")
        con<-carboutil::get_conn(group=DB_group)
        res       <- dbSendQuery(con, query_str)
        dbClearResult(res)
        dbDisconnect(con)
        
      }
    }
    
    if(H2O_CALIBRATION){
      
      id_insert <- which(!is.na(mm$H2O_CAL) & (all_timestamps%in%all_data_timestamps) & all_timestamps >=data_set_timestamp_from & all_timestamps < data_set_timestamp_to)
      
      if(length(id_insert)>0){
        
        query_str <- paste("UPDATE ",db_tables[ith_db_table]," SET H2O_CAL = -999 WHERE timestamp >= ",data_set_timestamp_from," and timestamp < ",data_set_timestamp_to,";",sep="")
        drv       <- dbDriver("MySQL")
        con<-carboutil::get_conn(group=DB_group)
        res       <- dbSendQuery(con, query_str)
        dbClearResult(res)
        dbDisconnect(con)
        
        #
        
        query_str <- paste("INSERT INTO ",db_tables[ith_db_table]," (timestamp,H2O_CAL)",sep="")
        query_str <- paste(query_str,"VALUES" )
        
        query_str <- paste(query_str,
                           paste("(",paste(mm$timestamp[id_insert],",",
                                           mm$H2O_CAL[id_insert],
                                           collapse = "),(",sep=""),")",sep=""),
                           paste(" ON DUPLICATE KEY UPDATE "))
        
        query_str <- paste(query_str,"H2O_CAL=VALUES(H2O_CAL);",sep="")
        
        drv       <- dbDriver("MySQL")
        con<-carboutil::get_conn(group=DB_group)
        res       <- dbSendQuery(con, query_str)
        dbClearResult(res)
        dbDisconnect(con)
        
      }
    }
    
    #
    
    data_set_timestamp_from           <- data_set_timestamp_to
    data_set_timestamp_to             <- data_set_timestamp_from + data_set_length
    data_set_timestamp_from_10min_av  <- data_set_timestamp_from - 10  * 60       # 10 minute average
    
  }
}
