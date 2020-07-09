# METEOSWISS_PressureInterpolation.r
# ----------------------------------
#
# Author: Michael Mueller
#
#
# ----------------------------------

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
library(carboutil)
### ----------------------------------------------------------------------------------------------------------------------------

# Steering parameters

DO_CROSSVALIDATION <- F

### ----------------------------------------------------------------------------------------------------------------------------

# directories / filenames

MCH_CV_fn      <- "/project/CarboSense/Carbosense_Network/TS_PRESSURE/MCH_CV.csv"
MCH_CV_hist_fn <- "/project/CarboSense/Carbosense_Network/TS_PRESSURE/MCH_CV.pdf"

if(file.exists((MCH_CV_fn))){
  file.remove(MCH_CV_fn)
}

if(file.exists((MCH_CV_hist_fn))){
  file.remove(MCH_CV_hist_fn)
}

### ----------------------------------------------------------------------------------------------------------------------------

FIRST_CV_EXPORT <- T

### ----------------------------------------------------------------------------------------------------------------------------

# Date_UTC_start  <- strptime("20180701000000","%Y%m%d%H%M%S",tz="UTC")
# Date_UTC_end    <- strptime("20180329120000","%Y%m%d%H%M%S",tz="UTC")

Date_UTC_end    <- strptime(strftime(Sys.time(),"%Y-%m-%d %H:00:00",tz="UTC"),"%Y-%m-%d %H:%M:%S",tz="UTC")
Date_UTC_start  <- Date_UTC_end - 60*86400

timestamp_start <- as.numeric(difftime(time1=Date_UTC_start,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
timestamp_end   <- as.numeric(difftime(time1=Date_UTC_end,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

### ----------------------------------------------------------------------------------------------------------------------------

# READ DB information

# TBL : Location

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_Location    <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

# TBL : Deployment

query_str       <- paste("SELECT * FROM Deployment WHERE SensorUnit_ID > 1000 OR SensorUnit_ID BETWEEN 426 AND 445;",sep="")
drv             <- dbDriver("MySQL")
con <-carboutil::get_conn()
res             <- dbSendQuery(con, query_str)
tbl_Deployment  <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_Deployment$Date_UTC_from  <-strptime(tbl_Deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_Deployment$Date_UTC_to    <-strptime(tbl_Deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

tbl_Deployment$timestamp_from <- as.numeric(difftime(time1=tbl_Deployment$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
tbl_Deployment$timestamp_to   <- as.numeric(difftime(time1=tbl_Deployment$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))

### ----------------------------------------------------------------------------------------------------------------------------

# Remove existing pressure values from the database

if(T){
  
  query_str       <- paste("DELETE FROM `PressureInterpolation` WHERE timestamp >= ",timestamp_start," and timestamp <= ",timestamp_end,";",sep="")
  drv             <- dbDriver("MySQL")
  con <-carboutil::get_conn()
  res             <- dbSendQuery(con, query_str)
  dbClearResult(res)
  dbDisconnect(con)
  
  #
  
  query_str       <- paste("DELETE FROM `PressureParameter` WHERE timestamp >= ",timestamp_start," and timestamp <= ",timestamp_end,";",sep="")
  drv             <- dbDriver("MySQL")
  con <-carboutil::get_conn()
  res             <- dbSendQuery(con, query_str)
  dbClearResult(res)
  dbDisconnect(con)
  
}

### ----------------------------------------------------------------------------------------------------------------------------


# LOOP over all 10 minutes intervals

timestamp_now            <- timestamp_start
timestamp_now_data_query <- timestamp_start
time_period_data_query   <- 86400

while(timestamp_now<timestamp_end){
  
  timestamp_from <- timestamp_now
  timestamp_to   <- timestamp_now + 600
  
  print(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+timestamp_now)
  
  
  ## Import MCH pressure data, assign coordinates (done once for a "time_period_data_query")
  
  
  if(timestamp_now>=timestamp_now_data_query){
    
    timestamp_get_data_from <- timestamp_now
    timestamp_get_data_to   <- timestamp_now + time_period_data_query
    
    if(timestamp_get_data_to > timestamp_end){
      timestamp_get_data_to <- timestamp_end
    }

    
    query_str       <- paste("SELECT LocationName,timestamp,Pressure as pressure FROM METEOSWISS_Measurements WHERE timestamp >= ",timestamp_get_data_from," and timestamp < ",timestamp_get_data_to,";",sep="")
    drv             <- dbDriver("MySQL")
    con <-carboutil::get_conn()
    res             <- dbSendQuery(con, query_str)
    tbl_MCH_P       <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    if(dim(tbl_MCH_P)[1]==0){
      timestamp_now            <- timestamp_now_data_query + time_period_data_query
      timestamp_now_data_query <- timestamp_now_data_query + time_period_data_query
      next
    }
    
    tbl_MCH_P$X_LV03 <- NA
    tbl_MCH_P$Y_LV03 <- NA
    tbl_MCH_P$h      <- NA
    tbl_MCH_P$P0     <- NA
    tbl_MCH_P$pred_pressure <- NA
    
    u_MCH_sites   <- sort(unique(tbl_MCH_P$LocationName))
    n_u_MCH_sites <- length(u_MCH_sites)
    
    for(ith_MCH_site in 1:n_u_MCH_sites){
      id_loc <- which(tbl_Location$LocationName==u_MCH_sites[ith_MCH_site])
      
      if(length(id_loc)==1){
        id              <-  which(tbl_MCH_P$LocationName==u_MCH_sites[ith_MCH_site])
        tbl_MCH_P$X_LV03[id] <-  tbl_Location$X_LV03[id_loc]
        tbl_MCH_P$Y_LV03[id] <-  tbl_Location$Y_LV03[id_loc]
        tbl_MCH_P$h[id]      <-  tbl_Location$h[id_loc]
      }
    }
    
    # Filter data (location, elevation, pressure value)
    
    tbl_MCH_P <- tbl_MCH_P[which(  !is.na(tbl_MCH_P$X_LV03)
                                   & !is.na(tbl_MCH_P$Y_LV03)
                                   & !is.na(tbl_MCH_P$h)),]
    
    
    tbl_MCH_P <- tbl_MCH_P[which(    tbl_MCH_P$pressure > 600 & tbl_MCH_P$pressure < 1080
                                     & tbl_MCH_P$X_LV03>60e3  & tbl_MCH_P$X_LV03<310e3
                                     & tbl_MCH_P$Y_LV03>100e3 & tbl_MCH_P$Y_LV03<845e3
                                     & tbl_MCH_P$h<=3000
                                     & !tbl_MCH_P$LocationName%in%c("MBISL")),]
    
    # Check pressure values
    
    u_MCH_sites   <- sort(unique(tbl_MCH_P$LocationName))
    n_u_MCH_sites <- length(u_MCH_sites)
    
    for(ith_MCH_site in 1:n_u_MCH_sites){
      
      id   <- which(tbl_MCH_P$LocationName==u_MCH_sites[ith_MCH_site])
      n_id <- length(id)
      
      if(n_id>100){
        
        minP    <- min(tbl_MCH_P$pressure[id])
        maxP    <- max(tbl_MCH_P$pressure[id])
        
        if(maxP-minP > 40){
          tbl_MCH_P$pressure[id] <- 999
          
          print(paste("data removed"))
        }
      }
    }
    
    tbl_MCH_P <- tbl_MCH_P[which(tbl_MCH_P$pressure != -999),]
    
  }
  
  ## Select Carbosense site for which pressure should be computed (done once for a "time_period_data_query")
  
  if(timestamp_now>=timestamp_now_data_query){
    
    timestamp_get_data_from <- timestamp_now
    timestamp_get_data_to   <- timestamp_now + time_period_data_query
    
    if(timestamp_get_data_to > timestamp_end){
      timestamp_get_data_to <- timestamp_end
    }
    
    id_PP_sites    <- which(tbl_Deployment$timestamp_from<=timestamp_get_data_to & tbl_Deployment$timestamp_to>=timestamp_get_data_from)
    
    if(length(id_PP_sites)==0){
      timestamp_now            <- timestamp_now_data_query + time_period_data_query
      timestamp_now_data_query <- timestamp_now_data_query + time_period_data_query
      next
    }
    
    u_PP_sites     <- sort(unique(tbl_Deployment$LocationName[id_PP_sites]))
    n_u_PP_sites   <- length(u_PP_sites)  
    u_timestamps   <- seq(timestamp_get_data_from,timestamp_get_data_to,600)
    n_u_timestamps <- length(u_timestamps)
    
    PressurePedictions <- data.frame(LocationName = rep(u_PP_sites,each=n_u_timestamps),
                                     timestamp    = rep(u_timestamps,n_u_PP_sites),
                                     X_LV03       = NA,
                                     Y_LV03       = NA,
                                     h            = NA,
                                     pressure     = -999,
                                     stringsAsFactors = F)
    
    
    for(ith_PP_site in 1:n_u_PP_sites){
      id_loc <- which(tbl_Location$LocationName==u_PP_sites[ith_PP_site])
      
      if(length(id_loc)==1){
        id <-  which(PressurePedictions$LocationName==u_PP_sites[ith_PP_site])
        PressurePedictions$X_LV03[id] <-  tbl_Location$X_LV03[id_loc]
        PressurePedictions$Y_LV03[id] <-  tbl_Location$Y_LV03[id_loc]
        PressurePedictions$h[id]      <-  tbl_Location$h[id_loc]
      }
    }
    
    PressurePedictions <- PressurePedictions[which(!is.na(PressurePedictions$X_LV03)
                                                   & !is.na(PressurePedictions$Y_LV03)
                                                   & !is.na(PressurePedictions$h)),]
  }
  
  ## Matrix for timestamp, H0
  if(timestamp_now>=timestamp_now_data_query){
    
    timestamp_get_data_from <- timestamp_now
    timestamp_get_data_to   <- timestamp_now + time_period_data_query
    
    if(timestamp_get_data_to > timestamp_end){
      timestamp_get_data_to <- timestamp_end
    }
    
    u_timestamps   <- seq(timestamp_get_data_from,timestamp_get_data_to,600)
    n_u_timestamps <- length(u_timestamps)
    
    H0_matrix <- data.frame(timestamp = u_timestamps,
                            H0 = NA,
                            stringsAsFactors = F)
  }
  
  ## Set "timestamp_now_data_query"
  if(timestamp_now>=timestamp_now_data_query){
    timestamp_now_data_query <- timestamp_now_data_query + time_period_data_query
  }
  
  
  ## Loop over 10 minutes interval (data selection, pressure interpolation)
  
  id_MCH_data_now   <- which(tbl_MCH_P$timestamp >= timestamp_now & tbl_MCH_P$timestamp < (timestamp_now+600))
  n_id_MCH_data_now <- length(id_MCH_data_now)
  
  id_PP_now         <- which(PressurePedictions$timestamp >= timestamp_now & PressurePedictions$timestamp < (timestamp_now+600))
  n_id_PP_now       <- length(id_PP_now)
  
  if(n_id_MCH_data_now>20 & n_id_PP_now>0){
    
    if(DO_CROSSVALIDATION==T){
      ncv <- n_id_MCH_data_now
    }else{
      ncv <- 0
    }
    
    for(ith_cv in ncv:0){
      
      if(ith_cv==0){
        id_training <- id_MCH_data_now
      }else{
        id_training <- id_MCH_data_now[(1:n_id_MCH_data_now) != ith_cv]
        id_test     <- id_MCH_data_now[(1:n_id_MCH_data_now) == ith_cv]
      }
      
      
      H0         <- 8500
      P0         <- 1013.25
      diff_P0    <- 1e3
      diff_H0    <- 1e3
      iterations <- 0 
      
      while((abs(diff_P0)>0.01 | abs(diff_H0)>0.1) & iterations<50){
        AA <- matrix(c(exp(-tbl_MCH_P$h[id_training]/H0),P0*(tbl_MCH_P$h[id_training]/(H0^2))*exp(-tbl_MCH_P$h[id_training]/H0)),ncol=2)
        FF <- matrix(tbl_MCH_P$pressure[id_training],ncol=1) - matrix(P0*exp(-tbl_MCH_P$h[id_training]/H0),ncol=1)
        
        xx <- solve(t(AA)%*%AA)%*%t(AA)%*%FF
        
        P0new   <- P0 + xx[1]
        H0new   <- H0 + xx[2]
        
        diff_P0 <- P0new - P0
        diff_H0 <- H0new - H0
        
        P0      <- P0new
        H0      <- H0new
        
        iterations <- iterations + 1
      }
      
      
      # Compute P0 for MCH sites
      
      tbl_MCH_P$P0[id_training] <- tbl_MCH_P$pressure[id_training] * exp(tbl_MCH_P$h[id_training]/H0)
      
      
      # Interpolate pressure for MCH sites (cross-validation)
      
      if(ith_cv!=0){
        
        dist  <- sqrt((tbl_MCH_P$X_LV03[id_test]-tbl_MCH_P$X_LV03[id_training])^2 + (tbl_MCH_P$Y_LV03[id_test]-tbl_MCH_P$Y_LV03[id_training])^2)
        
        id_ok   <- which(dist>0 & dist<=50000)
        n_id_ok <- length(id_ok)
        
        if(n_id_ok>0){
          
          ww <- 1/dist[id_ok]^2
          
          tbl_MCH_P$pred_pressure[id_test] <- sum( ww * tbl_MCH_P$P0[id_training[id_ok]] ) / sum(ww) * exp(-tbl_MCH_P$h[id_test]/H0)
        }
      }
    }
    
    # keep H0
    id_H0_matrix <- which(H0_matrix$timestamp==timestamp_now)
    
    if(length(id_H0_matrix)==1){
      H0_matrix$H0[id_H0_matrix] <- H0
    }
    
    
    # Interpolate pressure for Carbosense sites
    
    for(ith_PP in 1:n_id_PP_now){
      
      dist  <- sqrt((PressurePedictions$X_LV03[id_PP_now[ith_PP]]-tbl_MCH_P$X_LV03[id_training])^2 + (PressurePedictions$Y_LV03[id_PP_now[ith_PP]]-tbl_MCH_P$Y_LV03[id_training])^2)
      
      id_ok   <- which(dist>0 & dist<=50000)
      n_id_ok <- length(id_ok)
      
      if(n_id_ok>0){
        
        ww <- 1/dist[id_ok]^2
        
        PressurePedictions$pressure[id_PP_now[ith_PP]] <- sum( ww * tbl_MCH_P$P0[id_training[id_ok]] ) / sum(ww) * exp(-PressurePedictions$h[id_PP_now[ith_PP]]/H0)
      }
    }
  }
  
  
  ##
  timestamp_now <- timestamp_now + 600
  ##
  
  
  # Export MCH CV
  if(DO_CROSSVALIDATION==T){
    if(timestamp_now >= max(PressurePedictions$timestamp)){
      
      if(FIRST_CV_EXPORT==T){
        write.table(x = tbl_MCH_P,file = MCH_CV_fn,append = F,row.names = F,col.names = T,sep=";")
        FIRST_CV_EXPORT <- F
      }else{
        write.table(x = tbl_MCH_P,file = MCH_CV_fn,append = T,row.names = F,col.names = F,sep=";")
      }
    }
  }
  
  
  
  # Export H0 to database
  if(T & timestamp_now >= max(PressurePedictions$timestamp)){
    
    id_insert <- which(!is.na(H0_matrix$H0))
    
    if(length(id_insert)>0){
      query_str <- paste("INSERT INTO PressureParameter (timestamp,height)",sep="")
      query_str <- paste(query_str,"VALUES" )
      query_str <- paste(query_str,
                         paste("(",paste(H0_matrix$timestamp[id_insert],",",
                                         H0_matrix$H0[id_insert],
                                         collapse = "),(",sep=""),")",sep=""))
      
      query_str <- paste(query_str,paste(";",sep=""))
      
      drv             <- dbDriver("MySQL")
      con <-carboutil::get_conn()
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
  }
  
  # Export pressure of carbosense sites to database
  if(T & timestamp_now >= max(PressurePedictions$timestamp)){
    
    id_insert <- which(!is.na(PressurePedictions$LocationName)
                       & !is.na(PressurePedictions$timestamp)
                       & !is.na(PressurePedictions$pressure)
                       & PressurePedictions$pressure!= -999)
    
    if(length(id_insert)>0){
      query_str <- paste("INSERT INTO PressureInterpolation (LocationName,timestamp,pressure)",sep="")
      query_str <- paste(query_str,"VALUES" )
      query_str <- paste(query_str,
                         paste("(",paste("'",PressurePedictions$LocationName[id_insert],"',",
                                         PressurePedictions$timestamp[id_insert],",",
                                         PressurePedictions$pressure[id_insert],
                                         collapse = "),(",sep=""),")",sep=""),
                         paste(";"))
      
      drv             <- dbDriver("MySQL")
      con <-carboutil::get_conn()
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

# CV statistics

if(DO_CROSSVALIDATION==T){
  MCH_CV_data <- read.table(file = MCH_CV_fn,header=T,sep=";",as.is=T)
  
  id_ok       <- which(!is.na(MCH_CV_data$pressure) & !is.na(MCH_CV_data$pred_pressure))
  n_id_ok     <- length(id_ok)
  
  if(n_id_ok>10){
    
    def_par <- par()
    pdf(file = MCH_CV_hist_fn, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1,1,0.75,0.75),mfrow=c(1,1))
    
    MCH_CV_data   <- MCH_CV_data[id_ok,]
    
    u_MCH_sites   <- as.character(sort(unique(MCH_CV_data$LocationName)))
    n_u_MCH_sites <- length(u_MCH_sites)
    
    for(ii in 0:n_u_MCH_sites){
      
      if(ii==0){
        diff       <- MCH_CV_data$pred_pressure-MCH_CV_data$pressure
        mainString <- "ALL"
      }else{
        id_ok      <- which(MCH_CV_data$LocationName==u_MCH_sites[ii])
        n_id_ok    <- length(id_ok)
        diff       <- MCH_CV_data$pred_pressure[id_ok]-MCH_CV_data$pressure[id_ok]
        mainString <- paste(u_MCH_sites[ii]," [",MCH_CV_data$Y_LV03[id_ok[1]],"/",MCH_CV_data$X_LV03[id_ok[1]],"/",MCH_CV_data$h[id_ok[1]],"]",sep="")
      }
      
      if(n_id_ok>10){
        
        RMSE_CV <- sqrt(sum((diff)^2)/n_id_ok)
        
        str00 <- paste("Q000:",sprintf("%7.2f",quantile(diff,probs=0.00)))
        str01 <- paste("Q001:",sprintf("%7.2f",quantile(diff,probs=0.01)))
        str02 <- paste("Q005:",sprintf("%7.2f",quantile(diff,probs=0.05)))
        str03 <- paste("Q050:",sprintf("%7.2f",quantile(diff,probs=0.50)))
        str04 <- paste("Q095:",sprintf("%7.2f",quantile(diff,probs=0.95)))
        str05 <- paste("Q099:",sprintf("%7.2f",quantile(diff,probs=0.99)))
        str06 <- paste("Q100:",sprintf("%7.2f",quantile(diff,probs=1.00)))
        
        str07 <- paste("RMSE:",sprintf("%7.2f",RMSE_CV))
        str08 <- paste("N:   ",sprintf("%7.0f",n_id_ok))
        
        hist(MCH_CV_data$pred_pressure[id_ok]-MCH_CV_data$pressure[id_ok],seq(-200,200,0.5),xlim=c(-10,10),col="slategray",main=mainString,xlab="Pred Pressure - Pressure [hPa]",cex.axis=1.25,cex.lab=1.25,cex.main=1.25)
        
        par(family="mono")
        legend("topright",legend=c(str00,str01,str02,str03,str04,str05,str06,str07,str08),bg="white")
        par(family="")
      }
    }
    
    dev.off()
    par(def_par)
  }
  
}



