# Empa_OutlierDetection_P.r
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
library(MASS)
library(rpart)
library(parallel)

### ----------------------------------------------------------------------------------------------------------------------------

outlier_period <- 4*24*3600                 # seconds
dry_conditions <- 80                        # RH


### ----------------------------------------------------------------------------------------------------------------------------

## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

#

COMP <- "ALL"

if(length(args)==2){
  if(!args[2]%in%c("DUE","ALL","NO_DUE")){
    stop("args[2] must be DUE, ALL or NO_DUE!")
  }
  if(args[2]=="DUE"){
    COMP <- "DUE"
  }
  if(args[2]=="NO_DUE"){
    COMP <- "NO_DUE"
  }
}

#

if(!as.integer(args[1])%in%c(1,2,3,20,30)){
  stop("MODEL: 1, 2, 3, 20 or 30!")
}else{
  if(as.integer(args[1])==1){
    DB_TABLE  <- "CarboSense_CO2"
    if(COMP=="ALL"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/OutlierDetection_ALL/"
    }
    if(COMP=="DUE"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/OutlierDetection_DUE/"
    }
    if(COMP=="NO_DUE"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/OutlierDetection_NO_DUE/"
    }
  }
  if(as.integer(args[1])==2){
    DB_TABLE  <- "CarboSense_CO2_TEST00"
    if(COMP=="ALL"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/OutlierDetection_ALL/"
    }
    if(COMP=="DUE"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/OutlierDetection_DUE/"
    }
    if(COMP=="NO_DUE"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/OutlierDetection_NO_DUE/"
    }
  }
  if(as.integer(args[1])==3){
    DB_TABLE  <- "CarboSense_CO2_TEST01"
    if(COMP=="ALL"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/OutlierDetection_ALL/"
    }
    if(COMP=="DUE"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/OutlierDetection_DUE/"
    }
    if(COMP=="NO_DUE"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/OutlierDetection_NO_DUE/"
    }
  }
  if(as.integer(args[1])==20){
    DB_TABLE  <- "CarboSense_CO2_TEST00_AMT"
    if(COMP=="ALL"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT/OutlierDetection_ALL/"
    }
    if(COMP=="DUE"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT/OutlierDetection_DUE/"
    }
    if(COMP=="NO_DUE"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT/OutlierDetection_NO_DUE/"
    }
  }
  if(as.integer(args[1])==30){
    DB_TABLE  <- "CarboSense_CO2_TEST01_AMT"
    if(COMP=="ALL"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT/OutlierDetection_ALL/"
    }
    if(COMP=="DUE"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT/OutlierDetection_DUE/"
    }
    if(COMP=="NO_DUE"){
      resultdir <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT/OutlierDetection_NO_DUE/"
    }
  }
}


#


statistics <- data.frame(serialnumber            = -999,
                         SensorUnit_ID           = -999,
                         LocationName            = -999,
                         Date_UTC_from           = -999,
                         Date_UTC_to             = -999,
                         N                       = -999,
                         n_id_flag               = -999,
                         fraction_flagged        = -999,
                         n_id_scatter            = -999,
                         QQQ_abs_log_IR_M_L      = -999,
                         n_noisy                 = -999,
                         fraction_noisy          = -999,
                         QQQ_abs_d_logIR         = -999,
                         n_id_outlier_wo_sc      = -999,
                         n_id_outlier_neg_wo_sc  = -999,
                         n_id_outlier            = -999,
                         n_id_outlier_neg        = -999,
                         RH_max                  = -999,
                         T_DP_max                = -999,
                         n_id_flag_00            = -999,
                         n_id_flag_01            = -999,
                         n_id_flag_02            = -999,
                         n_id_flag_03            = -999,
                         n_id_flag_04            = -999,
                         SysTime                 = -999,
                         stringsAsFactors = F)

write.table(statistics,paste(resultdir,"statistics.csv",sep=""),col.names = T,row.names=F,sep=";")



### ----------------------------------------------------------------------------------------------------------------------------

outlierDetection <- function(serialnumber,DB_TABLE, tbl_depl,tbl_sensors,outlier_period,dry_conditions,resultdir){
  
  #
  
  CREATE_PLOTS <- T
  
  #
  
  ma <- function(x,n=7){filter(x,rep(1/n,n), sides=2)}
  
  #
  
  ith_sensor <- which(tbl_sensors$Serialnumber==serialnumber & tbl_sensors$Type=='LP8')
  
  # Get relevant deployments
  
  id_depl    <- which(tbl_depl$SensorUnit_ID    == tbl_sensors$SensorUnit_ID[ith_sensor]
                      & tbl_depl$timestamp_from >  tbl_sensors$timestamp_from[ith_sensor]
                      & tbl_depl$timestamp_to   <= tbl_sensors$timestamp_to[ith_sensor])
  
  n_id_depl  <- length(id_depl)
  
  if(n_id_depl==0){
    return(0)
  }
  
  
  # Loop over all deployments
  
  statistics <- NULL
  
  for(ith_depl in 1:n_id_depl){
    
    # Import of LP8 sensor data
    
    query_str       <- paste("SELECT * FROM ",DB_TABLE," WHERE SensorUnit_ID = ",tbl_depl$SensorUnit_ID[id_depl[ith_depl]]," and LocationName = '",tbl_depl$LocationName[id_depl[ith_depl]],"' and timestamp >= ",tbl_depl$timestamp_from[id_depl[ith_depl]]," and timestamp <= ",tbl_depl$timestamp_to[id_depl[ith_depl]],";",sep="")
    drv             <- dbDriver("MySQL")
    con<-carboutil::get_conn(group="CarboSense_MySQL")
    res             <- dbSendQuery(con, query_str)
    data            <- dbFetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)

    #
    
    if(dim(data)[1]==0){
      next
    }
    
    #
    
    data            <- data[order(data$timestamp),]
    
    date_UTC_from   <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + min(data$timestamp)
    date_UTC_to     <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + max(data$timestamp)
    
    descriptor      <- paste("SU", tbl_depl$SensorUnit_ID[id_depl[ith_depl]]," / S",serialnumber," @ ",tbl_depl$LocationName[id_depl[ith_depl]]," // Data: ",strftime(date_UTC_from,"%Y/%m/%d",tz="UTC")," - ",strftime(date_UTC_to,"%Y/%m/%d",tz="UTC"),sep="")
    descriptor      <- paste("Data: ",strftime(date_UTC_from,"%Y/%m/%d",tz="UTC")," - ",strftime(date_UTC_to,"%Y/%m/%d",tz="UTC"),sep="")
    
    # Additional variables
    
    data$logIR      <- -log(data$LP8_IR)
    data$logIR_L    <- -log(data$LP8_IR_L)
    data$d_LP8T     <- c(NA,diff(data$LP8_T))
    data$d_logIR    <- c(NA,diff(data$logIR))
    data$logIR_dLM  <- log(data$LP8_IR_L)-log(data$LP8_IR)
    data$logIR_dLM_2<- data$logIR_dLM^2
    data$SHT21_RH_P <- c(NA,data$SHT21_RH[1:(dim(data)[1]-1)])
    
    # Dew point
    
    K1      <-   6.112
    K2      <-  17.62
    K3      <- 243.12
    
    data$DP <- K3 * (((K2*data$SHT21_T)/(K3+data$SHT21_T) + log(data$SHT21_RH/100)) / ( (K2*K3)/(K3+data$SHT21_T) - log(data$SHT21_RH/100)))
    
    
    # Scatter of previous measurements [t-2h...t-10min]
    
    M_log_IR_M_L_2 <- matrix(NA,ncol=12,nrow=dim(data)[1])
    M_dt           <- matrix(NA,ncol=12,nrow=dim(data)[1])
    N              <- rep(0,dim(data)[1])
    
    for(i in 1:12){
      M_dt[1:dim(data)[1],i]           <- data$timestamp-c(rep(NA,i),data$timestamp[1:(dim(data)[1]-i)])
      M_log_IR_M_L_2[1:dim(data)[1],i] <- c(rep(NA,i),data$logIR_dLM_2[1:(dim(data)[1]-i)])
      
      ok <- !is.na(M_dt[,i]) & (M_dt[,i] <= (12*600+120))
      
      if(sum(ok)>0){
        M_log_IR_M_L_2[!ok,i] <- NA
        N[ok]                 <- N[ok] + 1
      }
    }
    
    N_ok                  <- N >= 6
    rmse_log_IR_M_L       <- rep(NA,dim(data)[1])
    
    rmse_log_IR_M_L[N_ok] <- sqrt(apply(M_log_IR_M_L_2[N_ok,],1,sum,na.rm=T)/N[N_ok])
    
    # |d_logIR| < 3 * rmse_log_IR_M_L/sqrt(10)
    id_scatter            <- which(abs(data$d_logIR)<=(rmse_log_IR_M_L/sqrt(10)*3))
    
    
    if(CREATE_PLOTS){
      figname <- paste(resultdir,"RMSE_IR_MEAN_LAST_",tbl_depl$SensorUnit_ID[id_depl[ith_depl]],"_",serialnumber,"_",tbl_depl$LocationName[id_depl[ith_depl]],".pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.0,1,0.1,0.1),mfrow=c(1,1))
      
      xlabString <- "RMSE LOG IR MEAN - LAST"
      h1 <- hist(rmse_log_IR_M_L[N_ok],          col="cadetblue1", xlab=xlabString,sub=descriptor,main="",cex.main=1.25,cex.lab=1.25,cex.axis=1.25,cex.sub=1)
      hist(rmse_log_IR_M_L[id_scatter],h1$breaks,col="coral",      xlab=xlabString,main="",cex.main=1.25,cex.lab=1.25,cex.axis=1.25,add=T)
      
      dev.off()
      par(def_par)
    }
    
    rm(N_ok,N,M_log_IR_M_L_2,M_dt,id)
    gc()
  
    
    # Consecutive measurements
    
    consec_meas        <- c(F,diff(data$timestamp)<=1400)
    
    
    # Noisy measurements
    
    QQQ_abs_log_IR_M_L <- mad(data$logIR_L-data$logIR)
    tmp                <- abs(data$logIR_L-data$logIR) > 5 * QQQ_abs_log_IR_M_L
    data$noisy         <- (ma(x=as.numeric(tmp),n=7) >= 1.5/7) | tmp
    # data$noisy         <- tmp
    id                 <- which(is.na(data$noisy))
    if(length(id)>0){
      data$noisy[id] <- F
    }
    
    rm(id,tmp)
    gc()
    
    # Large changes in IR of consecutive measurements
    
    QQQ_abs_d_logIR                 <- mad(data$d_logIR[consec_meas])
    data$large_d_logIR              <- NA
    data$large_d_logIR[consec_meas] <- abs(data$d_logIR[consec_meas]) > 5*QQQ_abs_d_logIR

    
    # Dependency d_LP8_T -- d_logIR
    
    dry         <- (data$SHT21_RH < dry_conditions) & consec_meas
    
    if(sum(dry)>10){
      fit         <- rlm(d_logIR~I(d_LP8T)+I(d_LP8T^2),data[dry,],psi=psi.huber,k=1.345)
    }else{
      return()
    }
    pred        <- as.numeric(predict(fit,data))
    res         <- data$d_logIR - pred
    mad_res_dry <- mad(res[dry],na.rm=T)
    
    # Positive/negative outliers
    
    id_outlier      <- which(consec_meas & res >  3*mad_res_dry & data$SHT21_RH > 70)
    id_outlier_neg  <- which(consec_meas & res < -3*mad_res_dry & data$SHT21_RH > 70)
    
    id_outlier_wo_sc       <- id_outlier
    id_outlier_neg_wo_sc   <- id_outlier_neg
    
    n_id_outlier_wo_sc     <- length(id_outlier)
    n_id_outlier_neg_wo_sc <- length(id_outlier_neg)
    
    id_outlier       <- id_outlier[    !id_outlier    %in%id_scatter]
    id_outlier_neg   <- id_outlier_neg[!id_outlier_neg%in%id_scatter]
    
    n_id_outlier     <- length(id_outlier)
    n_id_outlier_neg <- length(id_outlier_neg)
    
    if(length(id_outlier)>10){
      RH_max   <- quantile(data$SHT21_RH[id_outlier],probs=0.75)  # 0.50 oder 0.75
      T_DP_max <- quantile(data$SHT21_T[id_outlier]-data$DP[id_outlier],probs=0.25) # 0.50 oder 0.25
    }else{
      RH_max   <-  999
      T_DP_max <- -999
    }
    
    
    if(CREATE_PLOTS){
      
      #
      
      figname <- paste(resultdir,"RH_DISTRIB_OF_OUTLIERS_",tbl_depl$SensorUnit_ID[id_depl[ith_depl]],"_",serialnumber,"_",tbl_depl$SensorUnit_ID[id_depl[ith_depl]],".pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.0,1,0.1,0.1),mfrow=c(1,1))
      
      xlabString <- "RH [%]"
      hist(data$SHT21_RH[id_outlier],seq(0,120,2),xlim=c(0,100),col="slategray",main="",xlab=xlabString,sub=descriptor,cex.main=1.25,cex.lab=1.25,cex.axis=1.25,cex.sub=1)
      lines(c(RH_max,RH_max),c(-1e9,1e9),col=2,lwd=2)
      
      dev.off()
      par(def_par)
      
      #
      
      figname <- paste(resultdir,"dIR_dLP8T_",tbl_depl$SensorUnit_ID[id_depl[ith_depl]],"_",serialnumber,"_",tbl_depl$LocationName[id_depl[ith_depl]],".pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.0,1,0.25,0.1),mfrow=c(1,1))
      
      d_LP8T_tmp <- seq(min(data$d_LP8T,na.rm=T),max(data$d_LP8T,na.rm=T),length.out = 1e3)
      pred_plot  <- as.numeric(predict(fit,data.frame(d_LP8T=d_LP8T_tmp,stringsAsFactors=T)))
      
      xlabString <- expression(paste(Delta,"LP8_T"))
      ylabString <- expression(paste(Delta,"LOG(IR)"))
      
      plot(data$d_LP8T[consec_meas],data$d_logIR[consec_meas],xlab=xlabString,ylab=ylabString,sub=descriptor, pch=16 ,cex=0.5,cex.main=1.75,cex.lab=1.75,cex.axis=1.75,cex.sub=1.25)
      points(data$d_LP8T[id_outlier],    data$d_logIR[id_outlier],                  pch=16 ,cex=0.5,col=2)
      points(data$d_LP8T[id_outlier_neg],data$d_logIR[id_outlier_neg],              pch=16 ,cex=0.5,col=3)
      lines(d_LP8T_tmp,pred_plot,col="orange",lwd=2)
      
      leg_str_00 <- paste("N measurements:",sprintf("%6.0f",length(which(!is.na(data$d_LP8T) & !is.na(data$d_logIR) & consec_meas))))
      leg_str_01 <- paste("N outlier pos: ",sprintf("%6.0f",length(id_outlier)))
      leg_str_02 <- paste("N outlier neg: ",sprintf("%6.0f",length(id_outlier_neg)))
      
      lines(c(-1e9,1e9),c(0,0),lwd=2,lty=5,col="gray70")
      lines(c(0,0),c(-1e9,1e9),lwd=2,lty=5,col="gray70")
      
      par(family="mono")
      legend("topleft",legend=c(leg_str_00,leg_str_01,leg_str_02),bg="white",cex=1.75,col=c(1,2,3),pch=15)
      par(family="")
      
      dev.off()
      par(def_par)
      
      
      #
      
      figname <- paste(resultdir,"dIR_dLP8T_",tbl_depl$SensorUnit_ID[id_depl[ith_depl]],"_",serialnumber,"_",tbl_depl$LocationName[id_depl[ith_depl]],"_wo_sc.pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.0,1,0.25,0.1),mfrow=c(1,1))
      
      d_LP8T_tmp <- seq(min(data$d_LP8T,na.rm=T),max(data$d_LP8T,na.rm=T),length.out = 1e3)
      pred_plot  <- as.numeric(predict(fit,data.frame(d_LP8T=d_LP8T_tmp,stringsAsFactors=T)))
      
      xlabString <- expression(paste(Delta,"LP8_T"))
      ylabString <- expression(paste(Delta,"LOG(IR)"))
      
      plot(data$d_LP8T[consec_meas],data$d_logIR[consec_meas],xlab=xlabString,ylab=ylabString,sub=descriptor, pch=16 ,cex=0.5,cex.main=1.75,cex.lab=1.75,cex.axis=1.75,cex.sub=1.25)
      points(data$d_LP8T[id_outlier_wo_sc],    data$d_logIR[id_outlier_wo_sc],                  pch=16 ,cex=0.5,col=2)
      points(data$d_LP8T[id_outlier_neg_wo_sc],data$d_logIR[id_outlier_neg_wo_sc],              pch=16 ,cex=0.5,col=3)
      lines(d_LP8T_tmp,pred_plot,col="orange",lwd=2)
      
      leg_str_00 <- paste("N measurements:",sprintf("%6.0f",length(which(!is.na(data$d_LP8T) & !is.na(data$d_logIR) & consec_meas))))
      leg_str_01 <- paste("N outlier pos: ",sprintf("%6.0f",length(id_outlier_wo_sc)))
      leg_str_02 <- paste("N outlier neg: ",sprintf("%6.0f",length(id_outlier_neg_wo_sc)))
      
      lines(c(-1e9,1e9),c(0,0),lwd=2,lty=5,col="gray70")
      lines(c(0,0),c(-1e9,1e9),lwd=2,lty=5,col="gray70")
      
      par(family="mono")
      legend("topleft",legend=c(leg_str_00,leg_str_01,leg_str_02),bg="white",cex=1.75,col=c(1,2,3),pch=15)
      par(family="")
      
      dev.off()
      par(def_par)
      
      #
      
      figname <- paste(resultdir,"dIR_dLP8T_",tbl_depl$SensorUnit_ID[id_depl[ith_depl]],"_",serialnumber,"_",tbl_depl$LocationName[id_depl[ith_depl]],"_HIST.pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.0,1,0.25,0.1),mfrow=c(1,1))
      
      xlabString <- expression(paste("Residual ",Delta,"LOG(IR) - fitted curve"))
      
      seq_res <- seq(min(res,na.rm=T)-0.01,max(res,na.rm=T)+0.01,0.00020)
      
      hist(res[consec_meas],seq_res,xlim=c(-0.005,0.005),main="", xlab=xlabString,sub=descriptor, col="slategray",cex.main=1.75,cex.lab=1.75,cex.axis=1.75,cex.sub=1.25)
      hist(res[id_outlier],     seq_res,col="red",  add=T)
      hist(res[id_outlier_neg], seq_res,col="green",add=T)
      
      lines(c( 3*mad_res_dry, 3*mad_res_dry),c(-1e9,1e9),lwd=2,col=2)
      lines(c(-3*mad_res_dry,-3*mad_res_dry),c(-1e9,1e9),lwd=2,col=2)
      
      leg_str_00 <- paste("Q000:",sprintf("%7.4f",quantile(res[consec_meas],probs=0.00,na.rm=T)))
      leg_str_01 <- paste("Q001:",sprintf("%7.4f",quantile(res[consec_meas],probs=0.01,na.rm=T)))
      leg_str_02 <- paste("Q050:",sprintf("%7.4f",quantile(res[consec_meas],probs=0.50,na.rm=T)))
      leg_str_03 <- paste("Q099:",sprintf("%7.4f",quantile(res[consec_meas],probs=0.99,na.rm=T)))
      leg_str_04 <- paste("Q100:",sprintf("%7.4f",quantile(res[consec_meas],probs=1.00,na.rm=T)))
      
      par(family="mono")
      legend("topleft",legend=c(leg_str_00,leg_str_01,leg_str_02,leg_str_03,leg_str_04),bg="white",cex=1.75)
      par(family="")
      
      
      dev.off()
      par(def_par)
      
    }
    
    
    
    
    # check measurements preceding an outlier
    
    id_outlier_followers <- NULL
    
    if(n_id_outlier>0){
      for(ith_id_outlier in 1:n_id_outlier){
        
        id_0 <- id_outlier[ith_id_outlier]
        id_1 <- id_0 + 1
        id_2 <- id_0 + outlier_period/600 + 10
        
        if(id_1>dim(data)[1]){
          id_1 <- dim(data)[1]
        }
        if(id_2>dim(data)[1]){
          id_2 <- dim(data)[1]
        }
        
        id   <- (id_1:id_2)[(data$timestamp[id_1:id_2]-data$timestamp[id_0])<=outlier_period]
        
        id_RH_below <- which(data$SHT21_RH[id]<0.5*(data$SHT21_RH[id_0]+data$SHT21_RH_P[id_0]))
        
        if(length(id_RH_below)>0){
          id_RH_below          <- id[id_RH_below]
          id_outlier_followers <- c(id_outlier_followers,c(id_1:min(id_RH_below)))
        }else{
          id_outlier_followers <- c(id_outlier_followers,id)
        }
      }
    }else{
      id_outlier <- NULL
    }
    
    # check measurements following an outlier
    
    id_outlier_neg_predecessor <- NULL
    
    if(n_id_outlier_neg>0){
      for(ith_id_outlier in 1:n_id_outlier_neg){
        
        id_2 <- id_outlier_neg[ith_id_outlier]
        id_1 <- id_2 - 1
        id_0 <- id_2 - outlier_period/600 - 10
        
        if(id_0<1){
          id_0 <- 1
        }
        if(id_1<1){
          id_1 <- 1
        }
        
        id   <- (id_0:id_1)[(data$timestamp[id_2]-data$timestamp[id_0:id_1])<=outlier_period]
        
        # id_RH_below <- which(data$SHT21_RH_P[id]<data$SHT21_RH[id_2])
        
        id_RH_below <- which(data$SHT21_RH[id]<0.5*(data$SHT21_RH[id_2]+data$SHT21_RH_P[id_2]))
        
        if(length(id_RH_below)>0){
          id_RH_below                <- id[id_RH_below]
          id_outlier_neg_predecessor <- c(id_outlier_neg_predecessor,c(max(id_RH_below):id_1))
        }else{
          id_outlier_neg_predecessor <- c(id_outlier_followers,id)
        }
      }
    }else{
      id_outlier_neg <- NULL
    }
    
    
    # Measurement to be flagged
    
    id_flag_00   <- sort(unique(c(id_outlier,id_outlier_followers,id_outlier_neg,id_outlier_neg_predecessor)))
    n_id_flag_00 <- length(id_flag_00)
    
    n_id_flag_01 <- length(which(! which(data$SHT21_RH>RH_max)            %in% id_flag_00))
    n_id_flag_02 <- length(which(! which((data$SHT21_T-data$DP)<T_DP_max) %in% id_flag_00))
    n_id_flag_03 <- length(which(! which(data$noisy & data$SHT21_RH>85)   %in% id_flag_00))
    n_id_flag_04 <- length(which(! which(data$large_d_logIR==T)           %in% id_flag_00))
    
    id_flag      <- sort(unique(c(id_outlier,id_outlier_followers,id_outlier_neg,id_outlier_neg_predecessor,which(data$SHT21_RH>RH_max),which((data$SHT21_T-data$DP)<T_DP_max),which(data$noisy & data$SHT21_RH>85),which(data$large_d_logIR==T))))
    
    
    if(CREATE_PLOTS){
      
      figname <- paste(resultdir,"SUMMARY_OUTLIER_RH_",tbl_depl$SensorUnit_ID[id_depl[ith_depl]],"_",serialnumber,"_",tbl_depl$LocationName[id_depl[ith_depl]],".pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.0,1,0.1,0.1),mfrow=c(1,1))
      
      xlabString <- "RH [%]"
      hist(data$SHT21_RH,         seq(0,120,2),xlim=c(0,104),col="cadetblue1",main="",xlab=xlabString,sub=descriptor,cex.lab=1.5,cex.axis=1.5,cex.sub=1.0)
      hist(data$SHT21_RH[id_flag],seq(0,120,2),xlim=c(0,104),col="coral",cex.lab=1.5,cex.axis=1.5,add=T)
      
      leg_str_00 <- paste("N MEAS:",sprintf("%6.0f",length(which(!is.na(data$d_LP8T) & !is.na(data$d_logIR)))))
      leg_str_01 <- paste("N FLAG:",sprintf("%6.0f",length(id_flag)))
      
      par(family="mono")
      legend("topleft",legend=c(leg_str_00,leg_str_01),bg="white",cex=1.25,pch=15,col=c("cadetblue1","coral"))
      par(family="")
      
      dev.off()
      par(def_par)
      
      #
      
      figname <- paste(resultdir,"IR_LP8T_",tbl_depl$SensorUnit_ID[id_depl[ith_depl]],"_",serialnumber,"_",tbl_depl$LocationName[id_depl[ith_depl]],".pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.0,1,0.1,0.1),mfrow=c(1,1))
      
      xlabString <- expression(paste("LP8_T"))
      ylabString <- expression(paste("LOG(IR)"))
      
      plot(data$LP8_T,           data$logIR,         xlab=xlabString,ylab=ylabString,sub=descriptor, pch=16 ,cex=0.5,cex.main=1.5,cex.lab=1.5,cex.axis=1.5,cex.sub=1)
      
      dev.off()
      par(def_par)
    }
    
    
    # Update database table
    
    query_str       <- paste("UPDATE ",DB_TABLE," SET O_FLAG = 1 WHERE SensorUnit_ID = ",tbl_depl$SensorUnit_ID[id_depl[ith_depl]]," and LocationName = '",tbl_depl$LocationName[id_depl[ith_depl]],"' and timestamp >= ",tbl_depl$timestamp_from[id_depl[ith_depl]]," and timestamp <= ",tbl_depl$timestamp_to[id_depl[ith_depl]],";",sep="")
    drv             <- dbDriver("MySQL")
    con<-carboutil::get_conn(group="CarboSense_MySQL")
    res             <- dbSendQuery(con, query_str)
    dbClearResult(res)
    dbDisconnect(con)
    
    
    #
    
    if(length(id_flag)>0){
      # query_str       <- paste("UPDATE ",DB_TABLE," SET O_FLAG = 0 WHERE SensorUnit_ID = ",tbl_depl$SensorUnit_ID[id_depl[ith_depl]]," and LocationName = '",tbl_depl$LocationName[id_depl[ith_depl]],"' and timestamp IN (",paste(data$timestamp[id_flag],collapse = ","),");",sep="")
      # drv             <- dbDriver("MySQL")
      # con<-carboutil::get_conn(group="CarboSense_MySQL")
      # res             <- dbSendQuery(con, query_str)
      # dbClearResult(res)
      # dbDisconnect(con)
      
      query_str <- paste("INSERT INTO ",DB_TABLE," (timestamp,SensorUnit_ID,LocationName,O_FLAG) ",sep="")
      query_str <- paste(query_str,"VALUES ")
      query_str <- paste(query_str,
                         paste("(",paste(data$timestamp[id_flag],",",
                                         rep(tbl_depl$SensorUnit_ID[id_depl[ith_depl]],length(id_flag)),",'",
                                         rep(tbl_depl$LocationName[id_depl[ith_depl]], length(id_flag)),"',",
                                         rep(0,length(id_flag)),
                                         collapse = "),(",sep=""),")",sep=""),
                         paste(" ON DUPLICATE KEY UPDATE "))
      
      query_str       <- paste(query_str,paste("O_FLAG=VALUES(O_FLAG);",    sep=""))
      
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn(group="CarboSense_MySQL")
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
    
    #
    
    statistics <- rbind(statistics,data.frame(serialnumber            = serialnumber,
                                              SensorUnit_ID           = tbl_depl$SensorUnit_ID[id_depl[ith_depl]],
                                              LocationName            = tbl_depl$LocationName[id_depl[ith_depl]],
                                              Date_UTC_from           = strftime(date_UTC_from,"%Y-%m-%d",tz="UTC"),
                                              Date_UTC_to             = strftime(date_UTC_to,  "%Y-%m-%d",tz="UTC"),
                                              N                       = dim(data)[1],
                                              n_id_flag               = length(id_flag),
                                              fraction_flagged        = length(id_flag)/dim(data)[1]*100,
                                              n_id_scatter            = length(id_scatter),
                                              QQQ_abs_log_IR_M_L      = QQQ_abs_log_IR_M_L,
                                              n_noisy                 = sum(data$noisy),
                                              fraction_noisy          = sum(data$noisy)/dim(data)[1]*100,
                                              QQQ_abs_d_logIR         = QQQ_abs_d_logIR,
                                              n_id_outlier_wo_sc      = n_id_outlier_wo_sc,
                                              n_id_outlier_neg_wo_sc  = n_id_outlier_neg_wo_sc,
                                              n_id_outlier            = n_id_outlier,
                                              n_id_outlier_neg        = n_id_outlier_neg,
                                              RH_max                  = RH_max,
                                              T_DP_max                = T_DP_max,
                                              n_id_flag_00            = n_id_flag_00,
                                              n_id_flag_01            = n_id_flag_01,
                                              n_id_flag_02            = n_id_flag_02,
                                              n_id_flag_03            = n_id_flag_03,
                                              n_id_flag_04            = n_id_flag_04,
                                              SysTime                 = strftime(Sys.time(),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                              stringsAsFactors = F))
  }
  
  write.table(statistics,paste(resultdir,"statistics.csv",sep=""),sep=";",col.names = F,row.names=F,append = T)
  
  return()
}

### ----------------------------------------------------------------------------------------------------------------------------


# Database queries

query_str       <- paste("SELECT * FROM Sensors WHERE Type = 'LP8';",sep="")
# query_str       <- paste("SELECT * FROM Sensors WHERE Type = 'LP8' and SensorUnit_ID = 1181;",sep="")
drv             <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_sensors     <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_sensors$Date_UTC_from  <- strptime(tbl_sensors$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_sensors$Date_UTC_to    <- strptime(tbl_sensors$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

tbl_sensors$timestamp_from <- as.numeric(difftime(time1=tbl_sensors$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
tbl_sensors$timestamp_to   <- as.numeric(difftime(time1=tbl_sensors$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))


#


if(COMP=="ALL"){
  query_str       <- paste("SELECT * FROM Deployment WHERE LocationName NOT IN ('DUE2','DUE3','DUE4','DUE5','MET1');",sep="")
}
if(COMP=="NO_DUE"){
  query_str       <- paste("SELECT * FROM Deployment WHERE LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1');",sep="")
}
if(COMP=="DUE"){
  query_str       <- paste("SELECT * FROM Deployment WHERE LocationName = 'DUE1';",sep="")
}

drv             <- dbDriver("MySQL")
con<-carboutil::get_conn(group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_depl        <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_depl$Date_UTC_from  <- strptime(tbl_depl$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_depl$Date_UTC_to    <- strptime(tbl_depl$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

tbl_depl$timestamp_from <- as.numeric(difftime(time1=tbl_depl$Date_UTC_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
tbl_depl$timestamp_to   <- as.numeric(difftime(time1=tbl_depl$Date_UTC_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))


### ----------------------------------------------------------------------------------------------------------------------------

cluster <- makeCluster(2)

#

clusterEvalQ(cluster, library(xts))
clusterEvalQ(cluster, library(DBI))
clusterEvalQ(cluster, library(RMySQL))
clusterEvalQ(cluster, library(chron))
clusterEvalQ(cluster, library(MASS))

#

results <- clusterMap(cluster, outlierDetection, serialnumber=tbl_sensors$Serialnumber, MoreArgs=list(DB_TABLE=DB_TABLE, tbl_depl=tbl_depl,tbl_sensors=tbl_sensors,outlier_period=outlier_period,dry_conditions=dry_conditions,resultdir=resultdir))

#

stopCluster(cluster)


### ----------------------------------------------------------------------------------------------------------------------------

statistics <- read.table(paste(resultdir,"statistics.csv",sep=""),sep=";",header=T)

id_use     <- which(!statistics$LocationName %in% c("DUE1","DUE2","DUE3","DUE4","DUE5","MET1","-999"))

figname    <- paste(resultdir,"FRACTION_FLAGGED_MEAS.pdf",sep="")

def_par <- par()
pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
par(mai=c(1.0,1,0.1,0.1),mfrow=c(1,1))

xlabString <- "Flagged measurements [%]"
hist(statistics$fraction_flagged[id_use],seq(0,100,5),xlim=c(0,100),col="slategray",main="",xlab=xlabString,cex.lab=1.5,cex.axis=1.5,cex.sub=1.0)

leg_str_00 <- paste("N DEPL: ",sprintf("%4.0f",length(id_use)),sep="")
leg_str_01 <- paste("Q010:   ",sprintf("%3.0f",quantile(statistics$fraction_flagged[id_use],probs=0.1)),"%",sep="")
leg_str_02 <- paste("Q050:   ",sprintf("%3.0f",quantile(statistics$fraction_flagged[id_use],probs=0.5)),"%",sep="")
leg_str_03 <- paste("Q090:   ",sprintf("%3.0f",quantile(statistics$fraction_flagged[id_use],probs=0.9)),"%",sep="")

par(family="mono")
legend("topright",legend=c(leg_str_00,leg_str_01,leg_str_02,leg_str_03),bg="white",cex=1.25)
par(family="")

dev.off()
par(def_par)

