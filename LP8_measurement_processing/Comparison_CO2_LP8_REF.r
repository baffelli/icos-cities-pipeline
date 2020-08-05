# Comparison_CO2_LP8_REF.r
# ------------------------

# Remarks:
# - Comparison of CO2 data from LP8 and reference instrument.
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


source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

### ----------------------------------------------------------------------------------------------------------------------------

## Functions

ma <- function(x,n=10){filter(x,rep(1/n,n), sides=1)}

### ------------------------------------------------------------------------------------------------------------------------------

## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

# 

if(length(args)!=2 | !args[2]%in%c("WET","DRY")){
  stop("ARG[2]: Specify if WET or DRY concentrations should be compared.")
}else{
  MoleFraction_WET_DRY <- args[2]
}

#

if(!as.integer(args[1])%in%c(0,1,2,3,20,30)){
  stop("ARG[1]: MODEL: 1, 2, 3, 20, 30 or 0 (= FINAL)!")
}else{
  
  # Final 
  if(as.integer(args[1])==0){
    # Directories
    if(MoleFraction_WET_DRY=="DRY"){
      resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_FINAL/Comparison_LP8_REF_DRY/"
    }else{
      resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_FINAL/Comparison_LP8_REF/"
    }
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_FINAL"
    # Sites to process
    ALL_SITES       <- c("BRM","LAEG","PAY","PAYN","RIG","HAE")
    
    eq_label        <- "(FINAL)"
  }
  
  # Standard 
  if(as.integer(args[1])==1){
    # Directories
    if(MoleFraction_WET_DRY=="DRY"){
      resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/Comparison_LP8_REF_DRY/" 
    }else{
      resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/Comparison_LP8_REF/" 
    }
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2"
    # Sites to process
    ALL_SITES       <- c("BRM","LAEG","PAY","PAYN","RIG","HAE","DUE1")
    
    eq_label        <- "(Eq. 4)"
  }
  
  # Test 00
  if(as.integer(args[1])==2){
    # Directories
    if(MoleFraction_WET_DRY=="DRY"){
      resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/Comparison_LP8_REF_DRY/"
    }else{
      resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/Comparison_LP8_REF/"
    }
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_TEST00"
    # Sites to process
    ALL_SITES       <- c("BRM","LAEG","PAY","PAYN","RIG","HAE","DUE1")
    
    eq_label        <- "(Eq. 5)"
  }
  
  # Test 01
  if(as.integer(args[1])==3){
    # Directories
    if(MoleFraction_WET_DRY=="DRY"){
      resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/Comparison_LP8_REF_DRY/"
    }else{
      resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/Comparison_LP8_REF/"
    }
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_TEST01"
    # Sites to process
    ALL_SITES       <- c("BRM","LAEG","PAY","PAYN","RIG","HAE","DUE1")
    
    eq_label        <- "(Eq. 4)"
  }
  
  # Test 00 AMT
  if(as.integer(args[1])==20){
    # Directories
    if(MoleFraction_WET_DRY=="DRY"){
      resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT/Comparison_LP8_REF_DRY/"
    }else{
      resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT/Comparison_LP8_REF/"
    }
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_TEST00_AMT"
    # Sites to process
    ALL_SITES       <- c("BRM","LAEG","PAY","PAYN","RIG","HAE","DUE1")
    
    eq_label        <- "(Eq. 5)"
  }
  
  # Test 01 AMT
  if(as.integer(args[1])==30){
    # Directories
    if(MoleFraction_WET_DRY=="DRY"){
      resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT/Comparison_LP8_REF_DRY/"
    }else{
      resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT/Comparison_LP8_REF/"
    }
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_TEST01_AMT"
    # Sites to process
    ALL_SITES       <- c("BRM","LAEG","PAY","PAYN","RIG","HAE","DUE1")
    
    eq_label        <- "(Eq. 4)"
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

statistics        <- NULL

### ----------------------------------------------------------------------------------------------------------------------------

for(SITE in ALL_SITES){
  
  ### ---------------------------------------------------
  
  tableName         <- paste("NABEL_",SITE,sep="")
  
  if(tableName=="NABEL_DUE1"){
    tableName <- "NABEL_DUE"
  }
  if(tableName=="NABEL_PAYN"){
    tableName <- "NABEL_PAY"
  }
  if(tableName=="NABEL_BRM"){
    tableName <- "UNIBE_BRM"
  }
  if(tableName=="NABEL_LAEG"){
    tableName <- "EMPA_LAEG"
  }
  
  
  # Compare pressure
  # ----------------
  
  if(!SITE%in%c("PAYN","PAY","BRM","LAEG")){
    
    # Import interpolated pressure
    
    query_str         <- paste("SELECT timestamp, pressure FROM PressureInterpolation WHERE LocationName='",SITE,"';",sep="")
    drv               <- dbDriver("MySQL")
    con <-carboutil::get_conn()
    res               <- dbSendQuery(con, query_str)
    tbl_pressure      <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    minTimestamp      <- min(tbl_pressure$timestamp)
    maxTimestamp      <- max(tbl_pressure$timestamp)
    
    # Import reference pressure
    
    query_str         <- paste("SELECT timestamp, pressure, pressure_F FROM ",tableName," WHERE timestamp >= ",minTimestamp," and timestamp <= ",maxTimestamp,";",sep="")
    drv               <- dbDriver("MySQL")
    con <-carboutil::get_conn()
    res               <- dbSendQuery(con, query_str)
    tbl_REF           <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    colnames(tbl_REF) <- c("timestamp","pressureREF","pressureREF_F")
    
    id_SetToNA <- which(tbl_REF$pressureREF== -999 | tbl_REF$pressureREF_F == 0)
    if(length(id_SetToNA)>0){
      tbl_REF$pressureREF[id_SetToNA]   <- NA
      tbl_REF$pressureREF_F[id_SetToNA] <- 0
    }
    
    # merge data
    
    data <- merge(tbl_REF,tbl_pressure)
    
    # ANALYSIS
    
    residuals   <- data$pressure-data$pressureREF
    
    ok          <- !is.na(data$pressureREF) & !is.na(data$pressure)
    
    id_FLAG_0   <- which(ok)
    n_id_FLAG_0 <- length(id_FLAG_0)
    
    RMSE_0      <- sqrt(sum((residuals[id_FLAG_0])^2)/n_id_FLAG_0)
    COR_0       <- cor(x = data$pressure[id_FLAG_0],y = data$pressureREF[id_FLAG_0],method = "pearson",use = "complete.obs")
    
    fit_0       <- lm(pressure~pressureREF,data[id_FLAG_0,])
    
    # PLOT: SCATTER/HIST
    
    str_L_00 <- paste("SLOPE:",sprintf("%6.2f",fit_0$coefficients[2]))
    str_L_01 <- paste("INTER:",sprintf("%6.2f",fit_0$coefficients[1]))
    str_L_02 <- paste("RMSE :",sprintf("%6.1f",RMSE_0))
    str_L_03 <- paste("COR  :",sprintf("%6.1f",COR_0))
    str_L_04 <- paste("N    :",sprintf("%6.0f",n_id_FLAG_0))
    
    
    str_R_00 <- paste("MEAN A:",sprintf("%6.1f",mean(residuals[id_FLAG_0])))
    str_R_01 <- paste("Q001 A:",sprintf("%6.1f",quantile(residuals[id_FLAG_0],probs=0.01)))
    str_R_02 <- paste("Q050 A:",sprintf("%6.1f",quantile(residuals[id_FLAG_0],probs=0.50)))
    str_R_03 <- paste("Q099 A:",sprintf("%6.1f",quantile(residuals[id_FLAG_0],probs=0.99)))
    
    #
    
    figname <- paste(resultdir,SITE,"_pressure.pdf",sep="")
    
    mainStr <- paste(SITE,sep="")
    subStr  <- paste("Data: ",
                     strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+min(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),
                     "-",
                     strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+max(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),sep="")
    
    def_par <- par()
    pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,2))
    
    yrange <- range(c(data$pressure,data$pressure_F),na.rm=T)
    xrange <- yrange
    
    plot(0,0,xlim=xrange,ylim=yrange,main=mainStr,xlab="Pressure REF [hPa]",ylab="Pressure interp [hPa]",sub=subStr,cex.axis=1.25,cex.lab=1.25,cex.main=1.25)
    
    if(n_id_FLAG_0>0){
      points(data$pressureREF[id_FLAG_0],data$pressure[id_FLAG_0],col="black",pch=16,cex=0.5)
    }
    
    lines(c(0,1e4),c(fit_0$coefficients[1],fit_0$coefficients[1]+1e4*fit_0$coefficients[2]),col=2)
    
    lines(c(0,1e4),c(0,1e4),col=1)
    
    
    par(family="mono")
    legend("bottomright",legend=c(str_L_00,str_L_01,"",str_L_02,str_L_03,str_L_04),bg="white",cex=1.25)
    par(family="")
    
    #
    
    Q050_S <- quantile(residuals[id_FLAG_0],probs=0.50)
    MEAN_S <- mean(residuals[id_FLAG_0])
    
    hist(residuals[id_FLAG_0],seq(floor(min(residuals[id_FLAG_0])),ceiling(max(residuals[id_FLAG_0])),0.5),xlim=c(Q050_S-10,Q050_S+10),col="slategray",main="",xlab="Pressure interp - pressure REF [hPa]",ylab="Number",cex.axis=1.25,cex.lab=1.25)
    lines(c(MEAN_S,MEAN_S),c(-1e9,1e9),col=2,lwd=2)
    par(family="mono")
    legend("topright",legend=c(str_R_00,str_R_01,str_R_02,str_R_03),bg="white",cex=1.25)
    par(family="")
    
    #
    
    dev.off()
    par(def_par)
    
    #
    
    rm(tbl_pressure,tbl_REF,data)
    rm(xrange,yrange,Q050_S,MEAN_S)
    rm(id_SetToNA)
    rm(residuals,ok,id_FLAG_0,n_id_FLAG_0,RMSE_0,COR_0,fit_0,str_L_00,str_L_01,str_L_02,str_L_03,str_L_04,str_R_00,str_R_01,str_R_02,str_R_03)
    gc()
    
  }
  ### ---------------------------------------------------
  
  # Compare CO2 measurements
  # ------------------------
  
  
  if(!SITE%in%c("XXXX")){
    
    
    # Select SensorUnit_IDs
    query_str         <- paste("SELECT DISTINCT SensorUnit_ID FROM ",ProcDataTblName," WHERE LocationName='",SITE,"';",sep="")
    drv               <- dbDriver("MySQL")
   con <-carboutil::get_conn()
    res               <- dbSendQuery(con, query_str)
    tbl_CO2_SU        <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    # AMT PAPER
    
    if(as.integer(args[1])%in%c(20,30)){
      if(SITE == "BRM"){
        tbl_CO2_SU <- data.frame(SensorUnit_ID = 1013,
                                 stringsAsFactors = F)
      }
      if(SITE == "HAE"){
        tbl_CO2_SU <- data.frame(SensorUnit_ID = c(1182,1218,1219,1223,1314),
                                 stringsAsFactors = F)
      }
    }
    
    #
    
    u_SensorUnit_ID   <- sort(unique(tbl_CO2_SU$SensorUnit_ID))
    n_u_SensorUnit_ID <- length(u_SensorUnit_ID)
    
    rm(tbl_CO2_SU)
    gc()
    
    for(ith_SU in 1:n_u_SensorUnit_ID){
      
      
      # Import processed LP8 data
      
      query_str         <- paste("SELECT * FROM ",ProcDataTblName," WHERE LocationName='",SITE,"' and SensorUnit_ID=",u_SensorUnit_ID[ith_SU],";",sep="")
      drv               <- dbDriver("MySQL")
      con <-carboutil::get_conn()
      res               <- dbSendQuery(con, query_str)
      tbl_CO2           <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      #
      
      tbl_CO2      <- tbl_CO2[order(tbl_CO2$timestamp),]
      minTimestamp <- min(tbl_CO2$timestamp)
      maxTimestamp <- max(tbl_CO2$timestamp)
      
      #
      
      if(MoleFraction_WET_DRY=="DRY"){
        id <- which(tbl_CO2$CO2_A != -999 & tbl_CO2$H2O != -999)
        if(length(id)>0){
          tbl_CO2$CO2_A[id] <- tbl_CO2$CO2_A[id] / (1 - tbl_CO2$H2O[id]/100)
        }
        
        id <- which(tbl_CO2$CO2_A != -999 & tbl_CO2$H2O != -999)
        if(length(id)>0){
          tbl_CO2$CO2[id]   <- tbl_CO2$CO2[id]   / (1 - tbl_CO2$H2O[id]/100)
        }
        
        rm(id)
        gc()
      }
      
      
      # Import reference data 
      # 
      # (Import, DRY/WET, averaging to 10 min intervals)
      
      if(!tableName %in% c("UNIBE_BRM","EMPA_LAEG")){
        
        if(tableName=="NABEL_HAE"){
          query_str <- paste("SELECT timestamp, CO2_WET_COMP, CO2_DRY_CAL, H2O FROM ",tableName," ",sep="")
          query_str <- paste(query_str,"WHERE timestamp >= ",minTimestamp," and timestamp <= ",maxTimestamp," ",sep="")
          query_str <- paste(query_str,"AND CO2_WET_COMP != -999;",sep="")
        }
        if(tableName%in%c("NABEL_PAY","NABEL_RIG","NABEL_DUE")){
          query_str <- paste("SELECT timestamp, CO2_DRY_CAL, CO2_WET_COMP FROM ",tableName," WHERE timestamp >= ",minTimestamp," and timestamp <= ",maxTimestamp,";",sep="")
        }
        drv               <- dbDriver("MySQL")
       con <-carboutil::get_conn()
        res               <- dbSendQuery(con, query_str)
        tbl_REF           <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        tbl_REF <- tbl_REF[order(tbl_REF$timestamp),]
        
        #
        
        if(tableName=="NABEL_HAE"){
          
          tbl_REF$CO2 <- NA
          
          id_HAE_LICOR   <- which(tbl_REF$timestamp <= 1583935200 & tbl_REF$H2O != -999)
          id_HAE_PICARRO <- which(tbl_REF$timestamp >  1583935200 & tbl_REF$CO2_DRY_CAL != -999 & tbl_REF$H2O != -999)
          
          if(MoleFraction_WET_DRY=="WET"){
            tbl_REF$CO2 <- tbl_REF$CO2_WET_COMP
          }
          if(MoleFraction_WET_DRY=="DRY"){
            if(length(id_HAE_LICOR)>0){
              tbl_REF$CO2[id_HAE_LICOR] <- tbl_REF$CO2_WET_COMP[id_HAE_LICOR] / (1 - tbl_REF$H2O[id_HAE_LICOR]/100)
            }
            if(length(id_HAE_PICARRO)>0){
              tbl_REF$CO2[id_HAE_PICARRO] <- tbl_REF$CO2_DRY_CAL[id_HAE_PICARRO]
            }
          }
        }
        
        if(tableName%in%c("NABEL_RIG","NABEL_PAY","NABEL_DUE")){
          
          if(MoleFraction_WET_DRY=="DRY"){
            tbl_REF$CO2 <- tbl_REF$CO2_DRY_CAL
          }else{
            tbl_REF$CO2 <- tbl_REF$CO2_WET_COMP
          }
          
          id_setToNA <- which(tbl_REF$CO2_DRY_CAL == -999 | tbl_REF$CO2_WET_COMP == -999)
          if(length(id_setToNA)>0){
            tbl_REF$CO2[id_setToNA]     <- NA
          }
        }
        
        #
        
        dT          <- c(60,round(diff(tbl_REF$timestamp)))
        dt_nok      <- ma(dT,10)!=60
        
        tmp         <- ma(tbl_REF$CO2,10)
        tmp[dt_nok] <- NA
        tbl_REF     <- data.frame(timestamp=tbl_REF$timestamp-600, CO2_10MIN_AV = tmp,stringsAsFactors = F)
        
        data <- merge(tbl_REF,tbl_CO2)
        
        #
        
        rm(dT,dt_nok,tmp,tbl_REF,id_setToNA)
        gc
        
      }
      
      if(tableName=="UNIBE_BRM"){
        query_str         <- paste("SELECT timestamp, CO2, CO2_DRY FROM ",tableName," WHERE MEAS_HEIGHT = 12 and CO2_DRY_N > 5 and CO2 != -999 and timestamp >= ",minTimestamp," and timestamp <= ",maxTimestamp,";",sep="")
        drv               <- dbDriver("MySQL")
       con <-carboutil::get_conn()
        res               <- dbSendQuery(con, query_str)
        tbl_REF           <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        if(dim(tbl_REF)[1]==0){
          next
        }
        
        #
        
        if(MoleFraction_WET_DRY=="DRY"){
          tbl_REF <- tbl_REF[,c(which(colnames(tbl_REF)=="timestamp"), which(colnames(tbl_REF)=="CO2_DRY"))]
        }else{
          tbl_REF <- tbl_REF[,c(which(colnames(tbl_REF)=="timestamp"), which(colnames(tbl_REF)=="CO2"))]
        }
        colnames(tbl_REF) <- c("timestamp","CO2")
        
        #
        
        tbl_REF <- merge(data.frame(timestamp=seq(min(tbl_REF$timestamp),max(tbl_REF$timestamp),60),stringsAsFactors = F),
                         tbl_REF,by="timestamp",all.x=T)
        
        tmp     <- matrix(NA,ncol=10,nrow=dim(tbl_REF)[1])
        tmp[,1] <- tbl_REF$CO2
        for(ii in 1:9){
          tmp[,ii+1] <- c(rep(NA,ii),tbl_REF$CO2[1:(dim(tbl_REF)[1]-ii)])
        }
        
        tmp     <- apply(tmp,1,mean,na.rm=T)
        id_tmp  <- which(!is.na(tmp))
        data    <- merge(data.frame(timestamp=tbl_REF$timestamp[id_tmp]-600,CO2_10MIN_AV=tmp[id_tmp],stringsAsFactors = F),tbl_CO2,by="timestamp")
        
        rm(tbl_REF,id_tmp,tmp)
        gc()
      }
      
      if(tableName=="EMPA_LAEG"){
        query_str         <- paste("SELECT timestamp, CO2, CO2_DRY FROM ",tableName," WHERE VALVEPOS = 0 and CO2_DRY_N > 5 and CO2 != -999 and timestamp >= ",minTimestamp," and timestamp <= ",maxTimestamp,";",sep="")
        drv               <- dbDriver("MySQL")
       con <-carboutil::get_conn()
        res               <- dbSendQuery(con, query_str)
        tbl_REF           <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        if(dim(tbl_REF)[1]==0){
          next
        }
        
        #
        
        if(MoleFraction_WET_DRY=="DRY"){
          tbl_REF <- tbl_REF[,c(which(colnames(tbl_REF)=="timestamp"), which(colnames(tbl_REF)=="CO2_DRY"))]
        }else{
          tbl_REF <- tbl_REF[,c(which(colnames(tbl_REF)=="timestamp"), which(colnames(tbl_REF)=="CO2"))]
        }
        colnames(tbl_REF) <- c("timestamp","CO2")
        
        #
        
        tbl_REF <- merge(data.frame(timestamp=seq(min(tbl_REF$timestamp),max(tbl_REF$timestamp),60),stringsAsFactors = F),
                         tbl_REF,by="timestamp",all.x=T)
        
        tmp     <- matrix(NA,ncol=10,nrow=dim(tbl_REF)[1])
        tmp[,1] <- tbl_REF$CO2
        for(ii in 1:9){
          tmp[,ii+1] <- c(rep(NA,ii),tbl_REF$CO2[1:(dim(tbl_REF)[1]-ii)])
        }
        
        tmp     <- apply(tmp,1,mean,na.rm=T)
        id_tmp  <- which(!is.na(tmp))
        data    <- merge(data.frame(timestamp=tbl_REF$timestamp[id_tmp]-600,CO2_10MIN_AV=tmp[id_tmp],stringsAsFactors = F),tbl_CO2,by="timestamp")
        
        rm(tbl_REF,id_tmp,tmp)
        gc()
      }
      
      #
      
      data$date     <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
      
      #
      
      Data_SU_REF_ok <- !is.na(data$CO2_10MIN_AV) & !is.na(data$CO2) & data$CO2 != -999
      
      
      # Aggregation 
      
      weeks     <- floor(as.numeric(difftime(time1=data$date,time2=strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC"),units="weeks",tz="UTC")))
      u_weeks   <- sort(unique(weeks))
      n_u_weeks <- length(u_weeks)
      
      # ANALYSIS
      
      for(mode in c(1,2,3,4,5,6)){
        
        for(ith_week in 0:n_u_weeks){
          
          #
          
          if(ith_week==0){
            week_ok   <- rep(T,dim(data)[1])
            week_mode <- 0
          }else{
            week_ok   <- weeks == u_weeks[ith_week]
            week_mode <- u_weeks[ith_week]
          }
          
          #
          
          N_data_SU_REF <- sum(Data_SU_REF_ok[week_ok])
          
          #
          
          FRAC_RH_80 <- length(which(data$SHT21_RH[week_ok] >= 80))/sum(week_ok)*1e2
          FRAC_RH_85 <- length(which(data$SHT21_RH[week_ok] >= 85))/sum(week_ok)*1e2
          FRAC_RH_90 <- length(which(data$SHT21_RH[week_ok] >= 90))/sum(week_ok)*1e2
          
          if(mode==1){
            data$measurements <- data$CO2
            residuals         <- data$CO2-data$CO2_10MIN_AV
            ok                <- !is.na(data$CO2_10MIN_AV) & !is.na(data$CO2) & week_ok
          }
          if(mode==2){
            data$measurements <- data$CO2_A
            residuals         <- data$CO2_A-data$CO2_10MIN_AV
            ok                <- !is.na(data$CO2_10MIN_AV) & !is.na(data$CO2_A) & data$CO2_A != -999 & week_ok
          }
          if(mode==3){
            data$measurements <- data$CO2_A
            residuals         <- data$CO2_A-data$CO2_10MIN_AV
            ok                <- !is.na(data$CO2_10MIN_AV) & !is.na(data$CO2_A) & data$CO2_A != -999 & week_ok
          }
          if(mode==4){
            data$measurements <- data$CO2_A
            residuals         <- data$CO2_A-data$CO2_10MIN_AV
            ok                <- !is.na(data$CO2_10MIN_AV) & !is.na(data$CO2_A) & data$CO2_A != -999 & week_ok
          }
          if(mode==5){
            data$measurements <- data$CO2_A
            residuals         <- data$CO2_A-data$CO2_10MIN_AV
            ok                <- !is.na(data$CO2_10MIN_AV) & !is.na(data$CO2_A) & data$CO2_A != -999 & week_ok
          }
          if(mode==6){
            data$measurements <- data$CO2_A
            residuals         <- data$CO2_A-data$CO2_10MIN_AV
            ok                <- !is.na(data$CO2_10MIN_AV) & !is.na(data$CO2_A) & data$CO2_A != -999 & week_ok
          }
          
          
          if(mode %in% c(1,2)){
            id_FLAG_0   <- which(ok)
            id_FLAG_1   <- which(ok & data$FLAG==1)
            
            n_id_FLAG_0 <- length(id_FLAG_0)
            n_id_FLAG_1 <- length(id_FLAG_1)
          }
          if(mode %in%c(3)){
            id_FLAG_0   <- which(ok)
            id_FLAG_1   <- which(ok & data$Q_FLAG==1)
            
            n_id_FLAG_0 <- length(id_FLAG_0)
            n_id_FLAG_1 <- length(id_FLAG_1)
          }
          if(mode %in%c(4)){
            id_FLAG_0   <- which(ok)
            id_FLAG_1   <- which(ok & data$O_FLAG==1)
            
            n_id_FLAG_0 <- length(id_FLAG_0)
            n_id_FLAG_1 <- length(id_FLAG_1)
          }
          if(mode %in%c(5)){
            id_FLAG_0   <- which(ok)
            id_FLAG_1   <- which(ok & data$O_FLAG==1 & data$L_FLAG==1)
            
            n_id_FLAG_0 <- length(id_FLAG_0)
            n_id_FLAG_1 <- length(id_FLAG_1)
          }
          if(mode %in%c(6)){
            id_FLAG_0   <- which(ok)
            id_FLAG_1   <- which(ok & data$FLAG==1 & data$SHT21_RH <= 85)
            
            n_id_FLAG_0 <- length(id_FLAG_0)
            n_id_FLAG_1 <- length(id_FLAG_1)
          }
          
          RMSE_0 <- NA
          RMSE_1 <- NA
          RMSE_C <- NA
          COR_0  <- NA
          COR_1  <- NA
          fit_1  <- NULL
          c1     <- NA
          c2     <- NA
          MEAN   <- NA
          MEAN_A <- NA
          
          if(n_id_FLAG_0>2){
            RMSE_0 <- sqrt(sum((residuals[id_FLAG_0])^2)/n_id_FLAG_0)
            COR_0  <- cor(x = data$measurements[id_FLAG_0],y = data$CO2_10MIN_AV[id_FLAG_0],method = "pearson",use = "complete.obs")
            
            Q000_A <- quantile(residuals[id_FLAG_0],probs=0.00)
            Q001_A <- quantile(residuals[id_FLAG_0],probs=0.01)
            Q005_A <- quantile(residuals[id_FLAG_0],probs=0.05)
            Q050_A <- quantile(residuals[id_FLAG_0],probs=0.50)
            Q095_A <- quantile(residuals[id_FLAG_0],probs=0.95)
            Q099_A <- quantile(residuals[id_FLAG_0],probs=0.99)
            Q100_A <- quantile(residuals[id_FLAG_0],probs=1.00)
            
            MEAN_A <- mean(residuals[id_FLAG_0])
          }else{
            RMSE_0 <- NA
            COR_0  <- NA
            Q000_A <- NA
            Q001_A <- NA
            Q005_A <- NA
            Q050_A <- NA
            Q095_A <- NA
            Q099_A <- NA
            Q100_A <- NA
            MEAN_A <- NA
          }
          
          if(n_id_FLAG_1>2){
            RMSE_1 <- sqrt(sum((residuals[id_FLAG_1])^2)/n_id_FLAG_1)
            COR_1  <- cor(x = data$measurements[id_FLAG_1],y = data$CO2_10MIN_AV[id_FLAG_1],method = "pearson",use = "complete.obs")
            fit_1  <- lm(measurements~CO2_10MIN_AV,data[id_FLAG_1,])
            c1     <- fit_1$coefficients[1]
            c2     <- fit_1$coefficients[2]
            
            residuals_centered <- data$measurements[id_FLAG_1] - (data$CO2_10MIN_AV[id_FLAG_1] - (mean(data$CO2_10MIN_AV[id_FLAG_1]) - mean(data$measurements[id_FLAG_1])))
            
            RMSE_C <- sqrt(sum((residuals_centered)^2)/n_id_FLAG_1)
            
            Q000 <- quantile(residuals[id_FLAG_1],probs=0.00)
            Q001 <- quantile(residuals[id_FLAG_1],probs=0.01)
            Q005 <- quantile(residuals[id_FLAG_1],probs=0.05)
            Q050 <- quantile(residuals[id_FLAG_1],probs=0.50)
            Q095 <- quantile(residuals[id_FLAG_1],probs=0.95)
            Q099 <- quantile(residuals[id_FLAG_1],probs=0.99)
            Q100 <- quantile(residuals[id_FLAG_1],probs=1.00)
            
            MEAN <- mean(residuals[id_FLAG_0])
          }else{
            RMSE_1 <- NA
            COR_1  <- NA
            fit_1  <- NA
            c1     <- NA
            c2     <- NA
            RMSE_C <- NA
            Q000  <- NA
            Q001  <- NA
            Q005  <- NA
            Q050  <- NA
            Q095  <- NA
            Q099  <- NA
            Q100  <- NA
            MEAN  <- NA
          }
          
          statistics <- rbind(statistics,data.frame(LocationName  = SITE,
                                                    SensorUnit_ID = u_SensorUnit_ID[ith_SU],
                                                    MODE          = mode,
                                                    WEEK          = week_mode,
                                                    Date_UTC_from = strftime(min(data$date[id_FLAG_0]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                    Date_UTC_to   = strftime(max(data$date[id_FLAG_0]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                    N             = n_id_FLAG_1,
                                                    N_ALL         = n_id_FLAG_0,
                                                    N_data_SU_REF = N_data_SU_REF,
                                                    FRACTION_OK_F = n_id_FLAG_1/n_id_FLAG_0*1e2,
                                                    FRACTION_OK_A = n_id_FLAG_0/N_data_SU_REF*1e2,
                                                    FRACTION_OK_T = n_id_FLAG_1/N_data_SU_REF*1e2,
                                                    FRAC_RH_80    = FRAC_RH_80,
                                                    FRAC_RH_85    = FRAC_RH_85,
                                                    FRAC_RH_90    = FRAC_RH_90,
                                                    RMSE          = RMSE_1,
                                                    RMSE_ALL      = RMSE_0,
                                                    RMSE_P        = RMSE_C,
                                                    COR           = COR_1,
                                                    COR_ALL       = COR_0,
                                                    SLOPE         = c2,
                                                    INTERCEPT     = c1,
                                                    MEAN          = MEAN,
                                                    Q000          = Q000,
                                                    Q001          = Q001,
                                                    Q005          = Q005,
                                                    Q050          = Q050,
                                                    Q095          = Q095,
                                                    Q099          = Q099,
                                                    Q100          = Q100,
                                                    MEAN_ALL      = MEAN_A,
                                                    Q000_ALL      = Q000_A,
                                                    Q001_ALL      = Q001_A,
                                                    Q005_ALL      = Q005_A,
                                                    Q050_ALL      = Q050_A,
                                                    Q095_ALL      = Q095_A,
                                                    Q099_ALL      = Q099_A,
                                                    Q100_ALL      = Q100_A,
                                                    stringsAsFactors=F))
          
          
          if(week_mode!=0){
            next
          }
          
          # PLOT: SCATTER/HIST
          
          str_L_00 <- paste("SLOPE:    ",sprintf("%6.2f",c2))
          str_L_01 <- paste("INTERCEPT:",sprintf("%6.2f",c1))
          str_L_02 <- paste("RMSE:     ",sprintf("%6.1f",RMSE_1))
          str_L_03 <- paste("RMSE P:   ",sprintf("%6.1f",RMSE_C))
          str_L_04 <- paste("COR:      ",sprintf("%6.1f",COR_1))
          str_L_05 <- paste("N:        ",sprintf("%6.0f",n_id_FLAG_1))
          str_L_06 <- paste("N ALL:    ",sprintf("%6.0f",n_id_FLAG_0))
          
          str_L_00_SC <- paste("SLOPE:",sprintf("%6.2f",c2))
          str_L_02_SC <- paste("RMSE: ",sprintf("%6.1f",RMSE_1))
          str_L_04_SC <- paste("COR:  ",sprintf("%6.1f",COR_1))
          str_L_05_SC <- paste("N:    ",sprintf("%6.0f",n_id_FLAG_1))
          str_L_06_SC <- paste("N ALL:",sprintf("%6.0f",n_id_FLAG_0))
          
          
          str_R_00 <- paste("MEAN:",sprintf("%6.1f",mean(residuals[id_FLAG_1])))
          str_R_01 <- paste("Q000:",sprintf("%6.1f",quantile(residuals[id_FLAG_1],probs=0.00)))
          str_R_02 <- paste("Q001:",sprintf("%6.1f",quantile(residuals[id_FLAG_1],probs=0.01)))
          str_R_03 <- paste("Q050:",sprintf("%6.1f",quantile(residuals[id_FLAG_1],probs=0.50)))
          str_R_04 <- paste("Q099:",sprintf("%6.1f",quantile(residuals[id_FLAG_1],probs=0.99)))
          str_R_05 <- paste("Q100:",sprintf("%6.1f",quantile(residuals[id_FLAG_1],probs=1.00)))
          str_R_06 <- paste("N:   ",sprintf("%6.0f",n_id_FLAG_1))
          str_R_07 <- paste("RMSE:",sprintf("%6.1f",RMSE_1))
          
          
          # SCATTER + HIST
          
          figname <- paste(resultdir,SITE,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),".pdf",sep="")
          
          mainStr <- paste(SITE," ",u_SensorUnit_ID[ith_SU],sep="")
          subStr  <- paste("Data: ",
                           strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+min(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),
                           "-",
                           strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+max(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),sep="")
          
          def_par <- par()
          pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,2))
          
          if(MoleFraction_WET_DRY=="DRY"){
            xlabString <- bquote(CO[2*","*DRY] ~ .(paste("REF [ppm]")))
            ylabString <- bquote(CO[2*","*DRY] ~ .(paste("LP8 [ppm]")))
          }else{
            xlabString <- bquote(CO[2] ~ .(paste("REF [ppm]")))
            ylabString <- bquote(CO[2] ~ .(paste("LP8 [ppm]")))
          }
          
          plot(0,0,xlim=c(350,700),ylim=c(350,700),main=mainStr,xlab=xlabString,ylab=ylabString,sub=subStr,cex.axis=1.25,cex.lab=1.25,cex.main=1.25)
          
          if(any(data$FLAG==0)){
            points(data$CO2_10MIN_AV[id_FLAG_0],data$measurements[id_FLAG_0],col="gray70",pch=16,cex=0.5)
          }
          if(any(data$FLAG==1)){
            points(data$CO2_10MIN_AV[id_FLAG_1],data$measurements[id_FLAG_1],col="black",pch=16,cex=0.5)
          }
          
          lines(c(0,1e4),c(c1,c1+1e4*c2),col=2)
          
          if(F){
            lines(c(0,1e4),c(0,1e4),   col=1)
            lines(c(0,1e4),c(0,1e4)+20,col=1,lty=50)
            lines(c(0,1e4),c(0,1e4)-20,col=1,lty=50)
          }else{
            lines(c(0,1e4),c(0,1e4),   col=1)
            lines(c(0,1e4),c(0,1e4)+RMSE_1,col=1,lty=50)
            lines(c(0,1e4),c(0,1e4)-RMSE_1,col=1,lty=50)
          }
          par(family="mono")
          legend("bottomright",legend=c(str_L_00,str_L_01,"",str_L_02,str_L_03,str_L_04,str_L_05,str_L_06),bg="white",cex=1.25)
          par(family="")
          
          #
          
          Q050_S <- quantile(residuals[id_FLAG_1],probs=0.50)
          MEAN_S <- mean(residuals[id_FLAG_1])
          
          if(n_id_FLAG_1>2){
            
            if(MoleFraction_WET_DRY=="DRY"){
              xlabString <- bquote(CO[2*","*DRY] ~ .("LP8") ~ "-" ~ CO[2*","*DRY] ~ .("REF [ppm]"))
            }else{
              xlabString <- bquote(CO[2] ~ .("LP8") ~ "-" ~ CO[2] ~ .("REF [ppm]"))
            }
            
            hist(residuals[id_FLAG_1],seq(floor(min(residuals[id_FLAG_1])),ceiling(max(residuals[id_FLAG_1]))+2,2),xlim=c(Q050_S-40,Q050_S+40),col="slategray",main="",xlab=xlabString,ylab="Number",cex.axis=1.25,cex.lab=1.25)
            
            
            lines(c(MEAN_S,MEAN_S),c(-1e9,1e9),col=2,lwd=3)
            if(F){
              lines(c(MEAN_S+20,MEAN_S+20),c(-1e9,1e9),col=2,lwd=3,lty=5)
              lines(c(MEAN_S-20,MEAN_S-20),c(-1e9,1e9),col=2,lwd=3,lty=5)
            }else{
              lines(c(MEAN_S+RMSE_1,MEAN_S+RMSE_1),c(-1e9,1e9),col=2,lwd=3,lty=5)
              lines(c(MEAN_S-RMSE_1,MEAN_S-RMSE_1),c(-1e9,1e9),col=2,lwd=3,lty=5)
            }
            if(F){
              par(family="mono")
              legend("topright",legend=c(str_R_00,str_R_01,str_R_02,str_R_03,str_R_04,str_R_05),bg="white",cex=1.25)
              par(family="")
            }else{
              par(family="mono")
              legend("topright",legend=c(str_R_00,str_R_07),bg="white",cex=1.25)
              par(family="")
            }
          }else{
            plot.new()
          }
          
          #
          
          dev.off()
          par(def_par)
          
          
          # --- SCATTER 
          
          figname <- paste(resultdir,SITE,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_SCATTER.pdf",sep="")
          
          mainStr <- paste(u_SensorUnit_ID[ith_SU]," @ ",SITE," ",eq_label,sep="")
          
          if(u_SensorUnit_ID[ith_SU]%in%c(1081,1100,1139,1201,1210) & SITE=="PAYN"){
            mainStr <- paste(u_SensorUnit_ID[ith_SU]," @ PAY ",eq_label,sep="")  
          }
          if(SITE=="DUE1"){
            mainStr <- paste(u_SensorUnit_ID[ith_SU]," @ DUE ",eq_label,sep="")  
          }
          
          
          subStr  <- paste("Data: ",
                           strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+min(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),
                           "-",
                           strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+max(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),sep="")
          
          def_par <- par()
          pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,1))
          
          if(MoleFraction_WET_DRY=="DRY"){
            xlabString <- bquote(CO[2*","*DRY] ~ .(paste("REF [ppm]")))
            ylabString <- bquote(CO[2*","*DRY] ~ .(paste("LP8 [ppm]")))
          }else{
            xlabString <- bquote(CO[2] ~ .(paste("REF [ppm]")))
            ylabString <- bquote(CO[2] ~ .(paste("LP8 [ppm]")))
          }
          
          plot(0,0,xlim=c(350,700),ylim=c(350,700),main=mainStr,xlab=xlabString,ylab=ylabString,cex.axis=1.75,cex.lab=1.75,cex.main=1.75)
          
          if(any(data$FLAG==0)){
            points(data$CO2_10MIN_AV[id_FLAG_0],data$measurements[id_FLAG_0],col="gray70",pch=16,cex=0.5)
          }
          if(any(data$FLAG==1)){
            points(data$CO2_10MIN_AV[id_FLAG_1],data$measurements[id_FLAG_1],col="black",pch=16,cex=0.5)
          }
          
          lines(c(0,1e4),c(c1,c1+1e4*c2),col=2)
          
          if(F){
            lines(c(0,1e4),c(0,1e4),   col=1)
            lines(c(0,1e4),c(0,1e4)+20,col=1,lty=50)
            lines(c(0,1e4),c(0,1e4)-20,col=1,lty=50)
          }else{
            lines(c(0,1e4),c(0,1e4),   col=1)
            lines(c(0,1e4),c(0,1e4)+RMSE_1,col=1,lty=50)
            lines(c(0,1e4),c(0,1e4)-RMSE_1,col=1,lty=50)
          }
          
          mtext(text = subStr,side = 1,line = 4.5,cex=1.5)
          
          par(family="mono")
          legend("bottomright",legend=c(str_L_00_SC,str_L_02_SC,str_L_04_SC,str_L_05_SC,str_L_06_SC),bg="white",cex=1.75)
          par(family="")
          
          
          dev.off()
          par(def_par)
          
          # --- HIST
          
          figname <- paste(resultdir,SITE,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_HIST.pdf",sep="")
          
          mainStr <- paste(SITE," ",u_SensorUnit_ID[ith_SU],sep="")
          subStr  <- paste("Data: ",
                           strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+min(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),
                           "-",
                           strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+max(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),sep="")
          
          def_par <- par()
          pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,1))
          
          Q050_S <- quantile(residuals[id_FLAG_1],probs=0.50)
          MEAN_S <- mean(residuals[id_FLAG_1])
          
          if(n_id_FLAG_1>2){
            
            if(MoleFraction_WET_DRY=="DRY"){
              xlabString <- bquote(CO[2*","*DRY] ~ .("LP8") ~ "-" ~ CO[2*","*DRY] ~ .("REF [ppm]"))
            }else{
              xlabString <- bquote(CO[2] ~ .("LP8") ~ "-" ~ CO[2] ~ .("REF [ppm]"))
            }
            
            hist(residuals[id_FLAG_1],seq(floor(min(residuals[id_FLAG_1])),ceiling(max(residuals[id_FLAG_1]))+2,2),xlim=c(Q050_S-40,Q050_S+40),col="slategray",main="",xlab=xlabString,ylab="Number",cex.axis=1.25,cex.lab=1.25)
            
            
            lines(c(MEAN_S,MEAN_S),c(-1e9,1e9),col=2,lwd=3)
            if(F){
              lines(c(MEAN_S+20,MEAN_S+20),c(-1e9,1e9),col=2,lwd=3,lty=5)
              lines(c(MEAN_S-20,MEAN_S-20),c(-1e9,1e9),col=2,lwd=3,lty=5)
            }else{
              lines(c(MEAN_S+RMSE_1,MEAN_S+RMSE_1),c(-1e9,1e9),col=2,lwd=3,lty=5)
              lines(c(MEAN_S-RMSE_1,MEAN_S-RMSE_1),c(-1e9,1e9),col=2,lwd=3,lty=5)
            }
            if(F){
              par(family="mono")
              legend("topright",legend=c(str_R_00,str_R_01,str_R_02,str_R_03,str_R_04,str_R_05),bg="white",cex=1.25)
              par(family="")
            }else{
              par(family="mono")
              legend("topright",legend=c(str_R_00,str_R_07),bg="white",cex=1.25)
              par(family="")
            }
          }else{
            plot.new()
          }
          
          #
          
          dev.off()
          par(def_par)
          
          
          
          # --- PLOT: TS
          
          figname    <- paste(resultdir,SITE,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_TS.pdf",sep="")
          
          tmp1            <- rep(NA,dim(data)[1])
          tmp1[id_FLAG_1] <- data$measurements[id_FLAG_1]
          
          yyy        <- cbind(data$CO2_10MIN_AV,
                              data$measurements,
                              tmp1)
          
          xlabString <- "Date" 
          
          if(MoleFraction_WET_DRY=="DRY"){
            ylabString <- bquote(CO[2*","*DRY] ~ .("[ppm]"))
          }else{
            ylabString <- bquote(CO[2] ~ .("[ppm]"))
          }
          
          legend_str <- c("PIC CO2","LP8 CO2 [ALL]","LP8 CO2 [USE]")
          plot_ts(figname,data$date,yyy,"week",NULL,c(350,700),xlabString,ylabString,legend_str)
          
          # PLOT: TS RH
          
          if(mode==1){
            figname    <- paste(resultdir,SITE,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_RH_TS.pdf",sep="")
            yyy        <- cbind(data$SHT21_RH)
            xlabString <- "Date" 
            ylabString <- expression(paste("RH [%]"))
            legend_str <- c("LP8 RH")
            plot_ts(figname,data$date,yyy,"week",NULL,c(0,110),xlabString,ylabString,legend_str)
          }
          
          # PLOT: TS T
          
          if(mode==1){
            figname    <- paste(resultdir,SITE,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_T_TS.pdf",sep="")
            yyy        <- cbind(data$SHT21_T)
            xlabString <- "Date" 
            ylabString <- expression(paste("T [deg C]"))
            legend_str <- c("LP8 T")
            plot_ts(figname,data$date,yyy,"week",NULL,c(-20,40),xlabString,ylabString,legend_str)
          }
          
          
          # --- RESIDUAL - SHT21 T 
          
          bin_delta<- 2 
          bins     <- seq(-50,50,bin_delta)
          n_bins   <- length(bins)
          Q025_bin <- rep(NA,n_bins)
          Q050_bin <- rep(NA,n_bins)
          Q075_bin <- rep(NA,n_bins)
          MID_bin  <- rep(NA,n_bins)
          N_bin    <- rep(NA,n_bins)
          
          for(ith_bin in 1:n_bins){
            id_bin         <- which(data$SHT21_T[id_FLAG_1]>=bins[ith_bin] & data$SHT21_T[id_FLAG_1]<(bins[ith_bin]+bin_delta))
            N_bin[ith_bin] <- length(id_bin)
            
            if(N_bin[ith_bin]>10){
              Q025_bin[ith_bin] <- quantile(x = (data$measurements[id_FLAG_1]-data$CO2_10MIN_AV[id_FLAG_1])[id_bin],probs=0.25,na.rm=T)
              Q050_bin[ith_bin] <- quantile(x = (data$measurements[id_FLAG_1]-data$CO2_10MIN_AV[id_FLAG_1])[id_bin],probs=0.50,na.rm=T)
              Q075_bin[ith_bin] <- quantile(x = (data$measurements[id_FLAG_1]-data$CO2_10MIN_AV[id_FLAG_1])[id_bin],probs=0.75,na.rm=T)
              MID_bin[ith_bin]  <- bins[ith_bin] + 1/2*bin_delta
            }
          }
          
          
          figname <- paste(resultdir,SITE,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_RESIUDAL_SHT21T.pdf",sep="")
          
          mainStr <- paste(u_SensorUnit_ID[ith_SU]," @ ",SITE," ",eq_label,sep="")
          
          if(u_SensorUnit_ID[ith_SU]%in%c(1081,1100,1139,1201,1210) & SITE=="PAYN"){
            mainStr <- paste(u_SensorUnit_ID[ith_SU]," @ PAY ",eq_label,sep="")  
          }
          if(SITE=="DUE1"){
            mainStr <- paste(u_SensorUnit_ID[ith_SU]," @ DUE ",eq_label,sep="")  
          }
          
          subStr  <- paste("Data: ",
                           strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+min(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),
                           "-",
                           strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+max(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),sep="")
          
          
          def_par <- par()
          pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1.25,1,1.0,1.0),mfrow=c(1,1))
          
          if(MoleFraction_WET_DRY=="DRY"){
            ylabString <- bquote(CO[2*","*DRY] ~ .("LP8") ~ "-" ~ CO[2*","*DRY] ~ .("REF [ppm]"))
          }else{
            ylabString <- bquote(CO[2] ~ .("LP8") ~ "-" ~ CO[2] ~ .("REF [ppm]"))
          }
          
          plot(0,0,xlim=c(-15,45),ylim=c(-50,50),main=mainStr,ylab=ylabString,xlab=expression(paste("SHT21 T [deg C]")),cex.axis=1.75,cex.lab=1.75,cex.main=1.75)
          
          if(any(data$FLAG==0)){
            points(data$SHT21_T[id_FLAG_0],data$measurements[id_FLAG_0]-data$CO2_10MIN_AV[id_FLAG_0],col="gray70",pch=16,cex=0.5)
          }
          if(any(data$FLAG==1)){
            points(data$SHT21_T[id_FLAG_1],data$measurements[id_FLAG_1]-data$CO2_10MIN_AV[id_FLAG_1],col="black",pch=16,cex=0.5)
          }
          
          lines(c(-1e3,1e3),c(0,0),col="grey70",lwd=2,lty=1)
          
          lines(MID_bin,Q025_bin,lwd=2,lty=5,col=2)
          lines(MID_bin,Q050_bin,lwd=2,lty=5,col=2)
          lines(MID_bin,Q075_bin,lwd=2,lty=5,col=2)
          
          par(new=T)
          
          plot(MID_bin,N_bin,xlim=c(-15,45),pch=15,cex=1.5,col=2,xaxt="n",yaxt="n",xlab="",ylab="")
          axis(side = 4,cex.axis=1.75,cex.lab=1.75)
          mtext(text="Number of measurements",side = 4,line = 3.5,cex=1.75)
          
          mtext(text = subStr,side = 1,line = 4.5,cex=1.5)
          
          par(family="mono")
          legend("bottomright",legend=c(str_L_02_SC,str_L_05_SC,str_L_06_SC),bg="white",cex=1.75)
          par(family="")
          
          dev.off()
          par(def_par)
          
          
          
          # --- RESIDUAL - SHT21 RH 
          
          bin_delta<- 2.5 
          bins     <- seq(0,100,bin_delta)
          n_bins   <- length(bins)
          Q025_bin <- rep(NA,n_bins)
          Q050_bin <- rep(NA,n_bins)
          Q075_bin <- rep(NA,n_bins)
          MID_bin  <- rep(NA,n_bins)
          N_bin    <- rep(NA,n_bins)
          
          for(ith_bin in 1:n_bins){
            id_bin         <- which(data$SHT21_RH[id_FLAG_1]>=bins[ith_bin] & data$SHT21_RH[id_FLAG_1]<(bins[ith_bin]+bin_delta))
            N_bin[ith_bin] <- length(id_bin)
            
            if(N_bin[ith_bin]>10){
              Q025_bin[ith_bin] <- quantile(x = (data$measurements[id_FLAG_1]-data$CO2_10MIN_AV[id_FLAG_1])[id_bin],probs=0.25,na.rm=T)
              Q050_bin[ith_bin] <- quantile(x = (data$measurements[id_FLAG_1]-data$CO2_10MIN_AV[id_FLAG_1])[id_bin],probs=0.50,na.rm=T)
              Q075_bin[ith_bin] <- quantile(x = (data$measurements[id_FLAG_1]-data$CO2_10MIN_AV[id_FLAG_1])[id_bin],probs=0.75,na.rm=T)
              MID_bin[ith_bin]  <- bins[ith_bin] + 1/2*bin_delta
            }
          }
          
          figname <- paste(resultdir,SITE,"_",u_SensorUnit_ID[ith_SU],"_MODE_",sprintf("%02.0f",mode),"_RESIUDAL_SHT21RH.pdf",sep="")
          
          mainStr <- paste(u_SensorUnit_ID[ith_SU]," @ ",SITE," ",eq_label,sep="")
          
          if(u_SensorUnit_ID[ith_SU]%in%c(1081,1100,1139,1201,1210) & SITE=="PAYN"){
            mainStr <- paste(u_SensorUnit_ID[ith_SU]," @ PAY ",eq_label,sep="")  
          }
          if(SITE=="DUE1"){
            mainStr <- paste(u_SensorUnit_ID[ith_SU]," @ DUE ",eq_label,sep="")  
          }
          
          subStr  <- paste("Data: ",
                           strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+min(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),
                           "-",
                           strftime(strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+max(data$timestamp[id_FLAG_0]),"%Y/%m/%d",tz="UTC"),sep="")
          
          def_par <- par()
          pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1.25,1,1.0,1.0),mfrow=c(1,1))
          
          if(MoleFraction_WET_DRY=="DRY"){
            ylabString <- bquote(CO[2*","*DRY] ~ .("LP8") ~ "-" ~ CO[2*","*DRY] ~ .("REF [ppm]"))
          }else{
            ylabString <- bquote(CO[2] ~ .("LP8") ~ "-" ~ CO[2] ~ .("REF [ppm]"))
          }
          
          plot(-100,0,xlim=c(0,105),ylim=c(-50,50),main=mainStr,ylab=ylabString,xlab=expression(paste("SHT21 RH [%]")),cex.axis=1.75,cex.lab=1.75,cex.main=1.75)
          
          if(any(data$FLAG==0)){
            points(data$SHT21_RH[id_FLAG_0],data$measurements[id_FLAG_0]-data$CO2_10MIN_AV[id_FLAG_0],col="gray70",pch=16,cex=0.5)
          }
          if(any(data$FLAG==1)){
            points(data$SHT21_RH[id_FLAG_1],data$measurements[id_FLAG_1]-data$CO2_10MIN_AV[id_FLAG_1],col="black",pch=16,cex=0.5)
          }
          
          lines(c(-1e3,1e3),c(0,0),col="grey70",lwd=2,lty=1)
          
          lines(MID_bin,Q025_bin,lwd=2,lty=5,col=2)
          lines(MID_bin,Q050_bin,lwd=2,lty=5,col=2)
          lines(MID_bin,Q075_bin,lwd=2,lty=5,col=2)
          
          par(new=T)
          
          plot(MID_bin,N_bin,xlim=c(0,105),pch=15,cex=1.5,col=2,xaxt="n",yaxt="n")
          axis(side = 4,cex.axis=1.75,cex.lab=1.75)
          mtext(text="Number of measurements",side = 4,line = 3.5,cex=1.75)
          
          mtext(text = subStr,side = 1,line = 4.5,cex=1.5)
          
          par(family="mono")
          legend("bottomleft",legend=c(str_L_02_SC,str_L_05_SC,str_L_06_SC),bg="white",cex=1.75)
          par(family="")
          
          
          dev.off()
          par(def_par)
          
        }
      }
      
    }
  }
  
  #
  
  rm(id_FLAG_0,id_FLAG_1,n_id_FLAG_0,n_id_FLAG_1)
  rm(RMSE_0,RMSE_1,RMSE_C,COR_0,COR_1,fit_1,c1,c2,MEAN,MEAN_A)
  rm(FRAC_RH_80,FRAC_RH_85,FRAC_RH_90)
  rm(ok,residuals)
  rm(week,week_mode,s,u_weeks,n_u_weeks,Data_SU_REF_ok)
  rm(Q050_S,MEAN_S)
  rm(str_L_00,str_L_01,str_L_02,str_L_03,str_L_04,str_L_05,str_L_06)
  rm(str_L_00_SC,str_L_02_SC,str_L_04_SC,str_L_05_SC,str_L_06_SC)
  rm(str_R_00,str_R_01,str_R_02,str_R_03,str_R_04,str_R_05,str_R_06,str_R_07)
  
  gc()
  
  
  ### ---------------------------------------------------
  
  # Plot CO2 time series
  # ------------------------
  
  
  if(!SITE%in%c("PAY")){
    
    
    # Select SensorUnit_IDs
    
    query_str         <- paste("SELECT SensorUnit_ID, timestamp FROM ",ProcDataTblName," WHERE LocationName='",SITE,"';",sep="")
    drv               <- dbDriver("MySQL")
   con <-carboutil::get_conn()
    res               <- dbSendQuery(con, query_str)
    tbl_CO2_SU        <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    # AMT PAPER
    
    if(as.integer(args[1])%in%c(20,30)){
      if(SITE == "BRM"){
        tbl_CO2_SU <- tbl_CO2_SU[which(tbl_CO2_SU$SensorUnit_ID==1013),]
      }
      if(SITE == "HAE"){
        tbl_CO2_SU <- tbl_CO2_SU[which(tbl_CO2_SU$SensorUnit_ID%in%c(1182,1218,1219,1223,1314)),]
      }
    }
    
    #
    
    u_SensorUnit_ID   <- sort(unique(tbl_CO2_SU$SensorUnit_ID))
    n_u_SensorUnit_ID <- length(u_SensorUnit_ID)
    
    minTimestamp      <- min(tbl_CO2_SU$timestamp)
    maxTimestamp      <- max(tbl_CO2_SU$timestamp)
    
    rm(tbl_CO2_SU)
    gc()
    
    
    # Import reference data 
    # 
    # (Import, DRY/WET, averaging to 10 min intervals)
    
    if(!tableName%in%c("UNIBE_BRM","EMPA_LAEG")){
      if(tableName=="NABEL_HAE"){
        query_str <- paste("SELECT timestamp, CO2_WET_COMP, CO2_DRY_CAL, H2O FROM ",tableName," ",sep="")
        query_str <- paste(query_str,"WHERE timestamp >= ",minTimestamp," and timestamp <= ",maxTimestamp," ",sep="")
        query_str <- paste(query_str,"AND CO2_WET_COMP != -999;",sep="")
      }
      if(tableName%in%c("NABEL_PAY","NABEL_RIG","NABEL_DUE")){
        query_str <- paste("SELECT timestamp, CO2_DRY_CAL, CO2_WET_COMP FROM ",tableName," WHERE timestamp >= ",minTimestamp," and timestamp <= ",maxTimestamp,";",sep="")
      }
      drv               <- dbDriver("MySQL")
     con <-carboutil::get_conn()
      res               <- dbSendQuery(con, query_str)
      data              <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      data <- data[order(data$timestamp),]
      
      #
      
      if(tableName=="NABEL_HAE"){
        
        data$CO2 <- NA
        
        id_HAE_LICOR   <- which(data$timestamp <= 1583935200 & data$H2O != -999)
        id_HAE_PICARRO <- which(data$timestamp >  1583935200 & data$CO2_DRY_CAL != -999 & data$H2O != -999)
        
        if(MoleFraction_WET_DRY=="WET"){
          data$CO2 <- data$CO2_WET_COMP
        }
        if(MoleFraction_WET_DRY=="DRY"){
          if(length(id_HAE_LICOR)>0){
            data$CO2[id_HAE_LICOR] <- data$CO2_WET_COMP[id_HAE_LICOR] / (1 - data$H2O[id_HAE_LICOR]/100)
          }
          if(length(id_HAE_PICARRO)>0){
            data$CO2[id_HAE_PICARRO] <- data$CO2_DRY_CAL[id_HAE_PICARRO]
          }
        }
      }
      
      if(tableName%in%c("NABEL_RIG","NABEL_PAY","NABEL_DUE")){
        
        if(MoleFraction_WET_DRY=="DRY"){
          data$CO2 <- data$CO2_DRY_CAL
        }else{
          data$CO2 <- data$CO2_WET_COMP
        }
        
        id_setToNA <- which(data$CO2_DRY_CAL == -999 | data$CO2_WET_COMP == -999)
        if(length(id_setToNA)>0){
          data$CO2[id_setToNA]     <- NA
        }
      }
      
      #
      
      dT          <- c(60,round(diff(data$timestamp)))
      dt_nok      <- ma(dT,10)!=60
      
      tmp         <- ma(data$CO2,10)
      tmp[dt_nok] <- NA
      data        <- data.frame(timestamp=data$timestamp-600, CO2_10MIN_AV = tmp,stringsAsFactors = F)
      
      rm(tmp,dT,dt_nok)
      gc()
      
    }
    
    if(tableName=="UNIBE_BRM"){
      query_str         <- paste("SELECT timestamp, CO2, CO2_DRY FROM ",tableName," WHERE MEAS_HEIGHT = 12 and CO2_DRY_N > 5 and CO2 != -999 and timestamp >= ",minTimestamp," and timestamp <= ",maxTimestamp,";",sep="")
      drv               <- dbDriver("MySQL")
     con <-carboutil::get_conn()
      res               <- dbSendQuery(con, query_str)
      tmp1              <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      if(dim(tmp1)[1]==0){
        next
      }
      
      #
      
      if(MoleFraction_WET_DRY=="DRY"){
        tmp1 <- tmp1[,c(which(colnames(tmp1)=="timestamp"), which(colnames(tmp1)=="CO2_DRY"))]
      }else{
        tmp1 <- tmp1[,c(which(colnames(tmp1)=="timestamp"), which(colnames(tmp1)=="CO2"))]
      }
      colnames(tmp1) <- c("timestamp","CO2")
      
      #
      
      tmp1 <- merge(data.frame(timestamp=seq(min(tmp1$timestamp),max(tmp1$timestamp),60),stringsAsFactors = F),
                    tmp1,by="timestamp",all.x=T)
      
      tmp     <- matrix(NA,ncol=10,nrow=dim(tmp1)[1])
      tmp[,1] <- tmp1$CO2
      for(ii in 1:9){
        tmp[,ii+1] <- c(rep(NA,ii),tmp1$CO2[1:(dim(tmp1)[1]-ii)])
      }
      
      tmp     <- apply(tmp,1,mean,na.rm=T)
      id_tmp  <- which(!is.na(tmp) & tmp1$timestamp%%600==0)
      data    <- data.frame(timestamp = tmp1$timestamp[id_tmp]-600,CO2_10MIN_AV=tmp[id_tmp],stringsAsFactors = F)
      
      rm(tmp1,id_tmp,tmp)
      gc()
    }
    
    if(tableName=="EMPA_LAEG"){
      query_str         <- paste("SELECT timestamp, CO2, CO2_DRY FROM ",tableName," WHERE VALVEPOS = 0 and CO2_DRY_N > 5 and CO2 != -999 and timestamp >= ",minTimestamp," and timestamp <= ",maxTimestamp,";",sep="")
      drv               <- dbDriver("MySQL")
     con <-carboutil::get_conn()
      res               <- dbSendQuery(con, query_str)
      tmp1              <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      if(dim(tmp1)[1]==0){
        next
      }
      
      #
      
      if(MoleFraction_WET_DRY=="DRY"){
        tmp1 <- tmp1[,c(which(colnames(tmp1)=="timestamp"), which(colnames(tmp1)=="CO2_DRY"))]
      }else{
        tmp1 <- tmp1[,c(which(colnames(tmp1)=="timestamp"), which(colnames(tmp1)=="CO2"))]
      }
      colnames(tmp1) <- c("timestamp","CO2")
      
      #
      
      tmp1 <- merge(data.frame(timestamp=seq(min(tmp1$timestamp),max(tmp1$timestamp),60),stringsAsFactors = F),
                    tmp1,by="timestamp",all.x=T)
      
      tmp     <- matrix(NA,ncol=10,nrow=dim(tmp1)[1])
      tmp[,1] <- tmp1$CO2
      for(ii in 1:9){
        tmp[,ii+1] <- c(rep(NA,ii),tmp1$CO2[1:(dim(tmp1)[1]-ii)])
      }
      
      tmp     <- apply(tmp,1,mean,na.rm=T)
      id_tmp  <- which(!is.na(tmp) & (tmp1$timestamp%%600)==0)
      data    <- data.frame(timestamp = tmp1$timestamp[id_tmp]-600,CO2_10MIN_AV=tmp[id_tmp],stringsAsFactors = F)
      
      rm(tmp1,id_tmp,tmp)
      gc()
    }
    
    #
    
    data$CO2_10MIN_AV[which(data$CO2_10MIN_AV == -999)] <- NA
    
    data <- data[order(data$timestamp),]
    data <- data[data$timestamp%%600 == 0,]
    
    #
    
    for(ith_SU in 1:n_u_SensorUnit_ID){
      
      
      # Import processed LP8 data
      
      query_str         <- paste("SELECT timestamp, CO2, CO2_A, FLAG FROM ",ProcDataTblName," WHERE LocationName='",SITE,"' and SensorUnit_ID=",u_SensorUnit_ID[ith_SU],";",sep="")
      drv               <- dbDriver("MySQL")
     con <-carboutil::get_conn()
      res               <- dbSendQuery(con, query_str)
      tbl_CO2_SU        <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      #
      
      if(MoleFraction_WET_DRY=="DRY"){
        id <- which(tbl_CO2$CO2_A != -999 & tbl_CO2$H2O != -999)
        if(length(id)>0){
          tbl_CO2$CO2_A[id] <- tbl_CO2$CO2_A[id] / (1 - tbl_CO2$H2O[id]/100)
        }
        
        id <- which(tbl_CO2$CO2_A != -999 & tbl_CO2$H2O != -999)
        if(length(id)>0){
          tbl_CO2$CO2[id]   <- tbl_CO2$CO2[id]   / (1 - tbl_CO2$H2O[id]/100)
        }
        
        rm(id)
        gc()
      }
      
      #
      
      tbl_CO2_SU           <- tbl_CO2_SU[order(tbl_CO2_SU$timestamp),]
      tbl_CO2_SU$timestamp <- (tbl_CO2_SU$timestamp%/%600)*600
      colnames(tbl_CO2_SU) <- c("timestamp",paste(paste("SU",u_SensorUnit_ID[ith_SU],"_",sep=""),colnames(tbl_CO2_SU)[2:dim(tbl_CO2_SU)[2]],sep=""))
      
      
      # Match reference and LP8 data
      
      data <- merge(x=data,y=tbl_CO2_SU,by="timestamp",all.x = T)
      
    }
    
    data$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
    
    #
    
    
    for(version in 1:4){  
      
      yyy                       <- matrix(NA,nrow=dim(data)[1],ncol=n_u_SensorUnit_ID+1)
      yyy[,n_u_SensorUnit_ID+1] <- data$CO2_10MIN_AV
      
      if(version==1){
        figname <- paste(resultdir,SITE,"_ALL_LP8_TS.pdf",sep="")
      }
      if(version==2){
        figname <- paste(resultdir,SITE,"_ALL_LP8_F_TS.pdf",sep="")
      }
      if(version==3){
        figname <- paste(resultdir,SITE,"_ALL_LP8_IMP_TS.pdf",sep="")
      }
      if(version==4){
        figname <- paste(resultdir,SITE,"_ALL_LP8_IMP_F_TS.pdf",sep="")
      }
      
      for(ith_SU in 1:n_u_SensorUnit_ID){
        pos_LP8   <- which(colnames(data)==paste("SU",u_SensorUnit_ID[ith_SU],"_CO2",  sep=""))
        pos_LP8_A <- which(colnames(data)==paste("SU",u_SensorUnit_ID[ith_SU],"_CO2_A",sep=""))
        pos_FLAG  <- which(colnames(data)==paste("SU",u_SensorUnit_ID[ith_SU],"_FLAG", sep=""))
        
        if(version==1){
          id             <- which(data[,pos_LP8]!= -999)
          yyy[id,ith_SU] <- data[id,pos_LP8]
        }
        if(version==2){
          id             <- which(data[,pos_LP8]!= -999 & data[,pos_FLAG]==1)
          yyy[id,ith_SU] <- data[id,pos_LP8]
        }
        if(version==3){
          id             <- which(data[,pos_LP8_A]!= -999)
          yyy[id,ith_SU] <- data[id,pos_LP8_A]
        }
        if(version==4){
          id             <- which(data[,pos_LP8_A]!= -999 & data[,pos_FLAG]==1)
          yyy[id,ith_SU] <- data[id,pos_LP8_A]
        }
      }
      
      xlabString <- "Date" 
      ylabString <- expression(paste("CO"[2]*" [ppm]"))
      legend_str <- c(paste("SU",u_SensorUnit_ID),"REF")
      plot_ts_LP8atREF(figname,data$date,yyy,"all_day2day",NULL,c(350,650),xlabString,ylabString,legend_str)
      
      figname <- gsub(pattern = "TS.pdf",replacement = "TS_WEEK.pdf",figname)
      plot_ts_LP8atREF(figname,data$date,yyy,"week",NULL,c(350,650),xlabString,ylabString,legend_str)
      
    }
  }
  
  
  
  
  ### ---------------------------------------------------
  
  
  # Plot CO2 RMSE time series
  # --------------------------
  
  if(!SITE%in%c("PAY")){
    
    for(PROC_MODE in c(1,2,4,5,6)){
      
      SITE_P = SITE
      
      if(SITE=="PAYN"){
        SITE_P = "PAY"
      }
      if(SITE=="DUE1"){
        SITE_P = "DUE"
      }
      
      id <- which(statistics$LocationName==SITE
                  & statistics$WEEK!=0
                  & statistics$MODE==PROC_MODE
                  & !is.na(statistics$RMSE))
      
      if(length(id)==0){
        next
      }
      
      u_SensorUnit_ID   <- sort(unique(statistics$SensorUnit_ID[id]))
      n_u_SensorUnit_ID <- length(u_SensorUnit_ID)
      
      minDate <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + min(statistics$WEEK[id])*7*86400
      maxDate <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + max(statistics$WEEK[id])*7*86400 + 7*86400
      axDate  <- seq(strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20200101000000","%Y%m%d%H%M%S",tz="UTC"),"month")
      
      figname <- paste(resultdir,SITE,"_RMSE_MODE_",sprintf("%02.0f",PROC_MODE),".pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.0,1.0,0.5,0.5),mfrow=c(1,1))
      
      plot(c(minDate,maxDate),c(NA,NA),ylim=c(0,50),xaxt="n",cex.axis=1.5,cex.main=1.5,cex.lab=1.5,xlab="Date [month/year]",ylab="RMSE [ppm]", main=SITE_P)
      axis(side = 1,at = axDate,labels = strftime(axDate,"%m/%y",tz="UTC"),cex.axis=1.5,cex.lab=1.5)
      
      
      for(ll in seq(-100,100,10)){
        lines(c(-1e9,1e12),c(ll,ll),col="gray70",lty=1)
      }
      
      for(ith_SUID in 1:n_u_SensorUnit_ID){
        
        id_id      <- which(statistics$SensorUnit_ID[id]==u_SensorUnit_ID[ith_SUID])
        n_id_id    <- length(id_id)
        
        if(n_id_id==0){
          next
        }
        
        RMSE_dates <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + (statistics$WEEK[id[id_id]]+0.5)*7*86400
        
        if(n_u_SensorUnit_ID<=10){
          lines( RMSE_dates,statistics$RMSE[id[id_id]],col=rainbow(n_u_SensorUnit_ID)[ith_SUID],lwd=1, lty=1)
          points(RMSE_dates,statistics$RMSE[id[id_id]],col=rainbow(n_u_SensorUnit_ID)[ith_SUID],pch=16,cex=0.5)
        }else{
          lines( RMSE_dates,statistics$RMSE[id[id_id]],col="gray50",lwd=1, lty=1)
        }
      }
      
      if(n_u_SensorUnit_ID>10){
        for(ith_SUID in 1:n_u_SensorUnit_ID){
          id_id      <- which(statistics$SensorUnit_ID[id]==u_SensorUnit_ID[ith_SUID])
          n_id_id    <- length(id_id)
          RMSE_dates <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + (statistics$WEEK[id[id_id]]+0.5)*7*86400
          points(RMSE_dates,statistics$RMSE[id[id_id]],col=1,pch=16,cex=0.5)
        }
      }
      
      if(n_u_SensorUnit_ID<=10){
        par(family="mono")
        legend("topleft",legend=paste("SU",u_SensorUnit_ID,sep=""),pch=16,col=rainbow(n_u_SensorUnit_ID),bg="white",cex=1.5)
        par(family="")
      }
      
      dev.off()
      par(def_par)
    }
  }
  
  
  
  # Plot CO2 MEAN DIFF time series
  # ------------------------------
  
  if(!SITE%in%c("PAY")){
    
    for(PROC_MODE in c(1,2,4,5,6)){
      
      SITE_P = SITE
      
      if(SITE=="PAYN"){
        SITE_P = "PAY"
      }
      if(SITE=="DUE1"){
        SITE_P = "DUE"
      }
      
      id <- which(statistics$LocationName==SITE
                  & statistics$WEEK!=0
                  & statistics$MODE==PROC_MODE
                  & !is.na(statistics$MEAN))
      
      if(length(id)==0){
        next
      }
      
      u_SensorUnit_ID   <- sort(unique(statistics$SensorUnit_ID[id]))
      n_u_SensorUnit_ID <- length(u_SensorUnit_ID)
      
      minDate <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + min(statistics$WEEK[id])*7*86400
      maxDate <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + max(statistics$WEEK[id])*7*86400 + 7*86400
      axDate  <- seq(strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20200101000000","%Y%m%d%H%M%S",tz="UTC"),"month")
      
      #
      
      yvals <- NULL
      
      for(ith_SUID in 1:n_u_SensorUnit_ID){
        id_id    <- which(statistics$SensorUnit_ID[id]==u_SensorUnit_ID[ith_SUID])
        n_id_id  <- length(id_id)
        
        if(n_id_id>0){
          yvals <- c(yvals,statistics$MEAN[id[id_id]])
        }
      }
      
      yvals  <- yvals[!is.na(yvals) & !is.infinite(yvals)]
      yrange <- range(yvals) + c(-50,50)
      
      #
      
      figname <- paste(resultdir,SITE,"_MEAN_MODE_",sprintf("%02.0f",PROC_MODE),".pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.0,1.0,0.5,0.5),mfrow=c(1,1))
      
      plot(c(minDate,maxDate),c(NA,NA),ylim=yrange,xaxt="n",cex.axis=1.5,cex.main=1.5,cex.lab=1.5,xlab="Date [month/year]",ylab="Mean deviation [ppm]", main=SITE_P)
      axis(side = 1,at = axDate,labels = strftime(axDate,"%m/%y",tz="UTC"),cex.axis=1.5,cex.lab=1.5)
      
      
      for(ll in seq(-3000,3000,25)){
        lines(c(-1e9,1e12),c(ll,ll),col="gray70",lty=1)
      }
      
      for(ith_SUID in 1:n_u_SensorUnit_ID){
        
        id_id      <- which(statistics$SensorUnit_ID[id]==u_SensorUnit_ID[ith_SUID])
        n_id_id    <- length(id_id)
        
        if(n_id_id==0){
          next
        }
        
        RMSE_dates <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + (statistics$WEEK[id[id_id]]+0.5)*7*86400
        
        if(n_u_SensorUnit_ID<=10){
          lines( RMSE_dates,statistics$MEAN[id[id_id]],col=rainbow(n_u_SensorUnit_ID)[ith_SUID],lwd=1, lty=1)
          points(RMSE_dates,statistics$MEAN[id[id_id]],col=rainbow(n_u_SensorUnit_ID)[ith_SUID],pch=16,cex=0.75)
        }else{
          lines( RMSE_dates,statistics$MEAN[id[id_id]],col="gray50",lwd=1, lty=1)
          points(RMSE_dates,statistics$MEAN[id[id_id]],col=1,pch=16,cex=0.75)
        }
      }
      
      if(n_u_SensorUnit_ID<=10){
        par(family="mono")
        legend("topleft",legend=paste("SU",u_SensorUnit_ID,sep=""),pch=16,col=rainbow(n_u_SensorUnit_ID),bg="white",cex=1.5)
        par(family="")
      }
      
      dev.off()
      par(def_par)
    }
  }
  
  
  # Plot CO2 Q050 DIFF time series
  # ------------------------------
  
  if(!SITE%in%c("PAY")){
    
    for(PROC_MODE in c(2,4,5,6)){
      
      SITE_P = SITE
      
      if(SITE=="PAYN"){
        SITE_P = "PAY"
      }
      if(SITE=="DUE1"){
        SITE_P = "DUE"
      }
      
      id_1 <- which(statistics$LocationName==SITE
                    & statistics$WEEK!=0
                    & statistics$MODE==1
                    & !is.na(statistics$Q050))
      
      if(PROC_MODE==2){
        id_4 <- which(statistics$LocationName==SITE
                      & statistics$WEEK!=0
                      & statistics$MODE==2
                      & !is.na(statistics$Q050))
      }
      if(PROC_MODE==4){
        id_4 <- which(statistics$LocationName==SITE
                      & statistics$WEEK!=0
                      & statistics$MODE==4
                      & !is.na(statistics$Q050))
      }
      if(PROC_MODE==5){
        id_4 <- which(statistics$LocationName==SITE
                      & statistics$WEEK!=0
                      & statistics$MODE==5
                      & !is.na(statistics$Q050))
      }
      if(PROC_MODE==6){
        id_4 <- which(statistics$LocationName==SITE
                      & statistics$WEEK!=0
                      & statistics$MODE==6
                      & !is.na(statistics$Q050))
      }
      
      if(length(id_1)==0  | length(id_4)==0){
        next
      }
      
      u_SensorUnit_ID_1   <- sort(unique(statistics$SensorUnit_ID[id_1]))
      n_u_SensorUnit_ID_1 <- length(u_SensorUnit_ID_1)
      
      u_SensorUnit_ID_4   <- sort(unique(statistics$SensorUnit_ID[id_4]))
      n_u_SensorUnit_ID_4 <- length(u_SensorUnit_ID_4)
      
      minDate <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + min(statistics$WEEK[id_1])*7*86400
      maxDate <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + max(statistics$WEEK[id_1])*7*86400 + 7*86400
      axDate  <- seq(strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20200101000000","%Y%m%d%H%M%S",tz="UTC"),"2 months")
      
      #
      
      yvals <- NULL
      
      for(ith_SUID in 1:n_u_SensorUnit_ID){
        id_id_1    <- which(statistics$SensorUnit_ID[id_1]==u_SensorUnit_ID[ith_SUID])
        n_id_id_1  <- length(id_id_1)
        id_id_4    <- which(statistics$SensorUnit_ID[id_4]==u_SensorUnit_ID[ith_SUID])
        n_id_id_4  <- length(id_id_4)
        
        if(n_id_id_1>0 & n_id_id_4>0){
          yvals <- c(yvals,statistics$Q050[id_1[id_id_1]],statistics$Q050[id_4[id_id_4]])
        }
      }
      
      yvals  <- yvals[!is.na(yvals)]
      yrange <- range(yvals) + c(-50,50)
      
      #
      
      # yrange <- c(-200,200)
      
      #
      
      figname <- paste(resultdir,SITE,"_Q050_MODE_",sprintf("%02.0f",PROC_MODE),".pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.0,1.0,0.5,0.5),mfrow=c(1,1))
      
      plot(c(minDate,maxDate),c(NA,NA),ylim=yrange,xaxt="n",cex.axis=1.75,cex.main=1.75,cex.lab=1.75,xlab="Date [month/year]",ylab="Median deviation [ppm]", main=SITE_P)
      axis(side = 1,at = axDate,labels = strftime(axDate,"%m/%y",tz="UTC"),cex.axis=1.75,cex.lab=1.75)
      
      
      for(ll in seq(-3000,3000,25)){
        lines(c(-1e9,1e12),c(ll,ll),col="gray70",lty=1)
      }
      
      
      for(ith_SUID in 1:n_u_SensorUnit_ID_1){
        
        id_id_1      <- which(statistics$SensorUnit_ID[id_1]==u_SensorUnit_ID_1[ith_SUID])
        n_id_id_1    <- length(id_id_1)
        
        if(n_id_id_1==0){
          next
        }
        
        RMSE_dates <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + (statistics$WEEK[id_1[id_id_1]]+0.5)*7*86400
        
        if(n_u_SensorUnit_ID<=10){
          lines( RMSE_dates,statistics$Q050[id_1[id_id_1]],col=rainbow(n_u_SensorUnit_ID_4)[ith_SUID],lwd=1, lty=1)
          points(RMSE_dates,statistics$Q050[id_1[id_id_1]],col=rainbow(n_u_SensorUnit_ID_4)[ith_SUID],pch=16,cex=0.75)
        }else{
          lines( RMSE_dates,statistics$Q050[id_1[id_id_1]],col="green",lwd=1, lty=1)
          points(RMSE_dates,statistics$Q050[id_1[id_id_1]],col="green",pch=16,cex=0.75)
        }
      }
      
      for(ith_SUID in 1:n_u_SensorUnit_ID_4){
        
        id_id_4      <- which(statistics$SensorUnit_ID[id_4]==u_SensorUnit_ID_4[ith_SUID])
        n_id_id_4    <- length(id_id_4)
        
        if(n_id_id_4==0){
          next
        }
        
        RMSE_dates <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + (statistics$WEEK[id_4[id_id_4]]+0.5)*7*86400
        
        lines( RMSE_dates,statistics$Q050[id_4[id_id_4]],col="gray30",lwd=1, lty=1)
        points(RMSE_dates,statistics$Q050[id_4[id_id_4]],col="gray40",pch=16,cex=0.4)
        
        # if(n_u_SensorUnit_ID<=10){
        #   lines( RMSE_dates,statistics$Q050[id_4[id_id_4]],col=rainbow(n_u_SensorUnit_ID_4)[ith_SUID],lwd=2, lty=1)
        #   points(RMSE_dates,statistics$Q050[id_4[id_id_4]],col=rainbow(n_u_SensorUnit_ID_4)[ith_SUID],pch=16,cex=0.75)
        # }else{
        #   lines( RMSE_dates,statistics$Q050[id_4[id_id_4]],col="gray30",lwd=1, lty=1)
        #   points(RMSE_dates,statistics$Q050[id_4[id_id_4]],col="gray40",pch=16,cex=0.4)
        # }
      }
      
      if(n_u_SensorUnit_ID_4<=10){
        par(family="mono")
        legend("topleft",legend=paste(u_SensorUnit_ID_4,sep=""),pch=16,col=rainbow(n_u_SensorUnit_ID_4),bg="white",cex=1.75)
        par(family="")
      }
      
      dev.off()
      par(def_par)
      
      
    }
  }
  
  # Plot CO2 FRAC_RH_90 time series
  # ------------------------------
  
  if(!SITE%in%c("PAY")){
    
    for(PROC_MODE in c(1,2,4,5,6)){
      
      SITE_P = SITE
      
      if(SITE=="PAYN"){
        SITE_P = "PAY"
      }
      if(SITE=="DUE1"){
        SITE_P = "DUE"
      }
      
      id <- which(statistics$LocationName==SITE
                  & statistics$WEEK!=0
                  & statistics$MODE==PROC_MODE)
      
      u_SensorUnit_ID   <- sort(unique(statistics$SensorUnit_ID[id]))
      n_u_SensorUnit_ID <- length(u_SensorUnit_ID)
      
      minDate <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + min(statistics$WEEK[id])*7*86400
      maxDate <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + max(statistics$WEEK[id])*7*86400 + 7*86400
      axDate  <- seq(strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20200101000000","%Y%m%d%H%M%S",tz="UTC"),"month")
      
      #
      
      figname <- paste(resultdir,SITE,"_FRAC_RH_90_MODE_",sprintf("%02.0f",PROC_MODE),".pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.0,1.0,0.5,0.5),mfrow=c(1,1))
      
      plot(c(minDate,maxDate),c(NA,NA),ylim=c(0,100),xaxt="n",cex.axis=1.5,cex.main=1.5,cex.lab=1.5,xlab="Date [month/year]",ylab="Time fraction RH>90% [%]", main=SITE_P)
      axis(side = 1,at = axDate,labels = strftime(axDate,"%m/%y",tz="UTC"),cex.axis=1.5,cex.lab=1.5)
      
      
      for(ll in seq(0,100,10)){
        lines(c(-1e9,1e12),c(ll,ll),col="gray70",lty=1)
      }
      
      
      for(ith_SUID in 1:n_u_SensorUnit_ID){
        
        id_id      <- which(statistics$SensorUnit_ID[id]==u_SensorUnit_ID[ith_SUID])
        n_id_id    <- length(id_id)
        
        if(n_id_id==0){
          next
        }
        
        RMSE_dates <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + (statistics$WEEK[id[id_id]]+0.5)*7*86400
        
        if(n_u_SensorUnit_ID<=10){
          lines( RMSE_dates,statistics$FRAC_RH_90[id[id_id]],col=rainbow(n_u_SensorUnit_ID)[ith_SUID],lwd=1, lty=1)
          points(RMSE_dates,statistics$FRAC_RH_90[id[id_id]],col=rainbow(n_u_SensorUnit_ID)[ith_SUID],pch=16,cex=0.75)
        }else{
          lines( RMSE_dates,statistics$FRAC_RH_90[id[id_id]],col="gray50",lwd=1, lty=1)
          # points(RMSE_dates,statistics$FRAC_RH_90[id[id_id]],col=1,pch=16,cex=0.75)
        }
      }
      
      if(n_u_SensorUnit_ID>10){
        for(ith_SUID in 1:n_u_SensorUnit_ID){
          id_id      <- which(statistics$SensorUnit_ID[id]==u_SensorUnit_ID[ith_SUID])
          n_id_id    <- length(id_id)
          
          if(n_id_id==0){
            next
          }
          
          RMSE_dates <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC") + (statistics$WEEK[id[id_id]]+0.5)*7*86400
          points(RMSE_dates,statistics$FRAC_RH_90[id[id_id]],col=1,pch=16,cex=0.5)
        }
      }
      
      if(n_u_SensorUnit_ID<=10){
        par(family="mono")
        legend("topleft",legend=paste("SU",u_SensorUnit_ID,sep=""),pch=16,col=rainbow(n_u_SensorUnit_ID),bg="white",cex=1.5)
        par(family="")
      }
      
      dev.off()
      par(def_par)
      
      
    }
  }
}


### ----------------------------------------------------------------------------------------------------------------------------

# Save "statistics"

statistics$Date_UTC_from <- strftime(statistics$Date_UTC_from, "%Y-%m-%d %H:%M:%S", tz="UTC")
statistics$Date_UTC_to   <- strftime(statistics$Date_UTC_to,   "%Y-%m-%d %H:%M:%S", tz="UTC")

write.table(statistics,paste(resultdir,"statistics.csv",sep=""),col.names = T,row.names = F,sep=";")

### ----------------------------------------------------------------------------------------------------------------------------
