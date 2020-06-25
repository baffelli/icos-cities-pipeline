# Comparison_CO2_LP8_LP8.r
# ------------------------

# Remarks:
# - Comparison of CO2 data from LP8 at the same location
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

source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")


### ------------------------------------------------------------------------------------------------------------------------------

## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

# 

if(!as.integer(args[1])%in%c(0,1,2,3,20,30)){
  stop("MODEL: 1, 2, 3, 20, 30 or 0 (= FINAL)!")
}else{
  
  # Final 
  if(as.integer(args[1])==0){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_FINAL/Comparison_LP8_LP8/" 
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_FINAL"
    
    eq_label        <- "(FINAL)"
  }
  
  # Standard 
  if(as.integer(args[1])==1){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis/Comparison_LP8_LP8/" 
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2"
    
    eq_label        <- "(Eq. 4)"
  }
  
  # Test 00
  if(as.integer(args[1])==2){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/Comparison_LP8_LP8/" 
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_TEST00"
    
    eq_label        <- "(Eq. 5)"
  }
  
  # Test 01
  if(as.integer(args[1])==3){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01/Comparison_LP8_LP8/" 
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_TEST01"
    
    eq_label        <- "(Eq. 4)"
  }
  
  # Test 00 AMT
  if(as.integer(args[1])==20){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00_AMT/Comparison_LP8_LP8/" 
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_TEST00_AMT"
    
    eq_label        <- "(Eq. 5)"
  }
  
  # Test 01 AMT
  if(as.integer(args[1])==30){
    # Directories
    resultdir       <- "/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST01_AMT/Comparison_LP8_LP8/" 
    # Table with processed CO2 measurements
    ProcDataTblName <- "CarboSense_CO2_TEST01_AMT"
    
    eq_label        <- "(Eq. 4)"
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

SITE_PAIRS <- NULL

#

SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="LAEG",LocationName_2="LAEG",stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="HAE", LocationName_2="HAE", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="RIG", LocationName_2="RIG", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="PAY", LocationName_2="PAYN",stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="BRM", LocationName_2="BRM", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="MAGN",LocationName_2="MAGN",stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="LUG", LocationName_2="LUGN",stringsAsFactors = F))

#

SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="CHM", LocationName_2="CHAN", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="BAS", LocationName_2="BASN", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="ZBRC",LocationName_2="ZBRC", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="ZBRC",LocationName_2="ZBRC2",stringsAsFactors = F))

#
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="AJGR",LocationName_2="AJGR",stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="ZGHD",LocationName_2="ZGHD",stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="ZHRG",LocationName_2="ZHRG",stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="BSCR",LocationName_2="BSCR",stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="MOA", LocationName_2="MOA", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="MOE", LocationName_2="MOE", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="HLL", LocationName_2="HLL", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="GRE", LocationName_2="GRE", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="CGI", LocationName_2="CGI", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="REH", LocationName_2="REH", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="EBK", LocationName_2="EBK", stringsAsFactors = F))

#

SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="ZSBS", LocationName_2="ZHRG", stringsAsFactors = F))
SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="ZGHD", LocationName_2="ZSCH", stringsAsFactors = F))

#

SITE_PAIRS <- rbind(SITE_PAIRS,data.frame(LocationName_1="SION", LocationName_2="SAVE", stringsAsFactors = F))

### ----------------------------------------------------------------------------------------------------------------------------

query_str         <- paste("SELECT * FROM Location;",sep="")
drv               <- dbDriver("MySQL")
con               <- dbConnect(drv, group="CarboSense_MySQL")
res               <- dbSendQuery(con, query_str)
LOCATIONS         <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

### ----------------------------------------------------------------------------------------------------------------------------

statistics <- NULL

### ----------------------------------------------------------------------------------------------------------------------------

for(ith_SITE_PAIR in 1:dim(SITE_PAIRS)[1]){
  
  
  query_str         <- paste("SELECT DISTINCT SensorUnit_ID, LocationName FROM ",ProcDataTblName," WHERE LocationName='",SITE_PAIRS$LocationName_1[ith_SITE_PAIR],"';",sep="")
  drv               <- dbDriver("MySQL")
  con               <- dbConnect(drv, group="CarboSense_MySQL")
  res               <- dbSendQuery(con, query_str)
  SU_LOC_1          <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  
  query_str         <- paste("SELECT DISTINCT SensorUnit_ID, LocationName FROM ",ProcDataTblName," WHERE LocationName='",SITE_PAIRS$LocationName_2[ith_SITE_PAIR],"';",sep="")
  drv               <- dbDriver("MySQL")
  con               <- dbConnect(drv, group="CarboSense_MySQL")
  res               <- dbSendQuery(con, query_str)
  SU_LOC_2          <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  
  #
  
  SU_LOC <- rbind(SU_LOC_1,SU_LOC_2)
  SU_LOC <- unique(SU_LOC)
  
  if(dim(SU_LOC)[1]<=1){
    next
  }
  
  #
  
  
  for(ii_SU_LOC in 1:(dim(SU_LOC)[1]-1)){
    
    
    query_str         <- paste("SELECT * FROM ",ProcDataTblName," WHERE SensorUnit_ID = ",SU_LOC$SensorUnit_ID[ii_SU_LOC]," and LocationName='",SU_LOC$LocationName[ii_SU_LOC],"';",sep="")
    drv               <- dbDriver("MySQL")
    con               <- dbConnect(drv, group="CarboSense_MySQL")
    res               <- dbSendQuery(con, query_str)
    data_SU_1         <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)

    colnames(data_SU_1) <- paste("SU1_",colnames(data_SU_1),sep="")
    
    # Sync timestamp to regular 10 minute interval
    
    ts_sync <- (data_SU_1$SU1_timestamp%/%600)*600
    id_1    <- which((data_SU_1$SU1_timestamp-ts_sync)<=300)
    id_2    <- which((data_SU_1$SU1_timestamp-ts_sync) >300)
    
    if(length(id_1)>0){
      data_SU_1$SU1_timestamp[id_1] <- ts_sync[id_1] 
    }
    if(length(id_2)>0){
      data_SU_1$SU1_timestamp[id_2] <- ts_sync[id_2] + 600
    }
    
    data_SU_1 <- data_SU_1[!duplicated(data_SU_1$SU1_timestamp), ]
    
    #
    
    data_SU_1$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_SU_1$SU1_timestamp
    data_SU_1$date <- strptime(strftime(data_SU_1$date,"%Y%m%d000000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC") + as.numeric(strftime(data_SU_1$date,"%H",tz="UTC"))*3600 + (as.numeric(strftime(data_SU_1$date,"%M",tz="UTC"))%/%10)*600
    data_SU_1      <- data_SU_1[!duplicated(data_SU_1$date),]
    data_SU_1      <- data_SU_1[order(data_SU_1$date,decreasing = F),]
    
    for(jj_SU_LOC in (ii_SU_LOC+1):dim(SU_LOC)[1]){
      
      query_str    <- paste("SELECT * FROM ",ProcDataTblName," WHERE SensorUnit_ID = ",SU_LOC$SensorUnit_ID[jj_SU_LOC]," and LocationName='",SU_LOC$LocationName[jj_SU_LOC],"';",sep="")
      drv          <- dbDriver("MySQL")
      con          <- dbConnect(drv, group="CarboSense_MySQL")
      res          <- dbSendQuery(con, query_str)
      data_SU_2    <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      colnames(data_SU_2) <- paste("SU2_",colnames(data_SU_2),sep="")
      
      # Sync timestamp to regular 10 minute interval
      
      ts_sync <- (data_SU_2$SU2_timestamp%/%600)*600
      id_1    <- which((data_SU_2$SU2_timestamp-ts_sync)<=300)
      id_2    <- which((data_SU_2$SU2_timestamp-ts_sync) >300)
      
      if(length(id_1)>0){
        data_SU_2$SU2_timestamp[id_1] <- ts_sync[id_1] 
      }
      if(length(id_2)>0){
        data_SU_2$SU2_timestamp[id_2] <- ts_sync[id_2] + 600
      }
      
      data_SU_2 <- data_SU_2[!duplicated(data_SU_2$SU2_timestamp), ]
      
      #
      
      data_SU_2$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data_SU_2$SU2_timestamp
      data_SU_2$date <- strptime(strftime(data_SU_2$date,"%Y%m%d000000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC") + as.numeric(strftime(data_SU_2$date,"%H",tz="UTC"))*3600 + (as.numeric(strftime(data_SU_2$date,"%M",tz="UTC"))%/%10)*600
      data_SU_2      <- data_SU_2[!duplicated(data_SU_2$date),]
      data_SU_2      <- data_SU_2[order(data_SU_2$date,decreasing = F),]

      #
      
      data <- merge(data_SU_1,data_SU_2)
      
      #
      
      id_loc_ii <- which(LOCATIONS$LocationName==SU_LOC$LocationName[ii_SU_LOC])
      id_loc_jj <- which(LOCATIONS$LocationName==SU_LOC$LocationName[jj_SU_LOC])

      distBetweenSensors_SU1_SU2 <- sqrt((LOCATIONS$Y_LV03[id_loc_ii]-LOCATIONS$Y_LV03[id_loc_jj])^2+(LOCATIONS$X_LV03[id_loc_ii]-LOCATIONS$X_LV03[id_loc_jj])^2)

      #
      
      S01_M00 <- NULL
      S02_M00 <- NULL
      
      for(mode in 0:5){
        
        if(mode==0){
          
          id_compare <- which(data$SU1_CO2_A    != -999
                              & data$SU2_CO2_A  != -999
                              & !is.na(data$SU1_CO2_A)
                              & !is.na(data$SU2_CO2_A))
          
          pos1 <- which(colnames(data)=="SU1_CO2_A")
          pos2 <- which(colnames(data)=="SU2_CO2_A")
          
          if(length(id_compare)>10){
            S01_M00 <- data[id_compare,pos1]
            S02_M00 <- data[id_compare,pos2]
          }
          
          next
        }
        
        if(mode==1){
          
          id_compare <- which(data$SU1_CO2   != -999
                              & data$SU2_CO2 != -999
                              & !is.na(data$SU1_CO2)
                              & !is.na(data$SU2_CO2))
          
          pos1 <- which(colnames(data)=="SU1_CO2")
          pos2 <- which(colnames(data)=="SU2_CO2")
          DESC <- "MODE_01"
        }
        if(mode==2){
          
          id_compare <- which(data$SU1_CO2_A    != -999
                              & data$SU2_CO2_A  != -999
                              & !is.na(data$SU1_CO2_A)
                              & !is.na(data$SU2_CO2_A)
                              & data$SU1_FLAG == 1
                              & data$SU2_FLAG == 1)
          
          pos1 <- which(colnames(data)=="SU1_CO2_A")
          pos2 <- which(colnames(data)=="SU2_CO2_A")
          DESC <- "MODE_02"
        }
        if(mode==3){
          next
        }
        if(mode==4){
          
          id_compare <- which(data$SU1_CO2_A   != -999
                              & data$SU2_CO2_A != -999
                              & !is.na(data$SU1_CO2_A)
                              & !is.na(data$SU2_CO2_A)
                              & data$SU1_O_FLAG   == 1
                              & data$SU2_O_FLAG   == 1)
          
          pos1 <- which(colnames(data)=="SU1_CO2_A")
          pos2 <- which(colnames(data)=="SU2_CO2_A")
          DESC <- "MODE_04"
        }
        if(mode==5){
          
          id_compare <- which(data$SU1_CO2_A   != -999
                              & data$SU2_CO2_A != -999
                              & !is.na(data$SU1_CO2_A)
                              & !is.na(data$SU2_CO2_A)
                              & data$SU1_O_FLAG   == 1
                              & data$SU2_O_FLAG   == 1
                              & data$SU1_L_FLAG   == 1
                              & data$SU2_L_FLAG   == 1)
          
          pos1 <- which(colnames(data)=="SU1_CO2_A")
          pos2 <- which(colnames(data)=="SU2_CO2_A")
          DESC <- "MODE_05"
        }
        if(mode==6){
          next
        }
        
        n_id_compare <- length(id_compare)
        
        if(n_id_compare>10){
          M1 <- data[id_compare,pos1]
          M2 <- data[id_compare,pos2]
          DD <- M2-M1
        }else{
          next
        }
        
        RMSE    <- sqrt(sum(DD^2)/n_id_compare)
        CorCoef <- cor(M1,M2,method="pearson",use="complete.obs")
        
        fit <- lm(M2~M1,data.frame(M2=M2,M1=M1,stringsAsFactors = F))
        c1  <- fit$coefficients[1]
        c2  <- fit$coefficients[2]
        
        #
        
        statistics <- rbind(statistics,data.frame(LocationName_1  = SU_LOC$LocationName[ii_SU_LOC],
                                                  SU_ID1          = SU_LOC$SensorUnit_ID[ii_SU_LOC],
                                                  LocationName_2  = SU_LOC$LocationName[jj_SU_LOC],
                                                  SU_ID2          = SU_LOC$SensorUnit_ID[jj_SU_LOC],
                                                  DIST            = distBetweenSensors_SU1_SU2,
                                                  DESC            = DESC,
                                                  Date_UTC_from   = strftime(min(data$date),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                  Date_UTC_to     = strftime(max(data$date),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                  SLOPE           = c2,
                                                  RMSE            = RMSE,
                                                  COR             = CorCoef,
                                                  N               = n_id_compare,
                                                  LocationName_1_CO2_Q005 = quantile(M1,probs=0.05),
                                                  LocationName_1_CO2_Q095 = quantile(M1,probs=0.95),
                                                  LocationName_2_CO2_Q005 = quantile(M2,probs=0.05),
                                                  LocationName_2_CO2_Q095 = quantile(M2,probs=0.95),
                                                  stringsAsFactors = F))
        
        
        # PLOT: SCATTER/HIST
        
        str_L_00 <- paste("SLOPE:    ",sprintf("%5.2f",c2))
        str_L_01 <- paste("INTERCEPT:",sprintf("%5.1f",c1))
        str_L_02 <- paste("RMSE:     ",sprintf("%5.1f",RMSE))
        str_L_03 <- paste("COR:      ",sprintf("%5.2f",CorCoef))
        str_L_04 <- paste("N:        ",sprintf("%5.0f",n_id_compare))
        
        if(mode%in%c(4,5) & !is.null(S01_M00) & !is.null(S02_M00)){
          str_L_05 <- paste("N ALL:    ",sprintf("%5.0f",length(S01_M00)))
          str_L_06 <- paste("RMSE ALL: ",sprintf("%5.1f",sqrt(sum((S01_M00-S02_M00)^2)/length(S01_M00))))
          str_L_07 <- paste("COR ALL:  ",sprintf("%5.2f",cor(S01_M00,S02_M00,method="pearson",use="complete.obs")))
        }
        
        str_L_02_SCO <- paste("RMSE:    ",sprintf("%5.1f",RMSE))
        str_L_03_SCO <- paste("COR:     ",sprintf("%5.2f",CorCoef))
        str_L_04_SCO <- paste("N:      ", sprintf("%6.0f",n_id_compare))
        
        if(mode%in%c(4,5) & !is.null(S01_M00) & !is.null(S02_M00)){
          str_L_05_SCO <- paste("N ALL:  ", sprintf("%6.0f",length(S01_M00)))
          str_L_06_SCO <- paste("RMSE ALL:",sprintf("%5.1f",sqrt(sum((S01_M00-S02_M00)^2)/length(S01_M00))))
          str_L_07_SCO <- paste("COR ALL: ",sprintf("%5.2f",cor(S01_M00,S02_M00,method="pearson",use="complete.obs")))
        }
        
        
        str_R_00 <- paste("MEAN:",sprintf("%6.1f",mean(DD)))
        str_R_01 <- paste("Q000:",sprintf("%6.1f",quantile(DD,probs=0.00)))
        str_R_02 <- paste("Q001:",sprintf("%6.1f",quantile(DD,probs=0.01)))
        str_R_03 <- paste("Q050:",sprintf("%6.1f",quantile(DD,probs=0.50)))
        str_R_04 <- paste("Q099:",sprintf("%6.1f",quantile(DD,probs=0.99)))
        str_R_05 <- paste("Q100:",sprintf("%6.1f",quantile(DD,probs=1.00)))
        
        str_T_00 <- paste("DIST: ",sprintf("%4.0f",distBetweenSensors_SU1_SU2),"m",sep="")
        
        #
        
        figname <- paste(resultdir,SU_LOC$LocationName[ii_SU_LOC],"_",SU_LOC$SensorUnit_ID[ii_SU_LOC],"_",SU_LOC$LocationName[jj_SU_LOC],"_",SU_LOC$SensorUnit_ID[jj_SU_LOC],"_",DESC,".pdf",sep="")
        
        # mainStr <- paste(SU_LOC$LocationName[ii_SU_LOC],SU_LOC$SensorUnit_ID[ii_SU_LOC],"-",SU_LOC$LocationName[jj_SU_LOC],SU_LOC$SensorUnit_ID[jj_SU_LOC]," / ",DESC,sep=" ")
        
        mainStr <- bquote(.(SU_LOC$LocationName[ii_SU_LOC]) ~ .(SU_LOC$SensorUnit_ID[ii_SU_LOC]) ~ "-" ~ .(SU_LOC$LocationName[jj_SU_LOC]) ~ .(SU_LOC$SensorUnit_ID[jj_SU_LOC]) ~ "/" ~ .(DESC))
        
        subStr  <- paste("Data: ",
                         strftime(min(data$date),"%Y/%m/%d",tz="UTC"),
                         "-",
                         strftime(max(data$date),"%Y/%m/%d",tz="UTC"),sep="")
        
        def_par <- par()
        pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
        par(mai=c(1.25,1,1.0,0.5),mfrow=c(1,2))
        
        # xlabString <- paste(SU_LOC$LocationName[ii_SU_LOC]," SU ",SU_LOC$SensorUnit_ID[ii_SU_LOC], " CO2 [ppm]",sep="")
        # ylabString <- paste(SU_LOC$LocationName[jj_SU_LOC]," SU ",SU_LOC$SensorUnit_ID[jj_SU_LOC], " CO2 [ppm]",sep="")
        
        xlabString <- bquote(.(SU_LOC$LocationName[ii_SU_LOC]) ~ .(paste("SU",SU_LOC$SensorUnit_ID[ii_SU_LOC],sep="")) ~ CO[2] ~ .("[ppm]"))
        ylabString <- bquote(.(SU_LOC$LocationName[jj_SU_LOC]) ~ .(paste("SU",SU_LOC$SensorUnit_ID[jj_SU_LOC],sep="")) ~ CO[2] ~ .("[ppm]"))
        
        plot(0,0,xlim=c(350,700),ylim=c(350,700),main=mainStr,xlab=xlabString,ylab=ylabString,sub=subStr,cex.axis=1.25,cex.lab=1.25,cex.main=1.25)
        
        points(M1,M2,col="black",pch=16,cex=0.5)
        
        lines(c(0,1e4),c(c1,c1+1e4*c2),col=2)
        
        lines(c(0,1e4),c(0,1e4),   col=1)
        lines(c(0,1e4),c(0,1e4)+20,col=1,lty=50)
        lines(c(0,1e4),c(0,1e4)-20,col=1,lty=50)
        
        par(family="mono")
        legend("bottomright",legend=c(str_L_00,str_L_01,str_L_02,str_L_03,str_L_04),bg="white",cex=1.25)
        par(family="")
        
        #
        
        Q050_S <- quantile(DD,probs=0.50)
        MEAN_S <- mean(DD)
        
        # xlabString <- paste(SU_LOC$LocationName[ii_SU_LOC],"/SU",SU_LOC$SensorUnit_ID[ii_SU_LOC]," - ",SU_LOC$LocationName[jj_SU_LOC],"/SU",SU_LOC$SensorUnit_ID[jj_SU_LOC], " CO2 [ppm]",sep="")
        
        xlabString <- bquote(.(paste(SU_LOC$LocationName[ii_SU_LOC],"/SU",SU_LOC$SensorUnit_ID[ii_SU_LOC],sep="")) ~ "-" ~.(paste(SU_LOC$LocationName[jj_SU_LOC],"/SU",SU_LOC$SensorUnit_ID[ii_SU_LOC],sep="")) ~ CO[2] ~ "[ppm]")
        
        hist(DD,seq(floor(min(DD)),ceiling(max(DD))+2,2),xlim=c(Q050_S-40,Q050_S+40),col="slategray",main="",xlab=xlabString,ylab="Number",cex.axis=1.25,cex.lab=1.25)
        lines(c(MEAN_S,MEAN_S),c(-1e9,1e9),col=2,lwd=1)
        lines(c(MEAN_S+20,MEAN_S+20),c(-1e9,1e9),col=2,lwd=1,lty=5)
        lines(c(MEAN_S-20,MEAN_S-20),c(-1e9,1e9),col=2,lwd=1,lty=5)
        par(family="mono")
        legend("topright",legend=c(str_R_00,str_R_01,str_R_02,str_R_03,str_R_04,str_R_05),bg="white",cex=1.25)
        par(family="")
        
        #
        
        dev.off()
        par(def_par)
        
        ##
        
        if(mode%in%c(4,5) & !is.null(S01_M00) & !is.null(S02_M00)){
          
          figname <- paste(resultdir,SU_LOC$LocationName[ii_SU_LOC],"_",SU_LOC$SensorUnit_ID[ii_SU_LOC],"_",SU_LOC$LocationName[jj_SU_LOC],"_",SU_LOC$SensorUnit_ID[jj_SU_LOC],"_",DESC,"_SCATTER_ONLY.pdf",sep="")
          
          mainStr <- paste(SU_LOC$LocationName[ii_SU_LOC]," SU",SU_LOC$SensorUnit_ID[ii_SU_LOC]," - ",SU_LOC$LocationName[jj_SU_LOC]," SU",SU_LOC$SensorUnit_ID[jj_SU_LOC]," ",eq_label,sep="")
          subStr  <- paste("Data: ",
                           strftime(min(data$date),"%Y/%m/%d",tz="UTC"),
                           "-",
                           strftime(max(data$date),"%Y/%m/%d",tz="UTC"),sep="")
          
          def_par <- par()
          pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1.25,1,0.75,0.1),mfrow=c(1,1))
          
          # xlabString <- paste(SU_LOC$LocationName[ii_SU_LOC]," SU",SU_LOC$SensorUnit_ID[ii_SU_LOC], " CO2 [ppm]",sep="")
          # ylabString <- paste(SU_LOC$LocationName[jj_SU_LOC]," SU",SU_LOC$SensorUnit_ID[jj_SU_LOC], " CO2 [ppm]",sep="")
          
          xlabString <- bquote(.(SU_LOC$LocationName[ii_SU_LOC]) ~ .(paste("SU",SU_LOC$SensorUnit_ID[ii_SU_LOC],sep="")) ~ CO[2] ~ .("[ppm]"))
          ylabString <- bquote(.(SU_LOC$LocationName[jj_SU_LOC]) ~ .(paste("SU",SU_LOC$SensorUnit_ID[jj_SU_LOC],sep="")) ~ CO[2] ~ .("[ppm]"))
          
          plot(0,0,xlim=c(350,700),ylim=c(350,700),main=mainStr,xlab=xlabString,ylab=ylabString,cex.axis=1.75,cex.lab=1.75,cex.main=1.75)
          
          points(S01_M00,S02_M00,col="grey50",pch=16,cex=0.5)
          points(M1,   M2,   col="black", pch=16,cex=0.5)
          
          lines(c(0,1e4),c(0,1e4),   col=1)
          lines(c(0,1e4),c(0,1e4)+20,col=1,lty=50)
          lines(c(0,1e4),c(0,1e4)-20,col=1,lty=50)
          
          mtext(text = subStr,side = 1,line = 4.5,cex=1.5)
          
          par(family="mono")
          legend("bottomright",legend=c(str_L_02_SCO,str_L_03_SCO,str_L_04_SCO,"",str_L_06_SCO,str_L_07_SCO,str_L_05_SCO),bg="white",cex=1.75)
          par(family="")
          
          par(family="mono")
          legend("topleft",legend=c(str_T_00),bg="white",cex=1.75)
          par(family="")
          
          str_T_00
          
          #
          
          dev.off()
          par(def_par)
          
        }
        
        ##
        
        figname    <- paste(resultdir,SU_LOC$LocationName[ii_SU_LOC],"_",SU_LOC$SensorUnit_ID[ii_SU_LOC],"_",SU_LOC$LocationName[jj_SU_LOC],"_",SU_LOC$SensorUnit_ID[jj_SU_LOC],"_",DESC,"_TS.pdf",sep="")
        
        yyy        <- cbind(data[,pos1],
                            rep(NA,dim(data)[1]),
                            data[,pos2],
                            rep(NA,dim(data)[1]))
        
        yyy[id_compare,2] <- data[id_compare,pos1]
        yyy[id_compare,4] <- data[id_compare,pos2]
        
        xlabString <- "Date" 
        ylabString <- expression(paste("CO"[2]*" [ppm]"))
        legend_str <- c(paste(SU_LOC$SensorUnit_ID[ii_SU_LOC],"CO2 (ALL)"),
                        paste(SU_LOC$SensorUnit_ID[ii_SU_LOC],"CO2 (USE)"),
                        paste(SU_LOC$SensorUnit_ID[jj_SU_LOC],"CO2 (ALL)"),
                        paste(SU_LOC$SensorUnit_ID[jj_SU_LOC],"CO2 (USE)"))
        
        plot_ts(figname,data$date,yyy,"week",NULL,c(350,700),xlabString,ylabString,legend_str)
        
      }
    }
  }
}

#

write.table(statistics,paste(resultdir,"statistics.csv",sep=""),col.names=T,row.names=F,sep=";")
