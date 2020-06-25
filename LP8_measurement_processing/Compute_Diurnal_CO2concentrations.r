# Compute_Diurnal_CO2concentrations.r
# --------------------------------------------------
#
# Author: Michael Mueller
#
# --------------------------------------------------
#
# Remarks:
# - Computations refer to UTC.
#
# --------------------------------------------------
#


## clear variables
rm(list=ls(all=TRUE))
gc()

## libraries
library(openair)
library(DBI)
require(RMySQL)
require(chron)

### ------------------------------------------------------------------------------------------------------------------------------

## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

# 

if(!as.integer(args[1])%in%c(0,1,2,3,20,30)){
  stop("MODEL: 1, 2, 3, 20, 30 or 0 (= FINAL)!")
}else{
  
  # FINAL 
  if(as.integer(args[1])==0){
    # directories and filenames
    resultdir           <- "/project/CarboSense/Carbosense_Network/Diurnal_CO2_Variations_FINAL/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_FINAL"
  }
  
  # Standard 
  if(as.integer(args[1])==1){
    # directories and filenames
    resultdir           <- "/project/CarboSense/Carbosense_Network/Diurnal_CO2_Variations/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2"
  }
  
  # Test 00
  if(as.integer(args[1])==2){
    # directories and filenames
    resultdir           <- "/project/CarboSense/Carbosense_Network/Diurnal_CO2_Variations_TEST00/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST00"
  }
  
  # Test 01
  if(as.integer(args[1])==3){
    # directories and filenames
    resultdir           <- "/project/CarboSense/Carbosense_Network/Diurnal_CO2_Variations_TEST01/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST01"
  }
  
  # Test 00 AMT
  if(as.integer(args[1])==20){
    # directories and filenames
    resultdir           <- "/project/CarboSense/Carbosense_Network/Diurnal_CO2_Variations_TEST00_AMT/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST00_AMT"
  }
  
  # Test 01 AMT
  if(as.integer(args[1])==30){
    # directories and filenames
    resultdir           <- "/project/CarboSense/Carbosense_Network/Diurnal_CO2_Variations_TEST01_AMT/"
    # Table with processed CO2 measurements
    ProcMeasDBTableName <- "CarboSense_CO2_TEST01_AMT"
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

# Database queries : metadata

# Table "Deployment"

query_str       <- paste("SELECT * FROM Deployment WHERE Date_UTC_from >= '2017-07-01 00:00:00';",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_deployment  <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_deployment$Date_UTC_from <- strptime(tbl_deployment$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
tbl_deployment$Date_UTC_to   <- strptime(tbl_deployment$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")

id <- which(tbl_deployment$Date_UTC_to==strptime("2100-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"))

if(length(id)>0){
  date_now <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
  for(ii in 1:length(id)){
    tbl_deployment$Date_UTC_to[id[ii]] <- date_now
  }
}

tbl_deployment$duration_days <- as.numeric(difftime(time1=tbl_deployment$Date_UTC_to,time2=tbl_deployment$Date_UTC_from,units="days",tz="UTC"))


# Table "Location"

query_str       <- paste("SELECT * FROM Location;",sep="")
drv             <- dbDriver("MySQL")
con             <- dbConnect(drv, group="CarboSense_MySQL")
res             <- dbSendQuery(con, query_str)
tbl_location    <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)


# ----------------------------------------------------------------------------------------------------------------------

# Create directories

if(!dir.exists(resultdir)){
  dir.create((gsub(pattern = "/$",replacement = "",resultdir)))
}

# ----------------------------------------------------------------------------------------------------------------------

diurnalVariations     <- list()
dV_cc                 <- 0

for(ith_depl in 1:dim(tbl_deployment)[1]){
  
  
  # Filter particulat deployments
  
  if(!tbl_deployment$SensorUnit_ID[ith_depl]%in%c(1010:1334)){
    next
  }
  
  if(tbl_deployment$LocationName[ith_depl]%in%c("DUE1","DUE2","DUE3","DUE4","MET1")){
    next
  }
  
  if(tbl_deployment$duration_days[ith_depl]<7){
    next
  }
  
  Height    <- tbl_location$h[which(tbl_location$LocationName==tbl_deployment$LocationName[ith_depl])]
  X_LV03    <- tbl_location$X_LV03[which(tbl_location$LocationName==tbl_deployment$LocationName[ith_depl])]
  Y_LV03    <- tbl_location$Y_LV03[which(tbl_location$LocationName==tbl_deployment$LocationName[ith_depl])]
  LAT_WGS84 <- tbl_location$LAT_WGS84[which(tbl_location$LocationName==tbl_deployment$LocationName[ith_depl])]
  LON_WGS84 <- tbl_location$LON_WGS84[which(tbl_location$LocationName==tbl_deployment$LocationName[ith_depl])]
  Canton    <- tbl_location$Canton[which(tbl_location$LocationName==tbl_deployment$LocationName[ith_depl])]
  
  # import data
  query_str <- paste("SELECT timestamp, CO2, CO2_A, FLAG, O_FLAG FROM ",ProcMeasDBTableName," ",sep="")
  query_str <- paste(query_str,"WHERE SensorUnit_ID = ",tbl_deployment$SensorUnit_ID[ith_depl]," and LocationName = '",tbl_deployment$LocationName[ith_depl],"';",sep="")
  drv       <- dbDriver("MySQL")
  con       <- dbConnect(drv, group="CarboSense_MySQL")
  res       <- dbSendQuery(con, query_str)
  data      <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  data$date  <- data$timestamp + strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")
  data$year  <- as.numeric(strftime(data$date,"%Y",tz="UTC"))
  data$month <- as.numeric(strftime(data$date,"%m",tz="UTC"))
  data$day   <- as.numeric(strftime(data$date,"%d",tz="UTC"))
  data$hour  <- as.numeric(strftime(data$date,"%H",tz="UTC"))
  
  # Hist diurnal CO2 conentrations
  
  descriptor <- paste(tbl_deployment$SensorUnit_ID[ith_depl],"_",tbl_deployment$LocationName[ith_depl],sep="")
  
  for(version in 1:4){
    
    if(version==1){
      figname <- paste(resultdir,descriptor,"_C_ALL",".pdf",sep="")
      
      if(all(data$CO2==-999)==T){
        next
      }
    }
    if(version==2){
      figname <- paste(resultdir,descriptor,"_A_ALL",".pdf",sep="")
      
      if(all(data$CO2_A==-999)==T){
        next
      }
    }
    if(version==3){
      figname <- paste(resultdir,descriptor,"_C_F_1",".pdf",sep="")
      
      if(all(data$CO2==-999 | data$O_FLAG==0)==T){
        next
      }
    }
    if(version==4){
      figname <- paste(resultdir,descriptor,"_A_F_1",".pdf",sep="")
      
      if(all(data$CO2_A==-999 | data$O_FLAG==0)==T){
        next
      }
    }
    
    
    
    def_par <- par()
    pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1,1,0.75,1),mfrow=c(1,1))
    
    
    for(yy in unique(data$year)){
      for(mm in 1:12){
        
        id   <- which(data$year == yy & data$month == mm)
        n_id <- length(id)
        
        if(n_id == 0){
          next
        }
        
        tmp <- matrix(NA,ncol=24,nrow=n_id)
        
        for(hh in 0:23){
          if(version==1){
            id_id   <- which(data$hour[id]==hh & data$CO2[id] != -999)
          }
          if(version==2){
            id_id   <- which(data$hour[id]==hh & data$CO2_A[id] != -999)
          }
          if(version==3){
            id_id   <- which(data$hour[id]==hh & data$CO2[id] != -999 & data$O_FLAG[id]==1)
          }
          if(version==4){
            id_id   <- which(data$hour[id]==hh & data$CO2_A[id] != -999 & data$O_FLAG[id]==1)
          }
          
          n_id_id <- length(id_id)
          
          if(n_id_id>0){
            if(version%in%c(1,3)){
              tmp[1:n_id_id,hh+1] <- data$CO2[id[id_id]]
            }
            if(version%in%c(2,4)){
              tmp[1:n_id_id,hh+1] <- data$CO2_A[id[id_id]]
            }
            
          }
        }
        
        mainString <- paste(sprintf("%02.0f",mm),sprintf("%04.0f",yy),"/",tbl_deployment$SensorUnit_ID[ith_depl],tbl_deployment$LocationName[ith_depl])
        
        xlabString <- expression(paste("Time of day [UTC]"))
        
        if(version==1){
          ylabString <- expression(paste("CO"[2]*" [ppm]"))
        }
        if(version==2){
          ylabString <- expression(paste("CO"[2]*" [ppm] (linked to DUE)"))
        }
        if(version==3){
          ylabString <- expression(paste("CO"[2]*" [ppm] (FLAG=1)"))
        }
        if(version==4){
          ylabString <- expression(paste("CO"[2]*" [ppm] (linked to DUE; FLAG=1)"))
        }
        
        boxplot(tmp,xlim=c(0,25),ylim=c(380,550),xlab=xlabString,ylab=ylabString,main=mainString,col="slategray",cex=0.5,pch=16,cex.main=1.25,cex.lab=1.25,cex.axis=1.25,xaxt="n")
        for(y_ll_i in seq(400,500,50)){
          lines(c(-1e9,1e9),c(y_ll_i,y_ll_i),lwd=1,lty=1,col="darkred")
        }
        par(new=T)
        plot(1:24,apply(!is.na(tmp),2,sum),pch=15,col=2,xaxt="n",yaxt="n",xlab="",ylab="",main="",xlim=c(0,25),ylim=c(0,190))
        axis(side = 1,at = seq(0,24,4),labels = seq(0,24,4),cex.label=1.25,cex.axis=1.25)
        axis(4,cex.label=1.25,cex.axis=1.25)
        
        mtext(text = "Number of measurements",side = 4,line = 2,cex=1.25)
        
        ##
        
        dV_cc <- dV_cc + 1
        
        diurnalVariations[[dV_cc]] <- list(LocationName  = tbl_deployment$LocationName[ith_depl],
                                           SensorUnit_ID = tbl_deployment$SensorUnit_ID[ith_depl],
                                           Height        = Height,
                                           X_LV03        = X_LV03,
                                           Y_LV03        = Y_LV03,
                                           LAT_WGS84     = LAT_WGS84,
                                           LON_WGS84     = LON_WGS84,
                                           Canton        = Canton,
                                           year          = yy,
                                           version       = version,
                                           month         = mm,
                                           median        = apply(tmp,2,median,na.rm=T),
                                           N             = apply(!is.na(tmp),2,sum))
      }
    }
    
    dev.off()
    par(def_par)
  }
}




# ----------------------------------------------------------------------------------------------------------------------

# REF SITES

sites         <- c("HAE","RIG","DUE","PAY","BRM","LAEG","GIMM")
n_sites       <- length(sites)
table_names   <- c(paste("NABEL_",sites[1:4],sep=""),"UNIBE_BRM","EMPA_LAEG","UNIBE_GIMM")



for(ith_site in 1:n_sites){
  
  #
  
  id_loc_ref <- which(tbl_location$LocationName == sites[ith_site])
  
  if(sites[ith_site]=="DUE"){
    id_loc_ref <- which(tbl_location$LocationName == "DUE1")
  }
  if(sites[ith_site]=="PAY"){
    id_loc_ref <- which(tbl_location$LocationName == "PAYN")
  }
  
  Height    <- tbl_location$h[id_loc_ref]
  X_LV03    <- tbl_location$X_LV03[id_loc_ref]
  Y_LV03    <- tbl_location$Y_LV03[id_loc_ref]
  LAT_WGS84 <- tbl_location$LAT_WGS84[id_loc_ref]
  LON_WGS84 <- tbl_location$LON_WGS84[id_loc_ref]
  Canton    <- tbl_location$Canton[id_loc_ref]
  
  #
  
  for(species in c("O3","NO","NO2","CO2","CO")){
    
    if(sites[ith_site]%in%c("BRM","LAEG","GIMM") & species!="CO2"){
      next
    }
    
    # import data
    
    if(sites[ith_site]%in%c("DUE","RIG","PAY")){
      if(species=="CO2"){
        if(sites[ith_site]%in%c("DUE","RIG")){
          query_str <- paste("SELECT timestamp,CO2_DRY_CAL,CO2_DRY_F,H2O,H2O_F FROM ",table_names[ith_site],";",sep="")
        }
        if(sites[ith_site]=="PAY"){
          query_str <- paste("SELECT timestamp,CO2_DRY_CAL,CO2_DRY_F,H2O_CAL,H2O_F FROM ",table_names[ith_site],";",sep="")
        }
        drv       <- dbDriver("MySQL")
        con       <- dbConnect(drv, group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        data_tmp  <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        if(sites[ith_site]=="PAY"){
          colnames(data_tmp)[which(colnames(data_tmp)=="H2O_CAL")]     <- "H2O"
          colnames(data_tmp)[which(colnames(data_tmp)=="CO2_DRY_CAL")] <- "CO2_DRY"
        }
        
        if(sites[ith_site]%in%c("DUE","RIG")){
          colnames(data_tmp)[which(colnames(data_tmp)=="CO2_DRY_CAL")] <- "CO2_DRY"
        }
        
        data_tmp$CO2   <- data_tmp$CO2_DRY * (1 - data_tmp$H2O/100)
        data_tmp$CO2_F <- 1
        
        data           <- data_tmp[,c(which(colnames(data_tmp)=="timestamp"),which(colnames(data_tmp)=="CO2"),which(colnames(data_tmp)=="CO2_F"))]
        
        id_setToNA     <- which(data_tmp$CO2_DRY     == -999
                                | data_tmp$H2O       == -999
                                | data_tmp$CO2_DRY_F == 0
                                | data_tmp$H2O_F     == 0)
        
        if(length(id_setToNA)>0){
          data$CO2[id_setToNA]   <- -999
          data$CO2_F[id_setToNA] <- 0
        }
        
        rm(data_tmp)
        gc()
        
      }else{
        query_str <- paste("SELECT timestamp,",species,",",paste(species,"_F",sep="")," FROM ",table_names[ith_site],";",sep="")
        drv       <- dbDriver("MySQL")
        con       <- dbConnect(drv, group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        data      <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
      }
    }
    if(sites[ith_site]=="HAE"){
      query_str <- paste("SELECT timestamp,",species,",",paste(species,"_F",sep="")," FROM ",table_names[ith_site],";",sep="")
      drv       <- dbDriver("MySQL")
      con       <- dbConnect(drv, group="CarboSense_MySQL")
      res       <- dbSendQuery(con, query_str)
      data      <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
    }
    if(sites[ith_site]=="BRM"){
      query_str <- paste("SELECT timestamp,",species,",",paste(species,"_F",sep="")," FROM ",table_names[ith_site]," WHERE MEAS_HEIGHT=12;",sep="")
      drv       <- dbDriver("MySQL")
      con       <- dbConnect(drv, group="CarboSense_MySQL")
      res       <- dbSendQuery(con, query_str)
      data      <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
    }
    if(sites[ith_site]=="LAEG"){
      query_str <- paste("SELECT timestamp,",species,",",paste(species,"_F",sep="")," FROM ",table_names[ith_site]," WHERE VALVEPOS=0;",sep="")
      drv       <- dbDriver("MySQL")
      con       <- dbConnect(drv, group="CarboSense_MySQL")
      res       <- dbSendQuery(con, query_str)
      data      <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
    }
    if(sites[ith_site]=="GIMM"){
      query_str <- paste("SELECT timestamp,",species,",",paste(species,"_F",sep="")," FROM ",table_names[ith_site]," WHERE VALVEPOS=0;",sep="")
      drv       <- dbDriver("MySQL")
      con       <- dbConnect(drv, group="CarboSense_MySQL")
      res       <- dbSendQuery(con, query_str)
      data      <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
    }
    
    data$date  <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
    data$year  <- as.numeric(strftime(data$date,"%Y",tz="UTC"))
    data$month <- as.numeric(strftime(data$date,"%m",tz="UTC"))
    data$day   <- as.numeric(strftime(data$date,"%d",tz="UTC"))
    data$hour  <- as.numeric(strftime(data$date,"%H",tz="UTC"))
    
    species_pos   <- which(colnames(data)==species)
    species_F_pos <- which(colnames(data)==paste(species,"_F",sep=""))
    
    # Hist diurnal CO2 concentrations
    
    descriptor <- paste("REF_",sites[ith_site],sep="")
    
    figname    <- paste(resultdir,descriptor,"_",species,".pdf",sep="")
    
    def_par <- par()
    pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1,1,0.75,1),mfrow=c(1,1))
    
    
    for(yy in unique(data$year)){
      for(mm in 1:12){
        
        id   <- which(data$year == yy & data$month == mm)
        n_id <- length(id)
        
        if(n_id == 0){
          next
        }
        
        tmp <- matrix(NA,ncol=24,nrow=n_id)
        
        for(hh in 0:23){
          
          id_id   <- which(data$hour[id]==hh & data[id,species_pos] != -999)
          n_id_id <- length(id_id)
          
          if(n_id_id>0){
            tmp[1:n_id_id,hh+1] <- data[id[id_id],species_pos]
          }
        }
        
        mainString <- paste(sprintf("%02.0f",mm),sprintf("%04.0f",yy),"/",sites[ith_site])
        xlabString <- expression(paste("Time of day [UTC]"))
        
        if(species=="NO"){
          ylabString <- expression(paste("NO [ppb]"))
          yrange     <- c(0,250)
          y_ll       <- seq(0,250,20)
        }
        if(species=="NO2"){
          ylabString <- expression(paste("NO"[2]*" [ppb]"))
          yrange     <- c(0,80)
          y_ll       <- seq(0,80,10)
        }
        if(species=="CO2"){
          ylabString <- expression(paste("CO"[2]*" [ppm]"))
          yrange     <- c(380,550)
          y_ll       <- seq(400,500,50)
        }
        if(species=="CO"){
          ylabString <- expression(paste("CO [ppm]"))
          yrange     <- c(0,2)
          y_ll       <- seq(0,2,0.5)
        }
        if(species=="O3"){
          ylabString <- expression(paste("O"[3]*" [ppm]"))
          yrange     <- c(0,100)
          y_ll       <- seq(0,100,20)
        }
        
        boxplot(tmp,xlim=c(0,25),ylim=yrange,xlab=xlabString,ylab=ylabString,main=mainString,col="slategray",cex=0.5,pch=16,cex.main=1.25,cex.lab=1.25,cex.axis=1.25,xaxt="n")
        for(y_ll_i in y_ll){
          lines(c(-1e9,1e9),c(y_ll_i,y_ll_i),lwd=1,lty=1,col="darkred")
        }
        par(new=T)
        plot(1:24,apply(!is.na(tmp),2,sum),pch=15,col=2,xaxt="n",yaxt="n",xlab="",ylab="",main="",xlim=c(0,25),ylim=c(0,1860))
        axis(side = 1,at = seq(0,24,4),labels = seq(0,24,4),cex.label=1.25,cex.axis=1.25)
        axis(4,cex.label=1.25,cex.axis=1.25)
        
        mtext(text = "Number of measurements",side = 4,line = 2,cex=1.25)
        
        ##
        
        if(species=="CO2"){
          dV_cc <- dV_cc + 1
          
          diurnalVariations[[dV_cc]] <- list(LocationName  = sites[ith_site],
                                             SensorUnit_ID ="REF",
                                             Height        = Height,
                                             X_LV03        = X_LV03,
                                             Y_LV03        = Y_LV03,
                                             LAT_WGS84     = LAT_WGS84,
                                             LON_WGS84     = LON_WGS84,
                                             Canton        = Canton,
                                             year          = yy,
                                             version       = 0,
                                             month         = mm,
                                             median        = apply(tmp,2,median,na.rm=T),
                                             N             = apply(!is.na(tmp),2,sum))
        }
      }
    }
    
    dev.off()
    par(def_par)
  }
}

# ----------------------------------------------------------------------------------------------------------------------

# HPP SITES


query_str <- paste("SELECT DISTINCT LocationName, SensorUnit_ID FROM CarboSense_HPP_CO2 WHERE LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1');",sep="")
drv       <- dbDriver("MySQL")
con       <- dbConnect(drv, group="CarboSense_MySQL")
res       <- dbSendQuery(con, query_str)
HPP_LOC_SUID <- fetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)



for(ith_LOC_SUID in 1:dim(HPP_LOC_SUID)[1]){
  
  #
  
  id_loc_hpp <- which(tbl_location$LocationName == HPP_LOC_SUID$LocationName[ith_LOC_SUID])
  
  Height    <- tbl_location$h[id_loc_hpp]
  X_LV03    <- tbl_location$X_LV03[id_loc_hpp]
  Y_LV03    <- tbl_location$Y_LV03[id_loc_hpp]
  LAT_WGS84 <- tbl_location$LAT_WGS84[id_loc_hpp]
  LON_WGS84 <- tbl_location$LON_WGS84[id_loc_hpp]
  Canton    <- tbl_location$Canton[id_loc_hpp]
  
  
  
  # import data
  
  query_str <- paste("SELECT timestamp,CO2_CAL_ADJ FROM CarboSense_HPP_CO2 WHERE SensorUnit_ID = ",HPP_LOC_SUID$SensorUnit_ID[ith_LOC_SUID]," and LocationName='",HPP_LOC_SUID$LocationName[ith_LOC_SUID],"' and CO2_CAL_ADJ != -999 and Valve=0;",sep="")
  drv       <- dbDriver("MySQL")
  con       <- dbConnect(drv, group="CarboSense_MySQL")
  res       <- dbSendQuery(con, query_str)
  data      <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  colnames(data)[which(colnames(data)=="CO2_CAL_ADJ")] <- "CO2"
  
  
  data$date  <-  strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
  data$year  <- as.numeric(strftime(data$date,"%Y",tz="UTC"))
  data$month <- as.numeric(strftime(data$date,"%m",tz="UTC"))
  data$day   <- as.numeric(strftime(data$date,"%d",tz="UTC"))
  data$hour  <- as.numeric(strftime(data$date,"%H",tz="UTC"))
  
  # Hist diurnal CO2 concentrations
  
  descriptor <- paste("HPP_",HPP_LOC_SUID$LocationName[ith_LOC_SUID],"_",HPP_LOC_SUID$SensorUnit_ID[ith_LOC_SUID],sep="")
  
  figname    <- paste(resultdir,descriptor,"_CO2.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.75,1),mfrow=c(1,1))
  
  
  for(yy in unique(data$year)){
    for(mm in 1:12){
      
      id   <- which(data$year == yy & data$month == mm)
      n_id <- length(id)
      
      if(n_id == 0){
        next
      }
      
      tmp <- matrix(NA,ncol=24,nrow=n_id)
      
      for(hh in 0:23){
        
        id_id   <- which(data$hour[id]==hh & data$CO2[id] != -999)
        n_id_id <- length(id_id)
        
        if(n_id_id>0){
          tmp[1:n_id_id,hh+1] <- data$CO2[id[id_id]]
        }
      }
      
      mainString <- paste(sprintf("%02.0f",mm),sprintf("%04.0f",yy),"/",HPP_LOC_SUID$LocationName[ith_LOC_SUID],"-",HPP_LOC_SUID$SensorUnit_ID[ith_LOC_SUID])
      xlabString <- expression(paste("Time of day [UTC]"))
      
      
      ylabString <- expression(paste("CO"[2]*" [ppm]"))
      yrange     <- c(380,550)
      y_ll       <- seq(400,500,50)
      
      boxplot(tmp,xlim=c(0,25),ylim=yrange,xlab=xlabString,ylab=ylabString,main=mainString,col="slategray",cex=0.5,pch=16,cex.main=1.25,cex.lab=1.25,cex.axis=1.25,xaxt="n")
      for(y_ll_i in y_ll){
        lines(c(-1e9,1e9),c(y_ll_i,y_ll_i),lwd=1,lty=1,col="darkred")
      }
      par(new=T)
      plot(1:24,apply(!is.na(tmp),2,sum),pch=15,col=2,xaxt="n",yaxt="n",xlab="",ylab="",main="",xlim=c(0,25),ylim=c(0,1860))
      axis(side = 1,at = seq(0,24,4),labels = seq(0,24,4),cex.label=1.25,cex.axis=1.25)
      axis(4,cex.label=1.25,cex.axis=1.25)
      
      mtext(text = "Number of measurements",side = 4,line = 2,cex=1.25)
      
      ##
      
      
      dV_cc <- dV_cc + 1
      
      diurnalVariations[[dV_cc]] <- list(LocationName  = HPP_LOC_SUID$LocationName[ith_LOC_SUID],
                                         SensorUnit_ID = HPP_LOC_SUID$SensorUnit_ID[ith_LOC_SUID],
                                         Height        = Height,
                                         X_LV03        = X_LV03,
                                         Y_LV03        = Y_LV03,
                                         LAT_WGS84     = LAT_WGS84,
                                         LON_WGS84     = LON_WGS84,
                                         Canton        = Canton,
                                         year          = yy,
                                         version       = 0,
                                         month         = mm,
                                         median        = apply(tmp,2,median,na.rm=T),
                                         N             = apply(!is.na(tmp),2,sum))
      
    }
  }
  
  dev.off()
  par(def_par)
}



# ----------------------------------------------------------------------------------------------------------------------

figname <- paste(resultdir,"test.pdf",sep="")
def_par <- par()
pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
par(mai=c(1,1,0.75,0.1),mfrow=c(1,1))


for(year in c(2017,2018,2019)){
  for(month in 1:12){
    
    plot(NA,NA,xlim=c(0,24),ylim=c(350,550),main=paste(sprintf("%02.0f",month)," / ",year,sep=""),xlab="Time of day",ylab=expression(paste("CO"[2]*" [ppm]",sep="")),cex.main=1.25,cex.axis=1.25,cex.lab=1.25)
    
    for(ith_dV in 1:length(diurnalVariations)){
      
      if(diurnalVariations[[ith_dV]]$year!=year
         | diurnalVariations[[ith_dV]]$month!=month
         | diurnalVariations[[ith_dV]]$version!=2
         | !all(diurnalVariations[[ith_dV]]$N>120)){
        next
      }
      
      ok <- F
      
      if(diurnalVariations[[ith_dV]]$LocationName%in%c("ZBLG","ZECB","ZGHD","ZFRK","ZHRG","ZHRO","ZHRO0","ZLMT","ZNEU","ZORL","ZPFW","ZPFW0","ZPRD","ZSCH","ZSEF","ZSTA","ZTBN","ZUE","ZVKN")){
        ok  <- T
        col <- 2
      }
      if(diurnalVariations[[ith_dV]]$LocationName%in%c("OPF","SMA","SMA1","SMHK","ZALL","ZAML","ZAZG","ZBRC","ZDLT","ZFBL","ZHBID","ZHBR","ZHRZ","ZHUR","ZHUR0","ZRDH","ZSBN","ZSTL","ZUBG","ZWCH")){
        ok  <- T
        if(diurnalVariations[[ith_dV]]$Height<550){
          col <- 1
        }else{
          col <- "green"
        }
      }
      if(diurnalVariations[[ith_dV]]$LocationName%in%c("AJGR","BSCR","BUDF","DUB","KBRL","KTGM","RCTZ","REH","SZGL","ULGW","WMOO","WRTW","WSUM","ZFHB","ZLDW","ZSZW")){
        ok  <- T
        if(diurnalVariations[[ith_dV]]$Height<550){
          col <- 1
        }else{
          col <- "green"
        }
      }
      if(diurnalVariations[[ith_dV]]$LocationName%in%c("ALBS","GUB","UEB","UTLI","ZHBG")){
        ok  <- F
        col <- 1
      }
      
      if(!ok){
        next
      }
      
      lines(seq(0.5,23.5,1),diurnalVariations[[ith_dV]]$median,col=col,lwd=1)
      
    }
    
    #
    
    for(ith_dV in 1:length(diurnalVariations)){
      
      if(diurnalVariations[[ith_dV]]$year!=year
         | diurnalVariations[[ith_dV]]$month!=month
         | diurnalVariations[[ith_dV]]$SensorUnit_ID!="REF"
         | diurnalVariations[[ith_dV]]$LocationName!="DUE"){
        next
      }
      
      col="blue"
      
      lines(seq(0.5,23.5,1),diurnalVariations[[ith_dV]]$median,col=col,lwd=4)
      
    }
    
    par(family="mono")
    legend("topright",legend=c("SU urban","SU rural < 550 a.s.l","SU rural > 550 a.s.l","Picarro DUE"),col=c(2,1,"green","blue"),lwd=c(2,2,2,3),lty=c(1,1,1,1),bg="white")
    par(family="")
  }
}

dev.off()
par(def_par)


# ----------------------------------------------------------------------------------------------------------------------


for(ith_cg in 1:9){
  
  if(ith_cg == 1){
    cg_name    <- "GR"
    cg_cantons <- c("GR")
    add_sites  <- c("AAA")
    rm_sites   <- c("GRO")
  }
  if(ith_cg == 2){
    cg_name    <- "TI"
    cg_cantons <- c("TI")
    add_sites  <- c("GRO")
    rm_sites   <- c("AAA")
  }
  if(ith_cg == 3){
    cg_name    <- "VS"
    cg_cantons <- c("VS")
    add_sites  <- c("AAA")
    rm_sites   <- c("AAA")
  }
  if(ith_cg == 4){
    cg_name    <- "EastCH"
    cg_cantons <- c("SG","TG","GLA","SH","AI","AR")
    add_sites  <- c("AAA")
    rm_sites   <- c("AAA")
  }
  if(ith_cg == 5){
    cg_name    <- "ZH"
    cg_cantons <- c("ZH")
    add_sites  <- c("AAA")
    rm_sites   <- c("AAA")
  }
  if(ith_cg == 6){
    cg_name    <- "InCH"
    cg_cantons <- c("ZG","LU","NW","OW","SZ","UR")
    add_sites  <- c("AAA")
    rm_sites   <- c("AAA")
  }
  if(ith_cg == 7){
    cg_name    <- "NCH"
    cg_cantons <- c("BS","BL","AG","JU")
    add_sites  <- c("AAA")
    rm_sites   <- c("AAA")
  }
  if(ith_cg == 8){
    cg_name    <- "MCH"
    cg_cantons <- c("BE","SO")
    add_sites  <- c("AAA")
    rm_sites   <- c("AAA")
  }
  if(ith_cg == 9){
    cg_name    <- "WCH"
    cg_cantons <- c("GE","VD","NE","FR")
    add_sites  <- c("AAA")
    rm_sites   <- c("AAA")
  }
  
  #
  
  cg_sites <- NULL
  
  for(ith_dV in 1:length(diurnalVariations)){
    
    if((diurnalVariations[[ith_dV]]$Canton%in%cg_cantons | diurnalVariations[[ith_dV]]$LocationName%in%add_sites)
       & !diurnalVariations[[ith_dV]]$LocationName%in%rm_sites){
      cg_sites <- rbind(cg_sites,data.frame(LocationName = diurnalVariations[[ith_dV]]$LocationName,
                                            Height       = diurnalVariations[[ith_dV]]$Height,
                                            LegStr       = paste(sprintf("%-5s",diurnalVariations[[ith_dV]]$LocationName)," ",round(diurnalVariations[[ith_dV]]$Height,0),"m",sep=""),
                                            col          = NA,
                                            stringsAsFactors = F))
    }
  }
  
  cg_sites     <- cg_sites[order(cg_sites$Height),]
  cg_sites     <- cg_sites[!duplicated(cg_sites$LocationName),]
  cg_sites$col <- rainbow(dim(cg_sites)[1])
  
  print(cg_sites)
  
  #
  
  figname <- paste(resultdir,"DiurnalVariations_LP8_Cantons_",cg_name,".pdf",sep="")
  def_par <- par()
  pdf(file = figname, width=12, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.75,0.1),mfrow=c(1,1))
  
  
  
  for(year in c(2017,2018)){
    for(month in 1:12){
      
      plot(NA,NA,xlim=c(0,27),ylim=c(350,550),main=paste(sprintf("%02.0f",month)," / ",year,sep=""),xlab="Time of day",ylab=expression(paste("CO"[2]*" [ppm]",sep="")),cex.main=1.25,cex.axis=1.25,cex.lab=1.25)
      
      for(ith_dV in 1:length(diurnalVariations)){
        
        if(diurnalVariations[[ith_dV]]$year!=year
           | diurnalVariations[[ith_dV]]$month!=month
           | diurnalVariations[[ith_dV]]$version!=2
           | !all(diurnalVariations[[ith_dV]]$N>120)
           | !diurnalVariations[[ith_dV]]$LocationName%in%cg_sites$LocationName){
          next
        }
        
        ok <- F
        
        lines(seq(0.5,23.5,1),diurnalVariations[[ith_dV]]$median,col=cg_sites$col[which(cg_sites$LocationName==diurnalVariations[[ith_dV]]$LocationName)],lwd=1)
        
        text(0,diurnalVariations[[ith_dV]]$median[1],diurnalVariations[[ith_dV]]$LocationName,cex=0.5)
      }
      
      #
      
      for(ith_dV in 1:length(diurnalVariations)){
        
        if(diurnalVariations[[ith_dV]]$year!=year
           | diurnalVariations[[ith_dV]]$month!=month
           | diurnalVariations[[ith_dV]]$SensorUnit_ID!="REF"
           | !diurnalVariations[[ith_dV]]$LocationName%in%c("DUE","RIG","BRM","PAY","LAEG","HAE")){
          next
        }
        
        lines(seq(0.5,23.5,1),diurnalVariations[[ith_dV]]$median,col=1,lwd=2,lty=5)
        
        text(24,diurnalVariations[[ith_dV]]$median[24],diurnalVariations[[ith_dV]]$LocationName,cex=0.5)
      }
      
      par(family="mono")
      legend("topright",legend=cg_sites$LegStr,col=cg_sites$col,lwd=1,lty=1,bg="white",cex=0.75)
      par(family="")
    }
  }
  
  dev.off()
  par(def_par)
}


##

diurnalVariation_MEDIAN_MAX_MIN <- NULL

for(ith_dV in 1:length(diurnalVariations)){
  
  
  diurnalVariation_MEDIAN_MAX_MIN <- rbind(diurnalVariation_MEDIAN_MAX_MIN,data.frame(LocationName  = diurnalVariations[[ith_dV]]$LocationName,
                                                                                      SensorUnit_ID = diurnalVariations[[ith_dV]]$SensorUnit_ID,
                                                                                      X_LV03        = diurnalVariations[[ith_dV]]$X_LV03,
                                                                                      Y_LV03        = diurnalVariations[[ith_dV]]$Y_LV03,
                                                                                      Height        = diurnalVariations[[ith_dV]]$Height,
                                                                                      LAT_WGS84     = diurnalVariations[[ith_dV]]$LAT_WGS84,
                                                                                      LON_WGS84     = diurnalVariations[[ith_dV]]$LON_WGS84,
                                                                                      Canton        = diurnalVariations[[ith_dV]]$Canton,
                                                                                      year          = diurnalVariations[[ith_dV]]$year,
                                                                                      month         = diurnalVariations[[ith_dV]]$month,
                                                                                      version       = diurnalVariations[[ith_dV]]$version,
                                                                                      N_min         = min(diurnalVariations[[ith_dV]]$N,na.rm=T),
                                                                                      N_max         = max(diurnalVariations[[ith_dV]]$N,na.rm=T),
                                                                                      median_min    = min(diurnalVariations[[ith_dV]]$median,na.rm=T),
                                                                                      median_max    = max(diurnalVariations[[ith_dV]]$median,na.rm=T),
                                                                                      stringsAsFactors = T))
  
}

#

write.table(diurnalVariation_MEDIAN_MAX_MIN,file = paste(resultdir,"diurnalVariation_MEDIAN_MAX_MIN.csv",sep=""),col.names=T,row.names=F,sep=";")

# 

# ----------------------------------------------------------------------------------------------------------------------

# REF SITES

sites         <- c("HAE","RIG","DUE","PAY","BRM","LAEG","GIMM")
n_sites       <- length(sites)
table_names   <- c(paste("NABEL_",sites[1:4],sep=""),"UNIBE_BRM","EMPA_LAEG","UNIBE_GIMM")


for(ith_site in 1:n_sites){
  
  #
  
  id_loc <- which(tbl_location$LocationName == sites[ith_site])
  
  #
  
  for(species in c("O3","NO","NO2","CO2","CO")){
    
    if(sites[ith_site]%in%c("BRM","LAEG","GIMM") & species!="CO2"){
      next
    }
    
    # import data
    
    if(sites[ith_site]%in%c("DUE","RIG","PAY")){
      if(species=="CO2"){
        
        if(sites[ith_site]%in%c("DUE","RIG")){
          query_str <- paste("SELECT timestamp,CO2_DRY_CAL,CO2_DRY_F,H2O,H2O_F FROM ",table_names[ith_site],";",sep="")
        }
        if(sites[ith_site]=="PAY"){
          query_str <- paste("SELECT timestamp,CO2_DRY_CAL,CO2_DRY_F,H2O_CAL,H2O_F FROM ",table_names[ith_site],";",sep="")
        }
        drv       <- dbDriver("MySQL")
        con       <- dbConnect(drv, group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        data_tmp  <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        if(sites[ith_site]%in%c("DUE","RIG")){
          colnames(data_tmp)[which(colnames(data_tmp)=="CO2_DRY_CAL")] <- "CO2_DRY"
        }
        if(sites[ith_site]=="PAY"){
          colnames(data_tmp)[which(colnames(data_tmp)=="CO2_DRY_CAL")] <- "CO2_DRY"
          colnames(data_tmp)[which(colnames(data_tmp)=="H2O_CAL")]     <- "H2O"
        }
        
        data_tmp$CO2   <- data_tmp$CO2_DRY * (1 - data_tmp$H2O/100)
        data_tmp$CO2_F <- 1
        
        data           <- data_tmp[,c(which(colnames(data_tmp)=="timestamp"),which(colnames(data_tmp)=="CO2"),which(colnames(data_tmp)=="CO2_F"))]
        
        id_setToNA     <- which(data_tmp$CO2_DRY     == -999
                                | data_tmp$H2O       == -999
                                | data_tmp$CO2_DRY_F == 0
                                | data_tmp$H2O_F     == 0)
        
        if(length(id_setToNA)>0){
          data$CO2[id_setToNA]   <- -999
          data$CO2_F[id_setToNA] <- 0
        }
        
        rm(data_tmp)
        gc()
        
      }else{
        query_str <- paste("SELECT timestamp,",species,",",paste(species,"_F",sep="")," FROM ",table_names[ith_site],";",sep="")
        drv       <- dbDriver("MySQL")
        con       <- dbConnect(drv, group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        data      <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
      }
    }
    if(sites[ith_site]=="HAE"){
      query_str <- paste("SELECT timestamp,",species,",",paste(species,"_F",sep="")," FROM ",table_names[ith_site],";",sep="")
      drv       <- dbDriver("MySQL")
      con       <- dbConnect(drv, group="CarboSense_MySQL")
      res       <- dbSendQuery(con, query_str)
      data      <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
    }
    if(sites[ith_site]=="BRM"){
      query_str <- paste("SELECT timestamp,",species,",",paste(species,"_F",sep="")," FROM ",table_names[ith_site]," WHERE MEAS_HEIGHT=12;",sep="")
      drv       <- dbDriver("MySQL")
      con       <- dbConnect(drv, group="CarboSense_MySQL")
      res       <- dbSendQuery(con, query_str)
      data      <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
    }
    if(sites[ith_site]=="LAEG"){
      query_str <- paste("SELECT timestamp,",species,",",paste(species,"_F",sep="")," FROM ",table_names[ith_site]," WHERE VALVEPOS=0;",sep="")
      drv       <- dbDriver("MySQL")
      con       <- dbConnect(drv, group="CarboSense_MySQL")
      res       <- dbSendQuery(con, query_str)
      data      <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
    }
    if(sites[ith_site]=="GIMM"){
      query_str <- paste("SELECT timestamp,",species,",",paste(species,"_F",sep="")," FROM ",table_names[ith_site]," WHERE VALVEPOS=5;",sep="")
      drv       <- dbDriver("MySQL")
      con       <- dbConnect(drv, group="CarboSense_MySQL")
      res       <- dbSendQuery(con, query_str)
      data      <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
    }
    
    data$date  <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
    data$year  <- as.numeric(strftime(data$date,"%Y",tz="UTC"))
    data$month <- as.numeric(strftime(data$date,"%m",tz="UTC"))
    data$day   <- as.numeric(strftime(data$date,"%d",tz="UTC"))
    data$hour  <- as.numeric(strftime(data$date,"%H",tz="UTC"))
    
    species_pos   <- which(colnames(data)==species)
    species_F_pos <- which(colnames(data)==paste(species,"_F",sep=""))
    
    # Hist diurnal CO2 concentrations
    
    descriptor <- paste("REF_",sites[ith_site],sep="")
    
    figname    <- paste(resultdir,descriptor,"_",species,"_YEAR",".pdf",sep="")
    
    def_par <- par()
    pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1,1,0.75,1),mfrow=c(1,1))
    
    
    for(yy in c(2018,2017,2019)){
      
      id   <- which(data$year == yy)
      n_id <- length(id)
      
      if(n_id == 0){
        next
      }
      
      tmp <- matrix(NA,ncol=24,nrow=n_id)
      
      for(hh in 0:23){
        
        id_id   <- which(data$hour[id]==hh & data[id,species_pos] != -999)
        n_id_id <- length(id_id)
        
        if(n_id_id>0){
          tmp[1:n_id_id,hh+1] <- data[id[id_id],species_pos]
        }
      }
      
      mainString <- paste(sites[ith_site]," / ",sprintf("%04.0f",yy)," / ",sprintf("%.0f",tbl_location$h[id_loc]),"m a.s.l.",sep="")
      xlabString <- expression(paste("Time of day [UTC]"))
      
      if(species=="NO"){
        ylabString <- expression(paste("NO [ppb]"))
        yrange     <- c(0,250)
        y_ll       <- seq(0,250,20)
      }
      if(species=="NO2"){
        ylabString <- expression(paste("NO"[2]*" [ppb]"))
        yrange     <- c(0,80)
        y_ll       <- seq(0,80,10)
      }
      if(species=="CO2"){
        ylabString <- expression(paste("CO"[2]*" [ppm]"))
        yrange     <- c(380,550)
        y_ll       <- seq(400,500,50)
      }
      if(species=="CO"){
        ylabString <- expression(paste("CO [ppm]"))
        yrange     <- c(0,2)
        y_ll       <- seq(0,2,0.5)
      }
      if(species=="O3"){
        ylabString <- expression(paste("O"[3]*" [ppm]"))
        yrange     <- c(0,100)
        y_ll       <- seq(0,100,20)
      }
      
      boxplot(tmp,xlim=c(0,25),ylim=yrange,xlab=xlabString,ylab=ylabString,main=mainString,col="slategray",cex=0.5,pch=16,cex.main=1.5,cex.lab=1.5,cex.axis=1.5,xaxt="n")
      for(y_ll_i in y_ll){
        lines(c(-1e9,1e9),c(y_ll_i,y_ll_i),lwd=1,lty=1,col="darkred")
      }
      par(new=T)
      plot(1:24,apply(!is.na(tmp),2,sum),pch=15,col=2,xaxt="n",yaxt="n",xlab="",ylab="",main="",xlim=c(0,25),ylim=c(0,21900))
      axis(side = 1,at = seq(0,24,4),labels = seq(0,24,4),cex.label=1.5,cex.axis=1.5)
      axis(4,cex.label=1.25,cex.axis=1.5)
      
      mtext(text = "Number of measurements",side = 4,line = 2,cex=1.5)
      
    }
    
    dev.off()
    par(def_par)
  }
}

