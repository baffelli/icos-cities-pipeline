# Traffic_Corona.r
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

library(openair)

## clear variables
rm(list=ls(all=TRUE))
gc()

# ------------------------------------------------------------------------------------------------------------

resdir        <- "/project/CarboSense/Statistical_Models/Covid-19_ZH/Results/"
datadir       <- "/project/CarboSense/Statistical_Models/Covid-19_ZH/Data/"
plotdir_BP    <- "/project/CarboSense/Statistical_Models/Covid-19_ZH/Results/Zaehlstellen_Boxplots/"
plotdir_TS    <- "/project/CarboSense/Statistical_Models/Covid-19_ZH/Results/Zaehlstellen_Zeitreihen/"
plotdir_AQ_TS <- "/project/CarboSense/Statistical_Models/Covid-19_ZH/Results/AQ_Zeitreihen/"
plotdir_AQ_BP <- "/project/CarboSense/Statistical_Models/Covid-19_ZH/Results/AQ_Boxplots/"


if(!dir.exists(resdir)){
  dir.create(resdir)
}
if(!dir.exists(datadir)){
  dir.create(datadir)
  dir.create(paste(datadir,"Traffic/",sep=""))
}
if(!dir.exists(plotdir_BP)){
  dir.create(plotdir_BP)
}
if(!dir.exists(plotdir_TS)){
  dir.create(plotdir_TS)
  dir.create(paste(plotdir_TS,"Stundenwerte_Richtungsgetrennt/",sep=""))
  dir.create(paste(plotdir_TS,"Stundenwerte_Summe/",sep=""))
  dir.create(paste(plotdir_TS,"Tageswerte_Richtungsgetrennt/",sep=""))
  dir.create(paste(plotdir_TS,"Tageswerte_Summe/",sep=""))
}
if(!dir.exists(plotdir_AQ_TS)){
  dir.create(plotdir_AQ_TS)
  dir.create(paste(plotdir_AQ_TS,"Stundenwerte/",sep=""))
  dir.create(paste(plotdir_AQ_TS,"Tageswerte/",sep=""))
}
if(!dir.exists(plotdir_AQ_BP)){
  dir.create(plotdir_AQ_BP)
}

# ------------------------------------------------------------------------------------------------------------

fn_traffic_2018 <- paste(datadir,"Traffic/sid_dav_verkehrszaehlung_miv_od2031_2018.csv",sep="")
fn_traffic_2019 <- paste(datadir,"Traffic/sid_dav_verkehrszaehlung_miv_od2031_2019.csv",sep="")
fn_traffic_2020 <- paste(datadir,"Traffic/sid_dav_verkehrszaehlung_miv_od2031_2020.csv",sep="")

if(T){
  download.file("https://data.stadt-zuerich.ch/dataset/6212fd20-e816-4828-a67f-90f057f25ddb/resource/d5963dee-7841-4e64-9268-6c850a2fc497/download/sid_dav_verkehrszaehlung_miv_od2031_2018.csv",
                fn_traffic_2018, method = "wget", quiet = FALSE, mode = "w",
                cacheOK = TRUE,
                extra = getOption("download.file.extra"),
                headers = NULL)
}
if(T){
  download.file("https://data.stadt-zuerich.ch/dataset/6212fd20-e816-4828-a67f-90f057f25ddb/resource/fa64fa70-6328-4d47-bcf0-1eff694d7c22/download/sid_dav_verkehrszaehlung_miv_od2031_2019.csv",
                fn_traffic_2019, method = "wget", quiet = FALSE, mode = "w",
                cacheOK = TRUE,
                extra = getOption("download.file.extra"),
                headers = NULL)
}
if(T){
  download.file("https://data.stadt-zuerich.ch/dataset/6212fd20-e816-4828-a67f-90f057f25ddb/resource/44607195-a2ad-4f9b-b6f1-d26c003d85a2/download/sid_dav_verkehrszaehlung_miv_od2031_2020.csv",
                fn_traffic_2020, method = "wget", quiet = FALSE, mode = "w",
                cacheOK = TRUE,
                extra = getOption("download.file.extra"),
                headers = NULL)
}


# ------------------------------------------------------------------------------------------------------------

traffic_data <- NULL
#traffic_data <- rbind(traffic_data, read.table(file = fn_traffic_2018,header = T,sep = ",",as.is = T))
#traffic_data <- rbind(traffic_data, read.table(file = fn_traffic_2019,header = T,sep = ",",as.is = T))
traffic_data <- rbind(traffic_data, read.table(file = fn_traffic_2020,header = T,sep = ",",as.is = T))

traffic_data$AnzFahrzeugeStatus_TF <- traffic_data$AnzFahrzeugeStatus== "Gemessen"

traffic_data$date      <- strptime(traffic_data$MessungDatZeit,"%Y-%m-%dT%H:%M:%S",tz="UTC")
traffic_data$year      <- as.numeric(strftime(traffic_data$date,"%Y",tz="UTC"))
traffic_data$month     <- as.numeric(strftime(traffic_data$date,"%m",tz="UTC"))
traffic_data$day       <- as.numeric(strftime(traffic_data$date,"%d",tz="UTC"))
traffic_data$hour      <- as.numeric(strftime(traffic_data$date,"%H",tz="UTC"))
traffic_data$dow       <- as.numeric(strftime(traffic_data$date,"%w",tz="UTC"))
traffic_data$timestamp <- as.numeric(difftime(time1=traffic_data$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units ="secs",tz="UTC"))



for(ph in c("2020-01-01","2020-01-02","2020-04-10","2020-04-13","2020-05-01","2020-05-21","2020-06-01")){
  id <- which(strftime(traffic_data$date,"%Y-%m-%d",tz="UTC") == ph)
  if(length(id)>0){
    traffic_data$dow[id] <- 0
  }
}


# # Keine DST?
# if(T){
#   u_timestamp <- sort(unique(traffic_data$timestamp))
#   print(summary(diff(u_timestamp)))
#   
#   id <- which(traffic_data$date>=strptime("20200329000000","%Y%m%d%H%M%S",tz="UTC")
#               & traffic_data$date<=strptime("20200329050000","%Y%m%d%H%M%S",tz="UTC"))
#   
#   print(data.frame(date=traffic_data$date[id],
#                    AnzFahrzeuge = traffic_data$AnzFahrzeuge[id],
#                    stringsAsFactors = F))
# }
# stop()

# ------------------------------------------------------------------------------------------------------------

refPer_Date_from             <- strptime("20200201000000","%Y%m%d%H%M%S",tz="UTC")
refPer_Date_to               <- strptime("20200316235900","%Y%m%d%H%M%S",tz="UTC")

refPer_timestamp_from        <- as.numeric(difftime(time1=refPer_Date_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units ="secs",tz="UTC"))
refPer_timestamp_to          <- as.numeric(difftime(time1=refPer_Date_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units ="secs",tz="UTC"))

actionPeriod_Date_from       <- strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC")
actionPeriod_Date_to         <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")

actionPeriod_timestamp_from  <- as.numeric(difftime(time1=actionPeriod_Date_from,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units ="secs",tz="UTC"))
actionPeriod_timestamp_to    <- as.numeric(difftime(time1=actionPeriod_Date_to,  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units ="secs",tz="UTC"))

nDays_actionPeriod           <- ceiling((actionPeriod_timestamp_to-actionPeriod_timestamp_from)/86400)

# ------------------------------------------------------------------------------------------------------------

# Verkehr Boxplots

if(T){
  
  u_ZSName   <- sort(unique(traffic_data$ZSName))
  n_u_ZSName <- length(u_ZSName)
  
  for(ith_u_ZSName in 1:n_u_ZSName){
    
    u_ZSName_print <- u_ZSName[ith_u_ZSName]
    u_ZSName_print <-gsub(pattern = "-/",replacement = "_",u_ZSName_print)
    
    for(cat in c("ALL","Mo-Fr","Sat","Sun")){
      
      if(cat=="ALL"){
        dow <- 0:6
      }
      if(cat=="Mo-Fr"){
        dow <- 1:5
      }
      if(cat=="Sat"){
        dow <- 6
      }
      if(cat=="Sun"){
        dow <- 0
      }
      
      id_ref <- which(traffic_data$ZSName      == u_ZSName[ith_u_ZSName]
                      & traffic_data$timestamp >= refPer_timestamp_from
                      & traffic_data$timestamp <= refPer_timestamp_to
                      & traffic_data$dow      %in% dow
                      & traffic_data$AnzFahrzeugeStatus_TF == T)
      
      id_act <- which(traffic_data$ZSName      == u_ZSName[ith_u_ZSName]
                      & traffic_data$timestamp >= actionPeriod_timestamp_from
                      & traffic_data$timestamp <= actionPeriod_timestamp_to
                      & traffic_data$dow      %in% dow
                      & traffic_data$AnzFahrzeugeStatus_TF == T)
      
      id_ref_data_period_str <- paste(strftime(min(traffic_data$date[id_ref]),"%d %b %y",tz="UTC"),"-",strftime(max(traffic_data$date[id_ref]),"%d %b %y",tz="UTC"))
      id_act_data_period_str <- paste(strftime(min(traffic_data$date[id_act]),"%d %b %y",tz="UTC"),"-",strftime(max(traffic_data$date[id_act]),"%d %b %y",tz="UTC"))
      
      Richtung <- sort(unique(traffic_data$Richtung[id_ref]))
      
      if(length(Richtung)!=2){
        next
      }
      
      id_ref_R1 <- id_ref[traffic_data$Richtung[id_ref]==Richtung[1]]
      id_ref_R2 <- id_ref[traffic_data$Richtung[id_ref]==Richtung[2]]
      id_act_R1 <- id_act[traffic_data$Richtung[id_act]==Richtung[1]]
      id_act_R2 <- id_act[traffic_data$Richtung[id_act]==Richtung[2]]
      
      tmp_ref_R1 <- matrix(NA,ncol=24,nrow=length(id_ref_R1))
      tmp_ref_R2 <- matrix(NA,ncol=24,nrow=length(id_ref_R2))
      tmp_act_R1 <- matrix(NA,ncol=24,nrow=length(id_act_R1))
      tmp_act_R2 <- matrix(NA,ncol=24,nrow=length(id_act_R2))
      
      for(ii in 0:23){
        id_ref_R1_hour   <- id_ref_R1[which(traffic_data$hour[id_ref_R1]==ii)]
        id_ref_R2_hour   <- id_ref_R2[which(traffic_data$hour[id_ref_R2]==ii)]
        id_act_R1_hour   <- id_act_R1[which(traffic_data$hour[id_act_R1]==ii)]
        id_act_R2_hour   <- id_act_R2[which(traffic_data$hour[id_act_R2]==ii)]
        
        n_id_ref_R1_hour <- length(id_ref_R1_hour)
        n_id_ref_R2_hour <- length(id_ref_R2_hour)
        n_id_act_R1_hour <- length(id_act_R1_hour)
        n_id_act_R2_hour <- length(id_act_R2_hour)
        
        if(n_id_ref_R1_hour>0){
          tmp_ref_R1[1:n_id_ref_R1_hour,ii+1] <- traffic_data$AnzFahrzeuge[id_ref_R1_hour]
        }
        
        if(n_id_ref_R2_hour>0){
          tmp_ref_R2[1:n_id_ref_R2_hour,ii+1] <- traffic_data$AnzFahrzeuge[id_ref_R2_hour]
        }
        
        if(n_id_act_R1_hour>0){
          tmp_act_R1[1:n_id_act_R1_hour,ii+1] <- traffic_data$AnzFahrzeuge[id_act_R1_hour]
        }
        
        if(n_id_act_R2_hour>0){
          tmp_act_R2[1:n_id_act_R2_hour,ii+1] <- traffic_data$AnzFahrzeuge[id_act_R2_hour]
        }
      }
      
      max_traffic <- max(c(traffic_data$AnzFahrzeuge[id_ref],traffic_data$AnzFahrzeuge[id_act]))
      
      
      ##
      
      fn <- paste(plotdir_BP,u_ZSName_print,"_BP_",cat,".pdf",sep="")
      
      def_par <- par()
      pdf(file = fn, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1,1,0.5,0.1),mfrow=c(1,2))
      
      # ref R1 / act 1
      
      mainStr <- paste(Richtung[1]," [",cat,"]",sep="")
      xlabStr <- "Hour of day [LT]"
      
      boxplot(tmp_ref_R1,at=0:23,names=0:23,main=mainStr,ylim=c(0,max_traffic),xlab=xlabStr, col="orange",pch=16,cex=0.5,xaxt="n",cex.lab=1.5,cex.axis=1.5,cex.main=1.5)
      axis(side = 1,at = seq(0,23,6),labels = seq(0,23,6),cex.lab=1.5,cex.axis=1.5)
      
      if(any(!is.na(tmp_act_R1))){
        mainStr <- ""
        xlabStr <- ""
        
        boxplot(tmp_act_R1,at=0:23,names=0:23,main=mainStr,ylim=c(0,max_traffic),xlab=xlabStr, col="green",pch=16,cex=0.5,xaxt="n",cex.lab=1.5,cex.axis=1.5,cex.main=1.5,add=T)
        axis(side = 1,at = seq(0,23,6),labels = seq(0,23,6),cex.lab=1.5,cex.axis=1.5)
      }
      
      par(family="mono")
      legend("topleft",legend=c(id_ref_data_period_str,id_act_data_period_str),pch=15,col=c("orange","green"),bg="white",cex=1.5)
      par(family="")
      
      # ref R2 / act R2
      
      mainStr <- paste(Richtung[2]," [",cat,"]",sep="")
      xlabStr <- "Hour of day [LT]"
      
      boxplot(tmp_ref_R2,at=0:23,names=0:23,main=mainStr,ylim=c(0,max_traffic),xlab=xlabStr,col="orange",pch=16,cex=0.5,xaxt="n",cex.lab=1.5,cex.axis=1.5,cex.main=1.5)
      axis(side = 1,at = seq(0,23,6),labels = seq(0,23,6),cex.lab=1.5,cex.axis=1.5)
      
      if(any(!is.na(tmp_act_R2))){
        mainStr <- ""
        xlabStr <- ""
        
        boxplot(tmp_act_R2,at=0:23,names=0:23,main=mainStr,ylim=c(0,max_traffic),xlab=xlabStr,col="green",pch=16,cex=0.5,xaxt="n",cex.lab=1.5,cex.axis=1.5,cex.main=1.5,add=T)
        axis(side = 1,at = seq(0,23,6),labels = seq(0,23,6),cex.lab=1.5,cex.axis=1.5)
      }
      
      par(family="mono")
      legend("topleft",legend=c(id_ref_data_period_str,id_act_data_period_str),pch=15,col=c("orange","green"),bg="white",cex=1.5)
      par(family="")
      
      dev.off()
      par(def_par)
      
      
    }
  }
  
}

# ------------------------------------------------------------------------------------------------------------

# Verkehr Zeitreihen

if(T){
  
  u_ZSName   <- sort(unique(traffic_data$ZSName))
  n_u_ZSName <- length(u_ZSName)
  
  for(ith_u_ZSName in 1:n_u_ZSName){
    
    u_ZSName_print <- u_ZSName[ith_u_ZSName]
    u_ZSName_print <-gsub(pattern = "-/",replacement = "_",u_ZSName_print)
    
    
    id_ref <- which(traffic_data$ZSName == u_ZSName[ith_u_ZSName]
                    & traffic_data$AnzFahrzeugeStatus_TF == T)
    
    Richtung <- sort(unique(traffic_data$Richtung[id_ref]))
    
    if(length(Richtung)!=2){
      next
    }
    
    id_ref_r1 <- id_ref[traffic_data$Richtung[id_ref] == Richtung[1]]
    id_ref_r2 <- id_ref[traffic_data$Richtung[id_ref] == Richtung[2]]
    
    tmp_traffic_data <- merge(data.frame(date=traffic_data$date[id_ref_r1],
                                         AnzFahrzeuge_r1 = traffic_data$AnzFahrzeuge[id_ref_r1],
                                         stringsAsFactors = F),
                              data.frame(date=traffic_data$date[id_ref_r2],
                                         AnzFahrzeuge_r2 = traffic_data$AnzFahrzeuge[id_ref_r2],
                                         stringsAsFactors = F),all=T,by="date")
    
    tmp_traffic_data$AnzFahrzeuge <-  tmp_traffic_data$AnzFahrzeuge_r1 + tmp_traffic_data$AnzFahrzeuge_r2
    
    #
    
    tmp_traffic_data_day      <- timeAverage(mydata = tmp_traffic_data,avg.time = "day",
                                             data.thresh = 100,statistic = "sum",
                                             start.date = strptime("20191231000000","%Y%m%d%H%M%S",tz="UTC"))
    
    tmp_traffic_data_day      <- as.data.frame(tmp_traffic_data_day)
    
    tmp_traffic_data_day$date <- as.POSIXct(tmp_traffic_data_day$date)
    
    #
    
    for(ii in 1:6){
      
      if(ii==1){
        fn      <- paste(plotdir_TS,"Tageswerte_Richtungsgetrennt/",u_ZSName_print,"_",Richtung[1],"_TS.pdf",sep="")
        id      <- which(!is.na(tmp_traffic_data_day$AnzFahrzeuge_r1) & !is.infinite(tmp_traffic_data_day$AnzFahrzeuge_r1))
        pos     <- which(colnames(tmp_traffic_data_day)=="AnzFahrzeuge_r1")
        xx      <- tmp_traffic_data_day$date[id]+43200
        yy      <- tmp_traffic_data_day[id,pos]
        mainStr <- paste(u_ZSName_print," ",Richtung[1],sep="")
      }
      if(ii==2){
        fn      <- paste(plotdir_TS,"Tageswerte_Richtungsgetrennt/",u_ZSName_print,"_",Richtung[2],"_TS.pdf",sep="")
        id      <- which(!is.na(tmp_traffic_data_day$AnzFahrzeuge_r2) & !is.infinite(tmp_traffic_data_day$AnzFahrzeuge_r2))
        pos     <- which(colnames(tmp_traffic_data_day)=="AnzFahrzeuge_r2")
        xx      <- tmp_traffic_data_day$date[id]+43200
        yy      <- tmp_traffic_data_day[id,pos]
        mainStr <- paste(u_ZSName_print," ",Richtung[2],sep="")
      }
      if(ii==3){
        fn      <- paste(plotdir_TS,"Tageswerte_Summe/",u_ZSName_print,"_TS.pdf",sep="")
        id      <- which(!is.na(tmp_traffic_data_day$AnzFahrzeuge) & !is.infinite(tmp_traffic_data_day$AnzFahrzeuge))
        pos     <- which(colnames(tmp_traffic_data_day)=="AnzFahrzeuge")
        xx      <- tmp_traffic_data_day$date[id]+43200
        yy      <- tmp_traffic_data_day[id,pos]
        mainStr <- paste(u_ZSName_print,sep="")
      }
      if(ii==4){
        fn      <- paste(plotdir_TS,"Stundenwerte_Richtungsgetrennt/",u_ZSName_print,"_",Richtung[1],"_TS.pdf",sep="")
        id      <- which(!is.na(tmp_traffic_data$AnzFahrzeuge_r1) & !is.infinite(tmp_traffic_data$AnzFahrzeuge_r1))
        pos     <- which(colnames(tmp_traffic_data)=="AnzFahrzeuge_r1")
        xx      <- tmp_traffic_data$date[id]+1800
        yy      <- tmp_traffic_data[id,pos]
        mainStr <- paste(u_ZSName_print," ",Richtung[1],sep="")
      }
      if(ii==5){
        fn      <- paste(plotdir_TS,"Stundenwerte_Richtungsgetrennt/",u_ZSName_print,"_",Richtung[2],"_TS.pdf",sep="")
        id      <- which(!is.na(tmp_traffic_data$AnzFahrzeuge_r2) & !is.infinite(tmp_traffic_data$AnzFahrzeuge_r2))
        pos     <- which(colnames(tmp_traffic_data)=="AnzFahrzeuge_r2")
        xx      <- tmp_traffic_data$date[id]+1800
        yy      <- tmp_traffic_data[id,pos]
        mainStr <- paste(u_ZSName_print," ",Richtung[2],sep="")
      }
      if(ii==6){
        fn      <- paste(plotdir_TS,"Stundenwerte_Summe/",u_ZSName_print,"_TS.pdf",sep="")
        id      <- which(!is.na(tmp_traffic_data$AnzFahrzeuge) & !is.infinite(tmp_traffic_data$AnzFahrzeuge))
        pos     <- which(colnames(tmp_traffic_data)=="AnzFahrzeuge")
        xx      <- tmp_traffic_data$date[id]+1800
        yy      <- tmp_traffic_data[id,pos]
        mainStr <- paste(u_ZSName_print,sep="")
      }
      
      if(length(id)==0){
        next
      }
      
      #
      
      def_par <- par()
      pdf(file = fn, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1,1,0.5,0.1),mfrow=c(1,1))
      
      y_max         <- max(yy)
      
      x_min         <- strptime("20200101000000","%Y%m%d%H%M%S",tz="UTC")
      x_max         <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
      
      x_min_act     <- strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC")
      x_max_act     <- strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC")
      
      mondays       <- seq(strptime("20191230000000","%Y%m%d%H%M%S",tz="UTC"),
                           strptime("20201230000000","%Y%m%d%H%M%S",tz="UTC"),by="weeks")
      
      xlabTicks     <- seq(strptime("20200101000000","%Y%m%d%H%M%S",tz="UTC"),
                           strptime("20210101000000","%Y%m%d%H%M%S",tz="UTC"),by="2 weeks")
      
      xlabTicks_Lab <- strftime(xlabTicks,"%d/%m/%Y",tz="UTC")
      
      
      plot(c(x_min,x_max),c(NA,NA),ylim=c(0,1.05*y_max),xlab="Date [LT]",ylab="Number of vehicles per hour",
           cex.lab=1.5,cex.axis=1.5, cex.main=1.5, xaxt="n",main=mainStr)
      
      axis(side = 1,at = xlabTicks,labels = xlabTicks_Lab,cex.lab=1.5,cex.axis=1.5)
      
      polygon(c(x_min_act,x_max_act,x_max_act,x_min_act),c(-1e3,-1e3,1e6,1e6),col="gray80",border=NA)
      
      for(ith_mon in 1:length(mondays)){
        lines(c(mondays[ith_mon],mondays[ith_mon]),c(-1e6,1e6),col="gray70",lwd=1)
      }
      
      lines( xx,yy,col=1,lwd=1)
      
      if(ii %in% c(1:3)){
        points(xx,yy,col=1,pch=16,cex=1)
      }
      
      dev.off()
      par(def_par)
    }
  }
}

# ------------------------------------------------------------------------------------------------------------

# Normaler Verkehr (Referenz)

if(T){
  
  traffic_ref <- NULL
  
  u_ZSName    <- sort(unique(traffic_data$ZSName))
  n_u_ZSName  <- length(u_ZSName)
  
  for(ith_u_ZSName in 1:n_u_ZSName){
    
    for(dow in 0:6){
      
      
      id_ref <- which(traffic_data$ZSName      == u_ZSName[ith_u_ZSName]
                      & traffic_data$timestamp >= refPer_timestamp_from
                      & traffic_data$timestamp <= refPer_timestamp_to
                      & traffic_data$dow       %in% dow
                      & traffic_data$AnzFahrzeugeStatus_TF == T)
      
      Richtung <- sort(unique(traffic_data$Richtung[id_ref]))
      EKoord   <- traffic_data$EKoord[id_ref[1]]
      NKoord   <- traffic_data$NKoord[id_ref[1]]
      
      if(length(Richtung)!=2){
        next
      }
      
      for(ith_ri in 1:2){
        
        id_ref_R   <- id_ref[traffic_data$Richtung[id_ref]==Richtung[ith_ri]]
        
        if(length(id_ref_R)==0){
          next
        }
        
        DTV_ref_R  <- rep(NA,24)
        n_ref_R    <- rep(NA,24)
        
        for(hh in 0:23){
          id_hh_r    <- id_ref_R[which(traffic_data$hour[id_ref_R] == hh)]
          n_id_hh_r  <- length(id_hh_r)
          
          if(n_id_hh_r > 0){
            DTV_ref_R[hh+1] <- sum(traffic_data$AnzFahrzeuge[id_hh_r])/n_id_hh_r
            n_ref_R[hh+1]   <- n_id_hh_r
          }else{
            DTV_ref_R[hh+1] <- NA
            n_ref_R[hh+1]   <- 0
          }
        }
        
        tmp           <- as.data.frame(matrix(NA,nrow=1,ncol=1+2+1+24+24+2),stringsAsFactors = F)
        colnames(tmp) <- c("ZSName","Richtung","Ri","DOW",paste("hh_",0:23,sep=""),paste("nn_",0:23,sep=""),"EKoord","NKoord")
        
        tmp[1] <- u_ZSName[ith_u_ZSName]
        tmp[2] <- Richtung[ith_ri]
        tmp[3] <- ith_ri
        tmp[4] <- dow
        
        for(ii in (1:24)){
          tmp[ii+4]    <- DTV_ref_R[ii]
          tmp[ii+4+24] <- n_ref_R[ii]
        }
        
        tmp$EKoord <- EKoord
        tmp$NKoord <- NKoord
        
        traffic_ref <- rbind(traffic_ref, tmp)
      }
    }
  }
  
  write.table(x = traffic_ref,file = paste(resdir,"referencePeriodTraffic.csv",sep=""),sep=";",col.names = T,row.names = F)
}

# ------------------------------------------------------------------------------------------------------------

# Schadstoffe in ZH

if(T){
  
  # Data import
  
  AQM_sites       <- c("RGS","STA","HEU","SCH","ZUE")
  AQM_sites_desc  <- c("Rosengartenstrasse [UGZ]",
                       "Stampfenbachstrasse (UGZ)",
                       "Heubeeribuehel (UGZ)",
                       "Schimmelstrasse (UGZ)",
                       "Kaserne (NABEL)")
  n_AQM_sites     <- length(AQM_sites)
  
  AQM_data        <- NULL
  
  for(ith_AQM_site in 1:n_AQM_sites){
    
    AQM_data_tmp <- NULL
    
    if(AQM_sites[ith_AQM_site] %in% c("RGS","STA","HEU","SCH")){
      
      files   <- NULL
      files   <- c(files,list.files(path = "/newhome/muem/mnt/Win_K2/Data/UGZ/",pattern = paste(AQM_sites[ith_AQM_site],"_1min_2019",sep=""),full.names = T))
      files   <- c(files,list.files(path = "/newhome/muem/mnt/Win_K2/Data/UGZ/",pattern = paste(AQM_sites[ith_AQM_site],"_1min_2020",sep=""),full.names = T))
      n_files <- length(files)
      
      for(ith_files in 1:n_files){
        tmp    <- read.table(files[ith_files],header = F,skip = 4,sep = ";",as.is=T)
        n_cols <- dim(tmp)[2]
        
        if(AQM_sites[ith_AQM_site]%in%c("RGS","STA","SCH")){
          tmp <- tmp[,1:11]
        }
        if(AQM_sites[ith_AQM_site]%in%c("HEU")){
          tmp <- tmp[,1:7]
        }
        
        for(ith_col in seq(2,dim(tmp)[2],2)){
          id_setToNA <- which(tmp[,ith_col+1]!="" & tmp[,ith_col+1]!="NA" & !is.na(tmp[,ith_col+1]))
          if(length(id_setToNA)>0){
            tmp[id_setToNA,ith_col] <- NA
          }
        }
        
        if(AQM_sites[ith_AQM_site]%in%c("RGS","STA","SCH")){
          tmp <- tmp[,c(1,2,4,10)]
        }
        if(AQM_sites[ith_AQM_site]%in%c("HEU")){
          tmp <- tmp[,c(1,2,4,6)]
        }
        
        tmp$NewCol    <- tmp[,2] + tmp[,4]
        
        colnames(tmp) <- c("date",paste(AQM_sites[ith_AQM_site],c("_NO2","_O3","_NO","_NOX"),sep=""))
        
        tmp$date      <- strptime(tmp$date,"%d.%m.%Y %H:%M",tz="UTC") - 60
        
        AQM_data_tmp  <- rbind(AQM_data_tmp,tmp[which(!is.na(tmp$date)),])
      }
    }
    
    #
    
    if(AQM_sites[ith_AQM_site] %in% c("ZUE")){
      
      files   <- NULL
      files   <- c(files,list.files(path = "/newhome/muem/mnt/Win_K/Daten/Stationen/ZUE/",pattern = paste("ZUE O3 NO2 Temp Feucht MM1_19[[:digit:]]{4}.csv",sep=""),full.names = T))
      files   <- c(files,list.files(path = "/newhome/muem/mnt/Win_K/Daten/Stationen/ZUE/",pattern = paste("ZUE O3 NO2 Temp Feucht MM1_20[[:digit:]]{4}.csv",sep=""),full.names = T))
      n_files <- length(files)
      
      for(ith_files in 1:n_files){
        tmp <- read.table(files[ith_files],header = F,skip = 4,sep = ";",as.is=T)
        
        tmp <- tmp[,1:7]
        
        for(ith_col in seq(2,dim(tmp)[2],2)){
          id_setToNA <- which(tmp[,ith_col+1]!="")
          if(length(id_setToNA)>0){
            tmp[id_setToNA,ith_col] <- NA
          }
        }
        
        tmp <- tmp[,c(1,2,4,6)]
        
        tmp$NewCol    <- tmp[,3] + tmp[,4]
        
        colnames(tmp) <- c("date",paste(AQM_sites[ith_AQM_site],c("_O3","_NO2","_NO","_NOX"),sep=""))
        
        tmp$date      <- strptime(tmp$date,"%d.%m.%Y %H:%M",tz="UTC") - 60
        
        AQM_data_tmp  <- rbind(AQM_data_tmp,tmp[which(!is.na(tmp$date)),])
      }
    }
    
    #
    
    AQM_data_tmp <- AQM_data_tmp[order(AQM_data_tmp$date),]
    
    if(is.null(AQM_data)){
      AQM_data <- AQM_data_tmp
    }else{
      AQM_data <- merge(AQM_data,AQM_data_tmp,all=T)
    }
  }
  
  # Zeitvariablen
  
  AQM_data$hour <- as.numeric(strftime(AQM_data$date,"%H",tz="UTC"))
  AQM_data$dow  <- as.numeric(strftime(AQM_data$date,"%w",tz="UTC"))
  
  ph <- c("20190101","20190102","20190419","20190422","20190501","20190530","20190610","20190801",
          "20200101","20200102","20200410","20200413","20200501")
  
  id <- which(strftime(AQM_data$date,"%Y%m%d",tz="UTC") %in% ph)
  
  if(length(id)>0){
    AQM_data$dow[id] <- 0
  }
  
  # Zeitliche Mittelung
  
  min_date_day  <- strptime("20200101000000","%Y%m%d%H%M%S",tz="UTC")
  min_date_hour <- strptime("20200301000000","%Y%m%d%H%M%S",tz="UTC")
  
  AQM_data_hour <- timeAverage(mydata = AQM_data,avg.time = "hour", statistic = "mean",start.date = strptime("20181229000000","%Y%m%d%H%M%S",tz="UTC"))
  AQM_data_day  <- timeAverage(mydata = AQM_data,avg.time = "day",  statistic = "mean",start.date = strptime("20181229000000","%Y%m%d%H%M%S",tz="UTC"))
  
  AQM_data_hour <- as.data.frame(AQM_data_hour)
  AQM_data_day  <- as.data.frame(AQM_data_day)
  
  AQM_data_day$date <- AQM_data_day$date + 43200
  
  AQM_data_day  <- AQM_data_day[ which(AQM_data_day$date  >= min_date_day),]
  AQM_data_hour <- AQM_data_hour[which(AQM_data_hour$date >= min_date_hour),]
  
  
  # Zeitreihen Schadstoffe
  
  for(ith_AQM_site in 1:n_AQM_sites){
    
    for(tres in c("day","hour")){
      
      if(tres == "hour"){
        data_tmp <- AQM_data_hour[,c(which(colnames(AQM_data_hour)=="date"),
                                     which(colnames(AQM_data_hour)==paste(AQM_sites[ith_AQM_site],"_O3",sep="")),
                                     which(colnames(AQM_data_hour)==paste(AQM_sites[ith_AQM_site],"_NO",sep="")),
                                     which(colnames(AQM_data_hour)==paste(AQM_sites[ith_AQM_site],"_NO2",sep="")))]
      }
      
      if(tres == "day"){
        data_tmp <- AQM_data_day[,c(which(colnames(AQM_data_day)=="date"),
                                    which(colnames(AQM_data_day)==paste(AQM_sites[ith_AQM_site],"_O3",sep="")),
                                    which(colnames(AQM_data_day)==paste(AQM_sites[ith_AQM_site],"_NO",sep="")),
                                    which(colnames(AQM_data_day)==paste(AQM_sites[ith_AQM_site],"_NO2",sep="")))]
      }
      
      #
      
      for(species in c("O3","NO","NO2","NOX")){
        
        if(species == "O3"){
          yrange  <- c(0,80)
          ylabStr <- expression(paste("O"[3]*" [ppb]"))
          yy      <- data_tmp[,which(colnames(data_tmp)==paste(AQM_sites[ith_AQM_site],"_O3",sep=""))]
        }
        if(species == "NO"){
          yrange  <- c(0,200)
          ylabStr <- expression(paste("NO [ppb]"))
          yy      <- data_tmp[,which(colnames(data_tmp)==paste(AQM_sites[ith_AQM_site],"_NO",sep=""))]
        }
        if(species == "NO2"){
          yrange  <- c(0,80)
          ylabStr <- expression(paste("NO"[2]*" [ppb]"))
          yy      <- data_tmp[,which(colnames(data_tmp)==paste(AQM_sites[ith_AQM_site],"_NO2",sep=""))]
        }
        if(species == "NOX"){
          yrange  <- c(0,300)
          ylabStr <- expression(paste("NO"[X]*" [ppb]"))
          yy      <- data_tmp[,which(colnames(data_tmp)==paste(AQM_sites[ith_AQM_site],"_NO",sep=""))] + data_tmp[,which(colnames(data_tmp)==paste(AQM_sites[ith_AQM_site],"_NO2",sep=""))]
        }
        
        #
        
        if(tres=="day"){
          fn <- paste(plotdir_AQ_TS,"Tageswerte/",AQM_sites[ith_AQM_site],"_",species,"_",tres,"_TS.pdf",sep="")
        }
        if(tres=="hour"){
          fn <- paste(plotdir_AQ_TS,"Stundenwerte/",AQM_sites[ith_AQM_site],"_",species,"_",tres,"_TS.pdf",sep="")
        }
        
        def_par <- par()
        pdf(file = fn, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
        par(mai=c(1,1,0.5,0.1),mfrow=c(1,1))
        
        y_max         <- max(yy,na.rm=T)
        
        if(tres=="day"){
          x_min         <- min_date_day
          x_max         <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
        }
        if(tres=="hour"){
          x_min         <- min_date_hour
          x_max         <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
        }
        
        x_min_act     <- strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC")
        x_max_act     <- strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC")
        
        mondays       <- seq(strptime("20191230000000","%Y%m%d%H%M%S",tz="UTC"),
                             strptime("20201230000000","%Y%m%d%H%M%S",tz="UTC"),by="weeks")
        
        weekdays      <- seq(strptime("20191230000000","%Y%m%d%H%M%S",tz="UTC"),
                             strptime("20201230000000","%Y%m%d%H%M%S",tz="UTC"),by="days")
        
        xlabTicks     <- seq(strptime("20200101000000","%Y%m%d%H%M%S",tz="UTC"),
                             strptime("20210101000000","%Y%m%d%H%M%S",tz="UTC"),by="2 weeks")
        
        xlabTicks_Lab <- strftime(xlabTicks,"%d/%m/%Y",tz="UTC")
        
        
        plot(c(x_min,x_max),c(NA,NA),ylim=c(0,1.05*y_max),xlab="Date",ylab=ylabStr,
             cex.lab=1.5,cex.axis=1.5, cex.main=1.5, xaxt="n",main=AQM_sites_desc[ith_AQM_site])
        
        axis(side = 1,at = xlabTicks,labels = xlabTicks_Lab,cex.lab=1.5,cex.axis=1.5)
        
        polygon(c(x_min_act,x_max_act,x_max_act,x_min_act),c(-1e3,-1e3,1e6,1e6),col="gray80",border=NA)
        
        if(tres=="hour"){
          for(ith_wd in 1:length(weekdays)){
            lines(c(weekdays[ith_wd],weekdays[ith_wd]),c(-1e6,1e6),col="gray70",lwd=1)
          }
        }
        
        for(ith_mon in 1:length(mondays)){
          lines(c(mondays[ith_mon],mondays[ith_mon]),c(-1e6,1e6),col="gray20",lwd=1.5)
        }
        
        lines( data_tmp$date,yy,col=1,lwd=1)
        
        if(tres=="hour"){
          points(data_tmp$date,yy,col=1,pch=16,cex=0.5)
        }
        if(tres=="day"){
          points(data_tmp$date,yy,col=1,pch=16,cex=1.0)
        }
        
        dev.off()
        par(def_par)
        
      }
    }
  }
  
  
  # Boxplots pollution at site / difference relative to ZUE
  
  mmdd                <- strftime(Sys.time(),"%m%d",tz="UTC")
  period_19_Date_from <- strptime("20190317000000","%Y%m%d%H%M%S",tz="UTC")
  period_19_Date_to   <- strptime(paste("2019",mmdd,"000000",sep=""),"%Y%m%d%H%M%S",tz="UTC")
  period_20_Date_from <- strptime("20200317000000","%Y%m%d%H%M%S",tz="UTC")
  period_20_Date_to   <- strptime(paste("2020",mmdd,"000000",sep=""),"%Y%m%d%H%M%S",tz="UTC")
  
  for(modus in c("SITE","DIFF")){
    for(species in c("NO","NO2","NOX")){
      for(ith_AQM_site in 1:n_AQM_sites){
        
        if(modus=="DIFF"){
          if(!AQM_sites[ith_AQM_site] %in% c("RGS","SCH","STA","HEU")){
            next
          }
        }
        
        pos_1 <- which(colnames(AQM_data)==paste(AQM_sites[ith_AQM_site],"_",species,sep=""))
        
        if(modus=="DIFF"){
          pos_2 <- which(colnames(AQM_data)==paste("ZUE_",species,sep=""))
          ok_00    <- (!is.na(AQM_data[,pos_1] - AQM_data[,pos_2])
                       & AQM_data$date >= period_19_Date_from
                       & AQM_data$date <= period_19_Date_to)
          ok_01    <- (!is.na(AQM_data[,pos_1] - AQM_data[,pos_2])
                       & AQM_data$date >= period_20_Date_from
                       & AQM_data$date <= period_20_Date_to)
        }
        if(modus=="SITE"){
          ok_00 <- (!is.na(AQM_data[,pos_1])
                    & AQM_data$date >= period_19_Date_from
                    & AQM_data$date <= period_19_Date_to)
          ok_01 <- (!is.na(AQM_data[,pos_1])
                    & AQM_data$date >= period_20_Date_from
                    & AQM_data$date <= period_20_Date_to)
        }
        
        for(wd_set in c("Mo-Fr","Sat","Sun")){
          
          if(wd_set=="Mo-Fr"){
            wdays <- 1:5
          }
          if(wd_set=="Sat"){
            wdays <- 6
          }
          if(wd_set=="Sun"){
            wdays <- 0
          }
          
          tmp_00 <- matrix(NA,ncol=24,nrow=dim(AQM_data)[1])
          tmp_01 <- matrix(NA,ncol=24,nrow=dim(AQM_data)[1])
          
          for(hh in 0:23){
            
            id   <- which(AQM_data$hour == hh
                          & AQM_data$dow %in% wdays
                          & AQM_data$date >= period_19_Date_from
                          & AQM_data$date <= period_19_Date_to)
            n_id <- length(id)
            
            if(n_id>0){
              if(modus=="DIFF"){
                tmp_00[1:n_id,hh+1] <- AQM_data[id,pos_1] - AQM_data[id,pos_2]
              }
              if(modus=="SITE"){
                tmp_00[1:n_id,hh+1] <- AQM_data[id,pos_1]
              }
            }
            
            id   <- which(AQM_data$hour == hh
                          & AQM_data$dow %in% wdays
                          & AQM_data$date >= period_20_Date_from
                          & AQM_data$date <= period_20_Date_to)
            n_id <- length(id)
            
            if(n_id>0){
              if(modus=="DIFF"){
                tmp_01[1:n_id,hh+1] <- AQM_data[id,pos_1] - AQM_data[id,pos_2]
              }
              if(modus=="SITE"){
                tmp_01[1:n_id,hh+1] <- AQM_data[id,pos_1]
              }
            }
          }
          
          #
          
          if(modus=="SITE"){
            fn <- paste(plotdir_AQ_BP,modus,"_",AQM_sites[ith_AQM_site],"_",species,"_",wd_set,"_BP.pdf",sep="")
          }
          if(modus=="DIFF"){
            fn <- paste(plotdir_AQ_BP,modus,"_",AQM_sites[ith_AQM_site],"_ZUE_",species,"_",wd_set,"_BP.pdf",sep="")
          }
          
          def_par <- par()
          pdf(file = fn, width=10, height=8, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.5,0.1),mfrow=c(1,1))
          
          
          mainStr <- ""
          xlabStr <- "Hour of day [CET]"
          
          if(modus=="DIFF"){
            ylabStr <- paste(species," ",AQM_sites[ith_AQM_site]," - ",species," ZUE [ppb]",sep="")
          }
          if(modus=="SITE"){
            ylabStr <- paste(species," ",AQM_sites[ith_AQM_site]," [ppb]",sep="")
          }
          
          if(species=="NO"){
            if(modus=="DIFF"){
              yrange <- c(-20,100)
            }
            if(modus=="SITE"){
              yrange <- c(0,150)
            }
          }
          if(species=="NO2"){
            if(modus=="DIFF"){
              yrange <- c(-20,60)
            }
            if(modus=="SITE"){
              yrange <- c(0,100)
            }
          }
          if(species=="NOX"){
            if(modus=="DIFF"){
              yrange <- c(-20,120)
            }
            if(modus=="SITE"){
              yrange <- c(0,180)
            }
          }
          
          plot(c(0,24),c(NA,NA),main=mainStr,ylim=yrange,xlab=xlabStr,ylab=ylabStr,xaxt="n",cex.lab=1.5,cex.axis=1.5,cex.main=1.5)
          axis(side = 1,at = seq(0,24,6),labels = seq(0,24,6),cex.lab=1.5,cex.axis=1.5)
          
          lines(c(-10,100),c(0,0),col=1,lwd=1,lty=5)
          
          polygon(c(seq(0.5,23.5,1),seq(23.5,0.5,-1)),
                  c(apply(tmp_00,2,quantile,0.25,na.rm=T),rev(apply(tmp_00,2,quantile,0.75,na.rm=T))),
                  col="gray70",border=NA)
          
          lines(seq(0.5,23.5,1),apply(tmp_00,2,quantile,0.25,na.rm=T),col=1,lwd=1,lty=5)
          lines(seq(0.5,23.5,1),apply(tmp_00,2,quantile,0.50,na.rm=T),col=1,lwd=2,lty=1)
          lines(seq(0.5,23.5,1),apply(tmp_00,2,quantile,0.75,na.rm=T),col=1,lwd=1,lty=5)
          
          lines(seq(0.5,23.5,1),apply(tmp_01,2,quantile,0.25,na.rm=T),col=2,lwd=2,lty=5)
          lines(seq(0.5,23.5,1),apply(tmp_01,2,quantile,0.50,na.rm=T),col=2,lwd=2,lty=1)
          lines(seq(0.5,23.5,1),apply(tmp_01,2,quantile,0.75,na.rm=T),col=2,lwd=2,lty=5)
          
          
          leg_str_00 <- paste(strftime(min(AQM_data$date[ok_00]),"%d/%m/%y",tz="UTC")," - ",strftime(max(AQM_data$date[ok_00]),"%d/%m/%y",tz="UTC"),sep="")
          leg_str_01 <- paste(strftime(min(AQM_data$date[ok_01]),"%d/%m/%y",tz="UTC")," - ",strftime(max(AQM_data$date[ok_01]),"%d/%m/%y",tz="UTC"),sep="")
          
          par(family="mono")
          legend("topleft",legend=c(leg_str_00,leg_str_01),pch=15,col=c("gray70","red"),bg="white",cex=1.5)
          par(family="")
          
          dev.off()
          par(def_par)
          
        }
      }
    }
  }
}
