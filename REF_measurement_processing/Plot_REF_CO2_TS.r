# PLOT_CO2_TS_REFSITES.r
# --------------------------------
#
# Author: Michael Mueller
#
#
# --------------------------------


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

## source

source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

### ----------------------------------------------------------------------------------------------------------------------------

## directories

resultdir <- "/project/CarboSense/Carbosense_Network/TS_CO2_REF_Measurements/"

### ----------------------------------------------------------------------------------------------------------------------------

INCLUDE_ANCHOR_EVENTS <- F

### ----------------------------------------------------------------------------------------------------------------------------

tables   <- c("NABEL_HAE","NABEL_PAY","NABEL_DUE","NABEL_RIG","UNIBE_BRM","EMPA_LAEG","UNIBE_GIMM","nabelnrt_jun")
tables   <- c("NABEL_HAE","NABEL_PAY","NABEL_DUE","NABEL_RIG","UNIBE_BRM","EMPA_LAEG","UNIBE_GIMM")
n_tables <- length(tables)

site_colors <- c("gray40","green4","red","cyan","blue","orange","magenta","green")
site_colors <- c("gray40","green4","red","cyan","blue","orange","magenta")

### ----------------------------------------------------------------------------------------------------------------------------


# Plot data of all available sites together

date_start <- strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC")
date_end   <- strptime(strftime(Sys.time(),"%Y%m%d%H%M%S",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")+7*86400
time_agg   <- "week"

# Start on a Monday / first of month

date_start <- strptime(strftime(date_start,"%Y%m%d000000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")

if(time_agg=="week"){
  date_start <- date_start - ((as.numeric(strftime(date_start,"%w",tz="UTC"))+6)%%7)*86400
  all_dates  <- seq(date_start,date_end,"week")
}
if(time_agg=="month"){
  date_start <- date_start - (as.numeric(strftime(date_start,"%d",tz="UTC"))-1)*86400
  all_dates  <- seq(date_start,date_end,"month")
}

all_dates_yyyy <- as.numeric(strftime(all_dates,"%Y",tz="UTC"))

#

if(INCLUDE_ANCHOR_EVENTS){
  anchor_events <- read.table(file = "/project/muem/CarboSense/anchor_events.csv",header=T,as.is=T,sep=";")
  anchor_events$Date_UTC_start <- strptime(anchor_events$Date_UTC_start_str,"%Y-%m-%d %H:%M:%S",tz="UTC")
  anchor_events$Date_UTC_end   <- strptime(anchor_events$Date_UTC_end_str,  "%Y-%m-%d %H:%M:%S",tz="UTC")
}

#

for(yyyy_plot in c(2020,2020)){
  
  figname <- paste(resultdir,"/ALL_REF_CO2_",yyyy_plot,"_TS.pdf",sep="")
  
  def_par <- par()
  pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
  par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
  
  #
  
  for(ith_date in 1:(length(all_dates)-1)){
    
    if(all_dates_yyyy[ith_date]!=yyyy_plot & all_dates_yyyy[ith_date+1]!=yyyy_plot){
      next
    }
    
    timestamp_from <- as.numeric(difftime(time1=all_dates[ith_date],  time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
    timestamp_to   <- as.numeric(difftime(time1=all_dates[ith_date+1],time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
    
    if(time_agg=="week"){
      xlabTicks      <- seq(all_dates[ith_date],all_dates[ith_date+1],"day")
      xlabTicksLab   <- strftime(xlabTicks,"%d/%m/%Y",tz="UTC")
    }
    if(time_agg=="month"){
      xlabTicks      <- seq(all_dates[ith_date],all_dates[ith_date+1],"week")
      xlabTicksLab   <- strftime(xlabTicks,"%d/%m/%Y",tz="UTC")
    }
    
    plot(c(all_dates[ith_date],all_dates[ith_date+1]),c(0,0),ylim=c(350,650),xaxt="n",xlab="Date [UTC]",ylab=expression(paste("CO"[2]*" [ppm]")),cex.lab=1.25,cex.axis=1.25)
    axis(side = 1,at = xlabTicks,labels = xlabTicksLab,cex.lab=1.25,cex.axis=1.25)
    
    if(INCLUDE_ANCHOR_EVENTS){
      id_anchor_events <- which(anchor_events$Date_UTC_end>=date_now & anchor_events$Date_UTC_start<date_to & anchor_events$WIND_SITUATION=="WIND_ZH")
      
      if(length(id_anchor_events)>0){
        for(ith_id_anchor_events in 1:length(id_anchor_events)){
          polygon(c(anchor_events$Date_UTC_start[id_anchor_events[ith_id_anchor_events]],
                    anchor_events$Date_UTC_end[id_anchor_events[ith_id_anchor_events]],
                    anchor_events$Date_UTC_end[id_anchor_events[ith_id_anchor_events]],
                    anchor_events$Date_UTC_start[id_anchor_events[ith_id_anchor_events]]),c(-1e9,-1e9,1e9,1e9),col="blue",border=NA)
        }
      }
      
      #
      
      id_anchor_events <- which(anchor_events$Date_UTC_end>=date_now & anchor_events$Date_UTC_start<date_to & anchor_events$WIND_SITUATION=="WIND_CH_PLATEAU" & anchor_events$median_diff_CO2_PAY_DUE<=5)
      
      if(length(id_anchor_events)>0){
        for(ith_id_anchor_events in 1:length(id_anchor_events)){
          polygon(c(anchor_events$Date_UTC_start[id_anchor_events[ith_id_anchor_events]],
                    anchor_events$Date_UTC_end[id_anchor_events[ith_id_anchor_events]],
                    anchor_events$Date_UTC_end[id_anchor_events[ith_id_anchor_events]],
                    anchor_events$Date_UTC_start[id_anchor_events[ith_id_anchor_events]]),c(-1e9,-1e9,1e9,1e9),col="orange",border=NA)
        }
      }
    }
    
    for(dd in xlabTicks){
      lines(c(dd,dd),c(-1e9,1e9),lwd=1,col="gray70")
    }
    
    
    for(ith_table in 1:n_tables){
      
      if(tables[ith_table]%in%c("NABEL_PAY","NABEL_RIG","NABEL_HAE","NABEL_DUE")){
        query_str <- paste("SELECT timestamp, CO2_WET_COMP FROM ",tables[ith_table]," WHERE timestamp >= ",timestamp_from," and timestamp <= ",timestamp_to,";",sep="")
        drv       <- dbDriver("MySQL")
        con       <- dbConnect(drv, group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        data      <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        colnames(data)[which(colnames(data)=="CO2_WET_COMP")] <- "CO2"
        data$CO2_F <- as.numeric(data$CO2 != -999)
        data       <- data[,c(which(colnames(data)=="timestamp"),which(colnames(data)=="CO2"),which(colnames(data)=="CO2_F"))]
      }
      
      if(tables[ith_table]=="UNIBE_BRM"){
        query_str <- paste("SELECT timestamp, CO2, CO2_F FROM ",tables[ith_table]," WHERE MEAS_HEIGHT = 12 and CO2_DRY_N > 5 and CO2 != -999 and timestamp >= ",timestamp_from," and timestamp <= ",timestamp_to,";",sep="")
        drv       <- dbDriver("MySQL")
        con       <- dbConnect(drv, group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        data      <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
      }
      
      if(tables[ith_table]=="EMPA_LAEG"){
        query_str <- paste("SELECT timestamp, CO2, CO2_F FROM ",tables[ith_table]," WHERE VALVEPOS=0 and CO2_DRY_N > 1 and CO2 != -999 and timestamp >= ",timestamp_from," and timestamp <= ",timestamp_to,";",sep="")
        drv       <- dbDriver("MySQL")
        con       <- dbConnect(drv, group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        data      <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
      }
      
      if(tables[ith_table]=="UNIBE_GIMM"){
        query_str <- paste("SELECT timestamp, CO2, CO2_F FROM ",tables[ith_table]," WHERE VALVEPOS=5 and CO2_DRY_N > 12 and CO2 != -999 and timestamp >= ",timestamp_from," and timestamp <= ",timestamp_to,";",sep="")
        drv       <- dbDriver("MySQL")
        con       <- dbConnect(drv, group="CarboSense_MySQL")
        res       <- dbSendQuery(con, query_str)
        data      <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
      }
      
      if(tables[ith_table]=="nabelnrt_jun"){
        query_str <- paste("SELECT timed, CO2 FROM ",tables[ith_table]," WHERE CO2 IS NOT NULL and timed >= ",timestamp_from*1e3," and timed <= ",timestamp_to*1e3,";",sep="")
        drv       <- dbDriver("MySQL")
        con       <- dbConnect(drv, group="NabelGsn")
        res       <- dbSendQuery(con, query_str)
        data      <- fetch(res, n=-1)
        dbClearResult(res)
        dbDisconnect(con)
        
        data           <- data.frame(timestamp=data$timed/1e3,
                                     CO2=data$CO2,
                                     CO2_F=rep(1,dim(data)[1]),
                                     stringsAsFactors = 1)
      }
      
      
      #
      
      if(dim(data)[1]==0){
        next
      }
      
      id_nok <- which(data$CO2== -999 | data$CO2 < 350)
      
      if(length(id_nok)>0){
        data$CO2[id_nok] <- NA
      }
      
      if(all(is.na(data$CO2))){
        next
      }
      
      #
      
      id_fill <- which(diff(data$timestamp)>1200)
      if(length(id_fill)>0){
        data <- rbind(data,data.frame(timestamp = data$timestamp[id_fill]+60,
                                      CO2       = rep(NA,length(id_fill)),
                                      CO2_F     = rep(0,length(id_fill)),
                                      stringsAsFactors = F))
        
        data <- data[order(data$timestamp),]
      }
      
      data$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
      
      
      lines(data$date,data$CO2,ylim=c(350,650),xaxt="n",xlab="Date",ylab=expression(paste("CO"[2]*" [ppm]")),cex.lab=1.25,cex.axis=1.25,col=site_colors[ith_table],cex=1.25)
    }
    
    par(family="mono")
    legend("topright",legend=tables,col=site_colors,lwd=1,lty=1,bg="white")
    par(family="")
    
  }
  
  dev.off()
  par(def_par)
  
}


### ----------------------------------------------------------------------------------------------------------------------------

# Individual Plots

for(ith_table in 1:n_tables){
  
  if(tables[ith_table]%in%c("NABEL_PAY","NABEL_RIG","NABEL_HAE","NABEL_DUE")){
    query_str <- paste("SELECT timestamp, CO2_WET_COMP FROM ",tables[ith_table]," WHERE CO2_WET_COMP != -999;",sep="")
    drv       <- dbDriver("MySQL")
    con       <- dbConnect(drv, group="CarboSense_MySQL")
    res       <- dbSendQuery(con, query_str)
    data      <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    colnames(data)[which(colnames(data)=="CO2_WET_COMP")] <- "CO2"
    data$CO2_F <- as.numeric(data$CO2 != -999)
    data       <- data[,c(which(colnames(data)=="timestamp"),which(colnames(data)=="CO2"),which(colnames(data)=="CO2_F"))]
  }
  
  if(tables[ith_table]=="UNIBE_BRM"){
    query_str <- paste("SELECT timestamp, CO2,CO2_F FROM ",tables[ith_table]," WHERE MEAS_HEIGHT = 12 and CO2_DRY_N > 5 and CO2 != -999;",sep="")
    drv       <- dbDriver("MySQL")
    con       <- dbConnect(drv, group="CarboSense_MySQL")
    res       <- dbSendQuery(con, query_str)
    data      <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
  }
  if(tables[ith_table]=="EMPA_LAEG"){
    query_str <- paste("SELECT timestamp, CO2,CO2_F FROM ",tables[ith_table]," WHERE VALVEPOS=0 and CO2_DRY_N > 5 and CO2 != -999;",sep="")
    drv       <- dbDriver("MySQL")
    con       <- dbConnect(drv, group="CarboSense_MySQL")
    res       <- dbSendQuery(con, query_str)
    data      <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
  }
  if(tables[ith_table]=="UNIBE_GIMM"){
    query_str <- paste("SELECT timestamp, CO2,CO2_F FROM ",tables[ith_table]," WHERE VALVEPOS=5 and CO2_DRY_N > 12 and CO2 != -999;",sep="")
    drv       <- dbDriver("MySQL")
    con       <- dbConnect(drv, group="CarboSense_MySQL")
    res       <- dbSendQuery(con, query_str)
    data      <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
  }
  
  if(tables[ith_table]=="nabelnrt_jun"){
    query_str <- paste("SELECT timed, CO2 FROM ",tables[ith_table]," WHERE CO2 IS NOT NULL;",sep="")
    drv       <- dbDriver("MySQL")
    con       <- dbConnect(drv, group="NabelGsn")
    res       <- dbSendQuery(con, query_str)
    data      <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    data           <- data.frame(timestamp=data$timed/1e3,
                                 CO2=data$CO2,
                                 CO2_F=rep(1,dim(data)[1]),
                                 stringsAsFactors = 1)
  }
  
  
  
  
  # Daily minimum
  
  data_date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
  data_hour <- as.numeric(strftime(data_date,"%H",tz="UTC"))
  
  for(ith_dm in 1:2){
    
    if(ith_dm == 1){
      id <- which(data$CO2!=-999 
                  & data$CO2_F==1 
                  & data_date>=strptime("20180101000000","%Y%m%d%H%M%S",tz="UTC")
                  & data_date<=strptime("20190101000000","%Y%m%d%H%M%S",tz="UTC"))
    }
    if(ith_dm == 2){
      id <- which(data$CO2!=-999 
                  & data$CO2_F==1 
                  & data_date>=strptime("20180101000000","%Y%m%d%H%M%S",tz="UTC")
                  & data_date<=strptime("20190101000000","%Y%m%d%H%M%S",tz="UTC")
                  & data_hour%in%c(12,13,14,15,16,17))
    }
    
    data_day_min <- timeAverage(mydata     = data.frame(date=as.POSIXct(data_date[id]),CO2=data$CO2[id],stringsAsFactors = F),
                                statistic  = "min",avg.time = "day", 
                                start.date = strptime(strftime(min(as.POSIXct(data_date[id])),"%Y%m%d000000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
    
    figname <- paste(resultdir,"/",tables[ith_table],"_DAILY_MIN_",ith_dm,".pdf",sep="")
    
    def_par <- par()
    pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1,1,0.1,0.1),mfrow=c(1,2))
    
    leg_str_00 <- paste("Q000:",sprintf("%6.1f",quantile(data_day_min$CO2,probs=0.00,na.rm=T)))
    leg_str_01 <- paste("Q005:",sprintf("%6.1f",quantile(data_day_min$CO2,probs=0.05,na.rm=T)))
    leg_str_02 <- paste("Q050:",sprintf("%6.1f",quantile(data_day_min$CO2,probs=0.50,na.rm=T)))
    leg_str_03 <- paste("Q095:",sprintf("%6.1f",quantile(data_day_min$CO2,probs=0.95,na.rm=T)))
    leg_str_04 <- paste("Q100:",sprintf("%6.1f",quantile(data_day_min$CO2,probs=1.00,na.rm=T)))
    leg_str_05 <- paste("N:   ",sprintf("%6.0f",sum(!is.na(data_day_min$CO2))))
    
    data_min <- floor(  min(data_day_min$CO2-2.5,na.rm=T))
    data_max <- ceiling(max(data_day_min$CO2+2.5,na.rm=T))
    
    hist(data_day_min$CO2,seq(data_min,data_max,2.5),col="slategray",xlab="Daily CO2 minimum [ppb]",main="",cex.axis=1.25,cex.lab=1.25,xlim=c(350,450))
    
    par(family="mono")
    legend("topright",legend=c(leg_str_00,leg_str_01,leg_str_02,leg_str_03,leg_str_04,leg_str_05),bg="white",cex=1.25)
    par(family="")
    
    #
    
    diff_data <- diff(data_day_min$CO2)
    
    data_min <- floor(  min(diff_data-2.5,na.rm=T))
    data_max <- ceiling(max(diff_data+2.5,na.rm=T))
    
    leg_str_00 <- paste("Q000:",sprintf("%6.1f",quantile(diff_data,probs=0.00,na.rm=T)))
    leg_str_01 <- paste("Q005:",sprintf("%6.1f",quantile(diff_data,probs=0.05,na.rm=T)))
    leg_str_02 <- paste("Q050:",sprintf("%6.1f",quantile(diff_data,probs=0.50,na.rm=T)))
    leg_str_03 <- paste("Q095:",sprintf("%6.1f",quantile(diff_data,probs=0.95,na.rm=T)))
    leg_str_04 <- paste("Q100:",sprintf("%6.1f",quantile(diff_data,probs=1.00,na.rm=T)))
    leg_str_05 <- paste("N:   ",sprintf("%6.0f",sum(!is.na(diff_data))))
    
    hist(diff_data,seq(data_min,data_max,2.5),col="slategray",xlab="Difference of daily CO2 minimum [ppb]",main="",cex.axis=1.25,cex.lab=1.25,xlim=c(-30,30))
    
    par(family="mono")
    legend("topright",legend=c(leg_str_00,leg_str_01,leg_str_02,leg_str_03,leg_str_04,leg_str_05),bg="white",cex=1.25)
    par(family="")
    
    dev.off()
    par(def_par)
    
  }
  
  
  # ------------
  
  id_fill <- which(diff(data$timestamp)>1200)
  if(length(id_fill)>0){
    data <- rbind(data,data.frame(timestamp = data$timestamp[id_fill]+60,
                                  CO2       = rep(NA,length(id_fill)),
                                  CO2_F     = rep(0,length(id_fill)),
                                  stringsAsFactors = F))
    
    data <- data[order(data$timestamp),]
  }
  
  data$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + data$timestamp
  
  
  
  #
  
  figname <- paste(resultdir,"/",tables[ith_table],"_CO2_TS.pdf",sep="")
  
  yyy     <- cbind(data$CO2,rep(400,dim(data)[1]),rep(420,dim(data)[1]),rep(380,dim(data)[1]))
  yyy     <- data$CO2
  
  xlabString <- "Date" 
  ylabString <- expression(paste("CO"[2]*" [ppm]"))
  legend_str <- c("CO2")
  plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(350,650),xlabString,ylabString,legend_str)
  
  #
  
  figname <- paste(resultdir,"/",tables[ith_table],"_CO2_TS_YEAR.pdf",sep="")
  
  plot_ts(figname,data$date,yyy,"year",NULL,c(350,650),xlabString,ylabString,legend_str)
  
  #
  
  figname <- paste(resultdir,"/",tables[ith_table],"_CO2_TS_MONTH.pdf",sep="")
  
  plot_ts(figname,data$date,yyy,"month",NULL,c(350,650),xlabString,ylabString,legend_str)
  
  #
  
  figname <- paste(resultdir,"/",tables[ith_table],"_CO2_TS_WEEK.pdf",sep="")
  
  plot_ts(figname,data$date,yyy,"week",NULL,c(350,650),xlabString,ylabString,legend_str)
  
  #
  
  figname <- paste(resultdir,"/",tables[ith_table],"_CO2_TS_DAY.pdf",sep="")
  
  plot_ts(figname,data$date,yyy,"day",NULL,c(350,650),xlabString,ylabString,legend_str)
  
  #
  
  # if(tables[ith_table]=="NABEL_DUE"){
  #   
  #   data <- data[which(data$date>=strptime("20180101000000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20190101000000","%Y%m%d%H%M%S",tz="UTC")),]
  #   data <- data[,c(which(colnames(data)=="date"),which(colnames(data)=="CO2"))]
  #   
  #   #
  #   
  #   fn            <- paste(resultdir,"/CO2_DUE_2018_01min.csv",sep="")
  #   data_exp      <- data
  #   data_exp$date <- strftime(data_exp$date,"%Y-%m-%d %H:%M:%S",tz="UTC")
  #   data_exp$CO2  <- round(x=data_exp$CO2,digits=1)
  #   write.table(data_exp,file = fn,sep=";",row.names = F, col.names=T,quote = F)
  #   
  #   #
  #   
  #   data <- timeAverage(mydata = data,avg.time = "10 min", statistic = "mean",start.date = strptime("20171231230000","%Y%m%d%H%M%S",tz="UTC"))
  # 
  #   fn            <- paste(resultdir,"/CO2_DUE_2018_10min.csv",sep="")
  #   data_exp      <- data
  #   data_exp$date <- strftime(data_exp$date,"%Y-%m-%d %H:%M:%S",tz="UTC")
  #   data_exp$CO2  <- round(x=data_exp$CO2,digits=1)
  #   write.table(data_exp,file = fn,sep=";",row.names = F, col.names=T,quote = F)
  #   
  # }
  
}