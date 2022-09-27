# Comparison_CO2_HPP_REF.r
# -----------------------------

# Remarks:
# - Comparison of CO2 data from HPP and REF instrument.
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

ma <- function(x,n=10){filter(x,rep(1/n,n), sides=1)}

### ----------------------------------------------------------------------------------------------------------------------------

HPP_ProcDataTblName <- "CarboSense_HPP_CO2"

### ----------------------------------------------------------------------------------------------------------------------------

ANALYSE_WINDSIT <- T

### ----------------------------------------------------------------------------------------------------------------------------

query_str         <- paste("SELECT * FROM Deployment WHERE SensorUnit_ID BETWEEN 426 AND 445 and LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1','DUE5','DUE6');",sep="")
drv               <- dbDriver("MySQL")
con <-carboutil::get_conn()
res               <- dbSendQuery(con, query_str)
tbl_depl          <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_depl$Date_UTC_from  <- strptime(tbl_depl$Date_UTC_from, "%Y-%m-%d %H:%M:%S", tz="UTC")
tbl_depl$Date_UTC_to    <- strptime(tbl_depl$Date_UTC_to,   "%Y-%m-%d %H:%M:%S", tz="UTC")

tbl_depl$timestamp_from <- as.numeric(tbl_depl$Date_UTC_from)
tbl_depl$timestamp_to   <- as.numeric(tbl_depl$Date_UTC_to)

### ----------------------------------------------------------------------------------------------------------------------------

resultdir         <- "/project/CarboSense/Carbosense_Network/HPP_PerformanceAnalysis/Comparison_HPP_REF/"

statistics        <- NULL

weekly_statistics <- NULL

if(ANALYSE_WINDSIT){
  AE_SU <- read.table(file = paste("/project/CarboSense/Carbosense_Network/LP8_PerformanceAnalysis_TEST00/anchor_events_SU_ALL.csv"),header=T,sep=";")
}

### ----------------------------------------------------------------------------------------------------------------------------

for(ith_depl in 1:dim(tbl_depl)[1]){
  timestamp_from <- tbl_depl$timestamp_from[ith_depl]
  timestamp_to <- min(c(as.numeric(lubridate::now()), tbl_depl$timestamp_to[ith_depl]))
  print(paste("Deployment row", ith_depl))
  print(paste("Processing deployment", paste(tbl_depl[ith_depl,], collapse='')))
  print(paste("Between", tbl_depl$Date_UTC_from[ith_depl], "and",  tbl_depl$Date_UTC_to[ith_depl]))

  # Compare CO2 measurements
  # ------------------------
  
  HPP_query <- "SELECT 
                  timestamp, 
                  CO2_CAL_ADJ AS HPP_CO2, 
                  H2O AS HPP_H2O 
                FROM {`HPP_ProcDataTblName`}
                WHERE LocationName = {tbl_depl$LocationName[ith_depl]} AND SensorUnit_ID = {tbl_depl$SensorUnit_ID[ith_depl]} 
                AND timestamp BETWEEN {timestamp_from} AND {timestamp_to}"
  # query_str         <- paste("SELECT timestamp, CO2_CAL_ADJ as HPP_CO2, H2O as HPP_H2O FROM ",HPP_ProcDataTblName," ",sep="")
  # query_str         <- paste(query_str,"WHERE LocationName = '",tbl_depl$LocationName[ith_depl],"' and SensorUnit_ID = ",tbl_depl$SensorUnit_ID[ith_depl]," ",sep="")
  # query_str         <- paste(query_str,"AND CO2_CAL_ADJ != -999 and Valve = 0 ",sep="")
  # query_str         <- paste(query_str,"AND timestamp >= ",timestamp_from," and timestamp <= ",tbl_depl$timestamp_to[ith_depl],";",sep="")
  # drv               <- dbDriver("MySQL")
  con <-carboutil::get_conn()
  HPP_query_interp <- glue::glue_sql(HPP_query, .con = con)
  res               <- dbSendQuery(con, HPP_query_interp)
  tbl_HPP           <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  if(dim(tbl_HPP)[1]==0){
    next
  }
  
  tbl_HPP      <- tbl_HPP[order(tbl_HPP$timestamp),]
  tbl_HPP$date <- lubridate::as_datetime(tbl_HPP$timestamp, tz="UTC")
  
  
  for(ref_site in c("DUE","PAY","LAEG","RIG","BRM","HAE")){x
    
    if(ref_site %in% c("DUE","PAY","RIG","HAE")){
      query_str         <- paste("SELECT timestamp, CO2_WET_COMP, H2O FROM ",paste("NABEL_",ref_site,sep="")," ",sep="")
      query_str         <- paste(query_str, "WHERE CO2_WET_COMP != -999 AND H2O != -999 ",sep="")
      query_str         <- paste(query_str, "AND timestamp >= ",tbl_depl$timestamp_from[ith_depl]," and timestamp <= ",tbl_depl$timestamp_to[ith_depl],";",sep="")
      drv               <- dbDriver("MySQL")
      con <-carboutil::get_conn()
      res               <- dbSendQuery(con, query_str)
      tbl_REF           <- dbFetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      tbl_REF           <- tbl_REF[which(tbl_REF$CO2_WET_COMP>375),]
      
      colnames(tbl_REF) <- c("timestamp","CO2","H2O") 
    }
    
    if(ref_site == "LAEG"){
      query_str         <- paste("SELECT timestamp, CO2, H2O FROM ",paste("EMPA_",ref_site,sep="")," WHERE CO2 != -999 and timestamp >= ",tbl_depl$timestamp_from[ith_depl]," and timestamp <= ",tbl_depl$timestamp_to[ith_depl],";",sep="")
      drv               <- dbDriver("MySQL")
      con <-carboutil::get_conn()
      res               <- dbSendQuery(con, query_str)
      tbl_REF           <- dbFetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      tbl_REF      <- tbl_REF[order(tbl_REF$timestamp),]
    }
    
    if(ref_site == "BRM"){
      query_str         <- paste("SELECT timestamp, CO2, H2O FROM ",paste("UNIBE_",ref_site,sep="")," WHERE CO2 != -999 and timestamp >= ",tbl_depl$timestamp_from[ith_depl]," and timestamp <= ",tbl_depl$timestamp_to[ith_depl],";",sep="")
      drv               <- dbDriver("MySQL")
      con <-carboutil::get_conn()
      res               <- dbSendQuery(con, query_str)
      tbl_REF           <- dbFetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      tbl_REF      <- tbl_REF[order(tbl_REF$timestamp),]
    }
    
    #
    
    if(dim(tbl_REF)[1]==0){
      next
    }
    
    #
    
    tbl_REF$date <- lubridate::as_datetime(tbl_REF$timestamp, tz="UTC")
  
    #
     
    save.image("a.RData")
    quit()
    print("combining data with reference")
    data <- dplyr::left_join(timeAverage(mydata = tbl_HPP ,avg.time = "10 min",statistic = "mean"),
    timeAverage(mydata = tbl_REF ,avg.time = "10 min",statistic = "mean"), by=c("date"))
    print(data)
    if(dim(data)[1]==0){
      next()
    }

    #
    
    #
    if(dim(data)[1]==0){
      next()
    }

    if (T){

    id_ok   <- which(!is.na(data$CO2) & !is.na(data$HPP_CO2))
    n_id_ok <- length(id_ok)
    
    RMSE     <- sqrt( sum( (data$CO2[id_ok]-data$HPP_CO2[id_ok])^2 ) / n_id_ok)
    RMSE_H2O <- sqrt( sum( (data$H2O[id_ok]-data$HPP_H2O[id_ok])^2 ) / n_id_ok)
    
    Q000 <- quantile(data$HPP_CO2[id_ok]-data$CO2[id_ok],probs=0.00)
    Q005 <- quantile(data$HPP_CO2[id_ok]-data$CO2[id_ok],probs=0.05)
    Q050 <- quantile(data$HPP_CO2[id_ok]-data$CO2[id_ok],probs=0.50)
    Q095 <- quantile(data$HPP_CO2[id_ok]-data$CO2[id_ok],probs=0.95)
    Q100 <- quantile(data$HPP_CO2[id_ok]-data$CO2[id_ok],probs=1.00)
    MEAN <- mean(data$HPP_CO2[id_ok]-data$CO2[id_ok])
    
    Q000_H2O <- quantile(data$HPP_H2O[id_ok]-data$H2O[id_ok],probs=0.00)
    Q005_H2O <- quantile(data$HPP_H2O[id_ok]-data$H2O[id_ok],probs=0.05)
    Q050_H2O <- quantile(data$HPP_H2O[id_ok]-data$H2O[id_ok],probs=0.50)
    Q095_H2O <- quantile(data$HPP_H2O[id_ok]-data$H2O[id_ok],probs=0.95)
    Q100_H2O <- quantile(data$HPP_H2O[id_ok]-data$H2O[id_ok],probs=1.00)
    MEAN_H2O <- mean(data$HPP_H2O[id_ok]-data$H2O[id_ok])
    
    
    statistics <- rbind(statistics,data.frame(SensorUnit_ID = tbl_depl$SensorUnit_ID[ith_depl],
                                              LocationName  = tbl_depl$LocationName[ith_depl],
                                              REF_SITE      = ref_site,
                                              Date_UTC_from = strftime(min(data$date[id_ok]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                              Date_UTC_to   = strftime(max(data$date[id_ok]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                              RMSE          = RMSE,
                                              MEAN_DIFF     = MEAN,
                                              N             = n_id_ok,
                                              Q000          = Q000,
                                              Q005          = Q005,
                                              Q050          = Q050,
                                              Q095          = Q095,
                                              Q100          = Q100,
                                              MEAN_DIFF_H2O = MEAN,
                                              Q000_H2O      = Q000_H2O,
                                              Q005_H2O      = Q005_H2O,
                                              Q050_H2O      = Q050_H2O,
                                              Q095_H2O      = Q095_H2O,
                                              Q100_H2O      = Q100_H2O,
                                              stringsAsFactors = F))
    
    ### SCATTER CO2
    
    str_00 <- paste("RMSE:", sprintf("%6.1f",RMSE))
    str_01 <- paste("N:   ", sprintf("%6.0f",n_id_ok))
    
    str_10 <- paste("Q000: ",sprintf("%7.1f",Q000))
    str_11 <- paste("Q005: ",sprintf("%7.1f",Q005))
    str_12 <- paste("Q050: ",sprintf("%7.1f",Q050))
    str_13 <- paste("Q095: ",sprintf("%7.1f",Q095))
    str_14 <- paste("Q100: ",sprintf("%7.1f",Q100))
    str_15 <- paste("MEAN: ",sprintf("%7.1f",MEAN))
    
    
    descriptor <- paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl],"/",ref_site,sep="")
    
    subString  <- paste("Data:",strftime(min(data$date[id_ok]),"%Y-%m-%d",tz="UTC"),"-",strftime(max(data$date[id_ok]),"%Y-%m-%d",tz="UTC"))
    
    figname <- paste(resultdir,paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_",
                                     strftime(tbl_depl$Date_UTC_from[ith_depl],"%Y%m%d",tz="UTC"),"-",strftime(tbl_depl$Date_UTC_to[ith_depl],"%Y%m%d",tz="UTC"),
                                     "_REF_",ref_site,sep=""),"_SCATTER.pdf",sep="")
    
    def_par <- par()
    pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1.25,1,0.1,0.1),mfrow=c(1,2))
    
    xyrange <- range(c(data$CO2[id_ok],data$HPP_CO2[id_ok]))
    
    plot(data$CO2,data$HPP_CO2,pch=16,cex=0.75,cex.lab=1.25,cex.axis=1.25,
         xlim=xyrange,ylim=xyrange,
         xlab=paste(ref_site," CO2 [ppm]",sep=""),
         ylab=paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl]," CO2 [ppm]",sep=""),
         sub=subString)
    
    lines(c(0,1e5),c(0,1e5),col=2,lwd=1,lty=1)
    
    par(family="mono")
    legend("topleft",legend=c(str_00,str_01),bg="white")
    par(family="")
    
    #
    
    hist(data$HPP_CO2[id_ok]-data$CO2[id_ok],seq(-1e4,1e4,2.5),xlim=c(-100,100),col="slategray",
         cex.lab=1.25,cex.axis=1.25,
         xlab=paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl]," - ",ref_site," CO2 [ppm]",sep=""),
         main="")
    
    lines(c(0,0),c(-1e5,1e5),col=2,lwd=1,lty=1)
    
    par(family="mono")
    legend("topleft",legend=c(str_10,str_11,str_12,str_13,str_14,str_15),bg="white")
    par(family="")
    
    dev.off()
    par(def_par)
    
    
    ### TS CO2
    
    figname <- paste(resultdir,paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_",
                                     strftime(tbl_depl$Date_UTC_from[ith_depl],"%Y%m%d",tz="UTC"),"-",strftime(tbl_depl$Date_UTC_to[ith_depl],"%Y%m%d",tz="UTC"),
                                     "_REF_",ref_site,sep=""),"_TS_WEEK.pdf",sep="")
    
    yyy     <- cbind(data$CO2,
                     data$HPP_CO2)
    
    xlabString <- "Date" 
    ylabString <- expression(paste("CO"[2]*" [ppm]"))
    legend_str <- c(ref_site,paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl],sep=""))
    plot_ts(figname,data$date,yyy,"week",NULL,c(350,650),xlabString,ylabString,legend_str)
    
    
    ### TS CO2
    
    figname <- paste(resultdir,paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_",
                                     strftime(tbl_depl$Date_UTC_from[ith_depl],"%Y%m%d",tz="UTC"),"-",strftime(tbl_depl$Date_UTC_to[ith_depl],"%Y%m%d",tz="UTC"),
                                     "_REF_",ref_site,sep=""),"_TS_WEEK.pdf",sep="")
    
    yyy     <- cbind(data$CO2,
                     data$HPP_CO2)
    
    xlabString <- "Date" 
    ylabString <- expression(paste("CO"[2]*" [ppm]"))
    legend_str <- c(ref_site,paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl],sep=""))
    plot_ts(figname,data$date,yyy,"week",NULL,c(350,650),xlabString,ylabString,legend_str)
    
    #

    figname <- paste(resultdir,paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_",
                                     strftime(tbl_depl$Date_UTC_from[ith_depl],"%Y%m%d",tz="UTC"),"-",strftime(tbl_depl$Date_UTC_to[ith_depl],"%Y%m%d",tz="UTC"),
                                     "_REF_",ref_site,sep=""),"_TS_ALL.pdf",sep="")
    
    plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(350,650),xlabString,ylabString,legend_str)
    
    
    
    ### SCATTER H2O
    
    str_00 <- paste("RMSE:", sprintf("%6.2f",RMSE_H2O))
    str_01 <- paste("N:   ", sprintf("%6.0f",n_id_ok))
    
    str_10 <- paste("Q000: ",sprintf("%7.2f",Q000_H2O))
    str_11 <- paste("Q005: ",sprintf("%7.2f",Q005_H2O))
    str_12 <- paste("Q050: ",sprintf("%7.2f",Q050_H2O))
    str_13 <- paste("Q095: ",sprintf("%7.2f",Q095_H2O))
    str_14 <- paste("Q100: ",sprintf("%7.2f",Q100_H2O))
    str_15 <- paste("MEAN: ",sprintf("%7.2f",MEAN_H2O))
    
    
    descriptor <- paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl],"/",ref_site,sep="")
    
    subString  <- paste("Data:",strftime(min(data$date[id_ok]),"%Y-%m-%d",tz="UTC"),"-",strftime(max(data$date[id_ok]),"%Y-%m-%d",tz="UTC"))
    
    figname <- paste(resultdir,paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_",
                                     strftime(tbl_depl$Date_UTC_from[ith_depl],"%Y%m%d",tz="UTC"),"-",strftime(tbl_depl$Date_UTC_to[ith_depl],"%Y%m%d",tz="UTC"),
                                     "_REF_",ref_site,sep=""),"_H2O_SCATTER.pdf",sep="")
    
    def_par <- par()
    pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1.25,1,0.1,0.1),mfrow=c(1,2))
    
    xyrange <- range(c(data$H2O[id_ok],data$HPP_H2O[id_ok]))
    
    plot(data$H2O,data$HPP_H2O,pch=16,cex=0.75,cex.lab=1.25,cex.axis=1.25,
         xlim=xyrange,ylim=xyrange,
         xlab=paste(ref_site," H2O [Vol-%]",sep=""),
         ylab=paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl]," H2O [Vol-%]",sep=""),
         sub=subString)
    
    lines(c(0,1e5),c(0,1e5),col=2,lwd=1,lty=1)
    
    par(family="mono")
    legend("topleft",legend=c(str_00,str_01),bg="white")
    par(family="")
    
    #
    
    hist(data$HPP_H2O[id_ok]-data$H2O[id_ok],seq(-10,50,0.025),xlim=c(-1,1),col="slategray",
         cex.lab=1.25,cex.axis=1.25,
         xlab=paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl]," - ",ref_site," H2O [Vol-%]",sep=""),
         main="")
    
    lines(c(0,0),c(-1e5,1e5),col=2,lwd=1,lty=1)
    
    par(family="mono")
    legend("topleft",legend=c(str_10,str_11,str_12,str_13,str_14,str_15),bg="white")
    par(family="")
    
    dev.off()
    par(def_par)
    

    
    ### TS H2O
    
    figname <- paste(resultdir,paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_",
                                     strftime(tbl_depl$Date_UTC_from[ith_depl],"%Y%m%d",tz="UTC"),"-",strftime(tbl_depl$Date_UTC_to[ith_depl],"%Y%m%d",tz="UTC"),
                                     "_REF_",ref_site,sep=""),"_H2O_TS_WEEK.pdf",sep="")
    
    yyy     <- cbind(data$H2O,
                     data$HPP_H2O)
    
    xlabString <- "Date" 
    ylabString <- expression(paste("H"[2]*"O [Vol-%]"))
    legend_str <- c(ref_site,paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl],sep=""))
    plot_ts(figname,data$date,yyy,"week",NULL,c(0,3),xlabString,ylabString,legend_str)
    
    #

    figname <- paste(resultdir,paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_",
                                     strftime(tbl_depl$Date_UTC_from[ith_depl],"%Y%m%d",tz="UTC"),"-",strftime(tbl_depl$Date_UTC_to[ith_depl],"%Y%m%d",tz="UTC"),
                                     "_REF_",ref_site,sep=""),"_H2O_TS_ALL.pdf",sep="")
    
    plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(0,3),xlabString,ylabString,legend_str)
    
    
    ### HIST CO2/H2O wind situation
    
    if(ANALYSE_WINDSIT){
      
      if(tbl_depl$LocationName[ith_depl]!="RECK"){
        id_AE_SU <- which(AE_SU$LocationName     == tbl_depl$LocationName[ith_depl] 
                          & AE_SU$CO2_REF_SITE   == ref_site
                          & AE_SU$timestamp_from >= tbl_depl$timestamp_from[ith_depl]
                          & AE_SU$timestamp_to   <= tbl_depl$timestamp_to[ith_depl])
      }
      if(tbl_depl$LocationName[ith_depl]=="RECK"){
        id_AE_SU <- which(AE_SU$LocationName     == "REH" 
                          & AE_SU$CO2_REF_SITE   == ref_site
                          & AE_SU$timestamp_from >= tbl_depl$timestamp_from[ith_depl]
                          & AE_SU$timestamp_to   <= tbl_depl$timestamp_to[ith_depl])
      }
      
      n_id_AE_SU <- length(id_AE_SU)
      
      if(n_id_AE_SU==0){
        next
      }else{
        if(!dir.exists(paste(resultdir,"ANALYSE_WINDSIT",sep=""))){
          dir.create(paste(resultdir,"ANALYSE_WINDSIT",sep=""))
        }
      }
    }
    
    
    if(ANALYSE_WINDSIT){
      
      windy <- rep(F,dim(data)[1])
      
      for(ith_ws in 1:n_id_AE_SU){
        id_windy <- which(data$timestamp >= AE_SU$timestamp_from[id_AE_SU[ith_ws]] & data$timestamp <= AE_SU$timestamp_to[id_AE_SU[ith_ws]])
        if(length(id_windy)>0){
          windy[id_windy] <- T
        }
      }
    }
    
    if(ANALYSE_WINDSIT & sum(windy)>0){
      
      
      id_ok_ws   <- which(!is.na(data$CO2) & !is.na(data$HPP_CO2) & windy)
      n_id_ok_ws <- length(id_ok_ws)
      
      RMSE_ws     <- sqrt( sum( (data$CO2[id_ok_ws]-data$HPP_CO2[id_ok])^2 ) / n_id_ok_ws)
      RMSE_H2O_ws <- sqrt( sum( (data$H2O[id_ok_ws]-data$HPP_H2O[id_ok])^2 ) / n_id_ok_ws)
      
      Q000_ws <- quantile(data$HPP_CO2[id_ok_ws]-data$CO2[id_ok_ws],probs=0.00)
      Q005_ws <- quantile(data$HPP_CO2[id_ok_ws]-data$CO2[id_ok_ws],probs=0.05)
      Q050_ws <- quantile(data$HPP_CO2[id_ok_ws]-data$CO2[id_ok_ws],probs=0.50)
      Q095_ws <- quantile(data$HPP_CO2[id_ok_ws]-data$CO2[id_ok_ws],probs=0.95)
      Q100_ws <- quantile(data$HPP_CO2[id_ok_ws]-data$CO2[id_ok_ws],probs=1.00)
      MEAN_ws <- mean(data$HPP_CO2[id_ok_ws]-data$CO2[id_ok_ws])
      
      Q000_H2O_ws <- quantile(data$HPP_H2O[id_ok_ws]-data$H2O[id_ok_ws],probs=0.00)
      Q005_H2O_ws <- quantile(data$HPP_H2O[id_ok_ws]-data$H2O[id_ok_ws],probs=0.05)
      Q050_H2O_ws <- quantile(data$HPP_H2O[id_ok_ws]-data$H2O[id_ok_ws],probs=0.50)
      Q095_H2O_ws <- quantile(data$HPP_H2O[id_ok_ws]-data$H2O[id_ok_ws],probs=0.95)
      Q100_H2O_ws <- quantile(data$HPP_H2O[id_ok_ws]-data$H2O[id_ok_ws],probs=1.00)
      MEAN_H2O_ws <- mean(data$HPP_H2O[id_ok_ws]-data$H2O[id_ok_ws])
      
      
      # HIST H2O wind situation
      
      str_00 <- paste("RMSE: ",sprintf("%7.2f",RMSE_H2O_ws))
      str_01 <- paste("N:    ",sprintf("%7.0f",n_id_ok_ws))
      str_10 <- paste("Q000: ",sprintf("%7.2f",Q000_H2O_ws))
      str_11 <- paste("Q005: ",sprintf("%7.2f",Q005_H2O_ws))
      str_12 <- paste("Q050: ",sprintf("%7.2f",Q050_H2O_ws))
      str_13 <- paste("Q095: ",sprintf("%7.2f",Q095_H2O_ws))
      str_14 <- paste("Q100: ",sprintf("%7.2f",Q100_H2O_ws))
      str_15 <- paste("MEAN: ",sprintf("%7.2f",MEAN_H2O_ws))
      
      
      descriptor <- paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl],"/",ref_site,sep="")
      
      subString  <- paste("Data:",strftime(min(data$date[id_ok]),"%Y-%m-%d",tz="UTC"),"-",strftime(max(data$date[id_ok]),"%Y-%m-%d",tz="UTC"))
      
      figname <- paste(resultdir,"ANALYSE_WINDSIT/",paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_",
                                       strftime(tbl_depl$Date_UTC_from[ith_depl],"%Y%m%d",tz="UTC"),"-",strftime(tbl_depl$Date_UTC_to[ith_depl],"%Y%m%d",tz="UTC"),
                                       "_REF_",ref_site,sep=""),"_H2O_SCATTER_WS.pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=6, height=6, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.25,1,0.1,0.1),mfrow=c(1,1))
      
      hist(data$HPP_H2O[id_ok]-data$H2O[id_ok],seq(-10,50,0.025),xlim=c(-0.75,0.75),col="gray70",
           cex.lab=1.25,cex.axis=1.25,
           xlab=paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl]," - ",ref_site," H2O [Vol-%]",sep=""),
           main="")
      
      hist(data$HPP_H2O[id_ok_ws]-data$H2O[id_ok_ws],seq(-10,50,0.025),xlim=c(-0.75,0.75),col="gray30",
           cex.lab=1.25,cex.axis=1.25,
           xlab="",ylab="",main="",add=T)
      
      lines(c(0,0),c(-1e5,1e5),col=2,lwd=1,lty=1)
      
      par(family="mono")
      legend("topleft",legend=c(str_10,str_11,str_12,str_13,str_14,str_00,str_01),bg="white")
      par(family="")
      
      dev.off()
      par(def_par)
      
      # HIST CO2 wind situation
      
      str_00 <- paste("RMSE: ",sprintf("%7.2f",RMSE_ws))
      str_01 <- paste("N:    ",sprintf("%7.0f",n_id_ok_ws))
      str_10 <- paste("Q000: ",sprintf("%7.2f",Q000_ws))
      str_11 <- paste("Q005: ",sprintf("%7.2f",Q005_ws))
      str_12 <- paste("Q050: ",sprintf("%7.2f",Q050_ws))
      str_13 <- paste("Q095: ",sprintf("%7.2f",Q095_ws))
      str_14 <- paste("Q100: ",sprintf("%7.2f",Q100_ws))
      str_15 <- paste("MEAN: ",sprintf("%7.2f",MEAN_ws))
      
      
      descriptor <- paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl],"/",ref_site,sep="")
      
      subString  <- paste("Data:",strftime(min(data$date[id_ok]),"%Y-%m-%d",tz="UTC"),"-",strftime(max(data$date[id_ok]),"%Y-%m-%d",tz="UTC"))
      
      figname <- paste(resultdir,"ANALYSE_WINDSIT/",paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_",
                                                          strftime(tbl_depl$Date_UTC_from[ith_depl],"%Y%m%d",tz="UTC"),"-",strftime(tbl_depl$Date_UTC_to[ith_depl],"%Y%m%d",tz="UTC"),
                                                          "_REF_",ref_site,sep=""),"_SCATTER_WS.pdf",sep="")
      
      def_par <- par()
      pdf(file = figname, width=6, height=6, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1.25,1,0.1,0.1),mfrow=c(1,1))
      
      hist(data$HPP_CO2[id_ok]-data$CO2[id_ok],seq(-1e4,1e4,2.0),xlim=c(-50,50),col="gray70",
           cex.lab=1.25,cex.axis=1.25,
           xlab=paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl]," - ",ref_site," CO2 [Vol-%]",sep=""),
           main="")
      
      hist(data$HPP_CO2[id_ok_ws]-data$CO2[id_ok_ws],seq(-1e4,1e4,2.0),xlim=c(-50,50),col="gray30",
           cex.lab=1.25,cex.axis=1.25,
           xlab="",ylab="",main="",add=T)
      
      lines(c(0,0),c(-1e5,1e5),col=2,lwd=1,lty=1)
      
      par(family="mono")
      legend("topleft",legend=c(str_10,str_11,str_12,str_13,str_14,str_00,str_01),bg="white")
      par(family="")
      
      dev.off()
      par(def_par)
    }
  }
  }
}


### ----------------------------------------------------------------------------------------------------------------------------

write.table(statistics,paste(resultdir,"statistics.csv",sep=""),col.names = T,row.names = F,sep=";")

### ----------------------------------------------------------------------------------------------------------------------------



