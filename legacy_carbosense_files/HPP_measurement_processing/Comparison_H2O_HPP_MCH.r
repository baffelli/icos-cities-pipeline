# Comparison_H2O_HPP_MCH.r
# -----------------------------

# Remarks:
# - Comparison of H2O data from HPP and MCH instruments.
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

### ----------------------------------------------------------------------------------------------------------------------------

ma <- function(x,n=10){filter(x,rep(1/n,n), sides=1)}

### ----------------------------------------------------------------------------------------------------------------------------

HPP_ProcDataTblName <- "CarboSense_HPP_CO2"

### ----------------------------------------------------------------------------------------------------------------------------

query_str         <- paste("SELECT * FROM Deployment WHERE SensorUnit_ID BETWEEN 426 AND 445 and LocationName NOT IN ('DUE1','DUE2','DUE3','DUE4','DUE5','MET1','DUE5');",sep="")
drv               <- dbDriver("MySQL")
con <-carboutil::get_conn()
res               <- dbSendQuery(con, query_str)
tbl_depl          <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)

tbl_depl$Date_UTC_from  <- strptime(tbl_depl$Date_UTC_from, "%Y-%m-%d %H:%M:%S", tz="UTC")
tbl_depl$Date_UTC_to    <- strptime(tbl_depl$Date_UTC_to,   "%Y-%m-%d %H:%M:%S", tz="UTC")

tbl_depl$timestamp_from <- as.numeric(difftime(time1=tbl_depl$Date_UTC_from,time2=strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S", tz="UTC"),units="secs",tz="UTC"))
tbl_depl$timestamp_to   <- as.numeric(difftime(time1=tbl_depl$Date_UTC_to,  time2=strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S", tz="UTC"),units="secs",tz="UTC"))

###

query_str         <- paste("SELECT * FROM Location;",sep="")
drv               <- dbDriver("MySQL")
con <-carboutil::get_conn()
res               <- dbSendQuery(con, query_str)
tbl_loc           <- dbFetch(res, n=-1)
dbClearResult(res)
dbDisconnect(con)


### ----------------------------------------------------------------------------------------------------------------------------

resultdir         <- "/project/CarboSense/Carbosense_Network/HPP_PerformanceAnalysis/Comparison_HPP_MCH/"

if(!dir.exists(resultdir)){
  dir.create(resultdir)
}

#

statistics        <- NULL


### ----------------------------------------------------------------------------------------------------------------------------

for(ith_depl in 1:dim(tbl_depl)[1]){

  # Compare H2O measurements
  # ------------------------
  
  
  query_str         <- paste("SELECT timestamp, H2O as HPP_H2O FROM ",HPP_ProcDataTblName," ",sep="")
  query_str         <- paste(query_str, "WHERE LocationName = '",tbl_depl$LocationName[ith_depl],"' AND SensorUnit_ID = ",tbl_depl$SensorUnit_ID[ith_depl]," ",sep="")
  query_str         <- paste(query_str, "AND CO2_CAL_ADJ != -999 AND Valve = 0 ",sep="")
  query_str         <- paste(query_str, "AND timestamp >= ",tbl_depl$timestamp_from[ith_depl]," AND timestamp <= ",tbl_depl$timestamp_to[ith_depl],";",sep="")
  
  drv               <- dbDriver("MySQL")
  con <-carboutil::get_conn()
  res               <- dbSendQuery(con, query_str)
  tbl_HPP           <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  if(dim(tbl_HPP)[1]==0){
    next
  }
  
  tbl_HPP           <- tbl_HPP[order(tbl_HPP$timestamp),]
  tbl_HPP$date      <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tbl_HPP$timestamp
  tbl_HPP           <- timeAverage(mydata = tbl_HPP,avg.time = "10 min", statistic = "mean",start.date = strptime(strftime(min(tbl_HPP$date),"%Y%m%d000000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")-600)
  tbl_HPP$timestamp <- as.numeric(difftime(time1=tbl_HPP$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))
  
  #
  
  id_LOC_HPP   <- which(tbl_loc$LocationName == tbl_depl$LocationName[ith_depl] & tbl_depl$Date_UTC_from[ith_depl] >= tbl_loc$Date_UTC_from)
  print(tbl_depl$LocationName[ith_depl])
  #tbl_loc$Date_UTC_from <= tbl_depl$Date_UTC_from[ith_depl] &
  #tbl_loc$Date_UTC_to >= tbl_depl$Date_UTC_to[ith_depl])
  id_LOC_MCH   <- which(tbl_loc$Network == "METEOSWISS")


  
  id_dist_ok   <- which(sqrt((tbl_loc$Y_LV03[id_LOC_MCH]-tbl_loc$Y_LV03[id_LOC_HPP])^2 + (tbl_loc$X_LV03[id_LOC_MCH]-tbl_loc$X_LV03[id_LOC_HPP])^2) < 15000)
  
  #
  
  query_str         <- paste("SELECT DISTINCT LocationName FROM METEOSWISS_Measurements ",sep="")
  query_str         <- paste(query_str, "WHERE LocationName IN ('",paste(tbl_loc$LocationName[id_LOC_MCH[id_dist_ok]],collapse = "','"),"') ",sep="")
  query_str         <- paste(query_str, "AND timestamp >= ",tbl_depl$timestamp_from[ith_depl]," AND timestamp <= ",tbl_depl$timestamp_to[ith_depl]," ",sep="")
  query_str         <- paste(query_str, "AND RH != -999 AND Pressure != -999 AND Temperature != -999;",sep="")
  drv               <- dbDriver("MySQL")
  con <-carboutil::get_conn()
  res               <- dbSendQuery(con, query_str)
  tbl_MCH_sites     <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  #
  
  for(ith_MCH_site in 1:dim(tbl_MCH_sites)[1]){
    
    
    query_str         <- paste("SELECT timestamp, RH, Temperature,Pressure FROM METEOSWISS_Measurements ",sep="")
    query_str         <- paste(query_str, "WHERE LocationName = '",tbl_MCH_sites$LocationName[ith_MCH_site],"' ", sep="")
    query_str         <- paste(query_str, "AND timestamp >= ",tbl_depl$timestamp_from[ith_depl]," and timestamp <= ",tbl_depl$timestamp_to[ith_depl]," ", sep="")
    query_str         <- paste(query_str, "AND RH != -999 and Pressure != -999 and Temperature != -999;",sep="")
    drv               <- dbDriver("MySQL")
    con <-carboutil::get_conn()
    res               <- dbSendQuery(con, query_str)
    MCH_data          <- dbFetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    # Compute H2O [vol-%]
    
    coef_1 <-  -7.85951783
    coef_2 <-   1.84408259
    coef_3 <-  -11.7866497
    coef_4 <-   22.6807411
    coef_5 <-  -15.9618719
    coef_6 <-   1.80122502
    
    theta     <- 1 - (273.15+MCH_data$Temperature)/647.096
    Pws       <- 220640 * exp( 647.096/(273.15+MCH_data$Temperature) * (coef_1*theta + coef_2*theta^1.5 + coef_3*theta^3 + coef_4*theta^3.5 + coef_5*theta^4 + coef_6*theta^7.5))
    Pw        <- MCH_data$RH*(Pws*100)/100
    
    MCH_data$H2O  <- Pw / (MCH_data$Pressure*1e2)*1e2
    
    #
    
    data <- merge(tbl_HPP,MCH_data,by="timestamp")
    
    #
    
    id_ok   <- which(!is.na(data$H2O) & !is.na(data$HPP_H2O))
    n_id_ok <- length(id_ok)
    
    if(n_id_ok==0){
      print(id_dist_ok)
      print(tbl_MCH_sites$LocationName[ith_MCH_site])
      print(tbl_HPP)
      print(data)
    }
    


    RMSE    <- sqrt( sum( (data$H2O[id_ok]-data$HPP_H2O[id_ok])^2 ) / n_id_ok)
    corCoef <- cor(x = data$H2O[id_ok],y = data$HPP_H2O[id_ok],use = "complete.obs",method = "pearson")
    
    Q000    <- quantile(data$HPP_H2O[id_ok]-data$H2O[id_ok],probs=0.00)
    Q005    <- quantile(data$HPP_H2O[id_ok]-data$H2O[id_ok],probs=0.05)
    Q050    <- quantile(data$HPP_H2O[id_ok]-data$H2O[id_ok],probs=0.50)
    Q095    <- quantile(data$HPP_H2O[id_ok]-data$H2O[id_ok],probs=0.95)
    Q100    <- quantile(data$HPP_H2O[id_ok]-data$H2O[id_ok],probs=1.00)
    MEAN    <- mean(data$HPP_H2O[id_ok]-data$H2O[id_ok])
    
    
    
    statistics <- rbind(statistics,data.frame(SensorUnit_ID = tbl_depl$SensorUnit_ID[ith_depl],
                                              LocationName  = tbl_depl$LocationName[ith_depl],
                                              MCH_SITE      = tbl_MCH_sites$LocationName[ith_MCH_site],
                                              Date_UTC_from = strftime(min(data$date[id_ok]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                              Date_UTC_to   = strftime(max(data$date[id_ok]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                              RMSE          = RMSE,
                                              COR           = corCoef,
                                              MEAN_DIFF     = MEAN,
                                              N             = n_id_ok,
                                              Q000          = Q000,
                                              Q005          = Q005,
                                              Q050          = Q050,
                                              Q095          = Q095,
                                              Q100          = Q100,
                                              stringsAsFactors = F))
    
    
    
    ### SCATTER H2O
    
    str_00 <- paste("RMSE:", sprintf("%6.2f",RMSE))
    str_01 <- paste("COR: ", sprintf("%6.2f",corCoef))
    str_02 <- paste("N:   ", sprintf("%6.0f",n_id_ok))
    
    str_10 <- paste("Q000: ",sprintf("%7.2f",Q000))
    str_11 <- paste("Q005: ",sprintf("%7.2f",Q005))
    str_12 <- paste("Q050: ",sprintf("%7.2f",Q050))
    str_13 <- paste("Q095: ",sprintf("%7.2f",Q095))
    str_14 <- paste("Q100: ",sprintf("%7.2f",Q100))
    str_15 <- paste("MEAN: ",sprintf("%7.2f",MEAN))
    
    
    descriptor <- paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl],"/",tbl_MCH_sites$LocationName[ith_MCH_site],sep="")
    
    subString  <- paste("Data:",strftime(min(data$date[id_ok]),"%Y-%m-%d",tz="UTC"),"-",strftime(max(data$date[id_ok]),"%Y-%m-%d",tz="UTC"))
    
    figname <- paste(resultdir,paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_MCH_",tbl_MCH_sites$LocationName[ith_MCH_site],sep=""),"_H2O_SCATTER.pdf",sep="")
    
    def_par <- par()
    pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
    par(mai=c(1.25,1,0.1,0.1),mfrow=c(1,2))
    
    xyrange <- range(c(data$H2O[id_ok],data$HPP_H2O[id_ok]))
    
    plot(data$H2O,data$HPP_H2O,pch=16,cex=0.75,cex.lab=1.25,cex.axis=1.25,
         xlim=xyrange,ylim=xyrange,
         xlab=paste(tbl_MCH_sites$LocationName[ith_MCH_site]," H2O [Vol-%]",sep=""),
         ylab=paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl]," H2O [Vol-%]",sep=""),
         sub=subString)
    
    lines(c(0,1e5),c(0,1e5),col=2,lwd=1,lty=1)
    
    par(family="mono")
    legend("topleft",legend=c(str_00,str_01),bg="white")
    par(family="")
    
    #
    
    hist(data$HPP_H2O[id_ok]-data$H2O[id_ok],seq(-10,50,0.025),xlim=c(-1,1),col="slategray",
         cex.lab=1.25,cex.axis=1.25,
         xlab=paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl]," - ",tbl_MCH_sites$LocationName[ith_MCH_site]," H2O [Vol-%]",sep=""),
         main="")
    
    lines(c(0,0),c(-1e5,1e5),col=2,lwd=1,lty=1)
    
    par(family="mono")
    legend("topleft",legend=c(str_10,str_11,str_12,str_13,str_14,str_15),bg="white")
    par(family="")
    
    dev.off()
    par(def_par)
    
    
    ### TS H2O
    
    figname <- paste(resultdir,paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_MCH_",tbl_MCH_sites$LocationName[ith_MCH_site],sep=""),"_H2O_TS_WEEK.pdf",sep="")
    
    yyy     <- cbind(data$H2O,
                     data$HPP_H2O)
    
    xlabString <- "Date" 
    ylabString <- expression(paste("H"[2]*"O [Vol-%]"))
    legend_str <- c(tbl_MCH_sites$LocationName[ith_MCH_site],paste(tbl_depl$LocationName[ith_depl]," ",tbl_depl$SensorUnit_ID[ith_depl],sep=""))
    plot_ts(figname,data$date,yyy,"week",NULL,c(0,3),xlabString,ylabString,legend_str)
    
    #
    
    figname <- paste(resultdir,paste(tbl_depl$LocationName[ith_depl],"_",tbl_depl$SensorUnit_ID[ith_depl],"_MCH_",tbl_MCH_sites$LocationName[ith_MCH_site],sep=""),"_H2O_TS_ALL.pdf",sep="")
    
    plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(0,3),xlabString,ylabString,legend_str)
  }
}

### ----------------------------------------------------------------------------------------------------------------------------

write.table(statistics,paste(resultdir,"statistics.csv",sep=""),col.names = T,row.names = F,sep=";")





