# Import_DB_table_NABEL_DUE.r
# ---------------------------
#
# Author: Michael Mueller
#
#
# ---------------------------


## clear variables
rm(list=ls(all=TRUE))
gc()

## ----------------------------------------------------------------------------------------------------------------------

## libraries
library(DBI)
require(RMySQL)
require(chron)
library(dplyr)
library(purrr)
## ----------------------------------------------------------------------------------------------------------------------

## DB information

DB_group <- "CarboSense_MySQL"

## ----------------------------------------------------------------------------------------------------------------------

## Directories

fdirectory <- "K:/Nabel/Daten/Stationen/DUE/"
fdirectory <- "/project/CarboSense/Win_K/Daten/Stationen/DUE/"

## ----------------------------------------------------------------------------------------------------------------------

## NABEL DUE Data

# - automatically downloaded data files [directory: "K:/Nabel/Daten/Stationen/DUE"]
# - data files contain one minute data [date, values of NABEL measurement programm at DUE plus respective flags]
# - date refers to CET




if(T){
  print("Loading NABEL files")
  files   <- list.files(path=fdirectory,pattern = "CarboSens_",full.names = T)

  #Get dates of data in DB
  con <- carboutil::get_conn(group=DB_group)
  dates_in_db <- collect(tbl(con, sql("SELECT DISTINCT DATE(FROM_UNIXTIME(timestamp)) AS date FROM NABEL_DUE")))$date

  #Get dates of files
  dates_in_files <- map(files, function(x) lubridate::as_date(stringr::str_extract(x,"\\d{6}")))

  #Get dates to load
  files_to_load <-  match(setdiff(dates_in_files, dates_in_db), dates_in_files)

  n_files <- length(files_to_load)
  print(paste("In total", n_files, " files must be loaded"))
  for(ith_file in files_to_load){
    print(paste("Loading NABEL file:", files[ith_file]))
    data <- read.table(file = files[ith_file],header = F,sep = ";",skip = 4)
    colnames(data) <- c("timestamp","O3","O3_F","NO2","NO2_F","NO","NO_F","CO","CO_F","PM10_kont","PM10_kont_F","BabsM_25","BabsM_25_F","T","T_F","RH","RH_F","STRGLO","STRGLO_F","rain","rain_F","pressure","pressure_F","WIRI1","WIRI1_F","WIGE1","WIGE1_F","WIGEM1","WIGEM1_F","WIRI2","WIRI2_F","WIGE2","WIGE2_F","WIGEM2","WIGEM2_F")
    
    # Date (CET -> UTC; end of Interval -> begin of interval)
    data$timestamp <- strptime(data$timestamp,"%d.%m.%Y %H:%M",tz="UTC") - 3600 - 60
    data$timestamp <- as.numeric(difftime(time1=data$timestamp,time2=strptime("01.01.1970 00:00:00","%d.%m.%Y %H:%M:%S",tz="UTC"),tz="UTC",units="secs"))
    
    
    # Check data
    u_cn   <- colnames(data)
    u_cn   <- gsub(pattern = "_F",replacement = "",u_cn)
    u_cn   <- sort(unique(u_cn[which(u_cn!="timestamp")]))
    n_u_cn <- length(u_cn)
    
    for(ith_u_cn in 1:n_u_cn){
      pos   <- which(colnames(data)==u_cn[ith_u_cn])
      pos_F <- which(colnames(data)==paste(u_cn[ith_u_cn],"_F",sep=""))
      
      if(length(pos)==0 | length(pos_F)==0){
        stop()
      }
      
      id_ok <- which(data[,pos]!=""
                     & data[,pos]!= -999
                     & !is.na(data[,pos])
                     & (data[,pos_F]=="" | is.na(data[,pos_F])))
      
      data[,pos_F]      <- 0
      data[id_ok,pos_F] <- 1
      
      data[!(1:dim(data)[1])%in%id_ok,pos] <- -999
      
      
      if(length(which(is.na(data[,pos])))>0){
        print(paste(u_cn[ith_u_cn],": Import error 1!"))
      }
      if(length(which(is.na(data[,pos_F])))>0){
        print(paste(u_cn[ith_u_cn],": Import error 2!"))
      }
      if(length(which(!data[,pos_F]%in%c(0,1)))>0){
        print(paste(u_cn[ith_u_cn],": Import error 3!"))
      }
      if(length(which(data[,pos]==-999 & data[,pos_F]!=0))>0){
        print(paste(u_cn[ith_u_cn],": Import error 4!"))
      }
    }
    
    #
    
    data <- as.data.frame(data,stringsAsFactors=F)
    
    
    # insert data into DB
    
    query_str <- "SELECT MAX(timestamp) AS MAX_timestamp FROM NABEL_DUE WHERE NO2!=-999 or O3!=-999;";
    drv             <- dbDriver("MySQL")
    con<-carboutil::get_conn(group=DB_group)
    res             <- dbSendQuery(con, query_str)
    MAX_timestamp   <- dbFetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    MAX_timestamp <- as.integer(MAX_timestamp)
    
    if(is.na(MAX_timestamp)){
      MAX_timestamp <- 0
    }
    
    #
    
    id_insert <- which(data$timestamp>MAX_timestamp)
    
    if(length(id_insert)>0){
      
      query_str <- paste("INSERT INTO NABEL_DUE (",paste(colnames(data)[1:35],collapse = ","),") ",sep="")
      query_str <- paste(query_str,"VALUES" )
      query_str <- paste(query_str,
                         paste("(",paste(data[id_insert,01],",",
                                         data[id_insert,02],",",
                                         data[id_insert,03],",",
                                         data[id_insert,04],",",
                                         data[id_insert,05],",",
                                         data[id_insert,06],",",
                                         data[id_insert,07],",",
                                         data[id_insert,08],",",
                                         data[id_insert,09],",",
                                         data[id_insert,10],",",
                                         data[id_insert,11],",",
                                         data[id_insert,12],",",
                                         data[id_insert,13],",",
                                         data[id_insert,14],",",
                                         data[id_insert,15],",",
                                         data[id_insert,16],",",
                                         data[id_insert,17],",",
                                         data[id_insert,18],",",
                                         data[id_insert,19],",",
                                         data[id_insert,20],",",
                                         data[id_insert,21],",",
                                         data[id_insert,22],",",
                                         data[id_insert,23],",",
                                         data[id_insert,24],",",
                                         data[id_insert,25],",",
                                         data[id_insert,26],",",
                                         data[id_insert,27],",",
                                         data[id_insert,28],",",
                                         data[id_insert,29],",",
                                         data[id_insert,30],",",
                                         data[id_insert,31],",",
                                         data[id_insert,32],",",
                                         data[id_insert,33],",",
                                         data[id_insert,34],",",
                                         data[id_insert,35],
                                         collapse = "),(",sep=""),")",sep=""),
                         paste(" ON DUPLICATE KEY UPDATE "))
      
      for(ii in 1:35){
        if(ii==35){
          query_str <- paste(query_str,paste(colnames(data)[ii],"=VALUES(",colnames(data)[ii],"); ",sep=""))
        }else{
          query_str <- paste(query_str,paste(colnames(data)[ii],"=VALUES(",colnames(data)[ii],"), ",sep=""))
        }
      }
      
      
      
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn(group=DB_group)
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
  }
  
  rm(data,query_str,id_insert)
  gc()
  
}


## ----------------------------------------------------------------------------------------------------------------------

## Picarro CO2 DUE Data

# - manually downloaded data files [directory: "G:\503_Themen\Immissionen\muem\CarboSense_DUE_Picarro_Dec_16-Jan_17"]
# - data files contain 10 minutes data [date from, date to, CO2, CO2 dry, flag]
# - date refers to CET
# - data shall be used up to 06.01.2017 20:10 [CET]

if(F){
  
  files   <- list.files(path = "/project/CarboSense/Win_G/503_Themen/Immissionen/muem/CarboSense_DUE_Picarro_Dec_16-Jan_17",pattern = "_MM10.txt",full.names = T)
  n_files <- length(files)
  
  data_ref <- NULL
  for(ith_file in 1:n_files){
    data_ref <- rbind(data_ref,read.table(file = files[ith_file],sep="",as.is=T))
  }
  
  data_ref             <- data_ref[!duplicated(data_ref),]
  data_ref             <- data_ref[order(data_ref[1],decreasing = F),]
  
  colnames(data_ref)   <- c("date_from","date_to","CO2","CO2_dry","CO2_F")
  
  # timestamp [CET -> UTC]
  data_ref$date_from   <- strptime("19000101000000","%Y%m%d%H%M%S",tz="UTC") + round((data_ref$date_from-2)*86400) - 3600
  data_ref$date_to     <- strptime("19000101000000","%Y%m%d%H%M%S",tz="UTC") + round((data_ref$date_to  -2)*86400) - 3600
  
  
  date_begin           <- min(data_ref$date_from)
  
  if(max(data_ref$date_to)>strptime("20170106190000","%Y%m%d%H%M%S",tz="UTC")){
    date_end <- strptime("20170106190000","%Y%m%d%H%M%S",tz="UTC")
  }else{
    date_end <- max(data_ref$date_to)
  }
  
  data_ref$timestamp   <- as.numeric(difftime(time1=data_ref$date_from,
                                              time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                                              units="secs",
                                              tz="UTC"))
  
  timestamp            <- as.numeric(difftime(time1=seq(date_begin,date_end,by="min"),
                                              time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                                              units="secs",
                                              tz="UTC"))
  
  data_ref_minutes     <- data.frame(timestamp=timestamp,
                                     CO2=NA,
                                     CO2_F=NA)     
  
  # 10 minute values -> 1 minute values
  for(ep in 1:dim(data_ref)[1]){
    
    id   <- which(data_ref_minutes$timestamp>=data_ref$timestamp[ep] & data_ref_minutes$timestamp<(data_ref$timestamp[ep]+600))
    n_id <- length(id)
    
    if(n_id>0){
      data_ref_minutes$CO2[id]  <- data_ref$CO2[ep]
      if(data_ref$CO2_F[ep]==0){
        data_ref_minutes$CO2_F[id] <- 1
      }
    }
  }
  
  # Flag data of period 2017-01-03 to 2017-01-04
  
  t_A <- as.numeric(difftime(time1=strptime("20170103000000","%Y%m%d%H%M%S",tz="UTC"),
                             time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                             units="secs",
                             tz="UTC"))
  t_B <- as.numeric(difftime(time1=strptime("20170104000000","%Y%m%d%H%M%S",tz="UTC"),
                             time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                             units="secs",
                             tz="UTC"))              
  
  id  <- which(data_ref_minutes$timestamp>=t_A & data_ref_minutes$timestamp<t_B)
  
  if(length(id)>0){
    data_ref_minutes$CO2_F[id] <- 0
  }
  
  # import data into database
  
  id_insert <- which(!is.na(data_ref_minutes$timestamp)
                     & !is.na(data_ref_minutes$CO2)
                     & !is.na(data_ref_minutes$CO2_F))
  
  
  if(length(id_insert)>0){
    
    query_str <- paste("INSERT INTO NABEL_DUE (timestamp,CO2,CO2_F) ",sep="")
    query_str <- paste(query_str,"VALUES" )
    query_str <- paste(query_str,
                       paste("(",paste(data_ref_minutes[id_insert,01],",",
                                       data_ref_minutes[id_insert,02],",",
                                       data_ref_minutes[id_insert,03],
                                       collapse = "),(",sep=""),")",sep=""),
                       paste(" ON DUPLICATE KEY UPDATE timestamp=VALUES(timestamp), CO2=VALUES(CO2), CO2_F=VALUES(CO2_F);"))
    
    
    drv             <- dbDriver("MySQL")
    con<-carboutil::get_conn(group=DB_group)
    res             <- dbSendQuery(con, query_str)
    dbClearResult(res)
    dbDisconnect(con)
  }
  #
  
  rm(timestamp,data_ref,data_ref_minutes,id_insert,id,query_str)
  gc()
  
}



## ----------------------------------------------------------------------------------------------------------------------

## Picarro CO2 DUE Data

# - automatically downloaded data files [directory: "K:/Nabel/Daten/Stationen/DUE"]
# - data files contain 1 minute data [date, CO2, CO2 MAX, CO2 MIN, flag]
# - date refers to CET


if(T){
  print("Loading picarro data")
  con <- carboutil::get_conn(group=DB_group)
  dates_in_db <- collect(tbl(con, sql("SELECT DISTINCT DATE(FROM_UNIXTIME(timestamp)) AS date FROM NABEL_DUE WHERE CO2 <> -999")))$date
  files    <- list.files(path = fdirectory, pattern = "DUE Test CO2",full.names = T)

  #Get dates of files
  dates_in_files <- map(files, function(x) lubridate::as_date(stringr::str_extract(x,"\\d{6}")))
  #Get dates to load
  files_to_load <-  match(setdiff(dates_in_files, dates_in_db), dates_in_files)

  files_ok <- unlist(map(files[files_to_load], function(x) file.size(x) > 500))
  

  files   <- files[files_to_load]
  n_files <- length(files)
  
  rm(s,files_ok)
  gc()

  #  
  
  for(ith_file in 1:n_files){
    print(paste("Loading file", files[ith_file], ith_file, "out of", n_files))
    data_ref <- read.table(file = files[ith_file],sep=";",as.is=T,skip=4,header=F)
    
    # Type 1 file
    ok  <- F
    if(dim(data_ref)[2]==4){
      data_ref             <- as.data.frame(data_ref[,1:3],stringsAsFactors=F)
      colnames(data_ref)   <- c("timestamp","CO2","CO2_F")
      data_ref$CO2_MIN     <- -999
      data_ref$CO2_MAX     <- -999
      data_ref$CO2_DRY     <- -999
      data_ref$CO2_MIN_DRY <- -999
      data_ref$CO2_MAX_DRY <- -999
      data_ref$CO2_DRY_F   <- 0
      data_ref$H2O         <- -999
      data_ref$H2O_MIN     <- -999
      data_ref$H2O_MAX     <- -999
      data_ref$H2O_F       <- 0
      data_ref             <- data_ref[,c(1,2,4,5,3,6:13)]
      ok <- T
    }
    
    # Type 2 file
    if(dim(data_ref)[2]==6){
      data_ref             <- as.data.frame(data_ref[,1:5],stringsAsFactors=F)
      colnames(data_ref)   <- c("timestamp","CO2","CO2_MIN","CO2_MAX","CO2_F")
      data_ref$CO2_DRY     <- -999
      data_ref$CO2_MIN_DRY <- -999
      data_ref$CO2_MAX_DRY <- -999
      data_ref$CO2_DRY_F   <- 0
      data_ref$H2O         <- -999
      data_ref$H2O_MIN     <- -999
      data_ref$H2O_MAX     <- -999
      data_ref$H2O_F       <- 0
      ok <- T
    }
    
    # Type 3 file
    if(dim(data_ref)[2]==14){
      data_ref           <- as.data.frame(data_ref[,1:13],stringsAsFactors=F)
      colnames(data_ref) <- c("timestamp","CO2","CO2_MIN","CO2_MAX","CO2_F","CO2_DRY","CO2_DRY_MIN","CO2_DRY_MAX","CO2_DRY_F","H2O","H2O_MIN","H2O_MAX","H2O_F")
      ok <- T
    }
    
    if(!ok){
      stop("Error while importing daily picarro files (unexpected number of columns).")
    }
    
    # timestamp [CET -> UTC; End of interval -> begin of interval]
    data_ref$timestamp      <- as.numeric(difftime(time1=strptime(data_ref$timestamp, "%d.%m.%Y %H:%M",tz="UTC") - 3600 - 60,
                                                   time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                                                   units="secs",
                                                   tz="UTC"))
    
    # Remove rows without numerical CO2 value
    
    id_keep_rows <- which( !(data_ref$CO2 == "" | is.na(data_ref$CO2) ) )
    
    if(length(id_keep_rows)>0){
      data_ref <- data_ref[id_keep_rows,]
    }else{
      stop(paste("Empty file:",files[ith_file]))
    }
    
    # FLAG: CO2
    nok <- (  data_ref$CO2== -999
              | data_ref$CO2== ""
              | is.na(data_ref$CO2)
              | data_ref$CO2_MIN== ""
              | is.na(data_ref$CO2_MIN)
              | data_ref$CO2_MAX== ""
              | is.na(data_ref$CO2_MAX)
              | (data_ref$CO2_F!= "" & !is.na(data_ref$CO2_F) & data_ref$timestamp<1492041600)
              | (data_ref$CO2_F!= "" & !is.na(data_ref$CO2_F) & data_ref$timestamp>1494242580)
              | (data_ref$CO2_F!="Not Ready Meas not active No Gasflow Press not locked " & data_ref$CO2_F!="Not Ready Meas not active No Gasflow Press not locked" & data_ref$timestamp>1492041600 & data_ref$timestamp<1494242580))
    
    if(sum(!nok)>0){
      data_ref$CO2_F[!nok]  <- 1 
    }
    
    if(sum(nok)>0){
      data_ref$CO2[nok]     <- -999
      data_ref$CO2_MIN[nok] <- -999
      data_ref$CO2_MAX[nok] <- -999
      data_ref$CO2_F[nok]   <- 0 
    }
    
    # FLAG: CO2_DRY
    
    nok <- (data_ref$CO2_DRY== -999
            | data_ref$CO2_DRY== ""
            | is.na(data_ref$CO2_DRY)
            | data_ref$CO2_DRY_MIN== ""
            | is.na(data_ref$CO2_DRY_MIN)
            | data_ref$CO2_DRY_MAX== ""
            | is.na(data_ref$CO2_DRY_MAX)
            | (data_ref$CO2_DRY_F!= "" & !is.na(data_ref$CO2_DRY_F)))
    
    if(sum(!nok)>0){
      data_ref$CO2_DRY_F[!nok]  <- 1 
    }
    
    if(sum(nok)>0){
      data_ref$CO2_DRY[nok]     <- -999
      data_ref$CO2_DRY_MIN[nok] <- -999
      data_ref$CO2_DRY_MAX[nok] <- -999
      data_ref$CO2_DRY_F[nok]   <- 0 
    }
    
    # FLAG: H2O
    
    nok <- (data_ref$H2O== -999
            | data_ref$H2O== ""
            | is.na(data_ref$H2O)
            | data_ref$H2O_MIN== ""
            | is.na(data_ref$H2O_MIN)
            | data_ref$H2O_MAX== ""
            | is.na(data_ref$H2O_MAX)
            | (data_ref$H2O_F!= "" & !is.na(data_ref$H2O_F)))
    
    if(sum(!nok)>0){
      data_ref$H2O_F[!nok]  <- 1 
    }
    
    if(sum(nok)>0){
      data_ref$H2O[nok]     <- -999
      data_ref$H2O_MIN[nok] <- -999
      data_ref$H2O_MAX[nok] <- -999
      data_ref$H2O_F[nok]   <- 0 
    }
    
    
    # Special instrument problems
    
    # Cause unknown: indicated by identical subsequent values
    for(ith_row in 5:dim(data_ref)[1]){
      index <- (ith_row-4):(ith_row)
      
      if(sum(!is.na(data_ref$CO2[index]))>=3){
        
        diff_vec <- diff(data_ref$CO2[index])
        diff_vec <- diff_vec[!is.na(diff_vec)]
        if(all(diff_vec==0)){
          data_ref$CO2[index]   <- -999
          data_ref$CO2_F[index] <- 0
        }
      }
    }
    
    # Cause unknown, probably Picarro data transmission issue, leading to biased CO2 1 minute means: CO2_MIN != -999 & CO2_MIN < 390 ppm
    
    id_set_to_NA   <- which(data_ref$CO2_MIN != -999
                            & data_ref$CO2_MIN < 360)
    
    n_id_set_to_NA <- length(id_set_to_NA)
    
    if(n_id_set_to_NA>0){
      data_ref[id_set_to_NA,2:dim(data_ref)[2]] <- NA
    }
    
    rm(id_set_to_NA,n_id_set_to_NA)
    
    
    
    # import into database
    
    query_str <- "SELECT MAX(timestamp) AS MAX_timestamp FROM NABEL_DUE WHERE CO2 != -999;";
    drv             <- dbDriver("MySQL")
    con<-carboutil::get_conn(group=DB_group)
    res             <- dbSendQuery(con, query_str)
    MAX_timestamp   <- dbFetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    MAX_timestamp <- as.integer(MAX_timestamp)
    
    if(is.na(MAX_timestamp)){
      MAX_timestamp <- 0
    }
    
    
    id_insert <- which(!is.na(data_ref$timestamp)
                       & !is.na(data_ref$CO2)
                       & !is.na(data_ref$CO2_F)
                       & !is.na(data_ref$CO2_MIN)
                       & !is.na(data_ref$CO2_MAX)
                       & !is.na(data_ref$CO2_DRY)
                       & !is.na(data_ref$CO2_DRY_F)
                       & !is.na(data_ref$H2O)
                       & !is.na(data_ref$H2O_F)
                       & data_ref$timestamp>MAX_timestamp)
    
    
    if(length(id_insert)>0){
      
      query_str <- paste("INSERT INTO NABEL_DUE (timestamp,CO2,CO2_MIN,CO2_MAX,CO2_F,CO2_DRY,CO2_DRY_F,H2O,H2O_F) ",sep="")
      query_str <- paste(query_str,"VALUES" )
      query_str <- paste(query_str,
                         paste("(",paste(data_ref$timestamp[id_insert],",",
                                         data_ref$CO2[id_insert],",",
                                         data_ref$CO2_MIN[id_insert],",",
                                         data_ref$CO2_MAX[id_insert],",",
                                         data_ref$CO2_F[id_insert],",",
                                         data_ref$CO2_DRY[id_insert],",",
                                         data_ref$CO2_DRY_F[id_insert],",",
                                         data_ref$H2O[id_insert],",",
                                         data_ref$H2O_F[id_insert],
                                         collapse = "),(",sep=""),")",sep=""),
                         paste(" ON DUPLICATE KEY UPDATE timestamp=VALUES(timestamp), CO2=VALUES(CO2), CO2_MIN=VALUES(CO2_MIN), CO2_MAX=VALUES(CO2_MAX), CO2_F=VALUES(CO2_F), CO2_DRY=VALUES(CO2_DRY), CO2_DRY_F=VALUES(CO2_DRY_F), H2O=VALUES(H2O), H2O_F=VALUES(H2O_F);"))
      
      drv             <- dbDriver("MySQL")
      con<-carboutil::get_conn(group=DB_group)
      res             <- dbSendQuery(con, query_str)
      dbClearResult(res)
      dbDisconnect(con)
    }
    
    #
    
    rm(timestamp,data_ref,id_insert,id,query_str)
    gc()
    
  }
}


## ----------------------------------------------------------------------------------------------------------------------

## Complement Picarro CO2_DRY and H2O for DUE Data

# - manually downloaded data files [directory: "G:\503_Themen\Immissionen\muem\DUE\ManuallyDownloaded"]
# - data files contain 1 minute data [date, CO2, CO2 DRY, H2O, flag]
# - date refers to CET
# - In April and May 2017 [time_true - time_Picarro ~ 60 sec] as time_Picarro is not synchronized

if(F){
  
  files    <- list.files(path = paste(fdirectory,"ManuallyDownloaded/",sep=""), pattern = "^CO2",full.names = T)
  n_files  <- length(files)
  
  min_timestamp_update <- as.numeric(difftime(time1=strptime("20170413180000","%Y%m%d%H%M%S",tz="UTC"),
                                              time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                                              units="secs",
                                              tz="UTC"))
  
  for(ith_file in 1:n_files){
    
    data_ref <- read.table(file = files[ith_file],sep=";",as.is=T,header=T)
    
    colnames(data_ref) <- c("timestamp","DATE_CET_EndIntervall","CO2","H2O","CO2_DRY")
    
    data_ref$timestamp      <- as.numeric(difftime(time1=strptime(data_ref$timestamp, "%d.%m.%Y %H:%M",tz="UTC") - 3600 + 60,
                                                   time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                                                   units="secs",
                                                   tz="UTC"))
    
    
    
    min_timestamp <- min(data_ref$timestamp)
    max_timestamp <- max(data_ref$timestamp)
    
    #
    
    query_str  <- paste("SELECT timestamp,CO2,CO2_F,CO2_DRY,CO2_DRY_F,H2O,H2O_F FROM NABEL_DUE WHERE timestamp>=",min_timestamp," and timestamp<=",max_timestamp,";",sep="")
    drv        <- dbDriver("MySQL")
    con<-carboutil::get_conn(group=DB_group)
    res        <- dbSendQuery(con, query_str)
    data_db    <- dbFetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    if(dim(data_db)[1]==0){
      next
    }
    
    #
    
    data_db  <- data_db[order(data_db$timestamp),]
    data_ref <- data_ref[order(data_ref$timestamp),]
    
    id_corr_AB <- which(data_ref$timestamp %in% data_db$timestamp)
    id_corr_BA <- which(data_db$timestamp  %in% data_ref$timestamp)
    
    if(length(id_corr_AB)!=length(id_corr_BA) | length(id_corr_AB)==0 | length(id_corr_BA)==0){
      stop()
    }
    
    data_ref <- data_ref[id_corr_AB,]
    data_db  <- data_db[id_corr_BA,]
    
    # Updata DB
    
    update <- rep(F,dim(data_ref)[1])
    
    for(ith_row in 1:dim(data_ref)[1]){
      if(data_ref$timestamp[ith_row]  ==data_db$timestamp[ith_row]
         & data_ref$timestamp[ith_row]>=min_timestamp_update
         & data_db$CO2[ith_row]       != -999
         & data_db$CO2_F[ith_row]     != 0
         & data_db$CO2_DRY[ith_row]   == -999
         & data_db$CO2_DRY_F[ith_row] == 0
         & data_db$H2O[ith_row]       == -999
         & data_db$H2O_F[ith_row]     == 0
         & abs(data_db$CO2[ith_row]-data_ref$CO2[ith_row])<10000){
        
        update[ith_row] <- T
      }
    }
    
    # print(sqrt(sum((data_db$CO2[update]-data_ref$CO2[update])^2)/sum(update)))
    # print(sprintf("%10.2f",quantile(data_db$CO2[update]-data_ref$CO2[update],probs=c(0,0.025,0.5,.975,1))))
    
    if(sum(update)>0){
      for(ith_row in which(update==T)){
        query_str  <- paste("UPDATE NABEL_DUE SET CO2_DRY=",data_ref$CO2_DRY[ith_row],", CO2_DRY_F=1,H2O=",data_ref$H2O[ith_row],",H2O_F=1 WHERE timestamp=",data_ref$timestamp[ith_row],";",sep="")
        drv        <- dbDriver("MySQL")
        con<-carboutil::get_conn(group=DB_group)
        res        <- dbSendQuery(con, query_str)
        dbClearResult(res)
        dbDisconnect(con)
      }
    }
  }
}


## ----------------------------------------------------------------------------------------------------------------------

## Exclude measurements due to known problems, instrument manipulations, etc.

if(T){
  
  # Installation of the HPP reference gas calibration facility at DUT on 2017-09-11
  
  timestamps <- NULL
  timestamps <- rbind(timestamps,data.frame(t1 = as.numeric(difftime(time1=strptime("20170911140000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            t2 = as.numeric(difftime(time1=strptime("20170911153000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            stringsAsFactors = F))
  timestamps <- rbind(timestamps,data.frame(t1 = as.numeric(difftime(time1=strptime("20170911153000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            t2 = as.numeric(difftime(time1=strptime("20170911162000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            stringsAsFactors = F))
  timestamps <- rbind(timestamps,data.frame(t1 = as.numeric(difftime(time1=strptime("20170911162000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            t2 = as.numeric(difftime(time1=strptime("20170911163000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            stringsAsFactors = F))
  
  # Picarro malfunctioning between 2018-01-31 00:00 UTC and 2018-02-06 08:00 UTC
  timestamps <- rbind(timestamps,data.frame(t1 = as.numeric(difftime(time1=strptime("20180131000000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            t2 = as.numeric(difftime(time1=strptime("20180206080000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            stringsAsFactors = F))
  
  # Picarro malfunctioning between 2018-05-10 00:00 UTC and 2018-05-14 07:00 UTC
  timestamps <- rbind(timestamps,data.frame(t1 = as.numeric(difftime(time1=strptime("20180510000000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            t2 = as.numeric(difftime(time1=strptime("20180514070000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            stringsAsFactors = F))
  
  # Picarro malfunctioning between 2018-08-14 00:00 UTC and 2018-08-20 07:00 UTC
  timestamps <- rbind(timestamps,data.frame(t1 = as.numeric(difftime(time1=strptime("20180814000000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            t2 = as.numeric(difftime(time1=strptime("20180820070000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            stringsAsFactors = F))
  
  # Picarro malfunctioning between 2018-11-21 00:00 UTC and 2018-11-26 15:00 UTC
  timestamps <- rbind(timestamps,data.frame(t1 = as.numeric(difftime(time1=strptime("20181121000000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            t2 = as.numeric(difftime(time1=strptime("20181126150000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            stringsAsFactors = F))
  
  # Picarro malfunctioning between 2019-02-28 00:00 UTC and 2019-03-06 10:30 UTC
  timestamps <- rbind(timestamps,data.frame(t1 = as.numeric(difftime(time1=strptime("20190228000000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            t2 = as.numeric(difftime(time1=strptime("20190306103000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            stringsAsFactors = F))
  
  
  # Picarro malfunctioning between 2019-05-24 08:00 UTC and 2019-06-03 00:00 UTC
  timestamps <- rbind(timestamps,data.frame(t1 = as.numeric(difftime(time1=strptime("20190524080000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            t2 = as.numeric(difftime(time1=strptime("20190603000000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            stringsAsFactors = F))
  
  # Picarro malfunctioning between 2019-09-04 00:00 UTC and 2019-09-09 09:00 UTC
  timestamps <- rbind(timestamps,data.frame(t1 = as.numeric(difftime(time1=strptime("20190904000000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            t2 = as.numeric(difftime(time1=strptime("20190909090000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            stringsAsFactors = F))
  
  # Picarro malfunctioning between 2019-10-08 23:00 UTC and 2019-10-24 00:00 UTC
  timestamps <- rbind(timestamps,data.frame(t1 = as.numeric(difftime(time1=strptime("20191008230000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            t2 = as.numeric(difftime(time1=strptime("20191024000000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            stringsAsFactors = F))
                                            
  # Use of Picarro for CH4 measurements for Flair CSEM sensor between 2020-06-09 10:00 UTC and 2020-06-09 15:00 UTC
  timestamps <- rbind(timestamps,data.frame(t1 = as.numeric(difftime(time1=strptime("20200609100000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            t2 = as.numeric(difftime(time1=strptime("20200609150000","%Y%m%d%H%M%S",tz="UTC"),strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC")),
                                            stringsAsFactors = F))
  
  
  for(ii in 1:dim(timestamps)[1]){
    query_str <- paste("UPDATE `NABEL_DUE` SET `CO2`=-999,`CO2_MIN`=-999,`CO2_MAX`=-999,`CO2_F`=0,`CO2_DRY`=-999,`CO2_DRY_CAL`=-999,`CO2_DRY_F`=0,`H2O`=-999,`H2O_F`=0, `CO2_10MIN_AV`=-999, `CO2_DRY_10MIN_AV`=-999, `CO2_DRY_CAL_10MIN_AV`=-999, `H2O_10MIN_AV`=-999, `CO2_WET_COMP`=-999 WHERE timestamp >= ",timestamps$t1[ii]," and timestamp <= ",timestamps$t2[ii],";",sep="")
    drv        <- dbDriver("MySQL")
    con<-carboutil::get_conn(group=DB_group)
    res        <- dbSendQuery(con, query_str)
    dbClearResult(res)
    dbDisconnect(con)
  }
}




