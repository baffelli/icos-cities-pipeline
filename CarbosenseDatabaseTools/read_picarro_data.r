


read_pic <- function(path){
  nc <- 22
  wd <- 26
  headers <- read_fwf(path, col_positions = fwf_widths(rep(wd,nc)), n_max=1)
  data <- read_fwf(path, col_positions = fwf_widths(rep(wd,nc)), skip=1)
  pd <- set_names(data, as.character(headers))
  dplyr::select(mutate(pd, date=as.POSIXct(EPOCH_TIME, origin=lubridate::origin, tz="GMT")),
         date, CO2_dry_sync, CO2_sync, H2O_sync)
}


prepare_pic <- function(pd){
  pd_av <- openair::timeAverage(pd, avg.time = "1 min")
  
  mutate(pd_av,
         date_end=date + lubridate::minutes(1)) %>%
    dplyr::select("DATE_UTC"=date, 
                  "DATE_UTC_EndIntervall"=date_end,
                  "PIC_CO2"=CO2_sync,
                  "PIC_H2O"=H2O_sync,
                  "PIC_CO2_DRY"=CO2_dry_sync)
}

  
  pi2001_files <- Sys.glob("K:/Carbosense/Data/Klimakammer_Versuche_27022017_XXXXXXXX/Original_Data/pic2001/2021/01/*.dat")
  pd_2001 <- map(pi2001_files, function(x) prepare_pic(read_pic(x)))
  
  pi2098_files <- Sys.glob("K:/Carbosense/Data/Klimakammer_Versuche_27022017_XXXXXXXX/Original_Data/pic2098//2021/01/*.dat")
  pd_2098 <- map(pi2098_files, function(x) prepare_pic(read_pic(x)))

  
  pic_data <-
  bind_rows(
    bind_rows(pd_2001) %>% mutate(inst="2001"),
    bind_rows(pd_2098) %>% mutate(inst="2098")
  )