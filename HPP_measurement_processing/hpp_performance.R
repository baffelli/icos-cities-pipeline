
library(DBI)
library(dplyr)
library(tidyverse)
library(magrittr)
library(data.table)
library(sf)
library(carboutil)
library(gam)

#Connect to the database
con <- carboutil::get_conn( group="CarboSense_MySQL")


sens_query <- 
"
SELECT
*,
MIN(date) OVER (PARTITION BY Serialnumber, cal_number) AS cal_start_date
FROM
(
SELECT
	dep.LocationName,
	dep.SensorUnit_ID,
	HPP.timestamp,
	FROM_UNIXTIME(HPP.timestamp) AS date,
	dep.Date_UTC_from,
	dep.Date_UTC_to,
	sens.Serialnumber,
	rgc_d.CylinderID,
	DENSE_RANK() OVER (PARTITION BY Serialnumber,rgc_d.Date_UTC_from,dep.Date_UTC_from, CylinderID ORDER BY dep.Date_UTC_from) AS cylinder_number,
	DENSE_RANK() OVER (PARTITION BY rgc_d.CylinderID ORDER BY Serialnumber, rgc_d.Date_UTC_from,dep.Date_UTC_from, DATE(FROM_UNIXTIME(timestamp))) AS cal_number,
	rgc.CO2,
	rgc.Pressure AS Cylinder_Pressure,
	HPP.CO2_CAL_DRY,
	HPP.CO2_CAL_ADJ_DRY,
	HPP.RH,
	HPP.Pressure
FROM CarboSense_HPP_CO2 AS HPP
JOIN Deployment AS dep ON HPP.SensorUnit_ID = dep.SensorUnit_ID AND HPP.LocationName = dep.LocationName AND (FROM_UNIXTIME(HPP.timestamp) BETWEEN dep.Date_UTC_from AND dep.Date_UTC_to)
JOIN Sensors AS sens ON  sens.SensorUnit_ID = HPP.SensorUnit_ID AND  (FROM_UNIXTIME(HPP.timestamp) BETWEEN sens.Date_UTC_from AND sens.Date_UTC_to) AND sens.Date_UTC_from <= dep.Date_UTC_from AND sens.Date_UTC_to >= dep.Date_UTC_to
JOIN RefGasCylinder_Deployment AS rgc_d ON rgc_d.SensorUnit_ID = HPP.SensorUnit_ID AND rgc_d.LocationName = HPP.LocationName AND  (FROM_UNIXTIME(HPP.timestamp) BETWEEN rgc_d.Date_UTC_from AND rgc_d.Date_UTC_to)
JOIN RefGasCylinder AS rgc ON rgc.CylinderID = rgc_d.CylinderID AND rgc.Date_UTC_from <= rgc_d.Date_UTC_from AND rgc.Date_UTC_to >= rgc_d.Date_UTC_to
WHERE sens.Type='HPP' AND sens.SensorUnit_ID > 400 AND Valve = 1
) AS a



"

sens_tb <- collect(tbl(con, sql(sens_query))) %>%
  mutate(elapsed_time = date - cal_start_date) 
  
  
  
  # #Estimate remainign cylinder pressure
  # press <- sens_tb %>% ungroup() %>% group_by(CylinderID, SensorUnit_ID, Serialnumber) %>%
  # mutate(elapsed_time = as.numeric(date - cal_start_date)) %>%
           





get_project_folder <- function() {ifelse(Sys.info()['sysname'] == 'Windows', "Z:/CarboSense", "/project/CarboSense")}
  

plot_folder <- file.path(get_project_folder(), "Carbosense_Network/HPP_PerformanceAnalysis/CalibrationPeriod_Analysis")
  

th <- function() theme_minimal(base_size = 20) + theme(text = element_text(size = 25))

wd <- 30
ht <- 20

sens_tb %>%
  group_by(SensorUnit_ID,Serialnumber, LocationName, CylinderID, Date_UTC_from, Date_UTC_to) %>%
  group_map(
    function(x,y)
    {
      sn <- unique(y$Serialnumber)
      ln <- unique(y$LocationName)
      cid <-unique(y$CylinderID)
      start <- as.character(unique(y$Date_UTC_from))
      stop <- as.character(unique(y$Date_UTC_to))
      tit <- bquote(atop("Sensor:"~.(sn)~"Cylinder:"~.(cid)~"Location:"~.(ln),"Deployment:"~.(start)~'-'~.(stop)))
      
      #Compute sigma
      sigma_es <- sd(x$CO2_CAL_ADJ_DRY-x$CO2)
      lab <- bquote(sigma~"="~.(sigma_es))
      #Plot Gas histogram
      repf_2 <- ggplot(x) +
        geom_histogram(aes(x=CO2_CAL_ADJ_DRY-CO2), fill="cornflowerblue", bins=50) +
        geom_vline(aes(xintercept=0), color='purple', size=1)  + xlim(-5,5)  +  th() + xlab("Difference from reference [ppm]") + labs(title=tit, subtitle = lab)
        subfolder <- ifelse(unique(y$Date_UTC_to)>lubridate::today(),'Deployed','Old')
        pth <- file.path(plot_folder,subfolder, paste0("HPP_Calibration_Hist_",unique(y$Serialnumber), "_",unique(y$LocationName),"_",unique(y$CylinderID),".png"))
        ggsave(pth, plot=repf_2,  width = wd, height = ht, dpi=300, units = "cm")
        repf_2
    }
  )





gam_cv <- function(data){
  data_cv <- rsample::loo_cv(data)
  data_fit <- mutate(data_cv, 
                     fit = map(splits, function(x) gam::gam(adj~s(time), family = gaussian, data=rsample::analysis(x))),
                     pred = map2(fit, splits, function(x,y) predict(x, rsample::assessment(y))),
                     pred_orig =  map2(fit, splits, function(x,y) predict(x, rsample::analysis(y))))
    
  unnest(mutate(data_fit, orig=map(splits, function(x) rsample::assessment(x))), pred, orig)
  
}


sens_cv_res <- 
sens_tb %>%
  group_by(Serialnumber, CylinderID, LocationName) %>%
  mutate(adj=CO2_CAL_DRY - CO2,
         time=date - min(date)) %>%
  group_by(date=lubridate::date(date), CylinderID,LocationName, Date_UTC_from, Date_UTC_to, Serialnumber, cylinder_number, SensorUnit_ID) %>%
  summarise_all(mean) %>%
  ungroup() %>%
  group_by(Serialnumber, LocationName, CylinderID) %>%
  filter(n()>4) %>%
  group_modify(~ gam_cv(.x))
  

sens_cv_res %>% 
  group_by(SensorUnit_ID,Serialnumber, LocationName, CylinderID, Date_UTC_from, Date_UTC_to) %>%
  group_map(
    function(x,y){
      subfolder <- ifelse(unique(y$Date_UTC_to)>lubridate::today(),'Deployed','Old')
      base_pth <- file.path(plot_folder,subfolder)
      tit <- paste("Sensor:",unique(y$Serialnumber), "Cylinder:",unique(y$CylinderID), "Location:",unique(y$LocationName),"\n","Deployment:",unique(y$Date_UTC_from),'-',unique(y$Date_UTC_to))
      
      sens_cv_plot <- 
        ggplot(x, aes(x=date,y=adj-pred)) + geom_point(color='cornflowerblue', size=3)  + ylim(-10,10) +  
        geom_hline(aes(yintercept=-1), color='purple', size=2) +  geom_hline(aes(yintercept=1), color='purple', size=2)  + 
        xlab("Date") + ylab("Difference fron trend [ppm]") +   th()  + ggtitle(tit)
      ggsave(file.path(base_pth, paste0("HPP_reproducibility_cv_",unique(y$Serialnumber),"_",unique(y$LocationName),"_",unique(y$CylinderID),".png")), plot=sens_cv_plot,   width = wd, height = ht, dpi=300, units = "cm")
      
      fit_plot <- ggplot(x, aes(x=date, y=adj)) + 
        geom_point(color='cornflowerblue', size=3) + geom_line(aes(y=pred), size=3, color='purple') + xlab("Date") + ylab("Adjustment [ppm]") +th()  + ggtitle(tit)
      ggsave(file.path(base_pth, paste0("HPP_reproducibility_cv_fit_",unique(y$Serialnumber),unique(y$CylinderID),".png")), plot=fit_plot,   width = wd, height = ht, dpi=300, units = "cm")
      
      #Plot moisture
      hum_plot <- ggplot(x, aes(x=date, y=RH)) +  geom_point(color='cornflowerblue', size=3) + xlab("Date") + ylab("Relative Humidity") +th()  + ggtitle(tit) + ylim(-10,100)
      ggsave(file.path(base_pth, paste0("HPP_RH_",unique(y$Serialnumber),unique(y$LocationName),unique(y$CylinderID),".png")), plot=hum_plot,   width = wd, height = ht, dpi=300, units = "cm")
      
      #Make combined plot
      comb_plot <- gridExtra::arrangeGrob(sens_cv_plot, fit_plot, hum_plot)
      ggsave(file.path(base_pth, paste0("HPP_performance_",unique(y$Serialnumber),"_",unique(y$LocationName),"_",unique(y$CylinderID),".png")), plot=comb_plot,   width = wd, height = ht, dpi=300, units = "cm")
    }
  )


