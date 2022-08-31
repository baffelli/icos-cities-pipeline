library(checkpoint)
checkpoint('2020-04-01')
setwd("Z:/CarboSense/Statistical_Models/Covid-19_ZH")
library(DBI)
library(dplyr)
library(tidyverse)
library(magrittr)
library(data.table)
library(sf)
library(rstan)
library(carboutil)
library(argparse)



res_folder <- "Z:/CarboSense/Statistical_Models/Covid-19_ZH/Results/CO2"
plot_folder <- "Z:/CarboSense/Statistical_Models/Covid-19_ZH"
ct <- cols(date=col_datetime(),date_TT=col_datetime(), .default=col_double())
res <- 
  readr::read_delim(file.path(res_folder, "data.csv"),
                    ';', col_types  = ct )



con <- carboutil::get_conn(user="basi")
loc <- collect(tbl(con, "Location"))


stn_avg <- 
collect(tbl(con, sql(
"SELECT LocationName,SensorUnit_ID, AVG(CO2) FROM CarboSense_CO2_TEST00
WHERE YEAR(FROM_UNIXTIME(timestamp)) < 2020 
GROUP BY LocationName, SensorUnit_ID")))


name_mapping <- c("ZMAN"="Manessestrasse",
                  "ZSCH"="Schimmelstrasse",
                  "ZSBS"="Seebahnstrasse",
                  "ALBS"="Albis",
                  "ZUE"="Kasernenstrasse",
                  "RECK"="Reckenholz",
                  "LAEG_NABEL"="Laegern",
                  "ZBLG"="Bullingerhof",
                  "ZPRD"="Paradeplatz",
                  "ZDLT"="Döltschiweg",
                  "ZHRG"="Herman-Greulich",
                  "ZGLA"="Gladbachstrasse",
                  "DUE_NABEL"="Dübendof",
                  "ZGHD"="ZGHD",
                  "ZHBR"="Heubeeribuhel",
                  "ESMO"="Eschenmosen"
                  )


phases <- as.POSIXct(lubridate::as_date(c('2020-03-16 00:00:00', '2020-04-26 00:00:00','2020-06-09 00:00:00')))

name_re <- paste('(', paste0(names(name_mapping), collapse='|'),')', sep='')

res <- res %>% mutate( phase = 
                  case_when(
                    date > '2020-02-15 00:00:00' & date < '2020-03-16 00:00:00'  ~ 'Before LD',
                    date >= '2020-03-16 00:00:00' & date < '2020-04-26 00:00:00' ~ 'LD Phase 1',
                    date >= '2020-04-26 00:00:00'  ~ 'LD Phase 2'
                    )) %>%
  filter(!is.na(phase))



ref <- ("")



targets <- names(res)[stringr::str_detect(names(res), "^CO2_.*_PRED")]



res_long  <-
  res %>% 
  mutate(year=lubridate::year(date)) %>%
  select(date,phase, year, matches(name_re)) %>%
  pivot_longer(cols = c(-date,-phase, -year)) %>%
  mutate(location=stringr::str_extract(name, name_re)) %>%
  #Remove station frm the name
  mutate(name = stringr::str_remove(name, location)) %>%
  mutate(name=stringr::str_replace(name, "_+","_")) %>%
  mutate(instrument = stringr::str_extract(name, 'LP8')) %>%
  mutate(name=ifelse(!is.na(instrument), stringr::str_remove(name, instrument),name)) %>%
  mutate(instrument=coalesce(instrument, 'HPP'))%>%
  #Remove leading underscore
  mutate(name = stringr::str_remove(name,'^(_)*|(?=_)(_)*')) %>%
  mutate(name=stringr::str_replace(name, "_+","_")) %>%
  mutate(full_location=name_mapping[match(location, names(name_mapping))]) %>%
  spread(name, value) %>%
  pivot_longer(cols = c(CO2_BSL,CO2_BSL_PRED,CO2_ZUE_TRAFFIC_PRED, CO2_ZUE_PRED, CO2_ZUE, CO2_ZUE_TRAFFIC),
               names_to = c("ref","pred"), 
               names_pattern = "(?:CO2_)([A-Z]*(?:_TRAFFIC)*)((?:_)PRED)?") %>%
  mutate(pred=coalesce(str_remove(pred, "_"),"MEAS")) %>%
  pivot_wider(names_from=pred, values_from=value)

#Rolling average
res_roll <-   res_long %>% 
  group_by(location, instrument) %>%
  mutate(CO2_BSL=zoo::rollapply(CO2_BSL, 24*7, fill=NA, function(x) mean(x, na.rm = T)),
         CO2_BSL_PRED=zoo::rollapply(CO2_BSL_PRED, 24*7, fill=NA, function(x) mean(x, na.rm = T))) 


#Weekly average
res_avg <- 
  res_long %>%
  group_by(location, instrument, phase, full_location, woy=lubridate::week(date), year, ref) %>%
  summarise_all(function(x) mean(x, na.rm = T)) %>%
  filter(location %in% c('ZMAN','ZSCH','ZUE','ZSBN'))


#Save raw data
readr::write_csv(res_avg, file.path(plot_folder, "model_output.csv"))

pal <- RColorBrewer::brewer.pal(4, 'Set1') 
ls <- 0.7

res_avg %>%
  group_by(location, ref) %>%
  filter(location %in% c('ZMAN','ZSCH','ZUE','ZSBN')) %>%
  group_map(
    
    function(x,y){
      bin_plot <-
        ggplot(x, aes(x=date, y=MEAS - PRED, color=phase, fill=phase)) +
        geom_bar(stat="identity") + 
        geom_vline(xintercept=as.numeric(phases[1]),linetype=4, size=ls) +
        geom_vline(xintercept=as.numeric(phases[2]),linetype=4, size=ls) +
        geom_vline(xintercept=as.numeric(phases[3]),linetype=4, size=ls) +
        facet_wrap(vars(full_location, instrument), nrow = 2) + ylab(expression(Modeled~CO[2]~-~Observed~CO[2]~group("[",ppm,"]"))) +
        xlab('Date') + guides(fill=guide_legend(title ='Phase', direction = 'horizontal'), color=waiver()) +
        theme(legend.position = 'bottom')
      
      bn <- paste(y$location, y$ref, sep='_')
      ggsave(file.path(plot_folder, paste0(bn, "_ts_hist_difference.pdf")), plot = bin_plot, width=15, height=6)
      ggsave(file.path(plot_folder, paste0(bn, "_ts_hist_difference.png")), plot = bin_plot, width=15, height=6)
      
      box_plot <-
        x %>%
        group_by(date) %>%
        mutate(rk=row_number(instrument)) %>% filter(rk==1) %>%
        ggplot(., aes(x=phase, y=MEAS - PRED, color=phase)) + geom_boxplot() +
        facet_wrap(vars(full_location), nrow = 1) +
        ylab(expression(Modeled~CO[2]~-~Observed~CO[2]~group("[",ppm,"]"))) + ylim(-30,30) +
        xlab('Phase') + guides(color=guide_legend(title ='Phase', direction = 'horizontal'), color=waiver()) +
        geom_hline(aes(yintercept=0), linetype='dashed') +
        theme(legend.position = 'bottom', text = element_text(size=16))
      ggsave(file.path(plot_folder, paste0(bn,"_ts_hist_difference_boxplot.pdf")), plot = box_plot, width=15, height=6)
      ggsave(file.path(plot_folder, paste0(bn,"ts_hist_difference_boxplot.png")), plot = box_plot, width=15, height=6)
      
      box_plot_norm <-
        x %>%
        group_by(date) %>%
        mutate(rk=row_number(instrument)) %>% filter(rk==1) %>%
        ggplot(., aes(x=phase, y=(MEAS - PRED)/PRED*100, color=phase)) + geom_boxplot() +
        facet_wrap(vars(full_location), nrow = 1) +
        ylab(expression(Modeled~CO[2]~-~Observed~CO[2]~group("[",ppm,"]"))) +
        xlab('Phase') + guides(color=guide_legend(title ='Phase', direction = 'horizontal'), color=waiver()) +
        geom_hline(aes(yintercept=0), linetype='dashed') +
        theme(legend.position = 'bottom', text = element_text(size=16)) 
      

      ggsave(file.path(plot_folder, paste0(bn,"ts_hist_difference_boxplot_norm.png")), plot = box_plot_norm, width=15, height=6)
      ggsave(file.path(plot_folder, paste0(bn,"ts_hist_difference_boxplot_norm.pdf")), plot = box_plot_norm, width=15, height=6)
      
    }
    
  )



smooth_plot <-
ggplot(res_roll, aes(x=date, y=CO2_BSL_PRED, color='Prediction', group=instrument)) +
  geom_line(size=ls) + 
  geom_line(aes(y=CO2_BSL, color='Observation'),size=ls) + 
  geom_vline(xintercept=as.numeric(phases[1]),linetype=4, size=ls) +
  geom_vline(xintercept=as.numeric(phases[2]),linetype=4, size=ls) +
  facet_wrap(vars(full_location, instrument), nrow = 2) + ylab(expression(CO[2]~w.r.t~Baseline~group("[",ppm,"]"))) +
  xlab('Date') + guides(colour=guide_legend(title ='Data type', direction = 'horizontal')) +
  theme(legend.position = 'bottom')

ggsave(file.path(plot_folder, "smoothed_prediction.pdf"), plot = smooth_plot, width=15, height=6)


