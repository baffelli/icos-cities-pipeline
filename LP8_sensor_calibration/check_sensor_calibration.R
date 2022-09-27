library(checkpoint)
checkpoint("2020-04-01")
library(DBI)
library(dplyr)
library(tidyverse)
library(magrittr)
library(data.table)
library(sf)
library(rstan)
library(carboutil)
library(broom)
#Define constants
ref_pr <- 1013.25
ref_t <- 273.15

con <- carboutil::get_conn(user="basi", host = 'emp-sql-cs1')


get_cal_info <- function(con, sn)
{
  #Get calibration periods
  cal_query <-
  "
  SELECT
    cal.*,
    sens.Serialnumber,
    sens.Date_UTC_from AS sensor_start,
    IF(cal.Date_UTC_to>CURDATE(), CURDATE(), cal.Date_UTC_to) AS Date_UTC_end
  FROM Calibration AS cal
    JOIN Sensors AS sens
    ON sens.SensorUnit_ID = cal.SensorUnit_ID
    AND cal.Date_UTC_from BETWEEN sens.Date_UTC_from AND sens.Date_UTC_to
    AND Type = 'LP8'
  WHERE  sens.Serialnumber = {sn}
  AND  DBTableNameRefData <> 'PressureChamber_00_DUE'
  "
  carboutil::get_query_parametrized(con, cal_query, sn=sn)
}


get_sensors_to_calibrate <- function(con){
 tq<- "
SELECT
SensorUnit_ID,
Serialnumber,
sensor_start_date,
DATEDIFF(sensor_end_date , sensor_start_date) AS age
FROM
(
SELECT DISTINCT
    sens.SensorUnit_ID, 
    sens.Serialnumber,
    sens.Date_UTC_from AS sensor_start_date,
    IF(sens.Date_UTC_to > CURDATE(), CURDATE(),  sens.Date_UTC_to )  AS sensor_end_date
  FROM Calibration AS cal
   JOIN Sensors AS sens
    ON sens.SensorUnit_ID = cal.SensorUnit_ID
    AND cal.Date_UTC_from BETWEEN sens.Date_UTC_from AND sens.Date_UTC_to
    AND Type = 'LP8'
  WHERE cal.Date_UTC_to>CURDATE()
  AND sens.SensorUnit_ID > 499
  ) AS a
  "
  carboutil::get_query_parametrized(con, tq)
  
}






get_cal_data <- function(con, from, to, table){
  #Maps the table name to the corresponding select
  #query
  col_mapping <- c(
    "NABEL_DUE" =
      "timestamp,
        CO2_DRY_CAL AS CO2_DRY ,
        CO2_DRY_F,
        H2O,
        H2O_F,
        T,
        T_F,
        RH,
        RH_F,
        pressure,
        pressure_F",
    "PressureChamber_01_DUE" =
      "timestamp,
        CO2_DRY_10MIN_AV AS CO2_DRY,
        IF(CO2_DRY_10MIN_AV <> -999, 1, 0) AS CO2_DRY_F,
        H2O_10MIN_AV AS H2O,
        IF(H2O_10MIN_AV <> -999, 1, 0) AS H2O_F,
        T_10MIN_AV AS T,
        IF(T_10MIN_AV <> -999, 1, 0) AS T_F,
        -999 AS RH,
        0 AS RH_F,
        pressure_10MIN_AV AS pressure,
        IF(pressure_10MIN_AV <> -999, 1, 0) AS pressure_F
        ",
    "ClimateChamber_00_DUE" =
      "timestamp,
       CO2_DRY_10MIN_AV AS CO2_DRY,
      IF(CO2_DRY_10MIN_AV <> -999, 1, 0) AS CO2_DRY_F,
      H2O_10MIN_AV AS H2O,
      IF(H2O_10MIN_AV <> -999, 1, 0) AS H2O_F,
      T_10MIN_AV AS T,
      IF(T_10MIN_AV <> -999, 1, 0) AS T_F,
      RH_10MIN_AV AS RH,
      IF(RH_10MIN_AV <> -999, 1, 0) AS RH_F,
      pressure_10MIN_AV AS pressure,
      IF(pressure_10MIN_AV <> -999, 1, 0) AS pressure_F
    ",
    "PressureChamber_00_METAS"=
      "timestamp,
      CO2_DRY,
      CO2_DRY_F,
      H2O,
      H2O_F,
      -999 AS T,
      0 AS T_F,
      -999 AS RH,
      0 AS RH_F,
      pressure_10MIN_AV AS pressure,
      IF(pressure_10MIN_AV <> -999, 1, 0) AS pressure_F"
  )
  
  str(table)
  tq<- "
  SELECT 
    {cm},
    FROM_UNIXTIME(timestamp) AS date
  FROM {`table`}
  WHERE timestamp BETWEEN UNIX_TIMESTAMP({from}) AND UNIX_TIMESTAMP({to})
  "
  current_query <- glue::glue_sql(col_mapping[table], .con=con)
  carboutil::get_query_parametrized(con, tq, from=from, to=to, table=table, cm=current_query) %>%
    dplyr::mutate_if(is.numeric, ~na_if(.x, carboutil::carbosense_na))
}


get_sens_data <- function(con, unit, from, to){

  tf <- stringr::str_interp("time >= '${carboutil::format_decentlab_date(from)}' AND time <= '${carboutil::format_decentlab_date(to)}'")
  dt <- carboutil::query_decentlab(carboutil::get_decentlab_api_key(),
                             time_filter = tf,
                             agg_int = '10m',
                             agg_func = 'mean',
                             sensor = '/sensirion-sht21-humidity|sensirion-sht21-temperature|battery|senseair-lp8-status|senseair-lp8-co2-filtered|senseair-hpp-co2-filtered|/',
                             device = carboutil::device_list(unique(unit))) %>%
    rename_all(~stringr::str_replace_all(.x, '-','_')) %>%
    mutate(sensor=as.numeric(sensor))
  str(tf)
  dt
}




prepare_calibration_data <- function(con, serialnumber){
  cal_periods <- get_cal_info(con, serialnumber) %>%
  mutate(sensor_start_date = lubridate::as_datetime(sensor_start))
  #Get referemce data
  ref_data <- cal_periods %>%
    group_by(Serialnumber, CalMode, SensorUnit_ID, Date_UTC_from, Date_UTC_end) %>%
    group_modify(~ get_cal_data(con, .y$Date_UTC_from, .y$Date_UTC_end, .x$DBTableNameRefData) %>% mutate(sensor_start_date=.x$sensor_start)) 
  
    # group_by(date=lubridate::floor_date(lubridate::as_datetime(timestamp), unit = '10minutes'), 
    #          SensorUnit_ID, 
    #          CalMode,
    #          sensor_start_date,
    #          Serialnumber) %>% 
    # summarise_all(mean) 
    
  
  #Get sensor data
  sens_data <- cal_periods %>%
    group_by(SensorUnit_ID,Serialnumber,Date_UTC_from, Date_UTC_end) %>%
    group_modify(~ purrr::safely(get_sens_data, otherwise = data.frame())(con, .y$SensorUnit_ID, .y$Date_UTC_from, .y$Date_UTC_end)$result) 
  #Define some valudatio parameters
  max_rh <- 95
  
  #Comnbine the two data types
  cal_data <-
    inner_join(
      ref_data %>% mutate(date = lubridate::as_datetime(date)) %>%
        rename('t'=`T`),
      sens_data %>%
        mutate(sensor = as.numeric(sensor)) ,
      by = c(
        'SensorUnit_ID',
        'date' = 'time',
        'SensorUnit_ID' = 'sensor',
        'Serialnumber',
        "Date_UTC_from",
        "Date_UTC_end"
      )
    ) %>%
    mutate(
      dew_point = carboutil::dew_point(sensirion_sht21_humidity, sensirion_sht21_temperature),
      #Convert in kelving
      senseair_lp8_temperature = senseair_lp8_temperature + ref_t,
      sensirion_sht21_temperature = sensirion_sht21_temperature + ref_t,
      pressure_norm = (pressure - ref_pr) / ref_pr,
      t = t + ref_t,
      ah = carboutil::relative_to_absolute_humidity(sensirion_sht21_humidity, sensirion_sht21_temperature, pressure),
      CO2 = CO2_DRY * (1-H2O/100) * pressure / ref_pr *  ref_t / senseair_lp8_temperature,
      log_ir = -log(senseair_lp8_ir)
    ) %>%
    #Add information on the time differenge
    filter(CO2_DRY_F==1 & senseair_lp8_status == 0) %>%
    arrange(date,Serialnumber) %>%
    group_by(Serialnumber, Date_UTC_from,Date_UTC_end) %>% 
    arrange(date) %>%
    group_modify(
      ~mutate(.x,
             dt= c(0, diff(as.numeric(date), units='secs')),
             d_ir =  c(0,diff(senseair_lp8_ir)),
             d_CO2 = c(0, diff(CO2)),
             d_rh = c(0, diff(sensirion_sht21_humidity)),
             d_lp8_t =  c(0, diff(sensirion_sht21_temperature))
    )) %>%
    arrange(date) %>% 
    mutate(
      age=as.numeric(date- lubridate::as_datetime(sensor_start_date)),
      min_10 = (dt < 700 & dt > 500),
      ir_peak = ( min_10 &
          ((CalMode == 1 & abs(d_ir) < 400) |
            (CalMode == 2 & abs(d_ir)) < 200)),
      rh_peak = (min_10 & CalMode == 3 & abs(d_rh) < 5),
      rh_state = ((CalMode == 2 & sensirion_sht21_humidity < 70)| CalMode %in% c(1,3)),
      valid_cond = ((CalMode == 1 & sensirion_sht21_humidity < 75 & abs(d_lp8_t) < 0.5 & (sensirion_sht21_temperature - dew_point) > 5) | CalMode %in% c(2,3)),
      valid_data =
        (
          pressure_F & 
          T_F & 
          H2O_F &
          !is.na(senseair_lp8_temperature) &
            !is.na(senseair_lp8_ir) &
            !is.na(pressure) &
            !is.na(CO2) &
            !is.na(sensirion_sht21_temperature) &
            !is.na(sensirion_sht21_humidity) &
            !is.na(ah)
        ),
      rapid_change = (min_10 & abs(d_ir)>400 & CalMode ==2),
      valid = valid_data & (ir_peak | rh_peak) & rh_state & valid_cond & abs(d_co2) < d_CO2)
    #   (sensirion_sht21_humidity < max_rh) &
    #     CO2 > 0 &
    #     (dt < 700 & dt > 500) &
    #     abs(senseair_lp8_temperature - sensirion_sht21_temperature) < 0.7 &
    #     (
    #       (CalMode %in% c(2, 3) & sensirion_sht21_humidity < 95) |
    #         (CalMode == 1  &
    #            (
    #              sensirion_sht21_humidity < 70  | abs(d_lp8_t) < 0.05
    #            )) &
    #         (
    #           (abs(d_rh) < 5 & CalMode == 3) |
    #           (abs(d_ir) < 400 & CalMode == 1 & abs(d_lp8_t) < 2) |
    #           (abs(d_ir) < 200 & CalMode == 2)
    #         )
    #     )
    # )
  # #Define models
  # mod_mich <-
  #   formula(
  #     CO2 ~ log_ir + I(senseair_lp8_temperature) + I(senseair_lp8_temperature^2) + 
  #       I(senseair_lp8_temperature ^ 3) + I((senseair_lp8_temperature) / senseair_lp8_ir) +
  #       I((senseair_lp8_temperature) ^ 2 / senseair_lp8_ir) + I((senseair_lp8_temperature) ^ 3 / senseair_lp8_ir) +
  #       I(pressure_norm) + I(pressure_norm / senseair_lp8_ir)
  #   )
  # mod_for_h <-
  #   update.formula(mod_mich, . ~ log_ir  + 
  #                    s(senseair_lp8_temperature, k=3) + 
  #                    ti(senseair_lp8_temperature,senseair_lp8_ir, k=3) + ti(pressure_norm,senseair_lp8_ir,k=5)
  #                  +ti(sensirion_sht21_humidity,senseair_lp8_ir, k=3 ) + ti(battery,senseair_lp8_temperature, k=4))
  #   # formula(
  #   #   CO2 ~ -log(senseair_lp8_ir)  + I(senseair_lp8_temperature/ah)  + poly(senseair_lp8_temperature, degree = 2) + s(battery, k=6) 
  #   #    + pressure_norm +  I(pressure_norm / senseair_lp8_ir) +  s(senseair_lp8_temperature/senseair_lp8_ir)
  #   # )
  # 
  # fitters <- c(
  #   'nh' = function(x)
  #   {
  #     MASS::rlm(
  #       mod_mich,
  #       data = x,
  #       psi = MASS::psi.huber,
  #       k = 1.345
  #     )},
  #   'h' = function(x)
  #   {
  #     mgcv::gam(
  #       mod_for_h,
  #       data = x)
  #   }
  # )
  # 
  # 
  #   
  #     
  #   cal_res <-
  #   cal_data %>%
  #   filter(valid) %>%
  #   ungroup()%>%
  #   group_by(Serialnumber) %>%
  #   nest() %>%
  #   mutate(fit = list(map2(fitters, data, function(ft,x) ft(x))), 
  #            mod=list(names(fitters))) %>%
  #   unnest(fit, mod) %>% 
  #   mutate(CO2_pred = map2(fit ,data, function(x,y) predict(x,y)))
  # 
  #     
  # 
  # ps <- 0.1
  # 
  # #Plot 
  # cal_plot <- cal_res %>%     
  #   unnest(data,CO2_pred) %>% 
  #   ggplot(aes(x=CO2,y=CO2_pred, color=mod)) +
  #   geom_point(size=ps) + xlim(300,1000) + ylim(300, 1000)  + 
  #   geom_abline(aes(intercept=-20,slope=1))+ geom_abline(aes(intercept=20,slope=1)) + geom_abline()
  # 
  # scatter_orig <- ggplot(cal_data, aes(x=CO2, y=senseair_lp8_ir, color=valid)) + 
  #   geom_point(size=ps)
  # 
  # 
  # hist <- ggplot(cal_data, aes(x=CO2-CO2_pred)) + geom_histogram()
  # 
  # file_save_base <- stringr::str_interp("Y:/co2_mapping/plots/S${serialnumber}")
  # file_save_scatter <- stringr::str_interp("${file_save_base}_cal_scatter.pdf")
  # file_save_scatter_orig <- stringr::str_interp("${file_save_base}_cal_data_scattwe.pdf")
  # 
  # ggsave(plot=cal_plot,file_save_scatter)
  # ggsave(plot=scatter_orig,file_save_scatter_orig)
  cal_data
}




sens_to_cal <- get_sensors_to_calibrate(con)
cal_data <- sens_to_cal %>%
  sample_frac(0.12) %>%
  group_by(Serialnumber) %>%
  mutate(sensor_start_date = lubridate::as_datetime(sensor_start_date)) %>%
  group_map( ~prepare_calibration_data(con, .y$Serialnumber))


cal_data_df <- bind_rows(cal_data)  %>%
  group_by(Serialnumber) %>%
  arrange(date) %>%
  mutate(t_sum=cumsum(senseair_lp8_temperature))

#Determine the optimal shift
ccf_co2 <-
cal_data_df %>%
  group_by(Serialnumber,Date_UTC_from, CalMode) %>%
group_modify(~ccf(.x$CO2_DRY, .x$senseair_lp8_co2) %>% broom::tidy())

optimal_shift <- ccf_co2 %>% 
  group_by(Serialnumber, CalMode) %>%
  summarise(lg_mx=lag[which.max(acf)])
#Determine a filter
ft <- signal::fir1(200, 0.01)
#Shift the data
cal_data_df_shift <-
cal_data_df %>%
  left_join(optimal_shift) %>%
  group_by(Serialnumber, CalMode) %>%
  mutate(CO2_SHIFT = lead(CO2, min(lg_mx))) %>%
  ungroup() %>% 
  group_by(Serialnumber, Date_UTC_from) %>%
  mutate(
         log_ir_filt = signal::filter(ft, log_ir),
         RH_filt=signal::filter(ft, sensirion_sht21_humidity))



form <- formula(CO2 ~  poly(log_ir, 2) + RH_filt + 
                  ah + age + I(pressure/senseair_lp8_ir) + 
                  poly(senseair_lp8_temperature, 3)  +  d_rh +
                  I(senseair_lp8_temperature/senseair_lp8_ir) + 
                  I(senseair_lp8_temperature^2/senseair_lp8_ir^2))





cal_plts <-
  cal_data_df_shift %>%
  group_by(Serialnumber) %>% 
  group_map(
    ~lm(form_mich, data=.x %>% filter(valid==1)) %>% 
      broom::augment(newdata=.x ) %>%
      ggplot(aes(x=date, y=CO2 - .fitted, color=age)) +  geom_point() + facet_wrap('Date_UTC_from', scales = 
                                                                                     'free'))


  cd_2 <- cal_data[[3]] %>% filter(CalMode %in% 2 & valid==T & t < 500)
  
  clusts <- kmeans(cd_2 %>% ungroup() %>% select(CO2_DRY,t), 6)
  
  cd_2_aug <- bind_cols(cd_2, cluster=clusts$cluster) %>%
    group_by(cluster) %>%
    mutate(log_ir_norm  = log_ir - mean(log_ir))
  
  cd <- cal_data[[3]]  %>% filter(valid==T & t < 500 & sensirion_sht21_humidity < 70)
  #Fit a temperature compensation curve
  lm_fit_t <- lm(log_ir ~ poly(senseair_lp8_temperature,2) + poly(sensirion_sht21_humidity,2) + CO2 , data=cd_2_aug)
  #Now 
  cd_2_aug <- bind_cols(  cal_data[[3]], log_ir_pred=predict(lm_fit_t,  cal_data[[3]] %>% mutate(CO2=0))) %>%
    mutate(log_ir_comp = log_ir - log_ir_pred)
    
  lm_fit <- lm(CO2_DRY ~ log_ir  * senseair_lp8_temperature + I(senseair_lp8_temperature^2)
                 , data= cd_2_aug %>% filter(valid==T))  
  
  cd_2_aug <- bind_cols(cd_2_aug, CO2_PRED=predict(lm_fit,cd_2_aug ))
  
 
  
 
  bind_cols(cd, CO2_DRY_PRED=predict(lm_fit, cd)) %>%
    ggplot(aes(x=CO2_DRY,y=CO2_DRY_PRED, color=t)) + geom_point( ) +
    geom_abline(aes(intercept=-20, slope=1)) + geom_abline(aes(intercept=20, slope=1)) 



fit_cal <- function(x, mod_form, fitter, by=NA, all_modes=T){
  do_fit <- function(x, mod_form, fitter, all_modes=all_modes){
    set.seed(333)
    initial_split <- rsample::initial_split(x %>% filter(valid==1))
    if(!all_modes){
      train_data <- rsample::training(initial_split) %>% filter(CalMode %in% c(1,3))
    }
    else
    {
      train_data <- rsample::training(initial_split)
    }
    test_data <- rsample::testing(initial_split)
    fit <- purrr::invoke(fitter, list(mod_form, train_data))
    pred <- bind_cols(CO2_pred=predict(fit,  test_data), test_data)
    pred %>% mutate(all_modes=all_modes)
  }
  if(!is.na(by)){
    group_by(x, var(by)) %>%
      group_map(~do_fit(.x, mod_form, fitter, all_modes = all_modes)) %>%
      bind_rows()
  }
  else{
    do_fit(x, mod_form, fitter, all_modes = all_modes)
  }

}


fitter <- function(mod, data) lm(mod, data)
form <-  formula( CO2_SHIFT ~ log_ir +  poly(senseair_lp8_temperature, degree=2) + log_ir*senseair_lp8_temperature + log_ir*pressure )

form_mich <-
    formula(
      CO2_DRY ~ -log_ir + I(senseair_lp8_temperature) + I(senseair_lp8_temperature^2) +
        I(senseair_lp8_temperature ^ 3) + I((senseair_lp8_temperature) / senseair_lp8_ir) +
        I((senseair_lp8_temperature) ^ 2 / senseair_lp8_ir) + I((senseair_lp8_temperature) ^ 3 / senseair_lp8_ir) +
        I(pressure_norm) + I(pressure_norm / senseair_lp8_ir)
    )

all_data_pred <- bind_rows(
  fit_cal(cal_data_df_shift, form_mich, fitter, by="Serialnumber", all_modes = T ) %>% mutate(training="all modes"),
  fit_cal(cal_data_df_shift, form_mich, fitter, by="Serialnumber", all_modes = F )  %>% mutate(training="no pressure"))

com_plots <-
all_data_pred %>%
  group_by(Serialnumber) %>%
  group_map(~
              ggplot(.x, aes(x=log_ir, y=CO2-CO2_pred, color=as_factor(CalMode))) + 
              geom_point(size=0.2) + geom_abline() +  facet_wrap("training") + ylim(-100,100))
              #geom_abline(aes(slope=1, intercept=-20)) +
              #geom_abline(aes(slope=1, intercept=20)) +
              #geom_vline(aes(xintercept=380)) + 
              #geom_vline(aes(xintercept=600)) + facet_wrap("training"))

# fit_cal <- function(x, mod_form, fitter, by=NA, all_modes=T){
#   do_fit <- function(x, mod_form, fitter, all_modes=all_modes){
#     set.seed(333)
#     initial_split <- rsample::initial_split(x %>% filter(valid==1))
#     if(!all_modes){
#       train_data <- rsample::training(initial_split) %>% filter(CalMode %in% c(1,2))
#     }
#     else
#     {
#       train_data <- rsample::training(initial_split)
#     }
#     test_data <- rsample::testing(initial_split)
#     fit <- purrr::invoke(fitter, list(mod_form, train_data))
#     pred <- bind_cols(CO2_pred=predict(fit,  test_data), test_data)
#     pred %>% mutate(all_modes=all_modes)
#   }
#   if(!is.na(by)){
#     group_by(x, var(by)) %>%
#       group_map(~do_fit(.x, mod_form, fitter, all_modes = all_modes)) %>%
#       bind_rows()
#   }
#   else{
#     do_fit(x, mod_form, fitter, all_modes = all_modes)
#   }
#   
# }
# 
# #Combine all data
# all_cal_data <- cal_data %>%
#   map(~mutate(.x, age=cumsum(as.numeric(date - min(date), units="days"))))
#   
# hier_form <- formula(CO2~ log_ir + I(log_ir^2) +
#                        log_ir:senseair_lp8_temperature + I(log(sensirion_sht21_humidity)) +
#                        senseair_lp8_temperature + (0 + log_ir | Serialnumber) + (0 + senseair_lp8_temperature | Serialnumber))
# 
# mich_form <- 
#     formula(
#       CO2 ~ poly(log_ir, degree = 2) +
#         poly(senseair_lp8_temperature, degree = 3) + I((senseair_lp8_temperature)/log_ir) +
#         I((senseair_lp8_temperature)^2/log_ir) + I((senseair_lp8_temperature)^3/log_ir) + 
#         I(pressure_norm))
# simp_form <- formula(CO2~ poly(log_ir, degree=2) + poly(senseair_lp8_temperature, degree=3) +
#                        log_ir:senseair_lp8_temperature )
# 
# 
# fit_data <- bind_rows(all_cal_data) %>% 
#   group_by(Serialnumber,
#            ir = cut(log_ir, 5),
#            press_group = cut(pressure, 5),
#            temp_group=cut(senseair_lp8_temperature, c(270,300,350,400)),
#            CO2_group = cut(CO2, c(350,400,450,500,550,600,650))) %>%
#   dplyr::sample_n(2, replace=T)
# 
# hier_fit_pred <- fit_cal(fit_data, hier_form, lme4::lmer)
# lm_fit_pred <- fit_cal(fit_data, mich_form, lm, by = "Serialnumber")
# simp_fit_pred <- fit_cal(fit_data, simp_form, lm)
# 
# all_mod_fits <-
# bind_rows(
#   hier_fit_pred %>% mutate(method="hierarcical"),
#   simp_fit_pred %>% mutate(method="single model"),
#   lm_fit_pred %>% mutate(method="individual"))
# 
# 
# all_plots <-
# all_mod_fits %>%
#   group_by(Serialnumber) %>%
#   group_map(function(x,y){
#     ggplot(x,aes(x=CO2, y=CO2_pred, color=method)) + 
#       geom_abline() +
#       geom_abline(aes(intercept=-20, slope=1)) +
#       geom_abline(aes(intercept=20, slope=1)) +
#       geom_point(size=0.1)
#   })
# 
# 
# 
# 
# # #Define calibration formulas
# # mod_form <- formula(CO2 ~ (log_ir * sensirion_sht21_temperature * sensirion_sht21_humidity)^3 + 
# #                       log_ir * pressure_norm + battery + cal_group*log_ir)
# # 
# # 
# # models <-
# #   c('rh'=function(x) lm(mod_form, data=x), 
# #     'orig'=function(x)  lm(mod_form_mich, data=x))
# # 
# # 
# # 
# # 
# # fit_models <- function(data, models){
# #   lms <- imap(models, ~.x(data)) 
# #   #preds <- imap(lms, ~predict(.x ,data))
# #   #bind_cols(data, lmlms)
# #   lms
# #     #pivot_longer(names(models),names_to='model',values_to='.fitted')
# # }
# # fits <-
# # cal_data %>%
# #   set_names(map(cal_data,~unique(.x$SensorUnit_ID))) %>%
# #  imap( ~lm(mod_form_mich, .x %>% filter(valid==1))) 
# # 
# # tidy_fits_wide <- fits %>% map(tidy) %>% bind_rows(.id='SensorUnit_ID') %>% 
# #   pivot_wider(id_cols='SensorUnit_ID', names_from='term', values_from = 'estimate')
# # 
# # clusters <- kmeans(tidy_fits_wide %>% select(-SensorUnit_ID), 3)
# # 
# # 
# # cluster_id <- bind_cols(cluster=clusters$cluster, SensorUnit_ID=as.numeric(tidy_fits_wide$SensorUnit_ID))
# # 
# # clustered_data <- bind_rows(map2(fits,cal_data,~bind_cols(.y,CO2_pred=predict(.x,.y)))) %>% left_join(cluster_id)
# # 
# # 
# # fit <-
# # bind_rows(cal_data) %>% filter(valid) %>% 
# #   group_by(Serialnumber) %>% sample_frac(0.01) %>%  
# # 
# cal_data_thin <-
# cal_data %>%
#   map(.,
#   ~filter(.x, valid & CalMode %in% c(2,3)) %>%
#   group_by(Serialnumber,
#            rh_block=cut(sensirion_sht21_humidity,c(0,25,50,75,100)),
#            t_block=cut(sensirion_sht21_temperature,c(ref_t - 10, 0+ref_t, 10 + ref_t, 20+ref_t, 30 + ref_t)),
#            pressure_block=cut(pressure,c(600,700,800,900,1000))) %>%
#   mutate(age=cumsum(timestamp - min(timestamp))) %>%
#   sample_frac(0.1)) %>% bind_rows() %>%
#   ungroup()%>%
#   mutate(Serialnumber=as.factor(Serialnumber))
# 
# 
# 
# mod_form_mich <- formula(CO2 ~ log_ir + I(senseair_lp8_temperature) + I((senseair_lp8_temperature)^2) +
#                            I((senseair_lp8_temperature)^3) + I((senseair_lp8_temperature)/senseair_lp8_ir) +
#                            I((senseair_lp8_temperature)^2/senseair_lp8_ir) + I((senseair_lp8_temperature)^3/senseair_lp8_ir) +
#                            pressure_norm + I(pressure_norm/senseair_lp8_ir))
# mod_form_mich_hier <- update.formula(mod_form_mich, . ~ . + log_ir:Serialnumber)
# 
# lm(mod_form_mich, data=cal_data_thin)
# lm(mod_form_mich_hier, data=cal_data_thin)
# 
# mods <- c(
#   "only_ir"=formula(CO2 ~ log_ir),
#   "ir_hier"=formula(CO2 ~ SensorUnit_ID*log_ir),
#   "ir_t"=formula(CO2~SensorUnit_ID*log_ir + (senseair_lp8_temperature*senseair_lp8_ir)^3),
#   "ir_t_hier"=formula(CO2~SensorUnit_ID*log_ir + (senseair_lp8_temperature*senseair_lp8_ir*SensorUnit_ID)^3),
#   "ir_t_drift_hier"=formula(CO2~SensorUnit_ID*log_ir +  timestamp*SensorUnit_ID + (senseair_lp8_temperature*senseair_lp8_ir*SensorUnit_ID)^3),
#   "ir_t_rh_drift_hier"=formula(CO2~SensorUnit_ID*log_ir  + SensorUnit_ID * age +  timestamp*SensorUnit_ID + (senseair_lp8_temperature*senseair_lp8_ir*SensorUnit_ID)^3)
# )
# 
# 
# lms <-
# map(mods,
#     function(x,data=cal_data_thin %>% filter(valid==1))
#     {
#       mod_1 <- lm(x, data)
#       cal_data_aug <- augment(mod_1, newdata=data)
#       list(mod=mod_1, aug=cal_data_aug)
#     }
#     )
# 
# # # #Start simple: ir plus random effects
# # # mods <-
# # #   c(
# # #     "mod1" = formula(CO2 ~ s(log_ir, bs="tp") + s(SensorUnit_ID, bs='re')),
# # #     "mod2" = formula(CO2 ~ s(log_ir,  bs="tp") + s(SensorUnit_ID, bs='re') + s(sensirion_sht21_temperature)),
# # #     "mod3" = formula(CO2 ~ s(log_ir, bs="tp") + 
# # #                        s(log_ir, SensorUnit_ID) + 
# # #                        s(sensirion_sht21_temperature, SensorUnit_ID) + s(sensirion_sht21_temperature)),
# # #     "mod4" = formula(CO2 ~ s(log_ir, bs="tp") + 
# # #                        s(log_ir, SensorUnit_ID) + 
# # #                        s(sensirion_sht21_temperature, SensorUnit_ID) + ti(log_ir, sensirion_sht21_temperature))
# # #     )
# # #   
# # # gam_fits <-
# # # map(mods, 
# # #     function(x,data=cal_data_thin)
# # #     {
# # #       mod_1 <- mgcv::bam(x, data=data, 
# # #                          method="REML", family="gaussian")
# # #       cal_data_aug <- augment(mod_1, newdata=data)
# # #       list(mod=mod_1, aug=cal_data_aug)
# # #     }
# # #     )
# # 
# # 
# # form <- formula(CO2 ~ s(log_ir, bs="tp") + 
# #                   s(SensorUnit_ID, bs="re") +
# #                   te(log_ir, sensirion_sht21_temperature, by=Serialnumber))
# # mod_1 <- mgcv::bam(form, data=cal_data_thin, method="REML", family="gaussian")
# # cal_data_aug <- augment(mod_1, newdata=cal_data_thin)
# 
# 
# bind_rows(all_cal_data) %>% 
#   ungroup() %>% 
#   select(date,RH,t, SensorUnit_ID, CO2, senseair_lp8_co2) 
