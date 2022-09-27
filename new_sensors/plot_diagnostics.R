library(carboutil)
library(DBI)
library(dplyr)
library(zoo)
library(tidyverse)
#Load data from DB
q <- "
WITH ns AS
(
SELECT
  rs.timestamp AS date,
  rs.SensorUnit_ID,
  rs.sensor_temperature AS temperature,
  rs.air_pressure AS pressure,
  rs.air_moisture AS rh,
  rs.CO2 AS CO2,
  rs.H2O AS H2O,
  rs.Valve AS valve
FROM raw_sensor_data AS rs
LEFT JOIN Deployment AS dep
	ON dep.SensorUnit_ID = rs.SensorUnit_ID AND rs.timestamp BETWEEN dep.Date_UTC_from and dep.Date_UTC_to
LEFT JOIN RefGasCylinder_Deployment AS dep_gas
	ON dep.LocationName = dep_gas.LocationName AND dep.SensorUnit_ID = dep_gas.SensorUnit_ID AND dep_gas.Date_UTC_from >= dep.Date_UTC_from AND dep_gas.Date_UTC_to <= dep.Date_UTC_to
LEFT JOIN RefGasCylinder AS ref_cylinder
  ON ref_cylinder.CylinderID = dep_gas.CylinderID AND ref_cylinder.Date_UTC_from <= dep_gas.Date_UTC_from
  WHERE rs.CO2 IS NOT NULL AND rs.timestamp > {start_date}
),
pic AS
(
SELECT
  FROM_UNIXTIME(timestamp) AS date,
  1 AS SensorUnit_ID,
  T AS temperature,
  pressure AS pressure,
  RH AS rh,
  (CASE WHEN CO2_DRY_CAL <> -999 OR CO2_F = 1 THEN CO2_DRY_CAL ELSE NULL END) AS CO2,
  H2O AS H2O,
  0 AS valve
FROM NABEL_DUE 
WHERE FROM_UNIXTIME(timestamp) > {start_date}
),
HPP AS
(
SELECT
  FROM_UNIXTIME(timestamp) AS date,
  SensorUnit_ID AS SensorUnit_ID,
  T AS temperature,
  pressure AS pressure,
  RH AS rh,
  CO2_CAL_DRY AS CO2,
  NULL AS H2O,
  Valve AS valve
FROM CarboSense_HPP_CO2 
WHERE FROM_UNIXTIME(timestamp) > {start_date} AND LocationName = 'DUE1' AND SensorUnit_ID = 442 AND Valve = 0
)

SELECT
*
FROM ns
UNION ALL
SELECT
*
FROM pic
UNION ALL
SELECT
*
FROM HPP
"
con <- carboutil::get_conn(user="basi")
start_date <- lubridate::as_datetime('2021-08-26 17:00:00')
q_interp <- glue::glue_sql(q, .con = con)
meas <- lubridate::with_tz(collect(tbl(con, sql(q_interp)),"UTC"))



#Start of series
start_date <- lubridate::as_date('2021-08-25 16:00:00')

#Load K96 data

plot_dir <- "Z:/CarboSense/new_sensor_testing/"

k96_files <- Sys.glob("Z:/CarboSense/temp/k96_data/*.csv")

valid <- map(k96_files, function(x) lubridate::as_date(stringr::str_extract(x, "\\d{8}"))) >= start_date

k96_data <- bind_rows(purrr::map(k96_files[valid], readr::read_csv))


# Function for aggregation
sum_fun <- list(mean = function(x) mean(x, na.rm=TRUE), sd = function(x) sd(x, na.rm=TRUE))



#Average K96 to the minute

k96_minute <- k96_data %>%
  rename(SensorUnit_ID = sensor_id) %>%
  mutate(date = lubridate::as_datetime(date)) %>%
  filter(date > start_date) %>%
  group_by(SensorUnit_ID, date = lubridate::floor_date(date ,"10 minutes")) %>%
  complete(SensorUnit_ID, date = seq(min(date), max(date), "10 mins")) %>%
  #Water compensation
  mutate(
    CO2_WET = CO2,
    CO2 = CO2 * (1 - 1e-6 *  H2O)) %>%
  summarise_all(sum_fun)


dt_minute <-
  meas %>%
  filter(date > start_date) %>%
  group_by(SensorUnit_ID, date = lubridate::floor_date(date ,"10 minutes")) %>%
  complete(SensorUnit_ID, date = seq(min(date), max(date), "10 mins")) %>%
  summarise_all(sum_fun) 


sensor_data <- 
  bind_rows(
    k96_minute,
    dt_minute %>% filter(SensorUnit_ID != 1) %>% mutate(SensorUnit_ID = as.character(SensorUnit_ID))
    )  %>%
  inner_join(dt_minute %>% ungroup() %>% filter(SensorUnit_ID == 1) %>% select(date, CO2_pic =CO2_mean) , by="date") %>%
  filter(date > lubridate::as_datetime('2021-08-25 17:00:00')) %>%
  mutate(sensor = case_when(SensorUnit_ID == "4056" ~ "Vaisala GMP",
                            SensorUnit_ID == "4057" ~ "Licor LI 850",
                            str_detect(SensorUnit_ID , "4\\d{2}") ~ "SenseAir HPP",
                            str_detect(SensorUnit_ID , "0100.*") ~ "SenseAir K96")) %>%
  filter(date > lubridate::as_datetime('2021-08-27 00:00:00'))












# Calibration formula
models <-
  list("1 point" = formula(CO2_pic - CO2_mean ~ 1),
       "2 point" = formula(CO2_pic ~ CO2_mean))

# Determine a split of data in calibration and testing



plot_data <- sensor_data %>%
  group_by(SensorUnit_ID, sensor) %>%
  group_modify(
    function(x,y) {
      training_val <- x %>% mutate(train = date - min(date) < lubridate::weeks(x=1)) %>% pull(train)
      ind <- list(analysis = which(training_val), assessment=which(!training_val))
      spl <- rsample::make_splits(ind, x) 
      fit <- map(models, function(form) lm(form, rsample::analysis(spl)))
      # coeff <- broom::tidy(fit)
      # glance <- broom::glance(fit)
      predicted <- map(fit, function(ft) broom::augment(ft, newdata = x) %>% mutate(train=training_val))
      bind_rows(predicted, .id = "model")
    }

    )  %>%
  ungroup() %>%
  mutate(CO2_cal_mean = if_else(model == "1 point", CO2_mean + .fitted, .fitted))


training_period <- plot_data %>% group_by(SensorUnit_ID, sensor) %>% arrange(desc(date)) %>% filter(train) %>% slice(1)
#Prepare data for plotting

yl <- expression(CO[2] ~ bgroup("[", ppm, "]"))




plot_data %>%
  ungroup() %>%
  group_by(model) %>%
  group_walk(function(x, y) {
    plot_type <- stringr::str_replace_all(unique(y$model), " ", "_")
    #Normal timeseries
    tsplot <-
      ggplot(x, aes(x = date, group = SensorUnit_ID)) +
      geom_line(aes(y = CO2_cal_mean, color = sensor), linetype = 1) +
      #geom_errorbar(aes(ymin=CO2_cal_mean - CO2_sd, ymax=CO2_cal_mean + CO2_sd)) +
      geom_line(aes(y = CO2_pic, group = SensorUnit_ID), size = 0.4) + ylab(yl) +
      geom_vline(data = training_period, aes(xintercept=date), color="darkgrey", size=1)
      
    
    ggsave(tsplot, filename = file.path(plot_dir, paste0(plot_type, "_sensor_timeseries.png")))
    
    
    #Difference to picarro
    yl <- expression("Difference" ~ CO[2] ~ bgroup("[", ppm, "]"))
    difference_plot <-
      ggplot(x,
             aes(
               x = date,
               y = CO2_cal_mean - CO2_pic,
               group = SensorUnit_ID
             )) +
      geom_line(aes(color = sensor)) +  ylab(yl)
    
    
    ggsave(difference_plot,
           filename = file.path(plot_dir, paste0(plot_type,"_sensor_deviation_timeseries.png")))
    
    
    
    #Difference to picarro vs Rh
    yl <- expression("Difference" ~ CO[2] ~ bgroup("[", ppm, "]"))
    xl <- "Relative Humidity"
    difference_rh_plot <-
      ggplot(x,
             aes(
               x = rh_mean,
               y = CO2_cal_mean - CO2_pic,
               group = SensorUnit_ID
             )) +
      geom_point(aes(color = sensor)) +  ylab(yl) + xlab(xl) + facet_wrap("sensor", scales =
                                                                            'free_x')
    
    
    ggsave(difference_rh_plot,
           filename = file.path(plot_dir, paste0(plot_type, "_sensor_deviation_rh.png")))
    
    
    #scatterplot
    yl <- expression("Sensor" ~ CO[2] ~ bgroup("[", ppm, "]"))
    xl <- expression("Reference" ~ CO[2] ~ bgroup("[", ppm, "]"))
    scatter_plot <-
      ggplot(x,
             aes(x = CO2_pic, y = CO2_cal_mean, group = SensorUnit_ID)) +
      geom_point(aes(color = sensor)) + geom_abline() + facet_wrap("sensor") + xlab(xl) + ylab(yl)
    
    ggsave(scatter_plot, filename = file.path(plot_dir, paste0(plot_type, "_sensor_vs_picarro.png")))
    
  })




calibration_summary <-   plot_data %>%
  group_by(sensor, SensorUnit_ID, model) %>%
  summarise(precision = sd(CO2_cal_mean - CO2_pic, na.rm=TRUE),
            linearity = cor(CO2_cal_mean, CO2_pic, use = "pairwise.complete.obs"),
            accuracy  = sqrt(mean(abs(CO2_cal_mean - CO2_pic)^2, na.rm=TRUE)))

#Fit calibration model
#dt_train <- filter(rsample::training(dt_cal_split),  cal_valid)


# #Compare models
# cal_fit_base <-  lm(CO2_pic_mean ~  1, data = dt_train )
# cal_fit <-  lm(CO2_pic_mean ~  CO2_mean, data = dt_train )
# cal_fit_temp <-  lm(CO2_pic_mean ~  CO2_mean + CO2_mean:sensor_temperature_mean + I(sensor_temperature_mean^2) + I(sensor_temperature_mean^3)  , data = dt_train )
# 
# 
# 
# #Compute statistics
# min_dt <- min(dt_train$timestamp)
# max_dt <- max(dt_train$timestamp)
# fit_stat <- broom::glance(cal_fit)
# 
# dt_testing <-
#   rsample::testing(dt_cal_split)  %>%
#   bind_cols(CO2_cal=predict(cal_fit, .))
# 
# 
# dt_std <- dt_testing %>%
#   filter(cal_valid) %>% summarise(RMSE = sqrt(mean(abs(CO2_pic_mean - CO2_cal)^2)))
# 
# 
# 
# 
# 
# 
# 
# lab <- bquote("Data between"~.(as_character(strftime(min_dt)))~and~.(as_character(strftime(max_dt)))~","~R^2==.(fit_stat$r.squared)
#               ~"RMSE"==.(dt_std$RMSE)~"ppm")
# xl <- expression("Picarro"~CO[2]~bgroup("[",ppm,"]"))
# yl <- expression("Vaisala GMP"~CO[2]~bgroup("[",ppm,"]"))
# 
# 
# 
# 
# 
# #
# 
# 
# ggplot(dt_testing %>% filter(cal_valid), aes(x=CO2_pic_mean, y=CO2_cal)) + 
#   geom_point()  + 
#   geom_point(aes(y=CO2_mean),color="blue") +
#   geom_errorbar(aes(ymin=CO2_cal - CO2_sd, ymax=CO2_cal + CO2_sd)) +
#   geom_errorbarh(aes(xmin=CO2_pic_mean - CO2_pic_sd, xmax=CO2_pic_mean + CO2_pic_sd)) +
#   geom_abline(aes(slope=1,intercept=0)) +
#   geom_abline(aes(slope=1,intercept=-2)) +
#   geom_abline(aes(slope=1,intercept=2)) +
#   xlab(xl) + ylab(yl) + ggtitle(label="Comparison Picarro-Vaisala GMP 343", subtitle = lab)
# 
# 
# xl <- "Date"
# yl <- expression(CO[2]~bgroup("[",ppm,"]"))
# ggplot(dt_testing, aes(x=timestamp)) +
#   geom_line(aes(y=CO2), color="blue", linetype=2)  +
#   geom_line(aes(y=CO2_cal), color="purple")  +
#   geom_line(aes(y=CO2_pic), color="green") +
#   xlab(xl) + ylab(yl)  + facet_wrap("cal_valid")
# 
# 
# 
# #Test QC
# dm <- sensor_data %>% 
#   ungroup() %>%
#   filter(date > lubridate::as_date('2021-08-26 18:00:00')) %>%
#   pivot_wider(values_from = "CO2_mean", names_from="SensorUnit_ID", id_cols=c("CO2_pic_mean","date")) %>%
#   select((-date)) %>%
#   data.matrix() 
# 
# 
# cm <- cov(dm)
# #Determine bias
# 
# data_means <- colMeans(dm)
# 
# bias = c(data_means[2:4] - data_means[1])
# 
# #Determine gains
# 
# coef <- c(cm[1, 4]/cm[2, 4], cm[1, 3]/cm[2, 3], cm[1, 2]/cm[2, 3], cm[1, 4]/cm[3, 4], cm[1, 2]/cm[2, 4], cm[1, 3]/cm[3, 4])
# 
# A <- matrix(data=0, nrow = 6, ncol= 3)
# 
# A[1, 1] <- coef[1]
# A[2, 1] <- coef[2]
# A[3, 2] <- coef[3]
# A[4, 2] <- coef[4]
# A[5, 3] <- coef[5]
# A[6, 3] <- coef[6]
# 
# 
# gains <- qr.solve(A, rep(1,6))
# 
# gains_df <- tibble(gain=gains, 
#                    bias = bias,
#                    sensor_id = colnames(dm)[2:4])
# 
# 
# k96_test %>% 
#   left_join(gains_df) %>%
#   mutate(CO2_cal = (CO2_mean + bias)/gain) %>%
#   ggplot(aes(x=date, y=CO2_cal - CO2_pic, color=sensor_id)) + 
#   geom_point()  
# #geom_point(aes(y=CO2_pic), color='blue')
# 
# 
# 
# 
# #Plot
# ggplot(sensor_data %>% filter(date > lubridate::as_date('2021-08-25')), aes(y=CO2_mean, x=date, color=SensorUnit_ID)) + geom_point() + geom_point(aes(y=CO2_pic_mean), color="black") + ylim(350, 550)