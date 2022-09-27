setwd("K:/ICOS-Cities/Network/Scripts/")
#library(checkpoint)
#checkpoint::create_checkpoint("2021-10-01", log=FALSE)
library(tidyverse)
library(recipes)
library(parsnip)
library(workflows)
library(timetk)


get_cal_data <- function(con, si) {
  q <-
    "
WITH
mapping AS (WITH name_mapping(table_name, location_name) AS (VALUES ('NABEL_DUE','DUE1'),('ClimateChamber_00_DUE','ClimateChamber_00_DUE'), ('PressureChamber_01_DUE','PressureChamber_01_DUE'),('PressureChamber_00_DUE','PressureChamber_00_DUE'))
SELECT
	*
FROM name_mapping
)


SELECT
	cal.SensorUnit_ID,
	sen.Serialnumber,
	nm.table_name,
	nm.location_name,
	cal.CalMode,
	UNIX_TIMESTAMP(cal.Date_UTC_from) AS timestamp_from,
	UNIX_TIMESTAMP(LEAST(cal.Date_UTC_to, CURDATE())) AS timestamp_to
FROM Calibration as cal
JOIN Sensors AS sen ON sen.SensorUnit_ID = cal.SensorUnit_ID AND cal.Date_UTC_from > sen.Date_UTC_from
JOIN mapping AS nm ON cal.DBTableNameRefData = nm.table_name
WHERE cal.SensorUnit_ID > 500 AND sen.Type = 'LP8' AND cal.SensorUnit_ID = {si} AND cal.CalMode IN (1,2,3)
"
q_interp <- glue::glue_sql(q, .con=con)
dplyr::collect(dplyr::tbl(con, dplyr::sql(q_interp)))
}

get_ref_data <- function(con, location, timestamp_from, timestamp_to){
  q <- "
  SELECT
  (timestamp DIV 600) * 600 AS timestamp,
  LocationName,
  AVG(NULLIF(CO2_DRY, -999)) AS CO2_DRY_pic,
  AVG(NULLIF(CO2_DRY, -999) * (1 - NULLIF(H2O, -999)/100)) AS CO2_pic,
  AVG(NULLIF(RH, -999)) AS RH_pic,
  AVG(NULLIF(T, -999)) AS T_pic,
  AVG(NULLIF(H2O, -999)) AS H2O_pic,
  pressure AS p
  FROM picarro_data
  WHERE timestamp BETWEEN {timestamp_from} AND {timestamp_to} AND LocationName = {location}
  GROUP BY timestamp DIV 600, LocationName
  "
  q_interp <- glue::glue_sql(q, .con=con)
  print(q_interp)
  dplyr::collect(dplyr::tbl(con, dplyr::sql(q_interp)))
}


get_sens_data <- function(unit, timestamp_from, timestamp_to){
  q <-
  "
  SELECT
  *
  FROM lp8_data
  WHERE time BETWEEN {timestamp_from} AND {timestamp_to} AND SensorUnit_ID = {unit}
  "
  q_interp <- glue::glue_sql(q, .con=con)
  print(q_interp)
  dt <- dplyr::collect(dplyr::tbl(con, dplyr::sql(q_interp)))
  print(dt)
  dt
}

con <- carboutil::get_conn(user="basi")

date_con <- function(x)  carboutil::format_decentlab_date(lubridate::as_datetime(as.numeric(x)))

cd <- get_cal_data(con, 1060)
sensor_data <-
  cd %>% group_by(1:n(),Serialnumber) %>%
  group_modify(
    function(x, y)
      get_sens_data(
        x$SensorUnit_ID,
        (x$timestamp_from[[1]]),
        (x$timestamp_to[[1]])
      )
  )

ref_data <- cd %>%
  group_by(Serialnumber, CalMode, timestamp_from, timestamp_to, location_name) %>%
  group_modify(function(x,y) get_ref_data(con, y$location_name[[1]], y$timestamp_from[[1]], y$timestamp_to[[1]])) %>%
  mutate(time=(as.numeric(timestamp)))

t0 <- 273.15

diff_full <- function(v){c(0, diff(v))}

magnus_formula <- function(t, rh){
  H <- (log10(rh)-2)/0.4343 + (17.62*t)/(243.12+t);
   243.12*H/(17.62-H);
}

dry_to_wet <- function(co2, h2o){
  co2 / (1-h2o/100)
}




filter_meas <- function(data, max_RH){
  # Filtering 0 : based on sensor state
  cond_0 <- !is.na(data[["sensirion_sht21_humidity"]]) &
    data[["sensirion_sht21_humidity"]] <= max_RH &
    !is.na(data[["senseair_lp8_ir"]]) &
    !is.na(data[["log_ir"]]) &
    (data[["CalMode"]]!=2 | (data[["CalMode"]]==2 & data[["sensirion_sht21_humidity"]]<=70)) &
    (data[["CalMode"]] %in% c(2,3) | data[["CalMode"]]==1 & (data[['diff_sht21_t_dp']]>=5
                                                             | between(data[['diff_T_SHT_LP8']], -0.2, 0.7)
                                                             | data[["sensirion_sht21_humidity"]] < 75
                                                             | between(data[["d_lp8_T"]], -0.05, 0.05)))

  # Filtering 1: based on state change
  cond_1 <-
    !(between(data[['deltat']], 500, 600) & abs(data[['delta_rh']]) > 5 |
        between(data[['deltat']], 500, 700) & abs(data[['delta_ir']]) > 400 & data[['CalMode']] == 1 |
        between(data[['deltat']], 500, 700) & abs(data[['delta_ir']]) > 200 & data[['CalMode']] == 2)
  cond_1 & cond_0
}

#Fit temperature gradient (used for filtering)

fit_temp <- function(df, t_var){
  df <- df[is.finite(df[[t_var]]),]
  df$t <- 1:nrow(df)
  coefficients(lm.fit(x=matrix(c(rep(1,nrow(df)), df$t),ncol=2),y=df[[t_var]]))[[2]]
}


extent <- function(x) abs(diff(range(x)))

make_d_ir <- function(dt, cal_mode, dur='14 days'){
  dtv <- cut(dt, dur)
  changes <- which((cal_mode != lag(cal_mode, 1,default = 1)) | (lag(dtv, 1) != dtv))
  cv <- findInterval(1:length(cal_mode), changes)
  as_factor(cv)
  # print(unique(cv))
  # day_fact <- cut(dt, '14 days')
  # (if_else(
  #   (dt - min(dt)) < lubridate::as.period('24 months'), as.character(day_fact), rep("later", length(day_fact))
  # ))
}


reference_conc <- function(CO2, t){
  CO2 * t / t0
}


make_cal_table <- function(fit, timestamp_from, timestamp_to, serialnumber){
  ft <- broom::tidy(fit)
  tibble(Serialnumber=serialnumber,
         computation_date = as.numeric(lubridate::now(tzone = 'UTC'),
         data_timestamp_from = timestamp_from,
         timestamp_to = timestamp_to,
         parameter=ft[['term']],
         value=ft[['estimate']])
}


#prepare calibration data
cal_data <-
  inner_join(ref_data, sensor_data, by = c("time", "Serialnumber")) %>%
  group_by(Serialnumber) %>%
  mutate(
    date = (lubridate::as_datetime(as.numeric(timestamp))),
    year = lubridate::year(date),
    month = lubridate::month(date),
    t_abs = sensirion_sht21_temperature  + t0,
    CO2_pic_wet = dry_to_wet(CO2_pic, H2O_pic),
    CO2_pic_ref = CO2_pic_wet * t0 / (t_abs),
    deltat = diff_full(as.numeric(timestamp)),
    delta_ir = diff_full(senseair_lp8_ir),
    delta_rh = diff_full(sensirion_sht21_humidity),
    delta_t = diff_full(sensirion_sht21_temperature),
    sht21_dp = magnus_formula(sensirion_sht21_temperature, sensirion_sht21_humidity),
    diff_sht21_t_dp = sensirion_sht21_temperature - sht21_dp,
    diff_T_SHT_LP8 = sensirion_sht21_temperature - senseair_lp8_temperature,
    log_ir = log(senseair_lp8_ir),
    t_sq = t_abs^2,
    t_cub = t_abs^3,
    t_ir = t_abs / senseair_lp8_ir,
    t_ir_sq = (t_abs)^2 /senseair_lp8_ir ,
    t_ir_cub = (t_abs)^3 /senseair_lp8_ir,
    d_ir = make_d_ir(date, CalMode)
  ) %>%
  dplyr::ungroup() %>%
  filter(Serialnumber == 1060) %>%
  arrange(date) %>%
  ungroup() %>%
  group_by(Serialnumber) %>%
  mutate(d_lp8_T = slider::slide(.x = cur_data(), function(x)
    fit_temp(x, 'senseair_lp8_temperature'),
    .after = 6)) %>%
  unnest(d_lp8_T) %>%
  mutate(valid_for_cal = filter_meas(cur_data(), 75))










test_train <- rsample::initial_split(cal_data, prop=0.5, strata='valid_for_cal')
train_data <- rsample::analysis(test_train)
test_data  <- rsample::assessment(test_train)



#Create recipe
frms <- list(model_4= formula(CO2_pic_ref ~ log_ir + log_ir^2 + t_abs + t_sq + t_cub + t_ir + t_ir_sq + t_ir_cub + d_ir + year ),
             model_5= formula(CO2_pic_ref ~ log_ir + t_abs + t_sq + t_cub + d_ir))

extras <- formula( ~ . + d_ir + valid_for_cal + CalMode)

cal_rec <- map(frms, function(x) recipes::recipe(update.formula(x,extras) , train_data) %>%
                 recipes::update_role(valid_for_cal,new_role = 'splitting') %>%
                 recipes::update_role(CalMode, new_role='weight') %>%
                 recipes::step_mutate(year=as_factor(year))  %>%
                 recipes::step_dummy(d_ir, year))


#Extract fformula from recipe
cr <- cal_rec[[1]] %>% prep()
cf <- as.formula(cr)


#Prepare data
train_set <- recipes::bake(cr, train_data)
test_set <- recipes::bake(cr, test_data)


train_set_filter <- filter(train_set, valid_for_cal)
wt <-  add_count(train_set_filter, CalMode, name="ct") %>% mutate(mode_prop = ct / n())
ft <- MASS::rlm(cf, data=train_set_filter, weights = 1/wt$mode_prop, psi=MASS::psi.huber, k=1.345)
#Copy formula but remove d_ir except for the last

frm1 <- update.formula(frms[[1]], . ~ . - d_ir - year)
ft1 <- lm(frm1, data=train_set)
ft1$coefficients <- ft$coefficients[1:length(ft1$coefficients)]
#ft1$coefficients[1] <- ft$coefficients[1] + ft$coefficients[length(ft$coefficients)]


#Extract the parameters
broom::tidy(ft1)



cal_test_pred <- bind_cols(test_data ,fitted=predict(ft1, newdata=test_set) * test_set$t_abs / t0)
cal_train_pred <- bind_cols(train_data,fitted= predict(ft1, newdata=train_set) * train_set$t_abs / t0)
all_pred <- rbind(cal_test_pred, cal_train_pred)

pic_lab <- expression("reference CO"[2]~"concentration"~bgroup('[',ppm,']'))
meas_lab <- expression("calibrated CO"[2]~"concentration"~bgroup('[',ppm,']'))


ggplot(all_pred %>% filter(valid_for_cal), aes(x=CO2_pic_wet, y=fitted)) + geom_point(shape=46) +
  geom_abline(aes(slope=1, intercept=-20)) +
  geom_abline(aes(slope=1, intercept=20)) +
  xlim(300,1000) + ylim(300,1000) + xlab(pic_lab) + ylab(meas_lab) +
  ggtitle(paste("Cross-validation results for", unique(cal_test_pred$Serialnumber)))





ggplot(all_pred, aes(x=date)) +
  geom_point(aes(y=fitted), shape=46) +
  geom_point(aes(y=CO2_pic_wet), color='blue', shape=46) +
  ylim(300,1000) + xlab('date') + ylab(meas_lab)



#Plot histogram
ggplot(all_pred) +
  geom_histogram(aes(x=CO2_pic_wet - fitted), bins=100) +
  xlim(-100,100) +
  xlab(meas_lab) +
  geom_vline(aes(xintercept=-20)) +
  geom_vline(aes(xintercept=20)) +
  geom_vline(aes(xintercept=0)) + facet_wrap('year')




#Make a humidity ts
#
# clu_data <-
#   cal_train_pred  %>% filter(CalMode==1 & Serialnumber == 1060  & year==2017) %>%
#   ungroup() %>%
#   mutate(dt=lubridate::date(date), tm=hms::as_hms(date), RH=sensirion_sht21_temperature)
#
# clu_data_wide <- clu_data %>%
#   pivot_wider(id_cols = tm, values_from = RH, names_from = dt) %>%
#   mutate_if(is.numeric, function(x) coalesce(x, mean(c_across(-tm),na.rm=TRUE)))
#
#
#
#
# clsts <- dtwclust::tsclust(series=dplyr::select(clu_data_wide,-tm) %>% t() , type="h", k=5, args=dtwclust::tsclust_args(dist = list(window.size = 144L)))
#
# clsts_tb <- tibble(cluster=clsts@cluster, dt=lubridate::as_date(names(clsts@cluster)))
#
# clu_data_warp <-
# clu_data %>%
#   left_join(clsts_tb) %>%
#   group_by(cluster, dt) %>%
#   group_modify(function(x, y)
#     {
#     ci <- y[['cluster']][[1]]
#     ai <- dtw::warp(dtw::dtw(x[["RH"]], clsts@centroids[[ci]]))
#     x[as.integer(ai),]
#   })
#
#
# # clu_data_warp  <-
# #   map2_dfc(clu_data_wide %>% dplyr::select(-tm), clsts@cluster, function(x,y) x[dtw::warp(dtw::dtw(x, clsts@centroids[[y]]))]) %>%
# #   cbind(tm=clu_data_wide$tm) %clu>%
# #   pivot_longer(-tm, names_to='dt', values_to="RH_warp") %>%
# #   mutate(dt=lubridate::as_date(dt))
# #
# #
# #
# #
# clu_data_warp %>%
#   arrange(dt, tm) %>%
# ggplot(aes(x=RH, y=fitted, color=dt, group=dt)) + geom_path() + facet_wrap("cluster")
