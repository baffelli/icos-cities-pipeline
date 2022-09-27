
files <- Sys.glob("Z:/CarboSense/temp/k96_data/*.csv")
k96_data <- bind_rows(purrr::map(files, readr::read_csv))

q <- "SELECT *,FROM_UNIXTIME(timestamp) AS date FROM NABEL_DUE WHERE FROM_UNIXTIME(timestamp) >= '2021-08-25 16:00:00'"

pic_data <- lubridate::with_tz(collect(tbl(carboutil::get_conn(user="basi"), sql(q)),"UTC")) %>%
  mutate(date = lubridate::as_datetime(timestamp))




k96_avg <- 
  k96_data %>%
  mutate(date=lubridate::as_datetime(date)) %>%
  filter(lubridate::date(date) %in% unique(lubridate::date(pic_data$date))) %>%
  group_by(date=lubridate::as_datetime(cut(date, "1 min")), sensor_id, map_version) %>%
  summarise_all(list(mean=function(x) mean(x,na.rm=TRUE), sd=function(x) sd(x,na.rm=TRUE))) 




k96_test <- 
k96_avg %>%
  mutate(sensor="K96") %>%
  right_join(pic_data  %>% filter(CO2 !=-999) %>% rename(CO2_pic = CO2_DRY, H2O_pic = H2O)) 


k96_test %>% 
  mutate(CO2_res = CO2_pic - CO2_mean,
         H2O_res = H2O_pic * 1000 - H2O_mean) %>%
  ggplot(aes(x=date)) + 
  geom_point(aes(y=CO2_mean - CO2_pic, color=sensor_id)) +
  geom_errorbar(aes(ymax=CO2_mean - CO2_pic + CO2_sd, ymin=CO2_mean - CO2_pic - CO2_sd))


#Quadruple collocation



dm <- k96_test %>% 
  ungroup() %>%
  pivot_wider(values_from = "CO2_mean", names_from="sensor_id", id_cols=c("CO2_pic","date")) %>%
  select((-date)) %>%
  data.matrix() 


cm <- cov(dm)
#Determine bias

data_means <- colMeans(dm)

bias = c(data_means[2:4] - data_means[1])

#Determine gains

coef <- c(cm[1, 4]/cm[2, 4], cm[1, 3]/cm[2, 3], cm[1, 2]/cm[2, 3], cm[1, 4]/cm[3, 4], cm[1, 2]/cm[2, 4], cm[1, 3]/cm[3, 4])

A <- matrix(data=0, nrow = 6, ncol= 3)

A[1, 1] <- coef[1]
A[2, 1] <- coef[2]
A[3, 2] <- coef[3]
A[4, 2] <- coef[4]
A[5, 3] <- coef[5]
A[6, 3] <- coef[6]


gains <- qr.solve(A, rep(1,6))

gains_df <- tibble(gain=gains, 
                   bias = bias,
                   sensor_id = colnames(dm)[2:4])


k96_test %>% 
  left_join(gains_df) %>%
  mutate(CO2_cal = (CO2_mean + bias)/gain) %>%
  ggplot(aes(x=date, y=CO2_cal - CO2_pic, color=sensor_id)) + 
  geom_point()  
  #geom_point(aes(y=CO2_pic), color='blue')




