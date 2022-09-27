library(carboutil)
library(DBI)
library(dplyr)
library(purrr)
library(tibble)
library(argparse)

parser <- ArgumentParser()


parser$add_argument("-b", "--backfill", action="store_true", default=FALSE, help="Backfill last 3 months")
args <- parser$parse_args()


duration <- if_else(args$backfill, '90d', '2d')
print(paste("Importing last", duration))



get_sens <- function(sens, duration = '2d') {
  dt <- lubridate::with_tz(
    carboutil::query_decentlab(
      carboutil::get_decentlab_api_key(),
      time_filter = stringr::str_interp("time >= now() - ${duration}"),
      device = paste0('/', sens, '/'),
      location = "//",
      sensor = "//",
      agg_func = "mean",
      agg_int = "1m"
    ),
    "UTC"
  )
  dt
}

map_sens <- function(data, sens){
  mapping <-
    list(
      "4056" = c(
        SensorUnit_ID = "sensor",
        timestamp = "time",
        Valve = "calibration",
        CO2 = "vaisala-gmp343-co2",
        H2O = "sensirion-sht21-humidity",
        sensor_temperature = "vaisala-gmp343-temperature",
        air_pressure = "bosch-bmp280-pressure",
        air_temperature = "sensirion-sht21-temperature",
        air_moisture = "sensirion-sht21-humidity"
      ),
      "4057" = c(
        SensorUnit_ID = "sensor",
        timestamp = "time",
        Valve = "calibration",
        CO2 = "licor-li850-co2",
        H2O = "licor-li850-h2o",
        sensor_temperature = "licor-li850-cell-temperature",
        air_pressure = "bosch-bmp280-pressure",
        air_temperature = "sensirion-sht21-temperature",
        air_moisture = "sensirion-sht21-humidity"
      )
    )
  mp <- unlist(mapping[[sens]])
  col_to_add <- setdiff(mp, colnames(data))
  print(col_to_add)
  new_col_mapping <- map(col_to_add, function(x) NA_real_) %>%
  set_names(col_to_add)
  add_column(data, !!!new_col_mapping) %>%
  select(!!!mp) %>%
  mutate(SensorUnit_ID = as.numeric(SensorUnit_ID))
}



sens <- c("4056", "4057")
dt_db <-
map(sens, function(x){
  dt <- get_sens(x, duration = duration)
  map_sens(dt, x)
}) %>% bind_rows()


con <- carboutil::get_conn(group="CarboSense_MySQL")
DBI::dbWriteTable(con, "new_sensors_temp", dt_db, temporary=TRUE)


update_query <- 
  "
REPLACE INTO raw_sensor_data
SELECT   
   SensorUnit_ID,
   timestamp,
   Valve,
   CO2,
   H2O,
   sensor_temperature,
   air_pressure,
   air_temperature,
   air_moisture
FROM new_sensors_temp
"

DBI::dbExecute(con, update_query)



