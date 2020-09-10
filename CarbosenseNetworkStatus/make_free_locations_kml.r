library(carboutil)
library(tidyverse)
library(sf)
library(RMySQL)
con <- carboutil::get_conn(group="CarboSense_MySQL")

col2hex <- function(col){
  do.call(rgb, t(as.list((grDevices::col2rgb(col)/255))))
}



#Color mapping
cm <- map(c('SWISSCOM'='green',
        'EMPA'='blue',
        'UNIBE'='cyan',
        'METEOSWISS'='red'), function(x) col2hex(x))


q <-
"SELECT
*
FROM Location
WHERE LocationName NOT IN
(
SELECT DISTINCT LocationName
FROM Deployment
)
AND Canton NOT IN ('F','DE')
"


sens <- dplyr::tbl(con, sql(q)) %>%
  collect() %>%
  sf::st_as_sf(coords=c("Y_LV03","X_LV03"),crs=21781) %>%
  mutate(
    NAME=LocationName,
    COLOR=cm[match(Network, names(cm))],
    OGR_STYLE=stringr::str_glue("SYMBOL(c:{COLOR})"))



  sf::st_write(sens, delete_dsn  = TRUE,
               dsn = "/project/CarboSense/Carbosense_Network/CarboSense_KML/free_locations.kml",
               driver = "kml")
