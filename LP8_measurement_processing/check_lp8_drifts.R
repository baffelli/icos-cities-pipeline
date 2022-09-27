library(dplyr)
library(carboutil)
library(ggplot2)

q <- 
"
SELECT
SensorUnit_ID, co2.LocationName,
AVG(CO2_A - CO2) AS CO2_DRIFT,
loc.Network,
YEARWEEK(FROM_UNIXTIME(timestamp)) AS yw,
FROM_UNIXTIME(min(timestamp)) AS date
FROM CarboSense_CO2_TEST00 AS co2
JOIN Location AS loc
ON loc.LocationName = co2.LocationName
WHERE SensorUnit_ID IN
(
	SELECT DISTINCT
		SensorUnit_ID
	FROM Deployment
	WHERE Date_UTC_to > CURDATE()
	AND SensorUnit_ID BETWEEN 1010 AND 1340 AND LocationName NOT LIKE 'DUE%'
)
AND CO2_A <> -999 AND DATEDIFF(CURDATE(), FROM_UNIXTIME(timestamp)) < 100 AND CO2 <> -999 AND LRH_FLAG = 1
GROUP BY SensorUnit_ID, co2.LocationName, Network, yw
"




dt <- lubridate::with_tz(collect(tbl(carboutil::get_conn(user="basi"), sql(q))), "UTC") %>%
  mutate(SensorUnit_ID = forcats::fct_reorder(as_factor(SensorUnit_ID), CO2_DRIFT, .fun=function(x) mean(abs(x), na.rm=TRUE))) 



%>%
  ggplot(aes(x=SensorUnit_ID, y=CO2_DRIFT)) + 
  geom_boxplot() +
  geom_hline(aes(yintercept=-250)) + geom_hline(aes(yintercept=250)) +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))


ggplot(dt, aes(x=date, y=CO2_DRIFT)) + geom_line() + facet_wrap("SensorUnit_ID")