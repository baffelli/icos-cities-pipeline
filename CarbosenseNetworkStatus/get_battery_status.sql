SELECT
 lp8_data.SensorUnit_ID,
 FROM_UNIXTIME(time) AS date,
 battery
FROM lp8_data
JOIN
(
SELECT
	SensorUnit_ID,
	MAX(time) AS lt
FROM lp8_data
GROUP BY SensorUnit_ID
) AS mx
ON mx.lt = lp8_data.time AND mx.SensorUnit_ID = lp8_data.SensorUnit_ID