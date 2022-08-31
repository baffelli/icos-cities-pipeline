WITH latest_cyl AS
(
SELECT
*
FROM
(
SELECT
rgd.CylinderID,
rgd.LocationName,
rgd.SensorUnit_ID,
rg.Date_UTC_from AS filling_date,
rgd.Date_UTC_from AS deployment_date,
rgd.Date_UTC_to  AS deployment_end_date,
rg.CO2,
ROW_NUMBER() OVER (PARTITION BY rg.CylinderID, rg.Date_UTC_from, rgd.LocationName, rgd.SensorUnit_ID ORDER BY rgd.Date_UTC_from) AS bottle_filling_id
FROM RefGasCylinder_Deployment AS rgd
	JOIN  RefGasCylinder AS rg
	ON rg.CylinderID = rgd.CylinderID
	WHERE rgd.Date_UTC_from >= rg.Date_UTC_from
) AS cl 
 WHERE bottle_filling_id = 1
)

SELECT
 AVG(CO2_CAL),
 AVG(Cylinder_CO2) AS CO2_cylinder,
 AVG(CO2_CAL-Cylinder_CO2) AS CO2_adjustement,
 LocationName,
 SensorUnit_ID,
 calibration_counter,
 DATE(from_unixtime(timestamp)) AS date
FROM
(
SELECT 
	hp.*,
	lc.CylinderID,
	lc.CO2 AS Cylinder_CO2,
	ROW_NUMBER() OVER (PARTITION BY LocationName, SensorUnit_ID, deployment_date ORDER BY deployment_date) AS deployment_counter,
	ROW_NUMBER() OVER (PARTITION BY LocationName, SensorUnit_ID, DATE(from_unixtime(timestamp)) ORDER BY DATE(from_unixtime(timestamp))) AS calibration_counter
FROM CarboSense_HPP_CO2 AS hp
	JOIN latest_cyl AS lc
		ON lc.SensorUnit_ID = hp.SensorUnit_ID
		AND hp.LocationName = hp.LocationName
		AND FROM_UNIXTIME(timestamp) BETWEEN deployment_date AND deployment_end_date
	JOIN Deployment AS dep
		ON dep.LocationName = hp.LocationName
		AND dep.SensorUnit_ID = hp.SensorUnit_ID
		AND  dep.Date_UTC_from = deployment_date  AND dep.Date_UTC_to = deployment_end_date
WHERE Valve = 1) AS dt
GROUP BY LocationName, SensorUnit_ID, deployment_counter, calibration_counter, DATE(from_unixtime(timestamp))
ORDER BY calibration_counter DESC
