#Check Location
SELECT
*
FROM Location 
WHERE Network NOT 
IN ('EMPA','METEOSWISS','NABEL','METAS','SWISSCOM','UNIBE');

#Check Deployment
SELECT
*
FROM Deployment
WHERE LocationName NOT IN
(
	SELECT DISTINCT
		LocationName
	FROM Location
);
SELECT
*
FROM Deployment
WHERE SensorUnit_ID NOT IN
(
	SELECT DISTINCT
		SensorUnit_ID
	FROM SensorUnits
);
#Check calibration
SELECT
*
FROM Calibration
WHERE LocationName NOT IN
(
	SELECT DISTINCT
		LocationName
	FROM Location
);


SELECT
*
FROM Calibration
WHERE SensorUnit_ID NOT IN
(
	SELECT DISTINCT
		SensorUnit_ID
	FROM SensorUnits
);
-- Check if any calibration entry which does not have a deployment
SELECT
*
FROM Calibration AS cal
WHERE SensorUnit_ID NOT IN
(
	SELECT DISTINCT
	SensorUnit_ID
	FROM Deployment AS dep	
);

-- Check that calibration are in the right order


DROP TABLE IF EXISTS tmp_cal;
CREATE  TABLE IF NOT EXISTS tmp_cal
AS
(
SELECT
*,
(SELECT COUNT(*) FROM Calibration AS Ci WHERE Ci.SensorUnit_ID = Co.SensorUnit_ID  AND Ci.Date_UTC_from < Co.Date_UTC_from) AS rn
FROM Calibration AS Co
);

DROP TABLE IF EXISTS tmp_depl;
CREATE  TABLE IF NOT EXISTS tmp_depl
AS
(
SELECT
*,
(SELECT COUNT(*) FROM Deployment AS Ci WHERE Ci.SensorUnit_ID = Co.SensorUnit_ID  AND Ci.Date_UTC_from < Co.Date_UTC_from) AS rn
FROM Deployment AS Co
);

-- Perform the check for calibration
SELECT
*
FROM tmp_cal AS cal
	LEFT JOIN tmp_cal AS cal1
ON cal.SensorUnit_ID = cal1.SensorUnit_ID
AND cal.rn = cal1.rn - 1
WHERE cal.Date_UTC_to > cal1.Date_UTC_from;



DROP TABLE tmp_cal;	
DROP TABLE tmp_depl;		

-- Check that there are no cases where calibration and deployment differ (calibration is marked in a place while there is an ongoing deployment somewhere else)
SELECT
*
FROM Calibration AS Cal	
JOIN Deployment AS Dep
WHERE Cal.SensorUnit_ID = Dep.SensorUnit_ID
AND Cal.LocationName <> Dep.LocationName
AND Cal.Date_UTC_from BETWEEN Dep.Date_UTC_from AND Dep.Date_UTC_to
