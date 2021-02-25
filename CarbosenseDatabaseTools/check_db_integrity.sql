-- Check Location

-- Value of 'Network' must be in select list
SELECT
*
FROM Location 
WHERE Network NOT 
IN ('EMPA','METEOSWISS','NABEL','METAS','SWISSCOM','UNIBE');

-- Check Deployment

--  'LocationName' has to be defined in table 'Location'
SELECT
*
FROM Deployment
WHERE LocationName NOT IN
(
	SELECT DISTINCT
		LocationName
	FROM Location
);
-- 'SensorUnit_ID' has to be defined in table 'SensorUnits'
SELECT
*
FROM Deployment
WHERE SensorUnit_ID NOT IN
(
	SELECT DISTINCT
		SensorUnit_ID
	FROM SensorUnits
);

-- Check calibration

-- 'LocationName' has to be defined in table 'Location'
SELECT
*
FROM Calibration
WHERE LocationName NOT IN
(
	SELECT DISTINCT
		LocationName
	FROM Location
);

-- 'SensorUnit_ID' has to be defined in table 'SensorUnits'

SELECT
*
FROM Calibration
WHERE SensorUnit_ID NOT IN
(
	SELECT DISTINCT
		SensorUnit_ID
	FROM SensorUnits
);


-- CalMode must be in the defined list
SELECT
*
FROM Calibration
WHERE CalMode NOT IN (1,2,3,11,12,13,22,23,91);


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

-- Check deployment entries where the end time is before the start time
SELECT
*
FROM Deployment 
WHERE Date_UTC_from > Date_UTC_to;

-- Check deployment entries: no entry where the next calibration is before the previous 

WITH A AS
(
SELECT
*,
LAG(Date_UTC_to,1) OVER (PARTITION BY SensorUnit_ID ORDER BY Date_UTC_from) AS Date_UTC_to_prev,
FROM Deployment 
)

SELECT
*
FROM A
WHERE Date_UTC_from <= Date_UTC_to_prev;

-- Check calibration entries where the end time is before the start time
SELECT
*
FROM Calibration 
WHERE Date_UTC_from > Date_UTC_to;




-- Check that calibration are in the right order

WITH A AS
(
SELECT
*,
LAG(Date_UTC_to,1) OVER (PARTITION BY SensorUnit_ID ORDER BY Date_UTC_from) AS Date_UTC_to_prev
FROM Calibration 
)

SELECT
*
FROM A
WHERE Date_UTC_from <= Date_UTC_to_prev;


-- Check that no sensor start time is after the end time
SELECT
*
FROM Sensors
WHERE Date_UTC_from >= Date_UTC_to;


-- Check that no subsequent sensors period exactly overlap (there can be only one sensor in a sensor unit)
WITH A AS
(
SELECT
*,
LAG(Date_UTC_to,1) OVER (PARTITION BY SensorUnit_ID ORDER BY Date_UTC_from) AS Date_UTC_to_prev
FROM Sensors
WHERE Type IN ('HPP', 'LP8')
)

SELECT
*
FROM A
WHERE Date_UTC_from <= Date_UTC_to_prev;

-- Check that there is no deployment lasting longer than a sensor installation in the box

	SELECT 
		*
	FROM Deployment AS dep	
	JOIN Sensors AS sens
		ON dep.SensorUnit_ID = sens.SensorUnit_ID
		WHERE Type IN ('HPP', 'LP8') AND dep.Date_UTC_from >= sens.Date_UTC_from AND  
		dep.Date_UTC_from < sens.Date_UTC_to AND
		dep.Date_UTC_to > sens.Date_UTC_to;

-- Check that there is no deployment refering to a sensor which is not installed yet (starting before)

	SELECT 
		*
	FROM Deployment AS dep	
	JOIN Sensors AS sens
		ON dep.SensorUnit_ID = sens.SensorUnit_ID
		WHERE Type IN ('HPP', 'LP8') AND dep.Date_UTC_from >= sens.Date_UTC_from AND  
		dep.Date_UTC_to > sens.Date_UTC_from AND
		dep.Date_UTC_to < sens.Date_UTC_to AND 
		dep.Date_UTC_from < sens.Date_UTC_from;


-- Check that are no deployment that refer to two sensors
	SELECT
	*
	FROM
(
	SELECT 
		dep.SensorUnit_ID,
		COUNT(Serialnumber) OVER (PARTITION BY dep.SensorUnit_ID, dep.LocationName, dep.Date_UTC_from, dep.Date_UTC_to) AS nd
	FROM Deployment AS dep	
	JOIN Sensors AS sens
		ON dep.SensorUnit_ID = sens.SensorUnit_ID
		WHERE Type IN ('HPP', 'LP8')
		AND dep.Date_UTC_from >= sens.Date_UTC_from AND dep.Date_UTC_to <= sens.Date_UTC_to
) AS a
WHERE nd > 1;