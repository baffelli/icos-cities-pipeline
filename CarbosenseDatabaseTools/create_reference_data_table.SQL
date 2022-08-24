CREATE TABLE IF NOT EXISTS CarboSense.picarro_data
(
 LocationName CHAR(32), `timestamp` INT(11),
 VALVEPOS INTEGER(11),
 CO2 DOUBLE,
 CO2_F INTEGER(11),
 CO2_DRY DOUBLE,
 CO2_DRY_F INTEGER(11),
 pressure DOUBLE,
 pressure_F TINYINT(3),
 RH DOUBLE,
 RH_F TINYINT(3),
 T DOUBLE,
 T_F TINYINT(4),
 H2O DOUBLE,
 H2O_F INTEGER(11), CONSTRAINT PK_picarro_data PRIMARY KEY (LocationName, `timestamp`)
);
INSERT INTO picarro_data WITH HAE AS
(
SELECT
 'HAE' AS LocationName, `timestamp`, NULL AS VALVEPOS,
 NULLIF(CO2, -999) AS CO2,
 CO2_F, COALESCE(NULLIF(CO2_DRY_CAL,-999), NULLIF(CO2, -999) * (1 - NULLIF(H2O,-999)/100)) AS CO2_DRY, CASE WHEN CO2_DRY_F = -999 THEN CO2_F ELSE CO2_DRY_F END AS CO2_DRY_F,
 pressure,
 pressure_F,
 RH,
 RH_F,
 T,
 T_F,
 NULLIF(H2O, -999) AS H2O,
 H2O_F
FROM NABEL_HAE
),
DUE AS 
(
SELECT 
 'DUE1' AS LocationName, `timestamp`, NULL AS VALVEPOS,
 NULLIF(CO2, -999) AS CO2,
 CO2_F, COALESCE(NULLIF(CO2_DRY_CAL,-999), NULLIF(CO2, -999) * (1 - NULLIF(H2O,-999)/100)) AS CO2_DRY, CASE WHEN CO2_DRY_F = -999 THEN CO2_F ELSE CO2_DRY_F END AS CO2_DRY_F,
 pressure,
 pressure_F,
 RH,
 RH_F,
 T,
 T_F,
 NULLIF(H2O, -999),
 H2O_F
FROM NABEL_DUE
),
RIG AS
(
SELECT
 'RIG' AS LocationName, `timestamp`, NULL AS VALVEPOS,
  NULLIF(CO2, -999) AS CO2,
 CO2_F,
 CO2_DRY_CAL AS CO2_DRY,
 CO2_DRY_F,
 pressure,
 pressure_F,
 RH,
 RH_F,
 T,
 T_F,
 NULLIF(H2O, -999) AS H2O,
 H2O_F
FROM NABEL_RIG
),
PAY AS 
(
SELECT
 'PAY' AS LocationName, `timestamp`, NULL AS VALVEPOS,
  NULLIF(CO2, -999) AS CO2,
 CO2_F,
 CO2_DRY_CAL AS CO2_DRY,
 CO2_DRY_F, NULL AS pressure, NULL AS pressure_F, NULL AS RH, NULL AS RH_F, NULL AS T, NULL AS T_F,
 H2O,
 H2O_F
FROM NABEL_PAY
),
BRM AS
(
SELECT 
 'BRM' AS LocationName, `timestamp`,
 VALVEPOS,
  NULLIF(CO2, -999) AS CO2,
 CO2_F,
 CO2_DRY AS CO2_DRY,
 CO2_F AS CO2_DRY_F, NULL AS pressure, NULL AS pressure_F, NULL AS RH, NULL AS RH_F, NULL AS T, NULL AS T_F,
 NULLIF(H2O, -999) AS H2O,
 H2O_N AS H2O_F
FROM UNIBE_BRM 
),
GIMM AS 
(
SELECT
 'GIMM' AS LocationName, `timestamp`,
 VALVEPOS,
  NULLIF(CO2, -999) AS CO2,
 CO2_F,
 CO2_DRY AS CO2_DRY,
 CO2_F AS CO2_DRY_F, NULL AS pressure, NULL AS pressure_F, NULL AS RH, NULL AS RH_F, NULL AS T, NULL AS T_F,
NULLIF(H2O, -999) AS H2O,
 H2O_N AS H2O_F
FROM UNIBE_GIMM 
),
LAEG AS 
(
SELECT
 'LAEG' AS LocationName, `timestamp`, NULL AS VALVEPOS,
  NULLIF(CO2, -999) AS CO2,
 CO2_F,
 CO2_DRY AS CO2_DRY,
 CO2_DRY_FLAG AS CO2_DRY_F, NULL AS pressure, NULL AS pressure_F, NULL AS RH, NULL AS RH_F, NULL AS T, NULL AS T_F,
 NULLIF(H2O, -999) AS H2O,
 H2O_FLAG AS H2O_F
FROM EMPA_LAEG
),
CM AS (
SELECT
 'ClimateChamber_00_DUE' AS LocationName, `timestamp`, NULL AS VALVEPOS,
  NULLIF(CO2, -999) AS CO2,
 CO2_F,
 CO2_DRY,
 CO2_F AS CO2_DRY_FLAG,
 pressure AS pressure,
 pressure_F AS pressure_F,
 RH AS RH,
 RH_F AS RH_F,
 T AS T,
 T_F AS T_F,
NULLIF(H2O, -999) AS H2O,
 H2O_F
FROM ClimateChamber_00_DUE
),
PC0 AS 
(
SELECT
 'PressureChamber_01_DUE' AS LocationName, `timestamp`, NULL AS VALVEPOS,
  NULLIF(CO2, -999) AS CO2,
 CO2_F,
 CO2_DRY,
 CO2_F AS CO2_DRY_FLAG,
 pressure AS pressure,
 pressure_F AS pressure_F, NULL AS RH, NULL AS RH_F,
 T AS T,
 T_F AS T_F,
 NULLIF(H2O, -999) AS H2O,
 H2O_F
FROM PressureChamber_01_DUE
),
A AS
(
SELECT
*
FROM HAE
WHERE NOT EXISTS (
SELECT TIMESTAMP, LocationName
FROM picarro_data
WHERE HAE.timestamp = picarro_data.timestamp AND HAE.LocationName = picarro_data.LocationName) UNION ALL
SELECT 
*
FROM DUE
WHERE NOT EXISTS (
SELECT TIMESTAMP, LocationName
FROM picarro_data
WHERE DUE.timestamp = picarro_data.timestamp AND DUE.LocationName = picarro_data.LocationName) UNION ALL
SELECT 
*
FROM PAY
WHERE NOT EXISTS (
SELECT TIMESTAMP, LocationName
FROM picarro_data
WHERE PAY.timestamp = picarro_data.timestamp AND PAY.LocationName = picarro_data.LocationName) UNION ALL
SELECT 
*
FROM RIG
WHERE NOT EXISTS (
SELECT TIMESTAMP, LocationName
FROM picarro_data
WHERE RIG.timestamp = picarro_data.timestamp AND RIG.LocationName = picarro_data.LocationName) UNION ALL
SELECT 
*
FROM GIMM
WHERE NOT EXISTS (
SELECT TIMESTAMP, LocationName
FROM picarro_data
WHERE GIMM.timestamp = picarro_data.timestamp AND GIMM.LocationName = picarro_data.LocationName) UNION ALL
SELECT 
*
FROM BRM
WHERE NOT EXISTS (
SELECT TIMESTAMP, LocationName
FROM picarro_data
WHERE BRM.timestamp = picarro_data.timestamp AND BRM.LocationName = picarro_data.LocationName) UNION ALL
SELECT 
*
FROM LAEG
WHERE NOT EXISTS (
SELECT TIMESTAMP, LocationName
FROM picarro_data
WHERE LAEG.timestamp = picarro_data.timestamp AND LAEG.LocationName = picarro_data.LocationName) UNION ALL
SELECT 
*
FROM CM
WHERE NOT EXISTS (
SELECT TIMESTAMP, LocationName
FROM picarro_data
WHERE CM.timestamp = picarro_data.timestamp AND CM.LocationName = picarro_data.LocationName) UNION ALL
SELECT 
*
FROM PC0
WHERE NOT EXISTS (
SELECT TIMESTAMP, LocationName
FROM picarro_data
WHERE PC0.timestamp = picarro_data.timestamp AND PC0.LocationName = picarro_data.LocationName)

)
SELECT
*
FROM A


