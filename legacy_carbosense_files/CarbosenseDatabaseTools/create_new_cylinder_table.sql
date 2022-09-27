CREATE TABLE `CarboSense`.`ref_cylinder` (
  `cylinder_id` VARCHAR(20) NOT NULL,
  `start` DATE NOT NULL,
  `end` DATE NOT NULL,
  `volume` FLOAT NULL DEFAULT NULL,
  `test_valid_to` DATE NULL DEFAULT NULL,
  `old_id` VARCHAR(45) NULL DEFAULT NULL,
  PRIMARY KEY (`cylinder_id`, `start`, `end`));
INSERT INTO  `CarboSense`.`ref_cylinder`
SELECT
cylinder_id,
MIN(Date_UTC_from),
MAX(Date_UTC_to),
MAX(NULLIF(volume, -999)),
MAX(NULLIF(cylinder_test_valid_to, -999))
FROM ref_gas_cylinder
GROUP BY cylinder_id;
INSERT into ref_gas_cylinder_analysis (fill_from, fill_to, analysed, cylinder_id, CO2, CO2_sd, H2O, H2O_sd, pressure, fillnr)
SELECT
CAST(Date_UTC_from AS DATE) AS fill_from,
CAST(Date_UTC_to AS DATE) AS fill_to,
CAST(COALESCE(analysed, Date_UTC_from) AS DATE) AS analysed,
cylinder_id,
nullif(CO2, -999) AS CO2,
nullif(sdCO2, -999) AS CO2_sd,
nullif(H2O, -999) AS H2O,
nullif(sdH2O, -999) AS H2O_sd,
NULLIF(pressure, -999) AS pressure,
fillnr
FROM ref_gas_cylinder_old
WHERE CO2 IS NOT NULL