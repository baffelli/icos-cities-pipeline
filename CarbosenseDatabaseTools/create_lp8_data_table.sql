CREATE TABLE CarboSense.lp8_data
(
 `SensorUnit_ID` smallint(5) unsigned NOT NULL,
  `time` int(10) unsigned NOT NULL DEFAULT 0,
  `battery` float DEFAULT NULL,
  `senseair_lp8_temperature_last` float DEFAULT NULL,
  `senseair_lp8_temperature` float DEFAULT NULL,
  `sensirion_sht21_temperature_last` float DEFAULT NULL,
  `sensirion_sht21_temperature` float DEFAULT NULL,
  `sensirion_sht21_humidity` float DEFAULT NULL,
  `senseair_lp8_vcap2` float DEFAULT NULL,
  `senseair_lp8_vcap1` float DEFAULT NULL,
  `senseair_lp8_co2_filtered` float DEFAULT NULL,
  `senseair_lp8_co2` float DEFAULT NULL,
  `senseair_lp8_ir_filtered` float DEFAULT NULL,
  `senseair_lp8_ir_last` float DEFAULT NULL,
  `senseair_lp8_ir` float DEFAULT NULL,
  `senseair_lp8_status` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`SensorUnit_ID`,`time`),
  CONSTRAINT `fk_lp8_data_SensorUnit_ID` FOREIGN KEY (`SensorUnit_ID`) REFERENCES `SensorUnits` (`SensorUnit_ID`)
);
