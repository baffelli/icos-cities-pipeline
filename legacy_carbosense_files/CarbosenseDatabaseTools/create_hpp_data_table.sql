CREATE TABLE CarboSense.hpp_data
(
  `SensorUnit_ID` smallint(5) unsigned NOT NULL,
  `time` int(10) unsigned NOT NULL DEFAULT 0,
  `battery` float DEFAULT NULL,
  `calibration` SMALLINT(2),
  `senseair_hpp_co2_filtered` FLOAT DEFAULT NULL,
  `senseair_hpp_ir_signal` FLOAT DEFAULT NULL,
  `senseair_hpp_lpl_signal` FLOAT DEFAULT NULL,
  `senseair_hpp_ntc5_diff_temp` FLOAT DEFAULT NULL,
  `senseair_hpp_ntc6_se_temp` FLOAT DEFAULT NULL,
  `senseair_hpp_pressure_filtered` FLOAT DEFAULT NULL,
  `senseair_hpp_status` INT(10) DEFAULT NULL,
  `senseair_hpp_temperature_detector` FLOAT DEFAULT NULL,
  `senseair_hpp_temperature_mcu` FLOAT DEFAULT NULL,
  `sensirion_sht21_humidity` FLOAT DEFAULT NULL,
  `sensirion_sht21_temperature` FLOAT DEFAULT NULL,
  PRIMARY KEY (`SensorUnit_ID`,`time`),
  CONSTRAINT `fk_hpp_data_SensorUnit_ID` FOREIGN KEY (`SensorUnit_ID`) REFERENCES `SensorUnits` (`SensorUnit_ID`),
  INDEX  `ix_hpp_data_calibration`  (`SensorUnit_ID` ASC, `calibration` ASC)
);
