CREATE TABLE `Carbosense`.`calibration_parameters`
(
	`serial_number` smallint(5) unsigned not null,
    `type` character(20),
    `model_name` character(10),
    `date_from` int(10) not null default 0,
    `date_to` int(10) not null default 0,
    `computation_date` int(10) not null default 0,
    `parameter_name` character(32),
    `parameter_value` float,
    primary key  (`serialnumber`, `type`, `model_name`, `date_from`, `date_to`, `computation_date`),
constraint `fk_calibration_parameters_serial_number` foreign key (`serial_number`,`type`) references `Sensors` (`Serialnumber`,`type`)
)