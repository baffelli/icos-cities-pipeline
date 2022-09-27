CREATE VIEW `calibration_quality` AS
with last_cp AS
(
	SELECT
	id AS model_id,
    species,
    computed AS computation_time,
    device AS serial_number,
    `type` AS sensor_type
	FROM
	(
		SELECT
		*,
		row_number() over (partition by device order by computed DESC) AS cn
		FROM calibration_parameters
	) AS a
	WHERE cn = 1
)

SELECT
    Sensors.SensorUnit_ID AS sensor_id,
	last_cp.serial_number,
    last_cp.model_id,
    last_cp.computation_time,
    last_cp.sensor_type,
    calibration_performance.bias,
    calibration_performance.rmse,
    calibration_performance.correlation
FROM last_cp 
JOIN calibration_performance ON calibration_performance.model_id = last_cp.model_id
JOIN Sensors ON Sensors.Serialnumber = last_cp.serial_number