DROP TABLE IF EXISTS CarboSense.raw_sensor_data
CREATE TABLE CarboSense.raw_sensor_data(
    SensorUnit_ID SMALLINT(6) UNSIGNED NOT NULL ,
    timestamp TIMESTAMP,
    Valve BOOLEAN,
    CO2 FLOAT,
    H2O FLOAT,
    sensor_temperature FLOAT,
    air_pressure,
    air_temperature FLOAT,
    air_moisture FLOAT,
    CONSTRAINT PK_raw_sensor_data PRIMARY KEY (SensorUnit_ID, timestamp)
)