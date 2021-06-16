CREATE TABLE CarboSense.raw_sensor_data(
    SensorUnit_ID SMALLINT(6) UNSIGNED NOT NULL ,
    timestamp TIMESTAMP,
    Valve BOOLEAN,
    CO2 FLOAT,
    H2O FLOAT,
    sensor_temperature FLOAT,
    pressure,
    air_temperature FLOAT,
    air_moisture FLOAT
)