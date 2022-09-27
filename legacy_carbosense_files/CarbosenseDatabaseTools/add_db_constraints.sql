-- Constraints for the calibration table

ALTER TABLE Deployment
ADD CONSTRAINT fk_location FOREIGN KEY (LocationName) REFERENCES Location(LocationName);

ALTER TABLE Deployment
ADD CONSTRAINT fk_SensorUnit_ID FOREIGN KEY (SensorUnit_ID) REFERENCES SensorUnits(SensorUnit_ID);

ALTER TABLE Deployment
ADD CONSTRAINT to_larger_from CHECK (Date_UTC_to > Date_UTC_from);

-- For the deployment table
ALTER TABLE Calibration
ADD CONSTRAINT fk_location_cal FOREIGN KEY (LocationName) REFERENCES Location(LocationName);

ALTER TABLE Calibration
ADD CONSTRAINT fk_SensorUnit_ID_cal FOREIGN KEY (SensorUnit_ID) REFERENCES SensorUnits(SensorUnit_ID);

ALTER TABLE Calibration
ADD CONSTRAINT to_larger_from_cal CHECK (Date_UTC_to > Date_UTC_from);


-- For the reference gas cylinders

ALTER TABLE RefGasCylinder
ADD CONSTRAINT to_larger_from_gas CHECK (Date_UTC_to > Date_UTC_from);


-- For reference gas cylinder in deployment
ALTER TABLE RefGasCylinderDeplyoment
ADD CONSTRAINT to_larger_from_gas_dep CHECK (Date_UTC_to > Date_UTC_from);