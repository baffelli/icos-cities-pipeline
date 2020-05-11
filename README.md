# MeteoSwissData4Carbosense
- Import_DB_table_METEOSWISS_Measurements.r
  - Imports daily files from "/project/CarboSense/Data/METEO/MCH_DAILY_DATA_DUMP" and write it into database table "METEOSWISS\_Measurements"
  - Contacts: Stephan Henne (data from MeteoSwiss: m2m.empa.ch)
- Import_DB_table_METEOSWISS_Measurements.r
  - Spatial interpolation of pressure measurements in 10 minutes interval for all "LocationName" with deployed sensors [CarboSense.Deployment]
  - Data flow: CarboSense.METEOSWISS_Measurements -> Interpolation -> CarboSense.PressureInterpolation / CarboSense.PressureParameter