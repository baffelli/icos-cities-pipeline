# MeteoSwissData4Carbosense
- Import_DB_table_METEOSWISS_Measurements.r **CRJ**
  - Imports daily files from "/project/CarboSense/Data/METEO/MCH_DAILY_DATA_DUMP" and write it into database table "METEOSWISS\_Measurements"
  - Contacts: Stephan Henne (data from MeteoSwiss: m2m.empa.ch)
- METEOSWISS_PressureInterpolation.r **CRJ**
  - Spatial interpolation of pressure measurements in 10 minutes interval for all "LocationName" with deployed sensors [CarboSense.Deployment]
  - Data flow: CarboSense.METEOSWISS_Measurements -> Interpolation -> CarboSense.PressureInterpolation / CarboSense.PressureParameter
- [ *Import_DB_table_METEOSWISS_Measurements_MAN_DOWNLOADED_FILES.r* ]
  - Used to import MeteoSwiss data files that were manually downloaded before automated file transfer was established.  
- [ *METEOSWISS_PressureInterpolation_CV.r* ]
  - Same as "METEOSWISS_PressureInterpolation.r", used for accuracy estimation computation

# CarbosenseNetworkStatus
- Check_CarboSense_SensorHealth.r **CRJ**
  - Reports for each sensor: Number of LP8 and SHT measurements, status, battery, range of values for the preceding 24 hours 
  - Results in directory: /project/CarboSense/Carbosense_Network/CarboSense_SensorHealth
- CarboSenseDeployment2KML.r **CRJ**
  - Creates KML-file for GoogleEarth based on entries in Carbosense.Deployment
  - Results in directory: /project/CarboSense/Carbosense_Network/CarboSense_KML
- [ *GEO_ADMIN_LINKS_for_active_deployments.r* ]
  - Creates file that contains links for website "https://map.geo.admin.ch"
- [ *MeteoSwissNetwork2KML.r* ]
  - Generates a KML-file that includes the availability of P,T and wind measurements from MeteoSwiss (table: METEOSWISS_Measurements)

# CarbosenseDatabaseTools
- Import_DB_table_NABEL_DUE.r **CRJ** [1]
  - Imports data files (Picarro, Gases, Meteo) from directory "K:/Nabel/Daten/Stationen/DUE/" ("/newhome/muem/mnt/Win_K/Daten/Stationen/DUE/")
  - Exclusion periods accounted in the script
- Import_DB_table_NABEL_HAE.r [1]
  - Imports data files (**LICOR**, Gases, Meteo) from directory "K:/Nabel/Daten/Stationen/HAE/" ("/newhome/muem/mnt/Win_K/Daten/Stationen/HAE/")
  - Reads relevant entries from CarboSense.RefMeasExclusionPeriods
- Import_DB_table_NABEL_HAE_PIC.r **CRJ** [1]
  - Imports data files (**Picarro**, Gases, Meteo) from directory "K:/Nabel/Daten/Stationen/HAE/" ("/newhome/muem/mnt/Win_K/Daten/Stationen/HAE/")
  - Reads relevant entries from CarboSense.RefMeasExclusionPeriods
  - Replaced "Import_DB_table_NABEL_HAE.r" on 2020-03-13.
- Import_DB_table_NABEL_PAY.r **CRJ** [1]
  - Imports data files (Picarro, Gases, Meteo) from directory "K:/Nabel/Daten/Stationen/PAY/" ("/newhome/muem/mnt/Win_K/Daten/Stationen/PAY/")
  - Reads relevant entries from CarboSense.RefMeasExclusionPeriods
- Import_DB_table_NABEL_RIG.r **CRJ** [1]
  - Imports data files (Picarro, Gases, Meteo) from directory "K:/Nabel/Daten/Stationen/RIG/" ("/newhome/muem/mnt/Win_K/Daten/Stationen/RIG/")
  - Reads relevant entries from CarboSense.RefMeasExclusionPeriods

[1] Requires access to "K:/Nabel" from Linux.

- Import_DB_table_EMPA_LAEG.r **CRJ** [2]
  - Imports data from empaGSN.laegern_1min_cal to CarboSense.EMPA_LAEG
  - Reads relevant entries from CarboSense.RefMeasExclusionPeriods
- Import_DB_table_UNIBE_GIMM.r **CRJ** [2]
  - Imports data from empaGSN.gimmiz_1min_cal to CarboSense.UNIBE_GIMM
  - Reads relevant entries from CarboSense.RefMeasExclusionPeriods
- Import_DB_table_UNIBE_BRM.r **CRJ** [2]
  - Imports data from empaGSN.beromuenster_Xm_1min_cal to CarboSense.UNIBE_BRM

[2] Contact regarding data availability and access to empaGSN: Stephan Henne

- DB_REF_MEAS_Processing.r **CRJ**
  - Computes columns "?_10MIN_AV" in CarboSense database tables that contain data for LP8 calibration ("NABEL_DUE", "NABEL_HAE","NABEL_RIG", "NABEL_PAY", "ClimateChamber_00_DUE", "PressureChamber_01_DUE", "PressureChamber_00_METAS"). Only data of tables "NABEL_?" should be processed on a daily basis.
  - Filtering criteria are coded in the script and depend on specific tables
- Compute_NABEL_Picarro_CO2_WET.r **CRJ**
  - Computes CO2_WET_COMP for tables NABEL_DUE, NABEL_HAE, NABEL_PAY and NABEL_RIG

Miscellaneous:
- AddCantonNameToTableLocation.r
  - Determines for each location in CarboSense.Locations in which canton it is located and fills canton abbreviation in CarboSense.Locations

# UploadProcessedMeasurementsToDecentlabDB
Contacts related to "swiss.co2.live": Khash-Erdene Jalsan (khash.jalsan@decentlab.com), Reinhard Bischoff (reinhard.bischoff@decentlab.com)

- Upload_LP8_Measurements_To_swiss_co2_live.sh **CRJ**
  - Loop over all LP8 sensors: drops series (first of month) / deletes data from last 21 days (daily) in swiss.co2.live
  - Calls "Upload_LP8_processed_one_sensor_to_Decentlab_DB"
- Upload_HPP_Measurements_To_swiss_co2_live.sh **CRJ**
  - Loop over all HPP sensors: drops series (first of month) / deletes data from last 21 days (daily) in swiss.co2.live
  - Calls "Upload_HPP_processed_one_sensor_to_Decentlab_DB"
- Upload_LP8_processed_one_sensor_to_Decentlab_DB.r
  - Selects all data (first of month) / data of last 21 days (daily) from CarboSense_CO2_FINAL and uploads it to swiss.co2.live
- Upload_HPP_processed_one_sensor_to_Decentlab_DB.r
  - Selects all data (first of month) / data of last 21 days (daily) from CarboSense_HPP_CO2 and uploads it to swiss.co2.live