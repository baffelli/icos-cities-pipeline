# LP8_measurement_processing

The LP8 measurement processing is applied for two model versions:
- version [3]: full model -> database table: **CarboSense_CO2_TEST01**
- version [2]: simplified model -> database table: **CarboSense_CO2_TEST00**
- [*version [1]: not used anymore (database table: CarboSense_CO2)*]


File **Carbosense_LP8_DATA_PROCESSING.sh** contains a set of processing chains sequentially calling the scripts described below with specific input options.


First part: Computation of processed measurements, outlier correction, discontinuity in time series, drift correction, consistency check

- Compute_CarboSense_CO2_values.r **CronJob**
  - Required input variables: par1: partial/complete processing [T/F]; par2: version [1/2/3]; optional_par3: processing only measurements from DUE1 [DUE] 
  - Tasks:
    - Computation of CO2 concentration based on raw measurements and calibration model/parameters
    - Export processed measurements to database tables CarboSense_CO2_TEST01, CarboSense_CO2_TEST00 or CarboSense_CO2 (depending on **version**)
    - Flags outliers based on SHT21_RH [-> FLAG]
- Empa_OutlierDetection_P.r **CronJob**
  - Required input variables: par1: Version [1/2/3]; par2: processing measurements from DUE1 [DUE/NO_DUE] 
  - Tasks:
    - Flags outliers [-> O_FLAG]
- LP8_measurement_analysis.r **CronJob**
  - Required input variables: par1: Version [1/2/3]; optional_par2: processing only measurements from DUE1 [DUE] 
  - Tasks:
    - Detects discontinuity periods in LP8 time series 
- LP8_DriftCorrection.r **CronJob**
  - Required input variables: par1: Version [1/2/3]; par2: partial/complete processing [F/T]; optional_par3: processing only measurements from DUE1 [DUE] 
  - Tasks:
    - Computes and applies drift corrections for LP8 sensors based on MeteoSwiss wind measurements and CO2 reference measurements 
- LP8_ConsistencyCheck.r **CronJob**
  - Required input variables: par1: Version [1/2/3]; optional_par2: processing only measurements from DUE1 [DUE] 
  - Tasks:
    - Checks consistency of LP8 measurement by means of reference measurements
    - Flags suspicious measurements [-> L_FLAG]

Second part: analysis+visualisation of results

- Plot_CarboSense_CO2_TS.r **CronJob**
  - Required input variables: par1: Version [1/2/3]
  - Tasks:
    - Creates LP8 measurement time series
- Compute_Diurnal_CO2concentrations.r **CronJob**
  - Required input variables: par1: Version [1/2/3]
  - Tasks:
    - Computes diurnal CO2 variations (aggregated for months) based on LP8 measurements (+ HPP/Picarro measurements)
- Comparison_CO2_LP8_REF.r **CronJob**
  - Required input variables: par1: Version [1/2/3]; par2: comparison for CO2_DRY or CO2_WET [DRY/WET]
  - Tasks:
    - Compares LP8 measurements with measurements of co-located Picarro-instruments [sites HAE, PAY, DUE, RIG, BRM, LAEG]
- Comparison_CO2_LP8_LP8.r **CronJob**
  - Required input variables: par1: Version [1/2/3]
  - Tasks:
    - Compares measurements of co-located LP8 sensors
- Comparison_CO2_LP8_HPP.r **CronJob**
  - Required input variables: par1: Version [1/2/3]
  - Tasks:
    - Compares LP8 measurements with measurements of co-located HPP instruments [sites HAE, PAY, DUE, RIG, BRM, LAEG]

# HPP_measurement_processing
- Compute_CarboSense_HPP_CO2_values.r **CronJob**
  - Required input variables: par1: partial/complete processing [T/F]; optional_par2: LocationName (if omitted only operational sites are processed)
  - Tasks:
    - Computation of CO2 concentrations based on raw measurements, calibration model and on-site calibration
    - Export of computed concentrations to CarboSense.CarboSense_HPP_CO2
    - Reports of calibration conditions (delta HPP-REF, H2O, RH, cylinder pressure)
- Plot_HPP_CO2.r **CronJob**
  - Tasks:
    - Generation of HPP CO2 time series
- Comparison_CO2_HPP_REF.r **CronJob**
  - Tasks:
    - Comparison of HPP measurements and measurements from Picarro instruments
- Comparison_H2O_HPP_MCH.r **CronJob**
  - Tasks:
    - Comparison of computed HPP H2O values with H2O values computed from MeteoSwiss measurements

# REF_measurement_processing
- Plot_REF_CO2.r **CronJob**
  - Tasks:
    - Generation of reference site CO2 time series

# HPP_sensor_calibration
- CO2_HPP_SensorCalibration_LINUX_IR_2S.r **WorkingScript** 
  - Determination of HPP calibration model parameters (HPP 426-445)
    - Option to select / define different models
    - Export of model parameters CarboSense.CalibrationParameters
- CO2_HPP_SensorCalibration_LINUX_IR.r **WorkingScript**
  - Determination of HPP calibration model parameters (HPP 342+390)

# LP8_sensor_calibration
- CO2_LP8_SensorCalibration_LINUX_DRY.r **WorkingScript** 
  - Determination of LP8 calibration model parameters
    - Option to select / define different models
    - Export of model parameters CarboSense.CalibrationParameters

# MeteoSwissData4Carbosense
- Import_DB_table_METEOSWISS_Measurements.r **CronJob**
  - Imports daily files from "/project/CarboSense/Data/METEO/MCH_DAILY_DATA_DUMP" and write it into database table "METEOSWISS\_Measurements"
  - Contacts: Stephan Henne (data from MeteoSwiss: m2m.empa.ch)
- METEOSWISS_PressureInterpolation.r **CronJob**
  - Spatial interpolation of pressure measurements in 10 minutes interval for all "LocationName" with deployed sensors [CarboSense.Deployment]
  - Data flow: CarboSense.METEOSWISS_Measurements -> Interpolation -> CarboSense.PressureInterpolation / CarboSense.PressureParameter
- [ *Import_DB_table_METEOSWISS_Measurements_MAN_DOWNLOADED_FILES.r* ]
  - Used to import MeteoSwiss data files that were manually downloaded before automated file transfer was established.  
- [ *METEOSWISS_PressureInterpolation_CV.r* ]
  - Same as "METEOSWISS_PressureInterpolation.r", used for accuracy estimation computation

# CarbosenseNetworkStatus
- Check_CarboSense_SensorHealth.r **CronJob**
  - Reports for each sensor: Number of LP8 and SHT measurements, status, battery, range of values for the preceding 24 hours 
  - Results in directory: /project/CarboSense/Carbosense_Network/CarboSense_SensorHealth
- CarboSenseDeployment2KML.r **CronJob**
  - Creates KML-file for GoogleEarth based on entries in Carbosense.Deployment
  - Results in directory: /project/CarboSense/Carbosense_Network/CarboSense_KML
- [ *GEO_ADMIN_LINKS_for_active_deployments.r* ]
  - Creates file that contains links for website "https://map.geo.admin.ch"
- [ *MeteoSwissNetwork2KML.r* ]
  - Generates a KML-file that includes the availability of P,T and wind measurements from MeteoSwiss (table: METEOSWISS_Measurements)

# CarbosenseDatabaseTools
- Import_DB_table_NABEL_DUE.r **CronJob** [1]
  - Imports data files (Picarro, Gases, Meteo) from directory "K:/Nabel/Daten/Stationen/DUE/" ("/newhome/muem/mnt/Win_K/Daten/Stationen/DUE/")
  - Exclusion periods accounted in the script
- Import_DB_table_NABEL_HAE.r [1]
  - Imports data files (**LICOR**, Gases, Meteo) from directory "K:/Nabel/Daten/Stationen/HAE/" ("/newhome/muem/mnt/Win_K/Daten/Stationen/HAE/")
  - Reads relevant entries from CarboSense.RefMeasExclusionPeriods
- Import_DB_table_NABEL_HAE_PIC.r **CronJob** [1]
  - Imports data files (**Picarro**, Gases, Meteo) from directory "K:/Nabel/Daten/Stationen/HAE/" ("/newhome/muem/mnt/Win_K/Daten/Stationen/HAE/")
  - Reads relevant entries from CarboSense.RefMeasExclusionPeriods
  - Replaced "Import_DB_table_NABEL_HAE.r" on 2020-03-13.
- Import_DB_table_NABEL_PAY.r **CronJob** [1]
  - Imports data files (Picarro, Gases, Meteo) from directory "K:/Nabel/Daten/Stationen/PAY/" ("/newhome/muem/mnt/Win_K/Daten/Stationen/PAY/")
  - Reads relevant entries from CarboSense.RefMeasExclusionPeriods
- Import_DB_table_NABEL_RIG.r **CronJob** [1]
  - Imports data files (Picarro, Gases, Meteo) from directory "K:/Nabel/Daten/Stationen/RIG/" ("/newhome/muem/mnt/Win_K/Daten/Stationen/RIG/")
  - Reads relevant entries from CarboSense.RefMeasExclusionPeriods

[1] Requires access to "K:/Nabel" from Linux.

- Import_DB_table_EMPA_LAEG.r **CronJob** [2]
  - Imports data from empaGSN.laegern_1min_cal to CarboSense.EMPA_LAEG
  - Reads relevant entries from CarboSense.RefMeasExclusionPeriods
- Import_DB_table_UNIBE_GIMM.r **CronJob** [2]
  - Imports data from empaGSN.gimmiz_1min_cal to CarboSense.UNIBE_GIMM
  - Reads relevant entries from CarboSense.RefMeasExclusionPeriods
- Import_DB_table_UNIBE_BRM.r **CronJob** [2]
  - Imports data from empaGSN.beromuenster_Xm_1min_cal to CarboSense.UNIBE_BRM

[2] Contact regarding data availability and access to empaGSN: Stephan Henne

- DB_REF_MEAS_Processing.r **CronJob**
  - Computes columns "?_10MIN_AV" in CarboSense database tables that contain data for LP8 calibration ("NABEL_DUE", "NABEL_HAE","NABEL_RIG", "NABEL_PAY", "ClimateChamber_00_DUE", "PressureChamber_01_DUE", "PressureChamber_00_METAS"). Only data of tables "NABEL_?" should be processed on a daily basis.
  - Filtering criteria are coded in the script and depend on specific tables
- Compute_NABEL_Picarro_CO2_WET.r **CronJob**
  - Computes CO2_WET_COMP for tables NABEL_DUE, NABEL_HAE, NABEL_PAY and NABEL_RIG

Miscellaneous:
- AddCantonNameToTableLocation.r
  - Determines for each location in CarboSense.Locations in which canton it is located and fills canton abbreviation in CarboSense.Locations

# UploadProcessedMeasurementsToDecentlabDB
Contacts related to "swiss.co2.live": Khash-Erdene Jalsan (khash.jalsan@decentlab.com), Reinhard Bischoff (reinhard.bischoff@decentlab.com)

- Upload_LP8_Measurements_To_swiss_co2_live.sh **CronJob**
  - Loop over all LP8 sensors: drops series (first of month) / deletes data from last 21 days (daily) in swiss.co2.live
  - Calls "Upload_LP8_processed_one_sensor_to_Decentlab_DB"
- Upload_HPP_Measurements_To_swiss_co2_live.sh **CronJob**
  - Loop over all HPP sensors: drops series (first of month) / deletes data from last 21 days (daily) in swiss.co2.live
  - Calls "Upload_HPP_processed_one_sensor_to_Decentlab_DB"
- Upload_LP8_processed_one_sensor_to_Decentlab_DB.r
  - Selects all data (first of month) / data of last 21 days (daily) from CarboSense_CO2_FINAL and uploads it to swiss.co2.live
- Upload_HPP_processed_one_sensor_to_Decentlab_DB.r
  - Selects all data (first of month) / data of last 21 days (daily) from CarboSense_HPP_CO2 and uploads it to swiss.co2.live