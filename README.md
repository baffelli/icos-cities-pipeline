# CarboSense/ICOS Cities Pipeline
## Introduction
This repository contains *almost* all the code, configurations and scripts needed to process the ICOS-Cities/CarboSense mid- and low-cost sensor data. Usually, the entry point in the processing is done through the DigDag [DAG](process_carbosense.dig). If you inspect this (yaml) file, you will see a list of all processes and their dependencies. To learn more about digdag DAG, see the documentation [here](http://docs.digdag.io/concepts.html).

## Setup the Pipeline
To setup a working pipeline, you need to follow the steps below. In case of problems, you can contact Simone Baffelli. In the following sections, we assume you work on a Linux system with python>=3.10 and with an working installation of conda/anaconda.
### Install dependencies with conda
Using conda, install all the dependencies from [envrionment.yaml](config/environment.yaml):
```bash
conda env create -f config/environment.yml
```
The environment is called *carbosense-processing*. 
To activate the environment, use:
```bash
conda activate carbosense-processing
```

I also created a newer, python only environment called *icos-cities*. Its configuration file is found in [icos-cities.yaml](config/icos-cities.yaml)

Use the same commands as above to initialise and use the environment.
This environment is needed to run the python scripts that process the data, while the `carbosense-processing` envrionment is used to run the older scripts that were used during the CarboSense project.

### Install sensorutils manually
This is a python package containing utilities used in all other scripts. To install it in the project, run the following command from the main repository directory:
```
pip install -e .
```

The package is found in [sensorutils](./sensorutils/)

### Configure database connections

As the system relies heavily on database connections, we need to setup the database credentials such that you do not need to type them anywhere. Since the database is currently MariaDB, we will use a MySQL options file. For more informations on these files, see [here](https://mariadb.com/kb/en/configuring-mariadb-with-option-files/). 

To get started, create a file `.my.cnf` in your home folder and insert the following lines:

```
[CarboSense_MySQL]
user={insert your user here}
password={insert your password here}
host=emp-sql-cs1
port=3306
max_allowed_packet=128M
database=CarboSense
[NabelGsn]
database=NabelGsn
user={insert your user here}
password={insert your password here}
host=du-gsn1
port=3306
[empaGSN]
database=empaGSN
user={insert your user here}
password={insert your password here}
host=du-gsn1
port=3306
[client]
socket=/var/lib/mysql/mysql.sock
```
An example of this file is found [here](./config/databases.cnf)

At the moment, there are three databases that are necessary for the pipeline, as you can see in this file:
- *CarboSense_MySQL*
  - This is the metadata and raw data database which is used to store processed data and raw data. To obtain a password, ask Simone Baffelli or Patrick Burkhalter
- *NabelGsn*, *empaGSN*
  - These databases contain some of the sensor data from NABLE and EMPA. To obtain access, ask Stephan Henne


### Configure Decentlab API Key
Obtain a InfluxDB API Key from decentlab. This keys allows you access to the timeseries database where the raw measurement data resides. 
Once you have the key, store it in a yaml file called `secrets.yaml` in the `config` folder:
```
-decentlab:
  -key: your-api-key
```
Do not forget to make the file only readable from the user running the pipeline. 


### Configure network shares
Using automount, configure the following (Windows) network shares to be mounted on `/mnt/{username}/` in the following way:
- `K:/Projects/` 
  - One folder per project, e.g  `K:/Projects/Nabel`  mapped on  `/mnt/{username}/Nabel`
- `G:/`
  - On `/mnt/{username}/G`
To setup automount, ask Stephan Henne from 503 or Patrick Burckhalter from the IT department.
### Install DigDag
Follow the instructions [here](http://docs.digdag.io/getting_started.html#downloading-the-latest-version) to install the latest version of digdag. Digdag is used to automate the run of the pipeline at daily intervals.

## Running the pipeline
### Manual run
To manually run the pipeline, use the following commands:
```bash
conda activate carbosense-processing
digdag -r process_carbosense.dig
```
### Automated run
So far, the pipeline run are scheduled to run once per day using a crontab file calling the script [start_processing.sh](start_processing.sh). 

In the future, it would be desireable to install digdag as a service, so that the pipeline runs can be scheduled directly with digdag and their operation can be verified more robustly. For this, consult the digdag documentation [here](http://docs.digdag.io/command_reference.html#server-mode-commands).

# Structure of the repository
The repository is structured in directories containing group of scripts performing related tasks. These are listed below


- [**CarbosenseDatabaseTools**](./CarbosenseDatabaseTools/)

   Contains scripts used to interact with the data/metadata database. In particular, these
   scripts are used regularly or called from the digdag DAG:

  - [Import_DB_table_NABEL_DUE.r](./CarboSenseDatabaseTools/Import_DB_table_NABEL_DUE.r)
    - no longer used
    - Imports data files (Picarro, Gases, Meteo) from directory `K:/Nabel/Daten/Stationen/DUE/` (`/project/CarboSense/Win_K/Daten/Stationen/DUE/`) into database table ``NABEL_DUE``
    - Exclusion periods accounted in the script
  - [Import_DB_table_NABEL_HAE.r](./CarboSenseDatabaseTools/Import_DB_table_NABEL_HAE.r)
    - no longer used as Licor insturment is not installed in Harkingen anymore
    - Imports data files (**LICOR**, Gases, Meteo) from directory `K:/Nabel/Daten/Stationen/HAE/` (`/project/CarboSense/Win_K/Daten/Stationen/HAE/`) into database table ```NABEL_HAE```
    - Reads relevant entries from `CarboSense.RefMeasExclusionPeriods`
  - [Import_DB_table_NABEL_HAE_pic.r](./CarboSenseDatabaseTools/Import_DB_table_NABEL_HAE_pic.r)
    - no longer used 
    - Imports data files (**Picarro**, Gases, Meteo) from directory `K:/Nabel/Daten/Stationen/HAE/` (`/project/CarboSense/Win_K/Daten/Stationen/HAE/`) into database table `NABEL_HAE`
    - Reads relevant entries from CarboSense.RefMeasExclusionPeriods
  - [Import_DB_table_NABEL_PAY.r](./CarboSenseDatabaseTools/Import_DB_table_NABEL_PAY.r)
    - Imports data files (Picarro, Gases, Meteo) from directory `K:/Nabel/Daten/Stationen/PAY/` (`/project/CarboSense/Win_K/Daten/Stationen/HAE/`) into database table `NABEL_PAY`
    - Reads relevant entries from CarboSense.RefMeasExclusionPeriods

  - [Import_DB_table_NABEL_RIG.r](./CarboSenseDatabaseTools/Import_DB_table_NABEL_RIG.r)
    - Imports data files (Picarro, Gases, Meteo) from directory `K:/Nabel/Daten/Stationen/RIG/`  into database table `NABEL_RIG`

  - [Import_DB_table_EMPA_LAEG.r](./CarboSenseDatabaseTools/Import_DB_table_EMPA_LAEG.r)
    - no longer used
    - Imports data from `empaGSN.laegern_1min_cal` to `CarboSense.EMPA_LAEG`
    - Reads relevant entries from `CarboSense.RefMeasExclusionPeriods`
  - [Import_DB_table_UNIBE_GIMM.r](./CarboSenseDatabaseTools/Import_DB_table_UNIBE_GIMM.r)
    - no longer used
    - Imports data from `empaGSN.gimmiz_1min_cal` to `CarboSense.UNIBE_GIMM`
    - Reads relevant entries from `CarboSense.RefMeasExclusionPeriods`
  - [Import_DB_table_UNIBE_BRM.r](./CarboSenseDatabaseTools/Import_DB_table_UNIBE_GIMM.r)
    - no longer used
    - Imports data from `empaGSN.beromuenster_Xm_1min_cal` to `CarboSense.UNIBE_BRM`

  [2] Contact regarding data availability and access to empaGSN: Stephan Henne

  - [Import_DB_table_PressureChamber_00_METAS.r](./CarboSenseDatabaseTools/Import_DB_table_PressureChamber_00_METAS.r)
    - no longer used as calibration at Metas is not used
    - Imports the measurement files collected during the pressure calibration at METAS (directory: `K:/Carbosense/Data/Druckkammer_Versuche_Metas/Data/`) into database table `PressureChamber_00_METAS`
    - Empty database table before running the script
    - Pressure calibration at METAS was carried out only once in May 2017.
  - [Import_DB_table_ClimateChamber_00_DUE.r](./CarboSenseDatabaseTools/Import_DB_table_ClimateChamber_00_DUE.r)
    - Imports the measurement files (climate chamber data, CO2, pressure) collected during the climate chamber calibrations in LA064 (directory: `K:/Carbosense/Data/Klimakammer_Versuche_27022017_XXXXXXXX`) into database table `ClimateChamber_00_DUE`
    - Empty database table before running the script
    - Review code in terms of correct time zones (CEST,CET,UTC) when importing data from an additional calibration run. Climate chamber data is usually in local time.
    - **the entries for  Carbosense meta-database have to be set according to user in the script**
  - [Import_DB_table_PressureChamber_01_DUE.r](./CarboSenseDatabaseTools/Import_DB_table_PressureChamber_01_DUE.r )
    - Imports the measurement files (CO2, pressure) collected during the pressure calibration in LA003 (directory: `K:/Carbosense/Data//Druckkammer_Versuche_Empa_LA003/`) into database table `PressureChamber_01_DUE`
      - Empty database table before running the script
      - Review code in terms of correct time zones (CEST,CET,UTC) when importing data from an additional calibration run. Pressure data from the Additel pressure instrument might refer to local time.
      - **CS_DB_user, CS_DB_pass for Carbosense meta-database have to be set according to user in the script**
  - [check_db_integrity.sql](./CarbosenseDatabaseTools/check_db_integrity.sql)
    - Checks the integrity of database constraints. To start, you need a working sql connection and a sql interpreter. Will later be called from within a python script to produce a report 
  - [import_picarro_data.py](./CarbosenseDatabaseTools/import_picarro_data.py)
    - Imports all picarro / reference data from the various tables, replacing the `Import_DB_*` scripts above. Requires a configuration file, which you will find [here](config/picarro_mapping.yml). 
  - [dump_raw_data_from_influxdb.py](./CarbosenseDatabaseTools/dump_raw_data_from_influxdb.py)
    - (incrementally) dumps the raw HPP or LP8 data into the table `CarboSense.lp8_data` or
    `CarboSense.hpp_data` depending on the chosen mode. Run from the digdag DAG to alway have up-to-date raw data on the database

  - [create_reference_data_table.SQL](./CarbosenseDatabaseTools/create_reference_data_table.SQL)
    - Used to fill a table called `CarboSense.picarro_data` containing the consolidated reference data from all picarro instruments
  - [Check_CarboSense_Metadata_DB.r](./CarbosenseDatabaseTools/Check_CarboSense_Metadata_DB.r)
    - Older script (by Michael Mueller) to check the integrity of the metadata
  - [DB_REF_MEAS_Processing.r](./CarbosenseDatabaseTools/DB_REF_MEAS_Processing.r) 
    - Computes columns `?_10MIN_AV` in CarboSense database tables that contain data for LP8 calibration (`NABEL_DUE`, `NABEL_HAE`,`NABEL_RIG`, `NABEL_PAY`, `ClimateChamber_00_DUE`, `PressureChamber_01_DUE`, "`PressureChamber_00_METAS`). Only data of tables `NABEL_?` should be processed on a daily basis.
    - Filtering criteria are coded in the script and depend on specific tables
    - Applies CO2/H2O calibration for Picarros in DUE, HAE, PAY, RIG (e.g. CO2_DRY_CAL)
  - [Compute_NABEL_Picarro_CO2_WET.r](./CarbosenseDatabaseTools/Compute_NABEL_Picarro_CO2_WET.r) 
    - Computes CO2_WET_COMP for tables NABEL_DUE, NABEL_HAE, NABEL_PAY and NABEL_RIG
    - As the Picarros in HAE, PAY and RIG measures air that is dried before the measuring cell (since early 2020), meteo measurements (T,RH,P) from NABHAE, PAY and NABRIG are required for the computation of H2O. 
- [**CarboSenseUtilities**](./CarbosenseUtilities/)

  Various utilities used to process the data
  - [CarboSenseFunctions.r](./CarbosenseUtilities/CarboSenseFunctions.r)
    - Collection of plot functions, etc.
  - [api-v1.3.r](./CarboSenseUtilities/api-v1.3.r)
    - Decentlab API for data retrieval used in various scripts
  - [api-v1.3_2019-09-03.r](./CarboSenseUtilities/api-v1.3_2019-09-03.r)
   - Decentlab API for data retrieval/data upload used in various scripts

- [**LP8_measurement_processing**](./LP8_measurement_processing/)

  - The LP8 measurement processing is applied for two model versions:
    - version [3]: full model -> database table: `CarboSense_CO2_TEST01`
    - version [2]: simplified model -> database table: `CarboSense_CO2_TEST00`
    - [*version [1]: not used anymore (database table: CarboSense_CO2)*]


  - The directory contains a series of scripts that implement the LP8 processing chain  


    First part: Computation of processed measurements, outlier correction, discontinuity in time series, drift correction, consistency check

    1. [Compute_CarboSense_CO2_values.r](./LP8_measurement_processing/Compute_CarboSense_CO2_values.r)
        - Required input variables: par1: partial/complete processing [T/F]; par2: version [1/2/3]; optional_par3: processing only measurements from DUE1 [DUE] 
        - Tasks:
          - Computation of CO2 concentration based on raw measurements and calibration model/parameters
          - Export processed measurements to database tables CarboSense_CO2_TEST01, CarboSense_CO2_TEST00 or CarboSense_CO2 (depending on **version**)
          - Flags outliers based on SHT21_RH [-> FLAG]
    2.  [Empa_OutlierDetection_P.r](./LP8_measurement_processing/Empa_OutlierDetection_P.r)
        - Required input variables: par1: Version [1/2/3]; par2: processing measurements from DUE1 [DUE/NO_DUE] 
        - Tasks:
          - Flags outliers [-> O_FLAG]
    3. [LP8_measurement_analysis.r](./LP8_measurement_processing/LP8_measurement_analysis.r) 
        - Required input variables: par1: Version [1/2/3]; optional_par2: processing only measurements from DUE1 [DUE] 
        - Tasks:
          - Detects discontinuity periods in LP8 time series 
    4. LP8_DriftCorrection.r 
        - Required input variables: par1: Version [1/2/3]; par2: partial/complete processing [F/T]; optional_par3: processing only measurements from DUE1 [DUE] 
        - Tasks:
          - Computes and applies drift corrections for LP8 sensors based on MeteoSwiss wind measurements and CO2 reference measurements 
    5. LP8_ConsistencyCheck.r 
        - Required input variables: par1: Version [1/2/3]; optional_par2: processing only measurements from DUE1 [DUE] 
        - Tasks:
          - Checks consistency of LP8 measurement by means of reference measurements
          - Flags suspicious measurements [-> L_FLAG]

  - Second part: analysis+visualisation of results

    6. Plot_CarboSense_CO2_TS.r **CronJob**
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

Miscellaneous:
- SpatialSiteClassification.r
  - Required input variables: par1 [DEPLOYMENT/GRID]
  - Output of script [option "DEPLOYMENT"] is used for script "LP8_ConsistencyCheck.r" (run after each change in the LP8/HPP sensor deployment)
  - Tasks:
    - Computes spatial characteristics for sensor locations / grid
- TemperatureAroundZurich.r
  - Creation of maps with SHT21 / MeteoSwiss temperatures around the city of Zurich  


- [**HPP_measurement_processing**](./HPP_measurement_processing/)
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

## REF_measurement_processing
- Plot_REF_CO2.r **CronJob**
  - Tasks:
    - Generation of CO2 time series for sites equipped with Picarro or Licor instrument (CO2_WET_COMP or CO2 depending on measurement site)

## HPP_sensor_calibration
- CO2_HPP_SensorCalibration_LINUX_IR_2S.r **WorkingScript** 
  - Determination of HPP calibration model parameters (HPP 426-445)
    - Option to select / define different models
    - Data that is used for calibration
      -  CV_mode:
        -  1 --> all data from calibration periods
        -  4 --> data from pressure chamber calibration + 2 weeks before and after chamber calibration (selection of particular chamber calibration run coded in the script because some sensors were calibrated more than once.)
        -  **New feature required**
           -  Possibility for several calibration parameter sets per sensor which are valid for specific time periods in case a sensor has substantially drifted. Requires additional changes in database table "ProcessingParameters" and in script "Compute_CarboSense_HPP_CO2_values.r"
    - Export of model parameters CarboSense.CalibrationParameters
- CO2_HPP_SensorCalibration_LINUX_IR.r **WorkingScript**
  - Determination of HPP calibration model parameters (HPP 342+390)

## LP8_sensor_calibration
- CO2_LP8_SensorCalibration_LINUX_DRY.r **WorkingScript** 
  - Determination of LP8 calibration model parameters
    - Option to select / define different models
    - Export of model parameters CarboSense.CalibrationParameters

## MeteoSwissData4Carbosense
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

## CarbosenseNetworkStatus
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


## UploadProcessedMeasurementsToDecentlabDB
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

- Upload_Carbosense_MetaDBtables_to_DecentlabSFTP.pl
  - Uploads Carbosense meta-database table dump to Decentlab's FTP server

## ICOS_Carbosense_T_RH_Data_Release_October_2019
- Compute_CarboSense_T_RH_values_Version_October_2019.r
  - Download LP8 measurements from the Decentlab database, measurement processing (e.g. adjusting of timestamp, removal of duplicates, status), export in Carbosense database
- Carbosense_data_release_2019-10.r
  - Script that created the files for the ICOS data release in October 2019  


## sensorutils
This is a python package containing utilities used in all other scripts. To install it in the project, run the following command from the main repository directory:
```
pip install -e ./sensorutils
```




# Pipeline documentation
In the following sections, I will describe the most important sections of the pipeline and how to run / configure them.
## Importing raw sensor data
### Motivation
As of today (April 2022), the raw data from the LP8/HPP sensors is stored by DecentLab on their InfluxDB database. For safety and ease of processing, the first step in the pipeline is copying the raw data to our own relational database. 

This is accomplished using the script [dump_raw_data_from_influxdb.py](CarbosenseDatabaseTools/dump_raw_data_from_influxdb.py). The script needs a configuration file to map the source and the destination tables, the current configuration file is located in [co2_sensor_mapping.yml](config/co2_sensor_mapping.yml). 
### Background
The script bases on the [`DataSourceMapping`](sensorutils/files.py) class, which defines a flexible mapping between (tabular) data located in different systems (so far: CSV, relational databases and influxdb) and that can automate bulk and incremental loading between these systems (loosely inspired by [embulk](https://www.embulk.org/)).

The main concept in this system is the *file*. One file corresponds to the set of all measurement for one *group* and one *date*. The group is defined using the `grouping_key` keyword, while the date using the `date_column`. Using these information, the system checks which files are present on the source system that are absent in the destination. Files are then transferred with an optional column mapping / transformation step to convert / rename columns.

To understand the concept, we have a look at one entry of the yaml file, the one for SenseAir HPP sensors:

```
HPP:
  source:
    type: influxDB
    path: measurements
    date_from: 2017-01-01T00:00:00.000
    grouping_key: node
  dest:
    type: DB
    path: hpp_data
    db_prefix: CarboSense_MySQL
    date_from: 2017-01-01T00:00:00.000
    grouping_key: SensorUnit_ID
    date_column: time
    na: -999
  columns:
    - {name: time,  source_name: 'time', datatype: int, na: Null}
    - {name: SensorUnit_ID, source_name: node, datatype: int, na: Null}
    - {name: battery, source_name: battery, datatype: float, na: 0}
```

This defines a datasource mapping between the `source` and the `dest` systems. The source system is an InfluxDB database (the pipeline assumes by default this system to be the Decentlab one, but it can be easily changed, see the source code for this) with the *table* *measurements*. The field `grouping_key` is used to define a column identifying unique sensors in the source table so that incremental transfer is enabled. Because InfluxDB uses `time` as a default timestamp column, no `date_column` is define for the source system. The destination system in this example is the table *hpp_data* on the database defined by the prefix *CarboSense_MySQL* in the `.my.cnf` file as described [above](#configure-database-connections). 

The section `columns` contains the mapping between the source and the destination columns. The `name` key specifies the name of the column in the destination system, the `source_name` the name in the source system. Instrad of `source_name`, a SQL query can be used with `query` to perform data transformations. The query will be then run against an in-memory SQLLite database. 

### Using the script
The script is normally run with just the configuration command line argument, as in: 
```
python3 dump_raw_data_from_influxdb.py ./config/co2_sensor_mapping.yml
```
In this mode, the script will perform an incremental load of all the systems defined in the configuration file.

If you want to import data from a specific sensor only, you can use:
```
python3 dump_raw_data_from_influxdb.py ./config/co2_sensor_mapping.yml HPP 445
```

This will only incrementally load the data for HPP sensor 445.

If you set the `--import-all` flag, a bulk import will be triggered, which will copy all the data from the source system, including the one already present there. If you set `--backfill n`, the incremental load will perform a backfill of the previous *n* days.


## Import Reference sensor data (Picarro CRDS)
The import is performed in two steps:
- In a first step, the data is copied on a staging table, which corresponds to the old CarboSense *NABEL_DUE*, *EMPA_LAEG* etc... tables
- In a second step, the data from these tables is consolidated into a single table called *picarro_data*, where the location of the data is identified by a location column in the table. 
Below, I will explain how to run the various steps
### Import into staging tables
The reference data from the Picarro CRDS are imported with the script [import_picarro_data.py](./CarbosenseDatabaseTools/import_picarro_data.py). 
The import configuration is very similar to the one for [raw data](#importing-raw-sensor-data) so I won't describe it here. The script is usually run using
```
python3 ./CarbosenseDatabaseTools/import_picarro_data.py ./config/picarro_mapping.yml
```

### Consolidate data
To consolidate the data, the following script is run:
```
python3  ./REF_measurement_processing/consolidate_reference_measurements.py ./config/reference_table_mapping.yaml
```



## Import Climate Chamber Calibration
In this section, we will give a short overwiew of the processes / files needed to run a climate chamber / pressure chamber calibration, as this process is more manual than the regular co-located calibration, where the data is automatically imported from the NABEL exports in `K:\Projects\Nabel`
### Files
- *Picarro*:
When you run a climate chamber calibration, please place the **original** `.dat` files as exported from the Picarro instrument into a folder of your choosing (So far, this was `K:\Carbosense\Data\Klimakammer_Versuche_27022017_XXXXXXXX`). You can then import them into the database  using:
  ```
  import_climate_chamber_data.py --picarro your_path your_regex
  ```
  Where `your_regex` is a regular expression / glob pattern to find all the files you want to import.
  Do not forget to add the picarro id to the `Deployment` table in the database. Before / after the calibration, run a picarro calibration
  and store the results in `G:\503_Themen\CarboSense`
- *Climate Chamber*: export the data (as is) from the climate chamber control software and store it in a folder of your choosing. To import the data, use:
  ```
  import_climate_chamber_data.py --climate your_path your_regex
  ```
- *Pressure* as the climate chamber does not have a pressure measurement, ask Nabel to export the pressure data from the calibration lab (same floor as the climate chamber). Place the csv files in a folder and use the following command to import them: 
  ```
  import_climate_chamber_data.py --pressure your_path your_regex
  ```
  You can specify the target table for importing the data using `--dest NAME`, where `NAME` is the name of the target database table.
- For data exported from the calibration chamber built by Roger Vonbank (**DUE7** in the *Location* table), use `--climate-new path regex`.

After importing the data, do not forget to consolidate the data using the second step described [above](#import-reference-sensor-data-picarro-crds)


# Import Meteo data
TBD

# Process sensor data

To process the sensor data, two different modes are available:
- Calibration, used to determine the calibration parameter
- Processing, used to apply the calibration parameters fitted in the previous step on new (or old) data.
In the following sections, I will describe these modes

## Calibration mode
The sensor data is processed using the [process_hpp_data.py](./HPP_measurement_processing/process_hpp_data.py) script. Usually, the script will process all the sensor of one type sequentially, computing the calibration parameters for all time the sensor was placed in the calibration table.

To run the calibration, use
```
python3 (./HPP_measurement_processing/process_hpp_data.py  ./config/co2_sensor_mapping.yaml calibrate {HPP,LP8} --plot your_plot_directory
```
Where `{HPP, LP8}` can be either of these two sensor names.
If you want to calibrate a single sensor only, append the sensor id before the `--plot` command:
```
python3 (./HPP_measurement_processing/process_hpp_data.py  ./config/co2_sensor_mapping.yaml calibrate HPP 445 --plot your_plot_directory
```

## Processing mode