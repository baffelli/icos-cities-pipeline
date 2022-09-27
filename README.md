# CarboSense/ICOS Cities Pipeline

## Introduction

This repository contains *almost* all the code, configurations and scripts needed to process the ICOS-Cities/CarboSense mid- and low-cost sensor data. Usually, the entry point in the processing is done through the DigDag [DAG](process_carbosense.dig). If you inspect this (yaml) file, you will see a list of all processes and their dependencies. To learn more about digdag DAG, see the documentation [here](http://docs.digdag.io/concepts.html).

## Setup the Pipeline

To setup a working pipeline, you need to follow the steps below. In case of problems, you can contact Simone Baffelli. In the following sections, we assume you work on a Linux system with python>=3.10 and with an working installation of conda/anaconda.

To enable conda on DDM06, follow these steps:
1. Login on the server and load the conda module using: `module load anaconda`
2. Enable conda using `conda init`.

You are set!

### Install dependencies with conda

Using conda, install all the dependencies from [icos-cities.yaml](config/icos-cities.yml):
```bash
conda env create -f config/icos-cities.yml
```
The environment is called *icos-cities*. 
To activate the environment, use:
```bash
conda activate icos-cities
```

The repository also contains an older, mixed python and R  environment called *carbosense-processing*. Its configuration file is found in [environment.yaml](config/environment.yaml)

Use the same commands as above to initialise and use the environment.
The `carbosense-processing` envrionment is used to run the older scripts that were used during the CarboSense project.

### Install sensorutils manually

*Sensorutils* is a python package containing utilities used in all other scripts. To install it in the project, run the following command from the main repository directory:
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

Anyhwere where you see  a variable of the form `{something}`, replace it with the *value* of something in your file.

For example, assuming that your username is "user", the entry 
```
user = {insert your user here}
```
in the file above would be translated to:
```
user = user
```

An example of this file is found [here](./config/databases.cnf)

At the moment, there are three databases that are necessary for the pipeline, as you can see in this file:
- *CarboSense_MySQL*
  - This is the metadata and raw data database which is used to store processed data and raw data. To obtain a password, ask Simone Baffelli or Patrick Burkhalter
- *NabelGsn*, *empaGSN*
  - These databases contain some of the sensor data from NABLE and EMPA. To obtain access, ask Stephan Henne


### Configure Decentlab API Key
Obtain a InfluxDB API Key from decentlab. This keys allows you access to the timeseries database where the raw measurement data resides. 
Once you have the key, store it in a yaml file called `secrets.yml` in your unix home folder on the machine where the pipeline is run. 

For my user (*basi*), the full path would be `~/secrets.yml`, the contents are as follows:
```
- service:
          name: decentlab
          secret: some token
```
Do not forget to make the file only readable from the user running the pipeline. This can be done by the command:

```
chmod 700 ~/secrets.yml
```

### Configure network shares

Using automount, configure the following (Windows) network shares to be mounted on `/mnt/{username}/mnt/` in the following way:
- `K:/Projects/` 
  - One folder per project, e.g  `K:/Projects/Nabel`  mapped on  `/mnt/{username}/Nabel`
- `G:/`
  - On `/mnt/{username}/mnt/G`
To setup automount, ask Stephan Henne from 503 or Patrick Burckhalter from the IT department.

### Install DigDag

Follow the instructions [here](http://docs.digdag.io/getting_started.html#downloading-the-latest-version) to install the latest version of digdag. 

Additionally, configure the maximum number of task by creating the directory `~/.config/digdag/` with:
```
mkdir ~/.config/digdag/
```
And adding the following entry
```
executor.task_max_run = 3000
```
To the file `config`.
This can be done with:
```
echo "executor.task_max_run = 3000" > ~/.config/digdag/config
```

Digdag is used to automate the run of the pipeline at daily intervals. You might have to install jdk on conda if the currently available version from the OS doesn't let you run digdag.

The pipeline is self-explaining, all the processing steps are listed in the digdag DAG file.

## Running the pipeline

### Manual run

To manually run the pipeline, use the following commands:
```bash
conda activate icos-cities
digdag -r icos-cities.dig
```

### Automated run

So far, the pipeline run are scheduled to run once per day using a crontab file calling the script [start_processing.sh](start_processing.sh). 

In the future, it would be ideal to install digdag as a service, so that the pipeline runs can be scheduled directly with digdag and their operation can be verified more robustly. For this, consult the digdag documentation [here](http://docs.digdag.io/command_reference.html#server-mode-commands).

## Old CarboSense Pipeline

For documentation on the previous carbosense pipeline, please refer to [this document](./old_pipeline.md).

## sensorutils

This is a python package containing utilities used in all other scripts. To install it in the project, run the following command from the main repository directory:
```
pip install -e .
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
    grouping_key: 
      node: node
  dest:
    type: DB
    path: hpp_data
    db_prefix: CarboSense_MySQL
    date_from: 2017-01-01T00:00:00.000
    grouping_key: 
      node: SensorUnit_ID
    date_column: DATE(FROM_UNIXTIME(time))
    na: -999
  columns:
    - {name: time,  source_name: 'time', datatype: int, na: Null}
    - {name: SensorUnit_ID, source_name: node, datatype: int, na: Null}
    - {name: battery, source_name: battery, datatype: float, na: 0}
```

This defines a datasource mapping between the `source` and the `dest` systems. The source system is an InfluxDB database (the pipeline assumes by default this system to be the Decentlab one, but it can be easily changed, see the source code for this) with the *table* *measurements*. The field `grouping_key` is used to define a dictionary of columns identifying unique sensors in the source table so that incremental transfer is enabled. To make this work, the dictionary keys for both source and destination must match (or either of them should be left empty if either source or destination contains a single group).

Because InfluxDB uses `time` as a default timestamp column, no `date_column` is define for the source system. The destination system in this example is the table *hpp_data* on the database defined by the prefix *CarboSense_MySQL* in the `.my.cnf` file as described [above](#configure-database-connections). 

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
- ~~In a first step, the data is copied on a staging table, which corresponds to the old CarboSense *NABEL_DUE*, *EMPA_LAEG* etc... tables~~
- ~~In a second step, the data from these tables is consolidated into a single table called *picarro_data*, where the location of the data is identified by a location column in the table.~~ 
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

### The new way:

The instruction above refer to a more involved version that included staging tables for the different locations. You can also use a simpler way, by adding a `group` value to the destination configuration and calling this command:

```
python3  ./REF_measurement_processing/consolidate_reference_measurements.py ./config/reference_table_mapping.yaml DUE
```

This will run the import for the configuration item `DUE` in `reference_table_mapping.yaml`. Repeat this script 

## Import Climate Chamber Calibration

In this section, we will give a short overwiew of the processes / files needed to run a climate chamber / pressure chamber calibration, as this process is more manual than the regular co-located calibration, where the data is automatically imported from the NABEL exports in `K:\Projects\Nabel`

### Files

- *Picarro*:
When you run a climate chamber calibration, please place the **original** `.dat` files as exported from the Picarro instrument into a folder of your choosing (So far, this was `K:\Carbosense\Data\Klimakammer_Versuche_27022017_XXXXXXXX`). You can then import them into the database  using:
  ```
  import_climate_chamber_data.py picarro your_path your_regex dest_table {dest_location}
  ```
  Where `your_regex` is a regular expression / glob pattern to find all the files you want to import, `dest_table` is the table where to copy the data to, `{dest_location}` is the optional *LocationName* key, for example if you want the data be copied to the *picarro_data* table, where each location has a different id to distinguish the picarros.
  Do not forget to add the picarro id to the `Deployment` table in the database. Before / after the calibration, run a picarro calibration
  and store the results in `G:\503_Themen\CarboSense`
- *Climate Chamber*: export the data (as is) from the climate chamber control software and store it in a folder of your choosing. To import the data, use:
  ```
  import_climate_chamber_data.py climate your_path your_regex dest_table {dest_location}
  ```
- *Pressure* as the climate chamber does not have a pressure measurement, ask Nabel to export the pressure data from the calibration lab (same floor as the climate chamber). Place the csv files in a folder and use the following command to import them: 
  ```
  import_climate_chamber_data.py pressure your_path your_regex dest_table {dest_location}
  ```
- For data exported from the calibration chamber built by Roger Vonbank (**DUE7** in the *Location* table), use: `climate-new path regex`.
  ```
  import_climate_chamber_data.py climate-new your_path your_regex dest_table {dest_location}
  ```
In the latest version, this data directly contains the Picarro measurements, so you do not need any further steps.
After importing the data, do not forget to consolidate the data using the second step described [above](#import-reference-sensor-data-picarro-crds). However, this is only needed if you import in a staging table. 

If you want to add pressure measurements to the climate chamber from measurements in the Dubendorf NABEL station, run:

```
REF_measurement_processing/add_pressure_measurement_to_due7.py DUE1 DUE7
```
This example copies the pressure measurement from *DUE1* to *DUE7* assuming the pressure to be homogeneous in Dübendorf.

# Import Meteo data

To import the meteo data, run:

```
python3 MeteoSwissData4Carbosense/transfer_meteo_data.py ./config/meteo_data_mapping.yml /project/CarboSense/Data/METEO/MCH_DAILY_DATA_DUMP/ 'VQEA33*.csv'
```
The configuration file ` ./config/meteo_data_mapping.yml` follows the same incremental logic as described  [above](#background).

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

To process the data, run the code as shown above but use the `process` flag instead:

```
python3 (./HPP_measurement_processing/process_hpp_data.py  ./config/co2_sensor_mapping.yaml process HPP 445 --plot your_plot_directory
```

By default, the processed data is stored in the table `co2_level2`.


# Database Structure

## Motivation 
Most of the current processes in the icos-cities pipeline exchange data and store their state in the `CarboSense` database schema on `emp-sql-cs1.empa.emp-eaw.ch`.  This database also stores network metadata such as the deployment times of sensors, their geographical location and many more.

In the following section, I will describe the most important tables there, their structure and how to enter data.

## Database Tables
In the following, I will list the most relevant database tables and give some information how to enter / modify the data there. 

As a general information, consider that the date `2100-01-01 00:00:00` stands for an unknown time while the number `-999` is used to mark missing values. The latter is an historical artifact, which has been removed from most tables and replaced with SQL-conforming `NULL`. Feel free to replace this value with `NULL` whenever you encouter it. 

On the long term, it would be optimal if all the column names / table names  would be changed to *snake_case*. Feel free to do it, but ensure that the python code is updated accordingly. This must be mainly done through the [models](./sensorutils/models.py) file, where the SQLalchemy classes mapping the database tables reside, but do not forget to check the rest of the code.

Finally, here are the table descriptions:

### Tables

* `Deployment`: 
    
    Gives the location and the timespan
    when a sensor was deployed at a measurement station. The column `SensorUnit_ID` must refer to one ID in the table `SensorUnits`, while `LocationName` must be present in the `Location` table (foreign key). `CalibrationMode` should be set to 1 or 2 whenever a sensor is to be calibrated by climate chamber or co-location respectively.
    When you finish a deployment, finish it by setting the `Date_UTC_from` value to the finishing date and **NOT** by removing the entry. 
    
* `Location`:

  Contains the name, address and other metadata for measurement stations. The validity period gived by  `Date_UTC_from` and `Date_UTC_to` serves to identify measurement stations whose location was changed (as an example, the *DUE1* NABEL station which was moved in April 2020)

* `Sensors`:

  Contains a list of all sensors (identified by `serialnumber` and `Type`) that were ever contained in a Sensor Unit (the phisical container sending the data over LoRaWan identified by `SensorUnit_ID`). When you replace a sensor in a sensor unit, terminate the latest entriy and add a new entry with the new `serialnumber`.

* `ref_gas_cylinder`:
 
  Lists all the reference gas cylinders using through the project, their validity time, the volume, owner and date of the next due pressure test. To use a cylinder in other tables such as `ref_gas_cylinder_analysis`, you must first create an entry here (foreign key constraint).

* `ref_gas_cylinder_analysis`

  Lists the analysed value for filled reference gas cylinders. The values are usually imported using the python script [import_bottle_calibrations.py](./CarbosenseDatabaseTools/import_bottle_calibrations.py) after the cylinders are analysed. The table is used to compute the HPP two point calibration values

* `cylinder_deployment`
  Lists which 

### Views
