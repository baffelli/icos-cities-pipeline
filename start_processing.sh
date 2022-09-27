#!/bin/bash
source  ~/.bash_profile
project_folder=/project/CarboSense/
LOG=$project_folder/Software/LOGs/processing_$(date --iso-8601).log
echo "The log files are in ${LOG}"
echo "$(date  --rfc-3339='seconds') [INFO] Started processing at " >> $LOG
cd $project_folder/Software 
echo "$(date  --rfc-3339='seconds') [INFO] Moved into processing folder " >> $LOG
eval "$(conda shell.bash hook)"
echo "$(date  --rfc-3339='seconds') [INFO] Activating conda environment" >> $LOG
conda activate icos-cities
echo "$(date  --rfc-3339='seconds') [INFO] Conda environment active" >> $LOG
echo "$(date  --rfc-3339='seconds') [INFO] Conda environment used: $CONDA_PREFIX" >> $LOG
echo "$(date  --rfc-3339='seconds') [INFO] Starting digdag" >> $LOG
/newhome/$(whoami)/bin/digdag r process_carbosense.dig --session daily   1>> $LOG 2>&1 &
echo "$(date  --rfc-3339='seconds') [INFO] Starting digdag" >> $LOG& 
