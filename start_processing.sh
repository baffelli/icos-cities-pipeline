#!/bin/bash
source  ~/.bash_profile
LOG=/project/CarboSense/Software/LOGs/processing_$(date --iso-8601).log
echo "$(date  --rfc-3339='seconds') [INFO] Started processing at " >> $LOG
cd /project/CarboSense/Software 
echo "$(date  --rfc-3339='seconds') [INFO] Moved into processing folder " >> $LOG
eval "$(conda shell.bash hook)"
echo "$(date  --rfc-3339='seconds') [INFO] Activating conda environment" >> $LOG
conda activate carbosense-processing
echo "$(date  --rfc-3339='seconds') [INFO] Conda environment active" >> $LOG
echo "$(date  --rfc-3339='seconds') [INFO] Conda environment used: $CONDA_PREFIX" >> $LOG
echo "$(date  --rfc-3339='seconds') [INFO] Starting digdag" >> $LOG
/newhome/basi/bin/digdag r process_carbosense.dig --session daily   1>> $LOG 2>&1 &
echo "$(date  --rfc-3339='seconds') [INFO] Starting digdag" >> $LOG& 
