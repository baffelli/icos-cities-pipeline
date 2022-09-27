#!/bin/bash

# ---

logfile="/project/CarboSense/Carbosense_Network/PROCESSING/Processing_$1.txt"

if [ -e $logfile ]
  then
  rm -f $logfile
fi

# ---


# ----------------------------
# Processing: MODE 01 / NO_DUE
# ----------------------------

if [ "$1" == "PROC_MODE01_NO_DUE" ]
  then
  
  echo `date` "START" > $logfile
  
  echo `date` "Compute_CarboSense_CO2_values.r F 1" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_CarboSense_CO2_values.r F 1
  
  echo `date` "Empa_OutlierDetection_P.r 1 NO_DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Empa_OutlierDetection_P.r 1 NO_DUE
    
  echo `date` "LP8_measurement_analysis.r 1" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_measurement_analysis.r 1
    
  echo `date` "LP8_DriftCorrection.r 1 F" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_DriftCorrection.r 1 F
    
  echo `date` "LP8_ConsistencyCheck.r 1" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_ConsistencyCheck.r 1
    
  echo `date` "Plot_CarboSense_CO2_TS.r 1" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Plot_CarboSense_CO2_TS.r 1
    
  echo `date` "Compute_Diurnal_CO2concentrations.r 1" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_Diurnal_CO2concentrations.r 1
  
  echo `date` "Comparison_CO2_LP8_REF.r 1 WET" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_REF.r 1 WET
  
   echo `date` "Comparison_CO2_LP8_REF.r 1 DRY" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_REF.r 1 DRY

  echo `date` "Comparison_CO2_LP8_LP8.r 1" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_LP8.r 1

  echo `date` "Comparison_CO2_LP8_HPP.r 1" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_HPP.r 1
  
  echo `date` "END" >> $logfile
  
fi

# ----------------------------
# Processing: MODE 02 / NO_DUE
# ----------------------------

if [ "$1" == "PROC_MODE02_NO_DUE" ]
  then
  
  echo `date` "START" > $logfile
  
  echo `date` "Compute_CarboSense_CO2_values.r F 2" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_CarboSense_CO2_values.r F 2
  
  echo `date` "Empa_OutlierDetection_P.r 2 NO_DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Empa_OutlierDetection_P.r 2 NO_DUE
    
  echo `date` "LP8_measurement_analysis.r 2" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_measurement_analysis.r 2
    
  echo `date` "LP8_DriftCorrection.r 2 F" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_DriftCorrection.r 2 F
    
  echo `date` "LP8_ConsistencyCheck.r 2" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_ConsistencyCheck.r 2
    
  echo `date` "Plot_CarboSense_CO2_TS.r 2" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Plot_CarboSense_CO2_TS.r 2
    
  echo `date` "Compute_Diurnal_CO2concentrations.r 2" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_Diurnal_CO2concentrations.r 2
  
  echo `date` "Comparison_CO2_LP8_REF.r 2 WET" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_REF.r 2 WET
  
  echo `date` "Comparison_CO2_LP8_REF.r 2 DRY" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_REF.r 2 DRY
  
  echo `date` "Comparison_CO2_LP8_LP8.r 2" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_LP8.r 2
  
  echo `date` "Comparison_CO2_LP8_HPP.r 2" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_HPP.r 2
  
  echo `date` "END" >> $logfile
  
fi

# ----------------------------------
# Processing: MODE 02 / NO_DUE / AMT
# ----------------------------------

if [ "$1" == "PROC_MODE02_NO_DUE_AMT" ]
  then
  
  echo `date` "START" > $logfile
  
  echo `date` "Compute_CarboSense_CO2_values.r F 20" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_CarboSense_CO2_values.r F 20
  
  echo `date` "Empa_OutlierDetection_P.r 20 NO_DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Empa_OutlierDetection_P.r 20 NO_DUE
    
  echo `date` "LP8_measurement_analysis.r 20" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_measurement_analysis.r 20
    
  echo `date` "LP8_DriftCorrection.r 20 F" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_DriftCorrection.r 20 F
    
  echo `date` "LP8_ConsistencyCheck.r 20" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_ConsistencyCheck.r 20
    
  echo `date` "Plot_CarboSense_CO2_TS.r 20" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Plot_CarboSense_CO2_TS.r 20
    
  echo `date` "Compute_Diurnal_CO2concentrations.r 20" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_Diurnal_CO2concentrations.r 20
  
  echo `date` "Comparison_CO2_LP8_REF.r 20 WET" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_REF.r 20 WET
  
  echo `date` "Comparison_CO2_LP8_REF.r 20 DRY" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_REF.r 20 DRY
  
  echo `date` "Comparison_CO2_LP8_LP8.r 20" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_LP8.r 20
  
  echo `date` "Comparison_CO2_LP8_HPP.r 20" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_HPP.r 20
  
  echo `date` "END" >> $logfile
  
fi

# ----------------------------
# Processing: MODE 03 / NO_DUE
# ----------------------------

if [ "$1" == "PROC_MODE03_NO_DUE" ]
  then
  
  echo `date` "START" > $logfile
  
  echo `date` "Compute_CarboSense_CO2_values.r F 3" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_CarboSense_CO2_values.r F 3
  
  echo `date` "Empa_OutlierDetection_P.r 3 NO_DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Empa_OutlierDetection_P.r 3 NO_DUE
    
  echo `date` "LP8_measurement_analysis.r 3" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_measurement_analysis.r 3
    
  echo `date` "LP8_DriftCorrection.r 3 F" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_DriftCorrection.r 3 F
    
  echo `date` "LP8_ConsistencyCheck.r 3" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_ConsistencyCheck.r 3
    
  echo `date` "Plot_CarboSense_CO2_TS.r 3" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Plot_CarboSense_CO2_TS.r 3
    
  echo `date` "Compute_Diurnal_CO2concentrations.r 3" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_Diurnal_CO2concentrations.r 3
  
  echo `date` "Comparison_CO2_LP8_REF.r 3 WET" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_REF.r 3 WET
  
  echo `date` "Comparison_CO2_LP8_REF.r 3 DRY" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_REF.r 3 DRY
  
  echo `date` "Comparison_CO2_LP8_LP8.r 3" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_LP8.r 3
  
  echo `date` "Comparison_CO2_LP8_HPP.r 3" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_HPP.r 3
  
  echo `date` "END" >> $logfile
  
fi

# ----------------------------------
# Processing: MODE 03 / NO_DUE / AMT
# ----------------------------------

if [ "$1" == "PROC_MODE03_NO_DUE_AMT" ]
  then
  
  echo `date` "START" > $logfile
  
  echo `date` "Compute_CarboSense_CO2_values.r F 30" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_CarboSense_CO2_values.r F 30
  
  echo `date` "Empa_OutlierDetection_P.r 30 NO_DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Empa_OutlierDetection_P.r 30 NO_DUE
    
  echo `date` "LP8_measurement_analysis.r 30" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_measurement_analysis.r 30
    
  echo `date` "LP8_DriftCorrection.r 30 F" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_DriftCorrection.r 30 F
    
  echo `date` "LP8_ConsistencyCheck.r 30" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_ConsistencyCheck.r 30
    
  echo `date` "Plot_CarboSense_CO2_TS.r 30" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Plot_CarboSense_CO2_TS.r 30
    
  echo `date` "Compute_Diurnal_CO2concentrations.r 30" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_Diurnal_CO2concentrations.r 30
  
  echo `date` "Comparison_CO2_LP8_REF.r 30 WET" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_REF.r 30 WET
  
  echo `date` "Comparison_CO2_LP8_REF.r 30 DRY" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_REF.r 30 DRY
  
  echo `date` "Comparison_CO2_LP8_LP8.r 30" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_LP8.r 30
  
  echo `date` "Comparison_CO2_LP8_HPP.r 30" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_HPP.r 30
  
  echo `date` "END" >> $logfile
  
fi


# ----------------------------
# Processing: MODE 01 / DUE
# ----------------------------

if [ "$1" == "PROC_MODE01_DUE" ]
  then
  
  echo `date` "START" > $logfile
  
  echo `date` "Compute_CarboSense_CO2_values.r F 1 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_CarboSense_CO2_values.r F 1 DUE
  
  echo `date` "Empa_OutlierDetection_P.r 1 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Empa_OutlierDetection_P.r 1 DUE
    
  echo `date` "LP8_measurement_analysis.r 1 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_measurement_analysis.r 1 DUE
    
  echo `date` "LP8_DriftCorrection.r 1 F DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_DriftCorrection.r 1 F DUE
    
  echo `date` "LP8_ConsistencyCheck.r 1 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_ConsistencyCheck.r 1 DUE
  
  echo `date` "END" >> $logfile
  
fi


# ----------------------------
# Processing: MODE 02 / DUE
# ----------------------------

if [ "$1" == "PROC_MODE02_DUE" ]
  then
  
  echo `date` "START" > $logfile
  
  echo `date` "Compute_CarboSense_CO2_values.r F 2 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_CarboSense_CO2_values.r F 2 DUE
  
  echo `date` "Empa_OutlierDetection_P.r 2 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Empa_OutlierDetection_P.r 2 DUE
    
  echo `date` "LP8_measurement_analysis.r 2 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_measurement_analysis.r 2 DUE
    
  echo `date` "LP8_DriftCorrection.r 2 F DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_DriftCorrection.r 2 F DUE
    
  echo `date` "LP8_ConsistencyCheck.r 2 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_ConsistencyCheck.r 2 DUE
  
  echo `date` "END" >> $logfile
  
fi

# -------------------------------
# Processing: MODE 02 / DUE / AMT
# -------------------------------

if [ "$1" == "PROC_MODE02_DUE_AMT" ]
  then
  
  echo `date` "START" > $logfile
  
  echo `date` "Compute_CarboSense_CO2_values.r F 20 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_CarboSense_CO2_values.r F 20 DUE
  
  echo `date` "Empa_OutlierDetection_P.r 20 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Empa_OutlierDetection_P.r 20 DUE
    
  echo `date` "LP8_measurement_analysis.r 20 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_measurement_analysis.r 20 DUE
    
  echo `date` "LP8_DriftCorrection.r 20 F DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_DriftCorrection.r 20 F DUE
    
  echo `date` "LP8_ConsistencyCheck.r 20 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_ConsistencyCheck.r 20 DUE
  
  echo `date` "END" >> $logfile
  
fi


# ----------------------------
# Processing: MODE 03 / DUE
# ----------------------------

if [ "$1" == "PROC_MODE03_DUE" ]
  then
  
  echo `date` "START" > $logfile
  
  echo `date` "Compute_CarboSense_CO2_values.r F 3 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_CarboSense_CO2_values.r F 3 DUE
  
  echo `date` "Empa_OutlierDetection_P.r 3 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Empa_OutlierDetection_P.r 3 DUE
    
  echo `date` "LP8_measurement_analysis.r 3 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_measurement_analysis.r 3 DUE
    
  echo `date` "LP8_DriftCorrection.r 3 F DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_DriftCorrection.r 3 F DUE
    
  echo `date` "LP8_ConsistencyCheck.r 3 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_ConsistencyCheck.r 3 DUE
  
  echo `date` "END" >> $logfile
  
fi

# --------------------------------
# Processing: MODE 03 / DUE / AMT
# --------------------------------

if [ "$1" == "PROC_MODE03_DUE_AMT" ]
  then
  
  echo `date` "START" > $logfile
  
  echo `date` "Compute_CarboSense_CO2_values.r F 30 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_CarboSense_CO2_values.r F 30 DUE
  
  echo `date` "Empa_OutlierDetection_P.r 30 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Empa_OutlierDetection_P.r 30 DUE
    
  echo `date` "LP8_measurement_analysis.r 30 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_measurement_analysis.r 30 DUE
    
  echo `date` "LP8_DriftCorrection.r 30 F DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_DriftCorrection.r 30 F DUE
    
  echo `date` "LP8_ConsistencyCheck.r 30 DUE" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/LP8_ConsistencyCheck.r 30 DUE
  
  echo `date` "END" >> $logfile
  
fi


# ----------------------------
# Processing: FINAL / NO_DUE
# ----------------------------

if [ "$1" == "PROC_FINAL_NO_DUE" ]
  then
  
  echo `date` "START" > $logfile
  
  echo `date` "Compute_CarboSense_CO2_values_FINAL.r" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_CarboSense_CO2_values_FINAL.r
    
  echo `date` "Plot_CarboSense_CO2_TS.r 0" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Plot_CarboSense_CO2_TS.r 0
    
  echo `date` "Compute_Diurnal_CO2concentrations.r 0" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Compute_Diurnal_CO2concentrations.r 0
  
  echo `date` "Comparison_CO2_LP8_REF.r 0 WET" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_REF.r 0 WET
  
    echo `date` "Comparison_CO2_LP8_REF.r 0 DRY" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_REF.r 0 DRY

  echo `date` "Comparison_CO2_LP8_LP8.r 0" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_LP8.r 0

  echo `date` "Comparison_CO2_LP8_HPP.r 0" >> $logfile
  Rscript /project/CarboSense/Software/LP8_measurement_processing/Comparison_CO2_LP8_HPP.r 0
  
  echo `date` "END" >> $logfile
  
fi
  
  

