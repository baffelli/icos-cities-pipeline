# CO2_HPP_SensorCalibration.r
# --------------------------------
#
# Author: Michael Mueller
#
#
# --------------------------------

# Remarks:
# - Computations refer to UTC.
#
#

## clear variables
rm(list=ls(all=TRUE))
gc()

## libraries
library(openair)
library(DBI)
require(RMySQL)
require(chron)
library(MASS)
# library(pryr)
library(mgcv)
library(data.table)

## source

source("/project/CarboSense/Software/CarboSenseUtilities/api-v1.3.r")
source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

## directories

resultdir <- "/project/CarboSense/Carbosense_Network/HPP_Calibration/HPP_CalibrationResults_DEFAULT/"

## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

CV_mode   <- args[1]
resultdir <- args[2]

### ----------------------------------------------------------------------------------------------------------------------------

# Cross-validation (CV) mode
#
# CV_mode : 0 ->  no CV
# CV_mode : 1 ->  field only
# CV_mode : 3 ->  2+3 -> 1
# CV_mode : 4 ->  2+3+BP -> 1
# CV_mode : 5 ->  2  -> 1
# CV_mode : 6 ->  2+BP -> 1
# CV_mode : 7 ->  2+3+BP -> 1

if(!CV_mode%in%c(0:7)){
  stop()
}

### ----------------------------------------------------------------------------------------------------------------------------

## Decentlab DB information

DL_DB_domain <- "empa-503.decentlab.com"
DL_DB_apiKey <- "eyJrIjoiWkJaZjFDTEhUYm5sNmdWUG14a3NpdVcwTmZCaHloZVEiLCJuIjoibWljaGFlbC5tdWVsbGVyQGVtcGEuY2giLCJpZCI6MX0="

### ----------------------------------------------------------------------------------------------------------------------------

SensorUnit_ID_2_cal   <- c(390,342)
n_SensorUnit_ID_2_cal <- length(SensorUnit_ID_2_cal)

### ----------------------------------------------------------------------------------------------------------------------------

# sensor model versions

sensor_models   <- list()

if(T){
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_T",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I(T)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I(hpp_temperature_mcu)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_H2O",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I(H2O)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_AH",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I(AH_COMP)")
  # 
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_H2O_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I(H2O) + I(hpp_temperature_mcu)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_AH_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I(AH_COMP) + I(hpp_temperature_mcu)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_AHT_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I(AH_COMP*(273.15+T)/(273.15+65)) + I(hpp_temperature_mcu)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_H2O_HPPT_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I(H2O) + I(hpp_temperature_mcu) + I(days)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_AH_HPPT_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I(AH_COMP) + I(hpp_temperature_mcu) + I(days)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_H2O_HPPT2",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I(H2O) + I(hpp_temperature_mcu) + I(hpp_temperature_mcu^2)")
  
  
  #
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)))")
  
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + days")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_H2O",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + H2O")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_AH",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + AH_COMP")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + hpp_temperature_mcu")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_H2O_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + H2O + hpp_temperature_mcu")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_H2O_HPPT_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + H2O + hpp_temperature_mcu + days")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_AH_HPPT_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + AH_COMP + hpp_temperature_mcu + days")
  
  #
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))")
  
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + days")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_AH",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + AH_COMP")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_AH_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + AH_COMP + days")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_H2O",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I(H2O)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_H2O_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I(H2O) + days")
  
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + hpp_temperature_mcu")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_HPPT_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + hpp_temperature_mcu + days")
  
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_H2O_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + H2O + hpp_temperature_mcu")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_H2O_HPPT_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + H2O + hpp_temperature_mcu + days")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_AH_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + AH + hpp_temperature_mcu")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_AH_HPPT_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + AH + hpp_temperature_mcu + days")
  
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_AHT_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I(AH_COMP*(273.15+T)/(273.15+65)) + I(hpp_temperature_mcu)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_AHT2_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I(AH_COMP*(273.15+T)/(273.15+65)) + I((AH_COMP*(273.15+T)/(273.15+65))^2) + I(hpp_temperature_mcu)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_AHT_HPPT_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I(AH_COMP*(273.15+T)/(273.15+65)) + I(hpp_temperature_mcu)+days")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_AHT2_HPPT_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I(AH_COMP*(273.15+T)/(273.15+65)) + I((AH_COMP*(273.15+T)/(273.15+65))^2) + I(hpp_temperature_mcu)+days")
}


if(CV_mode==7){
  
  sensor_models   <- list()
  
  
  
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_NL_CP1_pIR2",
  #                                                 modelType="SM_IR",
  #                                                 formula=NA)
  
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_NL_CP1_pIR2_TFac",
  #                                                  modelType="SM_IR",
  #                                                  formula=NA)
  
  
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_H2O",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + H2O + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_HPPT",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + hpp_temperature_mcu + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_H2O_HPPT",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + H2O + hpp_temperature_mcu + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_HPPTdet",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + hpp_temperature_detector + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_H2O_HPPTdet",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + H2O + hpp_temperature_detector + deltaPC01 + deltaPC02")
  # 
  # 
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_H2O",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + H2O + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_HPPT",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + hpp_temperature_mcu + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_H2O_HPPT",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + H2O + hpp_temperature_mcu + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_HPPTdet",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + hpp_temperature_detector + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR_H2O_HPPTdet",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + H2O + hpp_temperature_detector + deltaPC01 + deltaPC02")
  # 
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_H2O",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + H2O + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_HPPT",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + hpp_temperature_mcu + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_H2O_HPPT",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + H2O + hpp_temperature_mcu + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_HPPTdet",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + hpp_temperature_detector + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_H2O_HPPTdet",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + H2O + hpp_temperature_detector + deltaPC01 + deltaPC02")
  # 
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR2",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pLin_pIR2_H2O",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + H2O + deltaPC01 + deltaPC02")
  # 
  
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_AH_DRIFT",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(AH_COMP) + days")
  # 
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + deltaPC01 + deltaPC02")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_H2O",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + H2O + deltaPC01 + deltaPC02")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + deltaPC01 + deltaPC02")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_H2O",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + H2O + deltaPC01 + deltaPC02")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + hpp_temperature_mcu + deltaPC01 + deltaPC02")
  
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_H2O_HPPTdT",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + H2O + I(hpp_temperature_mcu_dT) + deltaPC01 + deltaPC02")
  # 
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_GAM_PIR2_H2O_HPPTdT",
  #                                                  modelType="GAM",
  #                                                  formula="CO2 ~ hpp_logIR + div_hpp_ir + p2_div_ir + p_div_ir + H2O + s(hpp_temperature_mcu_dT) + deltaPC01 + deltaPC02")
  
  
  
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_H2OIR",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(H2O/hpp_ir) + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_H2OIR2",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(H2O/hpp_ir) + I(H2O^2/hpp_ir) + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_AH",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(AH_COMP/hpp_ir) + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_AH2",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(AH_COMP/hpp_ir) + I(AH_COMP^2/hpp_ir) + deltaPC01 + deltaPC02")
  
  
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_H2O_HPPT",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + H2O + hpp_temperature_mcu + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_AH",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(AH_COMP) + deltaPC01 + deltaPC02")
  # 
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR3",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^3/hpp_ir)) + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR3_H2O",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^3/hpp_ir)) + H2O + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR3_HPPT",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^3/hpp_ir)) + hpp_temperature_mcu + deltaPC01 + deltaPC02")
  # 
  
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + deltaPC01 + deltaPC02")
  # 
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR02",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR02_H2O",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + H2O + deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_TFac",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ -1 + TFac +  I(hpp_logIR*TFac) + I(TFac/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)*TFac/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2*TFac/hpp_ir)) + deltaPC01 + deltaPC02")
  
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIRTEST",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2)) + I((((hpp_pressure-1013.25)/1013.25)^2*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir))+ deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIRTEST_H2O",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2)) + I((((hpp_pressure-1013.25)/1013.25)^2*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) +  I(H2O) +deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIRTEST_H2OIR",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2)) + I((((hpp_pressure-1013.25)/1013.25)^2*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) +  I(H2O/hpp_ir) +deltaPC01 + deltaPC02")
  # 
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIRTEST_AH",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2)) + I((((hpp_pressure-1013.25)/1013.25)^2*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir))+  I(AH_COMP/hpp_ir) +deltaPC01 + deltaPC02")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIRTEST_AH2",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2)) + I((((hpp_pressure-1013.25)/1013.25)^2*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir))+  I(AH_COMP/hpp_ir)+  I(AH_COMP^2/hpp_ir) +deltaPC01 + deltaPC02")
  # 
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIRTEST_AH_RED",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2)) + I((((hpp_pressure-1013.25)/1013.25)^2*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir))+  I(AH_COMP/hpp_ir)")
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIRTEST_AH2_RED",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2)) + I((((hpp_pressure-1013.25)/1013.25)^2*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir))+  I(AH_COMP/hpp_ir)+  I(AH_COMP^2/hpp_ir)")
  # 
  # 
  # 
  # 
  # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIRTEST_AH_HPPT",
  #                                                  modelType="LM_IR",
  #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2)) + I((((hpp_pressure-1013.25)/1013.25)^2*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir))+  I(AH_COMP/hpp_ir) + hpp_temperature_mcu + deltaPC01 + deltaPC02")
  # 
  # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIRTEST_AH_HPPTT",
  # #                                                  modelType="LM_IR",
  # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2)) + I((((hpp_pressure-1013.25)/1013.25)^2*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir))+  I(AH_COMP/hpp_ir) + HPPT_T + deltaPC01 + deltaPC02")
  # # 
  
}


if(CV_mode%in%c(0,1)){
  
  sensor_models   <- list()
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir))")
  
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_H2O",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(H2O)")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(hpp_temperature_mcu)")
  
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_H2O_DRIFT",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(H2O) + days")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_H2O_DRIFT2",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(H2O) + days + I(days^2)")
}

### ----------------------------------------------------------------------------------------------------------------------------

statistics      <- NULL
statistic_scale <- NULL

if(!dir.exists(resultdir)){
  dir.create((gsub(pattern = "/$",replacement = "",resultdir)))
}

plotdir_allModels <- paste(resultdir,"AllModels/",sep="")
if(!dir.exists(plotdir_allModels)){
  dir.create((plotdir_allModels))
}


### ----------------------------------------------------------------------------------------------------------------------------


for(ith_SensorUnit_ID_2_cal in 1:n_SensorUnit_ID_2_cal){
  
  # calibration information
  
  query_str       <- paste("SELECT * FROM Calibration where SensorUnit_ID=",SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal]," and CalMode IN (1,2,3);",sep="")
  drv             <- dbDriver("MySQL")
  con             <- dbConnect(drv, group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tbl_calibration <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  tbl_calibration$Date_UTC_from <- strptime(tbl_calibration$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_calibration$Date_UTC_to   <- strptime(tbl_calibration$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
  
  # sensor information
  
  query_str   <- paste("SELECT * FROM Sensors where SensorUnit_ID=",SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal]," and Type='HPP';",sep="")
  drv         <- dbDriver("MySQL")
  con         <- dbConnect(drv, group="CarboSense_MySQL")
  res         <- dbSendQuery(con, query_str)
  tbl_sensors <- fetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  tbl_sensors$Date_UTC_from <- strptime(tbl_sensors$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_sensors$Date_UTC_to   <- strptime(tbl_sensors$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
  # Combination of information from Sensors / Calibration
  
  sensor_calibration_info <- NULL
  
  for(ith_sensor in 1:dim(tbl_sensors)[1]){
    
    for(ith_cal in 1:dim(tbl_calibration)[1]){
      
      if(tbl_calibration$Date_UTC_to[ith_cal]<tbl_sensors$Date_UTC_from[ith_sensor]){
        next
      }
      if(tbl_calibration$Date_UTC_from[ith_cal]>tbl_sensors$Date_UTC_to[ith_sensor]){
        next
      }
      
      if(tbl_calibration$Date_UTC_from[ith_cal]<=tbl_sensors$Date_UTC_from[ith_sensor]){
        date_UTC_from <- tbl_sensors$Date_UTC_from[ith_sensor]
      }else{
        date_UTC_from <- tbl_calibration$Date_UTC_from[ith_cal]
      }
      
      if(tbl_calibration$Date_UTC_to[ith_cal]<=tbl_sensors$Date_UTC_to[ith_sensor]){
        date_UTC_to <- tbl_calibration$Date_UTC_to[ith_cal]
      }else{
        date_UTC_to <- tbl_sensors$Date_UTC_to[ith_sensor]
      }
      
      sensor_calibration_info <- rbind(sensor_calibration_info, data.frame(SensorUnit_ID = SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],
                                                                           Type=tbl_sensors$Type[ith_sensor],
                                                                           Serialnumber=tbl_sensors$Serialnumber[ith_sensor],
                                                                           Date_UTC_from=date_UTC_from,
                                                                           Date_UTC_to=date_UTC_to,
                                                                           LocationName=tbl_calibration$LocationName[ith_cal],
                                                                           DBTableNameRefData=tbl_calibration$DBTableNameRefData[ith_cal],
                                                                           CalMode=tbl_calibration$CalMode[ith_cal],
                                                                           stringsAsFactors=F))
    }
  }
  
  #
  
  sensor_calibration_info$timestamp_from <- as.numeric(difftime(time1=sensor_calibration_info$Date_UTC_from,
                                                                time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                                                                units="secs",
                                                                tz="UTC"))
  
  sensor_calibration_info$timestamp_to   <- as.numeric(difftime(time1=sensor_calibration_info$Date_UTC_to,
                                                                time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),
                                                                units="secs",
                                                                tz="UTC"))
  
  #
  
  
  sensors2cal   <- sort(unique(sensor_calibration_info$Serialnumber))
  n_sensors2cal <- length(sensors2cal)
  
  for(ith_sensor2cal in 1:n_sensors2cal){
    
    id_cal_periods   <- which(sensor_calibration_info$Serialnumber==sensors2cal[ith_sensor2cal])
    n_id_cal_periods <- length(id_cal_periods)
    
    
    ## -------------------------------
    
    # Import of sensor data
    
    sensor_data <- NULL
    
    query_str   <- paste("SELECT * FROM SensorExclusionPeriods where SensorUnit_ID=",SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal]," and Serialnumber='",sensors2cal[ith_sensor2cal],"' and Type='HPP';",sep="")
    drv         <- dbDriver("MySQL")
    con         <- dbConnect(drv, group="CarboSense_MySQL")
    res         <- dbSendQuery(con, query_str)
    SEP         <- fetch(res, n=-1)
    dbClearResult(res)
    dbDisconnect(con)
    
    if(dim(SEP)[1]>0){
      SEP$Date_UTC_from <- strptime(SEP$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
      SEP$Date_UTC_to   <- strptime(SEP$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
    }
    
    for(ith_id_cal_period in 1:n_id_cal_periods){
      
      timeFilter <- paste("time >= ",sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]],"s AND time < ",sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]],"s",sep="")
      device     <- paste("/",sensor_calibration_info$SensorUnit_ID[id_cal_periods[ith_id_cal_period]],"/",sep="")
      
      tmp0 <- query(domain=DL_DB_domain,
                    apiKey=DL_DB_apiKey,
                    timeFilter=timeFilter,
                    device = device,
                    location = "//",
                    sensor = "/senseair-hpp-co2|senseair-hpp-ir-signal|senseair-hpp-lpl-signal|senseair-hpp-pressure-filtered|senseair-hpp-pressure|senseair-hpp-status|senseair-hpp-temperature-detector|senseair-hpp-temperature-mcu|time/",
                    channel = "//",
                    aggFunc = "",
                    aggInterval = "",
                    doCast = FALSE,
                    timezone = 'UTC')
      
      tmp0      <- as.data.table(tmp0)
      tmp0$time <- round(as.numeric(difftime(time1=tmp0$time,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))*1e3)
      tmp0      <- dcast.data.table(tmp0, time ~ series, fun.aggregate = mean,value.var = "value")
      tmp0$time <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp0$time/1e3
      tmp0      <- as.data.frame(tmp0)
      
      
      cn            <- colnames(tmp0)
      cn            <- gsub(pattern = "-",  replacement = "_", x = cn)
      cn            <- gsub(pattern = "\\.",replacement = "_", x = cn)
      cn            <- gsub(pattern = paste(sensor_calibration_info$SensorUnit_ID[id_cal_periods[ith_id_cal_period]],"_",sep=""), replacement = "", x = cn)
      
      cn            <- gsub(pattern = "battery",                           replacement = "battery",                  x=cn)
      cn            <- gsub(pattern = "senseair_hpp_co2_filtered",         replacement = "hpp_co2_flt",              x=cn)
      cn            <- gsub(pattern = "senseair_hpp_co2",                  replacement = "hpp_co2",                  x=cn)
      cn            <- gsub(pattern = "senseair_hpp_dt",                   replacement = "hpp_dt",                   x=cn)
      cn            <- gsub(pattern = "senseair_hpp_id",                   replacement = "hpp_id",                   x=cn)
      cn            <- gsub(pattern = "senseair_hpp_ir_high",              replacement = "hpp_ir_high",              x=cn)
      cn            <- gsub(pattern = "senseair_hpp_ir_low",               replacement = "hpp_ir_low",               x=cn)
      cn            <- gsub(pattern = "senseair_hpp_ir_signal",            replacement = "hpp_ir",                   x=cn)
      cn            <- gsub(pattern = "senseair_hpp_lpl_signal",           replacement = "hpp_lpl",                  x=cn)
      cn            <- gsub(pattern = "senseair_hpp_pressure_filtered",    replacement = "hpp_pressure_flt",         x=cn)
      cn            <- gsub(pattern = "senseair_hpp_pressure",             replacement = "hpp_pressure",             x=cn)
      cn            <- gsub(pattern = "senseair_hpp_sclraw",               replacement = "hpp_sclraw",               x=cn)
      cn            <- gsub(pattern = "senseair_hpp_status",               replacement = "hpp_status",               x=cn)
      cn            <- gsub(pattern = "senseair_hpp_supply",               replacement = "hpp_supply",               x=cn)
      cn            <- gsub(pattern = "senseair_hpp_temperature_detector", replacement = "hpp_temperature_detector", x=cn)
      cn            <- gsub(pattern = "senseair_hpp_temperature_mcu",      replacement = "hpp_temperature_mcu",      x=cn)
      cn            <- gsub(pattern = "senseair_hpp_vbb",                  replacement = "hpp_vbb",                  x=cn)
      cn            <- gsub(pattern = "time",                              replacement = "date",                     x=cn)
      
      colnames(tmp0) <- cn
      
      
      # Ensure that all required columns are defined
      
      cn_required   <- c("date","battery","hpp_co2","hpp_co2_flt","hpp_dt","hpp_id","hpp_ir_high","hpp_ir_low","hpp_ir","hpp_lpl","hpp_pressure","hpp_pressure_flt","hpp_sclraw","hpp_status","hpp_supply","hpp_temperature_detector","hpp_temperature_mcu","hpp_vbb")
      cn_required   <- c("date","hpp_co2","hpp_ir","hpp_lpl","hpp_pressure","hpp_status","hpp_temperature_detector","hpp_temperature_mcu")
      n_cn_required <- length(cn_required)
      
      tmp <- as.data.frame(matrix(NA,ncol=n_cn_required,nrow=dim(tmp0)[1]),stringsAsFactors=F)
      
      for(ith_cn_req in 1:n_cn_required){
        pos_tmp0 <- which(colnames(tmp0)==cn_required[ith_cn_req])
        if(length(pos_tmp0)>0){
          tmp[,ith_cn_req] <- tmp0[,pos_tmp0]
        }else{
          tmp[,ith_cn_req] <- NA
        }
      }
      
      colnames(tmp) <- cn_required
      
      rm(cn_required,n_cn_required,tmp0)
      gc()
      
      # Force time to full preceding minute
      
      tmp$date      <- as.POSIXct(tmp$date)
      tmp$date      <- strptime(strftime(tmp$date,"%Y%m%d%H%M00",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
      tmp$secs      <- as.numeric(difftime(time1=tmp$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
      
      # Check data imported from Influx-DB (no duplicates)
      
      tmp           <- tmp[!duplicated(tmp$date),]
      
      # set CO2 measurements to NA if status !=0
      
      id_set_to_NA <- which(!tmp$hpp_status%in%c(0,32))
      if(length(id_set_to_NA)){
        tmp$hpp_co2[id_set_to_NA] <- NA
      }
      
      # set CO2 measurements to NA if malfunctioning is known for specific period
      if(dim(SEP)[1]>0){
        for(ith_SEP in 1:dim(SEP)[1]){
          id <- which(tmp$date>=SEP$Date_UTC_from[ith_SEP] & tmp$date<=SEP$Date_UTC_to[ith_SEP])
          if(length(id)>0){
            tmp$hpp_co2[id] <- NA
          }
        }
      }
      
      
      # combine sensor_data and tmp
      if(is.null(sensor_data)){
        sensor_data <- tmp
      }else{
        sensor_data <- rbind(sensor_data,tmp)
      }
    }
    
    sensor_data              <- sensor_data[order(sensor_data$date),] 
    
    sensor_data$hpp_pressure <- sensor_data$hpp_pressure / 1e2
    
    
    ## -------------------------------
    
    
    # Import of reference data
    
    ref_data  <- NULL
    
    for(ith_id_cal_period in 1:n_id_cal_periods){
      
      if(sensor_calibration_info$CalMode[id_cal_periods[ith_id_cal_period]]==1){
        query_str <- paste("SELECT timestamp,CO2,CO2_MIN,CO2_MAX,CO2_F,CO2_DRY_CAL as CO2_DRY,CO2_DRY_F,H2O,H2O_F,T,T_F,RH,RH_F,pressure,pressure_F,CO2_10MIN_AV,CO2_DRY_10MIN_AV,H2O_10MIN_AV,T_10MIN_AV,RH_10MIN_AV, pressure_10MIN_AV FROM ",sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]," where timestamp >= ",sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]]," and timestamp < ",sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]],";",sep="")
      }
      if(sensor_calibration_info$CalMode[id_cal_periods[ith_id_cal_period]]%in%c(2)){
        query_str <- paste("SELECT timestamp,CO2,CO2_F,CO2_DRY,H2O,H2O_F,T,T_F,RH,RH_F,pressure,pressure_F,CO2_10MIN_AV,CO2_DRY_10MIN_AV,H2O_10MIN_AV,T_10MIN_AV,RH_10MIN_AV, pressure_10MIN_AV FROM ",sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]," where timestamp >= ",sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]]," and timestamp < ",sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]],";",sep="")
      }
      if(sensor_calibration_info$CalMode[id_cal_periods[ith_id_cal_period]]%in%c(3)){
        if(sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]%in%c("PressureChamber_00_METAS")){
          query_str <- paste("SELECT timestamp,CO2,CO2_F,CO2_DRY,CO2_DRY_F,H2O,H2O_F,pressure,pressure_F,CO2_10MIN_AV,CO2_DRY_10MIN_AV,H2O_10MIN_AV, pressure_10MIN_AV FROM ",sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]," where timestamp >= ",sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]]," and timestamp < ",sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]],";",sep="")
        }
        if(sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]%in%c("PressureChamber_01_DUE")){
          query_str <- paste("SELECT timestamp,CO2,CO2_F,CO2_DRY,CO2_DRY_F,H2O,H2O_F,T,T_F,pressure,pressure_F,CO2_10MIN_AV,CO2_DRY_10MIN_AV,H2O_10MIN_AV,T_10MIN_AV, pressure_10MIN_AV FROM ",sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]," where timestamp >= ",sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]]," and timestamp < ",sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]],";",sep="")
        }
      }
      
      drv       <- dbDriver("MySQL")
      con       <- dbConnect(drv, group="CarboSense_MySQL")
      res       <- dbSendQuery(con, query_str)
      tmp       <- fetch(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      if(dim(tmp)[1]>0){
        
        if(sensor_calibration_info$CalMode[id_cal_periods[ith_id_cal_period]]==1){
          
          # tmp$CO2_DRY_10MIN_AV <- -999
          # tmp$CO2_DRY          <- -999
          tmp$CalMode          <- 1
          
          tmp$timestamp <- tmp$timestamp - 60
        }
        
        if(sensor_calibration_info$CalMode[id_cal_periods[ith_id_cal_period]]==2){
          
          tmp <- data.frame(timestamp=tmp$timestamp,
                            CO2=tmp$CO2,
                            CO2_MIN=-999,
                            CO2_MAX=-999,
                            CO2_F=as.numeric(tmp$CO2_F==1 & tmp$CO2_10MIN_AV != -999),
                            CO2_DRY=tmp$CO2_DRY,
                            CO2_DRY_F=as.numeric(tmp$CO2_10MIN_AV != -999),
                            H2O=tmp$H2O,
                            H2O_F=as.numeric(tmp$H2O_F==1 & tmp$H2O_10MIN_AV != -999),
                            T=tmp$T,
                            T_F=as.numeric(tmp$T_F==1 & tmp$T_10MIN_AV != -999),
                            RH=tmp$RH,
                            RH_F=as.numeric(tmp$RH_F==1 & tmp$RH_10MIN_AV != -999),
                            pressure=tmp$pressure,
                            pressure_F=as.numeric(tmp$pressure_F==1 & tmp$pressure_10MIN_AV != -999),
                            CO2_10MIN_AV=-999,
                            CO2_DRY_10MIN_AV=-999,
                            H2O_10MIN_AV=-999,
                            T_10MIN_AV=-999,
                            RH_10MIN_AV=-999,
                            pressure_10MIN_AV=-999,
                            CalMode=2,
                            stringsAsFactors=F)
          
          if(all(tmp$RH == -999)){
            tmp$RH   <- 50
            tmp$RH_F <- 1
          }
        }
        
        if(sensor_calibration_info$CalMode[id_cal_periods[ith_id_cal_period]]==3){
          
          if(sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]%in%c("PressureChamber_00_METAS")){
            tmp <- data.frame(timestamp=tmp$timestamp,
                              CO2=tmp$CO2_10MIN_AV,
                              CO2_MIN=-999,
                              CO2_MAX=-999,
                              CO2_F=as.numeric(tmp$CO2_10MIN_AV != -999),
                              CO2_DRY=tmp$CO2_DRY,
                              CO2_DRY_F=as.numeric(tmp$CO2_DRY_F != -999),
                              H2O=tmp$H2O,
                              H2O_F=tmp$H2O_F,
                              T=-999,
                              T_F=0,
                              RH=-999,
                              RH_F=0,
                              pressure=tmp$pressure_10MIN_AV,
                              pressure_F=as.numeric(tmp$pressure_10MIN_AV != -999),
                              CO2_10MIN_AV=-999,
                              CO2_DRY_10MIN_AV=-999,
                              H2O_10MIN_AV=-999,
                              T_10MIN_AV=-999,
                              RH_10MIN_AV=-999,
                              pressure_10MIN_AV=-999,
                              CalMode=3,
                              stringsAsFactors=F)
          }
          
          if(sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]%in%c("PressureChamber_01_DUE")){
            tmp <- data.frame(timestamp=tmp$timestamp,
                              CO2=tmp$CO2,
                              CO2_MIN=-999,
                              CO2_MAX=-999,
                              CO2_F=as.numeric(tmp$CO2_F==1 & tmp$CO2_10MIN_AV != -999),
                              CO2_DRY=tmp$CO2_DRY,
                              CO2_DRY_F=as.numeric(tmp$CO2_DRY_F==1 & tmp$CO2_DRY_10MIN_AV != -999),
                              H2O=tmp$H2O,
                              H2O_F=as.numeric(tmp$H2O_F==1 & tmp$H2O_10MIN_AV != -999),
                              T=tmp$T,
                              T_F=as.numeric(tmp$T_F==1 & tmp$T_10MIN_AV != -999),
                              RH=-999,
                              RH_F=0,
                              pressure=tmp$pressure,
                              pressure_F=as.numeric(tmp$pressure_F==1 & tmp$pressure_10MIN_AV != -999),
                              CO2_10MIN_AV=-999,
                              CO2_DRY_10MIN_AV=-999,
                              H2O_10MIN_AV=-999,
                              T_10MIN_AV=-999,
                              RH_10MIN_AV=-999,
                              pressure_10MIN_AV=-999,
                              CalMode=3,
                              stringsAsFactors=F)
          }
          
          
          if(all(tmp$RH == -999)){
            tmp$RH   <- 50
            tmp$RH_F <- 1
          }
        }
        
        ref_data <- rbind(ref_data,tmp)
      }
    }
    
    
    if(dim(ref_data)[1]==0){
      stop("Error: No reference data found.")
    }
    
    
    #
    
    colnames(ref_data)[which(colnames(ref_data)=="timestamp")] <- "date"
    ref_data$date <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + ref_data$date
    ref_data      <- ref_data[order(ref_data$date),]
    ref_data$secs <- as.numeric(difftime(time1=ref_data$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
    
    
    # SET data to NA if flag==0 or data == -999
    
    u_cn   <- gsub(pattern = "_F",replacement = "",colnames(ref_data))
    u_cn   <- sort(unique(u_cn))
    u_cn   <- u_cn[which(!u_cn%in%c("date","CO2_MAX","CO2_MIN","CalMode","secs","CO2_10MIN_AV","CO2_DRY_10MIN_AV","H2O_10MIN_AV","T_10MIN_AV","RH_10MIN_AV","pressure_10MIN_AV"))]
    n_u_cn <- length(u_cn)
    
    for(ith_col in 1:n_u_cn){
      pos   <- which(colnames(ref_data)==u_cn[ith_col])
      pos_F <- which(colnames(ref_data)==paste(u_cn[ith_col],"_F",sep=""))
      
      id_set_to_NA <- which(ref_data[,pos]== -999
                            | is.na(ref_data[,pos])
                            | is.na(ref_data[,pos_F])
                            | ref_data[,pos_F]==0)
      
      if(length(id_set_to_NA)>0){
        ref_data[id_set_to_NA,pos] <- NA
      }
    }
    
    id_set_to_NA <- which(ref_data$CO2_DRY == -999)
    if(length(id_set_to_NA)>0){
      ref_data$CO2_DRY[id_set_to_NA] <- NA
    }
    
    
    
    ## -------------------------------
    
    
    # Merge sensor and reference data into data.frame "data"
    
    sensor_data$date <- as.POSIXct(sensor_data$date)
    ref_data$date    <- as.POSIXct(ref_data$date)
    
    data             <- merge(sensor_data,ref_data)
    
    data             <- data[which(data$date>strptime("20170725000000","%Y%m%d%H%M%S",tz="UTC")),]
    
    # data             <- data[which(data$date>=strptime("20180320170000","%Y%m%d%H%M%S",tz="UTC")
    #                                & data$CalMode==1),]
    
    ## -------------------------------
    
    # Apply corrections to CO2 reference value
    
    data$CO2 <- data$CO2_DRY * (1 - data$H2O/100)
    
    data$CO2_ppm <- data$CO2
    
    # pressure
    
    data$CO2 <- data$CO2 * (data$hpp_pressure / 1013.25)
    
    # water
    
    if(F){
      
      id_H2O_ok <- which(data$H2O != -999 & data$H2O_F!=1)
      
      if(length(id_H2O_ok)>0){
        data$CO2[id_H2O_ok] <- data$CO2[id_H2O_ok] * (1 + 0.01*data$H2O[id_H2O_ok])
      }
      
      rm(id_H2O_ok)
      gc()
    }
    
    
    data$TFac <- (273.15+65)/(273.15+data$hpp_temperature_mcu)
    
    
    ## -------------------------------
    
    # Additional columns in data.frame "data"
    
    # HPP
    
    # data$hpp_ir    <- data$hpp_lpl
    # data$hpp_ir    <- data$hpp_ir_high - data$hpp_ir_low
    
    data$hpp_logIR <- -log(data$hpp_ir)
    
    # GAM
    
    data$div_hpp_ir  <- 1/data$hpp_ir
    data$p_div_ir    <- (((data$hpp_pressure-1013.25)/1013.25)/data$hpp_ir)
    data$p2_div_ir   <- (((data$hpp_pressure-1013.25)/1013.25)^2/data$hpp_ir)
    
    # timestamp (continuous time)
    data$timestamp <- as.numeric(difftime(time1=data$date,strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
    
    # days (continuous time)
    data$days      <- as.numeric(difftime(time1=data$date,strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="days"))
    
    # hour of day
    data$hour      <- as.numeric(strftime(data$date,"%H",tz="UTC"))
    
    # minutes of day
    data$minutes   <- as.numeric(strftime(data$date,"%M",tz="UTC"))
    
    # DELTA pressure chamber
    
    data$deltaPC01 <- as.numeric(data$CalMode==3)
    data$deltaPC02 <- as.numeric((data$date>=strptime("20171205000000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20171207000000","%Y%m%d%H%M%S",tz="UTC")) | (data$date>=strptime("20171210000000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20171212000000","%Y%m%d%H%M%S",tz="UTC")))
    
    
    # abs humidity (sensor, reference) / H2O
    # [W. Wagner and A. Pruß: The IAPWS Formulation 1995 for the Thermodynamic Properties of Ordinary Water Substance for General and Scientific Use, Journal of Physical and Chemical Reference Data, June 2002 ,Volume 31, Issue 2, pp. 387535]
    
    coef_1 <-  -7.85951783
    coef_2 <-   1.84408259
    coef_3 <-  -11.7866497
    coef_4 <-   22.6807411
    coef_5 <-  -15.9618719
    coef_6 <-   1.80122502
    
    theta     <- 1 - (273.15+data$T)/647.096
    Pws       <- 220640 * exp( 647.096/(273.15+data$T) * (coef_1*theta + coef_2*theta^1.5 + coef_3*theta^3 + coef_4*theta^3.5 + coef_5*theta^4 + coef_6*theta^7.5))
    Pw        <- data$RH*(Pws*100)/100
    data$AH   <- 2.16679 * Pw/(273.15+data$T)
    data$AH_F <- 1
    
    id_set_to_NA   <- which(data$T_F * data$RH_F==0 | data$T_F == -999 | data$RH_F == -999)
    
    if(length(id_set_to_NA)>0){
      data$AH[id_set_to_NA]   <- NA
      data$AH_F[id_set_to_NA] <- 0
    }
    
    data$H2O_COMP   <- Pw / (data$pressure*1e2)*1e2
    
    # Abs Humidity from H2O Picarro
    
    data$AH_COMP    <- 2.16679 * (data$H2O*data$pressure*1e2 / (data$H2O + 100)) / (data$T+273.15)
    
    data$AH_COMP_dT <- data$AH_COMP * (65-data$T)
    
    
    # RH_65
    
    theta_65        <- 1 - (273.15+65)/647.096
    Pws_65          <- 220640 * exp( 647.096/(273.15+65) * (coef_1*theta_65 + coef_2*theta_65^1.5 + coef_3*theta_65^3 + coef_4*theta_65^3.5 + coef_5*theta_65^4 + coef_6*theta_65^7.5))
    data$RH_65      <- data$AH      / 2.16679 * (273.15+65) / Pws_65
    data$RH_65_COMP <- data$AH_COMP / 2.16679 * (273.15+65) / Pws_65
    
    rm(coef_1,coef_2,coef_3,coef_4,coef_4,coef_5,coef_6,theta,Pws,Pw,id_set_to_NA)
    gc()
    
    # Dew point (Magnus formula)
    
    K2 <-  17.62
    K3 <- 243.12
    
    data$DP              <- K3 * ( ( (K2*data$T)      /(K3 + data$T)       + log(data$RH/1e2) )      /( (K2*K3)/(K3+data$T)       - log(data$RH/1e2) ) )
    data$diff_T_DP       <- data$T - data$DP
    
    
    # Difference between HPP and REF pressure
    
    data$pressure_diff <- data$hpp_pressure - data$pressure
    
    
    # # Temperature difference 10 min
    # 
    # data$diff_10Min_T                   <- NA
    # data$diff_10Min_hpp_temperature_mcu <- NA
    # 
    # for(ith_row in 16:dim(data)[1]){
    #   
    #   id_n <- (ith_row-15):ith_row
    #   id   <- which(data$days[id_n]>=(data$days[ith_row]-15/1440) & data$days[id_n]<=(data$days[ith_row]-10/1440) 
    #                 & !is.na(data$T[id_n]) & !is.na(data$hpp_temperature_mcu[id_n])
    #                 & !is.na(data$T[ith_row]) & !is.na(data$hpp_temperature_mcu[ith_row]))
    #   n_id <- length(id)
    #   
    #   if(n_id>=1){
    #     
    #     cc <- max(id)
    #     
    #     data$diff_10Min_T[ith_row]                   <- data$T[ith_row]                   - data$T[id_n[id[cc]]]
    #     data$diff_10Min_hpp_temperature_mcu[ith_row] <- data$hpp_temperature_mcu[ith_row] - data$hpp_temperature_mcu[id_n[id[cc]]]
    #   }
    # }
    # 
    # # Temperatur slope
    # 
    # data$slope_T                     <- NA
    # data$slope_hpp_temperature_mcu   <- NA
    # 
    # for(ith_row in 5:dim(data)[1]){
    #   id_n <- (ith_row-4):ith_row
    #   id   <- which(data$days[ith_row]-data$days[id_n]<=0.003472222 
    #                 & !is.na(data$T[id_n]) & !is.na(data$hpp_temperature_mcu[id_n])
    #                 & !is.na(data$T[ith_row]) & !is.na(data$hpp_temperature_mcu[ith_row]))
    #   n_id <- length(id)
    #   
    #   if(n_id>=3){
    #     id <- id_n[id]
    #     
    #     tmp_T                   <- lm.fit(x=matrix(c(rep(1,n_id),data$days[id]),ncol=2),y=data$T[id])
    #     tmp_hpp_temperature_mcu <- lm.fit(x=matrix(c(rep(1,n_id),data$days[id]),ncol=2),y=data$hpp_temperature_mcu[id])
    #     
    #     data$slope_T[ith_row]                   <- tmp_T$coefficients[2]/24/60
    #     data$slope_hpp_temperature_mcu[ith_row] <- tmp_hpp_temperature_mcu$coefficients[2]/24/60
    #   }
    # }
    
    # Temperature delta
    
    data$hpp_temperature_mcu_dT <- c(NA,diff(data$hpp_temperature_mcu))
    
    # # AH slope
    # 
    # data$slope_AH <- NA
    # 
    # for(ith_row in 5:dim(data)[1]){
    #   id_n <- (ith_row-4):ith_row
    #   id   <- which(data$days[ith_row]-data$days[id_n]<=0.003472222 
    #                 & !is.na(data$AH[id_n])
    #                 & !is.na(data$AH[ith_row]))
    #   n_id <- length(id)
    #   
    #   if(n_id>=3){
    #     id <- id_n[id]
    #     
    #     tmp_AH                 <- lm.fit(x=matrix(c(rep(1,n_id),data$days[id]),ncol=2),y=data$T[id])
    #     data$slope_AH[ith_row] <- tmp_AH$coefficients[2]/24/60
    #   }
    # }
    
    # TEST col
    
    data$TU_T    <- 65 - data$T
    data$TU_HPPT <- 65 - data$hpp_temperature_mcu
    data$HPPT_T  <- data$hpp_temperature_mcu - data$T
    
    data$TU_d_T    <- (273.15+65)/(273.15+data$T)
    data$TU_d_HPPT <- (273.15+65)/(273.15+data$hpp_temperature_mcu)
    data$HPPT_d_T  <- (273.15+data$hpp_temperature_mcu)/(273.15+data$T)
    
    
    
    ## Sensor measurement time-series
    
    sensor_descriptor <- paste("SU", SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],"_S",sensors2cal[ith_sensor2cal],sep="")
    
    if(T){
      
      # T / dew point
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_T_DEWPOINT_TS.pdf",sep="")
      
      yyy     <- cbind(data$T,
                       data$hpp_temperature_mcu,
                       data$DP)
      
      id_setToNA <- which(data$CalMode!=1 | is.na(data$CalMode))
      
      yyy[id_setToNA,1] <- NA
      yyy[id_setToNA,2] <- NA
      yyy[id_setToNA,3] <- NA
      
      xlabString <- "Date" 
      ylabString <- expression(paste("T [deg C]"))
      legend_str <- c("T REF","T HPP","DP REF")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-15,55),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_T_DEWPOINT_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(-15,55),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_T_DEWPOINT_TS_WEEKS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"week",NULL,c(-15,55),xlabString,ylabString,legend_str)
      
      # T HPP - T REF 
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_diff_Thpp_Tref_TS.pdf",sep="")
      
      yyy     <- cbind(data$hpp_temperature_mcu - data$T)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("T [deg C]"))
      legend_str <- c("T HPP - T REF")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,NULL,xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_diff_Thpp_Tref_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,NULL,xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_diff_Thpp_Tref_TS_WEEKS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"week",NULL,NULL,xlabString,ylabString,legend_str)
      
      # hpp_temperature_detector
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_hpp_temperature_detector.pdf",sep="")
      
      yyy     <- cbind(data$hpp_temperature_detector)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("T detector[deg C]"))
      legend_str <- c("T detector")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,NULL,xlabString,ylabString,legend_str)
      
      
      # # slope T
      # 
      # figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_slope_T_10min_TS.pdf",sep="")
      # 
      # yyy     <- cbind(data$slope_T,
      #                  data$slope_hpp_temperature_mcu)
      # 
      # xlabString <- "Date" 
      # ylabString <- expression(paste("dT/Min [deg C]"))
      # legend_str <- c("dT REF","dT HPP")
      # plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-0.5,0.5),xlabString,ylabString,legend_str)
      # 
      # figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_slope_T_10min_TS_DAYS.pdf",sep="")
      # plot_ts(figname,data$date,yyy,"day",NULL,c(-0.5,0.5),xlabString,ylabString,legend_str)
      
      
      # # diff T 10 min
      # 
      # figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_diff_T_10min_TS.pdf",sep="")
      # 
      # yyy     <- cbind(data$diff_10Min_T,
      #                  data$diff_10Min_hpp_temperature_mcu)
      # 
      # xlabString <- "Date" 
      # ylabString <- expression(paste("diff T [deg C]"))
      # legend_str <- c("diff T REF","diff T HPP")
      # plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-4,4),xlabString,ylabString,legend_str)
      # 
      # figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_diff_T_10min_TS_DAYS.pdf",sep="")
      # plot_ts(figname,data$date,yyy,"day",NULL,c(-4,4),xlabString,ylabString,legend_str)
      
      
      # RH
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_RH_TS.pdf",sep="")
      
      yyy     <- cbind(data$RH)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("RH [%]"))
      legend_str <- c("RH REF")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(0,110),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_RH_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(0,110),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_RH_TS_WEEKS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"week",NULL,c(0,110),xlabString,ylabString,legend_str)
      
      # AH
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_AH_TS.pdf",sep="")
      
      yyy     <- cbind(data$AH,
                       data$AH_COMP)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("Absolute humidity [g/m^3]"))
      legend_str <- c("AH REF","AH PIC")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(0,20),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_AH_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(0,20),xlabString,ylabString,legend_str)
      
      # H2O Vol-%
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_TS.pdf",sep="")
      
      yyy     <- cbind(data$H2O,
                       data$H2O_COMP)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("H2O [Vol-%]"))
      legend_str <- c("H2O PIC","H2O COMP")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(0,5),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(0,5),xlabString,ylabString,legend_str)
      
      # pressure
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_pressure_TS.pdf",sep="")
      
      yyy     <- cbind(data$pressure,
                       data$hpp_pressure)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("Pressure [hPa]"))
      legend_str <- c("Pressure REF", "Pressure HPP")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(780,1025),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_pressure_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(780,1025),xlabString,ylabString,legend_str)
      
      # pressure difference
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_pressure_diff_TS.pdf",sep="")
      
      yyy     <- cbind(data$hpp_pressure - data$pressure)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("Pressure difference [hPa]"))
      legend_str <- c("HPP-REF")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-5,5),xlabString,ylabString,legend_str)
      
      # rain
      
      # figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_rain_TS.pdf",sep="")
      # 
      # yyy     <- cbind(data$rain)
      # 
      # xlabString <- "Date" 
      # ylabString <- expression(paste("Rain [mm]"))
      # legend_str <- c("Rain")
      # plot_ts(figname,data$date,yyy,"all_day2day",NULL,NULL,xlabString,ylabString,legend_str)
      # 
      # figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_rain_TS_DAYS.pdf",sep="")
      # plot_ts(figname,data$date,yyy,"day",NULL,NULL,xlabString,ylabString,legend_str)
      
      
      # STRGLO
      
      # figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_STRGLO_TS.pdf",sep="")
      # 
      # yyy     <- cbind(data$STRGLO)
      # 
      # xlabString <- "Date" 
      # ylabString <- expression(paste("Radiation [X]"))
      # legend_str <- c("Radiation")
      # plot_ts(figname,data$date,yyy,"day",NULL,NULL,xlabString,ylabString,legend_str)
      
      
      # TEST
      
      # figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_TEST_TS.pdf",sep="")
      # 
      # yyy     <- cbind(data$TEST)
      # 
      # xlabString <- "Date" 
      # ylabString <- expression(paste("TEST [XXX]"))
      # legend_str <- c("TEST")
      # plot_ts(figname,data$date,yyy,"day",NULL,NULL,xlabString,ylabString,legend_str)
      
    }
    
    
    ## Comparison of selected parameters
    
    if(T){
      
      comparison_info <- NULL
      comparison_info <- rbind(comparison_info,data.frame(factor_1="CO2",      unit_1="[ppm]",   factor_2="hpp_ir",             unit_2="[XX]"  ,one2one=F))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="CO2",      unit_1="[ppm]",   factor_2="hpp_logIR",          unit_2="[XX]"  ,one2one=F))
      
      
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="CO2",      unit_1="[ppm]",   factor_2="hpp_co2",             unit_2="[ppm]"  ,one2one=T))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="T",        unit_1="[deg C]", factor_2="hpp_temperature_mcu", unit_2="[deg C]",one2one=T))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="pressure", unit_1="[hPa]",   factor_2="hpp_pressure",        unit_2="[hPa]",  one2one=T))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="T",        unit_1="[deg C]", factor_2="RH",                  unit_2="[%]",    one2one=F))
      # 
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="CO2",                unit_1="[ppm]",   factor_2="AH", unit_2="[g/m3]", one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="T",                  unit_1="[deg C]", factor_2="AH", unit_2="[g/m3]", one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="hpp_temperature_mcu",unit_1="[deg C]", factor_2="AH", unit_2="[g/m3]", one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="days",               unit_1="[d]",     factor_2="AH", unit_2="[g/m3]", one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="RH",                 unit_1="[%]",     factor_2="AH", unit_2="[g/m3]", one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="H2O",                unit_1="[Vol-%]", factor_2="AH", unit_2="[g/m3]", one2one=F))
      # 
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="H2O",                unit_1="[Vol-%]", factor_2="H2O_COMP", unit_2="[Vol-%]", one2one=T))
      # 
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="hpp_lpl",            unit_1="[-]",     factor_2="hpp_ir",               unit_2="[-]", one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="hpp_lpl",            unit_1="[-]",     factor_2="hpp_ir_high_low_mean", unit_2="[-]", one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="hpp_lpl",            unit_1="[-]",     factor_2="hpp_ir_high_low_diff", unit_2="[-]", one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="hpp_lpl",            unit_1="[-]",     factor_2="hpp_sclraw",           unit_2="[-]", one2one=F))
      
      
      date_str         <- "Data: "
      for(ii in 1:n_id_cal_periods){
        if(ii>1){
          date_str <- paste(date_str, paste("      "),sep="")
        }
        
        date_str <- paste(date_str, paste(strftime(sensor_calibration_info$Date_UTC_from[id_cal_periods[ii]],"%d/%m/%y",tz="UTC"),"-",strftime(sensor_calibration_info$Date_UTC_to[id_cal_periods[ii]],"%d/%m/%y",tz="UTC")),sep="")
        
        if(ii<n_id_cal_periods){
          date_str <- paste(date_str,"\n",sep="")
        }
      }
      
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_COMPARISON_RAW_DATA.pdf",sep="")
      
      
      def_par <- par()
      pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
      par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
      
      for(ith_cmp in 1:dim(comparison_info)[1]){
        
        
        pos_factor_1 <- which(colnames(data)==comparison_info$factor_1[ith_cmp])
        pos_factor_2 <- which(colnames(data)==comparison_info$factor_2[ith_cmp])
        
        id_ok       <- which(!is.na(data[,pos_factor_1]) & !is.na(data[,pos_factor_2]))
        n_id_ok     <- length(id_ok)
        
        if(comparison_info$one2one[ith_cmp]){
          xrange <- range(c(data[id_ok,pos_factor_1],data[id_ok,pos_factor_2]))
          yrange <- xrange
        }else{
          xrange <- range(data[id_ok,pos_factor_1])
          yrange <- range(data[id_ok,pos_factor_2])
        }
        
        fit_cmp     <- lm(y~x,data.frame(x=data[id_ok,pos_factor_1],y=data[id_ok,pos_factor_2]))
        corCoef     <- cor(x=data[id_ok,pos_factor_1],y=data[id_ok,pos_factor_2],use="complete.obs",method="pearson")
        
        xlabString  <- paste(comparison_info$factor_1[ith_cmp],comparison_info$unit_1[ith_cmp])
        ylabString  <- paste(comparison_info$factor_2[ith_cmp],comparison_info$unit_2[ith_cmp])
        mainString  <- paste(sensor_descriptor,"[",paste("y = ",sprintf("%.7f",fit_cmp$coefficients[2]),"x + ",sprintf("%.3f",fit_cmp$coefficients[1]),sep=""),"/",paste("COR = ",sprintf("%.2f",corCoef),"]",sep="")) 
        
        plot(  data[id_ok,pos_factor_1], data[id_ok,pos_factor_2] ,xlim=xrange,ylim=yrange,xlab=xlabString,ylab=ylabString,main=mainString,pch=16,cex=0.5,cex.axis=1.5,cex.lab=1.5,col=1)
        if(comparison_info$one2one[ith_cmp]){
          lines(c(-1e4,1e4),c(-1e4,1e4),lwd=1,lty=1,col=1)
        }
        lines(c(-1e6,1e6),c(fit_cmp$coefficients[1]-1e6*fit_cmp$coefficients[2],fit_cmp$coefficients[1]+1e6*fit_cmp$coefficients[2]),col=2,lwd=1,lty=5)
        
        
        par(family="mono")
        text(xrange[1]+0.8*(xrange[2]-xrange[1]),yrange[1]+0.10*(yrange[2]-yrange[1]),date_str)
        par(family="")
      }
      
      par(def_par)
      dev.off()
      
    }
    
    
    ## Sensor calibration
    
    if(CV_mode==0){
      n_temp_data_selections <- 0
    }
    if(CV_mode==1){
      n_temp_data_selections <- 0
    }
    if(CV_mode==2){
      n_temp_data_selections <- 3
    }
    if(CV_mode==3){
      n_temp_data_selections <- 1
    }
    if(CV_mode==4){
      n_temp_data_selections <- 2
    }
    if(CV_mode==5){
      n_temp_data_selections <- 1
    }
    if(CV_mode==6){
      n_temp_data_selections <- 2
    }
    if(CV_mode==7){
      n_temp_data_selections <- 2
    }
    
    # Loop over temporal data selections (CV)
    
    for(ith_temp_data_selections in 0:n_temp_data_selections){
      
      CV_P <- sprintf("%02.0f",ith_temp_data_selections)
      
      if(CV_mode==0 | ith_temp_data_selections==0){
        data_training        <- rep(T,dim(data)[1])
        data_test            <- rep(T,dim(data)[1])
      }
      if(CV_mode==1){
        data_training        <- data$CalMode == 1 
        data_test            <- data$CalMode == 1
      }
      if(CV_mode==3 & ith_temp_data_selections>0){
        if(ith_temp_data_selections==1){
          data_training        <- data$CalMode %in% c(2,3) 
          data_test            <- data$CalMode == 1
        }
      }
      if(CV_mode==4 & ith_temp_data_selections>0){
        # FIT: CC/PC + 1 week + [BCP data] 
        if(ith_temp_data_selections==1){
          data_training        <- data$CalMode %in% c(2,3) | (data$date>strptime("20170726000000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20170808000000","%Y%m%d%H%M%S",tz="UTC"))
          data_test            <- data$CalMode == 1        & (data$date<strptime("20170726000000","%Y%m%d%H%M%S",tz="UTC") | data$date>strptime("20170808000000","%Y%m%d%H%M%S",tz="UTC"))
        }
        # FIT: CC/PC + 1 week 
        if(ith_temp_data_selections==2){
          data_training        <- data$CalMode %in% c(2,3) | (data$date>strptime("20170726000000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20170808000000","%Y%m%d%H%M%S",tz="UTC"))
          data_test            <- data$CalMode %in% 1      & (data$date<strptime("20170726000000","%Y%m%d%H%M%S",tz="UTC") | data$date>strptime("20170808000000","%Y%m%d%H%M%S",tz="UTC"))
        } 
      }
      if(CV_mode==5 & ith_temp_data_selections>0){
        # FIT: CC/PC + 1 week + [BCP data] 
        if(ith_temp_data_selections==1){
          data_training        <- data$CalMode == 2  
          data_test            <- data$CalMode == 1
        }
        
      }
      if(CV_mode==6 & ith_temp_data_selections>=0){
        # FIT: CC + [BCP data]
        if(ith_temp_data_selections==0){
          data_training        <- data$CalMode %in%c(1,2)
          data_test            <- data$CalMode %in%c(1,2)
        }
        if(ith_temp_data_selections==1){
          data_training        <- data$CalMode == 2
          data_test            <- data$CalMode == 1
        }
        # FIT: CC
        if(ith_temp_data_selections==2){
          data_training        <- data$CalMode == 2 
          data_test            <- data$CalMode == 1 
        } 
      }
      
      
      if(CV_mode==7){
        
        if(ith_temp_data_selections<2){
          next
        }else{
          # data_training        <- (data$CalMode %in% c(2,3) & !(data$date>strptime("20170818080000","%Y%m%d%H%M%S",tz="UTC")&data$date<strptime("20170818123000","%Y%m%d%H%M%S",tz="UTC"))) | (data$date>=strptime("20171205000000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20171207000000","%Y%m%d%H%M%S",tz="UTC")) | (data$date>=strptime("20171210000000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20171212000000","%Y%m%d%H%M%S",tz="UTC"))
          # data_test            <- data$CalMode == 1 & !((data$date>=strptime("20171205000000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20171207000000","%Y%m%d%H%M%S",tz="UTC")) | (data$date>=strptime("20171210000000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20171212000000","%Y%m%d%H%M%S",tz="UTC")))
          
          data_training        <- (data$CalMode %in% c(2,3) & !(data$date>strptime("20170818080000","%Y%m%d%H%M%S",tz="UTC")&data$date<strptime("20170818123000","%Y%m%d%H%M%S",tz="UTC"))) | (data$date>=strptime("20171205000000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20171212000000","%Y%m%d%H%M%S",tz="UTC"))
          data_test            <- data$CalMode == 1 & !((data$date>=strptime("20171205000000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20171212000000","%Y%m%d%H%M%S",tz="UTC")))
        }
      }
      
      
      
      
      # -------
      
      for(ith_sensor_model in 1:length(sensor_models)){
        
        if(CV_mode%in%c(5,6) & length(grep(pattern="_DRIFT",x=sensor_models[[ith_sensor_model]]$formula))>0){
          next
        }
        
        print(sensor_models[[ith_sensor_model]]$name)
        
        ## check measurements
        
        measurement_ok <- rep(T,dim(data)[1])
        
        ## nulling_periods
        
        nullingPeriod <- (data$date<strptime("20170819000000","%Y%m%d%H%M%S",tz="UTC") | data$date>strptime("20170824000000","%Y%m%d%H%M%S",tz="UTC")) & (data$date<strptime("20171002000000","%Y%m%d%H%M%S",tz="UTC") | data$date>strptime("20171010000000","%Y%m%d%H%M%S",tz="UTC")) & !is.na(data$hpp_co2) & data$hpp_co2<=25 & !is.na(data$pressure) & !is.na(data$hpp_pressure)
        
        if(sensor_models[[ith_sensor_model]]$modelType=="LM_IR"){
          nullingPeriod <- nullingPeriod & !is.na(data$hpp_ir)
        }
        
        
        id_nullingPeriod   <- which(nullingPeriod==T)
        n_id_nullingPeriod <- length(id_nullingPeriod)
        
        #
        
        if(F){
          data$CO2[id_nullingPeriod]        <- 0
          data$AH[id_nullingPeriod]         <- 0
          data$AH_COMP[id_nullingPeriod]    <- 0
          data$AH_COMP_dt[id_nullingPeriod] <- 0
          data$H2O[id_nullingPeriod]        <- 0
        }
        
        if(T){
          if(n_id_nullingPeriod>0){
            
            id_nullingPeriods_starts <- which(c(F,diff(as.numeric(nullingPeriod)) ==  1))
            
            # nullingPeriods without start within the data analysis period shall not be considered
            id_nok <- which(data$timestamp < data$timestamp[id_nullingPeriods_starts[1]] & nullingPeriod)
            if(length(id_nok)>0){
              nullingPeriod[id_nok]  <- F
              measurement_ok[id_nok] <- F
            }
            
            for(NPS in id_nullingPeriods_starts){
              
              id_ith_nullingPeriod <- NPS:(NPS+60)
              id_ith_nullingPeriod <- id_ith_nullingPeriod[which(id_ith_nullingPeriod<=dim(data)[1])]
              id_ith_nullingPeriod <- id_ith_nullingPeriod[which(nullingPeriod[id_ith_nullingPeriod])]
              
              # nullingPeriods with less than 20 measurements shall not be considered
              if(length(id_ith_nullingPeriod) < 20){
                nullingPeriod[id_ith_nullingPeriod]   <- F
                measurement_ok[id_ith_nullingPeriod]  <- F
                
                data$CO2[id_ith_nullingPeriod]        <- 0
                data$AH[id_ith_nullingPeriod]         <- 0
                data$AH_COMP[id_ith_nullingPeriod]    <- 0
                data$AH_COMP_dT[id_ith_nullingPeriod] <- 0
                data$H2O[id_ith_nullingPeriod]        <- 0
                
              }else{
                # remove first 5 minutes of nullingPeriods
                id_nok <- id_ith_nullingPeriod[which((data$timestamp[id_ith_nullingPeriod] - data$timestamp[NPS]) < 300)]
                if(length(id_nok)>0){
                  nullingPeriod[id_nok]   <- F
                  measurement_ok[id_nok]  <- F
                  
                  data$CO2[id_nok]        <- 0
                  data$AH[id_nok]         <- 0
                  data$AH_COMP[id_nok]    <- 0
                  data$AH_COMP_dT[id_nok] <- 0
                  data$H2O[id_nok]        <- 0
                }
                
                # set refererence CO2 to zero
                id_ok  <- id_ith_nullingPeriod[which(data$timestamp[id_ith_nullingPeriod] - data$timestamp[NPS] >= 300)]
                if(length(id_ok)>0){
                  data$CO2[id_ok]        <- 0
                  data$AH[id_ok]         <- 0
                  data$AH_COMP[id_ok]    <- 0
                  data$AH_COMP_dT[id_ok] <- 0
                  data$H2O[id_ok]        <- 0
                }
              }
              
              # remove 5 minutes before/after nulling period
              id_nok <- which((min(data$timestamp[id_ith_nullingPeriod]) - data$timestamp)    < 300 
                              & (min(data$timestamp[id_ith_nullingPeriod]) - data$timestamp)  > 0)
              
              if(length(id_nok)>0){
                nullingPeriod[id_nok]   <- F
                measurement_ok[id_nok]  <- F
              }
              
              id_nok <- which((data$timestamp - max(data$timestamp[id_ith_nullingPeriod]))    < 300 
                              & (data$timestamp - max(data$timestamp[id_ith_nullingPeriod]))  > 0)
              
              if(length(id_nok)>0){
                nullingPeriod[id_nok]   <- F
                measurement_ok[id_nok]  <- F
              }
              
            }
          }
          
          #
          
          id_nullingPeriod   <- which(nullingPeriod==T)
          n_id_nullingPeriod <- length(id_nullingPeriod)
          
          #
          
          rm(id_ith_nullingPeriod,id_ok,id_nok,id_nullingPeriods_starts)
          gc()
        }
        
        
        ## bottle_cal_periods 
        
        # Bottle cal periods : test stand, reference gas
        
        bottle_cal_periods <- rep(F,dim(data)[1])
        
        for(ii in 1:500){
          
          if(ii==0){
            tmp_ok  <- data$date> strptime("20170911153000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20170911162000","%Y%m%d%H%M%S",tz="UTC") & data$CalMode==1  & data$CO2_ppm>200 & data$CO2_ppm<1000 & data$hpp_co2>200 & !is.na(data$CO2) & !is.na(data$hpp_co2) & !is.na(data$pressure) & !is.na(data$hpp_pressure) & !is.na(data$hpp_ir) & !nullingPeriod 
            tmp_nok <- data$date>=strptime("20170911162000","%Y%m%d%H%M%S",tz="UTC") & data$date<strptime("20170911162300","%Y%m%d%H%M%S",tz="UTC")
          }
          if(ii==1){
            tmp_nok <-  data$date>=strptime("20170915040000","%Y%m%d%H%M%S",tz="UTC") & data$date<=strptime("20170915040200","%Y%m%d%H%M%S",tz="UTC")
            tmp_ok  <-  data$date> strptime("20170915040200","%Y%m%d%H%M%S",tz="UTC") & data$date< strptime("20170915043000","%Y%m%d%H%M%S",tz="UTC") & data$CalMode==1  & data$CO2_ppm>200 & data$CO2_ppm<1000 & data$hpp_co2>200 & !is.na(data$CO2) & !is.na(data$hpp_co2) & !is.na(data$pressure) & !is.na(data$hpp_pressure) & !is.na(data$hpp_ir) & !nullingPeriod
            tmp_nok <- (data$date>=strptime("20170915043000","%Y%m%d%H%M%S",tz="UTC") & data$date<=strptime("20170915043300","%Y%m%d%H%M%S",tz="UTC")) | tmp_nok
          }
          if(ii==2){
            tmp_nok <-  data$date>=strptime("20170919040000","%Y%m%d%H%M%S",tz="UTC") & data$date<=strptime("20170919040200","%Y%m%d%H%M%S",tz="UTC")
            tmp_ok  <-  data$date> strptime("20170919040200","%Y%m%d%H%M%S",tz="UTC") & data$date< strptime("20170919043000","%Y%m%d%H%M%S",tz="UTC") & data$CalMode==1  & data$CO2_ppm>200 & data$CO2_ppm<1000 & data$hpp_co2>200 & !is.na(data$CO2) & !is.na(data$hpp_co2) & !is.na(data$pressure) & !is.na(data$hpp_pressure) & !is.na(data$hpp_ir) & !nullingPeriod
            tmp_nok <- (data$date>=strptime("20170919043000","%Y%m%d%H%M%S",tz="UTC") & data$date<=strptime("20170919043300","%Y%m%d%H%M%S",tz="UTC")) | tmp_nok
          }
          if(ii==3){
            tmp_nok <-  data$date>=strptime("20170923040000","%Y%m%d%H%M%S",tz="UTC") & data$date<=strptime("20170923040200","%Y%m%d%H%M%S",tz="UTC")
            tmp_ok  <-  data$date> strptime("20170923040200","%Y%m%d%H%M%S",tz="UTC") & data$date< strptime("20170923043000","%Y%m%d%H%M%S",tz="UTC") & data$CalMode==1  & data$CO2_ppm>200 & data$CO2_ppm<1000 & data$hpp_co2>200 & !is.na(data$CO2) & !is.na(data$hpp_co2) & !is.na(data$pressure) & !is.na(data$hpp_pressure) & !is.na(data$hpp_ir) & !nullingPeriod
            tmp_nok <- (data$date>=strptime("20170923043000","%Y%m%d%H%M%S",tz="UTC") & data$date<=strptime("20170923043300","%Y%m%d%H%M%S",tz="UTC")) | tmp_nok
          }
          if(ii==4){
            tmp_nok <-  data$date>=strptime("20170927040000","%Y%m%d%H%M%S",tz="UTC") & data$date<=strptime("20170927040200","%Y%m%d%H%M%S",tz="UTC")
            tmp_ok  <-  data$date> strptime("20170927040200","%Y%m%d%H%M%S",tz="UTC") & data$date< strptime("20170927043000","%Y%m%d%H%M%S",tz="UTC") & data$CalMode==1  & data$CO2_ppm>200 & data$CO2_ppm<1000 & data$hpp_co2>200 & !is.na(data$CO2) & !is.na(data$hpp_co2) & !is.na(data$pressure) & !is.na(data$hpp_pressure) & !is.na(data$hpp_ir) & !nullingPeriod
            tmp_nok <- (data$date>=strptime("20170927043000","%Y%m%d%H%M%S",tz="UTC") & data$date<=strptime("20170927043300","%Y%m%d%H%M%S",tz="UTC")) | tmp_nok
          }
          if(ii>=5 & ii<=34){
            date_BCP_from <- strptime("20171001040000","%Y%m%d%H%M%S",tz="UTC") + (ii-5)*4*86400
            
            tmp_nok <-  data$date>=(date_BCP_from)     & data$date<=(date_BCP_from+ 120)
            tmp_ok  <-  data$date> (date_BCP_from+120) & data$date< (date_BCP_from+ 900) & data$CalMode==1  & data$CO2_ppm>200 & data$CO2_ppm<1000 & data$hpp_co2>200 & !is.na(data$CO2) & !is.na(data$hpp_co2) & !is.na(data$pressure) & !is.na(data$hpp_pressure) & !is.na(data$hpp_ir) & !nullingPeriod
            tmp_nok <- (data$date>=(date_BCP_from+900) & data$date<=(date_BCP_from+1080)) | tmp_nok
          }
          if(ii>=35 & ii<=500){
            
            date_BCP_from <- strptime("20180127040000","%Y%m%d%H%M%S",tz="UTC") + (ii-35)*2*86400
            
            if(ii%%2==0){
              tmp_nok <-  data$date>=(date_BCP_from)     & data$date<=(date_BCP_from+ 120)
              tmp_ok  <-  data$date> (date_BCP_from+120) & data$date< (date_BCP_from+ 900) & data$CalMode==1  & data$CO2_ppm>200 & data$CO2_ppm<1000 & data$hpp_co2>200 & !is.na(data$CO2) & !is.na(data$hpp_co2) & !is.na(data$pressure) & !is.na(data$hpp_pressure) & !is.na(data$hpp_ir) & !nullingPeriod
              tmp_nok <- (data$date>=(date_BCP_from+900) & data$date<=(date_BCP_from+1080)) | tmp_nok
            }else{
              tmp_ok  <- NULL
              tmp_nok <- (data$date>=(date_BCP_from)     & data$date<=(date_BCP_from+1080))
            }
            
          }
          
          if(!is.null(tmp_ok)){
            
            id_ok <- which(tmp_ok == T)
            
            if(length(id_ok)>0){
              id_id_ok <- which(data$date[id_ok]<=strptime("20180323090000","%Y%m%d%H%M%S",tz="UTC"))
              if(length(id_id_ok)>0){
                data$CO2[id_ok[id_id_ok]] <- 383.52 * data$hpp_pressure[id_ok[id_id_ok]]/1013.25
              }
              id_id_ok <- which(data$date[id_ok]>strptime("20180323090000","%Y%m%d%H%M%S",tz="UTC") & data$date[id_ok]<strptime("20181211133000","%Y%m%d%H%M%S",tz="UTC"))
              if(length(id_id_ok)>0){
                data$CO2[id_ok[id_id_ok]] <- 434.70 * data$hpp_pressure[id_ok[id_id_ok]]/1013.25
              }
              id_id_ok <- which(data$date[id_ok]>strptime("20181211133000","%Y%m%d%H%M%S",tz="UTC") & data$date[id_ok]<strptime("21000101000000","%Y%m%d%H%M%S",tz="UTC"))
              if(length(id_id_ok)>0){
                data$CO2[id_ok[id_id_ok]] <- 475.46 * data$hpp_pressure[id_ok[id_id_ok]]/1013.25
              }
              
              data$AH[id_ok]      <- 0
              data$AH_COMP[id_ok] <- 0
              data$H2O[id_ok]     <- 0
              
              bottle_cal_periods <- bottle_cal_periods | tmp_ok
            }
          }
          
          if(!is.null(tmp_nok)){
            
            id_nok <- which(tmp_nok == T)
            
            if(length(id_nok)>0){
              data$CO2[id_nok] <- NA
            }
          }
          
          rm(id_ok,id_id_ok,id_nok,tmp_ok,tmp_nok)
          gc()
        }
        
        # Bottle cal periods : ficticious, arbitrary time periods 04:00-04:30 (every 4th day) 
        if(CV_mode%in%c(4,6,7)){
          
          bottle_cal_periods <- bottle_cal_periods | (data$date > strptime("20170720000000","%Y%m%d%H%M%S",tz="UTC") & data$date < strptime("20170912000000","%Y%m%d%H%M%S",tz="UTC") & data$CalMode==1 & floor(data$days)%%4==0 & data$hour==4 & data$minutes<30 & data$CO2_ppm>200 & data$CO2_ppm<1000 & data$hpp_co2>300 & !is.na(data$CO2) & !is.na(data$hpp_co2) & !is.na(data$pressure) & !is.na(data$hpp_pressure) & !nullingPeriod)
          
          if(sensor_models[[ith_sensor_model]]$modelType=="LM_IR"){
            bottle_cal_periods <- bottle_cal_periods & !is.na(data$hpp_ir) 
          }
        }
        
        #
        
        id_bottle_cal_periods   <- which(bottle_cal_periods==T)
        n_id_bottle_cal_periods <- length(id_bottle_cal_periods)
        
        
        
        ## calibration data selection
        
        use4cal <- rep(T,dim(data)[1])
        
        use4cal <- use4cal &  !is.na(data$CO2)
        use4cal <- use4cal &  !is.na(data$pressure)
        use4cal <- use4cal &  !is.na(data$hpp_pressure)
        
        use4cal <- use4cal &  !is.na(data$hpp_co2)
        
        if(length(grep(pattern="AH",x=sensor_models[[ith_sensor_model]]$formula))>0){
          use4cal <- use4cal & !is.na(data$AH_COMP)
        }
        
        if(length(grep(pattern="AHdT",x=sensor_models[[ith_sensor_model]]$formula))>0){
          use4cal <- use4cal & !is.na(data$AH_COMP_dT)
        }
        
        if(length(grep(pattern="H2O",x=sensor_models[[ith_sensor_model]]$formula))>0){
          use4cal <- use4cal & !is.na(data$H2O)
        }
        
        if(length(grep(pattern="I\\(T\\)",x=sensor_models[[ith_sensor_model]]$formula))>0){
          use4cal <- use4cal & !is.na(data$T)
        }
        if(length(grep(pattern="HPPT_T",x=sensor_models[[ith_sensor_model]]$formula))>0){
          use4cal <- use4cal & !is.na(data$T)
        }
        if(length(grep(pattern="HPPTdT",x=sensor_models[[ith_sensor_model]]$name))>0){
          use4cal <- use4cal & !is.na(data$hpp_temperature_mcu_dT)
        }
        
        use4cal <- use4cal &  ( (data$CalMode ==1 & data$hpp_co2>150) | data$CalMode%in%c(2,3))
        
        use4cal <- use4cal &  measurement_ok
        
        if(sensor_models[[ith_sensor_model]]$modelType=="LM_IR"){
          use4cal <- use4cal & !is.na(data$hpp_ir)
        }
        
        id_use4cal    <- which(use4cal & data_training)
        n_id_use4cal  <- length(id_use4cal)
        
        id_use4pred   <- which(use4cal & data_test)
        n_id_use4pred <- length(id_use4pred)
        
        if(n_id_use4cal==0 | n_id_use4pred==0){
          next
        }
        
        ####
        
        # Use of nulling periods for parameter estimation
        
        if(F){
          id_use4cal                 <- sort(unique(c(id_use4cal,id_nullingPeriod)))
          n_id_use4cal               <- length(id_use4cal)
          
          id_use4pred                <- sort(unique(c(id_use4pred,id_nullingPeriod)))
          n_id_use4pred              <- length(id_use4pred)
          
        }else{
          id_use4cal                 <- id_use4cal[which(!id_use4cal%in%id_nullingPeriod)]
          n_id_use4cal               <- length(id_use4cal)
          
          id_use4pred                <- id_use4pred[which(!id_use4pred%in%id_nullingPeriod)]
          n_id_use4pred              <- length(id_use4pred)
        }
        
        ####
        
        if(CV_mode%in%c(4,6) & ith_temp_data_selections==1){
          id_use4cal                 <- sort(unique(c(id_use4cal,id_bottle_cal_periods)))
          n_id_use4cal               <- length(id_use4cal)
          
          id_use4pred                <- id_use4pred[which(!id_use4pred%in%id_bottle_cal_periods)]
          n_id_use4pred              <- length(id_use4pred)
          
        }
        
        ####
        
        
        plotdir <- paste(resultdir,sensor_models[[ith_sensor_model]]$name,sep="")
        if(!dir.exists(plotdir)){
          dir.create((plotdir))
        }
        
        ####
        
        if(sensor_models[[ith_sensor_model]]$modelType %in% c("GAM")){
          
          formula_str      <- sensor_models[[ith_sensor_model]]$formula
          fit              <- gam(formula = as.formula(formula_str),data = data[id_use4cal,])
          
          max_par          <- 14
          n_par            <- length(fit$coefficients)
          parameters       <- rep(NA,max_par)
          
          # model predictions
          
          tmp               <- predict.gam(object = fit,newdata = data,se.fit=T)
          
          CO2_predicted     <- tmp$fit
          CO2_predicted_se  <- tmp$se.fit
          
          CO2_predicted_ppm <- CO2_predicted * (1013.25/data$hpp_pressure)
          
          rm(tmp)
        }
        
        
        if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","LM_CO2")){
          
          formula_str      <- sensor_models[[ith_sensor_model]]$formula
          fit              <- rlm(as.formula(formula_str),data[id_use4cal,],psi=psi.huber,k=1.345)
          
          max_par          <- 14
          n_par            <- length(fit$coefficients)
          parameters       <- rep(NA,max_par)
          if(n_par>max_par){
            stop("Number of parameters exceeds maximum number of parameters.")
          }else{
            parameters[1:n_par] <- as.numeric(fit$coefficients)
          }
          
          # model predictions
          
          tmp               <- predict(fit,data,se.fit=T)
          
          CO2_predicted     <- tmp$fit
          CO2_predicted_se  <- tmp$se.fit
          
          CO2_predicted_ppm <- CO2_predicted * (1013.25/data$hpp_pressure)
          
          rm(tmp)
          
          if(F & sensor_models[[ith_sensor_model]]$name %in% c("HPP_CO2_IR","HPP_CO2_IR_CP1","HPP_CO2_IR_CP1_H2O_HPPT")){
            res_data_frame <- data.frame(timestamp    = data$timestamp,
                                         residual     = CO2_predicted-data$CO2,
                                         CalMode      = data$CalMode,
                                         CAL          = as.numeric((1:dim(data))%in%id_use4cal),
                                         PRED         = as.numeric((1:dim(data))%in%id_use4pred),
                                         hpp_ir       = data$hpp_ir,
                                         hpp_T        = data$hpp_temperature_mcu,
                                         hpp_pressure = data$hpp_pressure,
                                         AH           = data$AH,
                                         HPP_H2O      = data$H2O,
                                         stringsAsFactors = F)
            
            write.table(res_data_frame,paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_RESIDUALS.csv",sep=""),col.names =T,row.names=F,sep=";")
            
            rm(res_data_frame)
            gc()
          }
          
        }
        
        if(sensor_models[[ith_sensor_model]]$modelType=="SM_IR"){
          
          if(sensor_models[[ith_sensor_model]]$name%in%c("HPP_CO2_IR_NL_CP1_pIR2")){
            
            print(sensor_models[[ith_sensor_model]]$name)
            
            p_00 <- -16136.11451
            p_01 <- -1343.576828
            p_02 <-  75799019.21
            p_03 <-  0.1
            p_04 <-  0.1
            
            for(ith_it in 1:500){
              
              print(ith_it)
              
              A <- data.frame(1              / (  1 + p_03 * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * ((data$hpp_pressure-1013.25)/1013.25)^2),
                              data$hpp_logIR / (  1 + p_03 * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * ((data$hpp_pressure-1013.25)/1013.25)^2),
                              1/data$hpp_ir  / (  1 + p_03 * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * ((data$hpp_pressure-1013.25)/1013.25)^2),
                              -((data$hpp_pressure-1013.25)/1013.25)   * (p_00+p_01*data$hpp_logIR+p_02/data$hpp_ir)/(  1 + p_03 * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * ((data$hpp_pressure-1013.25)/1013.25)^2)^2,
                              -((data$hpp_pressure-1013.25)/1013.25)^2 * (p_00+p_01*data$hpp_logIR+p_02/data$hpp_ir)/(  1 + p_03 * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * ((data$hpp_pressure-1013.25)/1013.25)^2)^2,
                              stringsAsFactors = F)
              
              A_P     <- as.matrix(A)
              id_A_ok <- which(apply(!is.na(A_P) & !is.nan(A_P) & is.numeric(A_P) & is.finite(A_P),1,sum)==dim(A_P)[2])
              
              FF   <- data$CO2 - ((p_00 + p_01*data$hpp_logIR + p_02/data$hpp_ir) / (  1 + p_03 * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * ((data$hpp_pressure-1013.25)/1013.25)^2) )
              A    <- as.matrix(A[id_use4cal,])
              
              print(A[1:10,])
              
              FF   <- as.matrix(FF[id_use4cal],col=1)
              x    <- solve(t(A)%*%A)%*%t(A)%*%FF
              
              p_00 <- x[1] + p_00
              p_01 <- x[2] + p_01
              p_02 <- x[3] + p_02
              p_03 <- x[4] + p_03
              p_04 <- x[5] + p_04
              
              print(x)
              
            }
            
            max_par                <- 14
            n_par                  <- 5
            parameters             <- rep(NA,max_par)
            parameters[1:n_par]    <- c(x[1]+p_00,x[2]+p_01,x[3]+p_02,x[4]+p_03,x[5]+p_04)
            
            CO2_predicted          <- rep(NA,dim(data)[1])
            CO2_predicted_se       <- rep(NA,dim(data)[1])
            CO2_predicted[id_A_ok] <- ((p_00 + p_01*data$hpp_logIR + p_02/data$hpp_ir) / (  1 + p_03 * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * ((data$hpp_pressure-1013.25)/1013.25)^2) )[id_A_ok]
            CO2_predicted_ppm      <- CO2_predicted * (1013.25/data$hpp_pressure)
          }
          
          if(ith_temp_data_selections==0 & sensor_models[[ith_sensor_model]]$name%in%c("HPP_CO2_IR_NL_CP1_pIR2_TFac")){
            
            p_00 <- 0.00
            p_01 <- 1.00
            p_02 <- 0.00
            p_03 <- 0.00
            p_04 <- 0.00
            p_05 <- 1.00
            
            TFac <- (273.15+65)/(273.15+data$hpp_temperature_mcu)
            
            for(ith_it in 1:500){
              
              A <- data.frame(1              / (  p_05*TFac + p_03 * TFac * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * TFac * ((data$hpp_pressure-1013.25)/1013.25)^2),
                              data$hpp_logIR / (  p_05*TFac + p_03 * TFac * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * TFac * ((data$hpp_pressure-1013.25)/1013.25)^2),
                              1/data$hpp_ir  / (  p_05*TFac + p_03 * TFac * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * TFac * ((data$hpp_pressure-1013.25)/1013.25)^2),
                              - TFac * ((data$hpp_pressure-1013.25)/1013.25)   * (p_00+p_01*data$hpp_logIR+p_02/data$hpp_ir)/(  p_05 * TFac + p_03 * TFac * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * TFac * ((data$hpp_pressure-1013.25)/1013.25)^2)^2,
                              - TFac * ((data$hpp_pressure-1013.25)/1013.25)^2 * (p_00+p_01*data$hpp_logIR+p_02/data$hpp_ir)/(  p_05 * TFac + p_03 * TFac * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * TFac * ((data$hpp_pressure-1013.25)/1013.25)^2)^2,
                              - TFac *                                           (p_00+p_01*data$hpp_logIR+p_02/data$hpp_ir)/(  p_05 * TFac + p_03 * TFac * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * TFac * ((data$hpp_pressure-1013.25)/1013.25)^2)^2)
              
              A_P     <- as.matrix(A)
              id_A_ok <- which(apply(!is.na(A_P) & !is.nan(A_P) & is.numeric(A_P) & is.finite(A_P),1,sum)==dim(A_P)[2])
              
              FF   <- data$CO2 - ((p_00 + p_01*data$hpp_logIR + p_02/data$hpp_ir) / (  p_05 * TFac + p_03 * TFac * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * TFac * ((data$hpp_pressure-1013.25)/1013.25)^2) )
              A    <- as.matrix(A[id_use4cal,])
              
              FF   <- as.matrix(FF[id_use4cal],col=1)
              x    <- solve(t(A)%*%A)%*%t(A)%*%FF
              
              p_00 <- x[1] + p_00
              p_01 <- x[2] + p_01
              p_02 <- x[3] + p_02
              p_03 <- x[4] + p_03
              p_04 <- x[5] + p_04
              p_05 <- x[6] + p_05
              
            }
            
            max_par                <- 14
            n_par                  <- 5
            parameters             <- rep(NA,max_par)
            parameters[1:n_par]    <- c(x[1]+p_00,x[2]+p_01,x[3]+p_02,x[4]+p_03,x[5]+p_04,x[6]+p_05)
            
            CO2_predicted          <- rep(NA,dim(data)[1])
            CO2_predicted_se       <- rep(NA,dim(data)[1])
            CO2_predicted[id_A_ok] <- ((p_00 + p_01*data$hpp_logIR + p_02/data$hpp_ir) / (   p_05 * TFac + p_03 * TFac * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * TFac * ((data$hpp_pressure-1013.25)/1013.25)^2) )[id_A_ok]
            CO2_predicted_ppm      <- CO2_predicted * (1013.25/data$hpp_pressure)
          }
          
        }
        
        
        
        # nulling predictions
        
        if(n_id_nullingPeriod>0){
          nullingPeriods_df <- data.frame(date=data$date[id_nullingPeriod],
                                          HPP_CO2=data$hpp_co2[id_nullingPeriod],
                                          CO2=CO2_predicted[id_nullingPeriod],
                                          IR=data$hpp_ir[id_nullingPeriod],
                                          stringsAsFactors = F)
          
          
          
          
          #
          
          MEAN_NULL_PRED   <- mean(nullingPeriods_df$CO2)
          SD_NULL_PRED     <- sd(nullingPeriods_df$CO2)
          N_NULL_PRED      <- dim(nullingPeriods_df)[1]
          
          MEAN_NULL_HPP    <- mean(nullingPeriods_df$HPP_CO2)
          SD_NULL_HPP      <- sd(nullingPeriods_df$HPP_CO2)
          N_NULL_HPP       <- dim(nullingPeriods_df)[1]
          
          fit_NULL_PRED    <- lm(CO2~date,nullingPeriods_df)
          pred_NULL_PRED   <- predict(fit_NULL_PRED,data)
          slope_NULL_PRED  <- as.numeric(fit_NULL_PRED$coefficients[2]) * 86400 * 30
          
          fit_NULL_HPP     <- lm(HPP_CO2~date,nullingPeriods_df)
          pred_NULL_HPP    <- predict(fit_NULL_HPP,data)
          slope_NULL_HPP   <- as.numeric(fit_NULL_HPP$coefficients[2]) * 86400 * 30
          
          id_NULL_last     <- which(15>as.numeric(difftime(time1=max(nullingPeriods_df$date),time2=nullingPeriods_df$date,units="days",tz="UTC")))
          mean_PRED_LAST   <- mean(nullingPeriods_df$CO2[id_NULL_last])
          mean_HPP_LAST    <- mean(nullingPeriods_df$HPP_CO2[id_NULL_last])
          
          #
          
          leg_str_01_PRED  <- paste("MEAN:    ",sprintf("%6.2f",MEAN_NULL_PRED))
          leg_str_02_PRED  <- paste("SD:      ",sprintf("%6.2f",SD_NULL_PRED))
          leg_str_03_PRED  <- paste("dCO2/30d:",sprintf("%6.2f",slope_NULL_PRED))
          leg_str_04_PRED  <- paste("N:       ",sprintf("%6.0f",N_NULL_PRED))
          
          leg_str_01_HPP   <- paste("MEAN:    ",sprintf("%6.2f",MEAN_NULL_HPP))
          leg_str_02_HPP   <- paste("SD:      ",sprintf("%6.2f",SD_NULL_HPP))
          leg_str_03_HPP   <- paste("dCO2/30d:",sprintf("%6.2f",slope_NULL_HPP))
          leg_str_04_HPP   <- paste("N:       ",sprintf("%6.0f",N_NULL_HPP))
          
          #
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_NULLING.pdf",sep="")
          
          def_par <- par()
          pdf(file = figname, width=12, height=8, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
          
          yrange <- range(c(nullingPeriods_df$CO2,nullingPeriods_df$HPP_CO2)) + c(0,15)
          
          plot(nullingPeriods_df$date,nullingPeriods_df$CO2,pch=16,col=1,cex=0.5,ylim=yrange,xlab="Date",ylab=expression(paste("CO"[2]*" [PRED/HPP] in nulling periods [ppm]")),cex.lab=1.25,cex.axis=1.25)
          
          points(nullingPeriods_df$date,nullingPeriods_df$HPP_CO2,pch=1,col="gray50",cex=0.5,cex.lab=1.25,cex.axis=1.25)
          
          lines(data$date,pred_NULL_PRED,col="black", lwd=2,lty=5)
          lines(data$date,pred_NULL_HPP, col="gray50",lwd=2,lty=5)
          
          lines(data$date,rep(mean_PRED_LAST,dim(data)[1]), col="black", lwd=1,lty=3)
          lines(data$date,rep(mean_HPP_LAST, dim(data)[1]), col="gray50",lwd=1,lty=3)
          
          par(family="mono")
          legend("topleft", legend=c("PRED","HPP"),pch=c(16,1),col=c("black","gray50"),bg="white",cex=1.25)
          legend("topright",legend=c(leg_str_01_PRED,leg_str_02_PRED,leg_str_03_PRED,"",leg_str_01_HPP,leg_str_02_HPP,leg_str_03_HPP,"",leg_str_04_PRED),bg="white",cex=1.25)
          par(family="")
          
          par(def_par)
          dev.off()
          
          #
          
          if(F){
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_NULLING_HPPIR.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=12, height=8, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
            
            id        <- which(abs(nullingPeriods_df$IR-mean(nullingPeriods_df$IR))<10*mad(nullingPeriods_df$IR))
            yrange    <- range(nullingPeriods_df$IR[id])
            xTicks    <- seq(strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20180102000000","%Y%m%d%H%M%S",tz="UTC"),"week")
            xTicksLab <- strftime(xTicks,"%Y-%m-%d",tz="UTC")
            
            plot(nullingPeriods_df$date,nullingPeriods_df$IR,pch=16,col=1,cex=0.5,ylim=yrange,xlab="Date",ylab=expression(paste("IR [XX] (in nulling periods)")),cex.lab=1.25,cex.axis=1.25,xaxt="n")
            axis(side=1,at = xTicks,labels = xTicksLab)
            
            par(def_par)
            dev.off()
            
            #
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_NULLING_HPPCO2.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=12, height=8, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
            
            id        <- which(abs(nullingPeriods_df$HPP_CO2-mean(nullingPeriods_df$HPP_CO2))<10*mad(nullingPeriods_df$HPP_CO2))
            yrange    <- range(nullingPeriods_df$HPP_CO2[id])
            xTicks    <- seq(strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20180102000000","%Y%m%d%H%M%S",tz="UTC"),"week")
            xTicksLab <- strftime(xTicks,"%Y-%m-%d",tz="UTC")
            
            plot(nullingPeriods_df$date,nullingPeriods_df$HPP_CO2,pch=16,col=1,cex=0.5,ylim=yrange,xlab="Date",ylab=expression(paste("HPP CO2 [ppm] (in nulling periods)")),cex.lab=1.25,cex.axis=1.25,xaxt="n")
            axis(side=1,at = xTicks,labels = xTicksLab)
            
            par(def_par)
            dev.off()
            
            #
            
            rm(id,yrange,xTicks,xTicksLab)
            gc()
            
          }
        }
        
        
        
        # bottle cal predictions
        
        if(n_id_bottle_cal_periods>1){
          
          bottle_cal_periods_df <- data.frame(date=data$date[id_bottle_cal_periods],
                                              HPP_CO2=data$hpp_co2[id_bottle_cal_periods],
                                              CO2_PRED=CO2_predicted[id_bottle_cal_periods],
                                              CO2=data$CO2[id_bottle_cal_periods],
                                              stringsAsFactors = F)
          
          #
          
          u_BCP_days         <- sort(unique(floor(data$days[id_bottle_cal_periods])))
          n_u_BCP_days       <- length(u_BCP_days)
          BCP_days_RES       <- rep(NA,n_u_BCP_days)
          BCP_days_timestamp <- rep(NA,n_u_BCP_days)
          
          for(ith_BCP_day in 1:n_u_BCP_days){
            id   <- which(floor(data$days[id_bottle_cal_periods])==u_BCP_days[ith_BCP_day])
            n_id <- length(id)
            
            if(n_id>5){
              BCP_days_RES[ith_BCP_day]       <- mean(CO2_predicted[id_bottle_cal_periods[id]]-data$CO2[id_bottle_cal_periods[id]])
              BCP_days_timestamp[ith_BCP_day] <- mean(data$timestamp[id_bottle_cal_periods[id]])
            }
          }
          
          tmp               <- approx(x=BCP_days_timestamp,y=BCP_days_RES,data$timestamp,method="linear",rule = 2)
          BCP_adj           <- tmp$y
          
          if(CV_mode%in%c(4,6,7)){
            CO2_predicted_BCP <- CO2_predicted - BCP_adj
          }
          
          rm(u_BCP_days,n_u_BCP_days,BCP_days_RES,BCP_days_timestamp,id,n_id,tmp)
          gc()
        }
        
        
        #
        
        plotdir <- paste(resultdir,sensor_models[[ith_sensor_model]]$name,sep="")
        if(!dir.exists(plotdir)){
          dir.create((plotdir))
        }
        
        #
        
        if(n_id_bottle_cal_periods>1){
          
          fit_RES        <- lm(I(CO2_PRED-CO2)~date,bottle_cal_periods_df[bottle_cal_periods_df$date>strptime("20170915000000","%Y%m%d%H%M%S",tz="UTC"),])
          pred_RES       <- predict(fit_RES,data)
          slope_PRED_RES <- as.numeric(fit_RES$coefficients[2]) * 86400 * 30
          
          N_RES          <- dim(bottle_cal_periods_df)[1]
          SD_NULL_PRED   <- sqrt(sum((bottle_cal_periods_df$CO2_PRED-bottle_cal_periods_df$CO2)^2)/N_RES)
          MEAN_NULL_PRED <- mean(bottle_cal_periods_df$CO2_PRED-bottle_cal_periods_df$CO2)
          
          #
          
          leg_str_01  <- paste("MEAN:    ",sprintf("%6.2f",MEAN_NULL_PRED))
          leg_str_02  <- paste("SD:      ",sprintf("%6.2f",SD_NULL_PRED))
          leg_str_03  <- paste("dCO2/30d:",sprintf("%6.2f",slope_PRED_RES))
          leg_str_04  <- paste("N:       ",sprintf("%6.0f",N_RES))
          
          #
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL.pdf",sep="")
          
          def_par <- par()
          pdf(file = figname, width=12, height=8, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
          
          yrange <- range(bottle_cal_periods_df$CO2_PRED-bottle_cal_periods_df$CO2)
          
          plot(bottle_cal_periods_df$date,bottle_cal_periods_df$CO2_PRED-bottle_cal_periods_df$CO2,pch=16,col=1,cex=0.5,ylim=yrange,xlab="Date",ylab=expression(paste("CO"[2]*" PRED - CO"[2]*" REF [ppm]")),cex.lab=1.25,cex.axis=1.25)
          
          lines(data$date,pred_RES,col="black", lwd=2,lty=5)
          
          for(level in seq(-100,100,5)){
            lines(c(min(data$date),max(data$date)),c(level,level),col="gray50",lwd=1,lty=1)
          }
          
          lines(c(strptime("20170915000000","%Y%m%d%H%M%S",tz="UTC"),strptime("20170915000000","%Y%m%d%H%M%S",tz="UTC")),c(-1e9,1e9),col=2,lwd=1)
          
          par(family="mono")
          legend("topleft", legend=c("PRED","HPP"),pch=c(16,1),col=c("black","gray50"),bg="white",cex=1.25)
          legend("topright",legend=c(leg_str_03),bg="white",cex=1.25)
          par(family="")
          
          par(def_par)
          dev.off()
          
          #
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL_CO2.pdf",sep="")
          
          def_par <- par()
          pdf(file = figname, width=12, height=8, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
          
          plot(bottle_cal_periods_df$date,bottle_cal_periods_df$CO2,pch=16,col=1,cex=0.5,xlab="Date",ylab=expression(paste("CO"[2]*" [ppm]")),cex.lab=1.25,cex.axis=1.25)
          
          
          par(def_par)
          dev.off()
          
          #
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL_HPP_CO2.pdf",sep="")
          
          def_par <- par()
          pdf(file = figname, width=12, height=8, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
          
          yrange    <- c(0,0)
          yrange[1] <- min(bottle_cal_periods_df$HPP_CO2[which(bottle_cal_periods_df$date>=strptime("20170921000000","%Y%m%d%H%M%S",tz="UTC") & !is.na(bottle_cal_periods_df$HPP_CO2))]) 
          yrange[2] <- max(bottle_cal_periods_df$HPP_CO2[which(bottle_cal_periods_df$date>=strptime("20170921000000","%Y%m%d%H%M%S",tz="UTC") & !is.na(bottle_cal_periods_df$HPP_CO2))]) 
          
          plot(bottle_cal_periods_df$date,bottle_cal_periods_df$HPP_CO2,ylim=yrange,pch=16,col=1,cex=0.5,xlab="Date",ylab=expression(paste("HPP CO"[2]*" [ppm]")),cex.lab=1.25,cex.axis=1.25)
          
          par(def_par)
          dev.off()
          
        }
        
        
        # Statistics
        
        residuals_FIT  <- CO2_predicted[id_use4cal] - data$CO2[id_use4cal]
        RMSE_FIT       <- sqrt(sum((residuals_FIT)^2)/n_id_use4cal)
        COR_COEF_FIT   <- cor(x = CO2_predicted[id_use4cal],y=data$CO2[id_use4cal],method="pearson",use="complete.obs")
        
        residuals_INI  <- data$hpp_co2[id_use4cal] - data$CO2[id_use4cal]
        RMSE_INI       <- sqrt(sum((residuals_INI)^2)/n_id_use4cal)
        COR_COEF_INI   <- cor(x = data$hpp_co2[id_use4cal],y=data$CO2[id_use4cal],method="pearson",use="complete.obs")
        
        residuals_PRED <- CO2_predicted[id_use4pred] - data$CO2[id_use4pred]
        RMSE_PRED      <- sqrt(sum((residuals_PRED)^2)/n_id_use4pred)
        COR_COEF_PRED  <- cor(x = CO2_predicted[id_use4pred],y=data$CO2[id_use4pred],method="pearson",use="complete.obs")
        
        if(n_id_bottle_cal_periods>1 & CV_mode%in%c(4,6,7)){
          residuals_PRED_BCP <- CO2_predicted_BCP[id_use4pred] - data$CO2[id_use4pred]
          RMSE_PRED_BCP      <- sqrt(sum((residuals_PRED_BCP)^2)/n_id_use4pred)
          COR_COEF_PRED_BCP  <- cor(x = CO2_predicted_BCP[id_use4pred],y=data$CO2[id_use4pred],method="pearson",use="complete.obs")
        }else{
          residuals_PRED_BCP <- NA
          RMSE_PRED_BCP      <- NA
          COR_COEF_PRED_BCP  <- NA
        }
        
        Q000_FIT       <- quantile(residuals_FIT,probs=0.00)
        Q001_FIT       <- quantile(residuals_FIT,probs=0.01)
        Q005_FIT       <- quantile(residuals_FIT,probs=0.05)
        Q010_FIT       <- quantile(residuals_FIT,probs=0.10)
        Q050_FIT       <- quantile(residuals_FIT,probs=0.50)
        Q090_FIT       <- quantile(residuals_FIT,probs=0.90)
        Q095_FIT       <- quantile(residuals_FIT,probs=0.95)
        Q099_FIT       <- quantile(residuals_FIT,probs=0.99)
        Q100_FIT       <- quantile(residuals_FIT,probs=1.00)
        
        Q000_PRED      <- quantile(residuals_PRED,probs=0.00)
        Q001_PRED      <- quantile(residuals_PRED,probs=0.01)
        Q005_PRED      <- quantile(residuals_PRED,probs=0.05)
        Q010_PRED      <- quantile(residuals_PRED,probs=0.10)
        Q050_PRED      <- quantile(residuals_PRED,probs=0.50)
        Q090_PRED      <- quantile(residuals_PRED,probs=0.90)
        Q095_PRED      <- quantile(residuals_PRED,probs=0.95)
        Q099_PRED      <- quantile(residuals_PRED,probs=0.99)
        Q100_PRED      <- quantile(residuals_PRED,probs=1.00)
        
        if(n_id_bottle_cal_periods>1 & CV_mode%in%c(4,6,7)){
          Q000_PRED_BCP  <- quantile(residuals_PRED_BCP,probs=0.00)
          Q001_PRED_BCP  <- quantile(residuals_PRED_BCP,probs=0.01)
          Q005_PRED_BCP  <- quantile(residuals_PRED_BCP,probs=0.05)
          Q010_PRED_BCP  <- quantile(residuals_PRED_BCP,probs=0.10)
          Q050_PRED_BCP  <- quantile(residuals_PRED_BCP,probs=0.50)
          Q090_PRED_BCP  <- quantile(residuals_PRED_BCP,probs=0.90)
          Q095_PRED_BCP  <- quantile(residuals_PRED_BCP,probs=0.95)
          Q099_PRED_BCP  <- quantile(residuals_PRED_BCP,probs=0.99)
          Q100_PRED_BCP  <- quantile(residuals_PRED_BCP,probs=1.00)
        }else{
          Q000_PRED_BCP  <- NA
          Q001_PRED_BCP  <- NA
          Q005_PRED_BCP  <- NA
          Q010_PRED_BCP  <- NA
          Q050_PRED_BCP  <- NA
          Q090_PRED_BCP  <- NA
          Q095_PRED_BCP  <- NA
          Q099_PRED_BCP  <- NA
          Q100_PRED_BCP  <- NA
        }
        
        # 
        
        statistics <- rbind(statistics,
                            data.frame(SensorUnit=SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],
                                       sensor=sensors2cal[ith_sensor2cal],
                                       sensor_model = sensor_models[[ith_sensor_model]]$name,
                                       CALP=CV_P,
                                       date_FIT_UTC_from=strftime(min(data$date[id_use4cal]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       date_FIT_UTC_to  =strftime(max(data$date[id_use4cal]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       date_PRED_UTC_from=strftime(min(data$date[id_use4pred]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       date_PRED_UTC_to  =strftime(max(data$date[id_use4pred]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                       n_FIT=n_id_use4cal,
                                       n_PRED=n_id_use4pred,
                                       RMSE_FIT=RMSE_FIT,
                                       COR_FIT=COR_COEF_FIT,
                                       RMSE_PRED=RMSE_PRED,
                                       COR_PRED=COR_COEF_PRED,
                                       RMSE_PRED_BCP=RMSE_PRED_BCP,
                                       COR_COEF_PRED_BCP=COR_COEF_PRED_BCP,
                                       par00=parameters[01],
                                       par01=parameters[02],
                                       par02=parameters[03],
                                       par03=parameters[04],
                                       par04=parameters[05],
                                       par05=parameters[06],
                                       par06=parameters[07],
                                       par07=parameters[08],
                                       par08=parameters[09],
                                       par09=parameters[10],
                                       par10=parameters[11],
                                       par11=parameters[12],
                                       par12=parameters[13],
                                       par13=parameters[14],
                                       CO2_min_FIT       = min(data$CO2[id_use4cal],na.rm=T),
                                       CO2_max_FIT       = max(data$CO2[id_use4cal],na.rm=T),
                                       CO2_min_PRED      = min(data$CO2[id_use4pred],na.rm=T),
                                       CO2_max_PRED      = max(data$CO2[id_use4pred],na.rm=T),
                                       T_min_FIT         = min(data$T[id_use4cal],na.rm=T),
                                       T_max_FIT         = max(data$T[id_use4cal],na.rm=T),
                                       T_min_PRED        = min(data$T[id_use4pred],na.rm=T),
                                       T_max_PRED        = max(data$T[id_use4pred],na.rm=T),
                                       T_sht21_min_FIT   = min(data$sht21_T[id_use4cal],na.rm=T),
                                       T_sht21_max_FIT   = max(data$sht21_T[id_use4cal],na.rm=T),
                                       T_sht21_min_PRED  = min(data$sht21_T[id_use4pred],na.rm=T),
                                       T_sht21_max_PRED  = max(data$sht21_T[id_use4pred],na.rm=T),
                                       pressure_min_FIT  = min(data$pressure[id_use4cal],na.rm=T),
                                       pressure_max_FIT  = max(data$pressure[id_use4cal],na.rm=T),
                                       pressure_min_PRED = min(data$pressure[id_use4pred],na.rm=T),
                                       pressure_max_PRED = max(data$pressure[id_use4pred],na.rm=T),
                                       stringsAsFactors = F))
        
        
        ## Plot : SCATTER RAW OBS - REF OBS / FITTED OBS - REF_OBS
        
        if(T){
          
          plotdir <- paste(resultdir,sensor_models[[ith_sensor_model]]$name,sep="")
          if(!dir.exists(plotdir)){
            dir.create((plotdir))
          }
          
          #
          
          sensor_descriptor <- paste("SU", SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],"_S",sensors2cal[ith_sensor2cal],sep="")
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER.pdf",sep="")
          
          def_par <- par()
          pdf(file = figname, width=16, height=16, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.5,0.5),mfrow=c(2,2))
          
          # P1
          
          if(sensor_models[[ith_sensor_model]]$modelType %in% c("SM_CO2","LM_CO2")){
            xlabString <- paste("PIC CO2 [ppm]",sep="")
            ylabString <- paste(sensor_descriptor," CO2 [ppm]",sep="")
            
            plot(data$CO2,data$hpp_co2,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.5,cex.axis=1.5)
          }
          
          if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","SM_IR","GAM")){
            xlabString <- paste("PIC CO2 [ppm]",sep="")
            ylabString <- paste(sensor_descriptor," IR [xx]",sep="")
            
            plot(data$CO2,data$hpp_ir,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.5,cex.axis=1.5)
          }
          
          # P2
          
          if(ith_temp_data_selections==0){
            
            if(sensor_models[[ith_sensor_model]]$modelType %in% c("SM_CO2","LM_CO2")){
              str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_INI))
              str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_INI))
              str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
              leg_vec    <- c(str21,str22,str23)
              
              xrange     <- range(c(data$CO2[id_use4cal],data$hpp_co2[id_use4cal]))
              yrange     <- xrange
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," CO2 [ppm]",sep="")
              
              plot(data$CO2[id_use4cal],data$hpp_co2[id_use4cal],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.5,cex.axis=1.5)
            }
            
            if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","SM_IR","GAM")){
              # str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_INI))
              # str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_INI))
              str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
              leg_vec    <- c(str23)
              
              xrange     <- range(c(data$CO2[id_use4cal],data$hpp_co2[id_use4cal]))
              yrange     <- xrange
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," IR [XX]",sep="")
              
              plot(data$CO2[id_use4cal],data$hpp_ir[id_use4cal],pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.5,cex.axis=1.5)
            }
            
            
          }else{
            
            str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_FIT))
            str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_FIT))
            str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
            leg_vec    <- c(str21,str22,str23)
            
            xrange     <- range(c(data$CO2[id_use4cal],CO2_predicted[id_use4cal]))
            yrange     <- xrange
            xlabString <- paste("PIC CO2 [ppm]",sep="")
            ylabString <- paste(sensor_descriptor," CO2 [ppm] (CAL)",sep="")
            subString  <- paste("Data:",strftime(min(data$date[id_use4cal]),"%d/%m/%Y %H:%M",tz="UTC"),"-",strftime(max(data$date[id_use4cal]),"%d/%m/%Y %H:%M",tz="UTC"))
            
            plot(data$CO2[id_use4cal],CO2_predicted[id_use4cal],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.5,cex.axis=1.5,cex.sub=1.25)
            
          }
          
          lines(c(0,1e5),c(0,1e5),   col=2,lwd=1)
          lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
          lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
          
          par(family="mono")
          legend("topleft",legend=leg_vec,bg="white",cex=1.5)
          par(family="")
          
          # P3
          
          str31 <- paste("RMSE:", sprintf("%6.2f",RMSE_PRED))
          str32 <- paste("COR: ", sprintf("%6.2f",COR_COEF_PRED))
          str33 <- paste("N:   ", sprintf("%6.0f",n_id_use4pred))
          
          xrange     <- range(c(data$CO2[id_use4pred],CO2_predicted[id_use4pred]))
          yrange     <- xrange
          xlabString <- paste("PIC CO2 [ppm]",sep="")
          ylabString <- paste(sensor_descriptor," CO2 predicted [ppm]",sep="")
          subString  <- paste("Data:",strftime(min(data$date[id_use4pred]),"%d/%m/%Y %H:%M",tz="UTC"),"-",strftime(max(data$date[id_use4pred]),"%d/%m/%Y %H:%M",tz="UTC"))
          
          plot(data$CO2[id_use4pred], CO2_predicted[id_use4pred],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.5,cex.axis=1.5,cex.sub=1.25)
          lines(c(0,1e5),c(0,1e5),col=2,lwd=1)
          lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
          lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
          
          par(family="mono")
          legend("topleft",legend=c(str31,str32,str33),bg="white",cex=1.5)
          par(family="")
          
          # P4
          
          str41 <- paste("Q000:",sprintf("%6.2f",Q000_PRED))
          str42 <- paste("Q001:",sprintf("%6.2f",Q001_PRED))
          str43 <- paste("Q005:",sprintf("%6.2f",Q005_PRED))
          str44 <- paste("Q050:",sprintf("%6.2f",Q050_PRED))
          str45 <- paste("Q095:",sprintf("%6.2f",Q095_PRED))
          str46 <- paste("Q099:",sprintf("%6.2f",Q099_PRED))
          str47 <- paste("Q100:",sprintf("%6.2f",Q100_PRED))
          
          xlabString     <- paste(sensor_descriptor," CO2 predicted - PIC CO2 [ppm]",sep="")
          residuals_plot <- residuals_PRED[which(abs(residuals_PRED)<=1e3)]
          
          hist(residuals_plot,breaks = seq(-1e3,1e3,2.5),xlim=c(-50,50),col="slategray",xlab=xlabString,main="",cex.lab=1.5,cex.axis=1.5)
          lines(c(  0,  0),c(-1e9,1e9),col=2,lwd=1)
          lines(c( 20, 20),c(-1e9,1e9),col=2,lwd=1,lty=5)
          lines(c(-20,-20),c(-1e9,1e9),col=2,lwd=1,lty=5)
          
          par(family="mono")
          legend("topleft",legend=c(str41,str42,str43,str44,str45,str46,str47,str33),bg="white",cex=1.5)
          par(family="")
          
          #
          
          dev.off()
          par(def_par)
          
          
          
          ##
          
          if(n_id_bottle_cal_periods>1 & CV_mode%in%c(4,6,7)){
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER_BCP.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=16, height=16, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5),mfrow=c(2,2))
            
            # P1
            
            if(sensor_models[[ith_sensor_model]]$modelType %in% c("SM_CO2","LM_CO2")){
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," CO2 [ppm]",sep="")
              
              plot(data$CO2,data$hpp_co2,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.5,cex.axis=1.5)
            }
            
            if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","SM_IR","GAM")){
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," IR [xx]",sep="")
              
              plot(data$CO2,data$hpp_ir,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.5,cex.axis=1.5)
            }
            
            # P2
            
            if(ith_temp_data_selections==0){
              
              if(sensor_models[[ith_sensor_model]]$modelType %in% c("SM_CO2","LM_CO2")){
                str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_INI))
                str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_INI))
                str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
                leg_vec    <- c(str21,str22,str23)
                
                xrange     <- range(c(data$CO2[id_use4cal],data$hpp_co2[id_use4cal]))
                yrange     <- xrange
                xlabString <- paste("PIC CO2 [ppm]",sep="")
                ylabString <- paste(sensor_descriptor," CO2 [ppm]",sep="")
                
                plot(data$CO2[id_use4cal],data$hpp_co2[id_use4cal],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.5,cex.axis=1.5)
              }
              
              if(sensor_models[[ith_sensor_model]]$modelType %in% c("LM_IR","SM_IR","GAM")){
                # str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_INI))
                # str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_INI))
                str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
                leg_vec    <- c(str23)
                
                xrange     <- range(c(data$CO2[id_use4cal],data$hpp_co2[id_use4cal]))
                yrange     <- xrange
                xlabString <- paste("PIC CO2 [ppm]",sep="")
                ylabString <- paste(sensor_descriptor," IR [XX]",sep="")
                
                plot(data$CO2[id_use4cal],data$hpp_ir[id_use4cal],pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,cex.lab=1.5,cex.axis=1.5)
              }
              
              
            }else{
              
              str21      <- paste("RMSE:", sprintf("%6.2f", RMSE_FIT))
              str22      <- paste("COR: ", sprintf("%6.2f", COR_COEF_FIT))
              str23      <- paste("N:   ", sprintf("%6.0f", n_id_use4cal))
              leg_vec    <- c(str21,str22,str23)
              
              xrange     <- range(c(data$CO2[id_use4cal],CO2_predicted[id_use4cal]))
              yrange     <- xrange
              xlabString <- paste("PIC CO2 [ppm]",sep="")
              ylabString <- paste(sensor_descriptor," CO2 [ppm] (CAL)",sep="")
              subString  <- paste("Data:",strftime(min(data$date[id_use4cal]),"%d/%m/%Y %H:%M",tz="UTC"),"-",strftime(max(data$date[id_use4cal]),"%d/%m/%Y %H:%M",tz="UTC"))
              
              plot(data$CO2[id_use4cal],CO2_predicted[id_use4cal],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.5,cex.axis=1.5,cex.sub=1.25)
              
            }
            
            lines(c(0,1e5),c(0,1e5),   col=2,lwd=1)
            lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
            lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=leg_vec,bg="white",cex=1.5)
            par(family="")
            
            # P3
            
            str31 <- paste("RMSE:", sprintf("%6.2f",RMSE_PRED_BCP))
            str32 <- paste("COR: ", sprintf("%6.2f",COR_COEF_PRED_BCP))
            str33 <- paste("N:   ", sprintf("%6.0f",n_id_use4pred))
            
            xrange     <- range(c(data$CO2[id_use4pred],CO2_predicted_BCP[id_use4pred]))
            yrange     <- xrange
            xlabString <- paste("PIC CO2 [ppm]",sep="")
            ylabString <- paste(sensor_descriptor," CO2 predicted [ppm]",sep="")
            subString  <- paste("Data:",strftime(min(data$date[id_use4pred]),"%d/%m/%Y %H:%M",tz="UTC"),"-",strftime(max(data$date[id_use4pred]),"%d/%m/%Y %H:%M",tz="UTC"))
            
            plot(data$CO2[id_use4pred], CO2_predicted_BCP[id_use4pred],xlim=xrange,ylim=yrange,pch=16,cex=0.5,xlab=xlabString,ylab=ylabString,sub=subString,cex.lab=1.5,cex.axis=1.5,cex.sub=1.25)
            lines(c(0,1e5),c(0,1e5),col=2,lwd=1)
            lines(c(0,1e5),c(0,1e5)+20,col=2,lwd=1,lty=5)
            lines(c(0,1e5),c(0,1e5)-20,col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c(str31,str32,str33),bg="white",cex=1.5)
            par(family="")
            
            # P4
            
            str41 <- paste("Q000:",sprintf("%6.2f",Q000_PRED_BCP))
            str42 <- paste("Q001:",sprintf("%6.2f",Q001_PRED_BCP))
            str43 <- paste("Q005:",sprintf("%6.2f",Q005_PRED_BCP))
            str44 <- paste("Q050:",sprintf("%6.2f",Q050_PRED_BCP))
            str45 <- paste("Q095:",sprintf("%6.2f",Q095_PRED_BCP))
            str46 <- paste("Q099:",sprintf("%6.2f",Q099_PRED_BCP))
            str47 <- paste("Q100:",sprintf("%6.2f",Q100_PRED_BCP))
            
            xlabString     <- paste(sensor_descriptor," CO2 predicted BCP - PIC CO2 [ppm]",sep="")
            residuals_plot <- residuals_PRED_BCP[which(abs(residuals_PRED)<=1e3)]
            
            hist(residuals_plot,breaks = seq(-1e3,1e3,2.5),xlim=c(-50,50),col="slategray",xlab=xlabString,main="",cex.lab=1.5,cex.axis=1.5)
            lines(c(  0,  0),c(-1e9,1e9),col=2,lwd=1)
            lines(c( 20, 20),c(-1e9,1e9),col=2,lwd=1,lty=5)
            lines(c(-20,-20),c(-1e9,1e9),col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c(str41,str42,str43,str44,str45,str46,str47,str33),bg="white",cex=1.5)
            par(family="")
            
            #
            
            dev.off()
            par(def_par)
            
            
          }
          
          
          
          ## PLOT : Time-series
          
          # CO2
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS.pdf",sep="")
          
          yyy_ok             <- rep(NA,dim(data)[1])
          yyy_ok[id_use4cal] <- min(c(data$CO2,data$hpp_co2,CO2_predicted),na.rm=T)
          
          tmp_1   <- rep(NA,dim(data)[1])
          tmp_2   <- rep(NA,dim(data)[1])
          
          tmp_1[id_use4cal]  <-CO2_predicted[id_use4cal]
          tmp_2[id_use4pred] <-CO2_predicted[id_use4pred]
          
          yyy     <- cbind(data$CO2,
                           data$hpp_co2,
                           tmp_1,
                           tmp_2,
                           yyy_ok)
          
          xlabString <- "Date" 
          ylabString <- expression(paste("CO"[2]*" [ppm]"))
          legend_str <- c("PIC CO2",paste(sensor_descriptor,"RAW"),paste(sensor_descriptor,"CAL"),paste(sensor_descriptor,"PRED"))
          plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(300,1200),xlabString,ylabString,legend_str)
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS_DAYS.pdf",sep="")
          plot_ts(figname,data$date,yyy,"day",NULL,c(300,1200),xlabString,ylabString,legend_str)
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS_WEEKS.pdf",sep="")
          plot_ts(figname,data$date,yyy,"week",NULL,c(300,1200),xlabString,ylabString,legend_str)
          
          rm(tmp_1,tmp_2)
          gc()
          
          
          # CO2 (REF)
          
          if(T){
            
            figname    <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_REF_TS.pdf",sep="")
            
            yyy        <- cbind(data$CO2)
            
            xlabString <- "Date" 
            ylabString <- expression(paste("CO"[2]*" [ppm]"))
            legend_str <- c("PIC CO2")
            plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(300,1200),xlabString,ylabString,legend_str)
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_REF_TS_WEEKS.pdf",sep="")
            plot_ts(figname,data$date,yyy,"week",NULL,c(300,1200),xlabString,ylabString,legend_str)
            
            rm(tmp_1,tmp_2)
            gc()
            
          }
          
          # CO2-Residuals
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_TS.pdf",sep="")
          
          tmp_1   <- rep(NA,dim(data)[1])
          tmp_2   <- rep(NA,dim(data)[1])
          
          tmp_1[id_use4cal]  <- CO2_predicted[id_use4cal]  - data$CO2[id_use4cal]
          tmp_2[id_use4pred] <- CO2_predicted[id_use4pred] - data$CO2[id_use4pred]
          
          yyy     <- cbind(tmp_1,tmp_2)
          
          xlabString <- "Date" 
          ylabString <- expression(paste("CO"[2]*" PRED - CO"[2]*" REF [ppm]"))
          legend_str <- c("Residuals (FIT)","Residuals (PRED)")
          plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-20,20),xlabString,ylabString,legend_str)
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_TS_DAYS.pdf",sep="")
          plot_ts(figname,data$date,yyy,"day",NULL,c(-20,20),xlabString,ylabString,legend_str)
          
          rm(tmp_1,tmp_2)
          gc()
          
          # CO2-HPP-Residuals
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_HPP_RES_TS.pdf",sep="")
          
          tmp_1   <- rep(NA,dim(data)[1])
          
          
          tmp_1[id_use4pred]  <- data$hpp_co2[id_use4pred]  - data$CO2_ppm[id_use4pred]
          
          yyy     <- cbind(tmp_1)
          
          yrange  <- median(tmp_1,na.rm=T) + 1.5*(as.numeric(quantile(tmp_1,probs=c(0.05,.95),na.rm=T))-median(tmp_1,na.rm=T))
          
          xlabString <- "Date" 
          ylabString <- expression(paste("CO"[2]*" HPP - CO"[2]*" REF [ppm]"))
          legend_str <- c("Residuals")
          plot_ts(figname,data$date,yyy,"all_day2day",NULL,yrange,xlabString,ylabString,legend_str)
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_HPP_RES_TS_DAYS.pdf",sep="")
          plot_ts(figname,data$date,yyy,"day",NULL,yrange,xlabString,ylabString,legend_str)
          
          rm(tmp_1,tmp_2)
          gc()
          
          
          # CO2-Residuals BCP
          
          if(n_id_bottle_cal_periods>1 & CV_mode%in%c(4,6,7)){
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_BCP_TS.pdf",sep="")
            
            tmp_1   <- rep(NA,dim(data)[1])
            tmp_2   <- rep(NA,dim(data)[1])
            
            tmp_1[id_use4cal]  <- CO2_predicted_BCP[id_use4cal]  - data$CO2[id_use4cal]
            tmp_2[id_use4pred] <- CO2_predicted_BCP[id_use4pred] - data$CO2[id_use4pred]
            
            yyy     <- cbind(tmp_1,tmp_2)
            
            xlabString <- "Date" 
            ylabString <- expression(paste("CO"[2]*" PRED BCP - CO"[2]*" REF [ppm]"))
            legend_str <- c("Residuals (FIT)","Residuals (PRED)")
            plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-20,20),xlabString,ylabString,legend_str)
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_BCP_TS_DAYS.pdf",sep="")
            plot_ts(figname,data$date,yyy,"day",NULL,c(-20,20),xlabString,ylabString,legend_str)
            
            rm(tmp_1,tmp_2)
            gc()
          }
          
          # CO2-Residuals + BCP_adj
          
          if(n_id_bottle_cal_periods>1 & CV_mode%in%c(4,6,7)){
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_BCP_adj_TS.pdf",sep="")
            
            tmp_1   <- rep(NA,dim(data)[1])
            tmp_2   <- rep(NA,dim(data)[1])
            
            tmp_1[id_use4cal]  <- CO2_predicted[id_use4cal]  - data$CO2[id_use4cal]
            tmp_2[id_use4pred] <- CO2_predicted[id_use4pred] - data$CO2[id_use4pred]
            
            yyy     <- cbind(tmp_1,tmp_2,BCP_adj)
            
            xlabString <- "Date" 
            ylabString <- expression(paste("CO"[2]*" PRED - CO"[2]*" REF [ppm]"))
            legend_str <- c("Residuals (FIT)","Residuals (PRED)","BCP adj")
            plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-20,20),xlabString,ylabString,legend_str)
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_BCP_adj_TS_DAYS.pdf",sep="")
            plot_ts(figname,data$date,yyy,"day",NULL,c(-20,20),xlabString,ylabString,legend_str)
            
            rm(tmp_1,tmp_2)
            gc()
          }
          
          
          if(F){
            
            # CO2 Picarro/HPP EMPA CAL [ppm]
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_PICARRO_HPP_EMPA_CAL_TS.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
            
            xlabString <- "Date" 
            ylabString <- expression(paste("CO"[2]*" [ppm]"))
            legend_str <- c("Picarro","HPP CAL EMPA", "HPP CAL EMPA (APPLIED)")
            
            plot(data$date,data$CO2_ppm,xlab=xlabString,ylab=ylabString,ylim=c(350,1000),col=1,t="l",lwd=1,cex.lab=1.25,cex.axis=1.25)
            
            lines(data$date[id_use4pred],CO2_predicted_ppm[id_use4pred]+100,col=2,lwd=1)
            
            lines(data$date[id_use4cal],CO2_predicted_ppm[id_use4cal]+100,col=4,lwd=1)
            
            par(family="mono")
            legend("topright",legend=legend_str,lty=c(1,1,1),col=c(1,4,2),bg="white")
            par(family="")
            
            dev.off()
            par(def_par)
          }
          
          if(F){
            
            # CO2-Residuals, NULLING (hour)
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_NULLING_HOUR_TS.pdf",sep="")
            
            tmp_df  <- data.frame(date=data$date,
                                  timestamp=data$timestamp,
                                  RES_use4cal=NA,
                                  RES_use4pred=NA,
                                  HPP_CO2_NULL=NA,
                                  stringsAsFactors=F)
            
            tmp_df$RES_use4cal[id_use4cal]        <- CO2_predicted[id_use4cal]  - data$CO2[id_use4cal]
            tmp_df$RES_use4pred[id_use4pred]      <- CO2_predicted[id_use4pred] - data$CO2[id_use4pred]
            tmp_df$HPP_CO2_NULL[id_nullingPeriod] <- data$hpp_co2[id_nullingPeriod]
            
            tmp_df_hh <- timeAverage(mydata = tmp_df,avg.time = "hour",statistic = "mean",start.date = strptime(strftime(data$date,"%Y%m%d000000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
            
            id         <- which(!is.na(tmp_df$RES_use4pred))
            fit_res    <- lm(RES_use4pred~timestamp, tmp_df[id,])
            tmp        <- predict(fit_res,tmp_df,se.fit=T)
            tmp_df$RES_use4pred_trend <- tmp$fit
            
            id         <- which(!is.na(tmp_df$HPP_CO2_NULL))
            fit_null   <- lm(HPP_CO2_NULL~timestamp, tmp_df[id,])
            tmp        <- predict(fit_null,tmp_df,se.fit=T)
            tmp_df$HPP_CO2_NULL_trend <- tmp$fit
            
            def_par <- par()
            pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
            
            xlabString <- "Date" 
            ylabString <- expression(paste("CO"[2]*" CAL - CO"[2]*" REF [ppm]"))
            legend_str <- c("Residuals",paste("dRES/30d:",sprintf("%6.2f",fit_res$coefficients[2]*86400*30)),"HPP ZERO",paste("dRES/30d:",sprintf("%6.2f",fit_null$coefficients[2]*86400*30)))
            
            plot(tmp_df_hh$date,tmp_df_hh$RES_use4pred,xlab=xlabString,ylab=ylabString,ylim=c(-20,20),col=2,t="l",lwd=1,cex.lab=1.25,cex.axis=1.25)
            
            lines(tmp_df_hh$date,rep(mean(tmp_df$RES_use4pred,na.rm=T),dim(tmp_df_hh)[1]),lwd=1,lty=5)
            
            lines(tmp_df$date,rep(mean(tmp_df$HPP_CO2_NULL,na.rm=T),dim(tmp_df)[1]),lwd=1,lty=5)
            
            lines(tmp_df$date,tmp_df$RES_use4pred_trend,lwd=1,lty=5,col=2)
            lines(tmp_df$date,tmp_df$HPP_CO2_NULL_trend,lwd=1,lty=5,col=3)
            
            points(tmp_df$date,tmp_df$HPP_CO2_NULL,pch=16,col=3,cex=0.75)
            
            par(family="mono")
            legend("topright",legend=legend_str,lty=c(1,NA,NA,NA),pch=c(NA,NA,16,NA),col=c(2,NA,3,NA),bg="white")
            par(family="")
            
            
            dev.off()
            par(def_par)
            
            rm(id,tmp,tmp_df,fit_null,fit_res)
            gc()
          }
          
          if(F){
            
            # CO2-Residuals in climate chamber
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_PRESSURE_CHAMBER_TS.pdf",sep="")
            
            id   <- which(data$CalMode==2 
                          & data$date>=strptime("20171009120000","%Y%m%d%H%M%S",tz="UTC")
                          & data$date<=strptime("20171009180000","%Y%m%d%H%M%S",tz="UTC"))
            
            n_id <- length(id)
            
            if(n_id>0){
              
              RMSE_PC <- sqrt(sum((CO2_predicted_ppm[id] - data$CO2_ppm[id] - mean(CO2_predicted_ppm[id] - data$CO2_ppm[id],na.rm=T))^2,na.rm=T)/sum(!is.na(CO2_predicted_ppm[id] - data$CO2_ppm[id])))
              
              def_par <- par()
              pdf(file = figname, width=8, height=6, onefile=T, pointsize=12, colormodel="srgb")
              par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
              
              xlabString <- "Date" 
              ylabString <- expression(paste("CO"[2]*" PRED - CO"[2]*" REF [ppm]"))
              
              plot(data$date[id],CO2_predicted_ppm[id] - data$CO2_ppm[id] - mean(CO2_predicted_ppm[id] - data$CO2_ppm[id],na.rm=T),xlab=xlabString,ylab=ylabString,ylim=c(-10,10),col=1,t="l",lwd=1,cex.lab=1.75,cex.axis=1.75)
              
              par(family="mono")
              legend("topright",legend=c(paste("RMSE:",sprintf("%4.1f",RMSE_PC)),paste("N:   ",sprintf("%4.0f",sum(!is.na(CO2_predicted_ppm[id] - data$CO2_ppm[id]))))),cex=1.75,bg="white")
              par(family="")
              
              
              dev.off()
              par(def_par)
            }    
          }
          
          ## PLOT : SCATTER : Residuals vs different factors
          
          if(ith_temp_data_selections==0 | (ith_temp_data_selections==2 &  CV_mode%in%c(6,7) & n_id_bottle_cal_periods>1) ){
            
            factors <- data.frame(cn     =c("hpp_co2","CO2","T","hpp_temperature_mcu","pressure","hpp_pressure","days","RH","AH","AH_COMP","H2O","TU_T","TU_HPPT","HPPT_T","TU_d_T","TU_d_HPPT","HPPT_d_T"),
                                  cn_unit=c("[ppm]","[ppb]","[deg C]","[deg C]","[hPa]","[hPa]","[days]","[%]","[g/m3]","[g/m3]","[Vol-%]","[deg C]","[deg C]","[deg C]","[-]","[-]","[-]"),
                                  flag   =c(F,T,T,F,T,F,F,T,T,F,T,F,F,F,F,F,F),
                                  stringsAsFactors = F)
            
            #
            
            for(res_mode in c("abs","rel")){
              
              res_cmp <- CO2_predicted - data$CO2
              
              if(ith_temp_data_selections==0){
                if(res_mode == "abs"){
                  res_cmp <- CO2_predicted - data$CO2
                }
                if(res_mode == "rel"){
                  res_cmp <- CO2_predicted/data$CO2
                }
              }
              if(ith_temp_data_selections==2 &  CV_mode==6 & n_id_bottle_cal_periods>1){
                if(res_mode == "abs"){
                  res_cmp <- CO2_predicted - data$CO2
                }
                if(res_mode == "rel"){
                  res_cmp <- CO2_predicted/data$CO2
                }
              }
              if(CV_mode==7 & n_id_bottle_cal_periods>1){
                if(res_mode == "abs"){
                  res_cmp <- CO2_predicted - data$CO2
                }
                if(res_mode == "rel"){
                  res_cmp <- CO2_predicted/data$CO2
                }
              }
              
              #
              
              for(ith_factor in 1:dim(factors)[1]){
                
                figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SCATTER_RES_FACTORS_",res_mode,"_",factors$cn[ith_factor],".pdf",sep="")
                
                def_par <- par()
                pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
                par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
                
                pos_factor <- which(colnames(data)==factors$cn[ith_factor])
                
                if(factors$flag[ith_factor]){
                  pos_factor_F <- which(colnames(data)==paste(factors$cn[ith_factor],"_F",sep=""))
                  id_data4cmp  <- which(data[,pos_factor]!= -999 & data[,pos_factor_F] == 1)
                }else{
                  id_data4cmp  <- which(data[,pos_factor]!= -999)
                }
                
                id_res_ok   <- which(!is.na(data$CO2)
                                     & !is.na(data$hpp_co2))
                
                id_grp_01   <- sort(c(id_data4cmp,id_use4cal),decreasing = F)
                id_grp_01   <- id_grp_01[duplicated(id_grp_01)]
                n_id_grp_01 <- length(id_grp_01)
                
                id_grp_02   <- sort(c(id_data4cmp,id_res_ok),decreasing = F)
                id_grp_02   <- id_grp_02[duplicated(id_grp_02)]
                n_id_grp_02 <- length(id_grp_02)
                
                if(n_id_grp_01>0 & n_id_grp_02>0){
                  
                  xrange <- range(data[id_grp_02,pos_factor])
                  
                  if(res_mode == "abs"){
                    yrange <- c(-50,50)
                  }
                  if(res_mode == "rel"){
                    yrange <- c(0.875,1.125)
                  }
                  
                  xlabString <- paste(factors$cn[ith_factor],factors$cn_unit[ith_factor])
                  ylabString <- paste(sensor_descriptor,"residual [ppm]")
                  
                  corCoef <- cor(x=data[id_grp_01,pos_factor],y=res_cmp[id_grp_01],method="pearson",use="complete.obs")
                  fit_res <- lm(y~x,data.frame(x=data[id_grp_01,pos_factor],y=res_cmp[id_grp_01]))
                  
                  plot(  data[id_grp_02,pos_factor], res_cmp[id_grp_02],xlim=xrange,ylim=yrange,xlab=xlabString,ylab=ylabString,pch=16,cex=0.5,cex.axis=1.5,cex.lab=1.5,col="grey70")
                  points(data[id_grp_01,pos_factor], res_cmp[id_grp_01],pch=16,cex=0.5,col=1)
                  
                  lines(c(-1e6,1e6),c(fit_res$coefficients[1]-1e6*fit_res$coefficients[2],fit_res$coefficients[1]+1e6*fit_res$coefficients[2]),col=2,lwd=1,lty=5)
                  lines(c(-1e6,1e6),c(0,0),col=1,lwd=1,lty=5)
                  
                  par(family="mono")
                  legend("topright",legend=c(paste("y=",sprintf("%.2f",fit_res$coefficients[2]),"x+",sprintf("%.1f",fit_res$coefficients[1]),sep=""),
                                             paste("COR: ",sprintf("%5.2f",corCoef),sep="")),
                         bg="white",lty=c(5,NA),col=2)
                  par(family="")
                }
                
                par(def_par)
                dev.off()
              }
            }
          }
          
          ## CO2 vs IR/CO2_HPP/CO2_HPP_PRED
          
          if(CV_mode==0){
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2PIC_vs_IR_HPPCO2_HPPCO2PRED.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=18, height=6, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.1,0.1),mfrow=c(1,3))
            
            for(ith_plot in 1:3){
              
              id <- which(data$CalMode==ith_plot 
                          & !is.na(CO2_predicted_ppm) 
                          & !is.na(data$hpp_co2) 
                          & !is.na(data$CO2_ppm)
                          & !is.na(data$hpp_ir)
                          & use4cal)
              
              n_id <- length(id)
              
              if(n_id>0){
                
                RMSE_02 <- sqrt(sum((data$hpp_co2[id]-data$CO2_ppm[id])^2)/n_id)
                RMSE_03 <- sqrt(sum((CO2_predicted_ppm[id] -data$CO2_ppm[id])^2)/n_id) 
                
                CO2_xrange <- range(c(data$hpp_co2[id],CO2_predicted_ppm[id],data$CO2_ppm[id]),na.rm=T)
                CO2_yrange <- CO2_xrange
                IR_range   <- range(data$hpp_ir,na.rm=T)
                
                plot(data$CO2_ppm[id], data$hpp_ir[id],         xlim=CO2_xrange, ylim=IR_range,  pch=16,cex=0.5,xlab="CO2 PIC [ppm]", ylab="HPP IR [XX]",      cex.lab=1.75,cex.axis=1.75)
                
                plot(data$CO2_ppm[id], data$hpp_co2[id],        xlim=CO2_xrange, ylim=CO2_yrange,pch=16,cex=0.5,xlab="CO2 PIC [ppm]", ylab="HPP CO2 [ppm]",    cex.lab=1.75,cex.axis=1.75)
                lines(c(0,1e4),c(0,1e4),col=2,lwd=1)
                
                par(family="mono")
                legend("bottomright",legend=c(paste("RMSE:",sprintf("%6.1f",RMSE_02)),paste("N:   ",sprintf("%6.0f",n_id))),bg="white",cex=2)
                par(family="")
                
                plot(data$CO2_ppm[id], CO2_predicted_ppm[id],   xlim=CO2_xrange, ylim=CO2_yrange,pch=16,cex=0.5,xlab="CO2 PIC [ppm]", ylab="HPP_IR CO2 [ppm]", cex.lab=1.75,cex.axis=1.75)
                lines(c(0,1e4),c(0,1e4),col=2,lwd=1)
                
                par(family="mono")
                legend("bottomright",legend=c(paste("RMSE:",sprintf("%6.1f",RMSE_03)),paste("N:   ",sprintf("%6.0f",n_id))),bg="white",cex=2)
                par(family="")
              }
            }
            
            dev.off()
            par(def_par)
          }
          
          
          ## CO2 vs CO2_HPP/CO2_HPP_PRED
          
          if(CV_mode==0){
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2PIC_vs_HPPCO2_HPPCO2PRED.pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=12, height=6, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.1,0.1),mfrow=c(1,2))
            
            for(ith_plot in 1:3){
              
              id <- which(data$CalMode==ith_plot 
                          & !is.na(CO2_predicted_ppm) 
                          & !is.na(data$hpp_co2) 
                          & !is.na(data$CO2_ppm)
                          & use4cal)
              
              n_id <- length(id)
              
              if(n_id>0){
                
                RMSE_02 <- sqrt(sum((data$hpp_co2[id]-data$CO2_ppm[id])^2)/n_id)
                RMSE_03 <- sqrt(sum((CO2_predicted_ppm[id] -data$CO2_ppm[id])^2)/n_id) 
                
                CO2_xrange <- range(c(data$hpp_co2[id],CO2_predicted_ppm[id],data$CO2_ppm[id]),na.rm=T)
                CO2_yrange <- CO2_xrange
                
                plot(data$CO2_ppm[id], data$hpp_co2[id],        xlim=CO2_xrange, ylim=CO2_yrange,pch=16,cex=0.5,xlab="CO2 PIC [ppm]", ylab="HPP CO2 [ppm]",    cex.lab=1.75,cex.axis=1.75)
                lines(c(0,1e4),c(0,1e4),col=2,lwd=1)
                
                par(family="mono")
                legend("bottomright",legend=c(paste("RMSE:",sprintf("%6.1f",RMSE_02)),paste("N:   ",sprintf("%6.0f",n_id))),bg="white",cex=2)
                par(family="")
                
                plot(data$CO2_ppm[id], CO2_predicted_ppm[id],   xlim=CO2_xrange, ylim=CO2_yrange,pch=16,cex=0.5,xlab="CO2 PIC [ppm]", ylab="CO2 CAL EMPA [ppm]", cex.lab=1.75,cex.axis=1.75)
                lines(c(0,1e4),c(0,1e4),col=2,lwd=1)
                
                par(family="mono")
                legend("bottomright",legend=c(paste("RMSE:",sprintf("%6.1f",RMSE_03)),paste("N:   ",sprintf("%6.0f",n_id))),bg="white",cex=2)
                par(family="")
              }
            }
            
            dev.off()
            par(def_par)
          }
          
          ## Boxplot hpp_temperature_mcu_dT vs RES
          
          for(i in 1:4){
            
            if(i%in%c(2,4) & !CV_mode%in%c(4,6,7)){
              next
            }
            
            if(i %in% c(1,2)){
              delta   <- 0.025
              min_val <- round(min(data$hpp_temperature_mcu_dT,na.rm=T),1)-0.1
              max_val <- round(max(data$hpp_temperature_mcu_dT,na.rm=T),1)+0.1
              min_val <- -0.3
              max_val <-  0.3
              if(i==1){
                figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_hpp_temperature_mcu_dT_vs_RES_BP.pdf",sep="")
                ylabStr <- expression(paste("CO"[2]*" [ppm]"))
              }
              else{
                figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_hpp_temperature_mcu_dT_vs_RES_BCP_BP.pdf",sep="")
                ylabStr <- expression(paste("CO"[2]*" [ppm] (BCP)"))
              }
              xlabStr <- "MCU_T [deg C/min]"
              xrange  <- c(-0.3,0.3)
            }
            if(i %in% c(3,4)){
              delta   <- 2
              min_val <- round(min(data$hpp_temperature_mcu,na.rm=T),0)-2
              max_val <- round(max(data$hpp_temperature_mcu,na.rm=T),0)+2
              min_val <- -10
              max_val <-  60
              if(i==3){
                figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_hpp_temperature_mcu_vs_RES_BP.pdf",sep="")
                ylabStr <- expression(paste("CO"[2]*" [ppm]"))
              }
              else{
                figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_hpp_temperature_mcu_vs_RES_BCP_BP.pdf",sep="")
                ylabStr <- expression(paste("CO"[2]*" [ppm] (BCP)"))
              }
              xlabStr <- "MCU_T [deg C]"
              xrange  <- c(-5,35)
            }
            
            nbp   <- round((max_val-min_val)/delta,0)
            n     <- as.integer(dim(data)[1])
            tmp   <- matrix(NA, nrow=n,ncol=nbp)
            
            bpcol <- rep(NA,nbp)
            
            if(dim(tmp)[1]!=dim(data)[1]){
              print(paste("AA"))
              stop()
            }
            if(dim(tmp)[2]!=nbp){
              print(paste("BB"))
              stop()
            }
            
            for(ii in (1:nbp)){
              
              if(i==1){
                id   <- which(!is.na(CO2_predicted-data$CO2) 
                              & !is.na(data$hpp_temperature_mcu_dT) 
                              & data$hpp_temperature_mcu_dT >= (min_val+(ii-1)*delta) 
                              & data$hpp_temperature_mcu_dT <  (min_val+ii*delta))
              }
              if(i==2){
                id   <- which(!is.na(CO2_predicted_BCP-data$CO2) 
                              & !is.na(data$hpp_temperature_mcu_dT) 
                              & data$hpp_temperature_mcu_dT >= (min_val+(ii-1)*delta) 
                              & data$hpp_temperature_mcu_dT <  (min_val+ii*delta))
              }
              if(i==3){
                id   <- which(!is.na(CO2_predicted-data$CO2) 
                              & !is.na(data$hpp_temperature_mcu) 
                              & data$hpp_temperature_mcu >= (min_val+(ii-1)*delta) 
                              & data$hpp_temperature_mcu <  (min_val+ii*delta))
              }
              if(i==4){
                id   <- which(!is.na(CO2_predicted_BCP-data$CO2) 
                              & !is.na(data$hpp_temperature_mcu) 
                              & data$hpp_temperature_mcu >= (min_val+(ii-1)*delta) 
                              & data$hpp_temperature_mcu <  (min_val+ii*delta))
              }
              
              n_id <- length(id)
              
              if(n_id>=10){
                if(i %in% c(1,3)){
                  tmp[1:n_id,ii] <- CO2_predicted[id]-data$CO2[id]
                }else{
                  tmp[1:n_id,ii] <- CO2_predicted_BCP[id]-data$CO2[id]
                }
                bpcol[ii]      <- "gray70"
              }
              if(n_id>100){
                bpcol[ii]      <- "slategray"
              }
            }
            
            def_par <- par()
            pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.1,0.1),mfrow=c(1,2))
            
            loc <- round(min_val+(1:nbp)*delta-delta/2,3)
            boxplot(tmp,at=loc,xlim=xrange,boxwex=(0.75*delta),col = bpcol,outline = F,cex.lab=1.25,cex.axis=1.25,xlab=xlabStr,ylab=ylabStr,xaxt="n")
            axis(1,cex.axis=1.25,cex.lab=1.25)
            lines(c(-1e9,1e9),c(0,0),col=2,lwd=2)
            lines(c(0,0),c(-1e9,1e9),col=2,lwd=2)
            
            #
            
            if(i%in%c(1,2)){
              seq_min <- round(min(data$hpp_temperature_mcu_dT,na.rm=T),1)-1
              seq_max <- round(max(data$hpp_temperature_mcu_dT,na.rm=T),1)+1
              hist(data$hpp_temperature_mcu_dT,seq(seq_min,seq_max,delta),xlim=xrange,col="slategray",xlab=xlabStr,cex.lab=1.25,cex.axis=1.25,main="")
            }
            if(i%in%c(3,4)){
              seq_min <- round(min(data$hpp_temperature_mcu,na.rm=T),0)-2
              seq_max <- round(max(data$hpp_temperature_mcu,na.rm=T),0)+2
              hist(data$hpp_temperature_mcu,seq(seq_min,seq_max,delta),xlim=xrange,col="slategray",xlab=xlabStr,cex.lab=1.25,cex.axis=1.25,main="")
            }
            
            dev.off()
            par(def_par)
            
            #
            
            rm(tmp)
            gc()
          }
          
          
          ## Change of HPP scale
          
          if(T){
            
            min_date_UTC <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC")
            max_data_UTC <- strptime("20181031000000","%Y%m%d%H%M%S",tz="UTC")
            n_weeks      <- floor(as.numeric(difftime(time1=max_data_UTC,time2=min_date_UTC,units="weeks",tz="UTC")))
            
            for(ds in c("PRED","PRED_BCP","HPP")){
              
              if(ds=="PRED_BCP" & !CV_mode%in%c(4,6,7)){
                next
              }
              
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_WEEKLY_SCALE_REF_",ds,".pdf",sep="")
              
              def_par <- par()
              pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
              par(mai=c(1,1,0.1,0.1),mfrow=c(1,2))
              
              
              for(ith_week in 0:(n_weeks-1)){
                
                date_now  <- min_date_UTC + ith_week*86400*7
                date_till <- date_now     + 86400*7
                
                id_all   <- which(data$date>=date_now & data$date<date_till & use4cal & data_test)
                n_id_all <- length(id_all)
                
                if(ds=="PRED"){
                  id_sel   <- which(data$date>=date_now & data$date<date_till & use4cal & data_test & abs(data$CO2-CO2_predicted)<25)
                }
                if(ds=="PRED_BCP"){
                  id_sel   <- which(data$date>=date_now & data$date<date_till & use4cal & data_test & abs(data$CO2-CO2_predicted_BCP)<25)
                }
                if(ds=="HPP"){
                  id_sel   <- which(data$date>=date_now & data$date<date_till & use4cal & data_test & abs(data$CO2-data$hpp_co2)<500)
                }
                
                n_id_sel <- length(id_sel)
                
                if(n_id_all<1*1440 | n_id_sel<1*1440){
                  next
                }
                
                #
                
                xx_all_mean <- mean(data$CO2[id_all])
                xx_all      <- data$CO2[id_all] - xx_all_mean
                xx_sel_mean <- mean(data$CO2[id_sel])
                xx_sel      <- data$CO2[id_sel] - xx_sel_mean
                
                CO2_min_sel    <- min(data$CO2[id_sel])
                CO2_mean_sel   <- mean(data$CO2[id_sel])
                CO2_median_sel <- median(data$CO2[id_sel])
                CO2_max_sel    <- max(data$CO2[id_sel])
                
                xlabString  <- expression(paste("Picarro CO"[2]*" [ppm] (mean subtracted)"))
                
                #
                
                if(ds=="PRED"){
                  yy_all_mean <- mean(CO2_predicted[id_all])
                  yy_all      <- CO2_predicted[id_all] - yy_all_mean
                  yy_sel_mean <- mean(CO2_predicted[id_sel])
                  yy_sel      <- CO2_predicted[id_sel] - yy_sel_mean
                  
                  ylabString <- expression(paste("CO"[2]*" predicted [ppm] (mean subtracted)"))
                }
                if(ds=="PRED_BCP"){
                  yy_all_mean <- mean(CO2_predicted_BCP[id_all])
                  yy_all      <- CO2_predicted_BCP[id_all] - xx_all_mean
                  yy_sel_mean <- mean(CO2_predicted_BCP[id_sel])
                  yy_sel      <- CO2_predicted_BCP[id_sel] - yy_sel_mean
                  
                  ylabString <- expression(paste("CO"[2]*" predicted [ppm] (mean subtracted)"))
                }
                if(ds=="HPP"){
                  yy_all_mean <- mean(data$hpp_co2[id_all])
                  yy_all      <- data$hpp_co2[id_all] - yy_all_mean
                  yy_sel_mean <- mean(data$hpp_co2[id_sel])
                  yy_sel      <- data$hpp_co2[id_sel] - yy_sel_mean
                  
                  ylabString <- expression(paste("HPP CO"[2]*" [ppm] (mean subtracted)"))
                }
                
                subString <- paste("Data:",strftime(date_now,"%Y-%m-%d",tz="UTC"),"-",strftime(date_till,"%Y-%m-%d",tz="UTC"))
                
                fit_all   <- lm(y~x, data.frame(x=xx_all,y=yy_all,stringsAsFactors = F))
                fit_sel   <- lm(y~x, data.frame(x=xx_sel,y=yy_sel,stringsAsFactors = F))
                
                a_all_acc <- sqrt(vcov(fit_all)[2,2])
                a_sel_acc <- sqrt(vcov(fit_sel)[2,2])
                
                statistic_scale <- rbind(statistic_scale,data.frame(SU             = SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],
                                                                    Sensormodel    = sensor_models[[ith_sensor_model]]$name,
                                                                    Variable       = ds,
                                                                    Date_UTC_from  = strftime(date_now, "%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                                    Date_UTC_to    = strftime(date_till,"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                                    MIN_CO2_sel    = CO2_min_sel,
                                                                    MEAN_CO2_sel   = CO2_mean_sel,
                                                                    MEDIAN_CO2_sel = CO2_median_sel,
                                                                    MAX_CO2_sel    = CO2_min_sel,
                                                                    N_all          = n_id_all,
                                                                    MEAN_ALL_X     = xx_all_mean,
                                                                    MEAN_ALL_Y     = yy_all_mean,
                                                                    DELTA_ALL_MEAN = xx_all_mean-yy_all_mean,
                                                                    slope_ALL      = fit_all$coefficients[2],
                                                                    slope_ALL_ACC  = a_all_acc,
                                                                    N_sel          = n_id_sel,
                                                                    MEAN_SEL_X     = xx_sel_mean,
                                                                    MEAN_SEL_Y     = yy_sel_mean,
                                                                    DELTA_SEL_MEAN = xx_sel_mean-yy_sel_mean,
                                                                    slope_SEL      = fit_sel$coefficients[2],
                                                                    slope_SEL_ACC  = a_sel_acc,
                                                                    stringsAsFactors=F))
                
                
                str_00 <- paste("a_all:",sprintf("%8.3f",fit_all$coefficients[2]))
                str_01 <- paste("   +- ",sprintf("%8.3f",a_all_acc))
                str_02 <- paste("N_all:",sprintf("%8.0f",n_id_all))
                str_03 <- paste("a_sel:",sprintf("%8.3f",fit_sel$coefficients[2]))
                str_04 <- paste("   +- ",sprintf("%8.3f",a_sel_acc))
                str_05 <- paste("N_sel:",sprintf("%8.0f",n_id_sel))
                str_06 <- paste("CO2 min:",sprintf("%5.1f",CO2_min_sel))
                str_07 <- paste("CO2 max:",sprintf("%5.1f",CO2_max_sel))
                
                plot(  xx_sel,yy_sel,col="black",cex=0.25,pch=16,xlab=xlabString,ylab=ylabString,sub=subString,cex.axis=1.25,cex.lab=1.25,xlim=c(-100,150),ylim=c(-100,150))
                lines(c(-1e9,1e9),c(-1e9,1e9),col=1,lwd=1,lty=5)
                lines(c(-1e9,1e9),c(0,0),col="gray",lwd=1,lty=1)
                lines(c(0,0),c(-1e9,1e9),col="gray",lwd=1,lty=1)
                lines(c(-1e9,1e9),c(-1e9,1e9)*fit_sel$coefficients[2]+fit_sel$coefficients[1],col=2,lwd=1)
                
                par(family="mono")
                legend("topleft",legend=c(str_00,str_01,str_02,str_03,str_04,str_05,"",str_06,str_07),bg="white")
                par(family="")
                
                #
                
                plot(  xx_sel,yy_sel-xx_sel,col="black",cex=0.25,pch=16,xlab=xlabString,ylab="y - x [ppm]",sub=subString,cex.axis=1.25,cex.lab=1.25,xlim=c(-100,150),ylim=c(-20,20))
                lines(c(-1e9,1e9),c(0,0),col="gray",lwd=1,lty=1)
                lines(c(0,0),c(-1e9,1e9),col="gray",lwd=1,lty=1)
                lines(c(-1e9,1e9),c(-1e9,1e9)*(fit_sel$coefficients[2]-1),col=2,lwd=1)
                
              }
              
              par(def_par)
              dev.off()
            }
          }
          
          ## Analysis BCP
          
          if(T & CV_mode%in%c(4,6,7)){
            tmp                   <- data[id_bottle_cal_periods,]
            tmp$CO2_predicted     <- CO2_predicted[id_bottle_cal_periods]
            tmp$CO2_predicted_BCP <- CO2_predicted_BCP[id_bottle_cal_periods]
            tmp$date              <- strftime(tmp$date,"%Y-%m-%d %H:%M:%S",tz="UTC")
            
            write.table(x = tmp,file = paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BCP.csv",sep=""),sep=";",col.names = T, row.names = F)
            
            rm(tmp)
            gc()
          }
        }
      }
    }
  }
}


# Write statistics into file

write.table(x = statistics,     file = paste(resultdir,"statistics_HPP.csv",sep=""),  sep=";",col.names = T, row.names = F)

write.table(x = statistic_scale,file = paste(resultdir,"statistics_scale.csv",sep=""),sep=";",col.names = T, row.names = F)