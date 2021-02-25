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
library(DBI)
require(RMySQL)
require(chron)
library(MASS)
library(mgcv)
library(data.table)
library(dplyr)

## source

source("/project/CarboSense/Software/CarboSenseUtilities/api-v1.3.r")
source("/project/CarboSense/Software/CarboSenseUtilities/CarboSenseFunctions.r")

## directories

resultdir <- "/project/CarboSense/Carbosense_Network/HPP_Calibration/HPP_CalibrationResults_DEFAULT/"

#Functions
dbFetch_UTC <- function(...) lubridate::with_tz(dbFetch(...), 'UTC')


## ARGUMENTS

args = commandArgs(trailingOnly=TRUE)

CV_mode   <- args[1]
resultdir <- args[2]

### ----------------------------------------------------------------------------------------------------------------------------

# Cross-validation (CV) mode
#
# CV_mode : 0 ->  no CV


if(!CV_mode%in%c(0:9)){
  stop()
}

### ----------------------------------------------------------------------------------------------------------------------------

## Decentlab DB information

DL_DB_domain      <- "swiss.co2.live"
DL_DB_apiKey      <- "eyJrIjoiSFd4bWJhczJjclpaUnpHeXluck1WYlJ0MkdINWhneFciLCJuIjoibWljaGFlbC5tdWVsbGVyQGVtcGEuY2giLCJpZCI6MX0="

### ----------------------------------------------------------------------------------------------------------------------------

SensorUnit_ID_2_cal   <- c(426:445)

n_SensorUnit_ID_2_cal <- length(SensorUnit_ID_2_cal)

### ----------------------------------------------------------------------------------------------------------------------------

ma <- function(x,n=10){filter(x,rep(1/n,n), sides=1)}

### ----------------------------------------------------------------------------------------------------------------------------

# sensor model versions

sensor_models   <- list()


#

# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_SHTT_H2O_PLF14d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I((((hpp_pressure-1013.25)/1013.25)))+ sht21_T + sht21_H2O + PLF")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_SHTT_H2O_DCAL_PLF14d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I((((hpp_pressure-1013.25)/1013.25)))+ sht21_T + sht21_H2O + DCAL + PLF")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_SHTT_H2O_PLF14d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + sht21_T + sht21_H2O + PLF")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_SHTT_H2O_DCAL_PLF14d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + sht21_T + sht21_H2O + DCAL + PLF")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_H2O_PLF14d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + sht21_H2O + PLF")

# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_H2O_DCAL_PLF07d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + sht21_H2O + DCAL + PLF")


sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_H2O_DCAL_PLF14d",
                                                 modelType="LM_IR",
                                                 formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + sht21_H2O + DCAL + PLF")


# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTTX_H2O_DCAL_PLF14d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + I((sht21_T+273.15)/hpp_logIR) + sht21_H2O + DCAL + PLF")


if(F){
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_DCAL_PLF14d",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + DCAL + PLF")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_H2O_PLF14d",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + sht21_H2O + PLF")
  
  sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_PLF14d",
                                                   modelType="LM_IR",
                                                   formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + PLF")
  
}

# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_H2O_DCAL2_PLF14d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + sht21_H2O + I(DCAL*CO2_ppm) + PLF")
# 
# 
# #
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T")
# 
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_PLF07d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + PLF")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_PLF14d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + PLF")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_PLF21d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + PLF")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_DCAL_PLF14d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + DCAL + PLF")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_DCAL2_PLF14d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + I(DCAL*CO2_ppm) + PLF")
# 
# 
# #
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_H2O",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_H2O")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_H2O_PLF07d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_H2O + PLF")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_H2O_PLF14d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_H2O + PLF")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_H2O_PLF21d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_H2O + PLF")
# 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_H2O_PLF28d",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_H2O + PLF")
# 
# 
# #
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_PLF07d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + PLF")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_PLF14d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + PLF")
# 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_PLF21d",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + PLF")
# 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_PLF28d",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ -1 + hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + PLF")
# 
# 
# 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))")
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_HPPT_H2O",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + hpp_temperature_mcu + sht21_H2O")
# # 
# # 
# sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_HPPT_H2O_DRIFT",
#                                                  modelType="LM_IR",
#                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + hpp_temperature_mcu + sht21_H2O + days")
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_HPPT_DRIFT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + hpp_temperature_mcu + days")
# # 
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_DRIFT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + days")
# # 
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_DRIFT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + days")
# # 

# if(CV_mode==0){
#   sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_H2O_DRIFT",
#                                                    modelType="LM_IR",
#                                                    formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + sht21_H2O + days")
# }
# 
# if(CV_mode==0){
#   sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_SHTT_DRIFT",
#                                                    modelType="LM_IR",
#                                                    formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir))  + sht21_T + days")
# }

# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_H2O_DRIFT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + sht21_H2O + days")
# # 
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_p_SHTT_H2O_DRIFT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)))  + sht21_T + sht21_H2O + days")
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_SHTT_H2O_DRIFT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir)  + sht21_T + sht21_H2O + days")
# 
# 
# #####################
# 
# 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_GAM_PIR2_SHTT_SHTTdT",
# # #                                                  modelType="GAM",
# # #                                                  formula="CO2~hpp_logIR+div_hpp_ir+p2_div_ir+p_div_ir+sht21_T+s(sht21_T_dT)")
# # # 
# # # 
# # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_GAM_PIR2_HPPT_sHPPdT",
# # #                                                  modelType="GAM",
# # #                                                  formula="CO2~hpp_logIR+div_hpp_ir+p2_div_ir+p_div_ir+hpp_temperature_mcu+s(hpp_temperature_mcu_dT)")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_GAM_PIR2_sHPPT_sHPPdT",
# # #                                                  modelType="GAM",
# # #                                                  formula="CO2~hpp_logIR+div_hpp_ir+p2_div_ir+p_div_ir+s(hpp_temperature_mcu)+s(hpp_temperature_mcu_dT)")
# # 
# # 
# # # 
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_GAM_PIR2_HPPT_HPPdT_H2O",
# # #                                                  modelType="GAM",
# # #                                                  formula="CO2~hpp_logIR+div_hpp_ir+p2_div_ir+p_div_ir+hpp_temperature_mcu+s(hpp_temperature_mcu_dT)+sht21_H2O")
# # # 
# # # 
# # 
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_HPP_CO2",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_co2")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir)")
# # # 
# # #
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP2_pIR2",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I(1/hpp_ir^2) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir))")
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir))")
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_HPPT_H2O",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + sht21_H2O")
# # 
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_HPPT_H2O_DRIFT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + sht21_H2O + days")
# # 
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_HPPT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu")
# # 
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_H2O",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_H2O")
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_H2O_DRIFT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_H2O + days")
# # 
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_HPPT_DRIFT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + days")
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR_DRIFT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + days")
# # 
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir))")
# # # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_H2O",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_H2O")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_H2O_SHTT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_H2O + sht21_T")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_H2O_SHTTdT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_H2O + sht21_T_dT + I(sht21_T_dT^2) + I(sht21_T_dT^3) + I(sht21_T_dT^4)")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_H2O_SHTT_SHTTdT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_H2O + sht21_T + sht21_T_dT + I(sht21_T_dT^2) + I(sht21_T_dT^3) + I(sht21_T_dT^4)")
# # # 
# # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_SHTT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_T")
# # 
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu")
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT_H2O",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + sht21_H2O")
# # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT_AH",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + sht21_AH")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_AH",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_AH")
# # 
# # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT_EXT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(hpp_logIR*hpp_temperature_mcu) + I(hpp_temperature_mcu/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)*hpp_temperature_mcu) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)*hpp_temperature_mcu)")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT_EXT_HPPdT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(hpp_logIR*hpp_temperature_mcu_dT) + I(hpp_temperature_mcu_dT/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)*hpp_temperature_mcu_dT) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)*hpp_temperature_mcu_dT)")
# # 
# # 
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT_LOGHPPT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu+I(hpp_logIR*(273.15+hpp_temperature_mcu))")
# # 
# # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_SHTT_SHTTsl",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_T + slope_sht21_T + I(slope_sht21_T^2)+I(slope_sht21_T^3)")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT_HPPTsl",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + slope_hpp_temperature_mcu + I(slope_hpp_temperature_mcu^2)+I(slope_hpp_temperature_mcu^3)")
# # 
# # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_SHTTdT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(sht21_T_dT) + I(sht21_T_dT^2) + I(sht21_T_dT^3) + I(sht21_T_dT^4)")
# # 
# # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_SHTT_SHTTdT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_T + I(sht21_T_dT) + I(sht21_T_dT^2) + I(sht21_T_dT^3) + I(sht21_T_dT^4)")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT_HPPdT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + hpp_temperature_mcu_dT")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_SHTT_DRIFT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_T + days")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT_DRIFT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + days")
# # 
# # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT_H2O_DRIFT",
# #                                                  modelType="LM_IR",
# #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + sht21_H2O + days")
# # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT2_H2O_DRIFT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + I(hpp_temperature_mcu^2) + sht21_H2O + days")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPdT_H2O_DRIFT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + hpp_temperature_mcu_dT + sht21_H2O + days")
# # 
# # 
# # # 
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_SHTT_SHTTdT_DRIFT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_T + I(sht21_T_dT) + I(sht21_T_dT^2) + I(sht21_T_dT^3) + I(sht21_T_dT^4) + days")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT_HPPdT_DRIFT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + hpp_temperature_mcu_dT + I(hpp_temperature_mcu_dT^2) + I(hpp_temperature_mcu_dT^3) + I(hpp_temperature_mcu_dT^4) + days")
# # 
# # 
# # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_HPPT_SHTTdT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + hpp_temperature_mcu + I(sht21_T_dT) + I(sht21_T_dT^2) + I(sht21_T_dT^3) + I(sht21_T_dT^4)")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_SHTT_HPPdT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + sht21_T + hpp_temperature_mcu_dT + I(hpp_temperature_mcu_dT^2) + I(hpp_temperature_mcu_dT^3) + I(hpp_temperature_mcu_dT^4)")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIR2_SHTT_HPPT_SHTTdT_HPPdT",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) + I(sht21_T+hpp_temperature_mcu) + I(sht21_T_dT+hpp_temperature_mcu_dT) + I((sht21_T_dT+hpp_temperature_mcu_dT)^2) + I((sht21_T_dT+hpp_temperature_mcu_dT)^3) + I((sht21_T_dT+hpp_temperature_mcu_dT)^4)")
# # 
# # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIRTEST",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2)) + I((((hpp_pressure-1013.25)/1013.25)^2*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir))")
# # # 
# # # sensor_models[[length(sensor_models)+1]] <- list(name="HPP_CO2_IR_CP1_pIRTEST_H2O",
# # #                                                  modelType="LM_IR",
# # #                                                  formula="CO2 ~ hpp_logIR + I(1/hpp_ir) + I((((hpp_pressure-1013.25)/1013.25))) + I((((hpp_pressure-1013.25)/1013.25)*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)/hpp_ir)) + I((((hpp_pressure-1013.25)/1013.25)^2)) + I((((hpp_pressure-1013.25)/1013.25)^2*hpp_logIR)) + I((((hpp_pressure-1013.25)/1013.25)^2/hpp_ir)) +  I(H2O)")
# 


# tmp                <- sensor_models
# sensor_models      <- list()
# sensor_models[[1]] <- tmp[[1]]

### ----------------------------------------------------------------------------------------------------------------------------

statistics          <- NULL
statistic_scale     <- NULL
statistics_BCP      <- NULL
statistics_pressure <- NULL
statistics_H2O      <- NULL

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
  con <-carboutil::get_conn( group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tbl_calibration <- dbFetch_UTC(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
  tbl_calibration$Date_UTC_from <- strptime(tbl_calibration$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_calibration$Date_UTC_to   <- strptime(tbl_calibration$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
  
  print(tbl_calibration)
  # reference gas cylinder information
  
  query_str       <- paste("SELECT * FROM RefGasCylinder_Deployment where SensorUnit_ID=",SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],";",sep="")
  drv             <- dbDriver("MySQL")
  con <-carboutil::get_conn( group="CarboSense_MySQL")
  res             <- dbSendQuery(con, query_str)
  tbl_refGasCylDepl <- dbFetch_UTC(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  
 
  #
  
  if(dim(tbl_refGasCylDepl)[1]>=1){
    
    tbl_refGasCylDepl$Date_UTC_from <- strptime(tbl_refGasCylDepl$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
    tbl_refGasCylDepl$Date_UTC_to   <- strptime(tbl_refGasCylDepl$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
    
    tbl_refGasCylDepl$CO2           <- NA
    
    for(ith_refGasCylDepl in 1:dim(tbl_refGasCylDepl)[1]){
      
      query_str       <- paste("SELECT * FROM RefGasCylinder where CylinderID='",tbl_refGasCylDepl$CylinderID[ith_refGasCylDepl],"' and Date_UTC_from <= '",strftime(tbl_refGasCylDepl$Date_UTC_from[ith_refGasCylDepl],"%Y-%m-%d %H:%M:%S",tz="UTC"),"' and Date_UTC_to >= '",strftime(tbl_refGasCylDepl$Date_UTC_to[ith_refGasCylDepl],"%Y-%m-%d %H:%M:%S",tz="UTC"),"';",sep="")
      drv             <- dbDriver("MySQL")
      con <-carboutil::get_conn( group="CarboSense_MySQL")
      res             <- dbSendQuery(con, query_str)
      tbl             <- dbFetch_UTC(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
    
      if(dim(tbl)[1]!=1){
        print(query_str)
        print(tbl)
        stop()
      }
      
      tbl_refGasCylDepl$CO2[ith_refGasCylDepl] <- tbl$CO2[1]
    }
  }else{
    tbl_refGasCylDepl <- NULL 
  }
  

  
  # sensor information
  
  query_str   <- paste("SELECT * FROM Sensors where SensorUnit_ID=",SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal]," and Type='HPP';",sep="")
  drv         <- dbDriver("MySQL")
  con <-carboutil::get_conn( group="CarboSense_MySQL")
  res         <- dbSendQuery(con, query_str)
  tbl_sensors <- dbFetch(res, n=-1)
  dbClearResult(res)
  dbDisconnect(con)
  print(tbl_sensors)
  tbl_sensors$Date_UTC_from <- strptime(tbl_sensors$Date_UTC_from,"%Y-%m-%d %H:%M:%S",tz="UTC")
  tbl_sensors$Date_UTC_to   <- strptime(tbl_sensors$Date_UTC_to,  "%Y-%m-%d %H:%M:%S",tz="UTC")
  
  # Combination of information from Sensors / Calibration
  
  sensor_calibration_info <- NULL
  
  for(ith_sensor in 1:dim(tbl_sensors)[1]){
    
    for(ith_cal in 1:dim(tbl_calibration)[1]){

      print(tbl_calibration[ith_cal,])
      print(tbl_sensors[ith_sensor,])
      print(ith_cal)
      print(ith_sensor)
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
  
  #Get the info with a single SQL query
    info_q <- 
    "
    SELECT 
    cal.SensorUnit_ID,
    Type,
    Serialnumber,
    GREATEST(cal.Date_UTC_from, sens.Date_UTC_from) AS Date_UTC_from,
    GREATEST(cal.Date_UTC_to, sens.Date_UTC_to) AS Date_UTC_to,
    LocationName,
    DBTableNameRefData,
    CalMode
  FROM Sensors AS sens
    JOIN Calibration AS cal
    ON  sens.SensorUnit_ID = cal.SensorUnit_ID
  WHERE sens.SensorUnit_ID={ith_SensorUnit_ID_2_cal} AND Type='HPP' AND CalMode in (1,2,3)
  AND NOT (cal.Date_UTC_to < sens.Date_UTC_from OR cal.Date_UTC_from > sens.Date_UTC_to)
  "
  con <-carboutil::get_conn( group="CarboSense_MySQL")
  dt <- lubridate::with_tz(dplyr::tbl(con, sql(glue::glue_sql(info_q))) %>% collect(), 'UTC')
  print("#######################")
  print(dt)
  print(sensor_calibration_info)
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
    
    #
    
    if(sensors2cal[ith_sensor2cal]==427){
      next
    }
    
    ## -------------------------------
    
    # Import of sensor data
    
    sensor_data <- NULL
    
    query_str   <- paste("SELECT * FROM SensorExclusionPeriods where SensorUnit_ID=",SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal]," and Serialnumber='",sensors2cal[ith_sensor2cal],"' and Type='HPP';",sep="")
    drv         <- dbDriver("MySQL")
    con <-carboutil::get_conn( group="CarboSense_MySQL")
    res         <- dbSendQuery(con, query_str)
    SEP         <- dbFetch_UTC(res, n=-1)
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
                    sensor = "/calibration|senseair-hpp-co2-filtered|senseair-hpp-ir-signal|senseair-hpp-pressure-filtered|senseair-hpp-status|senseair-hpp-temperature-mcu|sensirion-sht21-humidity|sensirion-sht21-temperature/",
                    channel = "//",
                    aggFunc = "",
                    aggInterval = "",
                    doCast = FALSE,
                    timezone = 'UTC')
      
      
      # require(reshape)
      # tmp0$time <- round(as.numeric(difftime(time1=tmp0$time,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),units="secs",tz="UTC"))*1e6)
      # tmp0      <- data.frame(reshape::cast(tmp0, time ~ series, fun.aggregate = mean), check.names = FALSE)
      # tmp0$time <- strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC") + tmp0$time/1e6
      
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
      cn            <- gsub(pattern = "calibration",                       replacement = "valve",                    x=cn)
      cn            <- gsub(pattern = "senseair_hpp_co2_filtered",         replacement = "hpp_co2",                  x=cn)
      cn            <- gsub(pattern = "senseair_hpp_ir_signal",            replacement = "hpp_ir",                   x=cn)
      cn            <- gsub(pattern = "senseair_hpp_lpl_signal",           replacement = "hpp_lpl",                  x=cn)
      cn            <- gsub(pattern = "senseair_hpp_ntc5_diff_temp",       replacement = "hpp_ntc5_dT",              x=cn)
      cn            <- gsub(pattern = "senseair_hpp_ntc6_se_temp",         replacement = "hpp_ntc6_T",               x=cn)
      cn            <- gsub(pattern = "senseair_hpp_status",               replacement = "hpp_status",               x=cn)
      cn            <- gsub(pattern = "senseair_hpp_pressure_filtered",    replacement = "hpp_pressure",             x=cn)
      cn            <- gsub(pattern = "senseair_hpp_temperature_detector", replacement = "hpp_temperature_detector", x=cn)
      cn            <- gsub(pattern = "senseair_hpp_temperature_mcu",      replacement = "hpp_temperature_mcu",      x=cn)
      cn            <- gsub(pattern = "sensirion_sht21_humidity",          replacement = "sht21_RH",                 x=cn)
      cn            <- gsub(pattern = "sensirion_sht21_temperature",       replacement = "sht21_T",                  x=cn)
      cn            <- gsub(pattern = "time",                              replacement = "date",                     x=cn)
      
      colnames(tmp0) <- cn
      
      
      # Ensure that all required columns are defined
      
      cn_required   <- c("date","battery","valve","hpp_co2","hpp_ir","hpp_lpl","hpp_pressure","hpp_status","hpp_temperature_detector","hpp_temperature_mcu","sht21_RH","sht21_T")
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
      
      # Force time to full preceding minute / check data imported from Influx-DB (no duplicates)
      
      tmp           <- tmp[order(tmp$date),]
      tmp$date      <- as.POSIXct(tmp$date)
      
      tmp$secs      <- as.numeric(difftime(time1=tmp$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
      tmp           <- tmp[c(T,diff(tmp$secs)>50),]
      
      tmp$date      <- strptime(strftime(tmp$date,"%Y%m%d%H%M00",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC")
      tmp$secs      <- as.numeric(difftime(time1=tmp$date,time2=strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
      
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
            tmp$hpp_co2[id]      <- NA
            tmp$hpp_pressure[id] <- NA
            tmp$valve[id]        <- 0
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
        query_str <- paste("SELECT timestamp,CO2,CO2_MIN,CO2_MAX,CO2_F,CO2_DRY_CAL as CO2_DRY,CO2_DRY_F,H2O,H2O_F,T,T_F,RH,RH_F,pressure,pressure_F,CO2_10MIN_AV,CO2_DRY_CAL_10MIN_AV as CO2_DRY_10MIN_AV,H2O_10MIN_AV,T_10MIN_AV,RH_10MIN_AV, pressure_10MIN_AV FROM ",sensor_calibration_info$DBTableNameRefData[id_cal_periods[ith_id_cal_period]]," where timestamp >= ",sensor_calibration_info$timestamp_from[id_cal_periods[ith_id_cal_period]]," and timestamp < ",sensor_calibration_info$timestamp_to[id_cal_periods[ith_id_cal_period]],";",sep="")
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
      con <-carboutil::get_conn( group="CarboSense_MySQL")
      res       <- dbSendQuery(con, query_str)
      tmp       <- dbFetch_UTC(res, n=-1)
      dbClearResult(res)
      dbDisconnect(con)
      
      if(dim(tmp)[1]>0){
        
        if(sensor_calibration_info$CalMode[id_cal_periods[ith_id_cal_period]]==1){
          
          tmp$CO2_DRY_10MIN_AV <- -999
          #tmp$CO2_DRY          <- -999
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
    
    # Tests on 2018-05-07
    
    if(SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal] == 426){
      id <- which(ref_data$date>=strptime("2018-05-07 12:11:00","%Y-%m-%d %H:%M:%S",tz="UTC") & ref_data$date<strptime("2018-05-07 12:40:00","%Y-%m-%d %H:%M:%S",tz="UTC"))
      if(length(id)>0){
        ref_data$CO2[id]   <- 433.29
        ref_data$CO2_F[id] <- 1
        ref_data$H2O[id]   <- 0
        ref_data$H2O_F[id] <- 1
      }
      id <- which(ref_data$date>=strptime("2018-05-07 12:40:00","%Y-%m-%d %H:%M:%S",tz="UTC") & ref_data$date<=strptime("2018-05-07 12:45:00","%Y-%m-%d %H:%M:%S",tz="UTC"))
      if(length(id)>0){
        ref_data$CO2[id]   <- -999
        ref_data$CO2_F[id] <- 0
        ref_data$H2O[id]   <- -999
        ref_data$H2O_F[id] <- 0
      }
      id <- which(ref_data$date>=strptime("2018-06-17 14:00:00","%Y-%m-%d %H:%M:%S",tz="UTC") & ref_data$date<=strptime("2018-06-17 14:30:00","%Y-%m-%d %H:%M:%S",tz="UTC"))
      if(length(id)>0){
        ref_data$CO2[id]   <- -999
        ref_data$CO2_F[id] <- 0
        ref_data$H2O[id]   <- -999
        ref_data$H2O_F[id] <- 0
      }
    }
    
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
    
    id_set_to_NA <- which(ref_data$CO2 == -999 | is.na(ref_data$CO2))
    if(length(id_set_to_NA)>0){
      ref_data$H2O[id_set_to_NA]   <- NA
      ref_data$H2O_F[id_set_to_NA] <- 0
    }
    
    id_set_to_NA <- which(ref_data$pressure < 500)
    if(length(id_set_to_NA)>0){
      ref_data$pressure[id_set_to_NA] <- NA
    }
    
    ## METHAN
    
    # ch4_data           <- read.table("/home/muem/mnt/Win_G/503_Themen/Immissionen/muem/DUE/DUE CH4 Juni18.csv",skip=4,sep=";")
    # colnames(ch4_data) <- c("date","CH4")
    # ch4_data$date      <- strptime(ch4_data$date,"%d.%m.%Y %H:%M",tz="UTC")-600-3600
    # 
    # ch4_data$CH4[which(ch4_data$CH4 == -999 | ch4_data$CH4 == "" | is.na(ch4_data$CH4))] <- NA
    # 
    # ch4_1min           <- approx(x=ch4_data$date+300,y=ch4_data$CH4,xout=ref_data$date,rule=1)
    # 
    # ref_data$CH4       <- ch4_1min$y
    # ref_data$CH4_F     <- 0
    # ref_data$CH4_F[!is.na(ref_data$CH4)] <- 1
    # 
    # rm(ch4_data,ch4_1min)
    # gc()
    
    
    ## -------------------------------
    
    
    # Merge sensor and reference data into data.frame "data"
    
    sensor_data$date <- as.POSIXct(sensor_data$date)
    ref_data$date    <- as.POSIXct(ref_data$date)
    
    data             <- merge(sensor_data,ref_data)
    
    # data             <- data[which(data$date>strptime("20170725000000","%Y%m%d%H%M%S",tz="UTC")),]
    
    # data             <- data[which(data$date>strptime("20170725000000","%Y%m%d%H%M%S",tz="UTC") & data$valve==0),]
    
    # data             <- data[which(data$date>strptime("2018061300000000","%Y%m%d%H%M%S",tz="UTC")),]
    
    ## -------------------------------
    
    ## pressure sensor calibration
    
    if(T){
      
      id_ok <- which(!is.na(data$pressure)
                     & !is.na(data$hpp_pressure)
                     & !is.na(data$hpp_ir)
                     & !is.na(data$CO2))
      
      n_id_ok <- length(id_ok)
      
      if(length(id_ok)>0){
        fit_pressure <- lm(y~x, data.frame(x=data$hpp_pressure[id_ok],
                                           y=data$pressure[id_ok],
                                           stringsAsFactors = F))
        
        hpp_pressure_predict <- predict(fit_pressure,newdata=data.frame(x=data$hpp_pressure[id_ok],
                                                                        y=data$pressure[id_ok],
                                                                        stringsAsFactors = F))
        
        RMSE_FIT    <- sqrt(sum((hpp_pressure_predict     - data$pressure[id_ok])^2)/n_id_ok)
        RMSE_RAW    <- sqrt(sum((data$hpp_pressure[id_ok] - data$pressure[id_ok])^2)/n_id_ok)
        
        corCoef_FIT <- cor(x=hpp_pressure_predict,    y=data$pressure[id_ok],method="pearson",use="complete.obs")
        corCoef_RAW <- cor(x=data$hpp_pressure[id_ok],y=data$pressure[id_ok],method="pearson",use="complete.obs")
        
        
        statistics_pressure <- rbind(statistics_pressure,data.frame(SensorUnit_ID = SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],
                                                                    Sensor        = sensors2cal[ith_sensor2cal],
                                                                    Date_UTC_from = strftime(min(data$date[id_ok]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                                    Date_UTC_to   = strftime(max(data$date[id_ok]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                                    N             = n_id_ok,
                                                                    slope         = fit_pressure$coefficients[2],
                                                                    intercept     = fit_pressure$coefficients[1],
                                                                    RMSE_FIT      = RMSE_FIT,
                                                                    corCoef_FIT   = corCoef_FIT,
                                                                    RMSE_RAW      = RMSE_RAW,
                                                                    corCoef_RAW   = corCoef_RAW,
                                                                    MIN_P_REF     = min(data$pressure[id_ok]),
                                                                    MAX_P_REF     = max(data$pressure[id_ok]),
                                                                    MIN_P_SEN     = min(data$hpp_pressure[id_ok]),
                                                                    MAX_P_SEN     = max(data$hpp_pressure[id_ok]),
                                                                    CalMode_01    = any(data$CalMode[id_ok]==1),
                                                                    CalMode_02    = any(data$CalMode[id_ok]==2),
                                                                    CalMode_03    = any(data$CalMode[id_ok]==3),
                                                                    stringsAsFactors = F))
        
        #
        
        if(T){
          data$hpp_pressure <- fit_pressure$coefficients[1] + fit_pressure$coefficients[2] * data$hpp_pressure
        }
        
        
        # write sensor model parameters into database
        
        if(T & (CV_mode%in%c(0,4,7))) {
          
          query_str       <- paste("DELETE FROM `CalibrationParameters` WHERE Type = 'HPP_pressure' and Serialnumber = '",sensors2cal[ith_sensor2cal],"' and Mode = ",CV_mode," and CalibrationModelName = 'HPP_pressure_linear';",sep="");
          
          drv             <- dbDriver("MySQL")
          con <-carboutil::get_conn( group="CarboSense_MySQL")
          res             <- dbSendQuery(con, query_str)
          tbl_calibration <- dbFetch_UTC(res, n=-1)
          dbClearResult(res)
          dbDisconnect(con)
          
          #
          
          INDEX <- RMSE_FIT
          
          #
          
          n_par         <- 2
          parameters2DB <- c(fit_pressure$coefficients[1],fit_pressure$coefficients[2],rep(0,148))
          
          #
          
          Data_timestamp_from  <- min(sensor_calibration_info$timestamp_from[id_cal_periods])
          Data_timestamp_to    <- max(sensor_calibration_info$timestamp_to[id_cal_periods])
          Computation_date_UTC <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S",tz="UTC")
          
          query_str       <- paste("INSERT INTO `CalibrationParameters`(`Type`, `Serialnumber`, `CalibrationModelName`, `Data_timestamp_from`, `Data_timestamp_to`,`Mode`,`Index`, `N_PAR`, `Computation_date_UTC`, `PAR_00`, `PAR_01`, `PAR_02`, `PAR_03`, `PAR_04`, `PAR_05`, `PAR_06`, `PAR_07`, `PAR_08`, `PAR_09`, `PAR_10`, `PAR_11`, `PAR_12`, `PAR_13`, `PAR_14`, `PAR_15`, `PAR_16`, `PAR_17`, `PAR_18`, `PAR_19`,`PAR_20`, `PAR_21`, `PAR_22`, `PAR_23`, `PAR_24`, `PAR_25`, `PAR_26`, `PAR_27`, `PAR_28`, `PAR_29`,`PAR_30`, `PAR_31`, `PAR_32`, `PAR_33`, `PAR_34`, `PAR_35`, `PAR_36`, `PAR_37`, `PAR_38`, `PAR_39`,`PAR_40`, `PAR_41`, `PAR_42`, `PAR_43`, `PAR_44`, `PAR_45`, `PAR_46`, `PAR_47`, `PAR_48`, `PAR_49`,`PAR_50`, `PAR_51`, `PAR_52`, `PAR_53`, `PAR_54`, `PAR_55`, `PAR_56`, `PAR_57`, `PAR_58`, `PAR_59`,`PAR_60`, `PAR_61`, `PAR_62`, `PAR_63`, `PAR_64`, `PAR_65`, `PAR_66`, `PAR_67`, `PAR_68`, `PAR_69`,`PAR_70`, `PAR_71`, `PAR_72`, `PAR_73`, `PAR_74`, `PAR_75`, `PAR_76`, `PAR_77`, `PAR_78`, `PAR_79`,`PAR_80`, `PAR_81`, `PAR_82`, `PAR_83`, `PAR_84`, `PAR_85`, `PAR_86`, `PAR_87`, `PAR_88`, `PAR_89`,`PAR_90`, `PAR_91`, `PAR_92`, `PAR_93`, `PAR_94`, `PAR_95`, `PAR_96`, `PAR_97`, `PAR_98`, `PAR_99`, `PAR_100`,`PAR_101`,`PAR_102`,`PAR_103`,`PAR_104`,`PAR_105`,`PAR_106`,`PAR_107`,`PAR_108`,`PAR_109`,`PAR_110`,`PAR_111`,`PAR_112`,`PAR_113`,`PAR_114`,`PAR_115`,`PAR_116`,`PAR_117`,`PAR_118`,`PAR_119`,`PAR_120`,`PAR_121`,`PAR_122`,`PAR_123`,`PAR_124`,`PAR_125`,`PAR_126`,`PAR_127`,`PAR_128`,`PAR_129`,`PAR_130`,`PAR_131`,`PAR_132`,`PAR_133`,`PAR_134`,`PAR_135`,`PAR_136`,`PAR_137`,`PAR_138`,`PAR_139`,`PAR_140`,`PAR_141`,`PAR_142`,`PAR_143`,`PAR_144`,`PAR_145`,`PAR_146`,`PAR_147`,`PAR_148`,`PAR_149`,`SHT21_RH_max`,`SHT21_T_DP_min`) ",sep="")
          query_str       <- paste(query_str, paste("VALUES ('HPP_pressure','",sensors2cal[ith_sensor2cal],"','HPP_pressure_linear',",Data_timestamp_from,",",Data_timestamp_to,",",CV_mode,",",ceiling(INDEX),",",n_par,",'",Computation_date_UTC,"',",
                                                    parameters2DB[01],",",parameters2DB[02],",",parameters2DB[03],",",parameters2DB[04],",",parameters2DB[05],",",parameters2DB[06],",",parameters2DB[07],",",parameters2DB[08],",",parameters2DB[09],",",parameters2DB[10],",",
                                                    parameters2DB[11],",",parameters2DB[12],",",parameters2DB[13],",",parameters2DB[14],",",parameters2DB[15],",",parameters2DB[16],",",parameters2DB[17],",",parameters2DB[18],",",parameters2DB[19],",",parameters2DB[20],",",
                                                    parameters2DB[21],",",parameters2DB[22],",",parameters2DB[23],",",parameters2DB[24],",",parameters2DB[25],",",parameters2DB[26],",",parameters2DB[27],",",parameters2DB[28],",",parameters2DB[29],",",parameters2DB[30],",",
                                                    parameters2DB[31],",",parameters2DB[32],",",parameters2DB[33],",",parameters2DB[34],",",parameters2DB[35],",",parameters2DB[36],",",parameters2DB[37],",",parameters2DB[38],",",parameters2DB[39],",",parameters2DB[40],",",
                                                    parameters2DB[41],",",parameters2DB[42],",",parameters2DB[43],",",parameters2DB[44],",",parameters2DB[45],",",parameters2DB[46],",",parameters2DB[47],",",parameters2DB[48],",",parameters2DB[49],",",parameters2DB[50],",",
                                                    parameters2DB[51],",",parameters2DB[52],",",parameters2DB[53],",",parameters2DB[54],",",parameters2DB[55],",",parameters2DB[56],",",parameters2DB[57],",",parameters2DB[58],",",parameters2DB[59],",",parameters2DB[60],",",
                                                    parameters2DB[61],",",parameters2DB[62],",",parameters2DB[63],",",parameters2DB[64],",",parameters2DB[65],",",parameters2DB[66],",",parameters2DB[67],",",parameters2DB[68],",",parameters2DB[69],",",parameters2DB[70],",",
                                                    parameters2DB[71],",",parameters2DB[72],",",parameters2DB[73],",",parameters2DB[74],",",parameters2DB[75],",",parameters2DB[76],",",parameters2DB[77],",",parameters2DB[78],",",parameters2DB[79],",",parameters2DB[80],",",
                                                    parameters2DB[81],",",parameters2DB[82],",",parameters2DB[83],",",parameters2DB[84],",",parameters2DB[85],",",parameters2DB[86],",",parameters2DB[87],",",parameters2DB[88],",",parameters2DB[89],",",parameters2DB[90],",",
                                                    parameters2DB[91],",",parameters2DB[92],",",parameters2DB[93],",",parameters2DB[94],",",parameters2DB[95],",",parameters2DB[96],",",parameters2DB[97],",",parameters2DB[98],",",parameters2DB[99],",",parameters2DB[100],",",
                                                    parameters2DB[101],",",parameters2DB[102],",",parameters2DB[103],",",parameters2DB[104],",",parameters2DB[105],",",parameters2DB[106],",",parameters2DB[107],",",parameters2DB[108],",",parameters2DB[109],",",parameters2DB[110],",",
                                                    parameters2DB[111],",",parameters2DB[112],",",parameters2DB[113],",",parameters2DB[114],",",parameters2DB[115],",",parameters2DB[116],",",parameters2DB[117],",",parameters2DB[118],",",parameters2DB[119],",",parameters2DB[120],",",
                                                    parameters2DB[121],",",parameters2DB[122],",",parameters2DB[123],",",parameters2DB[124],",",parameters2DB[125],",",parameters2DB[126],",",parameters2DB[127],",",parameters2DB[128],",",parameters2DB[129],",",parameters2DB[130],",",
                                                    parameters2DB[131],",",parameters2DB[132],",",parameters2DB[133],",",parameters2DB[134],",",parameters2DB[135],",",parameters2DB[136],",",parameters2DB[137],",",parameters2DB[138],",",parameters2DB[139],",",parameters2DB[140],",",
                                                    parameters2DB[141],",",parameters2DB[142],",",parameters2DB[143],",",parameters2DB[144],",",parameters2DB[145],",",parameters2DB[146],",",parameters2DB[147],",",parameters2DB[148],",",parameters2DB[149],",",parameters2DB[150],",-999,-999);",sep=""))
          
          
          
          drv             <- dbDriver("MySQL")
          con <-carboutil::get_conn( group="CarboSense_MySQL")
          res             <- dbSendQuery(con, query_str)
          tbl_calibration <- dbFetch_UTC(res, n=-1)
          dbClearResult(res)
          dbDisconnect(con)
          
          rm(CAL_VALID_time_UTC_from,CAL_VALID_time_UTC_to,INDEX,n_par,parameters2DB)
          gc()
          
        }
        
        #
        
        rm(fit_pressure,hpp_pressure_predict,RMSE_FIT,RMSE_RAW,corCoef_FIT,corCoef_RAW)
        gc()
      }
    }
    
    ## -------------------------------
    
    # Apply corrections to CO2 reference value
    
    data$CO2 <- data$CO2_DRY * (1 - data$H2O/100)
    
    ##
    
    data$CO2_ppm <- data$CO2
    
    # pressure
    
    data$CO2 <- data$CO2 * (data$hpp_pressure / 1013.25)
    
    
    ## -------------------------------
    
    # Additional columns in data.frame "data"
    
    # HPP
    
    data$hpp_logIR <- -log(data$hpp_ir)
    
    # timestamp (continuous time)
    data$timestamp <- as.numeric(difftime(time1=data$date,strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="secs"))
    
    # days (continuous time)
    data$days      <- as.numeric(difftime(time1=data$date,strptime("20170101000000","%Y%m%d%H%M%S",tz="UTC"),tz="UTC",units="days"))
    
    # hour of day
    data$hour      <- as.numeric(strftime(data$date,"%H",tz="UTC"))
    
    # minutes of day
    data$minutes   <- as.numeric(strftime(data$date,"%M",tz="UTC"))
    
    # Offset for pressure calibration
    
    data$PCalOffset <- as.numeric(data$CalMode==3)
    
    # Offset for pressure calibration
    
    data$DCAL <- as.numeric(data$valve==1)
    
    # GAM
    
    data$div_hpp_ir  <- 1/data$hpp_ir
    data$p_div_ir    <- (((data$hpp_pressure-1013.25)/1013.25)/data$hpp_ir)
    data$p2_div_ir   <- (((data$hpp_pressure-1013.25)/1013.25)^2/data$hpp_ir)
    data$sht21_T_dIR <- data$sht21_T/data$hpp_ir
    
    
    # abs humidity (sensor, reference) / H2O
    # [W. Wagner and A. Pru�: The IAPWS Formulation 1995 for the Thermodynamic Properties of Ordinary Water Substance for General and Scientific Use, Journal of Physical and Chemical Reference Data, June 2002 ,Volume 31, Issue 2, pp. 387535]
    
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
    
    
    # Abs Humidity / H2O from SHT21 / hpp pressure
    
    
    theta         <- 1 - (273.15+data$sht21_T)/647.096
    Pws           <- 220640 * exp( 647.096/(273.15+data$sht21_T) * (coef_1*theta + coef_2*theta^1.5 + coef_3*theta^3 + coef_4*theta^3.5 + coef_5*theta^4 + coef_6*theta^7.5))
    Pw            <- data$sht21_RH*(Pws*100)/100
    data$sht21_AH <- 2.16679 * Pw/(273.15+data$sht21_T)
    data$sht21_H2O<- Pw / (data$hpp_pressure*1e2)*1e2
    
    
    # Dew point (Magnus formula)
    
    K2 <-  17.62
    K3 <- 243.12
    
    data$DP              <- K3 * ( ( (K2*data$T)      /(K3 + data$T)       + log(data$RH/1e2) )      /( (K2*K3)/(K3+data$T)       - log(data$RH/1e2) ) )
    data$diff_T_DP       <- data$T - data$DP
    
    
    # Difference between HPP and REF pressure
    
    data$pressure_diff <- data$hpp_pressure - data$pressure
    
    # SHT21/MCU temperatur slope
    
    data$slope_sht21_T               <- NA
    data$slope_hpp_temperature_mcu   <- NA
    
    for(ith_row in 5:dim(data)[1]){
      id_n <- (ith_row-4):ith_row
      id   <- which(data$days[ith_row]-data$days[id_n]<=0.003473 
                    & !is.na(data$sht21_T[id_n]) & !is.na(data$hpp_temperature_mcu[id_n]))
      n_id <- length(id)
      
      if(n_id>=4){
        id <- id_n[id]
        
        tmp_sht21_T             <- lm.fit(x=matrix(c(rep(1,n_id),data$days[id]),ncol=2),y=data$sht21_T[id])
        tmp_hpp_temperature_mcu <- lm.fit(x=matrix(c(rep(1,n_id),data$days[id]),ncol=2),y=data$hpp_temperature_mcu[id])
        
        data$slope_sht21_T[ith_row]             <- tmp_sht21_T$coefficients[2]/1440
        data$slope_hpp_temperature_mcu[ith_row] <- tmp_hpp_temperature_mcu$coefficients[2]/1440
      }
    }
    
    # SHT21 / MCU dT
    
    data$sht21_T_dT <- c( NA, diff(data$sht21_T) / diff(data$timestamp/60) ) 
    # data$sht21_T_dT <- c( NA, (data$sht21_T[3:dim(data)[1]]-data$sht21_T[1:(dim(data)[1]-2)])/((data$timestamp[3:dim(data)[1]]-data$timestamp[1:(dim(data)[1]-2)])/60),NA)
    
    data$hpp_temperature_mcu_dT <- c(NA,diff(data$hpp_temperature_mcu)/diff(data$timestamp/60))
    # data$hpp_temperature_mcu_dT <- c( NA, (data$hpp_temperature_mcu[3:dim(data)[1]]-data$hpp_temperature_mcu[1:(dim(data)[1]-2)])/((data$timestamp[3:dim(data)[1]]-data$timestamp[1:(dim(data)[1]-2)])/60),NA)
    
    data$diff_dT     <- data$sht21_T_dT - data$hpp_temperature_mcu_dT
    
    ##
    
    # tmp                      <- data$hpp_temperature_mcu
    # data$hpp_temperature_mcu <- NA
    # 
    # for(ith_row in 1:dim(data)[1]){
    #   id <- which( (data$timestamp[ith_row]-data$timestamp) > (1*60-5) & (data$timestamp[ith_row]-data$timestamp) < (1*60+5) )
    #   if(length(id)==1){
    #     data$hpp_temperature_mcu[ith_row] <- tmp[id]
    #   }
    # }
    
    # data$hpp_temperature_mcu <- data$hpp_temperature_mcu - 5*data$hpp_temperature_mcu_dT
    
    
    ### ---------------------------------------------------------------
    
    ## H2O calibration
    
    if(T){
      
      id_ok   <- which(  !is.na(data$H2O)       & data$H2O_F == 1
                         & !is.na(data$sht21_H2O) & data$sht21_H2O > 0.5
                         & !is.na(data$valve)     & data$valve == 0
                         & !is.na(data$CO2)       & data$CO2_F == 1)
      
      n_id_ok <- length(id_ok)
      
      if(length(id_ok)>0){
        fit_H2O     <- lm(y~x, data.frame(x=data$sht21_H2O[id_ok],
                                          y=data$H2O[id_ok],
                                          stringsAsFactors = F))
        
        H2O_predict <- predict(fit_H2O,newdata=data.frame(x=data$sht21_H2O[id_ok],
                                                          y=data$H2O[id_ok],
                                                          stringsAsFactors = F))
        
        RMSE_FIT    <- sqrt(sum((H2O_predict           - data$H2O[id_ok])^2)/n_id_ok)
        RMSE_RAW    <- sqrt(sum((data$sht21_H2O[id_ok] - data$H2O[id_ok])^2)/n_id_ok)
        
        corCoef_FIT <- cor(x=H2O_predict,           y=data$H2O[id_ok],method="pearson",use="complete.obs")
        corCoef_RAW <- cor(x=data$sht21_H2O[id_ok], y=data$H2O[id_ok],method="pearson",use="complete.obs")
        
        
        statistics_H2O <- rbind(statistics_H2O,data.frame(SensorUnit_ID    = SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],
                                                          Sensor           = sensors2cal[ith_sensor2cal],
                                                          Date_UTC_from    = strftime(min(data$date[id_ok]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                          Date_UTC_to      = strftime(max(data$date[id_ok]),"%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                          N                = n_id_ok,
                                                          slope            = fit_H2O$coefficients[2],
                                                          intercept        = fit_H2O$coefficients[1],
                                                          RMSE_FIT         = RMSE_FIT,
                                                          corCoef_FIT      = corCoef_FIT,
                                                          RMSE_RAW         = RMSE_RAW,
                                                          corCoef_RAW      = corCoef_RAW,
                                                          MIN_H2O_REF      = min(data$H2O[id_ok]),
                                                          MAX_H2O_REF      = max(data$H2O[id_ok]),
                                                          MIN_H2O_SEN      = min(data$sht21_H2O[id_ok]),
                                                          MAX_H2O_SEN      = max(data$sht21_H2O[id_ok]),
                                                          CalMode_01       = any(data$CalMode[id_ok]==1),
                                                          CalMode_02       = any(data$CalMode[id_ok]==2),
                                                          CalMode_03       = any(data$CalMode[id_ok]==3),
                                                          stringsAsFactors = F))
      }
    }
    
    ### ---------------------------------------------------------------
    
    ## Sensor measurement time-series
    
    sensor_descriptor <- paste("SU", SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],"_S",sensors2cal[ith_sensor2cal],sep="")
    
    if(T){
      
      # T / dew point
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_T_DEWPOINT_TS.pdf",sep="")
      
      yyy     <- cbind(data$T,
                       data$sht21_T,
                       data$hpp_temperature_mcu,
                       data$DP)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("T [deg C]"))
      legend_str <- c("T REF","T SHT21","T HPP","DP REF")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-15,55),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_T_DEWPOINT_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(-15,55),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_T_DEWPOINT_TS_WEEKS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"week",NULL,c(-15,55),xlabString,ylabString,legend_str)
      
      # T HPP - T REF 
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_diff_Thpp_Tref_TS.pdf",sep="")
      
      yyy     <- cbind(data$sht21_T - data$T,
                       data$hpp_temperature_mcu - data$T)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("T [deg C]"))
      legend_str <- c("T SHT21 - T REF","T HPP - T REF")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,NULL,xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_diff_Thpp_Tref_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,NULL,xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_diff_Thpp_Tref_TS_WEEKS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"week",NULL,NULL,xlabString,ylabString,legend_str)
      
      # RH
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_RH_TS.pdf",sep="")
      
      yyy     <- cbind(data$sht21_RH,data$RH)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("RH [%]"))
      legend_str <- c("RH SHT21","RH REF")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(0,110),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_RH_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(0,110),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_RH_TS_WEEKS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"week",NULL,c(0,110),xlabString,ylabString,legend_str)
      
      # AH
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_AH_TS.pdf",sep="")
      
      yyy     <- cbind(data$AH,
                       data$sht21_AH,
                       data$AH_COMP)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("Absolute humidity [g/m^3]"))
      legend_str <- c("AH REF","AH SHT21","AH PIC")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(0,20),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_AH_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(0,20),xlabString,ylabString,legend_str)
      
      # H2O Vol-%
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_TS.pdf",sep="")
      
      yyy     <- cbind(data$H2O,
                       data$sht21_H2O,
                       data$H2O_COMP)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("H2O [Vol-%]"))
      legend_str <- c("H2O PIC","H2O SHT","H2O COMP")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(0,5),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(0,5),xlabString,ylabString,legend_str)
      
      
      # diff H2O Vol-% [HPP - PIC]
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_wrt_PIC_TS.pdf",sep="")
      
      yyy     <- cbind(data$sht21_H2O-data$H2O)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("H2O [Vol-%]"))
      legend_str <- c("H2O SHT - H2O PIC")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-0.5,0.5),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_wrt_PIC_TS_WEEKS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"week",NULL,c(-0.5,0.5),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_wrt_PIC_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(-0.5,0.5),xlabString,ylabString,legend_str)
      
      # diff H2O Vol-% [HPP - NABEL REF]
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_wrt_NABEL_TS.pdf",sep="")
      
      yyy     <- cbind(data$sht21_H2O-data$H2O_COMP)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("H2O [Vol-%]"))
      legend_str <- c("H2O SHT - H2O NABEL")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-0.5,0.5),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_wrt_NABEL_TS_WEEKS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"week",NULL,c(-0.5,0.5),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_H2O_wrt_NABEL_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(-0.5,0.5),xlabString,ylabString,legend_str)
      
      
      # pressure
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_pressure_TS.pdf",sep="")
      
      yyy     <- cbind(data$pressure,
                       data$hpp_pressure)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("Pressure [hPa]"))
      legend_str <- c("Pressure REF", "Pressure HPP")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(930,1000),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_pressure_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(930,1000),xlabString,ylabString,legend_str)
      
      # pressure difference
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_pressure_diff_TS.pdf",sep="")
      
      yyy     <- cbind(data$hpp_pressure - data$pressure)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("Pressure difference [hPa]"))
      legend_str <- c("HPP-REF")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-3,3),xlabString,ylabString,legend_str)
      
      
      
      # SHT21 T dT
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_sht21_T_dT_TS.pdf",sep="")
      
      yyy     <- cbind(data$sht21_T_dT)
      
      xlabString <- "Date" 
      ylabString <- expression(paste("sht21 T [deg C/min]"))
      legend_str <- c("SHT21_T")
      plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(-0.3,0.3),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_sht21_T_dT_TS_WEEKS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"week",NULL,c(-0.3,0.3),xlabString,ylabString,legend_str)
      
      figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_sht21_T_dT_TS_DAYS.pdf",sep="")
      plot_ts(figname,data$date,yyy,"day",NULL,c(-0.3,0.3),xlabString,ylabString,legend_str)
      
    }
    
    if(T){
      
      id   <- which(data$CalMode==3)
      n_id <- length(id)
      
      if(n_id>0){
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_P_vs_CO2_PC.pdf",sep="")
        
        def_par <- par()
        pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
        par(mai=c(1,1,0.1,0.1))
        
        xlabStr <- expression(paste("CO"[2]*" [ppm]"))
        ylabStr <- "Pressure [hPa]"
        
        plot(data$CO2_ppm[id],data$hpp_pressure[id],xlab=xlabStr,ylab=ylabStr,main="",xlim=c(300,1050),ylim=c(780,980),pch=16,cex=0.75,cex.main=1.25,cex.axis=1.25,cex.lab=1.25)
        
        par(def_par)
        dev.off()
        
      }
      
      ##
      
      if(F){
        
        id   <- which(data$valve==1 & data$date>strptime("20180619000000","%Y%m%d%H%M%S",tz="UTC"))
        n_id <- length(id)
        
        if(length(id)>0){
          
          tmp <- data.frame(date          = data$date[id],
                            timestamp     = data$timestamp[id],
                            SensorUnit_ID = rep(SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],n_id),
                            TRUE_MEAS     = rep(1,n_id),
                            stringsAsFactors=F)
          
          add_dates      <- NULL
          add_timestamps <- NULL
          
          for(i in 2:dim(tmp)[1]){
            
            dT <- tmp$timestamp[i] - tmp$timestamp[i-1]
            
            if(dT>(60+30) & dT<(15*60)){
              add_dates      <- c(add_dates,     tmp$date[i-1]      + (1:(round(dT/60)-1))*60)
              add_timestamps <- c(add_timestamps,tmp$timestamp[i-1] + (1:(round(dT/60)-1))*60)
            }
          }
          
          if(!is.null(add_dates)){
            
            tmp <- rbind(tmp,data.frame(date          = strptime("19700101000000","%Y%m%d%H%M%S",tz="UTC")+add_timestamps,
                                        timestamp     = add_timestamps,
                                        SensorUnit_ID = rep(SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],length(add_dates)),
                                        TRUE_MEAS     = rep(0,length(add_dates)),
                                        stringsAsFactors=F))
          }
          
          tmp <- tmp[order(tmp$date),]
          
          write.table(tmp,paste(plotdir_allModels,"/",sensor_descriptor,"_DATES_CAL.csv",sep=""),sep=";",col.names=T,row.names=F)
          
          rm(tmp,add_dates,add_timestamps)
          gc()
        }
      }
      
    }
    
    
    ## Comparison of selected parameters
    
    if(T){
      
      comparison_info <- NULL
      comparison_info <- rbind(comparison_info,data.frame(factor_1="CO2",      unit_1="[ppm]",   factor_2="hpp_ir",              unit_2="[XX]"  , one2one=F))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="CO2",      unit_1="[ppm]",   factor_2="hpp_logIR",           unit_2="[XX]"  , one2one=F))
      
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="CH4",      unit_1="[ppm]",   factor_2="hpp_ir",              unit_2="[XX]"  , one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="CH4",      unit_1="[ppm]",   factor_2="hpp_logIR",           unit_2="[XX]"  , one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="CH4",      unit_1="[ppm]",   factor_2="CO2",                 unit_2="[ppm]" , one2one=F))
      
      comparison_info <- rbind(comparison_info,data.frame(factor_1="sht21_T",     unit_1="[deg C]", factor_2="hpp_temperature_mcu",    unit_2="[deg C]",one2one=T))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="sht21_T_dT",  unit_1="[deg C]", factor_2="hpp_temperature_mcu_dT", unit_2="[deg C]",one2one=T))
      
      comparison_info <- rbind(comparison_info,data.frame(factor_1="pressure", unit_1="[hPa]",   factor_2="hpp_pressure",        unit_2="[hPa]",  one2one=T))
      
      comparison_info <- rbind(comparison_info,data.frame(factor_1="H2O",                unit_1="[Vol-%]", factor_2="H2O_COMP",  unit_2="[Vol-%]", one2one=T))
      comparison_info <- rbind(comparison_info,data.frame(factor_1="H2O",                unit_1="[Vol-%]", factor_2="sht21_H2O", unit_2="[Vol-%]", one2one=T))
      
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="T",                unit_1="[deg C]", factor_2="CO2_ppm",   unit_2="[ppm]", one2one=F))
      
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="T",                unit_1="[deg C]", factor_2="AH",        unit_2="[g/m3]", one2one=F))
      
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="T",                unit_1="[deg C]", factor_2="H2O",       unit_2="[Vol-%]", one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="T",                unit_1="[deg C]", factor_2="sht21_H2O", unit_2="[Vol-%]", one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="hpp_temperature_mcu", unit_1="[deg C]", factor_2="sht21_H2O", unit_2="[Vol-%]", one2one=F))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="hpp_temperature_mcu", unit_1="[deg C]", factor_2="CO2_ppm",   unit_2="[ppm]", one2one=F))
      
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="CO2",      unit_1="[ppm]",   factor_2="hpp_co2",             unit_2="[ppm]"  ,one2one=T))
      # comparison_info <- rbind(comparison_info,data.frame(factor_1="T",        unit_1="[deg C]", factor_2="hpp_temperature_mcu", unit_2="[deg C]",one2one=T))
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
      
      
      
      for(ith_cmp in 1:dim(comparison_info)[1]){
        
        figname <- paste(plotdir_allModels,"/",sensor_descriptor,"_COMPARISON_RAW_DATA_",comparison_info$factor_1[ith_cmp],"_VS_",comparison_info$factor_2[ith_cmp],".pdf",sep="")
        
        def_par <- par()
        pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
        par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
        
        #
        
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
        
        if(comparison_info$one2one[ith_cmp]){
          RMSE_fit_cmp <- sqrt(sum((fit_cmp$residuals)^2)/n_id_ok)
        }else{
          RMSE_fit_cmp <- NA
        }
        
        xlabString  <- paste(comparison_info$factor_1[ith_cmp],comparison_info$unit_1[ith_cmp])
        ylabString  <- paste(comparison_info$factor_2[ith_cmp],comparison_info$unit_2[ith_cmp])
        mainString  <- paste(sensor_descriptor," [",paste("y = ",sprintf("%.7f",fit_cmp$coefficients[2]),"x + ",sprintf("%.3f",fit_cmp$coefficients[1]),"/", "COR = ",sprintf("%.2f",corCoef),"/RMSE = ",sprintf("%.2f",RMSE_fit_cmp),sep=""),"]",sep="") 
        
        plot(  data[id_ok,pos_factor_1], data[id_ok,pos_factor_2] ,xlim=xrange,ylim=yrange,xlab=xlabString,ylab=ylabString,main=mainString,pch=16,cex=0.5,cex.axis=1.5,cex.lab=1.5,col=1)
        if(comparison_info$one2one[ith_cmp]){
          lines(c(-1e4,1e4),c(-1e4,1e4),lwd=1,lty=1,col=1)
        }
        lines(c(-1e6,1e6),c(fit_cmp$coefficients[1]-1e6*fit_cmp$coefficients[2],fit_cmp$coefficients[1]+1e6*fit_cmp$coefficients[2]),col=2,lwd=1,lty=5)
        
        
        # par(family="mono")
        # text(xrange[1]+0.8*(xrange[2]-xrange[1]),yrange[1]+0.10*(yrange[2]-yrange[1]),date_str)
        # par(family="")
        
        #
        
        par(def_par)
        dev.off()
      }
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
      n_temp_data_selections <- 1
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
    if(CV_mode==8){
      n_temp_data_selections <- 2
    }
    if(CV_mode==9){
      n_temp_data_selections <- 0
    }
    
    # Loop over temporal data selections (CV)
    
    for(ith_temp_data_selections in 0:n_temp_data_selections){
      
      CV_P <- sprintf("%02.0f",ith_temp_data_selections)
      
      if(CV_mode==0 | ith_temp_data_selections==0){
        data_training        <- rep(T,dim(data)[1]) & (data$date<strptime("20180420142700","%Y%m%d%H%M%S",tz="UTC") | data$date>strptime("20180423061700","%Y%m%d%H%M%S",tz="UTC")) 
        data_test            <- rep(T,dim(data)[1])
      }
      if(CV_mode==1){
        data_training        <- data$CalMode == 1 
        data_test            <- data$CalMode == 1
      }
      if(CV_mode==3){
        if(ith_temp_data_selections<1){
          next
        }else{
          data_training        <- data$CalMode %in% c(2,3) 
          data_test            <- data$CalMode == 1
        }
      }
      if(CV_mode==4){
        if(ith_temp_data_selections<1){
          next
        }else{
          
          # 01-01-2019 (timestamp: 1546300800)
          
          ts_1                 <- min(data$timestamp[data$CalMode %in% c(2,3) & data$timestamp < 1546300800]) - 14*86400
          ts_2                 <- max(data$timestamp[data$CalMode %in% c(2,3) & data$timestamp < 1546300800]) + 14*86400
          
          data_training        <- data$timestamp>=ts_1 & data$timestamp<=ts_2
          data_test            <- data$timestamp< ts_1 | data$timestamp> ts_2
          
          rm(ts_1,ts_2)
          gc()
          
          ##
          
          # 01-01-2019 (timestamp: 1546300800) / 01-02-2019 (timestamp: 1548979200)
          
          if(T & sensors2cal[ith_sensor2cal] %in% c(1427)){
            ts_1                 <- min(data$timestamp[data$CalMode %in% c(2,3) & data$timestamp > 1546300800 & data$timestamp < 1548979200]) - 14*86400
            ts_2                 <- max(data$timestamp[data$CalMode %in% c(2,3) & data$timestamp > 1546300800 & data$timestamp < 1548979200]) + 14*86400
            
            data_training        <- data$timestamp>=ts_1 & data$timestamp<=ts_2
            data_test            <- data$timestamp< ts_1 | data$timestamp> ts_2
            
            rm(ts_1,ts_2)
            gc()
          }
          
          ##
          
          # 01-01-2020 (timestamp: 1577836800) / 01-02-2020 (timestamp: 1580515200)
          
          if(T & sensors2cal[ith_sensor2cal] %in% c(430,434)){
            ts_1                 <- min(data$timestamp[data$CalMode %in% c(2,3) & data$timestamp > 1577836800 & data$timestamp < 1580515200]) - 14*86400
            ts_2                 <- max(data$timestamp[data$CalMode %in% c(2,3) & data$timestamp > 1577836800 & data$timestamp < 1580515200]) + 14*86400
            
            data_training        <- data$timestamp>=ts_1 & data$timestamp<=ts_2
            data_test            <- data$timestamp< ts_1 | data$timestamp> ts_2
            
            rm(ts_1,ts_2)
            gc()
          }
          
          ##
          
          # 01-05-2020 (timestamp: 1588291200) / 18-05-2020 (timestamp: 1589760000)
          
          if(F & sensors2cal[ith_sensor2cal] %in% c(431,433,435)){
            ts_1                 <- min(data$timestamp[data$CalMode %in% c(2,3) & data$timestamp > 1588291200 & data$timestamp < 1589760000])
            ts_2                 <- max(data$timestamp[data$CalMode %in% c(2,3) & data$timestamp > 1588291200 & data$timestamp < 1589760000]) + 28*86400
            
            data_training        <- data$timestamp>=ts_1 & data$timestamp<=ts_2
            data_test            <- data$timestamp< ts_1 | data$timestamp> ts_2
            
            rm(ts_1,ts_2)
            gc()
          }
          
          ##
          
          if(T & sensors2cal[ith_sensor2cal] %in% c(1444)){
            
            # ts_1                 <- as.numeric(difftime(time1=strptime("2019-12-16 17:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
            #                                             time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
            # 
            # ts_3                 <- as.numeric(difftime(time1=strptime(strftime(Sys.time(),"%Y-%m-%d %H:%M:%S",tz="UTC"),"%Y-%m-%d %H:%M:%S",tz="UTC"),
            #                                             time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
            # 
            # ts_2                 <- min(c(ts_1 + 0.5*(ts_3 - ts_1), ts_1+28*86400))
            # ts_2                 <- ts_1 + 15 * 86400
            
            ts_1                 <- as.numeric(difftime(time1=strptime("2020-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                        time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
            
            ts_2                 <- as.numeric(difftime(time1=strptime("2020-02-14 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                        time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
            
            
            data_training        <- data$timestamp>=ts_1 & data$timestamp<=ts_2
            data_test            <- data$timestamp< ts_1 | data$timestamp> ts_2
            
            rm(ts_1,ts_2)
            gc()
          }
          
          if(T & sensors2cal[ith_sensor2cal] %in% c(2444)){
            
            
            ts_1                 <- as.numeric(difftime(time1=strptime("2020-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                        time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
            
            ts_2                 <- as.numeric(difftime(time1=strptime("2020-02-14 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),
                                                        time2=strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S",tz="UTC"),units="secs",tz="UTC"))
            
            
            data_training        <- data$timestamp>=ts_1 & data$timestamp<=ts_2
            data_test            <- data$timestamp< ts_1 | data$timestamp> ts_2
            
            rm(ts_1,ts_2)
            gc()
          }
          ##
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
          data_training        <- (data$CalMode %in% c(1,2,3) & data$date<=strptime("20180801000000","%Y%m%d%H%M%S",tz="UTC")) | data$CalMode==3
          data_test            <- (data$CalMode %in% c(1)     & data$date >strptime("20180801000000","%Y%m%d%H%M%S",tz="UTC"))
        }
      }
      
      if(CV_mode==8){
        
        if(ith_temp_data_selections<2){
          next
        }else{
          data_training        <- data$valve == 1 | data$sht21_RH < 5 
          data_test            <- data$valve != 1 & data$sht21_RH > 5
        }
      }
      
      if(CV_mode==9){
        data_training        <- data$valve == 0  & (data$date<strptime("20180420142700","%Y%m%d%H%M%S",tz="UTC") | data$date>strptime("20180423061700","%Y%m%d%H%M%S",tz="UTC")) 
        data_test            <- data$valve %in% c(0,1)
      }
      
      
      
      
      # -------
      
      for(ith_sensor_model in 1:length(sensor_models)){
        
        if(CV_mode%in%c(5,6) & length(grep(pattern="_DRIFT",x=sensor_models[[ith_sensor_model]]$formula))>0){
          next
        }
        
        # if(sensors2cal[ith_sensor2cal]!=1444 & sensor_models[[ith_sensor_model]]$name=="HPP_CO2_IR_CP1_pIR_SHTT_H2O_DCAL_PLF07d"){
        #   next
        # }
        # if(sensors2cal[ith_sensor2cal]==1444 & sensor_models[[ith_sensor_model]]$name=="HPP_CO2_IR_CP1_pIR_SHTT_H2O_DCAL_PLF14d"){
        #   next
        # }
        
        
        
        print(sensor_models[[ith_sensor_model]]$name)
        
        ## check measurements
        
        measurement_ok <- rep(T,dim(data)[1])
        
        
        ## bottle_cal_periods 
        
        if(!is.null(tbl_refGasCylDepl)){
          
          data$CO2[which(data$valve==1)]     <- NA
          data$CO2_ppm[which(data$valve==1)] <- NA
          
          for(ith_rgcd in 1:dim(tbl_refGasCylDepl)[1]){
            
            id <- which(data$valve==1
                        & data$date>=tbl_refGasCylDepl$Date_UTC_from[ith_rgcd]
                        & data$date<=tbl_refGasCylDepl$Date_UTC_to[ith_rgcd])
            
            if(length(id)>0){
              data$CO2[id]     <- tbl_refGasCylDepl$CO2[ith_rgcd] * data$hpp_pressure[id]/1013.25
              data$CO2_ppm[id] <- tbl_refGasCylDepl$CO2[ith_rgcd]
            }
          }
          
          data$valveMin               <- NA
          data$valveNo                <- NA
          data$valveNo[data$valve==1] <- cumsum( c(0,as.numeric(diff(data$timestamp[data$valve==1])>3600))  )
          
          u_valveNo   <- sort(unique(data$valveNo[data$valve==1]))
          n_u_valveNo <- length(u_valveNo)
          
          for(ith_wno in 1:n_u_valveNo){
            id   <- which(u_valveNo[ith_wno] == data$valveNo)
            id   <- id[order(data$timestamp[id])]
            n_id <- length(id)
            
            data$valveMin[id] <- round((data$timestamp[id]-data$timestamp[id[1]])/60)
            
            setToNA_01        <- which(data$timestamp[id]-data$timestamp[id[1]]<=70)
            setToNA_10        <- which(data$timestamp > data$timestamp[id[n_id]] & (data$timestamp-data$timestamp[id[n_id]]) <= 360)
            
            data$CO2[id[setToNA_01]] <- NA
            data$CO2[setToNA_10]     <- NA
          }
          
          bottle_cal_periods <- data$valve==1 & !is.na(data$CO2)
          if(CV_mode==7){
            bottle_cal_periods <- bottle_cal_periods & data$date>strptime("20180801000000","%Y%m%d%H%M%S",tz="UTC")
          }
          
          id_bottle_cal_periods        <- which(bottle_cal_periods==T)
          n_id_bottle_cal_periods      <- length(id_bottle_cal_periods)
          
        }else{
          n_id_bottle_cal_periods <- 0
        }
        
        
        
        
        ## calibration data selection
        
        use4cal <- rep(T,dim(data)[1])
        
        use4cal <- use4cal &  !is.na(data$CO2)
        # use4cal <- use4cal &  !is.na(data$pressure)
        use4cal <- use4cal &  !is.na(data$hpp_pressure)
        
        use4cal <- use4cal &  !is.na(data$hpp_co2)
        
        if(length(grep(pattern="AH",x=sensor_models[[ith_sensor_model]]$name))>0){
          use4cal <- use4cal & !is.na(data$sht21_AH)
        }
        
        if(length(grep(pattern="AHdT",x=sensor_models[[ith_sensor_model]]$formula))>0){
          use4cal <- use4cal & !is.na(data$AH_COMP_dT)
        }
        
        if(length(grep(pattern="H2O",x=sensor_models[[ith_sensor_model]]$formula))>0){
          use4cal <- use4cal & !is.na(data$sht21_H2O)
        }
        
        # if(length(grep(pattern="I\\(T\\)",x=sensor_models[[ith_sensor_model]]$formula))>0){
        #   use4cal <- use4cal & !is.na(data$T)
        # }
        if(length(grep(pattern="HPPT",x=sensor_models[[ith_sensor_model]]$name))>0){
          use4cal <- use4cal & !is.na(data$hpp_temperature_mcu)
        }
        if(length(grep(pattern="SHTTsl",x=sensor_models[[ith_sensor_model]]$name))>0){
          use4cal <- use4cal & !is.na(data$slope_sht21_T)
        }
        if(length(grep(pattern="HPPTsl",x=sensor_models[[ith_sensor_model]]$name))>0){
          use4cal <- use4cal & !is.na(data$slope_hpp_temperature_mcu)
        }
        if(length(grep(pattern="SHTTdT",x=sensor_models[[ith_sensor_model]]$name))>0){
          use4cal <- use4cal & !is.na(data$sht21_T_dT)
          use4cal <- use4cal &  abs(data$sht21_T_dT)<=0.5
        }
        if(length(grep(pattern="HPPdT",x=sensor_models[[ith_sensor_model]]$name))>0){
          use4cal <- use4cal & !is.na(data$hpp_temperature_mcu_dT)
          use4cal <- use4cal &  abs(data$hpp_temperature_mcu_dT)<=0.5
        }
        
        if(length(grep(pattern="diff_dT",x=sensor_models[[ith_sensor_model]]$name))>0){
          use4cal <- use4cal & !is.na(data$hpp_temperature_mcu_dT)
          use4cal <- use4cal & !is.na(data$sht21_T_dT)
          use4cal <- use4cal &  abs(data$hpp_temperature_mcu_dT)<=0.5
          use4cal <- use4cal &  abs(data$sht21_T_dT)<=0.5
        }
        
        use4cal <- use4cal &  ( (data$CalMode ==1 & data$hpp_co2>150) | data$CalMode%in%c(2,3))
        
        use4cal <- use4cal &  measurement_ok
        
        if(sensor_models[[ith_sensor_model]]$modelType%in%c("LM_IR","GAM")){
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
        
        if(CV_mode%in%c(6) & ith_temp_data_selections==1){
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
          
          max_par          <- 100
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
          
          # CV_mode == 7 and pressure calibration after calibration period
          
          if(CV_mode==7){
            if(any(data$PCalOffset==1)){
              if(all(data$date[data$PCalOffset==1]>=strptime("20180801000000","%Y%m%d%H%M%S",tz="UTC"))){
                formula_str <- paste(sensor_models[[ith_sensor_model]]$formula," + PCalOffset",sep="")
              }
            }
          }
          
          if(length(grep(pattern="PLF",x = sensor_models[[ith_sensor_model]]$formula)>0)){
            
            if(length(grep(pattern="PLF07d",x = sensor_models[[ith_sensor_model]]$name)>0)){
              PLFforXdays <- 7
            }
            if(length(grep(pattern="PLF14d",x = sensor_models[[ith_sensor_model]]$name)>0)){
              PLFforXdays <- 14
            }
            if(length(grep(pattern="PLF21d",x = sensor_models[[ith_sensor_model]]$name)>0)){
              PLFforXdays <- 21
            }
            if(length(grep(pattern="PLF28d",x = sensor_models[[ith_sensor_model]]$name)>0)){
              PLFforXdays <- 28
            }
            
            #
            
            timestamp_cal_min   <- min(data$timestamp[id_use4cal])
            timestamp_cal_max   <- max(data$timestamp[id_use4cal])
            
            n_PLF_vars          <- max(c(2,round((timestamp_cal_max-timestamp_cal_min)/(PLFforXdays*86400))+1))
            
            PLF_vars_timestamps <- seq(timestamp_cal_min,timestamp_cal_max,length.out = n_PLF_vars)
            
            PLF_vars_ok       <- rep(T,n_PLF_vars)
            
            for(ith_PLF_var in 2:(n_PLF_vars-1)){
              if(0==length(which(data$timestamp[id_use4cal]>PLF_vars_timestamps[ith_PLF_var-1] & data$timestamp[id_use4cal]<PLF_vars_timestamps[ith_PLF_var+1]))){
                PLF_vars_ok[ith_PLF_var] <- F
              }
            }
            
            PLF_vars_timestamps <- PLF_vars_timestamps[PLF_vars_ok]
            n_PLF_vars          <- length(PLF_vars_timestamps)
            
            #
            
            PLF_vars_names      <- paste("PLFvar",sprintf("%02.0f",1:n_PLF_vars),sep="")
            
            formula_str         <- gsub(pattern="PLF",replacement=paste(PLF_vars_names,sep="",collapse="+"),x=sensor_models[[ith_sensor_model]]$formula)
            
            for(ith_PLF_var in 1:n_PLF_vars){
              
              pos <- which(colnames(data)==PLF_vars_names[ith_PLF_var])
              
              if(length(pos)==1){
                data[,pos]          <- 0
              }else{
                data$newCol         <- 0
                pos                 <- which(colnames(data)=="newCol")
                colnames(data)[pos] <- PLF_vars_names[ith_PLF_var]
              }
            }
            
            for(ith_PLF_var in 2:n_PLF_vars){
              id_PLF                 <- which(data$timestamp>=PLF_vars_timestamps[ith_PLF_var-1] & data$timestamp<PLF_vars_timestamps[ith_PLF_var])
              
              pos_1                  <- which(colnames(data)==PLF_vars_names[ith_PLF_var-1])
              pos_2                  <- which(colnames(data)==PLF_vars_names[ith_PLF_var])
              
              delta_timestamp_PLF    <- PLF_vars_timestamps[ith_PLF_var] - PLF_vars_timestamps[ith_PLF_var-1]
              delta_timestamp_PLF_id <- data$timestamp[id_PLF]           - PLF_vars_timestamps[ith_PLF_var-1]
              
              data[id_PLF,pos_1]     <- 1-delta_timestamp_PLF_id/delta_timestamp_PLF
              data[id_PLF,pos_2]     <-   delta_timestamp_PLF_id/delta_timestamp_PLF
              
              #
              
              if(ith_PLF_var==n_PLF_vars){
                id_PLF <- which(data$timestamp>=PLF_vars_timestamps[ith_PLF_var])
                
                if(length(id_PLF)>0){
                  data[id_PLF,pos_2] <- 1
                }
              }
              
              #
              
              if(ith_PLF_var==2){
                id_PLF <- which(data$timestamp<PLF_vars_timestamps[ith_PLF_var-1])
                
                if(length(id_PLF)>0){
                  data[id_PLF,pos_1] <- 1
                }
              }
            }
          }
          
          # ww <- rep(1,dim(data)[1])
          # id_ww <- which(data$CalMode==3)
          # if(length(id_ww)>0){
          #   ww[id_ww] <- ww[id_ww]*100
          # }
          
          fit              <- rlm(as.formula(formula_str),data[id_use4cal,],psi=psi.huber,k=1.345)
          
          max_par          <- 150
          n_par            <- length(fit$coefficients)
          parameters       <- rep(NA,max_par)
          if(n_par>max_par){
            stop("Number of parameters exceeds maximum number of parameters.")
          }else{
            parameters[1:n_par] <- as.numeric(fit$coefficients)
          }
          
          
          write.table(x = vcov(fit),file = paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_vcov_fit.csv",sep=""),col.names=T,row.names=F,sep=";")
          
          
          # model predictions
          
          tmp               <- predict(fit,data,se.fit=T)
          
          CO2_predicted     <- tmp$fit
          CO2_predicted_se  <- tmp$se.fit
          
          CO2_predicted_ppm <- CO2_predicted * (1013.25/data$hpp_pressure)
          
          rm(tmp)
          
          if(sensor_models[[ith_sensor_model]]$name %in% c("HPP_CO2_IR","HPP_CO2_IR_CP1","HPP_CO2_IR_CP1_H2O_HPPT")){
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
            
            max_par                <- 50
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
            
            max_par                <- 50
            n_par                  <- 5
            parameters             <- rep(NA,max_par)
            parameters[1:n_par]    <- c(x[1]+p_00,x[2]+p_01,x[3]+p_02,x[4]+p_03,x[5]+p_04,x[6]+p_05)
            
            CO2_predicted          <- rep(NA,dim(data)[1])
            CO2_predicted_se       <- rep(NA,dim(data)[1])
            CO2_predicted[id_A_ok] <- ((p_00 + p_01*data$hpp_logIR + p_02/data$hpp_ir) / (   p_05 * TFac + p_03 * TFac * ((data$hpp_pressure-1013.25)/1013.25) + p_04 * TFac * ((data$hpp_pressure-1013.25)/1013.25)^2) )[id_A_ok]
            CO2_predicted_ppm      <- CO2_predicted * (1013.25/data$hpp_pressure)
          }
          
        }
        
        
        # write sensor model parameters into database
        
        MODELS_INSERT_DB <- c("HPP_CO2_IR_CP1_pIR_SHTT_H2O_DRIFT",
                              "HPP_CO2_IR_CP1_pIR_SHTT_H2O_DCAL_PLF14d",
                              "HPP_CO2_IR_CP1_pIR_SHTT_DCAL_PLF14d") 
        
        if(T & sensor_models[[ith_sensor_model]]$name %in% MODELS_INSERT_DB & 
           ((CV_mode==0 & ith_temp_data_selections==0)|(CV_mode==7 & ith_temp_data_selections==2)|(CV_mode==4 & ith_temp_data_selections==1))) {
          
          query_str       <- paste("DELETE FROM `CalibrationParameters` WHERE Type = 'HPP' and Serialnumber = '",sensors2cal[ith_sensor2cal],"' and Mode = ",CV_mode," and CalibrationModelName = '",sensor_models[[ith_sensor_model]]$name,"';",sep="");
          
          drv             <- dbDriver("MySQL")
          con <-carboutil::get_conn( group="CarboSense_MySQL")
          res             <- dbSendQuery(con, query_str)
          tbl_calibration <- dbFetch_UTC(res, n=-1)
          dbClearResult(res)
          dbDisconnect(con)
          
          #
          
          INDEX <- sqrt(sum((CO2_predicted[id_use4cal] - data$CO2[id_use4cal])^2)/n_id_use4cal)
          
          #
          
          parameters2DB <- parameters
          id_NA         <- which(is.na(parameters2DB))
          if(length(id_NA)>0){
            parameters2DB[id_NA] <- 0
          }
          
          #
          
          Data_timestamp_from  <- min(sensor_calibration_info$timestamp_from[id_cal_periods])
          Data_timestamp_to    <- max(sensor_calibration_info$timestamp_to[id_cal_periods])
          Computation_date_UTC <- strftime(Sys.time(),"%Y-%m-%d %H:%M:%S",tz="UTC")
          
          query_str       <- paste("INSERT INTO `CalibrationParameters`(`Type`, `Serialnumber`, `CalibrationModelName`, `Data_timestamp_from`, `Data_timestamp_to`,`Mode`,`Index`, `N_PAR`, `Computation_date_UTC`,`PAR_00`, `PAR_01`, `PAR_02`, `PAR_03`, `PAR_04`, `PAR_05`, `PAR_06`, `PAR_07`, `PAR_08`, `PAR_09`, `PAR_10`, `PAR_11`, `PAR_12`, `PAR_13`, `PAR_14`, `PAR_15`, `PAR_16`, `PAR_17`, `PAR_18`, `PAR_19`,`PAR_20`, `PAR_21`, `PAR_22`, `PAR_23`, `PAR_24`, `PAR_25`, `PAR_26`, `PAR_27`, `PAR_28`, `PAR_29`,`PAR_30`, `PAR_31`, `PAR_32`, `PAR_33`, `PAR_34`, `PAR_35`, `PAR_36`, `PAR_37`, `PAR_38`, `PAR_39`,`PAR_40`, `PAR_41`, `PAR_42`, `PAR_43`, `PAR_44`, `PAR_45`, `PAR_46`, `PAR_47`, `PAR_48`, `PAR_49`,`PAR_50`, `PAR_51`, `PAR_52`, `PAR_53`, `PAR_54`, `PAR_55`, `PAR_56`, `PAR_57`, `PAR_58`, `PAR_59`,`PAR_60`, `PAR_61`, `PAR_62`, `PAR_63`, `PAR_64`, `PAR_65`, `PAR_66`, `PAR_67`, `PAR_68`, `PAR_69`,`PAR_70`, `PAR_71`, `PAR_72`, `PAR_73`, `PAR_74`, `PAR_75`, `PAR_76`, `PAR_77`, `PAR_78`, `PAR_79`,`PAR_80`, `PAR_81`, `PAR_82`, `PAR_83`, `PAR_84`, `PAR_85`, `PAR_86`, `PAR_87`, `PAR_88`, `PAR_89`,`PAR_90`, `PAR_91`, `PAR_92`, `PAR_93`, `PAR_94`, `PAR_95`, `PAR_96`, `PAR_97`, `PAR_98`, `PAR_99`, `PAR_100`,`PAR_101`,`PAR_102`,`PAR_103`,`PAR_104`,`PAR_105`,`PAR_106`,`PAR_107`,`PAR_108`,`PAR_109`,`PAR_110`,`PAR_111`,`PAR_112`,`PAR_113`,`PAR_114`,`PAR_115`,`PAR_116`,`PAR_117`,`PAR_118`,`PAR_119`,`PAR_120`,`PAR_121`,`PAR_122`,`PAR_123`,`PAR_124`,`PAR_125`,`PAR_126`,`PAR_127`,`PAR_128`,`PAR_129`,`PAR_130`,`PAR_131`,`PAR_132`,`PAR_133`,`PAR_134`,`PAR_135`,`PAR_136`,`PAR_137`,`PAR_138`,`PAR_139`,`PAR_140`,`PAR_141`,`PAR_142`,`PAR_143`,`PAR_144`,`PAR_145`,`PAR_146`,`PAR_147`,`PAR_148`,`PAR_149`,`SHT21_RH_max`,`SHT21_T_DP_min`) ",sep="")
          query_str       <- paste(query_str, paste("VALUES ('",sensor_calibration_info$Type[1],"','",sensors2cal[ith_sensor2cal],"','",sensor_models[[ith_sensor_model]]$name,"',",Data_timestamp_from,",",Data_timestamp_to,",",CV_mode,",",ceiling(INDEX),",",n_par,",'",Computation_date_UTC,"',",
                                                    parameters2DB[01],",",parameters2DB[02],",",parameters2DB[03],",",parameters2DB[04],",",parameters2DB[05],",",parameters2DB[06],",",parameters2DB[07],",",parameters2DB[08],",",parameters2DB[09],",",parameters2DB[10],",",
                                                    parameters2DB[11],",",parameters2DB[12],",",parameters2DB[13],",",parameters2DB[14],",",parameters2DB[15],",",parameters2DB[16],",",parameters2DB[17],",",parameters2DB[18],",",parameters2DB[19],",",parameters2DB[20],",",
                                                    parameters2DB[21],",",parameters2DB[22],",",parameters2DB[23],",",parameters2DB[24],",",parameters2DB[25],",",parameters2DB[26],",",parameters2DB[27],",",parameters2DB[28],",",parameters2DB[29],",",parameters2DB[30],",",
                                                    parameters2DB[31],",",parameters2DB[32],",",parameters2DB[33],",",parameters2DB[34],",",parameters2DB[35],",",parameters2DB[36],",",parameters2DB[37],",",parameters2DB[38],",",parameters2DB[39],",",parameters2DB[40],",",
                                                    parameters2DB[41],",",parameters2DB[42],",",parameters2DB[43],",",parameters2DB[44],",",parameters2DB[45],",",parameters2DB[46],",",parameters2DB[47],",",parameters2DB[48],",",parameters2DB[49],",",parameters2DB[50],",",
                                                    parameters2DB[51],",",parameters2DB[52],",",parameters2DB[53],",",parameters2DB[54],",",parameters2DB[55],",",parameters2DB[56],",",parameters2DB[57],",",parameters2DB[58],",",parameters2DB[59],",",parameters2DB[60],",",
                                                    parameters2DB[61],",",parameters2DB[62],",",parameters2DB[63],",",parameters2DB[64],",",parameters2DB[65],",",parameters2DB[66],",",parameters2DB[67],",",parameters2DB[68],",",parameters2DB[69],",",parameters2DB[70],",",
                                                    parameters2DB[71],",",parameters2DB[72],",",parameters2DB[73],",",parameters2DB[74],",",parameters2DB[75],",",parameters2DB[76],",",parameters2DB[77],",",parameters2DB[78],",",parameters2DB[79],",",parameters2DB[80],",",
                                                    parameters2DB[81],",",parameters2DB[82],",",parameters2DB[83],",",parameters2DB[84],",",parameters2DB[85],",",parameters2DB[86],",",parameters2DB[87],",",parameters2DB[88],",",parameters2DB[89],",",parameters2DB[90],",",
                                                    parameters2DB[91],",",parameters2DB[92],",",parameters2DB[93],",",parameters2DB[94],",",parameters2DB[95],",",parameters2DB[96],",",parameters2DB[97],",",parameters2DB[98],",",parameters2DB[99],",",parameters2DB[100],",",
                                                    parameters2DB[101],",",parameters2DB[102],",",parameters2DB[103],",",parameters2DB[104],",",parameters2DB[105],",",parameters2DB[106],",",parameters2DB[107],",",parameters2DB[108],",",parameters2DB[109],",",parameters2DB[110],",",
                                                    parameters2DB[111],",",parameters2DB[112],",",parameters2DB[113],",",parameters2DB[114],",",parameters2DB[115],",",parameters2DB[116],",",parameters2DB[117],",",parameters2DB[118],",",parameters2DB[119],",",parameters2DB[120],",",
                                                    parameters2DB[121],",",parameters2DB[122],",",parameters2DB[123],",",parameters2DB[124],",",parameters2DB[125],",",parameters2DB[126],",",parameters2DB[127],",",parameters2DB[128],",",parameters2DB[129],",",parameters2DB[130],",",
                                                    parameters2DB[131],",",parameters2DB[132],",",parameters2DB[133],",",parameters2DB[134],",",parameters2DB[135],",",parameters2DB[136],",",parameters2DB[137],",",parameters2DB[138],",",parameters2DB[139],",",parameters2DB[140],",",
                                                    parameters2DB[141],",",parameters2DB[142],",",parameters2DB[143],",",parameters2DB[144],",",parameters2DB[145],",",parameters2DB[146],",",parameters2DB[147],",",parameters2DB[148],",",parameters2DB[149],",",parameters2DB[150],",-999,-999);",sep=""))
          
          
          drv             <- dbDriver("MySQL")
          con <-carboutil::get_conn( group="CarboSense_MySQL")
          res             <- dbSendQuery(con, query_str)
          tbl_calibration <- dbFetch_UTC(res, n=-1)
          dbClearResult(res)
          dbDisconnect(con)
          
          rm(id_NA,parameters2DB)
          gc()
          
        }
        
        
        
        # bottle cal predictions
        
        if(n_id_bottle_cal_periods>1){
          
          bottle_cal_periods_df <- data.frame(date         = data$date[id_bottle_cal_periods],
                                              days         = data$days[id_bottle_cal_periods],
                                              HPP_CO2      = data$hpp_co2[id_bottle_cal_periods],
                                              hpp_ir       = data$hpp_ir[id_bottle_cal_periods],
                                              hpp_pressure = data$hpp_pressure[id_bottle_cal_periods],
                                              CO2_PRED     = CO2_predicted[id_bottle_cal_periods],
                                              CO2_PRED_ppm = CO2_predicted_ppm[id_bottle_cal_periods],
                                              CO2        = data$CO2[id_bottle_cal_periods],
                                              CO2_ppm    = data$CO2_ppm[id_bottle_cal_periods],
                                              sht21_T_dT = data$sht21_T_dT[id_bottle_cal_periods],
                                              HPP_dT     = data$hpp_temperature_mcu_dT[id_bottle_cal_periods],
                                              sht21_T    = data$sht21_T[id_bottle_cal_periods],
                                              sht21_RH   = data$sht21_RH[id_bottle_cal_periods],
                                              hpp_temperature_mcu = data$hpp_temperature_mcu[id_bottle_cal_periods],
                                              T          = data$T[id_bottle_cal_periods],
                                              valveNo    = data$valveNo[id_bottle_cal_periods],
                                              valveMin   = data$valveMin[id_bottle_cal_periods],
                                              CylPress   = rep(NA,n_id_bottle_cal_periods),
                                              stringsAsFactors = F)
          
          #
          
          u_BCP_event        <- sort(unique(data$valveNo))
          n_u_BCP_event      <- length(u_BCP_event)
          BCP_days_RES       <- rep(NA,n_u_BCP_event)
          BCP_days_timestamp <- rep(NA,n_u_BCP_event)
          
          if(n_u_BCP_event>1){
            
            tmp_df <- NULL
            
            for(ith_BCP_event in 1:n_u_BCP_event){
              id   <- which(data$valveNo[id_bottle_cal_periods]==u_BCP_event[ith_BCP_event])
              n_id <- length(id)
              
              if(n_id>5){
                
                timestamp_00 <- min(data$timestamp[id_bottle_cal_periods[id]])
                
                id_beforeCal <- which(  timestamp_00 - data$timestamp >=  60
                                        & timestamp_00 - data$timestamp <= 900
                                        & !is.na(CO2_predicted)
                                        & !is.na(data$CO2))
                
                if(length(id_beforeCal)>0){
                  CO2_deviation_mean_bef   <- mean(  CO2_predicted[id_beforeCal]) - mean(  data$CO2[id_beforeCal])
                  CO2_deviation_median_bef <- median(CO2_predicted[id_beforeCal]) - median(data$CO2[id_beforeCal])
                  CO2_median_bef           <- median(data$CO2[id_beforeCal])
                  CO2_mean_bef             <- mean(  data$CO2[id_beforeCal])
                }else{
                  CO2_deviation_mean_bef   <- NA
                  CO2_deviation_median_bef <- NA
                  CO2_median_bef           <- NA
                  CO2_mean_bef             <- NA
                }
                
                
                tmp_df <- rbind(tmp_df,data.frame(SensorUnit_ID            = SensorUnit_ID_2_cal[ith_SensorUnit_ID_2_cal],
                                                  Sensor                   = sensors2cal[ith_sensor2cal],
                                                  Sensor_model_name        = sensor_models[[ith_sensor_model]]$name,
                                                  timestamp                = mean(data$timestamp[id_bottle_cal_periods[id]]),
                                                  date                     = min(data$date[id_bottle_cal_periods[id]]) + (mean(data$timestamp[id_bottle_cal_periods[id]])-timestamp_00),
                                                  N                        = n_id,
                                                  CO2_predicted_mean       = mean(  CO2_predicted[id_bottle_cal_periods[id]]),
                                                  CO2_predicted_median     = median(CO2_predicted[id_bottle_cal_periods[id]]),
                                                  CO2_predicted_sd         = sd(    CO2_predicted[id_bottle_cal_periods[id]]),
                                                  CO2_predicted_range      = diff(range(    CO2_predicted[id_bottle_cal_periods[id]])),
                                                  CO2_ref_mean             = mean(  data$CO2[id_bottle_cal_periods[id]]),
                                                  CO2_ref_median           = median(data$CO2[id_bottle_cal_periods[id]]),
                                                  CO2_mean_bef             = CO2_mean_bef,
                                                  CO2_median_bef           = CO2_median_bef,
                                                  CO2_deviation_mean       = mean(  CO2_predicted[id_bottle_cal_periods[id]])-mean(  data$CO2[id_bottle_cal_periods[id]]),
                                                  CO2_deviation_median     = median(CO2_predicted[id_bottle_cal_periods[id]])-median(data$CO2[id_bottle_cal_periods[id]]),
                                                  CO2_deviation_mean_bef   = CO2_deviation_mean_bef,
                                                  CO2_deviation_median_bef = CO2_deviation_median_bef,
                                                  T_MCU_mean               = mean(data$hpp_temperature_mcu[id_bottle_cal_periods[id]]),
                                                  T_MCU_range              = diff(range(data$hpp_temperature_mcu[id_bottle_cal_periods[id]])),
                                                  T_SHT21_mean             = mean(data$sht21_T[id_bottle_cal_periods[id]]),
                                                  T_SHT21_range            = diff(range(data$sht21_T[id_bottle_cal_periods[id]])),
                                                  T_mean                   = mean(data$T[id_bottle_cal_periods[id]]),
                                                  T_range                  = diff(range(data$T[id_bottle_cal_periods[id]])),
                                                  RH_SHT21_mean            = mean(data$sht21_RH[id_bottle_cal_periods[id]]),
                                                  RH_SHT21_range           = diff(range(data$sht21_RH[id_bottle_cal_periods[id]])),
                                                  RH_mean                  = mean(data$RH[id_bottle_cal_periods[id]]),
                                                  RH_range                 = diff(range(data$RH[id_bottle_cal_periods[id]])),
                                                  AH_mean                  = mean(data$AH[id_bottle_cal_periods[id]]),
                                                  AH_range                 = diff(range(data$AH[id_bottle_cal_periods[id]])),
                                                  H2O_mean                 = mean(data$H2O[id_bottle_cal_periods[id]]),
                                                  H2O_range                = diff(range(data$H2O[id_bottle_cal_periods[id]])),
                                                  hpp_pressure             = mean(data$hpp_pressure[id_bottle_cal_periods[id]]),
                                                  pressure                 = mean(data$pressure[id_bottle_cal_periods[id]]),
                                                  stringsAsFactors         = F))
                
                rm(timestamp_00,id_beforeCal,CO2_deviation_mean_bef,CO2_deviation_median_bef)
                gc()
              }
            }
            
            #
            
            tmp               <- approx(x=tmp_df$timestamp,y=tmp_df$CO2_deviation_median,xout=data$timestamp,method="linear",rule = 2)
            BCP_adj           <- tmp$y
            
            print(CV_mode)
            
            if(CV_mode%in%c(3,4,6,7)){
              CO2_predicted_BCP <- CO2_predicted - BCP_adj
            }
            
            #
            
            statistics_BCP <- rbind(statistics_BCP,tmp_df)
            
            rm(tmp_df)
            gc()
            
          }else{
            CO2_predicted_BCP <- CO2_predicted
            BCP_adj           <- rep(0,dim(data)[1])
          }
          
          rm(u_BCP_event,n_u_BCP_event,BCP_days_RES,BCP_days_timestamp,id,n_id,tmp)
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
          
          xrange <- range(bottle_cal_periods_df$date)
          yrange <- range(bottle_cal_periods_df$CO2_PRED-bottle_cal_periods_df$CO2,na.rm=T)
          
          plot(bottle_cal_periods_df$date,bottle_cal_periods_df$CO2_PRED-bottle_cal_periods_df$CO2,pch=16,cex=0.5,xlim=xrange,ylim=yrange,xlab="Date",ylab=expression(paste("CO"[2]*" PRED - CO"[2]*" REF [ppm]")),cex.lab=1.25,cex.axis=1.25,col=1)
          
          lines(data$date,pred_RES,col="black", lwd=2,lty=5)
          
          for(valveNo in sort(unique(bottle_cal_periods_df$valveNo))){
            id  <- which(bottle_cal_periods_df$valveNo==valveNo)
            points(bottle_cal_periods_df$date[id[1]],mean(bottle_cal_periods_df$CO2_PRED[id]-bottle_cal_periods_df$CO2[id]),  pch=16,col=2,cex=1.25)
            points(bottle_cal_periods_df$date[id[1]],median(bottle_cal_periods_df$CO2_PRED[id]-bottle_cal_periods_df$CO2[id]),pch=16,col=4,cex=1.25)
          }
          
          for(level in seq(-100,100,5)){
            lines(c(min(data$date),max(data$date)),c(level,level),col="gray50",lwd=1,lty=1)
          }
          
          par(family="mono")
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
          
          #
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL_TS.pdf",sep="")
          
          def_par <- par()
          pdf(file = figname, width=8, height=6, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
          
          plot(-1:20,rep(0,22),xlim=c(-0.5,22),ylim=c(-6,6),t="l",col="gray",xlab="Minutes",ylab="Residual [ppm]",cex.main=1.25,cex.lab=1.25,cex.axis=1.25)
          
          nValveEvents <- length(sort(unique(bottle_cal_periods_df$valveNo)))
          CRP          <- colorRampPalette(c("green","blue","purple","red"))
          leg_str      <- NULL
          leg_col      <- NULL
          counter      <- 0
          
          for(valveNo in sort(unique(bottle_cal_periods_df$valveNo))){
            
            counter <- counter + 1
            
            id  <- which(bottle_cal_periods_df$valveNo==valveNo)
            tmp <- (bottle_cal_periods_df$CO2_PRED[id]-bottle_cal_periods_df$CO2[id])
            
            lines(bottle_cal_periods_df$valveMin[id],tmp,col=CRP(nValveEvents)[counter],lwd=1,lty=1)
            lines(c( 0.0, 0.0),c(-1e9,1e9),col="gray",lty=5,lwd=3)
            lines(c(14.0,14.0),c(-1e9,1e9),col="gray",lty=5,lwd=3)
            points(16, mean(tmp), col=CRP(nValveEvents)[counter],pch=16,cex=0.5)
            
            leg_str <- c(leg_str,strftime(bottle_cal_periods_df$date[id[1]],"%d %b %H:%M",tz="UTC"))
            leg_col <- c(leg_col,CRP(nValveEvents)[counter])
          }
          
          legend("topright",legend=leg_str,col=leg_col,lty=1,lwd=2,bg="white",cex=0.3)
          
          par(def_par)
          dev.off()
          
          #
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL_IR_TS.pdf",sep="")
          
          def_par <- par()
          pdf(file = figname, width=8, height=6, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
          
          plot(-1:20,rep(0,22),xlim=c(-0.5,22),ylim=range(bottle_cal_periods_df$hpp_ir),t="l",col="gray",xlab="Minutes",ylab="IR [XX]",cex.main=1.25,cex.lab=1.25,cex.axis=1.25)
          
          nValveEvents <- length(sort(unique(bottle_cal_periods_df$valveNo)))
          CRP          <- colorRampPalette(c("green","blue","purple","red"))
          leg_str      <- NULL
          leg_col      <- NULL
          counter      <- 0
          
          for(valveNo in sort(unique(bottle_cal_periods_df$valveNo))){
            
            counter <- counter + 1
            
            id      <- which(bottle_cal_periods_df$valveNo==valveNo)
            
            lines(bottle_cal_periods_df$valveMin[id],bottle_cal_periods_df$hpp_ir[id],col=CRP(nValveEvents)[counter],lwd=1,lty=1)
            lines(c( 0.0, 0.0),c(-1e9,1e9),col="gray",lty=5,lwd=3)
            lines(c(14.0,14.0),c(-1e9,1e9),col="gray",lty=5,lwd=3)
            
            leg_str <- c(leg_str,strftime(bottle_cal_periods_df$date[id[1]],"%d %b %H:%M",tz="UTC"))
            leg_col <- c(leg_col,CRP(nValveEvents)[counter])
          }
          
          legend("topright",legend=leg_str,col=leg_col,lty=1,lwd=2,bg="white",cex=0.3)
          
          par(def_par)
          dev.off()
          
          #
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL_sht21_RH_TS.pdf",sep="")
          
          def_par <- par()
          pdf(file = figname, width=8, height=6, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
          
          plot(-1:20,rep(0,22),xlim=c(-0.5,22),ylim=c(-3,5),t="l",col="gray",xlab="Minutes",ylab="RH [%]",cex.main=1.25,cex.lab=1.25,cex.axis=1.25)
          
          nValveEvents <- length(sort(unique(bottle_cal_periods_df$valveNo)))
          CRP          <- colorRampPalette(c("green","blue","purple","red"))
          leg_str      <- NULL
          leg_col      <- NULL
          counter      <- 0
          
          for(valveNo in sort(unique(bottle_cal_periods_df$valveNo))){
            
            counter <- counter + 1
            
            id      <- which(bottle_cal_periods_df$valveNo==valveNo)
            
            lines(bottle_cal_periods_df$valveMin[id],bottle_cal_periods_df$sht21_RH[id],col=CRP(nValveEvents)[counter],lwd=1,lty=1)
            lines(c( 0.0, 0.0),c(-1e9,1e9),col="gray",lty=5,lwd=3)
            lines(c(14.0,14.0),c(-1e9,1e9),col="gray",lty=5,lwd=3)
            # points(16, mean(tmp), col=CRP(nValveEvents)[counter],pch=16,cex=0.5)
            
            leg_str <- c(leg_str,strftime(bottle_cal_periods_df$date[id[1]],"%d %b %H:%M",tz="UTC"))
            leg_col <- c(leg_col,CRP(nValveEvents)[counter])
          }
          
          legend("topright",legend=leg_str,col=leg_col,lty=1,lwd=2,bg="white",cex=0.3)
          
          par(def_par)
          dev.off()
          
          #
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL_sht21_T_REF_T_TS.pdf",sep="")
          
          def_par <- par()
          pdf(file = figname, width=8, height=6, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
          
          plot(-1:20,rep(0,22),xlim=c(-0.5,22),ylim=range(bottle_cal_periods_df$sht21_T-bottle_cal_periods_df$T,na.rm=T),t="l",col="gray",xlab="Minutes",ylab="SHT21 T - REF T [deg C]",cex.main=1.25,cex.lab=1.25,cex.axis=1.25)
          
          nValveEvents <- length(sort(unique(bottle_cal_periods_df$valveNo)))
          CRP          <- colorRampPalette(c("green","blue","purple","red"))
          leg_str      <- NULL
          leg_col      <- NULL
          counter      <- 0
          
          for(valveNo in sort(unique(bottle_cal_periods_df$valveNo))){
            
            counter <- counter + 1
            
            id      <- which(bottle_cal_periods_df$valveNo==valveNo & !is.na(bottle_cal_periods_df$sht21_T-bottle_cal_periods_df$T))
            
            if(length(id)==0){
              next
            }
            
            lines(bottle_cal_periods_df$valveMin[id],bottle_cal_periods_df$sht21_T[id]-bottle_cal_periods_df$T[id],col=CRP(nValveEvents)[counter],lwd=1,lty=1)
            lines(c( 0.0, 0.0),c(-1e9,1e9),col="gray",lty=5,lwd=3)
            lines(c(14.0,14.0),c(-1e9,1e9),col="gray",lty=5,lwd=3)
            
            leg_str <- c(leg_str,strftime(bottle_cal_periods_df$date[id[1]],"%d %b %H:%M",tz="UTC"))
            leg_col <- c(leg_col,CRP(nValveEvents)[counter])
          }
          
          legend("topright",legend=leg_str,col=leg_col,lty=1,lwd=2,bg="white",cex=0.3)
          
          par(def_par)
          dev.off()
          
          #
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL_TS_SINGLE.pdf",sep="")
          
          def_par <- par()
          pdf(file = figname, width=8, height=6, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
          
          for(valveNo in sort(unique(bottle_cal_periods_df$valveNo))){
            
            id     <- which(bottle_cal_periods_df$valveNo==valveNo)
            
            fit    <- lm(CO2_PRED_ppm~valveMin,bottle_cal_periods_df[id,])
            
            SD     <- sd(  bottle_cal_periods_df$CO2_PRED_ppm[id])
            MEAN   <- mean(bottle_cal_periods_df$CO2_PRED_ppm[id])
            DELTA  <- mean(bottle_cal_periods_df$CO2_PRED_ppm[id]) - 440.42
            RMSE   <- sqrt(sum((bottle_cal_periods_df$CO2_PRED_ppm[id]-440.42)^2)/length(id))
            
            str00  <- paste("SD:   ",sprintf("%6.2f",SD))
            str01  <- paste("MEAN: ",sprintf("%6.2f",MEAN))
            str02  <- paste("DELTA:",sprintf("%6.2f",DELTA))
            str03  <- paste("RMSE: ",sprintf("%6.2f",RMSE))
            str04  <- paste("SLOPE:",sprintf("%6.2f",fit$coefficients[2]))
            
            yrange <- 440 + c(-20,5)
            
            plot(-1:15,rep(0,17),xlim=c(-0.5,60.5),ylim=yrange,t="l",col="gray",xlab="Minutes",ylab=expression(paste("CO"[2]*" [ppm]")),main=strftime(bottle_cal_periods_df$date[id[1]],"%Y-%m-%d %H:%M",tz="UTC"),cex.main=1.25,cex.lab=1.25,cex.axis=1.25)
            
            tmp <- (bottle_cal_periods_df$CO2_PRED_ppm[id])
            
            lines(bottle_cal_periods_df$valveMin[id],tmp,col=2,lwd=1,lty=1)
            
            lines(bottle_cal_periods_df$valveMin[id],rep(440.42,length(id)),col=1,lwd=1,lty=1)
            
            lines(bottle_cal_periods_df$valveMin[id],rep(mean(tmp),length(id)),col=1,lwd=1,lty=5)
            
            lines(bottle_cal_periods_df$valveMin[id],bottle_cal_periods_df$valveMin[id]*fit$coefficients[2] + fit$coefficients[1],col=1,lwd=1,lty=5)
            
            lines(c( 0.0, 0.0),c(-1e9,1e9),col="gray",lty=5,lwd=3)
            lines(c(14.0,14.0),c(-1e9,1e9),col="gray",lty=5,lwd=3)
            
            legend("topright",legend=c("HPP","Cylinder"),col=c(2,1),lty=1,lwd=2,bg="white")
            
            par(family="mono")
            legend("bottomright",legend=c(str00,str01,str02,str03,str04),bg="white")
            par(family="")
            
          }
          
          par(def_par)
          dev.off()
          
          #
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL_TS_SHT21dT_SINGLE.pdf",sep="")
          
          def_par <- par()
          pdf(file = figname, width=8, height=6, onefile=T, pointsize=12, colormodel="srgb")
          par(mai=c(1,1,0.5,1.0),mfrow=c(1,1))
          
          for(valveNo in sort(unique(bottle_cal_periods_df$valveNo))){
            
            id     <- which(bottle_cal_periods_df$valveNo==valveNo)
            
            yrange <- c(-0.3,0.6)
            
            plot(-1:15,rep(0,17),xlim=c(-0.5,14.5),ylim=yrange,t="l",col="gray",xlab="Minutes",ylab=expression(paste(Delta,"Temperature [deg C/min]")),main=strftime(bottle_cal_periods_df$date[id[1]],"%Y-%m-%d %H:%M",tz="UTC"),cex.main=1.25,cex.lab=1.25,cex.axis=1.25)
            
            lines(bottle_cal_periods_df$valveMin[id],bottle_cal_periods_df$sht21_T_dT[id],col=2,lwd=1,lty=1)
            lines(bottle_cal_periods_df$valveMin[id],bottle_cal_periods_df$HPP_dT[id],    col=1,lwd=1,lty=1)
            
            lines(c(-1e9,1e9), c( 0.0, 0.0),col="gray",lty=1,lwd=1)
            
            lines(c( 0.0, 0.0),c(-1e9,1e9),col="gray",lty=5,lwd=3)
            lines(c(14.0,14.0),c(-1e9,1e9),col="gray",lty=5,lwd=3)
            
            par(new=T)
            
            yrange <- c(-10,45)
            
            plot(-1:15,rep(-999,17),xlim=c(-0.5,14.5),ylim=yrange,t="l",col="gray",xlab="",ylab="",main="",xaxt="n",yaxt="n",cex.main=1.25,cex.lab=1.25,cex.axis=1.25)
            
            lines(bottle_cal_periods_df$valveMin[id],bottle_cal_periods_df$sht21_T[id],             col=2,lwd=1,lty=5)
            lines(bottle_cal_periods_df$valveMin[id],bottle_cal_periods_df$hpp_temperature_mcu[id], col=1,lwd=1,lty=5)
            lines(bottle_cal_periods_df$valveMin[id],bottle_cal_periods_df$T[id],                   col=4,lwd=1,lty=5)
            
            axis(4,cex.main=1.25,cex.lab=1.25,cex.axis=1.25)
            mtext(text = "Temperature [deg C]",side=4,line=2.5)
            
            legend("topright",legend=c("SHT21","MCU","T"),col=c(2,1,4),lty=1,lwd=2,bg="white")
            
          }
          
          par(def_par)
          dev.off()
          
          #
          
          for(var in c("T","hpp_temperature_mcu","days")){
            
            if(var=="T"){
              xx      <- bottle_cal_periods_df$T
              xlabStr <- "T [deg C]"
            }
            if(var=="hpp_temperature_mcu"){
              xx      <- bottle_cal_periods_df$hpp_temperature_mcu
              xlabStr <- "T MCU [deg C]"
            }
            if(var=="CylPress"){
              xx      <- bottle_cal_periods_df$CylPress
              xlabStr <- "Cyl press [bar]"
            }
            if(var=="days"){
              xx      <- bottle_cal_periods_df$days
              xlabStr <- "Days"
            }
            
            fit     <- lm(y~x,data.frame(x=xx,y=bottle_cal_periods_df$CO2_PRED-bottle_cal_periods_df$CO2,stringsAsFactors = F))
            RMSE    <- sqrt(sum(fit$residuals^2)/length(fit$residuals))
            corCoef <- cor(x=xx,y=bottle_cal_periods_df$CO2_PRED-bottle_cal_periods_df$CO2,method="pearson",use="complete.obs")
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL_RES_",var,".pdf",sep="")
            
            def_par <- par()
            pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.5,0.5),mfrow=c(1,1))
            
            plot(xx,bottle_cal_periods_df$CO2_PRED-bottle_cal_periods_df$CO2,col="black",pch=16,cex=0.75,xlab=xlabStr,ylab=expression(paste("CO"[2]*" PRED - CO"[2]*" REF [ppm]")),main="",cex.main=1.25,cex.lab=1.25,cex.axis=1.25)
            lines(c(-1e9,1e9),c(-1e9,1e9)*fit$coefficient[2]+fit$coefficient[1],col=2,lty=5,lwd=2)
            
            
            par(family="mono")
            legend("topleft",legend=c(paste("RMSE:",sprintf("%6.2f",RMSE)),paste("COR: ",sprintf("%6.2f",corCoef))),bg="white")
            par(family="")
            
            par(def_par)
            dev.off()
            
          }
          
          #          
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL_T_TS.pdf",sep="")
          
          yyy     <- cbind(bottle_cal_periods_df$T,
                           bottle_cal_periods_df$hpp_temperature_mcu,
                           bottle_cal_periods_df$sht21_T)
          
          xlabString <- "Date" 
          ylabString <- expression(paste("T [deg C]"))
          legend_str <- c("T REF","T MCU","T SHT21")
          plot_ts(figname,bottle_cal_periods_df$date,yyy,"all_day2day",NULL,NULL,xlabString,ylabString,legend_str)
          
          #
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL_Pressure_TS.pdf",sep="")
          
          yyy     <- cbind(bottle_cal_periods_df$hpp_pressure)
          
          xlabString <- "Date" 
          ylabString <- expression(paste("Pressure [hPa]"))
          legend_str <- c("HPP pressure")
          plot_ts(figname,bottle_cal_periods_df$date,yyy,"all_day2day",NULL,NULL,xlabString,ylabString,legend_str)
          
          #
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BOTTLE_CAL_CO2ppm_TS.pdf",sep="")
          
          yyy     <- cbind(bottle_cal_periods_df$CO2_ppm)
          
          xlabString <- "Date" 
          ylabString <- expression(paste("CO2 [ppm]"))
          legend_str <- c("CO2 REF")
          plot_ts(figname,bottle_cal_periods_df$date,yyy,"all_day2day",NULL,NULL,xlabString,ylabString,legend_str)
          
        }
        
        
        # Statistics
        
        residuals_FIT  <- CO2_predicted[id_use4cal] - data$CO2[id_use4cal]
        RMSE_FIT       <- sqrt(sum((residuals_FIT)^2)/n_id_use4cal)
        MAD_FIT        <- mad(residuals_FIT)
        COR_COEF_FIT   <- cor(x = CO2_predicted[id_use4cal],y=data$CO2[id_use4cal],method="pearson",use="complete.obs")
        
        residuals_INI  <- data$hpp_co2[id_use4cal] - data$CO2[id_use4cal]
        RMSE_INI       <- sqrt(sum((residuals_INI)^2)/n_id_use4cal)
        MAD_INI        <- mad(residuals_INI)
        COR_COEF_INI   <- cor(x = data$hpp_co2[id_use4cal],y=data$CO2[id_use4cal],method="pearson",use="complete.obs")
        
        residuals_PRED <- CO2_predicted[id_use4pred] - data$CO2[id_use4pred]
        RMSE_PRED      <- sqrt(sum((residuals_PRED)^2)/n_id_use4pred)
        MAD_PRED       <- mad(residuals_PRED)
        COR_COEF_PRED  <- cor(x = CO2_predicted[id_use4pred],y=data$CO2[id_use4pred],method="pearson",use="complete.obs")
        
        if(n_id_bottle_cal_periods>16 & CV_mode%in%c(3,4,6,7)){
          residuals_PRED_BCP <- CO2_predicted_BCP[id_use4pred] - data$CO2[id_use4pred]
          RMSE_PRED_BCP      <- sqrt(sum((residuals_PRED_BCP)^2)/n_id_use4pred)
          MAD_PRED_BCP       <- mad(residuals_PRED_BCP)
          COR_COEF_PRED_BCP  <- cor(x = CO2_predicted_BCP[id_use4pred],y=data$CO2[id_use4pred],method="pearson",use="complete.obs")
        }else{
          residuals_PRED_BCP <- NA
          RMSE_PRED_BCP      <- NA
          MAD_PRED_BCP       <- NA
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
        
        if(n_id_bottle_cal_periods>16 & CV_mode%in%c(3,4,6,7)){
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
                                       MAD_FIT=MAD_FIT,
                                       COR_FIT=COR_COEF_FIT,
                                       RMSE_PRED=RMSE_PRED,
                                       MAD_PRED=MAD_PRED,
                                       COR_PRED=COR_COEF_PRED,
                                       RMSE_PRED_BCP=RMSE_PRED_BCP,
                                       MAD_PRED_BCP=MAD_PRED_BCP,
                                       COR_COEF_PRED_BCP=COR_COEF_PRED_BCP,
                                       Q001_FIT=Q001_FIT,
                                       Q099_FIT=Q099_FIT,
                                       Q001_PRED=Q001_PRED,
                                       Q099_PRED=Q099_PRED,
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
                                       CO2_min_FIT        = min(data$CO2[id_use4cal]),
                                       CO2_max_FIT        = max(data$CO2[id_use4cal]),
                                       CO2_min_PRED       = min(data$CO2[id_use4pred]),
                                       CO2_max_PRED       = max(data$CO2[id_use4pred]),
                                       T_min_FIT          = min(data$T[id_use4cal], na.rm=T),
                                       T_max_FIT          = max(data$T[id_use4cal], na.rm=T),
                                       T_min_PRED         = min(data$T[id_use4pred],na.rm=T),
                                       T_max_PRED         = max(data$T[id_use4pred],na.rm=T),
                                       T_sht21_min_FIT    = min(data$sht21_T[id_use4cal]),
                                       T_sht21_max_FIT    = max(data$sht21_T[id_use4cal]),
                                       T_sht21_min_PRED   = min(data$sht21_T[id_use4pred]),
                                       T_sht21_max_PRED   = max(data$sht21_T[id_use4pred]),
                                       H2O_sht21_min_FIT  = min(data$sht21_H2O[id_use4cal]),
                                       H2O_sht21_max_FIT  = max(data$sht21_H2O[id_use4cal]),
                                       H2O_sht21_min_PRED = min(data$sht21_H2O[id_use4pred]),
                                       H2O_sht21_max_PRED = max(data$sht21_H2O[id_use4pred]),
                                       pressure_min_FIT   = min(data$pressure[id_use4cal],  na.rm=T),
                                       pressure_max_FIT   = max(data$pressure[id_use4cal],  na.rm=T),
                                       pressure_min_PRED  = min(data$pressure[id_use4pred],na.rm=T),
                                       pressure_max_PRED  = max(data$pressure[id_use4pred],na.rm=T),
                                       IR_min_FIT         = min(data$hpp_ir[id_use4cal], na.rm=T),
                                       IR_max_FIT         = max(data$hpp_ir[id_use4cal], na.rm=T),
                                       IR_min_PRED        = min(data$hpp_ir[id_use4pred],na.rm=T),
                                       IR_max_PRED        = max(data$hpp_ir[id_use4pred],na.rm=T),
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
          legend("topleft",legend=leg_vec,bg="white",cex=1.75)
          par(family="")
          
          # P3
          
          str31 <- paste("RMSE:", sprintf("%6.2f",RMSE_PRED))
          str32 <- paste("MAD: ", sprintf("%6.2f",MAD_PRED))
          str33 <- paste("COR: ", sprintf("%6.2f",COR_COEF_PRED))
          str34 <- paste("N:   ", sprintf("%6.0f",n_id_use4pred))
          
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
          legend("topleft",legend=c(str31,str32,str33,str34),bg="white",cex=1.75)
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
          
          hist(residuals_plot,breaks = seq(-2e3,2e3,2.5),xlim=c(-50,50),col="slategray",xlab=xlabString,main="",cex.lab=1.5,cex.axis=1.5)
          lines(c(  0,  0),c(-1e9,1e9),col=2,lwd=1)
          lines(c( 20, 20),c(-1e9,1e9),col=2,lwd=1,lty=5)
          lines(c(-20,-20),c(-1e9,1e9),col=2,lwd=1,lty=5)
          
          par(family="mono")
          legend("topleft",legend=c(str41,str42,str43,str44,str45,str46,str47,str33),bg="white",cex=1.75)
          par(family="")
          
          #
          
          dev.off()
          par(def_par)
          
          
          ##
          
          if(n_id_bottle_cal_periods>16 & CV_mode%in%c(3,4,6,7)){
            
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
            legend("topleft",legend=leg_vec,bg="white",cex=2.00)
            par(family="")
            
            # P3
            
            str31 <- paste("RMSE:", sprintf("%6.2f",RMSE_PRED_BCP))
            str32 <- paste("MAD: ", sprintf("%6.2f",MAD_PRED_BCP))
            str33 <- paste("COR: ", sprintf("%6.2f",COR_COEF_PRED_BCP))
            str34 <- paste("N:   ", sprintf("%6.0f",n_id_use4pred))
            
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
            legend("topleft",legend=c(str31,str32,str33,str34),bg="white",cex=2.00)
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
            
            hist(residuals_plot,breaks = seq(-2e3,2e3,2.5),xlim=c(-50,50),col="slategray",xlab=xlabString,main="",cex.lab=1.5,cex.axis=1.5)
            lines(c(  0,  0),c(-1e9,1e9),col=2,lwd=1)
            lines(c( 20, 20),c(-1e9,1e9),col=2,lwd=1,lty=5)
            lines(c(-20,-20),c(-1e9,1e9),col=2,lwd=1,lty=5)
            
            par(family="mono")
            legend("topleft",legend=c(str41,str42,str43,str44,str45,str46,str47,str33),bg="white",cex=2.00)
            par(family="")
            
            #
            
            dev.off()
            par(def_par)
            
            
          }
          
          
          
          ## PLOT : Time-series
          
          # CO2
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS.pdf",sep="")
          
          tmp_1   <- rep(NA,dim(data)[1])
          tmp_2   <- rep(NA,dim(data)[1])
          
          tmp_1[id_use4cal]  <-CO2_predicted[id_use4cal]
          tmp_2[id_use4pred] <-CO2_predicted[id_use4pred]
          
          yyy     <- cbind(data$CO2,
                           data$hpp_co2,
                           tmp_1,
                           tmp_2)
          
          xlabString <- "Date" 
          ylabString <- expression(paste("CO"[2]*" [ppm]"))
          legend_str <- c("PIC CO2",paste(sensor_descriptor,"RAW"),paste(sensor_descriptor,"CAL"),paste(sensor_descriptor,"PRED"))
          plot_ts(figname,data$date,yyy,"all_day2day",NULL,c(350,600),xlabString,ylabString,legend_str)
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS_DAYS.pdf",sep="")
          plot_ts(figname,data$date,yyy,"day",NULL,c(300,600),xlabString,ylabString,legend_str)
          
          figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS_WEEKS.pdf",sep="")
          plot_ts(figname,data$date,yyy,"week",NULL,c(300,600),xlabString,ylabString,legend_str)
          
          rm(tmp_1,tmp_2)
          gc()
          
          
          ### EGU 2018
          
          if(F){
            
            for(aggInt in c("1min","10min")){
              
              id_plot      <- id_use4cal[which(abs(CO2_predicted[id_use4cal]-data$CO2[id_use4cal])<3*RMSE_FIT & data$valve[id_use4cal]==0)]
              n_id_plot    <- length(id_plot)
              
              df_tmp       <- data.frame(date = data$date,
                                         CO2  = data$CO2,
                                         CO2_predicted = CO2_predicted,
                                         stringsAsFactors = F)
              
              if(aggInt=="10min"){
                df_tmp    <- timeAverage(mydata = df_tmp[id_plot,],avg.time = "10 min",statistic = "mean", data.thresh = 0.5,start.date = strptime(strftime(min(df_tmp$date),"%Y%m%d%H0000",tz="UTC"),"%Y%m%d%H%M%S",tz="UTC"))
                id_plot   <- which(!is.na(df_tmp$CO2_predicted) & !is.na(df_tmp$CO2))
                n_id_plot <- length(id_plot)
              }
              
              RMSE_FLT     <- sqrt(sum((df_tmp$CO2_predicted[id_plot]-df_tmp$CO2[id_plot])^2)/n_id_plot)
              COR_COEF_FLT <- cor(x=df_tmp$CO2_predicted[id_plot],y=df_tmp$CO2[id_plot],method="pearson",use="complete.obs")
              
              #
              
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS_EGU2018_",aggInt,".pdf",sep="")
              
              tmp_1   <- rep(NA,dim(df_tmp)[1])
              tmp_2   <- rep(NA,dim(df_tmp)[1])
              
              tmp_1[id_plot]  <- df_tmp$CO2[id_plot]
              tmp_2[id_plot]  <- df_tmp$CO2_predicted[id_plot] + 50
              
              yyy     <- cbind(tmp_1,
                               tmp_2)
              
              xlabString <- "Date" 
              ylabString <- expression(paste("CO"[2]*" [ppm]"))
              legend_str <- c("PIC CO2",paste(sensor_descriptor,"CAL"))
              plot_ts(figname,df_tmp$date,yyy,"all_day2day",NULL,c(350,600),xlabString,ylabString,legend_str)
              
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS_EGU2018_",aggInt,"_DAYS.pdf",sep="")
              plot_ts(figname,df_tmp$date,yyy,"day",NULL,c(300,1200),xlabString,ylabString,legend_str)
              
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_TS_EGU2018_",aggInt,"_WEEKS.pdf",sep="")
              plot_ts(figname,df_tmp$date,yyy,"week",NULL,c(300,1200),xlabString,ylabString,legend_str)
              
              rm(tmp_1,tmp_2)
              gc
              
              #
              
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_TS_EGU2018_",aggInt,".pdf",sep="")
              
              tmp_1   <- rep(NA,dim(data)[1])
              
              tmp_1[id_plot]  <- df_tmp$CO2_predicted[id_plot] - df_tmp$CO2[id_plot]
              
              yyy     <- cbind(tmp_1)
              
              xlabString <- "Date" 
              ylabString <- expression(paste("HPP CO"[2]*" - PIC CO"[2]*" [ppm]"))
              legend_str <- c("HPP residuals")
              plot_ts(figname,df_tmp$date,yyy,"all_day2day",NULL,c(-10,10),xlabString,ylabString,legend_str)
              
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_TS_EGU2018_",aggInt,"_DAYS.pdf",sep="")
              plot_ts(figname,df_tmp$date,yyy,"day",NULL,c(-10,10),xlabString,ylabString,legend_str)
              
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_RES_TS_EGU2018_",aggInt,"_WEEKS.pdf",sep="")
              plot_ts(figname,df_tmp$date,yyy,"week",NULL,c(-10,10),xlabString,ylabString,legend_str)
              
              rm(tmp_1,tmp_2)
              gc
              
              #
              
              str01 <- paste("RMSE:", sprintf("%6.2f",RMSE_FLT))
              str02 <- paste("COR: ", sprintf("%6.2f",COR_COEF_FLT))
              str03 <- paste("N:   ", sprintf("%6.0f",n_id_plot))
              
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_PRED_vs_CO2_PIC_EGU2018_",aggInt,".pdf",sep="")
              
              def_par <- par()
              pdf(file = figname, width=8, height=8, onefile=T, pointsize=12, colormodel="srgb")
              par(mai=c(1,1,0.1,0.1),mfrow=c(1,1))
              
              xrange <- range(c(df_tmp$CO2[id_plot],df_tmp$CO2_predicted[id_plot],380,560))
              
              plot(df_tmp$CO2[id_plot],df_tmp$CO2_predicted[id_plot],xlim=xrange,ylim=xrange,pch=16,cex=0.5,
                   xlab=expression(paste("Picarro CO"[2]*" [ppm]")),ylab=expression(paste("HPP CO"[2]*" [ppm]")),
                   cex.lab=1.25,cex.axis=1.25)
              
              lines(c(-1e9,1e9),c(-1e9,1e9),lwd=1,col=2)
              
              par(family="mono")
              legend("topleft",legend=c(str01,str02,str03),bg="white",cex=1.5)
              par(family="")
              
              dev.off()
              par(def_par)
              
            }
          }
          
          
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
          
          if(n_id_bottle_cal_periods>16 & CV_mode%in%c(3,4,6,7)){
            
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
          
          if(n_id_bottle_cal_periods>16 & CV_mode%in%c(3,4,6,7)){
            
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
          
          
          if(T){
            
            # CO2 Picarro/HPP
            
            figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_CO2_PICARRO_HPP_TS.pdf",sep="")
            
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
          
          if(ith_temp_data_selections == 0 | (ith_temp_data_selections==2 &  CV_mode%in%c(3,6,7,8) & n_id_bottle_cal_periods>16) ){
            
            factors <- data.frame(cn     =c("hpp_co2","CO2","CO2_ppm","T","hpp_temperature_mcu","pressure","hpp_pressure","days","RH","AH","AH_COMP","sht21_AH","H2O","sht21_H2O","sht21_T","sht21_RH","sht21_T_dT","hour"),
                                  cn_unit=c("[ppm]","[ppm]","[ppm]","[deg C]","[deg C]","[hPa]","[hPa]","[days]","[%]","[g/m3]","[g/m3]","[g/m3]","[Vol-%]","[Vol-%]","[deg C]","[%]","[deg C/min]","Hour"),
                                  flag   =c(F,T,F,T,F,T,F,F,T,T,F,F,T,F,F,F,F,F),
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
              if(ith_temp_data_selections==2 &  CV_mode==6 & n_id_bottle_cal_periods>16){
                if(res_mode == "abs"){
                  res_cmp <- CO2_predicted - data$CO2
                }
                if(res_mode == "rel"){
                  res_cmp <- CO2_predicted/data$CO2
                }
              }
              if(CV_mode==7 & n_id_bottle_cal_periods>16){
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
                
                id_grp_03   <- which(data$valve==1)
                n_id_grp_03 <- length(id_grp_03)
                
                id_grp_04   <- which(data$date>=strptime("20180420142700","%Y%m%d%H%M%S",tz="UTC") & data$date<=strptime("20180421142700","%Y%m%d%H%M%S",tz="UTC"))
                n_id_grp_04 <- length(id_grp_04)
                
                
                
                if(n_id_grp_01>0 & n_id_grp_02>0){
                  
                  xrange <- range(data[id_grp_02,pos_factor])
                  
                  if(res_mode == "abs"){
                    yrange <- c(-10,10)
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
                  
                  if(n_id_grp_03>0){
                    points(data[id_grp_03,pos_factor], res_cmp[id_grp_03],pch=16,cex=0.5,col="magenta")
                  }
                  if(n_id_grp_04>0){
                    points(data[id_grp_04,pos_factor], res_cmp[id_grp_04],pch=16,cex=0.5,col="red")
                  }
                  
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
          
          
          ## Boxplot SHT21_T_dT/hpp_temperature_mcu_dT vs RES
          
          for(i in 1:4){
            
            if(i==1){
              delta   <- 0.025
              min_val <- round(min(data$sht21_T_dT,na.rm=T),1)-0.1
              max_val <- round(max(data$sht21_T_dT,na.rm=T),1)+0.1
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SHT21_T_dT_vs_RES_BP.pdf",sep="")
              xlabStr <- "SHT21_T [deg C/min]"
              xrange  <- c(-0.3,0.3)
            }
            if(i==2){
              delta   <- 0.025
              min_val <- round(min(data$hpp_temperature_mcu_dT,na.rm=T),1)-0.1
              max_val <- round(max(data$hpp_temperature_mcu_dT,na.rm=T),1)+0.1
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_hpp_temperature_mcu_dT_vs_RES_BP.pdf",sep="")
              xlabStr <- "MCU_T [deg C/min]"
              xrange  <- c(-0.3,0.3)
            }
            if(i==3){
              delta   <- 1
              min_val <- round(min(data$sht21_T,na.rm=T),0)-1
              max_val <- round(max(data$sht21_T,na.rm=T),0)+1
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_SHT21_T_vs_RES_BP.pdf",sep="")
              xlabStr <- "SHT21_T [deg C]"
              xrange  <- c(-5,35)
            }
            if(i==4){
              delta   <- 1
              min_val <- round(min(data$hpp_temperature_mcu,na.rm=T),0)-1
              max_val <- round(max(data$hpp_temperature_mcu,na.rm=T),0)+1
              figname <- paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_hpp_temperature_mcu_vs_RES_BP.pdf",sep="")
              xlabStr <- "MCU_T [deg C]"
              xrange  <- c(-5,35)
            }
            n_bp    <- round((max_val-min_val)/delta+1)
            
            tmp     <- matrix(NA,ncol=n_bp,nrow=dim(data)[1])
            bpcol   <- rep(NA,n_bp)
            
            for(ii in 1:n_bp){
              
              if(i==1){
                id   <- which(!is.na(CO2_predicted-data$CO2) 
                              & !is.na(data$sht21_T_dT) 
                              & data$sht21_T_dT >= (min_val+(ii-1)*delta) 
                              & data$sht21_T_dT <  (min_val+ii*delta))
              }
              if(i==2){
                id   <- which(!is.na(CO2_predicted-data$CO2) 
                              & !is.na(data$hpp_temperature_mcu_dT) 
                              & data$hpp_temperature_mcu_dT >= (min_val+(ii-1)*delta) 
                              & data$hpp_temperature_mcu_dT <  (min_val+ii*delta))
              }
              if(i==3){
                id   <- which(!is.na(CO2_predicted-data$CO2) 
                              & !is.na(data$sht21_T) 
                              & data$sht21_T >= (min_val+(ii-1)*delta) 
                              & data$sht21_T <  (min_val+ii*delta))
              }
              if(i==4){
                id   <- which(!is.na(CO2_predicted-data$CO2) 
                              & !is.na(data$hpp_temperature_mcu) 
                              & data$hpp_temperature_mcu >= (min_val+(ii-1)*delta) 
                              & data$hpp_temperature_mcu <  (min_val+ii*delta))
              }
              n_id <- length(id)
              
              if(n_id>100){
                tmp[1:n_id,ii] <- CO2_predicted[id]-data$CO2[id]
                bpcol[ii]      <- "slategray"
              }else{
                if(n_id>=10){
                  tmp[1:n_id,ii] <- CO2_predicted[id]-data$CO2[id]
                  bpcol[ii]      <- "gray70"
                }
              }
            }
            
            def_par <- par()
            pdf(file = figname, width=16, height=8, onefile=T, pointsize=12, colormodel="srgb")
            par(mai=c(1,1,0.1,0.1),mfrow=c(1,2))
            
            loc <- round(min_val+(1:n_bp)*delta-delta/2,3)
            boxplot(tmp,at=loc,xlim=xrange,boxwex=(0.75*delta),col = bpcol,outline = F,cex.lab=1.25,cex.axis=1.25,xlab=xlabStr,ylab=expression(paste("CO"[2]*" [ppm]")),xaxt="n")
            axis(1,cex.axis=1.25,cex.lab=1.25)
            lines(c(-1e9,1e9),c(0,0),col=2,lwd=2)
            lines(c(0,0),c(-1e9,1e9),col=2,lwd=2)
            
            #
            
            if(i==1){
              hist(data$sht21_T_dT,seq(min_val,max_val,delta),xlim=xrange,col="slategray",xlab=xlabStr,cex.lab=1.25,cex.axis=1.25,main="")
            }
            if(i==2){
              hist(data$hpp_temperature_mcu_dT,seq(min_val,max_val,delta),xlim=xrange,col="slategray",xlab=xlabStr,cex.lab=1.25,cex.axis=1.25,main="")
            }
            if(i==3){
              hist(data$sht21_T,seq(min_val,max_val,delta),xlim=xrange,col="slategray",xlab=xlabStr,cex.lab=1.25,cex.axis=1.25,main="")
            }
            if(i==4){
              hist(data$hpp_temperature_mcu,seq(min_val,max_val,delta),xlim=xrange,col="slategray",xlab=xlabStr,cex.lab=1.25,cex.axis=1.25,main="")
            }
            
            dev.off()
            par(def_par)
          }
          
          
          
          ## Change of HPP scale
          
          if(T){
            
            min_date_UTC <- strptime("20170102000000","%Y%m%d%H%M%S",tz="UTC")
            max_data_UTC <- strptime("20191029000000","%Y%m%d%H%M%S",tz="UTC")
            n_weeks      <- floor(as.numeric(difftime(time1=max_data_UTC,time2=min_date_UTC,units="weeks",tz="UTC")))
            
            for(ds in c("PRED","PRED_BCP","HPP")){
              
              if(ds=="PRED_BCP" & !CV_mode%in%c(3,4,6,7)){
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
          
          if(T & CV_mode%in%c(3,4,6,7)){
            tmp                   <- data[id_bottle_cal_periods,]
            tmp$CO2_predicted     <- CO2_predicted[id_bottle_cal_periods]
            tmp$CO2_predicted_BCP <- CO2_predicted_BCP[id_bottle_cal_periods]
            tmp$date              <- strftime(tmp$date,"%Y-%m-%d %H:%M:%S",tz="UTC")
            
            write.table(x = tmp,file = paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_BCP.csv",sep=""),sep=";",col.names = T, row.names = F)
            
            rm(tmp)
            gc()
          }
          
          ## TEST: Resiudal function
          
          # id_ok <- which(!is.na(data$sht21_T) & !is.na(data$sht21_T_dT) & !is.na(CO2_predicted))
          # res_approx <- c(NA,exp(data$sht21_T_dT))
          # 
          # M1 <- matrix(NA,ncol=60,nrow=dim(data)[1])
          # 
          # for(nn_M1 in 1:60){
          #   M1[,nn_M1] <- c(data$timestamp[1:(dim(data)[1]-nn_M1+1)],rep(NA,nn_M1-1))
          # }
          
          if(T & CV_mode%in%c(0,4)){
            if(CV_mode==0){
              tmp <- data.frame(timestamp           = data$timestamp,
                                CO2                 = data$CO2,
                                CO2_ppm             = data$CO2_ppm,
                                CO2_predicted       = CO2_predicted,
                                CO2_predicted_BCP   = rep(NA,dim(data)[1]),
                                sht21_T             = data$sht21_T,
                                sht21_T_dT          = data$sht21_T_dT,
                                sht21_H2O           = data$sht21_H2O,
                                hpp_temperature_mcu = data$hpp_temperature_mcu,
                                valve               = data$valve,
                                stringsAsFactors = F)
            }
            
            if(CV_mode==4){
              tmp <- data.frame(timestamp           = data$timestamp,
                                CO2                 = data$CO2,
                                CO2_ppm             = data$CO2_ppm,
                                CO2_predicted       = CO2_predicted,
                                CO2_predicted_BCP   = CO2_predicted_BCP,
                                sht21_T             = data$sht21_T,
                                sht21_T_dT          = data$sht21_T_dT,
                                sht21_H2O           = data$sht21_H2O,
                                hpp_temperature_mcu = data$hpp_temperature_mcu,
                                valve               = data$valve,
                                stringsAsFactors = F)
            }
            
            write.table(x = tmp,file = paste(plotdir,"/",sensor_descriptor,"_",CV_P,"_TEST_DATA.csv",sep=""),sep=";",col.names = T, row.names = F)
          }
          
        }
      }
    }
  }
}


# Write statistics into file

write.table(x = statistics,          file = paste(resultdir,"CV_MODE_",sprintf("%02.0f",as.numeric(CV_mode)),"_statistics_HPP.csv",  sep=""),     sep=";",col.names = T, row.names = F)

write.table(x = statistic_scale,     file = paste(resultdir,"CV_MODE_",sprintf("%02.0f",as.numeric(CV_mode)),"_statistics_scale.csv",sep=""),     sep=";",col.names = T, row.names = F)

write.table(x = statistics_BCP,      file = paste(resultdir,"CV_MODE_",sprintf("%02.0f",as.numeric(CV_mode)),"_statistics_BCP.csv",  sep=""),     sep=";",col.names = T, row.names = F)

write.table(x = statistics_pressure, file = paste(resultdir,"CV_MODE_",sprintf("%02.0f",as.numeric(CV_mode)),"_statistics_pressure.csv", sep=""), sep=";",col.names = T, row.names = F)

write.table(x = statistics_H2O,      file = paste(resultdir,"CV_MODE_",sprintf("%02.0f",as.numeric(CV_mode)),"_statistics_H2O.csv", sep=""),      sep=";",col.names = T, row.names = F)
