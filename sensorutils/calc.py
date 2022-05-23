"""
This module contains functions to perform 
sensor-specific calculations such as conversion between units, 
computation of absolute moisture from relative moisture, conversion
between molar and mass concentrations and similar operations.
It also includes some useful constants

Attributes
----------
T0: float
    The 0 C temperature in K
TC: float
    The critical point temperature of water
PC: float
    The critical point pressure of water
P0: float
    Reference pressure at sea level
"""
import numpy as np
#TODO mark constants as final in python > 3.9
#from typing import Final

"""
Constants
"""
T0 = 273.15
TC = 647.096
PC = 22.064e6
P0 = 1013.25e2
G0 = 9.80665
M  = 0.0289644 
R_STAR =  8.3144598 

def absolute_temperature(t: float):
    return t + T0


def saturation_vapor_pressure(t:float) -> float:
    """
    Compute saturation vapor pressure of water (in pascal)
    at given absolute temperature 
    Parameters
    ----------
    t: float    
        Temperature in K
    """
    coef_1 = -7.85951783
    coef_2 =  1.84408259
    coef_3 = -11.7866497
    coef_4 =  22.6807411
    coef_5 = -15.9618719
    coef_6 =  1.80122502
    theta =  (1 - t / TC)
    pw = np.exp(TC / t * 
        (coef_1 * theta + 
        coef_2 * theta**1.5 + 
        coef_3 * theta**3 + 
        coef_4 * theta**3.5 + 
        coef_5 * theta**4+
        coef_6 * theta**7.5)) * PC
    return pw

def rh_to_ah(rh: float, t:float) -> float:
    """
    Conver relative to absolute humidity (in g/m^3)
    given temperature and pressure
    Parameters
    ----------
    rh: float
        Relative humidity in %
    pressure: float
        Pressure 
    """
    pw = saturation_vapor_pressure(t)
    C = 2.16679
    return C * pw / t * rh / 100

def rh_to_molar_mixing(rh:float, t:float, p:float) -> float:
    """
    Convert the give relative humidity (in 100%)
    to a molar mixing ratio (in ppm)
    Parameters
    ----------
    rh: float
        The relative humidity
    t: float
        The absolute temperature in K
    p: float
        Pressure in Pa
    """
    return saturation_vapor_pressure(t) * rh / 100  * 1/p

def molar_mixing_to_rh(ppm: float, t: float, p: float) -> float:
    """
    Convert the given molar mixing ratio to
    relative humidity at the given pressure and temperature
    """
    return ppm / saturation_vapor_pressure(t)  * p

def dry_to_wet_molar_mixing(conc:float, H2O:float) -> float:
    """
    Convert the dry molar mixing ratio to wet molar mixing ratio given
    the water mixing ratio (both in ppm)
    """
    return conc * (1 - H2O / 100)


def pressure_interpolation(p_ref: float, t_ref: float, h_ref: float, h_stat: float) -> float:
    """
    Computes the pressure [in Pa] at a station located at altitude `h_stat` from a measurement
    `p_ref` of a measurement station at `h_ref` with temperature `t_ref` [in K] using the barometric formula
    """
    return p_ref * np.exp(- G0 * M * (h_stat - h_ref) / (T0 * R_STAR))