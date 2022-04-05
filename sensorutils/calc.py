"""
This module contains functions to perform 
sensor-specific calculations such as conversion between units, 
computation of absolute moisture from relative moisture, conversion
between molar and mass concentrations and similar operations
"""
import numpy as np

"""
Constants
"""
T_0 = 273.15
TC = 647.096
PC = 22.064e6

def absolute_temperature(t: float):
    return t + T_0


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

def rh_to_molar_mixing(rh:float, t:float, p:float):
    """
    Conver the give relative humidity (in 100%)
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
