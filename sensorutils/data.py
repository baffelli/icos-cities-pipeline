"""
This module contains functions and classes to operate with CarboSense / ICOS-Cities measurement data
"""
from typing import Iterable, Union, List, Optional, Pattern, Dict, Set
from dataclasses import dataclass
from pyparsing import col
import yaml
import pandas as pd
import datetime as dt
import pathlib as pl
import statsmodels.formula.api as smf
import openpyxl as opx
from openpyxl.worksheet.worksheet import Cell
from statsmodels.regression import linear_model 
from statsmodels import tools as sttools
@dataclass 
class CalibrationParameters():
    """
    Simple class to represent two-point calibration parameters
    for a given species
    Attributes
    ----------
    species: str
        The species to be calibrated
    species_cal: str
        The name of the calibrated species
    zero: float
        The instrument zero point
    sensitivity: float
        The instrument sensitivity
    valid_from: date
        The start of the validity range of this calibration
    valid_to:  date
        The end of the validity range of this calibration
    """
    species: str
    zero: float
    sensitivity: float
    valid_from: dt.datetime
    valid_to: dt.datetime

@dataclass
class InstrumentCalibration():
    """
    Class to represent the calibration of a certain
    instrument (= Picarro at a given station) as the calibration
    parameters are time-dependent, the parameters are stored as a list
    """
    parameters: List[CalibrationParameters]
    location: str

def cells_to_df(cells: Iterable[Cell]) -> pd.DataFrame:
    return [(c.value, c.row, c.column) for c in cells]

def read_picarro_calibration_parameters(path: pl.Path) -> pd.DataFrame:
    """
    Reads the picarro CRD calibration parameters
    from an excel file (this is usually filled by the NABEL team)
    and returns a pandas dataframe with the parameters in a sensible format.
    It may be replaced  with another script if the NABEL team can export calibration
    events in a more useful format
    """
    # #Read file
    # wb = opx.load_workbook(path)
    # ws = wb.worksheets[0]
    # #Get headers of groups (These are merged cells with the device type)
    # #group_headers = [(c.value, c.column) for c in ws.unmerge_cells('3')['3']]
    # #Unmerge all cells
    # for merge in list(ws.merged_cells):
    #     ws.unmerge_cells(merge.coord)

    # #Get headers of values
    # value_headers = cells_to_df(ws['3'])
    # #xl = pd.ExcelFile(path, engine="openpyxl")


    #Import the first columns as an index

    colnames = {'Datum':'date', 'Ort':'location', 'Nr.':'cylinder_id', 'Compound':'compound', 'Soll-Konz. [ppm]':'target_concentration'}
    index_skip = 4
    cal_data = pd.read_excel(path, engine="openpyxl",  header=[index_skip]).rename(columns=colnames)
    #Import device ids (second row of the data)
    device_ids =  pd.read_excel(path, engine="openpyxl", nrows=3,  header=[3], usecols=range(5,19,2)).dropna(axis=0,how='all').iloc[0:1].to_dict(orient='records')[0]
    device_mapping = [k for k,v in device_ids.items()]

    #Find column index which is not na (this gives us the corresponding sensor ID)

    cal_data['device'] = cal_data.filter(regex='Ist*', axis=1).set_axis(device_mapping, axis=1, inplace=False).dropna(how='all').notna().idxmax(axis=1)
    # Fill column
    cal_data['measured_concentration'] =  cal_data.filter(regex='Ist*', axis=1).bfill(axis=1).iloc[:,0].astype(float)
    return cal_data.dropna(how='all')[list(colnames.values()) + ['measured_concentration','device']]
    
def make_calibration_parameters_table(table: pd.DataFrame) -> CalibrationParameters:
    """
    From  the table of calibration parameters, make
    an object representing the calibration parameters as a InstrumentCalibration type
    with validity periods and bias/sensitivity values
    """
    def get_cal_params(grp: pd.DataFrame, grouping):
        if grp.shape[0] == 1:
            zero = grp['measured_concentration'] - grp['target_concentration']
            sensitivity = 1
        else:
            lm = linear_model.OLS(grp['target_concentration'], sttools.add_constant(grp['measured_concentration'])).fit()
            zero = lm.params['const']
            sensitivity = lm.params['measured_concentration']
        import pdb; pdb.set_trace()
        species = grp['compound']
        species_cal = species + '_CAL'
        valid_from = grp['date']
        valid_to = grp['next_date']
        CalibrationParameters(species=species, species_cal=species_cal, zero=zero, sensitivity=sensitivity, valid_from=valid_from, valid_to=valid_to)
    #Add the previous date as begin of validity
    table = table.set_index(['location','compound','device'])
    table['next_date'] = table['date'].shift(-1)
    table.reset_index().groupby(["device", "location", "compound","date"]).apply(lambda x: get_cal_params(x, x.name))
