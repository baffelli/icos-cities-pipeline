#from CarbosenseDatabaseTools.import_picarro_data import read_nabel_csv
from sys import intern
import pandas as pd
import getpass as gp
import pathlib as pl
import datetime as dt
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor
from typing import Union, List, Optional, Pattern
from dataclasses import dataclass
from abc import ABC, abstractmethod
import re as re
from . import db as db_utils
import json
import sqlalchemy as sqa
from sqlalchemy.orm import sessionmaker
import platform

"""
This module contains functions used to interact with files and read and write specific file formats used in this 
project
"""

def get_user() -> str: 
    """
    Get the current user as string.
    This is used to generate user specific path to project folders.
    """
    return gp.getuser()

def get_k_drive() -> pl.PurePath: 
    """
    Get the path to the K: drive, where the project folders are stored.
    For unix, this assumes that the folders in K: are mapped using ``automount``
    under ``/mnt/{user_name}``. To set this up, contact Stephan Henne or Patrick Burkhalter from the ICT
    support.

    Returns
    -------

    pt: pathlib.Path 
        the path of the K: drive 
    """
    if platform.system() == 'Windows':
        return pl.Path('K:/')
    else:
        return pl.Path('/mnt', get_user())

def get_nabel_dir() -> pl.PurePath: 
    """
    Get path to the NABEL project folder
    """
    return pl.Path(get_k_drive(), 'Nabel')

def get_nabel_station_data_path(station_name: str) -> pl.Path:
    """
    Get path to NABEL data exports for a certain station

    Parameters
    ----------
    station_name: str
        The name of the station which data is desired

    Returns
    -------
    pl.Path
        The full path of the nabel data
    """
    return pl.Path(get_nabel_dir(),'Daten','Stationen', station_name)

def parse_nabel_date(date: str) -> dt.datetime:
    """
    Parses a string according to the format used by NABEL exports for filenames.
    For more information, contact Beat Schwarzenbach.

    Parameters
    ----------
    date : str
        The string to parse

    Returns
    -------
    datetime.datetime
        Parsed date

    """
    return dt.datetime.strptime(date, '%y%m%d')

def parse_nabel_datetime(date: str) -> dt.datetime:
    """
    Parses the datetime string in NABEL exports into a datetime object

    Parameters
    ----------
    date : str
        the date to parse

    Returns
    -------
    datetime.datetime
        Parsed date
    """
    return dt.datetime.strptime(date, '%d.%m.%Y %H:%M')

def format_nabel_date(date: dt.datetime) -> str:
    """
    Formats a datetime object into the date format used by NABEL exports
    """
    return dt.datetime.strftime(date, '%y%m%d')
 
def get_available_measurements_on_db(station: str, date_column:str='timestamp', date_mult:float=1, db_group='CarboSense_MySQL') -> pd.DataFrame:
    """
    List all available datasets (grouped by date) on the database for the (NABEL) station with the ID `station`.
    To determine the available files, the following query is used:
    ```
    SELECT DISTINCT
    CAST(FROM_UNIXTIME({date_column} * %date_mult)) AS date
    FROM {station}
    ```
    To connect to the database, this function assumes that a file called ``.my.cnf`` exists in the user home 
    directory. For more information, see :obj:`db`
    Parameters
    ----------
    station: str
        The name of the station, which corresponds to the database table of interest
    date_column: str
        Column name containing the time information (as numeric timestamp)
    date_mult: float
        Multiplier for the timestamp, used to deal with table where the timestamp is stored as bigint and
        therefore would cause an  overflow when converting to a datetime object using MariaDB
    db_group: str
        The name of the options group used to get the database connection. 

    """
    eng = db_utils.connect_to_metadata_db(group=db_group)
    metadata = sqa.MetaData(bind=eng)
    table = sqa.Table(station, metadata, autoload=True)
    Session = sessionmaker(eng)
    session = Session()
    dq = session.query(sqa.cast(sqa.func.from_unixtime(table.columns[date_column] * date_mult), sqa.DATE).label('date')).distinct()
    qs = dq.statement.compile(compile_kwargs={"literal_binds": True})
    return pd.read_sql_query(str(qs), eng, parse_dates =['date'])

def get_all_picarro_files(station: str, rexp:str='.*\.(csv|CSV)') -> List[pl.Path]:
    """
    Lists all (csv) files in the NABEL export folder of the station `station`. This
    path is determined using :func:`get_nabel_dir`

    Parameters
    ----------
    station: str
        The name of the nabel station
    rexp: str
        A regular expression to filter the file types
    """

    rg = re.compile(rexp)
    return [f for f in get_nabel_station_data_path(station).iterdir() if re.match(rg, f.name)]

def get_date_from_filename(filename: pl.Path) -> Optional[dt.datetime]:
    """
    Parses a NABEL export filename to extract the date. This is done using :fun:`parse_nabel_date`
    
    Parameters
    ----------
    filename: pathlib.Path
        The filename to parse
    """
    m = re.search('(\d){6}', filename.name)
    res = parse_nabel_date(m.group()) if m else None
    return res

def get_available_files(station, rexp='*.csv') -> pd.DataFrame:
    """
    Gets a list of available files at a given NABEL station and returns
    a :obj:`pandas.DataFrame` with date, station name and fully resolved path to the file.
    The dates are parsed using :fun:`parse_nabel_date`.

    Parameters
    ----------
    station: str
        The name of the nabel station
    rexp: str
        A regular expression to filter the file types
    """
    return pd.DataFrame([(get_date_from_filename(f), f, station) for f in get_all_picarro_files(station, rexp=rexp)], columns=['date', 'path', 'station'])

def parse_nabel_headers(text: str) -> Grammar:
    """
    A :obj:`parsimonious` grammar definition used to correctly parse NABEL CSV exports
    
    Parameters
    ----------
    text: str
        The text to parse
    Returns:
        parsimonious.
    """
    grammar = Grammar(
        r"""
        data_file = header grouping_row data?
        header = (metadata_row)*
        data = (data_row)*
        metadata_row = kw entry newline
        data_row = date (value)* newline
        grouping_row = "Spalten-ID" kw_sep (value)* newline
        date = ~"\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}"
        entry = (value)*
        value = sep entry_terminal
        entry_terminal = ~"[^;\n]*"
        sep = ";"
        kw = kw_word kw_sep 
        kw_word = "Station" / "Kanal" / "ME" 
        kw_sep = ":"
        newline = ~"\n*"
        """
    )
    return grammar.parse(text)



nabel_flags = {
    5: 'AV',
    7: 'VALID',
    8: 'VALID_text',
    10: 'MIN',
    11: 'MIN_time',
    12: 'MAX',
    13: 'MAX_time',
    14: 'agg_duration_min',
    15: 'agg_duration_hour',
    18: 'F',
    19: 'F_text'
}


def read_nabel_csv(path: Union[pl.Path, str]) -> pd.DataFrame:
    nabel_encoding = 'latin-1'
    nabel_sep = ';'
    skip = 4
    #Read row names
    with open(path, 'r', encoding=nabel_encoding) as inf:
        text = [inf.readline() for l in range(skip)]
    #Get headers
    headers, rest = extract_nabel_headers(''.join(text))
    #Import data
    data = pd.read_csv(path, encoding=nabel_encoding, header=0, names=headers,  skiprows=skip, keep_default_na=True, sep=nabel_sep, index_col=False,  na_values=['',])
    data['date'] = pd.to_datetime(data['date']).dt.tz_localize('CET')
    return data.rename(lambda x: x.replace('_AV',''), axis='columns')

class NabelVisitor(NodeVisitor):

    def __init__(self):
        self.df = pd.DataFrame()
        self.headers = None

    def generic_visit(self, node, visited_children):
        return visited_children or node

    def visit_header(self, node, visited_children):
        headers  = pd.concat([pd.DataFrame.from_dict(f) for f in visited_children], axis=1).dropna()
        self.headers 
        return  headers


    def visit_grouping_row(self, node, visited_children):
        kw, sep, groups, nl = visited_children
        group_nm = pd.Series([int(e) for e in groups if e != ''])
        group_indices = (group_nm.diff() < -1).cumsum()
        return group_indices, group_nm
    
    def visit_data(self, node, visited_children):
        return pd.DataFrame(visited_children)

    def visit_metadata_row(self, node, visited_children):
        kw, entry, nl = visited_children
        return {kw: entry}
    
    def visit_kw(self, node, visited_children):
        kw, _ = visited_children
        return kw 

    def visit_kw_word(self, node, visited_children):
        return node.text

    def visit_entry(self, node, visited_children):
        return visited_children

    def visit_value(self, node, visited_children):
        sep, val = node
        return val.text

    def visit_data_row(self, node, visited_children):
        date, values, *rest = visited_children
        return [date, *values]

    def visit_data_file(self, node, visited_children):
        headers, (group, group_code), data, *rest = visited_children
        group_name = group_code.replace(nabel_flags)
        headers_full = headers.assign(group=group, flags=group_name).groupby('group').apply(lambda x: x.mask(x == '')).dropna(how='all').ffill()
        colnames = ['date', *(headers_full['Kanal'] + '_' + headers_full['flags'])]
        dt = data[0].set_axis(colnames, axis=1, inplace=False) if not data[0].empty else pd.DataFrame()
        return colnames, dt

    def visit_date(self, node, visited_children):
        return parse_nabel_datetime(node.text)
    
            

def extract_nabel_headers(text: str) -> pd.DataFrame:
    tree = parse_nabel_headers(text)
    import pdb; pdb.set_trace()
    visitor = NabelVisitor()
    return visitor.visit(tree)
 
@dataclass
class DataSource(ABC):
    path: str

    @abstractmethod
    def list_files(self, rexp: Union[str, Pattern]) -> pd.DataFrame:
        pass

    @abstractmethod 
    def read_file(date) -> pd.DataFrame:
        pass

    @abstractmethod 
    def write_file(input: pd.DataFrame) -> None:
        pass
@dataclass
class DBSource(DataSource):
    db_prefix: str
    date_column: str
    date_mult: float

    def list_files(self) -> pd.DataFrame:
        return get_available_measurements_on_db(self.path, date_mult=self.date_mult, date_column=self.date_column, db_group=self.db_prefix)

    def read_file(self, date):
        eng = db_utils.connect_to_metadata_db(group=self.db_prefix)
        metadata = sqa.MetaData(bind=eng)
        table = sqa.Table(self.path, metadata, autoload=True)
        Session = sessionmaker(eng)
        session = Session()
        dq = session.query(*table.columns, sqa.cast(sqa.func.from_unixtime(table.columns[self.date_column] * self.date_mult), sqa.DATE).label('group_date')).subquery()
        fq = session.query(dq).filter(dq.c.group_date==date)
        qs = fq.statement.compile(compile_kwargs={"literal_binds": True})
        return pd.read_sql(str(qs), eng).drop("group_date", axis=1)
    
    def write_file(self, input: pd.DataFrame) -> None:
        return super().write_file()

@dataclass
class CsvSource(DataSource):
    re: str

    def list_files(self) -> pd.DataFrame:
        return get_available_files(self.path, rexp = self.re)

    def read_file(self, date) -> pd.DataFrame:
        #path = pl.Path(get_nabel_station_data_path(self.path), )
        return read_nabel_csv(date)
        
    def write_file(self, data):
        pass


@dataclass
class SourceMapping():
    source: DataSource
    dest: DataSource

    def list_files_missing_in_dest(self) -> pd.DataFrame:
        sf = self.source.list_files()
        df = self.dest.list_files()
        merged_data = pd.merge(sf, df, on='date', how='outer', indicator=True)
        missing_data = merged_data[merged_data['_merge']=='left_only']
        return missing_data




class DataMappingFactory():

    @staticmethod
    def create_data_source(source: dict) -> DataSource:
        source_type = source.pop('type')
        if source_type == 'file':
            source = CsvSource(**source) 
        elif source_type == 'DB':
            source = DBSource(**source) 
        return source
    @staticmethod
    def create_mapping(source: dict, dest:dict):
        source = DataMappingFactory.create_data_source(source)
        dest = DataMappingFactory.create_data_source(dest)
        return SourceMapping(source=source, dest=dest)

    @staticmethod
    def read_json(path: Union[pl.Path, str]) -> List[SourceMapping]:
        with open(path, 'r') as js:
            configs = json.load(js)
            mappings = [DataMappingFactory.create_mapping(it['source'], it['dest'])  for k, it in configs.items()]
            return mappings