from argparse import ArgumentError
import imp
from sys import intern
from turtle import st
import pandas as pd
import getpass as gp
import pathlib as pl
import datetime as dt
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor
from typing import Union, List, Optional, Pattern, Dict, Set
from dataclasses import dataclass
from abc import ABC, abstractmethod
import re as re
from . import db as db_utils
import json
import sqlalchemy as sqa
from sqlalchemy.orm import sessionmaker, Session
import platform
import sqlite3
import yaml
import numpy as np

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

def get_g_drive() -> pl.Path:
    """
    Get the the path of the G: drive, where several directories
    of department-wide data (including some NABEL files) are stored.
    To use it, consult the documentation of :obj:`get_k_drive`
    """
    if platform.system() == 'Windows':
        return pl.Path('G:/')
    else:
        return pl.Path('/mnt', get_user(), 'G')


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
    return dt.datetime.strptime(date, nabel_format)

def format_nabel_date(date: dt.datetime) -> str:
    """
    Formats a datetime object into the date format used by NABEL exports
    """
    return dt.datetime.strftime(date, '%y%m%d')
 
def get_available_measurements_on_db(station: str, date_column:sqa.orm.Query, db_group='CarboSense_MySQL') -> pd.DataFrame:
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
    date_column: sqlalchemy.orm.Query
        Column name containing the time information (as numeric timestamp) or a select expression to generate the date
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
    dq = date_column.select_from(table).distinct().with_session(session)
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
    Returns
    -------
    parsimonious.Grammar
        The parsed grammar tree for the NABEL data file

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


"""
Mapping between numeric codes in NABEL export files and textual description of these flags.
For more information on the mapping, ask the NABEL team
"""
nabel_flags = {
    5: 'AV',
    7: 'VALID_num',
    8: 'VALID',
    10: 'MIN',
    11: 'MIN_time',
    12: 'MAX',
    13: 'MAX_time',
    14: 'agg_duration_min',
    15: 'agg_duration_hour',
    18: 'F_num',
    19: 'F'
}

nabel_format = "%d.%m.%Y %H:%M"


def read_nabel_csv(path: Union[pl.Path, str], encoding:str='latin-1') -> pd.DataFrame:
    """
    Reads a NABEL csv export file considering the special headers and returns a :obj:`pandas.DataFrame` with the data.
    Be careful with datetime operations: NABEL data is exported as CET by default. The output timestamp corresponds to the input timestamp
    minus one minute (because NABEL uses a different convention)

    Parameters
    ----------
    path: pathlib.Path or str
        The path to read the file from
    encoding: str
        A string to define the data encoding

    Returns
    -------
    pandas.DataFrame
        The parsed data
    """
    nabel_encoding = encoding
    nabel_sep = ';'
    skip = 4
    #Read headers text
    with open(path, 'r', encoding=nabel_encoding) as inf:
        text = [inf.readline() for l in range(skip)]
    #Parse headers
    headers, rest = extract_nabel_headers(''.join(text))
    #Import data
    data = pd.read_csv(path, encoding=nabel_encoding, header=0, names=headers,  skiprows=skip, keep_default_na=True, sep=nabel_sep, index_col=False,  na_values=['',])
    data['date'] = pd.to_datetime(data['date'], format=nabel_format).dt.tz_localize('CET') - dt.timedelta(minutes=1)
    return data.rename(lambda x: x.replace('_AV',''), axis='columns')

class NabelVisitor(NodeVisitor):
    """
    This class implements a :obj:`parsimonious.nodes.NodeVisitor`
    used to parse a NABEL file header and return the correct columsn and flags
    The various `visit_*` functions implement visiting functions for the different elements of the grammar.
    """

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
    """
    Parses an input text into NABEL headers using the grammar specified in :func:`parse_nabel_headers`
    
    Arguments
    ---------
    text: str
        The input text to parse
    """
    tree = parse_nabel_headers(text)
    visitor = NabelVisitor()
    return visitor.visit(tree)
 
@dataclass
class DataSource(ABC):
    """
    Abstract base class to represent a generic data source which is used to list datasets or create and read
    existing datasets (grouped by date). This is an helper class to assist with incremental import of reference (picarro CRDS) data
    from NABEL exports or from other database sources.
    The data is assumed to be grouped by station and date and one instance of this class represents all available
    datasets from a specific station, which is represented by the attribute `path`.

    Attributes
    ----------
    path: str
        The "path" of the datasource
    date_from: 
        The first date available on the datasource. Used to filter older datasets out
    na:
        A dict representing missing values (used when storing the data). 
    """
    path: str
    date_from: dt.datetime
    na: Union[str, float]

    @abstractmethod
    def list_files(self) -> pd.DataFrame:
        pass

    @abstractmethod 
    def read_file(self, date: str) -> pd.DataFrame:
        pass

    @abstractmethod 
    def write_file(self, input: pd.DataFrame) -> None:
        pass

@dataclass
class DBSource(DataSource):
    """
    This subclass of :obj:`DataSource`represents a datasource located on a relational database.
    It is assumed that the datasource represents data from a single measurement station and that the data is 
    stored in a single table identified by the attribute :obj:`path`. One *file* in this datasource is represented
    by all measurement for a single date. This is meant to help with incremental loading of data from other sources (e.g NABEL exports), where
    the data is exported by daily files.
    In order to identify the numerber of *files*,  the class methods use SQL queries on the table, therefore the date column
    is specified at the class initalisation time as the :obj:`date_column` parameter as a sql select statement generating the unix time stamp of
    the date group.

    Attributes
    ----------
    db_prefix: str
        The group of the mariadb options files used to connect to the specific DB. See :obj:`db.connect_to_metadata_db` for more information.
    date_column: str
        The database column representing the date (as a unix timestamp) or a SQL expression returning the unix timestamp of the date
    na: str
        A string representing missing values
    """
    db_prefix: str
    date_column: str
    na: str

    def date_expression(self) -> sqa.sql.text:
        """
        Safely transforms the :obj:`date_column` expression into
        a SQL statement for sqlalchemy. Used to construct the query in `list_files`
        """
        return sqa.sql.text(f"{self.date_column}")
    
    def date_group_query(self, label='date') -> sqa.orm.Query:
        """
        Creates a :obj:`sqlalchem.orm.Query` representation of 
        the query used to identify the files in the database backend

        Parameters
        ----------
        label: str
            An optional label to rename the column of the date group
        """
        return sqa.orm.Query(sqa.cast(sqa.func.from_unixtime(self.date_expression()), sqa.DATE).label(label))

    def list_files(self) -> pd.DataFrame:
        """
        Lists all available *files* on the data source. One *file* is assumed to be the set of all measurements sharing the same date.
        
        Returns
        -------
        pandas.DataFrame
            A dataframe with the available files
        """
        meas =  get_available_measurements_on_db(self.path, date_column=self.date_group_query(), db_group=self.db_prefix)
        return meas[meas['date'] > self.date_from]

    def read_file(self, date: dt.datetime):
        """
        Reads one file from the database source with a given date

        Parameters
        ----------
        date: datetime.datetime
            The date of the dataset to read

        """

        eng = db_utils.connect_to_metadata_db(group=self.db_prefix)
        metadata = sqa.MetaData(bind=eng)
        table = sqa.Table(self.path, metadata, autoload=True)
        Session = sessionmaker(eng)
        session = Session()
        lb = 'date_group'
        dq = self.date_group_query(label=lb).with_session(session).select_from(table).add_columns(*table.columns).subquery()
        fq = session.query(dq).filter(dq.c[lb]==date.strftime('%Y-%m-%d'))
        qs = fq.statement.compile(compile_kwargs={"literal_binds": True})
        return pd.read_sql(qs, eng).drop(lb, axis=1).replace(self.na, np.NaN)
    
    def write_file(self, input: pd.DataFrame, temporary:bool=False) -> pd.DataFrame:
        """
        Writes one file to the database. If the keys are duplicated,
        the data are upserted using :obj:`sensorutils.db.create_upsert_metod`

        Parameters
        ----------
        input: pandas.DataFrame
            The input dataset to write. It should have the same columns as contained in the database
        temporary: bool
            If set to `True` only perform insert in a temporary uncommited transaction. Useful when testing import

        Returns
        -------
        pd.DataFrame
            The affected rows in database format. The rows are taken from the database by filtering the date column on the dates available
            in the source file. In combination with ``temporary=True`` this facilitates the testing of data loading as the data is copied with a database
            transaction that is rolled back after returning the affected rows.
        """
        #Connect to the database
        eng = db_utils.connect_to_metadata_db(group=self.db_prefix)
        metadata = sqa.MetaData(bind=eng)
        metadata.reflect()
        #Create an upsert method
        method  = db_utils.create_upsert_metod(metadata)
        #Fill na with the na value
        input_filled = input.fillna(self.na)
        Session = sessionmaker(bind=eng)
        session = Session()
        output_table = metadata.tables[self.path]
        with eng.connect() as con:
            tran = con.begin()
            input_filled.to_sql(self.path, con, method=method,  index=False, if_exists='append')
            new_data = session.query(output_table).filter(output_table.columns.timestamp.between(input['timestamp'].min(), input['timestamp'].max())).statement.compile(compile_kwargs={"literal_binds": True})
            affected = pd.read_sql(new_data, con)
            print(f"Affected rows {affected}")
            if temporary:
                print("Rolling back as the temporary flag was set")
                tran.rollback()
                return affected
            else:
                tran.commit()
                print("Commiting affected rows")
                return affected            



        

@dataclass
class CsvSource(DataSource):
    """
    This subclass of :obj:`DataSource`represents a datasource located in a CSV file. One *file* 
    in this dataset corresponds to a day of measurements for that station.
    """
    re: str

    def list_files(self) -> pd.DataFrame:
        """
        Lists all the available files in the :obj:`path` with the specified regex.
        """
        files = get_available_files(self.path, rexp = self.re)
        return files[files['date']  > self.date_from]

    def read_file(self, date:dt.datetime) -> Optional[pd.DataFrame]:
        """
        Read a file with the given date from the datasource.
        If there are multiple files with the same date these are (outer) joined on the date key.

        Parameters
        ----------
        date: str
            The date to read

        Returns
        -------
        pandas.DataFrame
            The dataframe with all measurements
        """
        fl = self.list_files()
        fl_to_read = fl[fl['date'] == date]

        ds = pd.concat([read_nabel_csv(f) for f in fl_to_read['path']], axis=1, join='outer')
        return ds
    
        
    def write_file(self, data):
        pass


@dataclass
class ColumnMapping():
    """
    Represents the mapping between columns in two systems
    as a SQL statement

    Attributes
    ----------
    name: str
        The output column name
    query: str
        A SQL query used to generated the output column
    datatype: str
        The datatype of the output column
    na: str or float or int
        The value used to represen NaN / Null in the destination system
    """
    name: str
    query: str
    datatype: str
    na: Union[str, float, int]

    def make_query(self) -> sqa.sql.text:
        """
        Returns the column transformation as a :obj:`sqlalchemy.sql.text` element
        """
        return sqa.sql.text(f"{self.query} AS {self.name}")
    


@dataclass
class SourceMapping():
    """
    Represents the mapping between
    two datasources. Also stores the column mapping 

    Attributes
    ----------
    source: DataSource
        The source DataSource object
    source: DataSource
        The destination DataSource object
    columns:
        A mapping between source and destination columns. The dictionary key represent
        the destination column name, the value the source column name (or a SQL expression 
        if the column is derived from multiple source columns).
    """
    source: DataSource
    dest: DataSource
    columns: List[ColumnMapping]

    def list_files_missing_in_dest(self, all:bool=False, backfill:int=3) -> Set:
        """
        Lists the files available in the source that are missing in the destination. Using the `backfill` parameter,
        a certain number of files in the past (with respect to current date) is added to the list

        Parameters
        ----------
        backfill: int
            The number of days to backfill from today
        all: bool
            If set to `True`, list all files as missing. Useful for reimporting all data
        Returns
        -------
        pandas.DataFrame   
            A :obj:`pandas.DataFrame` object with dates and paths of the missing files
        """
        sf = self.source.list_files()
        df = self.dest.list_files()
        source_dates = (sf['date'].astype(object).unique())
        dest_dates = (sf['date'].astype(object).unique())
        #Find dates to backfill
        backfill_dates = set(source_dates[(dt.datetime.now() - source_dates) < dt.timedelta(days=backfill)])
        #Find dates missing in dest
        missing_dates = (set(source_dates) -  set(dest_dates)).union(backfill_dates) if not all else source_dates
        return missing_dates

    def mapping_to_query(self) -> List[sqa.sql.text]:
        """
        Transform the mapping specified in the objects `columns` attribute into a
        list of :obj:`sqlalchemy.sql.text` expression. These are then combined
        to prepare a query in 
        """
        q = [expr.make_query() for  expr in self.columns]
        return q

    def map_file(self, file: pd.DataFrame) -> pd.DataFrame:
        """
        Map one file from source to destination using :obj:`columns` 
        and returns a :obj:`pandas.DataFrame`.
        The mapping is done using an in-memory SQLlite database to allow SQL-like expression in the datasource
        configuration.

        Parameters
        ----------
        file: pandas.DataFrame
            The dataset to write
        
        Returns
        -------
        pandas.DataFrame
            A dataframe with mapped columns and names.
        """
        #Connect to a in-memory SQLlite database
        # and write to a table
        engine = sqa.engine.create_engine('sqlite://',echo=False)
        #Store in engine
        file.to_sql("source", engine)
        #Represent the source as sqlalchemy object
        md = sqa.MetaData(bind=engine)
        md.reflect()
        #Create mappping query
        cols = self.mapping_to_query()
        #Apply mapping
        sq = sqa.select([q for q in cols]).select_from(md.tables["source"])
        mapped  = pd.read_sql(sq, engine)
        # Replace nas and convert columns to proper type
        return mapped.astype({c.name:c.datatype for c in self.columns}).fillna({c.name:c.na for c in self.columns})

    def transfer_file(self, date:dt.datetime, temporary:bool=False) -> pd.DataFrame:
        """
        Transfers the file identified by the date `date` from the source to the
        destination. 

        Parameters
        ----------
        date: datetime.datetime
            The date of the file
        Returns
        -------
        pandas.DataFrame
            A dataframe of the affected rows. Useful for testing / debugging. 
        """
        data = self.source.read_file(date)
        data_mapped = self.map_file(data)
        affected = self.dest.write_file(data_mapped, temporary=temporary)
        return affected


def try_parse(in_dt: Union[str, dt.datetime], fmt:str):
    """
    Tries parsin a string as a date, unless
    the passed object is already a date object
    """
    if isinstance(in_dt, str):
        out = dt.datetime.strptime(in_dt, fmt)
    elif isinstance(in_dt, dt.datetime):
        out = in_dt
    else:
        raise TypeError("Input is neither date nor striny")
    return out




class DataMappingFactory():
    """

    Factory class to implement creation of data SourceMapping objects
    from dictionaries or configuration files
    """

    @staticmethod
    def create_column_mapping(d:dict) -> ColumnMapping:
        """
        Creates a new :obj:`sensorutils.files.ColumnMapping` object from a dict.
        """
        try:
            return ColumnMapping(**d)
        except TypeError as e:
            raise TypeError(f"The input {d} is not a valid configuration of a ColumnMapping")
            

    @staticmethod
    def create_data_source(source: dict) -> Union[DBSource, CsvSource]:
        source_type = source.pop('type')
        source['date_from'] = try_parse(source['date_from'], '%Y-%m-%d %H:%M:%S')
        if source_type == 'file':
            source_obj = CsvSource(**source) 
        elif source_type == 'DB':
            source_obj = DBSource(**source) 
        return source_obj

    @staticmethod
    def create_mapping(source: dict, dest:dict, coulmns:dict):
        source = DataMappingFactory.create_data_source(source)
        dest = DataMappingFactory.create_data_source(dest)
        return SourceMapping(source=source, dest=dest, columns=coulmns)

    @staticmethod
    def read_config(path: Union[pl.Path, str], type='yaml') -> List[SourceMapping]:
        """
        Reads a YAML/JSON configuration file representing a group of SourceMapping object
        and returns a list. The validation of the file is delegated to the constructors 
        of the various objects
        """
        if type not in ('yaml', 'json'):
            raise ArgumentError("`type` must be in ('yaml', 'json')")
        loaders = {'yaml':yaml.load, 'json':json.load}
        with open(path, 'r') as js:
            configs = loaders[type](js)
            mappings = [DataMappingFactory.create_mapping(it['source'], it['dest'], [DataMappingFactory.create_column_mapping(i) for i  in it['columns']])  for k, it in configs.items()]
            return mappings