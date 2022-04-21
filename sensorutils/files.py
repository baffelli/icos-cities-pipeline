from argparse import ArgumentError
import imp
from sklearn.exceptions import DataConversionWarning
from yaml import Loader
from sys import intern
from turtle import st
import influxdb
import pandas as pd
import getpass as gp
import pathlib as pl
import datetime as dt
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor
from typing import Callable, Union, List, Optional, Pattern, Dict, Set, Any, Tuple
from dataclasses import dataclass
from abc import ABC, ABCMeta, abstractmethod
import re as re
from . import db as db_utils
from . import data as du
import json
import sqlalchemy as sqa
from sqlalchemy.orm import sessionmaker, Session
import platform
import sqlite3
import yaml
import numpy as np
import pytz
import functools
import string
import random

import itertools
import functools
from .log import logger

"""
This module contains functions used to interact with files and read and write specific file formats used in this 
project

Attributes
----------

"""


def get_user() -> str:
    """
    Get the current user as string.
    This is used to generate user specific path to project folders.
    """
    return gp.getuser()


def get_drive(drv: str) -> pl.Path:
    """
    Get the path to the drive with the letter `drv`. 
    For unix, this assumes that the drives are mapped using ``automount``
    """
    if platform.system() == 'Windows':
        return pl.Path(f'{drv}:/')
    else:
        return pl.Path('/mnt/', get_user(), drv)


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
        return get_drive('K')
    else:
        return get_drive('')


def get_g_drive() -> pl.Path:
    """
    Get the the path of the G: drive, where several directories
    of department-wide data (including some NABEL files) are stored.
    To use it, consult the documentation of :obj:`get_k_drive`
    """
    return get_drive('G')


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
    return pl.Path(get_nabel_dir(), 'Daten', 'Stationen', station_name)


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


def get_available_measurements_on_db(engine: sqa.engine.Engine, station: str,
                                     date_column: sqa.orm.Query,
                                     grouping_column: Optional[str] = None,
                                     group_id: Optional[Union[str, int]] = None,
                                     ) -> pd.DataFrame:
    """
    List all available datasets (grouped by date) on the database for the (NABEL) station with the ID `station`.
    To determine the available files, the following query is used:
    ```
    SELECT DISTINCT
    CAST(FROM_UNIXTIME({date_column} * %date_mult)) AS date
    FROM {station}
    (WHERE {grouping_column} = %group_id )
    ```
    Where the last part is optional and is used to only select a subgroup from the table.
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
    group_column: str 
        An optional column name. Specify the name of a column used to filter the data if only a subgroup is needed
    group_id: str or int
        If `grouping_column` is specified, specifiy the value of the group of interest
    engine: sqlalchemy.engine.Engine
        A sqlalchemy engine

    """
    metadata = sqa.MetaData(bind=engine)
    table = sqa.Table(station, metadata, autoload=True)
    Session = sessionmaker(engine)
    session = Session()
    dq = date_column.select_from(table).distinct()
    if group_id:
        stmt = dq.filter(table.columns[grouping_column] == group_id)
    else:
        stmt = dq
    qs = stmt.with_session(session).statement.compile(
        compile_kwargs={"literal_binds": True})
    return pd.read_sql_query(str(qs), engine, parse_dates=['date'])


def get_all_picarro_files(station: str, rexp: str = '.*\.(csv|CSV)') -> List[pl.Path]:
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
nabel_format_full = "%d.%m.%Y %H:%M:%S"


def read_nabel_csv(path: Union[pl.Path, str], encoding: str = 'latin-1') -> pd.DataFrame:
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
    # Read headers text
    with open(path, 'r', encoding=nabel_encoding) as inf:
        text = [inf.readline() for l in range(skip)]
    # Parse headers
    headers, rest = extract_nabel_headers(''.join(text))
    # Import data
    data = pd.read_csv(path, encoding=nabel_encoding, header=0, names=headers,  skiprows=skip,
                       keep_default_na=True, sep=nabel_sep, index_col=False,  na_values=['', ])
    data['date'] = pd.to_datetime(data['date'], format=nabel_format).dt.tz_localize(
        'CET') - dt.timedelta(minutes=1)
    return data.rename(lambda x: x.replace('_AV', ''), axis='columns')


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
        headers = pd.concat([pd.DataFrame.from_dict(f)
                            for f in visited_children], axis=1).dropna()
        self.headers
        return headers

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
        headers_full = headers.assign(group=group, flags=group_name).groupby(
            'group').apply(lambda x: x.mask(x == '')).dropna(how='all').ffill()
        colnames = ['date', *(headers_full['Kanal'] +
                              '_' + headers_full['flags'])]
        dt = data[0].set_axis(
            colnames, axis=1, inplace=False) if not data[0].empty else pd.DataFrame()
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
    """
    date_from: Optional[dt.datetime]
    path: Optional[str]

    @abstractmethod
    def list_files(self, *args, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def read_file(self, date: dt.datetime) -> pd.DataFrame:
        pass

    @abstractmethod
    def write_file(self, input: pd.DataFrame, temporary: bool = False) -> None:
        pass



@dataclass
class DatabaseSource(DataSource):
    """
    Represents a generic DataSource stored in a database. Additionally
    to the usual properties, it also keeps a database connection object and provides
    a method to connect to the db at runtime by passing the connection object.
    """
    date_column: str
    group: Optional[Union[str, int]] = None
    grouping_key: Optional[str] = None
    eng: Optional[Union[sqa.engine.Engine, influxdb.client.InfluxDBClient]] = None

    @abstractmethod
    def attach_db(self, db:Any) -> None:
        pass

def check_db(fun):
    """
    This decorator ensures that the instance of the object with the decorated method
    has a db instance attached before running the method
    """
    def deco(self:DatabaseSource, *args, **kwargs):
        if self.eng:
            return fun(self, *args, **kwargs)
        else:
            raise AttributeError(f'This instance: {self} has no database connection attached. Please connect using `attach_db`')
    return deco

@dataclass
# TODO make db connection external to object like in InfluxdbSource
class DBSource(DatabaseSource):
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
    group: str
        A group id (to handle tables with multiple "files", e.g multiple sensors in a single table)
    grouping_key: str
        The column name used to generate the grouping key
    """
    db_prefix: Optional[str] = None
    na: Optional[str | int] = None
    
    def attach_db(self, eng:sqa.engine.Engine):
        self.eng = eng
    
    def attach_db_from_config(self):
        if self.db_prefix:
            eng = db_utils.connect_to_metadata_db(group=self.db_prefix)
            self.attach_db(eng)

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

    @check_db
    def list_files(self, group=None, *args) -> pd.DataFrame:
        """
        Lists all available *files* on the data source. One *file* is assumed to be the set of all measurements sharing the same date.

        Returns
        -------
        pandas.DataFrame
            A dataframe with the available files
        """
        meas = get_available_measurements_on_db(
            self.eng, self.path, date_column=self.date_group_query(), grouping_column=self.grouping_key, group_id=group)
        meas_full = meas
        if self.group:
            meas_full['group_id'] = self.group
        return meas_full[meas['date'] > self.date_from]

    @check_db
    def read_file(self, date: dt.datetime, group:Optional[Union[str, int]]=None):
        """
        Reads one file from the database source with a given date.
        If the `group` parameter is passed, only read the part where `grouping_key` 
        equals the value of `group`

        Parameters
        ----------
        date: datetime.datetime
            The date of the dataset to read
        group: 
            Optional group id to filter the data
        """

        
        metadata = sqa.MetaData(bind=self.eng)
        table = sqa.Table(self.path, metadata, autoload=True)
        Session = sessionmaker(self.eng)
        session = Session()
        lb = 'date_group'
        dq = self.date_group_query(label=lb).with_session(
            session).select_from(table).add_columns(*table.columns).subquery()
        fq = session.query(dq).filter(dq.c[lb] == date.strftime('%Y-%m-%d'))
        if group:
            fq_tot = fq.filter(dq.c[self.grouping_key] == self.group)
        else:
            fq = fq
        qs = fq_tot.statement.compile(compile_kwargs={"literal_binds": True})
        logger.debug(f"The query is {qs}")
        return pd.read_sql(qs, self.eng).drop(lb, axis=1).replace(self.na, np.NaN)

    @check_db
    #TODO fix returing of affected
    def write_file(self, input: pd.DataFrame, group:Optional[Union[str, int]]=None, temporary: bool = False) -> pd.DataFrame:
        """
        Writes one file to the database. If the keys are duplicated,
        the data are upserted using :obj:`sensorutils.db.create_upsert_metod`

        Parameters
        ----------
        input: pandas.DataFrame
            The input dataset to write. It should have the same columns as contained in the database
        temporary: bool
            If set to `True` only perform insert in a temporary uncommited transaction. Useful when testing import
        group:
            Set if you want to write to a specific group only (it is only needed to return the affected rows for that group, not for writing)
        Returns
        -------
        pd.DataFrame
            The affected rows in database format. The rows are taken from the database by filtering the date column on the dates available
            in the source file. In combination with ``temporary=True`` this facilitates the testing of data loading as the data is copied with a database
            transaction that is rolled back after returning the affected rows.
        """
        # Connect to the database
        metadata = sqa.MetaData(bind=self.eng)
        metadata.reflect()
        # Create an upsert method
        method = db_utils.create_upsert_metod(metadata)
        # Fill na with the na value
        input_filled = input.fillna(self.na)
        Session = sessionmaker(bind=self.eng)
        session = Session()
        output_table = metadata.tables[self.path]
        with self.eng.connect() as con:
            tran = con.begin()
            input_filled.to_sql(self.path, con, method=method,
                                index=False, if_exists='append')
            if group and self.grouping_key:
                new_data = session.query(output_table).filter(output_table.columns[self.date_column].between(
                input[self.date_column].min(), input[self.date_column].max())
                & (output_table.columns[self.grouping_key] == group))
                affected = pd.read_sql(new_data.statement.compile(compile_kwargs={"literal_binds": True}), con)
                logger.debug(f"Affected rows {affected}")
            if temporary:
                logger.debug("Rolling back as the temporary flag was set")
                tran.rollback()
                return pd.DataFrame()
            else:
                tran.commit()
                logger.debug("Commiting affected rows")
                return pd.DataFrame()


def combine_first_column(df:pd.DataFrame) -> pd.DataFrame:
    """
    Coalesce duplicated columns into a single column
    """
    def cf(cols: pd.DataFrame) -> pd.Series:
        return cols.bfill(axis=1).iloc[:,0]
    df_col = df.groupby(df.columns, axis=1).agg(cf)
    return df_col

@dataclass
class InfluxdbSource(DatabaseSource):
    """
    This subclass of :obj:`DataSource`represents a datasource located in an influxdb data. One *file* 
    in this dataset corresponds to a day of measurements for a specific sensor node.
    To set the sensor, the object uses the 'group' attribute.
    The object expexcts that you pass a :obj:`influxdb.InfluxDBClient` connection in order
    for the methods to work. This can be done at run time by monkey patching.
    """
    eng: Optional[influxdb.client.InfluxDBClient] = None
    group: Optional[Union[str, int]] = None
    grouping_key: Optional[str] = 'node'
    date_column: str = 'time'

    def attach_db(self, db: influxdb.InfluxDBClient) -> None:
        self.eng = db

    @check_db
    def list_files(self, *args, group=None) -> pd.DataFrame:
        if self.eng:
            gq = f"{self.grouping_key} =~ /{group}/ AND" if group else ""
            q = f"""
            SELECT
            *
            FROM
            (
            SELECT COUNT("value") FROM "measurements" WHERE {gq} sensor =~ /senseair|sensirion|battery|calibration/ AND time > '2017-01-01 00:00:00' GROUP BY time(1d)
            )
            WHERE count > 0
            """
            # Send query
            fs = self.eng.query(q, epoch='s', params={
                                f"{self.grouping_key}": group})
            # Make datafame
            if len(fs) > 0:
                available_data = pd.DataFrame([a for a in fs['measurements']])
                available_data['date'] = pd.to_datetime(
                    available_data[self.date_column], unit='s')
                available_data['path'] = self.path
                available_data['group'] = group
            else:
                available_data = pd.DataFrame(columns=['date'])
            return available_data
    @check_db
    def read_file(self, date: dt.datetime, group:str=None, av_time:Optional[str]=None) -> pd.DataFrame:
        """
        Reads a date of measurements from the influxdb db
        with the given date. Optionally specify `group`
        if you want to read only the subgroup specified at initalisation
        by the parameter `grouping_key`.
        If you want to perform additional time averaging, pass the influxdb averaging string with the keyword `av_time`.
        """
        # format date
        ut = pytz.utc
        min_ts, max_ts = [
            f"{ut.localize(d).isoformat()}" for d in day_ts(date)]
        #Make query componend conditionally
        gq = f"{self.grouping_key} =~ /{group}/ AND" if group else ""
        aq = f", time({av_time})" if av_time else ""
        sq = 'mean("value") AS value' if av_time else "value"
        query = \
            f"""
        SELECT
        *
        FROM
        (
            SELECT {sq} FROM "measurements" WHERE {gq}
            sensor =~ /senseair*|sensirion*|battery|calibration*/ AND "time" > $start_ts AND "time" < $end_ts
            GROUP BY "{self.grouping_key}","sensor"{aq} fill(previous)
        )
        """
        logger.debug(f'The query is {query}')
        fs = self.eng.query(query, epoch='s', bind_params={
                               "group": group, "start_ts": min_ts, "end_ts": max_ts})
        df = du.influxql_results_to_df(fs)
        grp = [self.date_column, self.grouping_key] if self.grouping_key else [
            self.date_column]
        df_wide = du.reshape_influxql_results(df, grp, 'sensor', 'value')
        return df_wide

    @check_db
    def write_file(self, input: pd.DataFrame, temporary: bool = False) -> None:
        pass


def day_ts(day: dt.datetime) -> Tuple[dt.datetime, dt.datetime]:
    """
    Return the minimum and maximum date object for a given day
    """
    return (day.replace(hour=0, minute=0, second=0), day.replace(hour=23, minute=59, second=59))


@dataclass
class CsvSource(DataSource):
    """
    This subclass of :obj:`DataSource`represents a datasource located in a CSV file. One *file* 
    in this dataset corresponds to a day of measurements for that station.
    """
    re: str

    def list_files(self, *args, group=None) -> pd.DataFrame:
        """
        Lists all the available files in the :obj:`path` with the specified regex.
        """
        files = get_available_files(self.path, rexp=self.re)
        return files[files['date'] > self.date_from]

    def read_file(self, date: dt.datetime, group=None) -> Optional[pd.DataFrame]:
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

        ds = pd.concat([read_nabel_csv(f)
                       for f in fl_to_read['path']], axis=1, join='outer')
        return ds

    def write_file(self, data, temporary: bool = False):
        pass

@dataclass
class ColumnMapping(ABC):
    """
    Abstract base class representing the mapping of columns 
    between two pandas.DataFrame. This is needed to flexibly
    support complex operations, ranging from just renaming the column
    to SQL-like joins and other column operations.
    """
    name: str
    datatype: str
    na: Optional[Union[str, int]]

    @abstractmethod
    def make_mapper(self) -> Callable:
        pass

@dataclass
class NominalColumnMapping(ColumnMapping):
    """
    Represents the mapping between columns
    as a renaming operation
    """
    source_name: str

    def make_mapper(self) -> Callable:
        def rename(df: pd.DataFrame, eng:Optional[sqa.engine.Engine]) -> pd.DataFrame:
            renamed = df.rename(columns={self.source_name:self.name})
            renamed_selected = renamed[self.name] if self.name in renamed.columns else pd.DataFrame()
            return renamed_selected
        return rename


@dataclass
class SQLColumnMapping(ColumnMapping):
    """
    Represents the mapping between columns in two systems
    as a SQL statement

    Attributes
    ----------
    name: str
        The output column name
    query: str
        A SQL query used to generated the output column
    source_name: str
        An alternative to the query for simple renaming
    datatype: str
        The datatype of the output column
    na: str or float or int
        The value used to represent NaN / Null in the destination system
    """
    name: str
    query: str


    def make_query(self) -> sqa.sql.literal_column:
        """
        Returns the column transformation as a :obj:`sqlalchemy.sql.literal_column` element
        """
        return sqa.sql.literal_column(self.query).label(self.name)

    def make_mapper(self) -> Callable:
        """
        Makes a mapping function that maps
        a column by using a SQL query
        """
        def mapper(df: pd.DataFrame, eng:sqa.engine.Engine) -> pd.DataFrame:
            tb = id_generator()
            df.to_sql(tb, eng,  )
            # Represent the source as sqlalchemy object
            md = sqa.MetaData(bind=eng)
            md.reflect()
            # Apply mapping
            sq = sqa.select([self.make_query()]).select_from(md.tables[tb])
            mapped = pd.read_sql(sq, eng)
            return mapped
        return mapper

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
    filter:
        An optional SQL where expression to filter the source data
    """
    source: DataSource
    dest: DataSource
    columns: List[ColumnMapping]

    def connect_all_db(self, 
        source_eng:Optional[Union[sqa.engine.Engine, influxdb.client.InfluxDBClient]]=None, 
        dest_eng:Optional[Union[sqa.engine.Engine, influxdb.client.InfluxDBClient]]=None) -> None:
        """
        Connect all dbs using the passed arguments `source_eng` and `dest_eng` or
        the specified connection configuration group name as given by `db_prefix`.
        """
        def connect_db(ds:DatabaseSource, eng:Optional[Union[sqa.engine.Engine, influxdb.client.InfluxDBClient]]=None) ->DatabaseSource:
            if isinstance(ds, DatabaseSource):
                if eng:
                    ds.attach_db(eng)
            if isinstance(ds, DBSource):
                if ds.db_prefix:
                    ds.attach_db_from_config()
            return ds

        self.source = connect_db(self.source)
        self.dest = connect_db(self.dest)

    def list_files_missing_in_dest(self, group=None, all: bool = False, backfill: int = 3) -> List[dt.datetime]:
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
        source_files = self.source.list_files(group=group).sort_values('date').drop_duplicates()
        destination_files = self.dest.list_files(group=group).sort_values('date').drop_duplicates()
        if len(source_files) > 0:
            source_dates = source_files['date'].dt.to_pydatetime()
            dest_dates = destination_files['date'].dt.to_pydatetime()
            # Find dates to backfill
            backfill_dates = set(
                source_dates[(dt.datetime.now() - source_dates) <= dt.timedelta(days=backfill)])
            # Find dates missing in dest
            missing_dates = (set(source_dates) - set(dest_dates)
                             ).union(backfill_dates) if not all else source_dates
        else:
            missing_dates = []
        return sorted(missing_dates)

    def mapping_to_query(self) -> List[sqa.sql.text]:
        """
        Transform the mapping specified in the objects `columns` attribute into a
        list of :obj:`sqlalchemy.sql.text` expression. These are then combined
        to prepare a query in 
        """
        q = [expr.make_query() for expr in self.columns if not expr.source_name]
        return q
    
    def data_type_mapping(self) -> Dict[str, str]:
        """
        Transform the column mapping into
        a dictonary of datatype mapping used by pandas.astype
        """
        return {c.name: c.datatype for c in self.columns}
    
    def unique_columns(self) -> Set[str]:
        """
        Return only unique column names
        from the objects column mapping
        """
        return set([c.name for c in self.columns])

    def na_mapping(self):
        """
        Transform the column mapping into
        a dictionary of na values for each column
        """
        return {c.name: c.na for c in self.columns if c.na is not None}

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
        # Connect to a in-memory SQLlite database
        # and write to a table
        engine = sqa.engine.create_engine('sqlite://', echo=False)
        #Store in engine
        file.to_sql("source", engine)
        # Represent the source as sqlalchemy object
        md = sqa.MetaData(bind=engine)
        md.reflect()
        #Map columns
        mapped_cols = [s.make_mapper()(file, engine) for s,file,engine in itertools.product(self.columns,[file],[engine])]
        #Exclude empty
        valid_cols = [m for m in mapped_cols if not m.empty]
        mapped = pd.concat(valid_cols, axis=1)
        #Remove duplicate columns by coalescing
        mapped_dedup = combine_first_column(mapped)
        mapped_na = mapped_dedup.fillna({k:v for k,v in self.na_mapping().items() if k in mapped_dedup.columns})
        # Replace nas and convert columns to proper type
        dt_mapping = {k:v for k,v in self.data_type_mapping().items() if k in mapped_na.columns}
        mapped_new_types = mapped_na.astype(dt_mapping)
        return mapped_new_types

    def transfer_file(self, date: dt.datetime, temporary: bool = False) -> pd.DataFrame:
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


def try_parse(in_dt: Union[str, dt.datetime], fmt: str):
    """
    Tries parsing a string as a date, unless
    the passed object is already a date object
    """
    if isinstance(in_dt, str):
        out = dt.datetime.strptime(in_dt, fmt)
    elif isinstance(in_dt, dt.datetime):
        out = in_dt
    else:
        raise TypeError("Input is neither date nor string")
    return out


class DataMappingFactory():
    """

    Factory class to implement creation of data SourceMapping objects
    from dictionaries or configuration files
    """

    @staticmethod
    def create_column_mapping(d: dict) -> ColumnMapping:
        """
        Creates a new :obj:`sensorutils.files.ColumnMapping` object from a dict.
        """
        em =  f"The input {d} is not a valid configuration of a ColumnMapping"
        try:
            if 'query' in d.keys():
                mp = SQLColumnMapping(**d)
            elif 'source_name' in d.keys():
                mp = NominalColumnMapping(**d)
            else:
                raise TypeError(em)
        except TypeError as e:
            raise TypeError(em)
        return mp

    @staticmethod
    def create_data_source(source: dict) -> Union[DBSource, CsvSource, InfluxdbSource]:
        source_type = source.pop('type')
        source['date_from'] = try_parse(
            source['date_from'], '%Y-%m-%d %H:%M:%S')
        if source_type == 'file':
            return CsvSource(**source)
        elif source_type == 'DB':
            return DBSource(**source)
        elif source_type == 'influxDB':
            return InfluxdbSource(**source)
        else:
            raise TypeError(
                f'The data source type {source_type} does not exist')

    @staticmethod
    def create_mapping(source: dict, dest: dict, coulmns: List[ColumnMapping]):
        sd = DataMappingFactory.create_data_source(source)
        dd = DataMappingFactory.create_data_source(dest)
        return SourceMapping(source=sd, dest=dd, columns=coulmns)

    @staticmethod
    def read_config(path: Union[pl.Path, str], type='yaml') -> Dict[str, SourceMapping]:
        """
        Reads a YAML/JSON configuration file representing a group of SourceMapping object
        and returns a list. The validation of the file is delegated to the constructors 
        of the various objects
        """
        if type not in ('yaml', 'json'):
            raise ArgumentError("`type` must be in ('yaml', 'json')")
        with open(path, 'r') as js:
            loader: function = (lambda x: yaml.load(
                x, Loader)) if type == 'yaml' else (lambda x: json.load(x))
            configs = loader(js)
            mappings = {k: DataMappingFactory.create_mapping(it['source'], it['dest'], [ DataMappingFactory.create_column_mapping(i) for i in it['columns']]) for k, it in configs.items()}
            return mappings


@dataclass
class RefProcessingConfiguration():
    """
    Class to configure the processing of reference data (from picarro or licor instrument)

    Attributes
    ----------
    source: str
        the source table of the data
    location: str
        the name of the location (used to lookup the sensor in the deployment table)
    sensor: str
        The string corresponding to the `Type` column in the `Sensors` table in the metadata DB. Usually *picarro* for
        Picarro CRDS instruments
    valid_from: date
        The beginning of validity period for this configuration
    valid_to: date
        The ending of validity period for this configuration
    species: str
        The column containing the species value
    species_cal: str 
        The column or sql expression giving the calibrated species value
    calibrated: boolean
        If set to true, the data is assumed to be already calibrated

    """
    source: str
    location: str
    sensor: str
    valid_from: dt.datetime
    valid_to: dt.datetime
    species: str
    species_cal: str
    calibrated: bool


def read_picarro_processing_configuration(path: Union[str, pl.Path]) -> List[RefProcessingConfiguration]:
    with open(path, 'r') as content:
        configs = yaml.load(content)
    return [RefProcessingConfiguration(**c) for c in configs]


def read_picarro_data(path: Union[str, pl.Path], tz='CET') -> pd.DataFrame:
    """
    Reads the data in the picarro `.dat` format into
    a :obj:`pandas.DataFrame`. 
    Assumes that the date is in CET, returns a date column in UTC. 
    For another timezone, use the `tz` argument
    """
    logger.debug(f"Loading file {path}")
    data = pd.read_csv(path, delimiter=r"\s+", parse_dates=[['DATE', 'TIME']])
    col_map = {
        'DATE_TIME': 'date',
        'CO2_sync': 'CO2',
        'CO2_dry_sync': 'CO2_DRY',
        'H2O_sync': 'H2O',
        'ALARM_STATUS': 'status'
    }
    data_map = data.rename(columns=col_map)[[l for l in col_map.values()]]
    data_map['date'] = data_map['date'].dt.tz_localize(tz).dt.tz_convert('UTC')
    data_map['valid'] = data_map['status'].eq(0).astype(int)
    data_map['CO2_DRY_F'] = data_map['valid']
    data_map['H2O_F'] = data_map['valid']
    data_map['CO2_F'] = data_map['valid']
    return data_map[['date','CO2', 'CO2_DRY', 'CO2_F', 'CO2_DRY_F', 'H2O', 'H2O_F']]


def read_climate_chamber_data(path:  Union[str, pl.Path], tz='CET') -> pd.DataFrame:
    """
    Reads the climate chamber data from the given path and return a pandas dataframe
    The date and time is supposed to be in CET for the input data, returns UTC data.
    """
    logger.debug(f"Loading file {path}")
    cols = ['date', 'target_temperature', 'temperature', 'target_RH', 'RH']
    def date_parser(s): return dt.datetime.strptime(
        s.strip(), nabel_format_full)
    data = pd.read_csv(path, sep=';', encoding='latin1', skiprows=3, header=0, date_parser=date_parser, parse_dates=[
                       0], names=cols, usecols=[i for i in range(len(cols))])
    data['date'] = pd.to_datetime(
        data['date']).dt.tz_localize(tz).dt.tz_convert('UTC')
    return data

def read_new_climate_chamber_data(path: pl.Path, tz='CET') -> pd.DataFrame:
    """
    Reads the climate chamber data from the given path and returns a pandas DataFrame.
    The date and time is supposed to be in CET (with switch to DST at the correct time).
    Returns data in utc format
    """
    name_mapping =  {'Zeit':'time', 'Phase':'phase', 'CO2_soll':'CO2_setpoint', 'Status':'status', 'T':'T', 'RH':'RH','CO2ref':'CO2_LI850'}
    #Regex for the phase column
    soll_re = re.compile(r"(\d+)\/(\d+)\s* (\d+)")
    dt = pd.read_excel(path)
    dt_new = dt.copy().rename(columns=name_mapping)
    dt_new['date'] = dt_new['time'].dt.tz_localize(tz='CET').dt.tz_convert('UTC')
    dt_new['status'] = dt_new['status'].apply(lambda x: du.map_climate_chamber_status_code(du.ClimateChamberStatusCode(x)))
    dt_new['T_F'] = dt_new['status']
    dt_new['RH_F'] = dt_new['status']
    setpoints = dt_new['phase'].str.extract(soll_re).rename(columns={0:'T_soll', 1:'RH_soll', 2:'CO2_soll'}).astype(np.float64)
    dt_full = pd.concat([dt_new, setpoints], axis=1)
    return dt_full.reset_index()[['date', 'T', 'T_F', 'RH', 'RH_F', 'CO2_soll', 'RH_soll', 'T_soll']]

def tz_to_epoch(series: pd.Series) -> pd.Series:
    """
    Convert a series in pandas.TimeStamp to unix epoch
    """
    return (series - pd.Timestamp("1970-01-01",  tz='UTC')) // pd.Timedelta('1s')

def read_pressure_data(path: Union[str, pl.Path], tz='CET'):
    """
    Reads the exported pressure measurements from the NABEL calibration laboratory
    and stores them in a pandas dataframe. The date is assumed to be in CET, the output in UTC
    """
    cols = ['date', 'pressure']

    def date_parser(s): return dt.datetime.strptime(
        s.strip(), '%d.%m.%y %H:%M')
    data = pd.read_csv(path, sep=';', encoding='latin1', skiprows=3, header=0, date_parser=date_parser, parse_dates=[
                       0], names=cols, usecols=[i for i in range(len(cols))])
    data['date'] = pd.to_datetime(
        data['date']).dt.tz_localize(tz).dt.tz_convert('UTC')
    return data


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    """
    Generates a random id of given length
    """
    return ''.join(random.choice(chars) for _ in range(size))