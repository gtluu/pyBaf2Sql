import sqlite3
import pandas as pd
from pyBaf2Sql.baf import *
from pyBaf2Sql.error import *


class BafData(object):
    """
    Class containing metadata from BAF files and methods from Baf2Sql library to work with BAF format data.

    :param bruker_d_folder_name: Path to Bruker .d directory containing analysis.baf and analysis.sqlite.
    :type bruker_d_folder_name: str
    :param baf2sql: Library initialized by pyBaf2Sql.init_baf2sql.init_baf2sql_api().
    :type baf2sql: ctypes.CDLL
    :param raw_calibration: Whether to use recalibrated data (False) or not (True), defaults to False.
    :type raw_calibration: bool
    :param all_variables: Whether to load all variables from analysis.sqlite database, defaults to False.
    :type all_variables: bool
    """
    def __init__(self, bruker_d_folder_name: str, baf2sql, raw_calibration=False, all_variables=False):
        """
        Constructor Method
        """
        self.api = baf2sql
        self.source_file = bruker_d_folder_name
        self.handle = open_storage(self.api, self.source_file, raw_calibration)
        if self.handle == 0:
            throw_last_baf2sql_error(self.api)
        self.all_variables = all_variables

        get_sqlite_cache_filename_v2(self.api, self.source_file, self.all_variables)
        self.conn = sqlite3.connect(os.path.join(bruker_d_folder_name, 'analysis.sqlite'))

        self.analysis = None

        self.get_db_tables()
        self.close_sql_connection()

    def __del__(self):
        """
        Close connection to raw data handle.
        """
        if hasattr(self, 'handle'):
            close_storage(self.api, self.handle, self.conn)

    def get_db_tables(self):
        """
        Get a dictionary of all tables found in the analysis.sqlite SQLite database in which the table names act as
        keys and the tables as a pandas.DataFrame of values; this is stored in pyBaf2Sql.classes.BafData.analysis.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = cursor.fetchall()
        table_names = [table[0] for table in table_names]
        self.analysis = {name: pd.read_sql_query("SELECT * FROM " + name, self.conn) for name in table_names}
        self.analysis['Properties'] = {row['Key']: row['Value']
                                       for index, row in self.analysis['Properties'].iterrows()}
        cursor.close()

    def close_sql_connection(self):
        """
        Close the connection to analysis.sqlite.
        """
        self.conn.close()
