# The following code has been modified from TIMSCONVERT 1.0.0.
# For more information see: https://github.com/gtluu/timsconvert/tree/manuscript_v1.0.0


import sqlite3
import pandas as pd
from pyBaf2Sql.baf import *
from pyBaf2Sql.util import *
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
    :param sql_chunksize: Number of rows to read from SQL database query at once when reading tables/views from
            analysis.sqlite.
    :type sql_chunksize: int
    """
    def __init__(self, bruker_d_folder_name: str, baf2sql, raw_calibration=False, all_variables=True,
                 sql_chunksize=1000):
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

        self.get_db_tables(sql_chunksize=sql_chunksize)
        self.close_sql_connection()

    def __del__(self):
        """
        Close connection to raw data handle.
        """
        if hasattr(self, 'handle'):
            close_storage(self.api, self.handle, self.conn)

    def get_db_tables(self, sql_chunksize=1000):
        """
        Get a dictionary of all tables found in the analysis.sqlite SQLite database in which the table names act as
        keys and the tables as a pandas.DataFrame of values; this is stored in pyBaf2Sql.classes.BafData.analysis.

        :param sql_chunksize: Number of rows to read from SQL database query at once when reading tables/views from
            analysis.sqlite.
        :type sql_chunksize: int
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = cursor.fetchall()
        table_names = [table[0] for table in table_names]
        self.analysis = {name: pd.concat([i for i in pd.read_sql_query("SELECT * FROM " + name,
                                                                       self.conn,
                                                                       chunksize=sql_chunksize)],
                                         ignore_index=True)
                         for name in table_names if name != 'SupportedVariables'}
        self.analysis['Variables'] = pd.concat([i for i in pd.read_sql_query('SELECT * FROM Variables',
                                                                             self.conn,
                                                                             chunksize=sql_chunksize)],
                                               ignore_index=True)
        self.analysis['Properties'] = {row['Key']: row['Value']
                                       for index, row in self.analysis['Properties'].iterrows()}
        cursor.close()

    def close_sql_connection(self):
        """
        Close the connection to analysis.sqlite.
        """
        self.conn.close()


class BafSpectrum(object):
    """
    Class for parsing and storing spectrum metadata and data arrays from BAF format data.

    :param baf_data: BafData object containing metadata from analysis.sqlite database.
    :type baf_data: pyBaf2Sql.classes.BafData
    :param frame: ID of the frame of interest.
    :type frame: int
    :param mode: Data array mode, either "profile", "centroid", or "raw".
    :type mode: str
    :param profile_bins: Number of bins to bin spectrum to.
    :type profile_bins: int
    :param mz_encoding: m/z encoding command line parameter, either "64" or "32".
    :type mz_encoding: int
    :param intensity_encoding: Intensity encoding command line parameter, either "64" or "32".
    :type intensity_encoding: int
    """

    def __init__(self, baf_data, frame: int, mode: str, profile_bins=0, mz_encoding=64, intensity_encoding=64):
        """
        Constructor Method
        """
        self.baf_data = baf_data
        self.scan_number = None
        self.scan_type = None
        self.ms_level = None
        self.mz_array = None
        self.intensity_array = None
        self.mobility_array = None
        self.polarity = None
        self.centroided = None
        self.retention_time = None
        self.coord = None
        self.total_ion_current = None
        self.base_peak_mz = None
        self.base_peak_intensity = None
        self.high_mz = None
        self.low_mz = None
        self.target_mz = None
        self.isolation_lower_offset = None
        self.isolation_upper_offset = None
        self.selected_ion_mz = None
        self.selected_ion_intensity = None
        self.selected_ion_mobility = None
        self.selected_ion_ccs = None
        self.charge_state = None
        self.activation = None
        self.collision_energy = None
        self.frame = frame
        self.parent_frame = None
        self.parent_scan = None
        self.ms2_no_precursor = False
        self.mode = mode
        self.profile_bins = profile_bins
        self.mz_encoding = mz_encoding
        self.intensity_encoding = intensity_encoding

        self.get_baf_data()

    def get_baf_data(self):
        frames_dict = self.baf_data.analysis['Spectra'][self.baf_data.analysis['Spectra']['Id'] ==
                                                        self.frame].to_dict(orient='records')[0]
        acquisitionkey_dict = self.baf_data.analysis['AcquisitionKeys'][self.baf_data.analysis['AcquisitionKeys']['Id'] ==
                                                                        frames_dict['AcquisitionKey']].to_dict(orient='records')[0]
        # Polarity == 0 -> 'positive'; Polarity == 1 -> 'negative"?
        if int(acquisitionkey_dict['Polarity']) == 0:
            self.polarity = '+'
        elif int(acquisitionkey_dict['Polarity']) == 1:
            self.polarity = '-'
        self.centroided = get_centroid_status(self.mode)
        self.retention_time = float(frames_dict['Rt']) / 60
        self.mz_array, self.intensity_array = extract_baf_spectrum(self.baf_data,
                                                                   self.frame,
                                                                   self.mode,
                                                                   self.profile_bins,
                                                                   self.mz_encoding,
                                                                   self.intensity_encoding)
        if self.mz_array is not None and self.intensity_array is not None and \
                self.mz_array.size != 0 and self.intensity_array.size != 0 and \
                self.mz_array.size == self.intensity_array.size:
            self.total_ion_current = sum(self.intensity_array)
            base_peak_index = np.where(self.intensity_array == np.max(self.intensity_array))
            self.base_peak_mz = self.mz_array[base_peak_index][0].astype(float)
            self.base_peak_intensity = self.intensity_array[base_peak_index][0].astype(float)
            self.high_mz = float(max(self.mz_array))
            self.low_mz = float(min(self.mz_array))
            # MS1
            if int(acquisitionkey_dict['ScanMode']) == 0:
                self.scan_type = 'MS1 spectrum'
                self.ms_level = 1
            # Auto MS/MS and MRM MS/MS
            elif int(acquisitionkey_dict['ScanMode']) == 2:
                steps_dict = self.baf_data.analysis['Steps'][self.baf_data.analysis['Steps']['TargetSpectrum'] ==
                                                             self.frame].to_dict(orient='records')[0]
                self.scan_type = 'MSn spectrum'
                self.ms_level = 2
                self.target_mz = float(self.baf_data.analysis['Variables'][(self.baf_data.analysis['Variables']['Spectrum'] == self.frame) &
                                                                           (self.baf_data.analysis['Variables']['Variable'] == 7)].to_dict(orient='records')[0]['Value'])
                isolation_width = float(self.baf_data.analysis['Variables'][(self.baf_data.analysis['Variables']['Spectrum'] == self.frame) &
                                                                            (self.baf_data.analysis['Variables']['Variable'] == 8)].to_dict(orient='records')[0]['Value'])
                self.isolation_lower_offset = isolation_width / 2
                self.isolation_upper_offset = isolation_width / 2
                self.selected_ion_mz = float(steps_dict['Mass'])
                self.charge_state = float(self.baf_data.analysis['Variables'][(self.baf_data.analysis['Variables']['Spectrum'] == self.frame) &
                                                                              (self.baf_data.analysis['Variables']['Variable'] == 6)].to_dict(orient='records')[0]['Value'])
                self.collision_energy = float(self.baf_data.analysis['Variables'][(self.baf_data.analysis['Variables']['Spectrum'] == self.frame) &
                                                                                  (self.baf_data.analysis['Variables']['Variable'] == 5)].to_dict(orient='records')[0]['Value'])
                self.activation = 'collision-induced dissociation'
                self.parent_frame = int(frames_dict['Parent'])
            # isCID MS/MS
            elif int(acquisitionkey_dict['ScanMode']) == 4:
                self.scan_type = 'MSn spectrum'
                self.ms_level = 2
                self.activation = 'in-source collision-induced dissociation'
                self.collision_energy = float(self.baf_data.analysis['Variables'][(self.baf_data.analysis['Variables']['Spectrum'] == self.frame) &
                                                                                  (self.baf_data.analysis['Variables']['Variable'] == 5)].to_dict(orient='records')[0]['Value'])
                self.ms2_no_precursor = True
            # bbCID MS/MS
            elif int(acquisitionkey_dict['ScanMode']) == 5:
                self.scan_type = 'MSn spectrum'
                self.ms_level = 2
                self.activation = 'collision-induced dissociation'
                self.collision_energy = float(self.baf_data.analysis['Variables'][(self.baf_data.analysis['Variables']['Spectrum'] == self.frame) &
                                                                                  (self.baf_data.analysis['Variables']['Variable'] == 5)].to_dict(orient='records')[0]['Value'])
                self.ms2_no_precursor = True
