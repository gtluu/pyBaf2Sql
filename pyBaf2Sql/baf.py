# The following code has been modified from TIMSCONVERT 1.0.0.
# For more information see: https://github.com/gtluu/timsconvert/tree/manuscript_v1.0.0


import os
from ctypes import c_uint64, POINTER, c_double, c_float, c_uint32, create_string_buffer
import numpy as np
from pyBaf2Sql.util import get_encoding_dtype, bin_profile_spectrum
from pyBaf2Sql.error import throw_last_baf2sql_error


def close_storage(baf2sql, handle, conn):
    """
    Close BAF dataset.

    :param baf2sql: Library initialized by pyBaf2Sql.init_baf2sql.init_baf2sql_api().
    :type baf2sql: ctypes.CDLL
    :param handle: Handle value for BAF dataset initialized using pyBaf2Sql.baf.open_storage().
    :type handle: int
    :param conn: SQL database connection to analysis.sqlite.
    :type conn: sqlite3.Connection
    :return: Tuple of the handle and connection.
    :rtype: tuple
    """
    if handle is not None:
        baf2sql.baf2sql_array_close_storage(handle)
        handle = None
    if conn is not None:
        conn.close()
        conn = None
    return handle, conn


def get_num_elements(baf2sql, handle, identity):
    """
    Get the number of elements stored in an array.

    :param baf2sql: Library initialized by pyBaf2Sql.init_baf2sql.init_baf2sql_api().
    :type baf2sql: ctypes.CDLL
    :param handle: Handle value for BAF dataset initialized using pyBaf2Sql.baf.open_storage().
    :type handle: int
    :param identity: ID of the desired array.
    :type identity: str | int
    :return: Number of elements in the array of the specified ID.
    :rtype: int
    """
    n = c_uint64(0)
    if not baf2sql.baf2sql_array_get_num_elements(handle, identity, n):
        throw_last_baf2sql_error(baf2sql)
    return n.value


def open_storage(baf2sql, bruker_d_folder_name, raw_calibration=False):
    """
    Open BAF dataset and return a non-zero instance handle to be passed to subsequent API calls.

    :param baf2sql: Library initialized by pyBaf2Sql.init_baf2sql.init_baf2sql_api().
    :type baf2sql: ctypes.CDLL
    :param bruker_d_folder_name: Path to Bruker .d directory containing analysis.baf and analysis.sqlite.
    :type bruker_d_folder_name: str
    :param raw_calibration: Whether to use recalibrated data (False) or not (True), defaults to False.
    :type raw_calibration: bool
    :return: Non-zero instance handle.
    :rtype: int
    """
    handle = baf2sql.baf2sql_array_open_storage(1 if raw_calibration else 0,
                                                os.path.join(bruker_d_folder_name,
                                                             'analysis.baf').encode('utf-8'))
    return handle


def read_double(baf2sql, handle, identity):
    """
    Read array into a user provided buffer. The data will be converted to the requested type on the fly. The provided
    buffer must be large enough to hold the entire array.

    :param baf2sql: Library initialized by pyBaf2Sql.init_baf2sql.init_baf2sql_api().
    :type baf2sql: ctypes.CDLL
    :param handle: Handle value for BAF dataset initialized using pyBaf2Sql.baf.open_storage().
    :type handle: int
    :param identity: ID of the desired array.
    :type identity: str | int
    :return: Double array from the specified ID.
    :rtype: numpy.array
    """
    buf = np.empty(shape=get_num_elements(baf2sql, handle, identity), dtype=np.float64)
    if not baf2sql.baf2sql_array_read_double(handle, identity, buf.ctypes.data_as(POINTER(c_double))):
        throw_last_baf2sql_error(baf2sql)
    return buf


def read_float(baf2sql, handle, identity):
    """
    Read array into a user provided buffer. The data will be converted to the requested type on the fly. The provided
    buffer must be large enough to hold the entire array.

    :param baf2sql: Library initialized by pyBaf2Sql.init_baf2sql.init_baf2sql_api().
    :type baf2sql: ctypes.CDLL
    :param handle: Handle value for BAF dataset initialized using pyBaf2Sql.baf.open_storage().
    :type handle: int
    :param identity: ID of the desired array.
    :type identity: str | int
    :return: Float array from the specified ID.
    :rtype: numpy.array
    """
    buf = np.empty(shape=get_num_elements(baf2sql, handle, identity), dtype=np.float32)
    if not baf2sql.baf2sql_array_read_float(handle, identity, buf.ctypes.data_as(POINTER(c_float))):
        throw_last_baf2sql_error(baf2sql)
    return buf


def read_uint32(baf2sql, handle, identity):
    """
    Read array into a user provided buffer. The data will be converted to the requested type on the fly. The provided
    buffer must be large enough to hold the entire array.

    :param baf2sql: Library initialized by pyBaf2Sql.init_baf2sql.init_baf2sql_api().
    :type baf2sql: ctypes.CDLL
    :param handle: Handle value for BAF dataset initialized using pyBaf2Sql.baf.open_storage().
    :type handle: int
    :param identity: ID of the desired array.
    :type identity: str | int
    :return: uint32 array from the specified ID.
    :rtype: numpy.array
    """
    buf = np.empty(shape=get_num_elements(baf2sql, handle, identity), dtype=np.uint32)
    if not baf2sql.baf2sql_array_read_uint32(handle, identity, buf.ctypes.data_as(POINTER(c_uint32))):
        throw_last_baf2sql_error(baf2sql)
    return buf


def get_sqlite_cache_filename(baf2sql, bruker_d_folder_name):
    """
    Find the filename of the SQLite cache corresponding to the specified BAF file. The SQLite cache will be created
    with the filename "analysis.sqlite" if it does not exist yet.

    :param baf2sql: Library initialized by pyBaf2Sql.init_baf2sql.init_baf2sql_api().
    :type baf2sql: ctypes.CDLL
    :param bruker_d_folder_name: Path to Bruker .d directory containing analysis.baf and analysis.sqlite.
    :type bruker_d_folder_name: str
    :return: SQLite filename.
    :rtype: str
    """
    u8path = os.path.join(bruker_d_folder_name, 'analysis.baf').encode('utf-8')
    baf_len = baf2sql.baf2sql_get_sqlite_cache_filename(None, 0, u8path)
    if baf_len == 0:
        throw_last_baf2sql_error(baf2sql)
    buf = create_string_buffer(baf_len)
    baf2sql.baf2sql_get_sqlite_cache_filename(buf, baf_len, u8path)
    return buf.value


def get_sqlite_cache_filename_v2(baf2sql, bruker_d_folder_name, all_variables=False):
    """
    Find the filename of the SQLite cache corresponding to the specified BAF file. The SQLite cache will be created
    with the filename "analysis.sqlite" if it does not exist yet with the option to include all supported variables.

    :param baf2sql: Library initialized by pyBaf2Sql.init_baf2sql.init_baf2sql_api().
    :type baf2sql: ctypes.CDLL
    :param bruker_d_folder_name: Path to Bruker .d directory containing analysis.baf and analysis.sqlite.
    :type bruker_d_folder_name: str
    :param all_variables: Whether to load all variables from analysis.sqlite database, defaults to False.
    :type all_variables: bool
    :return: SQLite filename.
    :rtype: str
    """
    u8path = os.path.join(bruker_d_folder_name, 'analysis.baf').encode('utf-8')
    baf_len = baf2sql.baf2sql_get_sqlite_cache_filename_v2(None, 0, u8path, all_variables)
    if baf_len == 0:
        throw_last_baf2sql_error(baf2sql)
    buf = create_string_buffer(baf_len)
    baf2sql.baf2sql_get_sqlite_cache_filename_v2(buf, baf_len, u8path, all_variables)
    return buf.value


def set_num_threads(baf2sql, num_threads):
    """
    Set the number of threads that this DLL is allowed to use internally. The index <-> m/z transformation is
    internally parallelized using OpenMP. This call is simply forwarded to omp_set_num_threads(). This function has no
    real effect on Linux.

    :param baf2sql: Library initialized by pyBaf2Sql.init_baf2sql.init_baf2sql_api().
    :type baf2sql: ctypes.CDLL
    :param num_threads: Number of threads to use (>= 1)
    :type num_threads: int
    """
    baf2sql.baf2sql_set_num_threads(num_threads)


def extract_baf_spectrum(baf_data, frame, mode, profile_bins=0, mz_encoding=64, intensity_encoding=64):
    """
    Extract spectrum from BAF data with m/z and intensity arrays. Spectrum can either be centroid or profile mode. If
    "raw" mode is chosen, centroid mode will automatically be used.

    :param baf_data: baf_data object containing metadata from analysis.sqlite database.
    :type baf_data: timsconvert.classes.TimsconvertBafData
    :param frame: Frame to extract spectrum from.
    :type frame: int
    :param mode: Mode command line parameter, either "profile", "centroid", or "raw".
    :type mode: str
    :param profile_bins: Number of bins to bin spectrum to.
    :type profile_bins: int
    :param mz_encoding: m/z encoding command line parameter, either "64" or "32".
    :type mz_encoding: int
    :param intensity_encoding: Intensity encoding command line parameter, either "64" or "32".
    :type intensity_encoding: int
    :return: Tuple of mz_array (np.array) and intensity_array (np.array).
    :rtype: tuple[numpy.array]
    """
    frames_dict = baf_data.analysis['Spectra'][baf_data.analysis['Spectra']['Id'] == frame].to_dict(orient='records')[0]
    if mode == 'raw' or mode == 'centroid':
        mz_array = np.array(read_double(baf_data.api, baf_data.handle, int(frames_dict['LineMzId'])),
                            dtype=get_encoding_dtype(mz_encoding))
        intensity_array = np.array(read_double(baf_data.api, baf_data.handle, int(frames_dict['LineIntensityId'])),
                                   dtype=get_encoding_dtype(intensity_encoding))
    elif mode == 'profile':
        mz_array = np.array(read_double(baf_data.api, baf_data.handle, int(frames_dict['ProfileMzId'])),
                            dtype=get_encoding_dtype(mz_encoding))
        intensity_array = np.array(read_double(baf_data.api, baf_data.handle, int(frames_dict['ProfileIntensityId'])),
                                   dtype=get_encoding_dtype(intensity_encoding))
        if profile_bins != 0:
            mz_array, intensity_array = bin_profile_spectrum(mz_array, intensity_array, profile_bins, mz_encoding)
    return mz_array, intensity_array
