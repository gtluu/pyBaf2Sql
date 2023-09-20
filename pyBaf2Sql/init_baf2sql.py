import os
import platform
from ctypes import cdll, c_uint64, c_uint32, c_char_p, c_int, c_double, c_float, POINTER


def init_baf2sql_api(bruker_api_file_name=''):
    """
    Initialize functions from Bruker's Baf2Sql library using ctypes.

    :param bruker_api_file_name: Path to Baf2Sql library, defaults to packaged library paths if no custom paths are
        provided.
    :type bruker_api_file_name: str
    :return: Handle for Baf2Sql library.
    :rtype: ctypes.CDLL
    """
    if bruker_api_file_name == '':
        if platform.system() == 'Windows':
            bruker_api_file_name = os.path.join(os.path.split(os.path.dirname(__file__))[0],
                                                'Baf2Sql',
                                                'baf2sql_c.dll')
        elif platform.system() == 'Linux':
            bruker_api_file_name = os.path.join(os.path.split(os.path.dirname(__file__))[0],
                                                'Baf2Sql',
                                                'libbaf2sql_c.so')

    baf2sql = cdll.LoadLibrary(bruker_api_file_name)

    baf2sql.baf2sql_array_close_storage.argtypes = [c_uint64]
    baf2sql.baf2sql_array_close_storage.restype = None

    baf2sql.baf2sql_array_get_num_elements.argtypes = [c_uint64,
                                                       c_uint64,
                                                       POINTER(c_uint64)]
    baf2sql.baf2sql_array_get_num_elements.restype = c_int

    baf2sql.baf2sql_array_open_storage.argtypes = [c_int, c_char_p]
    baf2sql.baf2sql_array_open_storage.restype = c_uint64

    baf2sql.baf2sql_array_read_double.argtypes = [c_uint64,
                                                  c_uint64,
                                                  POINTER(c_double)]
    baf2sql.baf2sql_array_read_double.restype = c_int

    baf2sql.baf2sql_array_read_float.argtypes = [c_uint64,
                                                 c_uint64,
                                                 POINTER(c_float)]
    baf2sql.baf2sql_array_read_float.restype = c_int

    baf2sql.baf2sql_array_read_uint32.argtypes = [c_uint64,
                                                  c_uint64,
                                                  POINTER(c_uint32)]
    baf2sql.baf2sql_array_read_uint32.restype = c_int

    baf2sql.baf2sql_get_last_error_string.argtypes = [c_char_p, c_uint32]
    baf2sql.baf2sql_get_last_error_string.restype = c_uint32

    baf2sql.baf2sql_get_sqlite_cache_filename.argtypes = [c_char_p,
                                                          c_uint32,
                                                          c_char_p]
    baf2sql.baf2sql_get_sqlite_cache_filename.restype = c_uint32

    baf2sql.baf2sql_get_sqlite_cache_filename_v2.argtypes = [c_char_p,
                                                             c_uint32,
                                                             c_char_p,
                                                             c_int]
    baf2sql.baf2sql_get_sqlite_cache_filename_v2.restype = c_uint32

    baf2sql.baf2sql_set_num_threads.argtypes = [c_uint32]
    baf2sql.baf2sql_set_num_threads.restype = None

    return baf2sql
