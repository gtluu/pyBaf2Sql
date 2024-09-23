# The following code has been modified from TIMSCONVERT 1.0.0.
# For more information see: https://github.com/gtluu/timsconvert/tree/manuscript_v1.0.0


from ctypes import create_string_buffer


def throw_last_baf2sql_error(baf2sql):
    """
    Error handling for Bruker raw data originating from BAF files. Modified from baf2sql.py example API.

    :param baf2sql: Handle for Baf2sql library.
    :type baf2sql: ctypes.CDLL
    """
    length = baf2sql.baf2sql_get_last_error_string(None, 0)
    buf = create_string_buffer(length)
    baf2sql.baf2sql_get_last_error_string(buf, length)
    raise RuntimeError(buf.value)
