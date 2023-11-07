import numpy as np


def get_encoding_dtype(encoding):
    """
    Use "encoding" command line parameter to determine numpy dtype.

    :param encoding: Encoding command line parameter, either "64" or "32".
    :type encoding: int
    :return: Numpy dtype, either float64 or float32
    :rtype: numpy.dtype
    """
    if encoding == 32:
        return np.float32
    elif encoding == 64:
        return np.float64


def get_centroid_status(mode):
    """
    Use "mode" command line parameter to determine whether output data is centroided in psims compatible format.

    :param mode: Mode command line parameter, either "profile", "centroid", or "raw".
    :type mode: str
    :return: Dictionary containing standard spectrum data.
    :return: Tuple of (centroided status (bool), exclude_mobility status (bool))
    :rtype: tuple[bool]
    """
    if mode == 'profile':
        return False
    elif mode == 'centroid' or mode == 'raw':
        return True


def bin_profile_spectrum(mz_array, intensity_array, profile_bins, encoding):
    """
    Bin profile mode spectrum into N number of bins.

    :param mz_array: Array containing m/z values.
    :type mz_array: numpy.array
    :param intensity_array: Array containing intensity values.
    :type intensity_array: numpy.array
    :param profile_bins: Number of bins to bin spectrum to.
    :type profile_bins: int
    :param encoding: Encoding command line parameter, either "64" or "32".
    :type encoding: int
    :return: Tuple of binned_mz_array (np.array) and binned_intensity_array (np.array).
    :rtype: tuple[numpy.array]
    """
    mz_acq_range_lower = float(mz_array[0])
    mz_acq_range_upper = float(mz_array[-1])
    bins = np.linspace(mz_acq_range_lower, mz_acq_range_upper, profile_bins, dtype=get_encoding_dtype(encoding))
    unique_indices, inverse_indices = np.unique(np.digitize(mz_array, bins), return_inverse=True)
    bin_counts = np.bincount(inverse_indices)
    np.place(bin_counts, bin_counts < 1, [1])
    mz_array = np.bincount(inverse_indices, weights=mz_array) / bin_counts
    intensity_array = np.bincount(inverse_indices, weights=intensity_array)
    return mz_array, intensity_array