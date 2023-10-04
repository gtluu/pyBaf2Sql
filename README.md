# pyBaf2Sql

## About
This package is a Python wrapper for Bruker's Baf2Sql data access library to be used with other Python packages. Functions have been taken and modified from Baf2Sql and TIMSCONVERT.

## Installation
This package can be installed to a Python virtual environment using `pip`. 
```
pip install git+https://github.com/gtluu/pyBaf2Sql
```

## Example Usage
```python
import numpy as np
import pandas as pd
from pyBaf2Sql.init_baf2sql import init_baf2sql_api
from pyBaf2Sql.classes import BafData
from pyBaf2Sql.baf import read_double


# Initialize the Baf2Sql library using the packaged .dll or .so files for Windows or Linux, respectively.
dll = init_baf2sql_api()

data = BafData(bruker_d_folder_name='some_timstof_data.d', baf2sql=dll)

# Get all spectra from a BAF file as a list of pd.DataFrames with columns for m/z and intensity in centroid mode.
spectra_dfs = []
for frame in range(1, data.analysis['Spectra'].shape[0]+1):
    frame_dict = data.analysis['Spectra'][data.analysis['Spectra']['Id'] == frame].to_dict(orient='records')[0]
    mz_array = np.array(read_double(baf2sql=dll, handle=data.handle, identity=int(frame_dict['LineMzId'])),
                        dtype=np.float64)
    intensity_array = np.array(read_double(baf2sql=dll, handle=data.handle, identity=frame_dict['LineIntensityId']),
                               dtype=np.float64)
    spectra_dfs.append(pd.DataFrame({'mz': mz_array, 'intensity': intensity_array}))

# Print number of spectra parsed from TSF file.
print(len(spectra_dfs))
# Print spectrum from first frame.
print(spectra_dfs[0])
```
