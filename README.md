
<!-- badges: start -->
[![pytest](https://github.com/cgauvi/geo_py_utils/actions/workflows/tests.yaml/badge.svg)](https://github.com/cgauvi/geo_py_utils/actions/workflows/tests.yaml)
<!-- badges: end -->


# General python functions meant to be reused in different projects

# Contents 

### Generic geospatial functions

- computing a buffer with geodf
- compute 2D KDE estimates
- bounding box computation, centroid addition
- h3 and geohash utils
- inverse distance weighted interpolation
- etc.

### Census and open data download

- download geo bundaries for census (e.g. FSAs, DA, CSD)
- download ad-hoc datasets from open data portal

### Basic ETL 

- downloading zipped shp files to disk
- downloading an uploading zipped shp files to:
    - postGIS
    - spatialite

 
 
 # Installation 

The project requires installing geopandas through `conda`. The rest of the dependencies can be installed manually with `pip`

 ```bash
conda create --name geo_py_utils python=3.9 geopandas
conda activate geo_py_utils
python -m pip install --upgrade pip
pip install -r requirements.txt
 ```

 ## Note

 The package depends on another [custom package](https://github.com/cgauvi/ben_py_utils). The following line was added to the `requirements.txt`:
 
 `git+https://github.com/cgauvi/ben_py_utils.git@master`