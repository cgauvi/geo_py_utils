
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

 

---

# Setup and usage 


## Installation 

### General information 

The project has been tested by installing geopandas through `conda`. The rest of the dependencies can be installed manually with `pip` or `poetry`. Conda was initially chosen in an attempt to reduce friction and avoid having to tweak environment variables and point to correct libs. In hindsight, this might not be the best approach since some libraries like `mod_spatialite` are not packaged with conda anyways and require some sort of manual sudo install on windows or linux/max. 

```bash
conda create --name geo_py_utils python=3.9 geopandas
conda activate geo_py_utils
python -m pip install --upgrade pip
python -m pip install "geo_py_utils @ git+ssh://git@github.com/LaCapitale/geo_py_utils.git"
```

### Details 

- It is possible to install extra optional dependencies. For instance if running notebooks and trying to plot/map/visualize results, it will be referable to run `python -m pip install  "geo_py_utils[notebooks] @ git+ssh://git@github.com/LaCapitale/geo_py_utils.git"` to install all required packages. 
- It is also possible to select a specific version rather than taking the latest; for instance by using `"git+ssh://git@github.com/LaCapitale/geo_py_utils.git@v0.7.1"`.
- Also note that `poetry` can be used as a drop-in replacement for `pip`: `poetry add "geo_py_utils[notebooks] @ git+ssh://git@github.com/LaCapitale/geo_py_utils.git@v0.7.1"`


## Testing

Tests are implemented with `pytest`. By default tests that require some sort of database connectivity are disabled in the `pyproject.toml`. Running `pytest` from within `geo_py_utils` root will skip these tests with the `-m` markers. This ensures that unit tests run in github actions or other similar framewrosk which might not necessarily have access to a given DB do not fail and crash the rest of the pipeline. 
 
If you want to run a given test on a machine that meets the connectivity requirements, run someething like `pytest tests -m "requires_remote_pg_connection_prod"`. See `pyproject.toml` for other registered `pytest.marks` that are excluded by default. 

To be on the safe side, explicitely exclude markers when there are many E.g.:

`pytest tests/ -m 'requires_remote_pg_connection_prod and not requires_remote_pg_connection_dev'` to run the postgis backup tests on workbench which has access to the prod db, but not the dev db.

 
