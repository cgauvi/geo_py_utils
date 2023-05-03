geo_py_utils 0.9.0

- new feature: getting 2021 FSA boundary shp files 

==================

geo_py_utils 0.8.0

- new feature: getting boroughs (arrondissements) from the qc administrative data

==================

geo_py_utils 0.7.3

- updates on installation details in readme

==================

geo_py_utils 0.7.2

- snowflake connector slight refactor with new env vars

==================

geo_py_utils 0.7.0

==================

- new packaging of python project with dependencies + fixed pyproj to version that seems to work.


geo_py_utils 0.6.0

==================

- new feature: getting dissolved municipalities from qc administrative open data

geo_py_utils 0.5.0

==================

- new feature: renaming columns in spatialite table

geo_py_utils 0.4.5

==================

- fix for utf errors in spatial_lite_to_gdf


geo_py_utils 0.4.4

==================

- tox.ini for better testing 
- fixed missing imports in requirements.txt


geo_py_utils 0.4.3

==================

- new test for sfkl to postgis + added markers to pyproject.toml


geo_py_utils 0.4.2

==================

- major linting after error in github worfklow

geo_py_utils 0.4.1

==================

- debugged Url_to_db
- debugged testing + pymarks

geo_py_utils 0.4.0

==================

- load snowflake to postgis
- load spatialite to postgis
- postgis utils like: 
    * db creation
    * listing tables
    * droping tables 

geo_py_utils 0.3.1

==================

- list_table returns an empty list: avoids issues with non-existing DB returning None

geo_py_utils 0.3.0

==================

- Added possibility to download qc administrative boundaries + perform corresponding dissolve operation


geo_py_utils 0.2.0

==================

Geohash improvements

- renamed `geohash.py` to `geohash_utils.py` to avoid circular module imports when the `path/to/geo_py_utils/geo_py_utils` gets erroneously added to the `PYTHONPATH`