
================== 
geo_py_utils 1.6.2
- minor:
    * new SnowflakeAbstractConnection class 
- patch:
    * fixed SnowflakeGeoWriter by converting object to string + encoding
    
================== 
geo_py_utils 1.6.1

- patch:
    * pyarow deps conflict with snowflake connector: <=10.0.0

==================
geo_py_utils 1.6.0

- minor:
    * new SnowflakeGeoWriter class to write gpd to snowflake using sso connection
    
================== 
geo_py_utils 1.5.6

- patch:
    * fixed bug in tests
    * fixed error when dest download dir does not exist in zip fct
    
================== 

geo_py_utils 1.5.5

- patch:
    * poetry dependencies update
    
================== 

geo_py_utils 1.5.0

- minor:
    * new generic sql string manip functions

================== 

geo_py_utils 1.4.1

- patch:
    * fixed generic spatialite utils

==================

geo_py_utils 1.4.0

- minor:
    * new spatialite utils from other project (remove_apostrophe, build_sql_select_with_fct,execute_spatialite_cmd)


geo_py_utils 1.3.4

==================
- patch:
    * PostGISDBConnection can load multiple env files
    

geo_py_utils 1.3.3

==================
- patch:
    * ogr2ogr shell call works on Windows through powershell 



geo_py_utils 1.3.2

==================

- patch:
    * added a safe check for valid geom col name in spatialite_db_to_gdf


geo_py_utils 1.3.1

==================

- patch:
    * fix for geo renaming: throw and catch GeomColumnAbsentError when geo col absent + ALTER TABLE RENAME with '' for new name string.

geo_py_utils 1.3.0

==================

- minor:
    * new - minor: can rename geo column

geo_py_utils 1.2.0

==================

- minor:
    * spatialite renaming of column
    * pg & spatialite set column name with ogr2ogr



geo_py_utils 1.1.3

==================

- patch:
    * changed default arg of borough download

geo_py_utils 1.1.2

==================


- minor:
    * new `get_df_geometry_columns` method in `PostGISDBTablesIdentifier` to get the geometry columns from a given schema and table


geo_py_utils 1.1.1

==================


- patch:
    * use the same interface for the pg backup regardless of if we select ALL or only a SUBSET of tables - no use in having a different function since we iterate over tables anyhowcreated a mock tmp cache for more robust debugging (open data)
    

geo_py_utils 1.0.0

==================

- major:
    * removed the dependency on ben_py_utils
- debug/minor feature:
    * created a mock tmp cache for more robust debugging (census)


geo_py_utils 0.11.2

==================

- debug/minor feature:
    * added -preserve_fid in postgis backup to avoid error when writing pg -> gpkg and a table has fid column which is not unique 


geo_py_utils 0.11.1

==================

- debug/minor feature:
    * added - lco LAUNDER=NO in postgis backup to keep variable case

geo_py_utils 0.11.0

- new features:  
    * postgres connection object from .env file
    * postres table listor
    * postres backup to gpkg

==================

geo_py_utils 0.10.0

- new feature: connection strings from postgres credentials
    * ogr2ogr
    * sqlalchemy

==================

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