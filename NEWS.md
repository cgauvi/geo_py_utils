================== 

geo_py_utils 1.10.0

- minor:
    * `map_simple` function can take in functions of numerical data in the features from the geojson. Works for colors and weights. Actually a breaking change for non default calls since some arguments like cmap were removed. Offloads a lot of work to the styler function but `get_default_map_fun_weight_numeric_data` and `get_default_map_fun_color_numeric_data`  help creating styling based on some numerical column

- patch: 
    * `table_exists` works even if connection object has no associated schema (for snowflake)
    * `SnowflakeAEConnection` forced to have a schema, db and warehouse associated by using same sql as `SnowflakeExternalBrowserConnection`

================== 

geo_py_utils 1.9.3

- patch: 
    * fix: passing tuple of shapely Polygon to instantiate a pandas df seemed to cause bugs in `intersect_gdf`: pass a list of polyons instead 
    * fix: removed the deprecated `op` argument from geopandas.sjoin and replaced with `predicate`
    * fix: writing to gepparquet requires geos >= 3.10 -> this is a lib that is bundled with `pygeos >=0.14` (only available from pypi)
    * fix: added libspatialite sqlite geopandas to test.yaml: libspatialite needs to be installed through conda (or sudo apt-get or some windows manual download)
    * build: relaxed dependencies when possible


================== 

geo_py_utils 1.9.2

- patch: 
    * fix the requirements: `shapely.get_num_coordinates()` requires >= 2.0.1 (used in `geohash_utils.py`)

================== 

geo_py_utils 1.9.1

- patch: 

    * fix: changed the expectation for `test_muni_dissolved` - based on https://www.donneesquebec.ca/recherche/dataset/decoupages-administratifs# - `MUS_CO_GEO == "92903"` - "Sainte-Élisabeth-de-Proulx" added - `MUS_CO_MOD == CONTNO` (Constitution en territoire non organisé)

================== 

geo_py_utils 1.9.0


- patch: 
    * fixed `SnowflakeCsvWriter`: added new `nullify_invalid_geometries = True` parameter. By default, we will now write invalid geometries to snowflake and these will get nulled out (better than being strict and always throwing errors)
    * fix: minor bugs like replacing assertion by tests + Exceptions 
    * chore: linted some old code for formatting
- minor:
    * feat: created `compare_two_gdf_predicate` that returns a np.array to compare 2 geodataframes 
    * fix: `get_all_geohash_from_gdf` now returns the appropriate geohashes based on the spatial predicate. Previously, spatial relation was based on bounding box rather than exact geometry.
    * feat: implemented `recursively_partition_polygons_geohash_cells` that can recursively subdivide a polygon with finer and finer geohases, based e.g. on area or number of points until a condition is met
    * fix: fixed a bug in `geo_py_utils/geo_general/grid.py`: `width_x_m > (xmax - xmin)` used to consider ymax-ymin + init crs manipulations where weird
    - feat: implemented `geo_py_utils/geo_general/intersection.py:intersect_gdf` : that does the equivalent of `st_intersection` in postgis (but is less efficient and creates a bunch of artefacts like points and edges)
    - feat: implemented `geo_py_utils/geo_general/valid_geom.py:make_extra_valid_gpd` that makes valid + checks for slivers, area, geom type, can remove geom collections, etc.
    - feat: added back `geo_py_utils/misc/ssl.py:no_ssl_verification` and `geo_py_utils/misc/profiler.py:time_function`

    


================== 

geo_py_utils 1.8.1


- patch: 
    * fixed `SnowflakeCsvWriter`: added new `format_type_options = DEFAULT_FORMAT_TYPE_OPTIONS` parameter (breaking change)
    

================== 

geo_py_utils 1.8.0

- minor:
    * implemented the new `SnowflakeCsvWriter` along with some basic type mapping from pyarrow to snowflake.
- patch: 
    * fixed `CLIRunner ` : backwards compatible change: get stdout and stderror output easily + print easily
    * fixed the `DownloadQcDissolvedAdmReg`: 2 municipalities were merged into one in august 2024
    * fixed the pyarrow deps
    


================== 

geo_py_utils 1.7.7

- patch: 
    * fixed the sqlite db contextmanager manager (finally - after 2 years.). No more PermissionErrors in Windows

================== 

geo_py_utils 1.7.6

- patch:
    * fixed `SnowflakeExternalBrowserConnection` and `SnowflakeAEConnection`: lazy connection creation + context manager fix + check for connection existence (bug introduced in 1.7.5)
    * fixed `SnowflakeGeoWriter` accordinly: maintain a single snowflake connection using `SnowflakeAbstractConnection` which we open lazily and close in the `SnowflakeGeoWriter` context manager.
    * changed `snowflake-connector-python` version requirements in `pyproject.toml` to >= 3 , <4


================== 

geo_py_utils 1.7.5

- patch:
    * fixed `get_centroid_gpd` to make sure we recover the appropriate crs from the original geodf when possible.
 

================== 

geo_py_utils 1.7.4

- patch:
    * fixed `DownloadQcDissolvedAdmReg` to make sure we really retrieve all 17 unique administrative regions + works when merging with OPUS data mapping. 
 

================== 

geo_py_utils 1.7.3

- patch:
    * fixed `SnowflakeExternalBrowserConnection` and `SnowflakeAEConnection`: close the connection in the context manager. 
    * geopandas < 1.0.0 added as dependency

================== 

geo_py_utils 1.7.2

- patch:
    * fixed the `CLIRunner` will now return error messages aong with std out in a json parsable text format
================== 

geo_py_utils 1.7.1

- patch:
    * fixed the `SnowflakeGeoWriter` date cast: allow variable names with spaces

================== 

geo_py_utils 1.7.0

- patch:
    * fixed the ogr2ogr wrapper in windows: can deal with multiline cmd
    * tweak to allow writing a regular df with `SnowflakeGeoWriter`
- minor
    * implemented datetime management + tested with `SnowflakeGeoWriter`



================== 

geo_py_utils 1.6.6

- patch:
    * fixed qc adm regions (AGAIN) : zip structure changed
    * refactored ogr2ogr_windows_wrapper and implemented test
    * fixed sql_to_df
    * refactored sfkl_to_gpd: no crs
    * implemented appending for snowflakegeowriter
    * new faster testing for spatialite with `to_file(db_name,   driver="SQLite",   spatialite=True, layer=tbl_name)`
    * tests + debugging for pg_get_table_rows
    * tests for get_table_column_all_metadata (spatialite)

================== 

geo_py_utils 1.6.5

- patch:
    * Windows debugging: make sure `ogr2ogr` is available - even when called from a shell. 
    * added bunch of try catch for RequestException and PermissionError to avoid ci fails on downloads or spatialite concurrency on windows

================== 
geo_py_utils 1.6.4

- patch:
    *  refactored tests

================== 

geo_py_utils 1.6.3

- patch:
    *  refactored ae tests + qc adm regions (CI should now run)

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