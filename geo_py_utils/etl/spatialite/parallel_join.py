from multiprocessing import Pool
import geopandas as gpd
from typing import List, Union
from os.path import join, exists
from os import makedirs
import numpy as np
import sqlite3
import pandas as pd
import logging
import sys


from geo_py_utils.etl.spatialite.db_utils import list_tables, get_table_rows, sql_to_df, drop_table, get_table_crs    
from geo_py_utils.etl.spatialite.gdf_load import spatialite_db_to_gdf
from geo_py_utils.geo_general.geohash_utils import (
    recursively_partition_geohash_cells, 
    get_all_geohash_from_gdf,
    add_geohash_index,
    get_all_geohash_from_geohash_indices
)
from geo_py_utils.etl.db_etl import Url_to_spatialite

from ben_py_utils.misc.cache import Cache_wrapper


# Logger
logger = logging.getLogger(__file__)


class ParallelSpatialJoin:

    """Class to perform spatial join of 2 geo spatialite tables + create a new mapping table to the same db

    Attributes:
            db_name (_type_): _description_
            tbl_left_geo (_type_): _description_
            tbl_right_geo (_type_): _description_
            left_geo_id (_type_): _description_
            right_geo_id (_type_): _description_
            tbl_new_name (_type_): _description_
            target_proj (_type_): _description_
            batch_size (int, optional): _description_. Defaults to 16.
            min_count_parallel (int, optional): _description_. Defaults to 50.
            max_geohash_precision (int, optional): _description_. Defaults to 5.
            overwrite (bool, optional): _description_. Defaults to True.
            no_overwrite_append (bool, optional): _description_. Defaults to False.
            predicate (str, optional): _description_. Defaults to 'ST_WITHIN'.
    """


    def __init__(self,
                db_name,
                tbl_left_geo,
                tbl_right_geo,
                left_geo_id,
                right_geo_id,
                tbl_new_name, 
                target_proj,
                batch_size = 16,
                min_count_parallel = 50,
                max_geohash_precision = 5,
                overwrite = True,
                no_overwrite_append=False,
                predicate='ST_WITHIN'):
        
        self.db_name = db_name
        self.tbl_right_geo = tbl_right_geo
        self.tbl_left_geo = tbl_left_geo
        self.tbl_new_name = tbl_new_name
        self.target_proj = target_proj

        self.left_geo_id = left_geo_id
        self.right_geo_id = right_geo_id


        self.min_count_parallel = min_count_parallel
        self.max_geohash_precision = max_geohash_precision

        self.no_overwrite_append =no_overwrite_append
        self.overwrite = overwrite

        self.predicate = predicate

        self.batch_size = batch_size

        self._shp_partionned_right = None
        self._shp_right = None
        self._shp_right_points = None
        self._shp_left = None
 
        self.total_left_points = get_table_rows(self.db_name, self.tbl_left_geo)
        self.cumul_points_completed = 0

    @property 
    def shp_right (self):
        if self._shp_right is None:
            self._shp_right = spatialite_db_to_gdf(self.db_name, self.tbl_right_geo)

        return self._shp_right
    
    @property
    def shp_left(self):
        if self._shp_left is None:
            self._shp_left = spatialite_db_to_gdf(self.db_name, self.tbl_left_geo)
            self._shp_left['geometry'] = self._shp_left.geometry.centroid  # hack to avoid the multipoint

        return self._shp_left
 
    def _get_crs(self) -> int:
        """Helper fct to get the crs from the 2 tables we are trying to spatial join

        Returns:
            int: crs (epsg code)
        """

        # Get the crs
        crs_right = get_table_crs(self.db_name, self.tbl_right_geo, return_srid = True)
        crs_left = get_table_crs(self.db_name, self.tbl_right_geo, return_srid = True)
            
        assert crs_right == crs_left, \
                f"Fatal error! mismatch between crs: right {crs_right} vs left {crs_left}"

        assert crs_right in [4326, 4269], \
                f"Fatal error! use a geographic coordinate system, not {crs_right}"

        return crs_left


    def _clean_geo(self, shp_merged):
        
        # Remove duplicate columns
        shp_merged = shp_merged.loc [:,~ shp_merged.columns.duplicated()].copy()
            
        # Rename geo
        if 'GEOMETRY' in shp_merged.columns:
            shp_merged.drop(columns = 'GEOMETRY', inplace=True)
            shp_merged['GEOMETRY'] = shp_merged['GEOMETRY_NEW']
            shp_merged.drop(columns = 'GEOMETRY_NEW', inplace=True)
                
            shp_merged = shp_merged.set_geometry("GEOMETRY")
            shp_merged = shp_merged.set_crs(self._get_crs())

        return shp_merged


    def _merge_left_right_geohash(self, geohash_index : str) -> gpd.GeoDataFrame :
        """Merge all left data that falls within a given a geohash index with ALL the postal codes

        Args:
            geohash_index (str): 

        Returns:
            merge gdf: gpd.GeoDataFrame
        """
        
        geohash_prec = len(geohash_index)

        # Collect the geohashes 
        query_geohash_left = f"SELECT *, st_geohash(geometry, {geohash_prec}) as geohash_index_{geohash_prec} "\
            f" FROM {self.tbl_left_geo} " \
            f" WHERE st_geohash(geometry, {geohash_prec}) = \'{geohash_index}\'"
        
        # Select all fsaldu
        query_geohash_right = f"SELECT GEOMETRY , {self.right_geo_id} FROM {self.tbl_right_geo}"

        # Spatial join the subqueries
        query_merge = " SELECT * , Hex(AsBinary(left.GEOMETRY)) as GEOMETRY_NEW " \
                        f"FROM ({query_geohash_left}) as left " \
                        f"JOIN ({query_geohash_right}) as right " \
                        f"ON {self.predicate}(left.GEOMETRY, right.GEOMETRY)"


        with sqlite3.connect(self.db_name) as con:
            con.enable_load_extension(True)
            con.load_extension("mod_spatialite")

            # Read in the geodf
            shp_merged = gpd.read_postgis(query_merge,
                                         con,
                                         'GEOMETRY_NEW',
                                         crs=self._get_crs()
            )

        prop_completed = shp_merged.shape[0]/self.total_left_points
        self.cumul_points_completed += shp_merged.shape[0]
        logger.info(
            f"""
            Merged {shp_merged.shape[0]} left points at this iteation - 
            ({self.cumul_points_completed*100})% completed)
            """
        )


        return shp_merged





    def _merge_left_right_missing(self, list_left_ids:List[str])-> gpd.GeoDataFrame :
        """Convenience function called by _serial_join_remaining to mege missing left data with right

        Args:
            list_left_ids (_type_): _description_

        Returns:
            gpd.GeoDataFrame : shp with geo column
        """
        
        list_left_ids_str = ",".join([f"\'{id_p}\'" for id_p in list_left_ids])
    
        # where is just a list ids
        query_left_subset = f" SELECT * " \
                           f" FROM {self.tbl_left_geo} "\
                           f" WHERE {self.left_geo_id} in ({list_left_ids_str})"
        
        # No more where clause
        query_right = f"SELECT GEOMETRY , {self.right_geo_id} FROM {self.tbl_right_geo}"

        query_merge = " SELECT * , Hex(AsBinary(left.GEOMETRY)) as GEOMETRY_NEW " \
                        f"FROM ({query_left_subset}) as left " \
                        f"JOIN ({query_right}) as right " \
                        f"ON {self.predicate}(left.GEOMETRY, right.GEOMETRY)"

        with sqlite3.connect(self.db_name) as con:
            con.enable_load_extension(True)
            con.load_extension("mod_spatialite")

            shp_merged = gpd.read_postgis(query_merge, con, 'GEOMETRY_NEW', crs=self._get_crs())
        
 

        return shp_merged


    def _serial_join_remaining (self, shp_left_with_right : gpd.GeoDataFrame) -> Union[None, gpd.GeoDataFrame] :
        """ Perform a regular spatial join on the ids that have not been assigned a postal code yet 

        Args:
            shp_left_with_right (gpd.GeoDataFrame): 

        Returns:
            merge gdf: gpd.GeoDataFrame
        """
        
        # Outer join `left`` on `left with right ``
        if (shp_left_with_right is not None) and (shp_left_with_right.shape[0] > 0):
            outer_join = self.shp_left.merge(shp_left_with_right[[self.right_geo_id, self.left_geo_id]], 
                                            on = self.left_geo_id, 
                                            how='outer')

            # Get the missing left data with no right_geo_id (primary key of right table)
            shp_not_merged_yet = outer_join.loc[ outer_join[self.right_geo_id].isna(), :]
        else:
            shp_not_merged_yet= self.shp_left


        # Extract the primary keys 
        list_left_ids = shp_not_merged_yet.id_provinc.values 

        if list_left_ids.size > 0:
            # Regular spatial join with filter on province
            shp_merged_missing = self._merge_left_right_missing(list_left_ids)

        else:
            logger.warn(f"Warning! all {self.left_geo_id} found by the parallel join")
            shp_merged_missing = None

        return shp_merged_missing


    def _parallel_join(self, shp_part) -> List[gpd.GeoDataFrame]:
        """Parallel spatial joins based on geohash grid

        Args:
            shp_part (_type_): _description_

        Returns:
            List[gpd.GeoDataFram]: _description_
        """
        
        # Now get the geohashes with at least self.min_count_parallel counts 
        shp_partionned_right_nz = shp_part [shp_part['counts'] > self.min_count_parallel ]

        # Create the number of batches to run
        num_batches = int(np.ceil(shp_partionned_right_nz.shape[0]/self.batch_size))
 
        list_all_df = []
        for batch in np.arange(1,num_batches+1):
            with Pool(self.batch_size) as p:

                # Subset the rows on the matching geohash
                rows = np.arange((batch-1)*self.batch_size, 
                                min((batch)*self.batch_size,shp_partionned_right_nz.shape[0]))

                # Parallelize by running join on each geohash 
                results = p.map(self._merge_left_right_geohash, 
                                shp_partionned_right_nz.geohash_index.iloc[rows].values)

                # Filter out zeros
                list_nz_df = list(filter(lambda df: df.shape[0] > 0, results))
                if len(list_nz_df) > 0:
                    list_all_df.extend(list_nz_df)
                    
        return list_all_df

 


    def _merge_manual(self)->gpd.GeoDataFrame:
        """Regular FULL spatial join: mostly for comparisons on test datasets

        Returns:
            gpd.GeoDataFrame: _description_
        """

        shp_merged = gpd.sjoin(self.shp_left, 
                                self.shp_right,
                                how='inner',
                                predicate='within')

        return shp_merged


    def _merge_all (self)->gpd.GeoDataFrame:

        """Main function to spatial join postal codes and left data

        Arguments:
            min_num_points (int): min number of postal code centroids each geohash cell needs to have
            max_precision (int): max geohash precision : should probably be <= 6 or 5 (any larger leads to excessively precise cells)

        Returns:
            gpd.GeoDataFrame: _description_
        """


        # Get the postal code centroids
        self.shp_right_points = self.shp_right.copy(deep=True)
        self.shp_right_points['geometry'] = self._shp_right.geometry.centroid 

        # Create the spatial geohash grid where each cell has a similar number of postal code centroids ~ proxy for population density
        self.shp_partionned_right, _ = recursively_partition_geohash_cells(self.shp_right_points, 
                                                                        min_num_points=self.min_count_parallel,
                                                                        max_precision=self.max_geohash_precision)

        # Run the *parallel* spatial joins on these geohashes 
        list_all_df_parallel = self._parallel_join(self.shp_partionned_right)
        shp_left_with_right_parallel = None
        list_merged_df = []
        if (list_all_df_parallel is not None) and \
            (len(list_all_df_parallel) > 0):
            # Cocnat sub queries 
            shp_left_with_right_parallel = pd.concat(list_all_df_parallel, ignore_index = True, axis=0)
            # Remove duplicate columns + clean up
            shp_left_with_right_parallel = self._clean_geo(shp_left_with_right_parallel)
            # append to results
            list_merged_df.append(shp_left_with_right_parallel)
        else:
            logger.warning("Warning! did not merge any left and right using the parallel algo!")

        # Run the *serial* spatial joins on the remaining left points
        shp_merged_missing = self._serial_join_remaining(shp_left_with_right_parallel) 
        if shp_merged_missing is not None:
            # Remove duplicate columns + clean up
            shp_merged_missing = self._clean_geo(shp_merged_missing)
            list_merged_df.append(shp_merged_missing)


        # Concat 
        assert (len(list_merged_df) > 0), \
            "Fatal error! did not manage to join ANY rights and left points"

        # Concat the results of parallel and remaining serial joins
        shp_left_with_right_final = pd.concat(list_merged_df, ignore_index = True, axis=0)

        return shp_left_with_right_final

    def upload_and_merge(self):

        # Spatial join
        shp_results = self._merge_all()
        
        # Write the results to a tmp location on disk
        dir_dict = join(Projet_paths.DATA_PATH, self.tbl_new_name)
        if not exists(dir_dict): makedirs(dir_dict)
        path_shp_file = join(dir_dict, f"{self.tbl_new_name}.shp")
        shp_results[[self.left_geo_id,self.right_geo_id, 'GEOMETRY']].to_file(path_shp_file, mode='w')  #overwrite each time

        # Upload to Db
        with Url_to_spatialite(
                    db_name = self.db_name, 
                    table_name = self.tbl_new_name,
                    download_url = path_shp_file,
                    download_destination = dir_dict,
                    overwrite = self.overwrite,
                    target_projection = self.target_proj,
                    no_overwrite_append = self.no_overwrite_append
            ) as spatialite_etl:

            spatialite_etl.upload_url_to_database()


if __name__ == '__main__':

    DB_PATH = "test.db"
    TBL_ROLE_NAME =  "geo_role_eval" #"geo_role_eval_subset_random" # systematically sampling the first rows leads to disjoint data
    TBL_right_NAME =  "geo_postal_codes" #"geo_postal_codes_subset_random"
    NEW_TBL_NAME = "geo_role_right_mapping" #"geo_role_right_mapping_subset_random"

    left_right_merger = ParallelSpatialJoin(
        DB_PATH,
         TBL_right_NAME,
         TBL_ROLE_NAME,
         NEW_TBL_NAME 
          )
    
    
    shp_results = left_right_merger.upload_and_merge()


 

