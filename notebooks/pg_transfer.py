# %%
 

from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
from geo_py_utils.constants import ProjectPaths
from geo_py_utils.etl.postgis.load_postgis_to_postgis import LoadPostgis2Postgis
from geo_py_utils.etl.postgis.db_utils import pg_list_tables


# %%

connectors={}

connectors['aws'] =  PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / 'aws' / "dev" / ".env")
connectors['gcp_dev']=  PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / 'gcp' / "dev" / ".env")
connectors['gcp_prod']=  PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / 'gcp' / "prod" / ".env")


# %%
# Use these list and filter them
pg_list_tables(
    connectors['gcp_dev'].get_sql_alchemy_engine(),
    connectors['gcp_dev'].get_credentials()['database'],
    'public'
)

# %%

pg_list_tables(
    connectors['gcp_prod'].get_sql_alchemy_engine(),
    connectors['gcp_prod'].get_credentials()['database'],
    'public'
)

# %%


tables_to_transfer={}
tables_to_transfer['gcp_dev'] = [
    'buildings_analysis_and_ds_4326',
    'gic_gw_perso_2024_04_16_subset_merged_on_build_subset',
    'gic_coverage',
    'gic_gw_perso_2024_04_16_subset_merged_on_build_subset_canopy',
    'geo_role_eval_cleaned_pc_adm_da_2023', 
    'gic_geo_role_2023_pc_mapping', 
    'gic_etl_metadata',
    'zip_codes_voronoi', 
    'aq_wms_point_addresses',
    'gic_geo_aq_geocoder_voronoi_overlapping_pc',
    'tbl_agg_combined_2023', 
    'gic_geo_road_types', 
    'tbl_fisc_2023',
    'tbl_building_char_2023', 
    'gic_role_normalized_for_search_2023',
    'building_footprints_open_data', 
    'geo_qc_city_hhspend_pc',
    'gic_perf_matching_subset_3347', 
    'gic_perf_matching_subset',
    'gic_gw_perso_2024_04_16_subset_3347', 
    'probl_points_build',
    'probl_build', 
    'probl_build_simplified',
    'probl_build_simpl_removed', 
    'gic_gw_perso_2024_04_16_subset',
    'building_footprints_open_data_valid_4326', 
    'building_footprints_open_data_with_dist', 
    'cadastres'
]

tables_to_transfer['gcp_prod'] = [
    'aq_osm_pc_voronoi', 
    'voronoi_line_segments',
    'voronoi_line_segments_filtered',
    'aq_geocoder_pts_pc',
    'osm_roads_rails_water_polygonized',
    'voronoi_segments_ptx',
    'buffered_osm_poly', 
    'voronoi_undivided', 
    'dissolved_osm_no_pc',
    'voronoi_segments_distinct_ptx',
    'buffered_osm_poly_dissolved',
    'aq_voronoi_stage_2_ptx', 
    'tmp_aq_voronoi',
    'intersecting_voronoi_points'
]



# %%

# Transfer tables
# Potentially overwriting if there are dups
for db in ['gcp_dev', 'gcp_prod']:

    # Set up transfer params from gcp to aws
    creds = {}
    creds_new = {}
    for k,o in [ (connectors['aws'],'dest'), (connectors[db], 'src')]:
        creds[o] = k.get_credentials().copy()
        creds_new[o] = {}
        for kk,v in creds[o].items():
            creds_new[o][f'{o}_{kk}'] = v

    all_params = creds_new['dest'].copy()
    all_params.update(creds_new['src'].copy())

    loader = LoadPostgis2Postgis(
        tables_to_transfer= tables_to_transfer[db],
        overwrite=True,
         **all_params
    )

    loader.upload_url_to_database()

# %%

new_tbls=pg_list_tables(
    connectors['aws'].get_sql_alchemy_engine(),
    connectors['aws'].get_credentials()['database'],
    connectors['aws'].default_schema
)
print(f"Successfully wrote {new_tbls} to {connectors['aws'].get_sql_alchemy_engine()}")
