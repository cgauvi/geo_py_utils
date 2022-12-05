
    
from os.path import exists
import logging
from typing import Union
import pandas as pd
import sqlite3

logger = logging.getLogger(__file__)
    
def list_tables(db_name) -> Union[None, list]:
    """ List tables  in a spatialite db

    Args:

    Returns:
        list of table names : list

    """

    list_tables = None
    if exists(db_name):
        logger.info(f"DB {db_name} exists!")

        with sqlite3.connect(db_name) as con:
            df_tbls = pd.read_sql("SELECT tbl_name FROM sqlite_master WHERE type = 'table';", con)

        list_tables = df_tbls.tbl_name.values
    else:
        logger.info(f"DB {db_name} does not exists")

        
    return list_tables


if __name__ == "__main__":

    import os
    from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
    from geo_py_utils.misc.constants import DATA_DIR

    existing_tables = list_tables(os.path.join(DATA_DIR, "test.db"))