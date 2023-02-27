import warnings
import os
import snowflake.connector

import logging

logger = logging.getLogger(__file__)

def connnect_snowflake_ext_browser(role: str = "M_DATA_SCIENCE",
                                    warehouse: str = "DATA_SCIENCE",
                                    db: str = "M_DATA_SCIENCE",
                                    schema: str = "SANDBOX"):

    """Connect to snowflake using an external browser

    Args:
        role (str, optional):  Defaults to "M_DATA_SCIENCE".
        warehouse (str, optional):  Defaults to "DATA_SCIENCE".
        db (str, optional):  Defaults to "M_DATA_SCIENCE".
        schema (str, optional):  Defaults to "SANDBOX".

    Returns:
        snowflake connection
    """

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        print(os.environ["USER_EMAIL"])

        conn = snowflake.connector.connect(user=os.environ["USER_EMAIL"],
                                            account="beneva-da",
                                            authenticator="externalbrowser")

        # Point to the correct DB + schema
        # That way, no need to use statements like `select * from db.schema.table`
        with conn.cursor() as cursor:
            cursor.execute(f"USE ROLE {role};")
            cursor.execute(f"USE WAREHOUSE {warehouse}")
            cursor.execute(f"USE DATABASE {db};")
            cursor.execute(f"USE SCHEMA {schema};")
   

    return conn


  
