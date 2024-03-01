import dotenv
import logging
import os
from pathlib import Path
import snowflake.connector
from snowflake.connector import connection
import warnings

logger = logging.getLogger(__file__)

from geo_py_utils.constants import ProjectPaths
from geo_py_utils.etl.snowflake.snowflake_abstract_connection import SnowflakeAbstractConnection

class SnowflakeExternalBrowserConnection(SnowflakeAbstractConnection):
    """Connection parameters to snowflake DB read in from env file and saved/returned as dict

    Args:
        path_env_file (Path, optional): Credential configs. Path(ProjectPaths.CONFIG_PATH) / 'snowflake' / ".env".
    """
 
    def __init__(self, path_env_file = Path(ProjectPaths.CONFIG_PATH) / 'snowflake' / "extbrowser" / ".env"):

        assert dotenv.load_dotenv(path_env_file , override=True), 'Failure to load .env! Cannot determine ext browser DB parameters'
        
        # Read in the environment variables at instantiation 
        # This ensures we can have multiple credential dictionnaries alive at the same time
        self.creds = {    
            "user": os.environ['USER_EMAIL_SFKL'] ,
            "account": os.environ["USER_ACCOUNT_SFKL"]
        }

        if "PRIVATE_KEY" in os.environ.keys():
            self.creds.update({"private_key": os.environ['PRIVATE_KEY']})
                
        self.role = os.environ['ROLE']
        self.warehouse = os.environ['WAREHOUSE']
        self.db = os.environ['DB']
        self.schema = os.environ['SCHEMA']

        self.conn = self._init_connection()


    def _init_connection(self):
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            conn = snowflake.connector.connect(
                user=os.environ["USER_EMAIL_SFKL"],
                account=os.environ["USER_ACCOUNT_SFKL"],
                authenticator="externalbrowser"
            )

            # Point to the correct DB + schema
            # That way, no need to use statements like `select * from db.schema.table`
            with conn.cursor() as cursor:
                cursor.execute(f"USE ROLE {self.role};")
                cursor.execute(f"USE WAREHOUSE {self.warehouse}")
                cursor.execute(f"USE DATABASE {self.db};")
                cursor.execute(f"USE SCHEMA {self.schema};")
        
        return conn


    def get_credentials(self) -> dict :
        """Get a dict with connection string credentials 
        
        - "user"
        - "account"

        Returns:
            dict: 
        """

        return self.creds
 
            
    def close_connection(self):
        try:
            self.conn.cursor().close()
        except Exception as e:
            logger.debug('Error closing cursor connection')

        
    def get_connection(self) -> connection.SnowflakeConnection:
        return self.conn
     
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Check https://github.com/snowflakedb/snowflake-connector-python/blob/066f1ec8bfc1e27766cb514b1b4aee24bd8eee91/src/snowflake/connector/connection.py#L1696 for inspiration + context manager implementations
        self.close_connection()
 

 
 

    
