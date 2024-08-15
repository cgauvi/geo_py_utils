import dotenv
import os
from pathlib import Path
import logging
try:
    from snowflakewrapper import SnowflakeWrapper
except ImportError:
    pass
from snowflake.connector import connection
import warnings

logger = logging.getLogger(__file__)

from geo_py_utils.constants import ProjectPaths
from geo_py_utils.etl.snowflake.snowflake_abstract_connection import SnowflakeAbstractConnection

class SnowflakeAEConnection(SnowflakeAbstractConnection):

    def __init__(self, path_env_file = Path(ProjectPaths.CONFIG_PATH) / 'snowflake' / "ae" / ".env"):

        assert dotenv.load_dotenv(path_env_file , override=True), 'Failure to load .env! Cannot determine AE conn DB parameters'
        
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

        # Lazy eval
        self.conn = None

    def _init_connection(self):
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            self.snowflake_wrapper = SnowflakeWrapper() 
            self.snowflake_wrapper.send_query(f"USE ROLE {self.role};")
            self.snowflake_wrapper.send_query(f"USE DATABASE {self.db};")
            self.snowflake_wrapper.send_query(f"USE WAREHOUSE {self.warehouse};")
            self.snowflake_wrapper.send_query(f"USE SCHEMA {self.schema};")

        return self.snowflake_wrapper.get_connection()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close_connection()

    def close_connection(self):
        
        try:
            self.snowflake_wrapper.close_connection()       #closes the cursor
        except Exception as e:
            logger.debug(f'Error closing AE cursor  - {str(e)}')
    
        try:
            self.snowflake_wrapper.get_connection().close() #closes the actual connection
        except Exception as e:
            logger.debug(f'Error closing AE connection - {str(e)}')

    def get_connection(self) ->  connection.SnowflakeConnection:

        # Watch out with context manager: if we point to the same SnowflakeAbstractConnection after closing the connection we will return None
        if self.conn is None:
            self._init_connection()
            
        return self.conn
    
    def __str__(self) -> str:
        return super().__str__()
 