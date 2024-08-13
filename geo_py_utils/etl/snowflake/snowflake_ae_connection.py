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

        self.conn = self._init_connection()

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
        try:
            if self.snowflake_wrapper and self.snowflake_wrapper.get_connection():
                self.close_connection()
                self.get_connection().close()
        except Exception as e:
            logger.debug(f'Error closing AE cursor + connection - {str(e)}')

    def close_connection(self):
        self.snowflake_wrapper.close_connection()

    def get_connection(self) ->  connection.SnowflakeConnection:
        return self.conn
    
    def __str__(self) -> str:
        return super().__str__()
 