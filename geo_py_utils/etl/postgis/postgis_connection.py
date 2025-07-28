import dotenv
import os
from pathlib import Path

from geo_py_utils.etl.postgis.db_utils import pg_create_engine
from geo_py_utils.constants import ProjectPaths

class PostGISDBConnection:
    """Connection parameters to postgres DB read in from env file and saved/returned as dict

    Args:
        path_env_file (_type_, optional): _description_. Defaults to Path(ProjectPaths.PATH_PROJET_ROOT)/".env".
    """

    def __init__(self, path_env_file: Path = ProjectPaths.CONFIG_PATH / 'postgis' / 'dev' / ".env"):

        # Watch out with optional kwargs - reset each time to make sure 
        if os.environ.get('POSTGRES_DEFAULT_SCHEMA'):
            del os.environ['POSTGRES_DEFAULT_SCHEMA']

        assert dotenv.load_dotenv(path_env_file , override=True), 'Failure to load .env! Cannot determine DB parameters'
        
        # Read in the environment variables at instantiation 
        # This ensures we can have multiple credential dictionnaries alive at the same time
        self.creds = {    
            "host": os.environ['POSTGRES_HOST'] ,
            "database": os.environ['POSTGRES_DBNAME'],
            "user": os.environ['POSTGRES_USER'] ,
            "password": os.environ['POSTGRES_PASSWORD'] ,
            "port": os.environ['POSTGRES_PORT']
        }

        # Save this as a separate variable for backward compatibility
        self.default_schema = os.environ.get('POSTGRES_DEFAULT_SCHEMA')

    def get_credentials(self) -> dict :
        """Get a dict with connection string credentials 
        
        - "host"
        - "database"
        - "user"
        - "password"
        - "port"

        Returns:
            dict: 
        """

        return self.creds
    
    def get_sql_alchemy_engine(self):

        creds_with_default_schema = self.get_credentials().copy()
        if self.default_schema:
            creds_with_default_schema.update({'default_schema': f'{self.default_schema}' } )

        return pg_create_engine(**creds_with_default_schema)
 