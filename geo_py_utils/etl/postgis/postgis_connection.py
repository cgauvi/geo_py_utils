import dotenv
import os
from pathlib import Path
import sys

 
from geo_py_utils.etl.postgis.db_utils import pg_list_tables, pg_create_ogr2ogr_str, pg_create_engine
from geo_py_utils.constants import ProjectPaths

class PostGISDBConnection:
    """Connection parameters to postgres DB read in from env file and saved/returned as dict

    Args:
        path_env_file (_type_, optional): _description_. Defaults to Path(ProjectPaths.PATH_PROJET_ROOT)/".env".
    """

    def __init__(self, path_env_file = Path(ProjectPaths.PATH_PROJET_ROOT) / ".env"):

        assert dotenv.load_dotenv(path_env_file , override=True), 'Failure to load .env! Cannot determine DB parameters'
        

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

        return {    
            "host": os.environ['POSTGRES_HOST'] ,
            "database": os.environ['POSTGRES_DBNAME'],
            "user": os.environ['POSTGRES_USER'] ,
            "password": os.environ['POSTGRES_PASSWORD'] ,
            "port": os.environ['POSTGRES_PORT']
        }
    
    def get_sql_alchemy_engine(self):
        return pg_create_engine(**self.get_credentials())


if __name__ == '__main__':
    db_connector = PostGISDBConnection()