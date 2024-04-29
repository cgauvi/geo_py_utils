from abc import ABC, abstractmethod
from snowflake.connector import connection

class SnowflakeAbstractConnection(ABC):

    @abstractmethod
    def close_connection(self, exc_type, exc_value, traceback):
        raise NotImplementedError()

    @abstractmethod
    def get_connection(self) -> connection.SnowflakeConnection:
        raise NotImplementedError()
    
    def __str__(self) -> str:

        sfkl_session_details = {
            'role': self.role,
            'warehouse': self.warehouse,
            'db': self.db,
            'schema': self.schema
        }

        return f'{ {**sfkl_session_details} }'