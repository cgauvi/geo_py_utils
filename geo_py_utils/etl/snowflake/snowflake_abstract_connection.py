from abc import ABC, abstractmethod
from snowflake.connector import connection

class SnowflakeAbstractConnection(ABC):

    @abstractmethod
    def close_connection(self, exc_type, exc_value, traceback):
        raise NotImplementedError()

    @abstractmethod
    def get_connection(self) -> connection.SnowflakeConnection:
        raise NotImplementedError()