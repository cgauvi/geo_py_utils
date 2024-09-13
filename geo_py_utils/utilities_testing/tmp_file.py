from pathlib import Path
import random
import string

random.seed = 123

class TempFile:

    """ Implement the same functionaiyy as tempfile.NamedTemporaryFile() without the headaches of permission errors on windows and pyarrow read errors
    """

    def __init__(self) -> None:
        self.name = Path(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10)))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Check https://github.com/snowflakedb/snowflake-connector-python/blob/066f1ec8bfc1e27766cb514b1b4aee24bd8eee91/src/snowflake/connector/connection.py#L1696 for inspiration + context manager implementations
        if self.name.exists():
            self.name.unlink()