
from functools import wraps
import time

def time_function(fun):
    """ Basic decorator to time the execution of a function.
    """
    @wraps(fun)
    def inner_wrapper(*kws, **kwargs):
        """ Basic decorator to time the execution of a function.

        Args:
            *kws, **kwargs
        Returns:
            result, time of execution (tuple): result of fun(*kws, **kwargs) + time to run
        """

        start = time.time() 
        results = fun(*kws, **kwargs)
        end = time.time() 

        return results, end-start

    return inner_wrapper