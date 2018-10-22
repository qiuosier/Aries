import time
import math


class Task:
    """Represents a task of running a function.
    """
    def __init__(self, func, *args, **kwargs):
        """Initializes a task to run a function.

        Args:
            func: The function to be executed.
            *args: A list of arguments for the function to be executed.
            **kwargs: A dictionary of keyword arguments for the function to be executed.
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run_and_retry(self, max_retry=10, exceptions=Exception):
        """Runs the task and retry a few times if certain exceptions occurs.
        The time interval between the ith and (i+1)th retry is e**i, i.e. interval increases exponentially.

        Args:
            max_retry (int): The number of times to re-try.
            exceptions (Exception or tuple): An exception class or A tuple of exception classes.

        Returns: The return value of the function.

        """
        error = None
        for i in range(max_retry):
            try:
                results = self.func(*self.args, **self.kwargs)
            except exceptions as ex:
                error = ex
                time.sleep(math.exp(i))
            else:
                return results
        # The following will be executed only if for loop finishes without break/return
        else:
            raise error
