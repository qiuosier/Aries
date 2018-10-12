import time


def retry_if_exceptions(max_retry, exceptions, func, *args, **kwargs):
    """Runs a function and retry a few times if certain exceptions occurs.

    Args:
        max_retry (int): The number of times to re-try.
        exceptions (list): A list of exception classes.
        func: The function to be executed.
        *args: A list of arguments for the function to be executed.
        **kwargs: A dictionary of keyword arguments for the function to be executed.

    Returns:

    """
    error = None
    for _ in range(max_retry):
        try:
            results = func(*args, **kwargs)
        except exceptions as ex:
            error = ex
            time.sleep(1)
        else:
            return results
    else:
        raise error
