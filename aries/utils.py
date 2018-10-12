import string


def remove_non_alphanumeric(s):
    """Removes non alpha-numeric characters from a string

    Args:
        s (str): A string.

    Returns:

    """
    new_str = "".join([
        c for c in s
        if (c in string.digits or c in string.ascii_letters)
    ])
    return new_str
