import re
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


def remove_escape_sequence(s):
    """Removes ANSI escape sequences, including color codes.

    Args:
        s (str): A string

    Returns: A string with escape sequence removed.

    """
    return re.sub(r"\x1b\[.*m", "", s)
