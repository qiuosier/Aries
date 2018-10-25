import string
import re


class AString(str):
    def __new__(cls, string_literal):
        return super().__new__(cls, string_literal)

    def remove_non_alphanumeric(self):
        """Removes non alpha-numeric characters from a string

        Returns:

        """
        new_str = "".join([
            c for c in self
            if (c in string.digits or c in string.ascii_letters)
        ])
        return AString(new_str)

    def remove_escape_sequence(self):
        """Removes ANSI escape sequences, including color codes.

        Returns: A string with escape sequence removed.

        """
        return AString(re.sub(r"\x1b\[.*m", "", self))
