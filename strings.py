import string
import re


class AString(str):
    def __new__(cls, string_literal):
        return super().__new__(cls, string_literal)

    def __getattribute__(self, item):
        """

        Args:
            item:

        Returns:

        See Also:
            https://docs.python.org/3.5/reference/datamodel.html#object.__getattribute__
            https://stackoverflow.com/questions/7255655/how-to-subclass-str-in-python

        """
        if item in dir(str):  # only handle str methods here
            def method(s, *args, **kwargs):
                value = getattr(super(), item)(*args, **kwargs)
                # Return value is str, list, tuple:
                if isinstance(value, str):
                    return type(s)(value)
                elif isinstance(value, list):
                    return [type(s)(i) for i in value]
                elif isinstance(value, tuple):
                    return tuple(type(s)(i) for i in value)
                else:
                    # dict, bool, or int
                    return value

            return method.__get__(self, type(self))  # bound method
        else:
            # delegate to parent
            return super().__getattribute__(item)

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
