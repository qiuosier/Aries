import string
import re
import random
import datetime


class AString(str):
    """AString represents an "Aries String", a sub-class of python built-in str with additional methods.
    AString inherits all methods of the python str.
    Instance of AString can be use in place of python str.

    For methods in python str returning a str, list, or tuple,
        additional post-processing are added to convert the returning str values to instances AString.
    For example, AString("hello").title() will return AString("Title").
    This is designed to enable method chaining for AString methods.

    """
    def __new__(cls, string_literal):
        return super(AString, cls).__new__(cls, string_literal)

    def __getattribute__(self, item):
        """Wraps the existing methods of python str
        If the existing method returns a str, it will be converted to an instance of AString.
        If the existing method returns a list or tuple,
            the str values in the list or tuple will be converted to instances of AString.

        See Also:
            https://docs.python.org/3.5/reference/datamodel.html#object.__getattribute__
            https://stackoverflow.com/questions/7255655/how-to-subclass-str-in-python

        """
        if item in dir(str):  # only handle str methods here
            def method(s, *args, **kwargs):
                value = getattr(super(AString, self), item)(*args, **kwargs)
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
            # Bound method
            return method.__get__(self, type(self))
        else:
            # Delegate to parent
            return super(AString, self).__getattribute__(item)

    def prepend(self, s, delimiter='_'):
        """Prepends the string with another string or a list of strings, connected by the delimiter.

        Args:
            s (str/list): A string or a list of strings to be prepended to the filename.
            delimiter: A string concatenating the original filename and each of the prepended strings.

        Returns: An AString instance

        """
        if not isinstance(s, list):
            s = [s]
        return AString(delimiter.join(s + [self]))

    def append(self, s, delimiter='_'):
        """Appends a list of strings to the filename, connected by the delimiter.

        Args:
            s (str/list): A string or a list of strings to be appended to the filename.
            delimiter: A string concatenating the original filename and each of the appended strings.

        Returns: FileName instance (self)

        """
        if not isinstance(s, list):
            s = [s]
        return AString(delimiter.join([self] + s))

    def remove_non_alphanumeric(self):
        """Removes non alpha-numeric characters from a string, including space and special characters.

        Returns: An AString with only alpha-numeric characters.

        """
        new_str = "".join([
            c for c in self
            if (c in string.digits or c in string.ascii_letters)
        ])
        return AString(new_str)

    def remove_escape_sequence(self):
        """Removes ANSI escape sequences, including color codes.

        Returns: An AString with escape sequence removed.

        """
        return AString(re.sub(r"\x1b\[.*m", "", self))


class FileName(AString):
    """Represents a filename and provides methods for modifying the filename.

    A "filename" is a string consist of a "name" and an "extension".
    For example, in filename "hello_world.txt", "hello_world" is the name and ".txt" is the extension.
    The extension can also be empty string. The filename will not contain a "." if the extension is empty.
    For example, "hello_world" is a filename with no extension.

    This class provides methods for prepending or appending strings to the "name" part of the filename.

    This class is a sub-class of AString
    Most methods in this class support "Method Chaining", i.e. they return the FileName instance itself.
    Use to_string() or str() to obtain the modified filename.

    Example:
        modified_filename = FileName(original_filename).append_today().append_random_uppercase(2).to_string()

    Attributes:
        basename: The original filename used to initialize the FileName instance.
        modified_name: The

    """
    def __init__(self, filename):
        """Initializes a FileName instance

        Args:
            filename (str): filename (including file extension) as a string, e.g. "hello_world.txt".
        """
        self.basename = AString(filename)
        self.modified_name = AString(filename)

        name_splits = self.basename.rsplit('.', 1)

        self.name_no_extension = AString(name_splits[0])
        self.name_alphanumeric = AString(self.name_no_extension).remove_non_alphanumeric()

    @property
    def name_without_extension(self):
        name_splits = self.rsplit('.', 1)
        return AString(name_splits[0])

    @property
    def extension(self):
        name_splits = self.rsplit('.', 1)
        if len(name_splits) == 1:
            return AString('')
        else:
            return AString('.' + name_splits[1])

    def __str__(self):
        return self.modified_name

    def to_string(self):
        """Returns the modified filename.
        """
        return self.modified_name



    def append_datetime(self, dt=datetime.datetime.now(), fmt="%Y%m%d_%H%M%S"):
        """Appends date and time to the filename.
        The current date and time will be appended by default.

        Args:
            dt (datetime.datetime): A datetime.datetime instance.
            fmt (str): The format of the datetime.

        Returns: FileName instance (self)

        """
        datetime_string = dt.strftime(fmt)
        return self.append_strings(datetime_string)

    def append_today(self, fmt="%Y%m%d"):
        """Appends today's date

        Args:
            fmt (str): The format of the date.

        Returns: FileName instance (self)

        """
        return self.append_datetime(fmt=fmt)

    def append_random(self, choices, n):
        """Appends a random string of n characters to the filename.

        Args:
            choices (str): A string including the choices of characters.
            n (int): The number of characters to be appended.

        Returns: FileName instance (self)

        """
        random_chars = ''.join(random.choice(choices) for _ in range(n))
        return self.append_strings(random_chars)

    def append_random_uppercase(self, n):
        return self.append_random(string.ascii_uppercase, n)
