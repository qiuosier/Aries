import string
import re
import random
import datetime
import base64
import os
import json
import copy
from collections import abc


class AString(str):
    """AString represents "Aries String", a sub-class of python built-in str with additional methods.
    AString inherits all methods of the python str.
    Instance of AString can be use in place of python str.

    Important: AString converts NoneType to empty string.

    For methods in python str returning a str, list, or tuple,
        additional post-processing are added to convert the returning str values to instances AString.
        e.g., AString("hello").title() will return AString("Hello").
    This is designed to enable method chaining for AString methods,
        e.g., AString("hello").title().append_today().

    """
    def __new__(cls, string_literal):
        """Creates a new string from string literal.
        str is immutable and it cannot be modified in __init__

        Args:
            string_literal (str): String literal

        Returns:
            str: [description]
        """
        if string_literal is None:
            return super(AString, cls).__new__(cls, "")
        else:
            return super(AString, cls).__new__(cls, string_literal)

    def __getattribute__(self, item):
        """Wraps the existing methods of python str to return AString objects instead of build-in strings.
        If the existing method returns a str, it will be converted to an instance of AString.
        If the existing method returns a list or tuple,
            the str values in the list or tuple will be converted to instances of AString.

        See Also:
            https://docs.python.org/3.5/reference/datamodel.html#object.__getattribute__
            https://stackoverflow.com/questions/7255655/how-to-subclass-str-in-python

        """
        def method(s, *args, **kwargs):
            # super() returns a a proxy object that delegates method calls to a parent or sibling class
            # See https://docs.python.org/3/library/functions.html#super
            value = getattr(super(AString, self), item)(*args, **kwargs)
            # Return value is str, list, tuple:
            if isinstance(value, str):
                return type(s)(value)
            elif isinstance(value, list):
                return [type(s)(i) for i in value]
            elif isinstance(value, tuple):
                return tuple(type(s)(i) for i in value)
            # Return value is dict, bool, or int
            return value

        # If the method is a method of str
        if item in dir(str):  # only handle str methods here
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
        """Appends a list of strings, connected by the delimiter.

        Args:
            s (str/list): A string or a list of strings to be appended to the filename.
            delimiter: A string concatenating the original filename and each of the appended strings.

        Returns: An AString instance

        """
        if not isinstance(s, list):
            s = [s]
        return AString(delimiter.join([self] + s))

    def append_datetime(self, dt=datetime.datetime.now(), fmt="%Y%m%d_%H%M%S"):
        """Appends date and time.
        The current date and time will be appended by default.

        Args:
            dt (datetime.datetime): A datetime.datetime instance.
            fmt (str): The format of the datetime.

        Returns: An AString instance

        """
        datetime_string = dt.strftime(fmt)
        return self.append(datetime_string)

    def append_today(self, fmt="%Y%m%d"):
        """Appends today's date.

        Args:
            fmt (str): The format of the date.

        Returns: An AString instance

        """
        return self.append_datetime(fmt=fmt)

    def append_random(self, choices, n):
        """Appends a random string of n characters.

        Args:
            choices (str): A string including the choices of characters.
            n (int): The number of characters to be appended.

        Returns: An AString instance

        """
        random_chars = ''.join(random.choice(choices) for _ in range(n))
        return self.append(random_chars)

    def append_random_letters(self, n):
        """Appends a random string of letters.

        Args:
            n (int): The number of characters to be appended.

        Returns: An AString instance

        """
        return self.append_random(string.ascii_letters, n)

    def append_random_uppercase(self, n):
        """Appends a random string of uppercase letters.

        Args:
            n (int): The number of characters to be appended.

        Returns: An AString instance

        """
        return self.append_random(string.ascii_uppercase, n)

    def append_random_lowercase(self, n):
        """Appends a random string of lowercase letters.

        Args:
            n (int): The number of characters to be appended.

        Returns: An AString instance

        """
        return self.append_random(string.ascii_lowercase, n)

    def remove_non_alphanumeric(self):
        """Removes non alpha-numeric characters from a string, including space and special characters.

        Returns: An AString with only alpha-numeric characters.

        """
        new_str = "".join([
            c for c in self
            if (c in string.digits or c in string.ascii_letters)
        ])
        return AString(new_str)

    def remove_non_ascii(self):
        """Removes non ASCII characters in the string.
        
        Returns: An AString with only ASCII characters.
        """
        return AString("".join([c for c in self if ord(c) <= 127]))

    def remove_escape_sequence(self):
        """Removes ANSI escape sequences, including color codes.

        Returns: An AString with escape sequence removed.

        """
        return AString(re.sub(r"\x1B\[[0-?]*[ -/]*[@-~]", "", self))


class FileName(AString):
    """Represents a filename and provides methods for modifying the filename.

    A "filename" is a string consist of a "basename" and an "extension".
    For example, in filename "hello_world.txt", "hello_world" is the basename and ".txt" is the extension.
    The extension can also be empty string. The filename should not contain a "." if the extension is empty.
    For example, "hello_world" is a filename with no extension.

    This class provides methods for modifying the "basename" of the filename.
    No modification will be applied to the "extension".

    Attributes:
        basename: The filename without extension
        extension: The file extension starting with "."

    Remarks:
        All methods will be operate on the "basename" only.
        This class is a sub-class of AString
        Most methods in this class support "Method Chaining", i.e. they return the FileName instance itself.
        If the method is inherited from AString or str, and the return value is also an AString or str
            the extension will be appended to the return value.
        If the return value of the method is not an AString or str,
            the extension will NOT be included in the return value.
        When using with operators (including +, *, slice, in),
            an FileName instance will be treated the same as str.
        len() will return the length of the whole filename, including "." and extension.

    """
    def __new__(cls, string_literal):
        name_splits = string_literal.rsplit('.', 1)
        filename = super(FileName, cls).__new__(cls, string_literal)
        filename.basename = name_splits[0]
        if len(name_splits) == 1:
            filename.extension = ""
        else:
            filename.extension = "." + name_splits[1]
        return filename

    def __getattribute__(self, item):
        """Wraps the existing methods of python AString to return FileName objects.
        """
        if item not in FileName.__dict__ and item in dir(AString):
            def method(s, *args, **kwargs):
                value = getattr(AString(self.basename), item)(*args, **kwargs)
                if isinstance(value, AString) or isinstance(value, str):
                    filename = type(s)(str(value) + self.extension)
                    return filename
                else:
                    return value
            # Bound method
            return method.__get__(self, type(self))
        else:
            # Delegate to parent
            return super(FileName, self).__getattribute__(item)

    @property
    def name_without_extension(self):
        """The filename without extension.
        """
        return os.path.basename(self.basename)

    def to_string(self):
        """Convert the FileName object to a string including basename and extension.
        """
        return str(self)


class Base64String(str):
    """Represents a string that is base64encoded from bytes.

    The error parameter used in the methods are passed into the str.encode() method.
    See Also: https://docs.python.org/3/library/stdtypes.html#str.encode
    
    """
    @staticmethod
    def encode_from_file(file_path, encoding='utf-8', errors="strict"):
        """Encodes a file into Base64 string.
        
        Args:
            file_path (str): Location of the file
            encoding (str, optional): Character encoding. Defaults to 'utf-8'.
            errors (str, optional): Error handling scheme. Defaults to "strict".
        
        Returns:
            Base64String: The encoded Base64 string.
        """
        with open(file_path, "rb") as f:
            content = f.read()
            return Base64String(base64.b64encode(content).decode(encoding, errors))

    @staticmethod
    def encode_from_string(s, encoding='utf-8', errors="strict"):
        """Encodes a string into Base64 string.
        
        Args:
            s (str): The string to be encoded.
            encoding (str, optional): Character encoding. Defaults to 'utf-8'.
            errors (str, optional): Error handling scheme. Defaults to "strict".
        
        Returns:
            Base64String: The encoded Base64 string.
        """
        return Base64String(base64.b64encode(s.encode(encoding, errors)).decode(encoding, errors))

    def decode_to_file(self, file_path, encoding='utf-8', errors="strict"):
        """Decodes the Base64 string and saves the contents into a file.
        
        Args:
            file_path (str): The location to save the file. Existing file will be overwritten.
            encoding (str, optional): Character encoding. Defaults to 'utf-8'.
            errors (str, optional): Error handling scheme. Defaults to "strict".

        """
        folder = os.path.dirname(file_path)
        if not os.path.exists(folder):
            os.makedirs(folder)
        with open(file_path, "wb") as f:
            f.write(base64.b64decode(self.encode(encoding, errors)))

    def decode_to_string(self, encoding='utf-8', errors="strict"):
        """Decodes the Base64 string and saves the contents into a string.
        
        Args:
            encoding (str, optional): Character encoding. Defaults to 'utf-8'.
            errors (str, optional): Error handling scheme. Defaults to "strict".
        
        Returns:
            [type]: [description]
        """
        return base64.b64decode(self.encode(encoding, errors)).decode(encoding, errors)


def stringify(obj):
    """Convert object to string.
    If the object is a dictionary-like object or list,
    the objects in the dictionary or list will be converted to strings, recursively.

    Returns: If the input is dictionary or list, the return value will also be a list or dictionary.

    """
    if isinstance(obj, abc.Mapping):
        obj = copy.deepcopy(obj)
        obj_dict = {}
        for key, value in obj.items():
            obj_dict[key] = stringify(value)
        return obj_dict
    elif isinstance(obj, list):
        str_list = []
        for item in obj:
            str_list.append(stringify(item))
        return str_list
    else:
        try:
            json.dumps(obj)
            return obj
        except TypeError:
            return str(obj)
