import datetime
import gzip
import random
import string
from .utils import remove_non_alphanumeric


class FileName:
    """Represents a filename and provides methods for modifying the filename.

    A filename is a string consist of a "name" and an "extension".
    For example, in filename "hello_world.txt", "hello_world" is the name and ".txt" is the extension.
    This class provides methods for prepending or appending strings to the "name" part of the filename.

    Most methods in this class support "Method Chaining", i.e. they return the FileName instance itself.
    Use to_string() or str() to obtain the modified filename.

    Example:
        modified_filename = FileName(original_filename).append_today().append_random_uppercase(2).to_string()

    """
    def __init__(self, filename):
        """Initializes a FileName instance

        Args:
            filename (str): filename (including file extension) as a string, e.g. "hello_world.txt".
        """
        self.basename = filename
        self.modified_name = filename

        name_splits = self.basename.rsplit('.', 1)

        self.name_no_extension = name_splits[0]
        self.name_alphanumeric = remove_non_alphanumeric(self.name_no_extension)

        if len(name_splits) == 1:
            self.extension = ''
        else:
            self.extension = '.' + name_splits[1]

    def __str__(self):
        return self.modified_name

    def to_string(self):
        """Returns the modified filename.
        """
        return self.modified_name

    def prepend_strings(self, s, delimiter='_'):
        """Prepends a list of strings to the filename, connected by the delimiter.

        Args:
            s (str/list): A string or a list of strings to be prepended to the filename.
            delimiter: A string concatenating the original filename and each of the prepended strings.

        Returns: FileName instance (self)

        """
        if not isinstance(s, list):
            s = [s]

        prefix = delimiter.join(s + [self.name_no_extension])
        self.modified_name = prefix + self.extension
        return self

    def append_strings(self, s, delimiter='_'):
        """Appends a list of strings to the filename, connected by the delimiter.

        Args:
            s (str/list): A string or a list of strings to be appended to the filename.
            delimiter: A string concatenating the original filename and each of the appended strings.

        Returns: FileName instance (self)

        """
        if not isinstance(s, list):
            s = [s]

        prefix = delimiter.join([self.name_no_extension] + s)
        self.modified_name = prefix + self.extension
        return self

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


def unzip_gz_file(file_path):
    """Un-zips a .gz file within the same directory.

    Args:
        file_path (str): the file to be unzipped.

    Returns:
        The output filename with full path.
    """
    output_file = file_path[:-3]
    with gzip.open(file_path, 'rb') as gzip_file:
        with open(output_file, "wb") as unzipped_file:
            print("Unzipping %s..." % file_path.split("/")[-1])
            block_size = 1 << 20
            while True:
                block = gzip_file.read(block_size)
                if not block:
                    break
                unzipped_file.write(block)
    return output_file
