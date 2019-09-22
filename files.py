import binascii
import gzip
import os
import json
import tempfile
import logging
from shutil import copyfile
from .strings import FileName

logger = logging.getLogger(__name__)


class File:
    """Provides shortcuts for handling files.
    """

    __signatures = None
    __sign_size = None

    @staticmethod
    def load_json(file_path, default=None):
        """Loads data from a JSON file to a Python dictionary

        Args:
            file_path: file path of a json file.
            default: default value to be returned if the file does not exist.
                If default is None and file is not found , an empty dictionary will be returned.

        Returns: A python dictionary containing data from the json file.
        """
        if os.path.exists(file_path):
            with open(file_path) as f:
                data = json.load(f)
        else:
            logger.info("File %s Not Found." % file_path)
            if default:
                data = default
            else:
                data = {}
        return data

    @staticmethod
    def load_signatures(json_path=None):
        """Loads the file signature dictionary.
        
        See Also: helpers.file_signatures.py
        
        Args:
            json_path (str, optional): Path of the JSON file containing the signature dictionary. 
            If json_path is None, "./assets/file_signatures.json" will be used.
        
        Returns:
            dict: Signature dictionary
        """
        if json_path is None:
            json_path = os.path.join(os.path.dirname(__file__), "assets", "file_signatures.json")
        with open(json_path, "r") as f:
            signatures = json.load(f)
        return signatures

    def __init__(self, file_path):
        self.file_path = file_path

    @property
    def signatures(self):
        """Signature dictionary

        Each key is the number of offset bytes.
        Each value is a dictionary with signatures(hex) as keys and file mine as values.
        
        Returns:
            dict: Signature dictionary
        """
        if self.__signatures is None:
            self.__signatures = self.load_signatures()
            self.__sign_size = {}
            for offset, v in self.__signatures.items():
                signs = v.keys()
                max_size = 0
                for sign in signs:
                    size = len(sign)
                    max_size = size if size > max_size else max_size
                self.__sign_size[offset] = max_size
        return self.__signatures

    def hex(self, size, offset=0):
        """Reads bytes from the file as hexadecimal string
        
        Args:
            size (int): The number of bytes to read.
            offset (int, optional): The offset to start reading. Defaults to 0.
        
        Returns:
            str: Return the hexadecimal representation of the binary data. 
            Every byte of data is converted into the corresponding 2-digit hex representation. 
            The resulting string is therefore twice of size.
        """
        with open(self.file_path, 'rb') as f:
            if offset:
                f.read(offset)
            return binascii.hexlify(f.read(size))

    def file_type(self):
        """Tries to identify the file type
        
        Returns:
            str: file mime type if identified, otherwise None.
        """
        signatures = self.signatures
        for offset, max_size in self.__sign_size.items():
            signs = sorted(signatures.get(offset).keys(), reverse=True)
            hex_value = self.hex(max_size, offset=int(offset)).decode()
            for sign in signs:
                if hex_value.startswith(sign):
                    return signatures.get(offset).get(sign)
        return None

    def unzip(self, to_path=None):
        """Un-zips a .gz file within the same directory.

        Args:
            to_path (str): the output file path to store the unzipped file.

        Returns:
            The output filename with full path.
        """
        if to_path is None:
            output_file = self.file_path[:-3]
        else:
            output_file = to_path
        with gzip.open(self.file_path, 'rb') as gzip_file:
            with open(output_file, "wb") as unzipped_file:
                logger.debug("Unzipping %s..." % self.file_path)
                block_size = 1 << 20
                while True:
                    block = gzip_file.read(block_size)
                    if not block:
                        break
                    unzipped_file.write(block)
        return output_file


class Markdown:
    """Represents a Markdown file.
    """
    def __init__(self, file_path):
        """Initialize a Markdown object
        
        Args:
            file_path (str): File path of a Markdown(.md) file.
        """
        self.file_path = file_path
        with open(self.file_path) as f:
            self.text = f.read()

    @property
    def title(self):
        """Gets the first title of the Markdown file.
        """
        lines = self.text.split("\n")
        for line in lines:
            if line.startswith("#"):
                return line.strip("#").strip("\n").strip()
        return ""


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


class TemporaryFile:
    """Represents a temporary file.

    Examples:
        with TemporaryFile(template) as temp_file:
            ...

    If "template" is None, a new tempfile.NamedTemporaryFile() will be created.

    This will create a temp_file by copying an existing file (template).
    The temp_file will be deleted when exiting the "with"

    """
    def __init__(self, template=None):
        """Initialize with the file path of a template.

        Args:
            template: A template for creating the temporary file.
                The temp file will be a copy of the template.
                A empty NamedTemporaryFile() will be created if template is None.

        """
        self.template = template
        self.temp_file = None

    def __enter__(self):
        """Creates a temp file by copying the template, if any.

        Returns: The full path of the temp file.

        """
        if self.template:
            filename = os.path.basename(self.template)
            filename = FileName(filename).append_random_letters(8).to_string()

            temp_folder = tempfile.gettempdir()
            self.temp_file = os.path.join(temp_folder, filename)
            copyfile(self.template, self.temp_file)
        else:
            f = tempfile.NamedTemporaryFile(delete=False)
            f.close()
            self.temp_file = f.name
        return self.temp_file

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Removes the temp_file.
        """
        if os.path.exists(self.temp_file):
            os.remove(self.temp_file)
