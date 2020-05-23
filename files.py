import binascii
import gzip
import os
import json
import tempfile
import logging
import re
from urllib.parse import urljoin, urlparse
from shutil import copyfile, copyfileobj
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
            hex_value = self.hex(max_size, offset=int(offset)).decode().upper()
            for sign in signs:
                if hex_value.startswith(sign.upper()):
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
                logger.debug("Unzipping %s to %s ..." % (self.file_path, output_file))
                block_size = 1 << 20
                while True:
                    block = gzip_file.read(block_size)
                    if not block:
                        break
                    unzipped_file.write(block)
        return output_file

    def gzip(self, to_path=None):
        if to_path is None:
            to_path = self.file_path + ".gz"
        with open(self.file_path, 'rb') as f_in:
            with gzip.open(to_path, 'wb') as f_out:
                copyfileobj(f_in, f_out)
        return to_path


class Markdown:
    """Represents a Markdown file.
    """

    # A list of regex patterns for links,
    # If there is a match, group(1) must be the url
    link_patterns = [
        # Inline link
        r"\[.+\]\((\S+)( [\"\'\(].+[\"\'\)])?\)",
        # HTML link
        r"<a .*?[\n]?.*?href=[\"\'](.+?)[\"\'].*?[\n]?.*?>.*?[\n]?.*?</a>",
        # Reference link with title in a new line
        r"\[.+\]:[ \t]*(\S+)\s*([\"\'\(].+[\"\'\)])",
        # Reference link
        r"\[.+\]:[ \t]*(\S+)[ \t]*([\"\'\(].+[\"\'\)])?",
    ]

    def __init__(self, file_path=None):
        """Initialize a Markdown object
        
        Args:
            file_path (str): File path of a Markdown(.md) file.
        """
        self.file_path = file_path
        if not self.file_path:
            self.text = None
            return
        with open(self.file_path) as f:
            self.text = f.read()

    @staticmethod
    def from_text(s):
        md = Markdown()
        md.text = s
        return md

    @property
    def title(self):
        """Gets the first title of the Markdown file.
        """
        lines = self.text.split("\n")
        for line in lines:
            if line.startswith("#"):
                return line.strip("#").strip("\n").strip()
        return ""

    def find_links(self):
        """Finds all links in the markdown.

        Returns: A list of 2-tuples. In each tuple,
            The first element is the string matching a link pattern
            The second element is the link URL

        """
        patterns = "|".join(["(%s)" % pattern for pattern in self.link_patterns])
        match_iter = re.finditer(patterns, self.text)
        matches = list(match_iter)
        match_list = []
        for match in matches:
            groups = [element for element in match.groups() if element is not None]
            logger.debug(groups)
            if len(groups) > 1:
                match_list.append((groups[0], groups[1]))
            else:
                match_list.append((groups[0], None))
        return match_list

    def make_links_absolute(self, base_url):
        """Converts all links in the file to absolute links.

        Returns:

        """
        replace_dict = {}
        links = self.find_links()
        for link in links:
            text = link[0]
            url = link[1]
            result = urlparse(url)
            if not result.scheme:
                abs_url = urljoin(base_url, url)
                replace_dict[text] = text.replace(url, abs_url)
        for key, val in replace_dict.items():
            self.text = re.sub(re.escape(key), val, self.text, 1)
        return self.text


class TemporaryFile:
    # TODO: Inherit from NamedTemporaryFile
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
        self.temp_file = "None"

    def new(self):
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

    def remove(self):
        """Removes the temp_file.
        """
        if os.path.exists(self.temp_file):
            os.remove(self.temp_file)

    def filename(self):
        return os.path.basename(self.temp_file)

    def __enter__(self):
        return self.new()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove()
