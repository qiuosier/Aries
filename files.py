import gzip
import os
import tempfile
from shutil import copyfile
try:
    from .strings import FileName
except SystemError:
    import sys
    from os.path import dirname
    aries_dir = dirname(__file__)
    if aries_dir not in sys.path:
        sys.path.append(aries_dir)
    from strings import FileName


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
            filename = FileName(filename).append_random_letters(8)
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
