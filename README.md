# Aries

Aries is a Python package providing shortcuts to a wide range of small tasks like access files on the cloud, running background tasks, configuring logging, etc.

[![PyPI version](https://badge.fury.io/py/Aries-Python.svg)](https://pypi.org/project/Aries-Python/)
[![Build Status](https://travis-ci.org/qiuosier/Aries.svg?branch=master)](https://travis-ci.org/qiuosier/Aries)
[![Coverage Status](https://coveralls.io/repos/github/qiuosier/Aries/badge.svg?branch=master)](https://coveralls.io/github/qiuosier/Aries?branch=master)
[![codebeat badge](https://codebeat.co/badges/f7217133-f495-455d-8808-d2cdaf46bc60)](https://codebeat.co/projects/github-com-qiuosier-aries-master)

Aries includes:
* [Storage](docs/storage.md) sub-package, which provides a unified interface for accessing files on local computer, Google Cloud Storage and Amazon S3 Storage. The storage interface includes shortcuts for listing, copying, deleting files/folder.
* [Outputs](docs/outputs.md)(`outputs.py`) module for logging and capturing outputs.
* [Task](docs/tasks.md)(`tasks.py`) module to manage asynchronous/background functions and commands.
* [Google](docs/GoogleDrive.md) sub-package, providing simple API for accessing data in Google Drive and Google Sheets.
* String(`string.py`) module to provide enhanced String types, as well as FileName, Base64String.
* Excel(`excel.py`) module to create, read and modify Microsoft Excel spreadsheets.
* Files(`files.py`) module to handle JSON, Markdown and temporary files with templates.
* Web(`web.py`) module to access web APIs and HTML pages.

## Installation
Aries is developed with Python 3.7. You can install the full package with `pip`:
```
pip3 install Aries-Python
```

Alternatively, you can also install only the storage package using:
```
pip3 install Aries-storage
```

## Usage
To use this package, import the module/sub-package from `Aries`
```
from Aries.tasks import ShellCommand
```

As an example, here is how to use Aries to write a `pandas` DataFrame to a CSV file on Google Cloud Storage:
```
import pandas as pd
df = pd.DataFrame([1, 3, 5])

uri = "gs://bucket_name/path/to/file.txt"

# Using StorageFile with pandas
with StorageFile.init(uri, 'w') as f:
    # f will be a file-like object
    df.to_csv(f)
```

See the documentation for sub-packages for more details.
