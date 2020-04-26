# Aries.storage: A Unified Storage Interface

_Read and write files on Google Cloud Storage and Amazon S3 as if they are on local computer_

The Aries storage sub-package provides a unified interface for accessing files and folders on local and cloud storage systems. The `StorageFile` class transform a file on cloud storage (e.g. Google Cloud Bucket) into a file-like object (stream). This enables us to read and write files on cloud storage like reading and writing on local disks. In addition, this package also includes high level APIs like copy, move and delete files and folders.

## Motivation
As cloud platforms getting closer to our daily lives, file storage means more than just the hard drive on local computer. However, there is no standard cloud storage interface for reading and writing file on the cloud. The methods depends on the APIs provided by different providers. Also, reading and writing files on the cloud are so different from reading and writing files on the local computer. We have to treat them differently in the code. This package solves the problem by providing a unified way to access local and cloud storage. The IO interface is also designed to be the same as the way we access files on local computer. With this package, the modification on existing code to support cloud storage can be reduced significantly.

## Implementation
Data access is provided through three classes: `Aries.storage.StorageFile`, `Aries.storage.StorageFolder` and `Aries.storage.StoragePrefix`. Each of them wraps an underlying "raw (or raw_io)" class, which contains platform dependent implementation. The [Uniform Resource Identifier (URI))](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier), e.g. `file:///var/text.txt` or `gs://bucket_name/text.txt`, is used to locate a file or folder. `StorageFile` and `StorageFolder` determine the underlying "raw" class automatically based on the scheme from the URI.

Currently, the following schemes are implemented:
* Local computer (`file://`)
* Google Cloud Storage (`gs://`)
* Amazon S3 Storage (`s3://`)

## The StorageFile Class
A `StorageFile` object can be initialized by
```
from Aries.storage import StorageFile

# uri: the Uniform Resource Identifier for a file
# local file path can also be used as uri.
uri = "/path/to/file.txt"
f = StorageFile(uri)
```
`StorageFile()` automatically determines the storage type by the scheme in the URI. For local file, URI can also be `/var/text.txt` without the scheme.

With a `StorageFile`, you can:
* Get the file size: `StorageFile("path/to/file").size`
* Get the md5 hex: `StorageFile("path/to/file").md5_hex`
* Get the last update time: `StorageFile("path/to/file").updated_time`
* Check if the file exist: `StorageFile("path/to/file").exist()`
* Create an empty file: `StorageFile("path/to/file").create()`
* Copy the file to another location: `StorageFile("path/to/file").copy("gs://path/to/destination")`
* Move the file to another location: `StorageFile("path/to/file").move("gs://path/to/destination")`
* Read the file (as bytes) into memory: `StorageFile("path/to/file").read()`
* Delete the file: `StorageFile("path/to/file").delete()`

StorageFile is a file-like object implementing the I/O stream interface with [BufferedIOBase and TextIOBase](https://docs.python.org/3/library/io.html#class-hierarchy). The static `StorageFile.init(uri, mode)` method is designed to replace the built-in `open()` method.

However, initializing the `StorageFile` does NOT open the file. The `StorageFile` object provides `open()` and `close()` methods for opening and closing the file for read/write. The `open()` method returns the `StorageFile` instance itself.

Here is an example of using `StorageFile` with [`pandas`](https://pandas.pydata.org/):
```
from Aries.storage import StorageFile
import pandas as pd
df = pd.DataFrame([1, 3, 5])

uri = "gs://bucket_name/path/to/file.txt"
# Using StorageFile in pandas
f = StorageFile(uri).open('w'):
# f will be a file-like object
df.to_csv(f)
f.close()
```

The `StorageFile.init()` static method provides a shortcut for initializing and opening the file. This is designed to replace the built-in python `open()` method. The `init()` method returns a `StorageFile` instance. `StorageFile` also support context manager to open and close the file:
```
from Aries.storage import StorageFile
import pandas as pd
df = pd.DataFrame([1, 3, 5])

uri = "gs://bucket_name/path/to/file.txt"
# Using StorageFile in pandas
with StorageFile.init(uri, 'w') as f:
    # f will be a file-like object
    df.to_csv(f)
```
Once the file is opened, it can be used as a file-like object. The data can be accessed through methods like `read()` and `write()`. However, for Cloud Storage, the `StorageFile` might not have a `fileno` or file descriptor. In that case, it cannot be used when `fileno` is needed.

The `init()` and `open()` methods supports the same arguments as the Python built-in [`open()`](https://docs.python.org/3/library/functions.html#open) function. However, at this time, only the `mode` argument is used when opening cloud storage files.

## High-Level APIs
The `StorageFile` class also supports high-level operations, including:
* `copy()`, for copying the file to another location, e.g. `StorageFile('/path/to/file.txt').copy('gs://bucket_name/path/to/file.txt')`
* `move()`, for moving the file, e.g. `StorageFile('/path/to/file.txt').move('s3://bucket_name/path/to/file.txt')`
* `delete()`, for deleting the file, e.g. `StorageFile('/path/to/file.txt').delete()`.

The `copy()` and `move()` methods also support cross-platform operations. For example: 
```
# Move a file from local computer to Google cloud storage.
StorageFile('/path/to/file.txt').move('gs://bucket_name/path/to/file.txt')
```

## The StorageFolder Class
The `StorageFolder` class provides the same high level APIs as the `StorageFile` class, as well as shortcuts for listing the files in a folder.
