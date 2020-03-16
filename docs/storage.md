# Aries.storage: A Unified Storage Interface
The Aries storage sub-package is intended to provide a unified interface for accessing files and folders. This enables us to read and write cloud storage (e.g. Google Cloud Bucket) like reading and writing local disks. It also includes object-oriented shortcuts for listing, copying, deleting files/folder.

As cloud platforms getting closer to our daily lives, file storage means more than just the hard drive on local computer. However, there is no standard cloud storage interface for reading and writing file on the cloud. The methods depends on the APIs provided by different providers. Also, reading and writing files on the cloud are so different from reading and writing files on the local computer. We have to treat them differently in the code. This package enables us to access the files on the cloud using the same way we access the files on local computer. 

File access is achieved by implementing the `BufferedIOBase` interface for cloud storage. The `StorageFile.init(uri, mode)` method is designed to replace the built-in `open()` method. It returns a file-like object implementing the `BufferedIOBase` or `TextIOBase`, depending on the `mode`.

Currently, the following schemes are implemented:
* Local computer (`file://`)
* Google Cloud Storage (`gs://`)
* Amazon S3 Storage (`s3://`)

## The StorageFile Class
StorageFile is a file-like object implementing the I/O stream interface with [BufferedIOBase and TextIOBase](https://docs.python.org/3/library/io.html#class-hierarchy). 

Instead of using a file path to locate the file, the files are represented by [Uniform Resource Identifier (URI))](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier), e.g. `file://var/text.txt` or `gs://bucket_name/text.txt`. A `StorageFile` subclass object can be initialized by
```
from Aries.storage import StorageFile

# uri: the Uniform Resource Identifier for a file
# local file path can also be used as uri.
uri = "/path/to/file.txt"
f = StorageFile(uri)
```
`StorageFile()` automatically determines the storage type by the scheme in the URI. For local file, URI can also be `/var/text.txt` without the scheme.

Initializing the `StorageFile` does NOT open the file.
The instance will have:
* `open()` and `close()` for opening and closing the file for read/write
* `exists()` for determining whether the file exists.

Here is an example of using `StorageFile` with [`pandas`](https://pandas.pydata.org/):
```
import pandas as pd
df = pd.DataFrame([1, 3, 5])

uri = "gs://bucket_name/path/to/file.txt"
# Using StorageFile in pandas
f = StorageFile(uri).open('w'):
# f will be a file-like object
df.to_csv(f)
f.close()
```

The `StorageFile.init()` static method provides a shortcut for initializing and opening the file. This method returns a `StorageFile` instance. `StorageFile` also support context manager to open and close the file:
```
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
* copy(), for copying the file to another location, e.g. `StorageFile('/path/to/file.txt').copy('gs://bucket_name/path/to/file.txt')`
* move(), for moving the file, e.g. `StorageFile('/path/to/file.txt').move('gs://bucket_name/path/to/file.txt')`
* delete(), for deleting the file, e.g. `StorageFile('/path/to/file.txt').delete()`.

## The StorageFolder Class
The `StorageFolder` class provides the same high level APIs as the `StorageFile` class, as well as shortcuts for listing the files in a folder.
