# A Unified Interface for Storage
The storage and related modules are intended to provide a unified interface for accessing files and folders. This enables us to read and write cloud storage (e.g. Google Cloud Bucket) like reading and writing local disks. It also includes object-oriented shortcuts for listing, copying, deleting files/folder.

As cloud platforms getting closer to our daily lives, file storage means more than just the hard drive on local computer. However, there is no standard cloud storage interface for reading and writing file on the cloud. The methods depends on the APIs provided by different providers. Also, reading and writing files on the cloud are so different from reading and writing files on the local computer. We have to treat them differently in the code. The Aries Storage modules are designed to improve the ability to re-use our existing code and provide a simpler way of accessing files on the cloud. This is achieved by implementing the `IOBase` interface for cloud storage and having a way to automatically initialize file-like objects corresponding to the underlying storage types from URI.

Currently, this is implemented for:
* Local computer (`storage.LocalFile` and `storage.LocalFolder`)
* Google Cloud Storage (`gcp.storage.GSFile` and `gcp.storage.GSFolder`)

## Storage File
StorageFile is an abstract class for implementing file-like objects with [IOBase interface](https://docs.python.org/3/library/io.html#class-hierarchy). 

Both `LocalFile` and `GSFile` implements all IOBase stub methods and mixin methods and properties. They are seekable, readable and writable. With file path represented in a [Uniform Resource Identifier (URI))](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier), e.g. `file://var/text.txt` or `gs://bucket_name/text.txt`, a `StorageFile` subclass object can be initialized by
```
# uri the Uniform Resource Identifier for file path
# If uri is 'gs://bucket_name/text.txt', a GSFile instance will be returned.
# If uri is '/var/text.txt', a LocalFile instance will be returned.
f = StorageFile.init(uri)
```
`StorageFile.init()` automatically determines the storage type by the schemes in the URI. For local file, it can be also written as `/var/text.txt` without the schema. An instance of a subclass of StorageFile will be returned. The instance will have:
* `open()` and `close()` for opening and closing the file for read/write
* `exists()` for determining whether the file exists.

It also provides context manager to open and close the file:
```
import pandas as pd
df = pd.DataFrame([1, 3, 5])

# Using StorageFile in pandas
with StorageFile.init(uri) as f:
    # f will be a file-like object
    df.to_csv(f)
```
Once the file is opened, it can be used as a file-like object. However, the `StorageFile` does not have a `fileno` or file descriptor. It cannot be used when `fileno` is needed.

Both `LocalFile` and `GSFile` also support `read()` and `seek()` without opening the file.

## Storage Folder
The `StorageFolder` class provides shortcuts for listing the files in a folder.

## Google Cloud Storage
The `GSFile` class provides a file-like interface for reading and writing files on Google Cloud Storage Buckets.
