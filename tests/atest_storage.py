"""Contains tests for the storage package.
"""
import datetime
import logging
import os
import sys
import time
import traceback
logger = logging.getLogger(__name__)


try:
    from ..test import AriesTest
    from ..storage import StoragePrefix, StorageFolder, StorageFile
    from ..storage import gs
except:
    aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
    if aries_parent not in sys.path:
        sys.path.append(aries_parent)
    from Aries.storage import gs
    from Aries.test import AriesTest
    from Aries.storage import StoragePrefix, StorageFile, StorageFolder


class TempFolder:
    def __init__(self, folder_uri):
        self.folder_uri = folder_uri

    def __enter__(self):
        folder = StorageFolder(self.folder_uri)
        if not folder.exists():
            folder.create()
        return folder

    def __exit__(self, exc_type, exc_val, exc_tb):
        folder = StorageFolder(self.folder_uri)
        if folder.exists():
            folder.delete()


class TestStorage(AriesTest):
    SCHEME = "file"
    HOST = ""
    TEST_ROOT_PATH = os.path.join(os.path.dirname(__file__), "fixtures", "test_folder")
    TEST_ROOT = "%s://%s%s" % (SCHEME, HOST, TEST_ROOT_PATH)
    test_folder = StorageFolder(TEST_ROOT)

    @classmethod
    def create_folder(cls, relative_path):
        abs_path = os.path.join(cls.TEST_ROOT, relative_path)
        StorageFolder(abs_path).create()

    @classmethod
    def create_file(cls, relative_path, content):
        """Creates a file relative to the test root
        """
        abs_path = os.path.join(cls.TEST_ROOT, relative_path)
        with StorageFile.init(abs_path, "w") as f:
            f.write(content)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if not cls.test_folder.exists():
            cls.test_folder.create()
        cls.test_folder.empty()
        cls.create_folder("test_folder_0")
        cls.create_folder("test_folder_1")
        cls.create_file("file_in_test_folder", "")
        cls.create_file("test_folder_0/empty_file", "")
        cls.create_file("test_folder_0/abc.txt", "abc\ncba\n")
        cls.create_file("test_folder_1/.hidden_file", "")

    @classmethod
    def tearDownClass(cls):
        StorageFolder(cls.TEST_ROOT).empty()
        StorageFolder(cls.TEST_ROOT).delete()
        super().tearDownClass()

    def setUp(self):
        super().setUp()

    def test_prefix(self):
        storage_prefix = StoragePrefix(os.path.join(self.TEST_ROOT, "test_folder"))
        self.assertEqual(len(storage_prefix.objects), 3)

    def test_storage_folder(self):
        # Scheme
        self.assertEqual(self.test_folder.scheme, self.SCHEME)
        # Hostname
        if self.test_folder.hostname or self.HOST:
            self.assertEqual(self.test_folder.hostname, self.HOST)
        # Folder URI will always end with "/"
        self.assertTrue(str(self.test_folder).endswith("/"))
        # Path
        self.assertEqual(self.test_folder.path.rstrip("/"), self.TEST_ROOT_PATH.rstrip("/"))
        # Name
        self.assertEqual(self.test_folder.basename, os.path.basename(self.TEST_ROOT_PATH))
        self.assertEqual(self.test_folder.name, os.path.basename(self.TEST_ROOT_PATH))
        # Folder attributes
        self.assertGreater(len(self.test_folder.get_folder_attributes()), 1)
        # File attributes
        self.assertIn("file_in_test_folder", self.test_folder.get_file_attributes("name"))

    def test_get_folder_names(self):
        folder = StorageFolder(self.TEST_ROOT)
        folder_names = folder.folder_names
        # There are two sub-folders
        self.assertEqual(len(folder_names), 2, 'Folders: %s' % folder_names)
        self.assertIn("test_folder_0", folder_names)
        self.assertIn("test_folder_1", folder_names)
        # "/" should not be found in folder names
        self.assertNotIn("/", str(folder_names))

    def test_get_paths(self):
        sub_folders = self.test_folder.folder_paths
        # There are two sub-folders
        self.assertEqual(len(sub_folders), 2, "Folders: %s" % sub_folders)
        # TODO: local paths not having scheme?
        if self.SCHEME == "file":
            self.assertIn(os.path.join(self.TEST_ROOT_PATH, "test_folder_0"), sub_folders)
            self.assertIn(os.path.join(self.TEST_ROOT_PATH, "test_folder_1"), sub_folders)
        else:
            # TODO: folders ends with "/"??
            self.assertIn(os.path.join(self.TEST_ROOT, "test_folder_0/"), sub_folders)
            self.assertIn(os.path.join(self.TEST_ROOT, "test_folder_1/"), sub_folders)

    def test_get_folder_and_file(self):
        # Try to get a folder that does not exist. Should return None
        self.assertIsNone(self.test_folder.get_folder("not_exist"))

        # Get a sub folder
        sub_folder = self.test_folder.get_folder("test_folder_1")
        self.assertTrue(
            isinstance(sub_folder, StorageFolder),
            "Failed to get sub folder. Received %s: %s" % (sub_folder.__class__, sub_folder))

        # Get the filename of hidden_file
        filenames = sub_folder.file_names
        self.assertEqual(len(filenames), 1)
        self.assertEqual(filenames[0], ".hidden_file")

        # Get a file that does not exist
        f = sub_folder.get_file("not_exist")
        self.assertIsNone(f)

        # Get the hidden file
        f = sub_folder.get_file(".hidden_file")
        self.assertTrue(f.exists(), "Failed to get the file.")
        self.assertEqual(f.basename, ".hidden_file")
        self.assertTrue(
            isinstance(f, StorageFile),
            "Failed to get storage file from a folder. Received %s: %s" % (f.__class__, f))

    def test_text_read(self):
        with StorageFile.init(os.path.join(self.TEST_ROOT, "file_in_test_folder")) as f:
            self.assertEqual(f.size, 0)
            self.assertEqual(f.tell(), 0)
            self.assertEqual(f.seek(0, 2), 0)
            self.assertEqual(len(f.read()), 0)

    def test_text_read_write(self):
        # Write a new file
        temp_file_path = os.path.join(self.TEST_ROOT, "temp_file.txt")
        with StorageFile.init(temp_file_path, 'w+') as f:
            self.assertTrue(f.writable())
            self.assertEqual(f.tell(), 0)
            self.assertEqual(f.write("abc"), 3)
            self.assertEqual(f.tell(), 3)
            f.seek(0)
            self.assertEqual(f.read(), "abc")
            # TODO: File may not exist on the cloud until it is closed.
            # self.assertTrue(f.exists())
        f.delete()

    def test_binary_read_write(self):
        # File does not exist, a new one will be created
        file_uri = os.path.join(self.TEST_ROOT, "test.txt")
        storage_file = StorageFile(file_uri).open("wb")
        self.assertEqual(storage_file.scheme, self.SCHEME)
        self.assertTrue(storage_file.seekable())
        self.assertFalse(storage_file.readable())
        self.assertEqual(storage_file.write(b"abc"), 3)
        self.assertEqual(storage_file.tell(), 3)
        self.assertEqual(storage_file.write(b"def"), 3)
        self.assertEqual(storage_file.tell(), 6)
        storage_file.close()
        self.assertTrue(storage_file.exists())
        storage_file.open('rb')
        self.assertEqual(storage_file.read(), b"abcdef")
        storage_file.close()
        storage_file.delete()
        self.assertFalse(storage_file.exists())

    def test_copy_folder(self):
        # Source folder to be copied
        src_folder_uri = os.path.join(self.TEST_ROOT, "test_folder_0")
        sub_folder = StorageFolder(src_folder_uri)
        # Source folder should exist
        self.assertTrue(sub_folder.exists())

        # Destination folder
        dst_folder_uri = os.path.join(self.TEST_ROOT, "new_folder", "test_folder_0")

        dst_parent = os.path.join(self.TEST_ROOT, "new_folder")
        with TempFolder(dst_parent):
            # Destination folder should not exist
            dst_folder = StorageFolder(dst_folder_uri)
            if dst_folder.exists():
                dst_folder.delete()
            self.assertFalse(dst_folder.exists())

            # Copy the folder
            if not dst_parent.endswith("/"):
                dst_parent += "/"
            logger.debug("Copying from %s into %s" % (sub_folder.uri, dst_parent))
            sub_folder.copy(dst_parent)

            # Destination folder should now exist and contain an empty file
            self.assertTrue(dst_folder.exists())
            file_paths = [f.uri for f in dst_folder.files]
            self.assertEqual(len(file_paths), 2)
            self.assertIn(os.path.join(dst_folder_uri, "empty_file"), file_paths)
            self.assertIn(os.path.join(dst_folder_uri, "abc.txt"), file_paths)

    def test_create_copy_and_delete_file(self):
        new_folder_uri = os.path.join(self.TEST_ROOT, "new_folder")
        with TempFolder(new_folder_uri) as folder:
            self.assertTrue(folder.is_empty())

            # Create a sub folder inside the new folder
            sub_folder_uri = os.path.join(new_folder_uri, "sub_folder")
            logger.debug(sub_folder_uri)
            sub_folder = StorageFolder(sub_folder_uri).create()
            self.assertTrue(sub_folder.exists())

            # Copy an empty file
            src_file_path = os.path.join(self.TEST_ROOT, "test_folder_0", "empty_file")
            dst_file_path = os.path.join(new_folder_uri, "copied_file")
            f = StorageFile(src_file_path)
            logger.debug(f.exists())
            time.sleep(2)
            f.copy(dst_file_path)
            self.assertTrue(StorageFile(dst_file_path).exists())

            # Copy a file with content and replace the empty file
            src_file_path = os.path.join(self.TEST_ROOT, "test_folder_0", "abc.txt")
            dst_file_path = os.path.join(new_folder_uri, "copied_file")
            f = StorageFile(src_file_path)
            f.copy(dst_file_path)
            dst_file = StorageFile(dst_file_path)
            self.assertTrue(dst_file.exists())
            # Use the shortcut to read file, the content will be binary.
            self.assertEqual(dst_file.read(), b"abc\ncba\n")

            # Empty the folder. This should delete file and sub folder only
            folder.empty()
            self.assertTrue(folder.exists())
            self.assertTrue(folder.is_empty())
            self.assertFalse(sub_folder.exists())
            self.assertFalse(dst_file.exists())


class TestStorageGCP(TestStorage):
    SCHEME = "gs"
    HOST = "aries_test"
    TEST_ROOT_PATH = "/storage_test"
    TEST_ROOT = "%s://%s%s" % (SCHEME, HOST, TEST_ROOT_PATH)
    test_folder = StorageFolder(TEST_ROOT)

    @classmethod
    def setUpClass(cls):
        cls.CREDENTIALS = False
        # Google credentials are required for setting up the class.
        gs.setup_credentials("GOOGLE_CREDENTIALS", os.path.join(os.path.dirname(__file__), "gcp.json"))
        try:
            super().setUpClass()
            cls.CREDENTIALS = True
        except Exception as ex:
            print("%s: %s" % (type(ex), str(ex)))
            traceback.print_exc()

    def setUp(self):
        # Skip test if GCP_ACCESS is not True.
        if not self.CREDENTIALS:
            self.skipTest("GCP Credentials not found.")
        super().setUp()
        time.sleep(0.5)


class TestStorageAWS(TestStorage):
    SCHEME = "s3"
    HOST = "davelab-test"
    TEST_ROOT_PATH = "/storage_test"
    TEST_ROOT = "%s://%s%s" % (SCHEME, HOST, TEST_ROOT_PATH)
    test_folder = StorageFolder(TEST_ROOT)

    @classmethod
    def setUpClass(cls):
        cls.CREDENTIALS = False
        # AWS credentials are loaded from environment variable directly.
        try:
            super().setUpClass()
            cls.CREDENTIALS = True
        except Exception as ex:
            print("%s: %s" % (type(ex), str(ex)))
            traceback.print_exc()
            raise ex

    def setUp(self):
        # Skip test if self.CREDENTIALS is not True.
        if not self.CREDENTIALS:
            self.skipTest("Credentials for %s not found." % self.__class__.__name__)
        super().setUp()
        time.sleep(0.5)
