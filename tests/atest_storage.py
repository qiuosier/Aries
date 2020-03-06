"""Contains tests for the storage module.
"""
import datetime
import logging

import os
import sys
import shutil
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.test import AriesTest
from Aries.storage import StorageFile, StorageFolder

logger = logging.getLogger(__name__)


class TestLocalStorage(AriesTest):

    test_folder_path = os.path.join(os.path.dirname(__file__), "fixtures", "test_folder")
    test_new_folder_path = os.path.join(test_folder_path, "new_folder")

    temp_files = [
        os.path.join(test_folder_path, "temp_file.txt"),
        os.path.join(test_folder_path, "test.txt")
    ]

    def setUp(self):
        self.test_folder = StorageFolder(self.test_folder_path)

    def tearDown(self):
        # Remove temp folder
        if os.path.exists(self.test_new_folder_path):
            shutil.rmtree(TestLocalStorage.test_new_folder_path)
        # Remove temp files
        for tmp in self.temp_files:
            if os.path.exists(tmp):
                os.remove(tmp)

    def test_get_names(self):
        """Tests getting the folder names
        """
        folder_names = self.test_folder.folder_names
        # There are two sub-folders
        self.assertEqual(len(folder_names), 2, 'Folders: %s' % folder_names)
        self.assertIn("test_subfolder0", folder_names)
        self.assertIn("test_subfolder1", folder_names)
        # "/" should not be found in folder names
        self.assertNotIn("/", str(folder_names))
    
    def test_get_folder_and_file(self):
        # Try to get a folder that does not exist. Should return None
        self.assertIsNone(self.test_folder.get_folder("not_exist"))
        # Get a sub folder
        sub_folder = self.test_folder.get_folder("test_subfolder1")
        self.assertTrue(
            isinstance(sub_folder, StorageFolder),
            "Failed to get sub folder. Received %s: %s" % (sub_folder.__class__, sub_folder))
        # Get the filename
        filenames = sub_folder.file_names
        self.assertEqual(len(filenames), 1)
        self.assertEqual(filenames[0], ".hidden_file")
        # Get a file
        f = sub_folder.get_file("not_exist")
        self.assertIsNone(f)
        f = sub_folder.get_file(".hidden_file")
        self.assertTrue(f.exists(), "Failed to get the file.")
        self.assertEqual(f.basename, ".hidden_file")
        
    def test_get_paths(self):
        sub_folders = self.test_folder.folder_paths
        # There are two sub-folders
        self.assertEqual(len(sub_folders), 2)
        self.assertIn(os.path.join(self.test_folder_path, "test_subfolder0"), sub_folders)
        self.assertIn(os.path.join(self.test_folder_path, "test_subfolder1"), sub_folders)

    def test_create_copy_and_delete(self):
        sub_folder_path = os.path.join(self.test_new_folder_path, "sub_folder")
        # Folder should not exist
        self.assertFalse(os.path.exists(self.test_new_folder_path))
        
        # Create a new empty folder
        folder = StorageFolder(self.test_new_folder_path).create()
        self.assertTrue(os.path.isdir(self.test_new_folder_path))
        self.assertTrue(folder.is_empty())
        
        # Create a sub folder inside the new folder
        StorageFolder(sub_folder_path).create()
        self.assertTrue(os.path.isdir(sub_folder_path))

        # Copy an empty file
        src_file_path = os.path.join(self.test_folder_path, "test_subfolder0", "empty_file")
        dst_file_path = os.path.join(self.test_new_folder_path, "copied_file")
        f = StorageFile(src_file_path)
        f.copy(dst_file_path)
        self.assertTrue(os.path.isfile(dst_file_path))
        # Copy a file with content and replace the empty file
        src_file_path = os.path.join(self.test_folder_path, "test_subfolder0", "abc.txt")
        dst_file_path = os.path.join(self.test_new_folder_path, "copied_file")
        f = StorageFile(src_file_path)
        f.copy(dst_file_path)
        self.assertTrue(os.path.isfile(dst_file_path))
        # Use the shortcut to read file, the content will be binary.
        self.assertEqual(StorageFile(dst_file_path).read(), b"abc\ncba\n")
        
        # Empty the folder. This should delete file and sub folder only
        folder.empty()
        self.assertTrue(os.path.exists(self.test_new_folder_path))
        self.assertTrue(folder.is_empty())
        self.assertFalse(os.path.exists(sub_folder_path))
        self.assertFalse(os.path.exists(dst_file_path))

        # Delete the folder.
        folder.delete()
        self.assertFalse(os.path.exists(self.test_new_folder_path))

    def test_copy_folder(self):
        # Source folder to be copied
        src_folder_path = os.path.join(self.test_folder_path, "test_subfolder0")
        sub_folder = StorageFolder(src_folder_path)
        # Source folder should exist
        self.assertTrue(sub_folder.exists())

        # Destination folder
        dst_folder_path = os.path.join(self.test_new_folder_path, "test_subfolder0")
        dst_parent = self.test_new_folder_path

        # Destination folder should not exist
        if os.path.exists(dst_folder_path):
            shutil.rmtree(dst_folder_path)

        if not dst_parent.endswith("/"):
            dst_parent += "/"
        logger.debug("Copying from %s into %s" % (sub_folder.uri, dst_parent))
        sub_folder.copy(dst_parent)
        # Destination folder should now exist and contain an empty file
        self.assertTrue(os.path.exists(dst_folder_path))
        self.assertTrue(os.path.exists(os.path.join(dst_folder_path, "empty_file")))
        self.assertTrue(os.path.exists(os.path.join(dst_folder_path, "abc.txt")))

        # Delete destination file
        StorageFolder(dst_parent).delete()
        self.assertFalse(os.path.exists(dst_parent))

    def test_storage_object(self):
        self.assertEqual(self.test_folder.scheme, "file")
        self.assertEqual(str(self.test_folder).rstrip("/"), self.test_folder_path.rstrip("/"))
        self.assertEqual(self.test_folder.basename, os.path.basename(self.test_folder_path))
        self.assertEqual(self.test_folder.name, os.path.basename(self.test_folder_path))

    def test_storage_folder(self):
        """Tests methods and properties of StorageFolder.
        """
        self.assertGreater(len(self.test_folder.get_folder_attributes()), 1)
        self.assertIn("file_in_test_folder", self.test_folder.get_file_attributes("name"))

    def test_local_binary_read_write(self):
        # File does not exist, a new one will be created
        file_path = os.path.join(self.test_folder_path, "test.txt")
        local_file = StorageFile("file://%s" % file_path).open("wb")
        self.assertEqual(local_file.scheme, "file")
        # self.assertEqual(str(type(local_file).__name__), "LocalFile")
        self.assertTrue(local_file.seekable())
        self.assertFalse(local_file.readable())
        self.assertEqual(local_file.write(b"abc"), 3)
        self.assertEqual(local_file.tell(), 3)
        self.assertEqual(local_file.write(b"def"), 3)
        self.assertEqual(local_file.tell(), 6)
        local_file.close()
        local_file.open('rb')
        self.assertEqual(local_file.read(), b"abcdef")
        local_file.close()
        local_file.delete()
        self.assertFalse(os.path.exists(file_path))

    def test_local_text_read(self):
        with StorageFile.init(os.path.join(self.test_folder_path, "file_in_test_folder")) as f:
            self.assertEqual(f.size, 0)
            self.assertEqual(f.tell(), 0)
            self.assertEqual(f.seek(0, 2), 0)
            self.assertEqual(len(f.read()), 0)

    def test_local_text_write(self):
        # Write a new file
        temp_file_path = os.path.join(self.test_folder_path, "temp_file.txt")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        with StorageFile.init(temp_file_path, 'w+') as f:
            self.assertTrue(f.writable())
            self.assertEqual(f.tell(), 0)
            print(f.buffered_io.buffer)
            self.assertEqual(f.write("abc"), 3)
            print(f.buffered_io.buffer.tell())
            self.assertEqual(f.tell(), 3)
            f.seek(0)
            self.assertEqual(f.read(), "abc")
            self.assertTrue(f.exists())
        f.delete()
