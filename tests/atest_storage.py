"""Contains tests for the storage module.
"""
import datetime
import logging

import os
import sys
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.test import AriesTest
from Aries.storage import LocalFolder, LocalFile

logger = logging.getLogger(__name__)


class TestLocalStorage(AriesTest):

    test_folder_path = os.path.join(os.path.dirname(__file__), "fixtures", "test_folder")

    def setUp(self):
        self.test_folder = LocalFolder(self.test_folder_path)

    def test_get_names(self):
        """Tests getting the folder names
        """
        folder_names = self.test_folder.folder_names
        # There are two sub-folders
        self.assertEqual(len(folder_names), 2)
        self.assertIn("test_subfolder0", folder_names)
        self.assertIn("test_subfolder1", folder_names)
        # "/" should not be found in folder names
        self.assertNotIn("/", str(folder_names))
    
    def test_get_folder_and_file(self):
        # Try to get a folder that does not exist. Should return None
        self.assertIsNone(self.test_folder.get_folder("not_exist"))
        # Get a sub folder
        sub_folder = self.test_folder.get_folder("test_subfolder1")
        self.assertTrue(isinstance(sub_folder, LocalFolder), "Failed to get sub folder.")
        # Get the filename
        filenames = sub_folder.file_names
        self.assertEqual(len(filenames), 1)
        self.assertEqual(filenames[0], ".hidden_file")
        # Get a file
        f = sub_folder.get_file("not_exist")
        self.assertIsNone(f)
        f = sub_folder.get_file(".hidden_file")
        self.assertTrue(isinstance(f, LocalFile), "Failed to get the file.")
        self.assertEqual(f.basename, ".hidden_file")
        
    def test_get_paths(self):
        sub_folders = self.test_folder.folder_paths
        # There are two sub-folders
        self.assertEqual(len(sub_folders), 2)
        self.assertIn(os.path.join(self.test_folder_path, "test_subfolder0"), sub_folders)
        self.assertIn(os.path.join(self.test_folder_path, "test_subfolder1"), sub_folders)

    def test_create_and_delete(self):
        new_folder_path = os.path.join(self.test_folder_path, "new_folder")
        sub_folder_path = os.path.join(new_folder_path, "sub_folder")
        # Folder should not exist
        self.assertFalse(os.path.exists(new_folder_path))
        
        # Create a new folder
        folder = LocalFolder(new_folder_path).create()
        self.assertTrue(os.path.isdir(new_folder_path))
        self.assertTrue(folder.is_empty())
        
        # Create a sub folder inside the new folder
        LocalFolder(sub_folder_path).create()
        self.assertTrue(os.path.isdir(sub_folder_path))

        # Copy a file
        src_file_path = os.path.join(self.test_folder_path, "test_subfolder0", "empty_file")
        dst_file_path = os.path.join(new_folder_path, "copied_file")
        f = LocalFile(src_file_path)
        f.copy(dst_file_path)
        self.assertTrue(os.path.isfile(dst_file_path))
        
        # Empty the folder. This should delete file and sub folder only
        folder.empty()
        self.assertTrue(os.path.exists(new_folder_path))
        self.assertTrue(folder.is_empty())
        self.assertFalse(os.path.exists(sub_folder_path))
        self.assertFalse(os.path.exists(dst_file_path))
        
        # Copy a folder into this folder, so that the folder is no longer empty.
        src_folder_path = os.path.join(self.test_folder_path, "test_subfolder0")
        dst_folder_path = os.path.join(new_folder_path, "test_subfolder0")
        # dst_path ends with "/"
        dst_path = os.path.join(new_folder_path, "")
        LocalFolder(src_folder_path).copy(dst_path)
        self.assertFalse(folder.is_empty())
        self.assertTrue(os.path.exists(dst_folder_path))
        self.assertTrue(os.path.exists(os.path.join(dst_folder_path, "empty_file")))

        # Delete the folder.
        folder.delete()
        self.assertFalse(os.path.exists(new_folder_path))
