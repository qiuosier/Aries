"""Contains tests for the strings module.
"""
import logging
import unittest

import os
import sys
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
os.environ["BASESPACE_CREDENTIALS"] = os.path.join(aries_parent, "Aries/private/basespace.json")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.basespace import basespace, bs_project
logger = logging.getLogger(__name__)


class TestBaseSpace(unittest.TestCase):
    def test_bs_project(self):
        """Tests project API."""
        # Gets a list of projects. For this test, there should be at least one project in BaseSpace
        projects = bs_project.get_list()
        self.assertGreater(len(projects), 0, "No project found in BaseSpace.")
        # Get the details of each project
        for project in projects:
            project_id = project.get("Id")
            details = bs_project.get_details(project_id)
            self.assertGreater(len(str(details)), 0, "Project ID %s has no detail information." % project_id)
        # Get the samples in each project
        for project in projects:
            project_name = project.get("Name")
            samples = bs_project.get_samples(project_name)
            self.assertGreater(len(samples), 0, "No sample found in project %s." % project_name)

