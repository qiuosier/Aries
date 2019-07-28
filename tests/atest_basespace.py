"""Contains tests for the basespace module.
"""
import logging
import os
import sys
import base64
import json
import random

aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.test import AriesTest
from Aries.basespace import basespace, bs_project, bs_app_session, bs_run, bs_sample
from Aries.outputs import PackageLogFilter
logger = logging.getLogger(__name__)


def setUpModule():
    credentials = os.environ.get("BASESPACE_CREDENTIALS")
    json_file = os.path.join(aries_parent, "Aries/private/basespace.json")
    if not credentials and os.path.exists(json_file):
        os.environ["BASESPACE_CREDENTIALS"] = json_file
    random.seed()


class TestBaseSpace(AriesTest):
    logger_names = PackageLogFilter.get_packages(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )

    def setUp(self):
        # Skip test if "BASESPACE_CREDENTIALS" is not found.
        if not os.environ.get("BASESPACE_CREDENTIALS"):
            self.skipTest("BaseSpace Credentials not found.")

    def assert_bs_interface(self, bs_module, name):
        """Tests the BaseSpace interface."""
        # Gets a list of items.
        items = bs_module.get_list()
        # The list should not be None if the request is successful
        self.assertIsNotNone(items, "Failed to get a list of %s." % name)

        # Get the details of a random item
        item = random.choice(items)
        item_id = item.get("Id")
        details = bs_module.get_details(item_id)
        self.assertGreater(
            len(str(details)), 
            0, 
            "%s ID %s has no detail information." % (name, item_id)
        )

        # Get the samples of a random item
        item = random.choice(items)
        if name == "Project":
            item_id = item.get("Name")
        else:
            item_id = item.get("Id")
        samples = bs_module.get_samples(item_id)
        # self.assertGreater(len(samples), 0, "No sample found in %s %s." % (name, item_id))

    def test_bs_project(self):
        self.assert_bs_interface(bs_project, "Project")

    def test_bs_session(self):
        self.assert_bs_interface(bs_app_session, "Session")

    def test_bs_run(self):
        self.assert_bs_interface(bs_run, "Run")

    def test_bs_sample(self):
        sessions = bs_app_session.get_list("FASTQ*")
        self.assertIsNotNone(sessions)
        session = random.choice(sessions)
        samples = bs_app_session.get_samples(session.get("Id"))
        self.assertIsNotNone(samples)
        sample = random.choice(samples)
        r1, r2 = bs_sample.get_fastq_pair(sample.get("Id"))
