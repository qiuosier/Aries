"""Contains tests for the web module.
"""
import datetime
import logging
import os
import sys
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.test import AriesTest
from Aries import web
logger = logging.getLogger(__name__)


class TestWeb(AriesTest):
    def test_web_api_get(self):
        api = web.WebAPI("https://api.weather.gov/")
        res = api.get_json("")
        self.assertEqual(res.get("status"), "OK")

    def test_download(self):
        file_path = os.path.join(os.path.dirname(__file__), "test_download")
        if os.path.exists(file_path):
            os.remove(file_path)
        web.download("https://www.google.com", file_path)
        self.assertTrue(os.path.exists(file_path))
        os.remove(file_path)

    def test_get_html_table(self):
        url = "https://en.wikipedia.org/wiki/List_of_file_signatures"
        tables = web.HTML(url).get_tables()
        self.assertEqual(len(tables), 2, "There should be two tables in the HTML page.")
