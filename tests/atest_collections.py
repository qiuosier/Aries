"""Contains tests for the collections module.
"""
import datetime
import logging
import os
import sys
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.test import AriesTest
from Aries import collections
logger = logging.getLogger(__name__)


class TestDictList(AriesTest):
    """Contains tests for the DictList class
    """
    dict_list = [
        {
            "name": "A",
            "order": 1,
        },
        {
            "name": "B",
            "order": 2,
        },
        {
            "name": "X",
            "order": 0,
        },
    ]

    def test_sort_dict_list(self):
        """Tests sorting a list of dictionaries.
        """
        dl = collections.DictList(self.dict_list)
        dl = dl.sort_by_value("order")
        self.assertEqual(dl[0], {"name": "X", "order": 0})
        self.assertEqual(dl[2], {"name": "B", "order": 2})

    def test_get_unique_keys(self):
        """Test getting unique keys from a list of dictionaries.
        """
        dl = collections.DictList(self.dict_list)
        keys = dl.unique_keys()
        self.assertIn("order", keys)
        self.assertIn("name", keys)
        self.assertEqual(len(keys), 2)


class TestLists(AriesTest):
    def test_sort_lists(self):
        """Tests sorting two lists
        """
        order_list = [2, 1, 3]
        label_list = ['A', 'B', 'C']
        order_list, label_list = collections.sort_lists(order_list, label_list)
        self.assertEqual(order_list, [1, 2, 3])
        self.assertEqual(label_list, ['B', 'A', 'C'])
