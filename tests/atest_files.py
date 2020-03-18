"""Contains tests for the file module.
"""
import logging
import os
import sys
try:
    from ..test import AriesTest
    from ..files import File, Markdown
except:
    aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
    if aries_parent not in sys.path:
        sys.path.append(aries_parent)
    from Aries.test import AriesTest
    from Aries.files import File, Markdown

logger = logging.getLogger(__name__)


class TestFile(AriesTest):
    def test_load_json(self):
        """Tests loading a json file
        """
        # File exists
        json_file = os.path.join(os.path.dirname(__file__), "fixtures", "test.json")
        json_dict = File.load_json(json_file)
        self.assertIn("key", json_dict)
        self.assertIn("list", json_dict)
        self.assertIn("dict", json_dict)
        # File does not exist, no default
        json_dict = File.load_json(os.path.join(os.path.dirname(__file__), "no_exist.json"))
        self.assertEqual(json_dict, {})
        # File does not exist, with default
        json_dict = File.load_json(
            os.path.join(os.path.dirname(__file__), "no_exist.json"),
            {"default_key": "default_value"}
        )
        self.assertEqual(json_dict["default_key"], "default_value")

    def test_file_signature(self):
        file_path = os.path.join(os.path.dirname(__file__), "fixtures", "excel_test_file.xlsx")
        f = File(file_path)
        self.assertIn("application/vnd.openxmlformats-officedocument", f.file_type())


class TestMarkdown(AriesTest):

    test_file = os.path.join(os.path.dirname(__file__), "fixtures", "test.md")

    def test_markdown_title(self):
        """Tests getting the title of a markdown file.
        """
        md = Markdown(self.test_file)
        self.assertEqual(md.title, "This is title")

    def test_markdown_links(self):
        expected_links = [
            '[an example](http://example.com/ "Title")',
            '[This link](http://example.net/)',
            '<a href="http://example.com/" title="Title">\nan example</a>',
            '<a href="http://example.net/">This link</a>',
            '[About](/about/)',
            '[id]: http://example.com/  "Optional Title Here"',
            '[foo]: http://example.com/  "Optional Title Here"',
            "[foo]: http://example.com/  'Optional Title Here'",
            '[foo]: http://example.com/  (Optional Title Here)',
            '[id]: <http://example.com/>  "Optional Title Here"',
            '[id]: http://example.com/longish/path/to/resource/here\n    "Optional Title Here"',
            '[Google]: http://google.com/',
            '[Daring Fireball]: http://daringfireball.net/',
            '[1]: http://google.com/        "Google"',
            '[2]: http://search.yahoo.com/  "Yahoo Search"',
            '[3]: http://search.msn.com/    "MSN Search"',
            '[google]: http://google.com/        "Google"',
            '[yahoo]:  http://search.yahoo.com/  "Yahoo Search"',
            '[msn]:    http://search.msn.com/    "MSN Search"',
            '<a href="http://google.com/"\ntitle="Google">Google</a>',
            '<a href="http://search.yahoo.com/" title="Yahoo Search">Yahoo</a>',
            '<a href="http://search.msn.com/" title="MSN Search">MSN</a>',
            '[Google](http://google.com/ "Google")',
            '[Yahoo](http://search.yahoo.com/ "Yahoo Search")',
            '[MSN](http://search.msn.com/ "MSN Search")'

        ]
        md = Markdown(os.path.join(os.path.dirname(__file__), "fixtures", "links.md"))
        matches = md.find_links()
        for i in range(len(expected_links)):
            self.assertEqual(expected_links[i], matches[i][0])
            # logger.debug("%s: %s" % (matches[i][0], matches[i][1]))
