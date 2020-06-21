import requests
import logging
import contextlib
from io import BytesIO
from .storage import StorageObject
from lxml import etree
from urllib import request
logger = logging.getLogger(__name__)


class WebAPI:
    """Provides method to access web API.

    This class uses python requests package.
    See https://2.python-requests.org/en/master/user/advanced/#request-and-response-objects

    Attributes:
        base_url: The base URL for all API endpoint.
        If base_url is specified, relative URL can be used to make requests.
        Relative URL will be appended to base URL when making the requests.
    
    """
    def __init__(self, base_url="", **kwargs):
        """Initializes API.

        Args:
            base_url: Base URL, the common URL prefix for all the API endpoints.
            **kwargs: keyword arguments to be encoded as GET parameters in the URL in all future requests.
        """
        self.kwargs = kwargs
        self.headers = {}

        base_url = base_url
        if base_url.startswith("http://") or base_url.startswith("https://"):
            self.base_url = base_url
        else:
            raise ValueError("Base URL should start with http:// or https://")

    def add_header(self, **kwargs):
        """Adds a header to be used in all future HTTP requests

        Args:
            **kwargs: The key-value pairs to be added as the headers.

        """
        self.headers.update(kwargs)

    def request(self, method, url, **kwargs):
        """Sends a request to a URL endpoint.
        data in self.headers will be added to the request header.

        This method uses the same arguments as the python requests package
        https://github.com/psf/requests/blob/master/requests/api.py

        Args:
            method: Request method, e.g. GET, OPTIONS, HEAD, POST, PUT, PATCH, or DELETE
            url: URL endpoint for the request, which can be relative URL.
            **kwargs: See https://github.com/psf/requests/blob/master/requests/api.py

        Returns: Request response

        """
        url = self.build_url(url)
        method = str(method).lower()
        if not hasattr(requests, method):
            raise ValueError("Invalid method: %s" % method)
        request_func = getattr(requests, method)
        headers = kwargs.get("headers", {})
        headers.update(self.headers)
        kwargs["headers"] = headers
        response = request_func(url, **kwargs)
        return response

    def get(self, url, **kwargs):
        """Makes a get request.
        Use keyword arguments to specify the query strings in the request.
        
        Args:
            url (str): The URL/Endpoint of the API.
                This can be a relative URL if base_url is specified in initialization.
            **kwargs: keyword arguments to be encoded as GET parameters in the URL.
        
        Returns: A Response Object
        """
        url = self.build_url(url, **kwargs)
        logger.debug("Requesting data from %s" % url)
        response = requests.get(url, headers=self.headers)
        logger.debug("Response code: %s" % response.status_code)
        if response.status_code != 200:
            logger.debug(response.content)
        return response

    def get_json(self, url, **kwargs):
        return self.get(url, **kwargs).json()

    def post(self, url, data, **kwargs):
        url = self.build_url(url, **kwargs)
        logger.debug("Posting data to %s" % url)
        response = requests.post(url, json=data, headers=self.headers)
        logger.debug("Response code: %s" % response.status_code)
        if response.status_code != 200:
            logger.debug(response.content)
        return response

    def post_json(self, url, data, **kwargs):
        return self.post(url, data, **kwargs).json()

    def delete(self, url, **kwargs):
        url = self.build_url(url, **kwargs)
        logger.debug("Deleting data from %s" % url)
        response = requests.delete(url, headers=self.headers)
        return response

    def build_url(self, url, **kwargs):
        """Builds the URL/Endpoint for a request.
        Keyword arguments are converted to query string in the URL.
        
        Args:
            url (str): The URL/Endpoint of the API.
            If url is relative url, it will be appended to the base_url.
            If url is absolute URL (starts with https:// or http://), the base_url will be ignored.
        
        Returns:
            str: The absolute URL/Endpoint of the API with query string.
        """
        url = url
        if not (url.startswith("http://") or url.startswith("https://")):
            url = "%s%s" % (self.base_url, url)
        query_dict = self.kwargs.copy()
        query_dict.update(kwargs)
        return self.append_query_string(url, **query_dict)
    
    @staticmethod
    def append_query_string(url, **kwargs):
        """Appends query string to a URL

        Query string is specified as keyword arguments.
        
        Args:
            url (str): URL
        
        Returns:
            str: URL with query string.
        """
        for key, val in kwargs.items():
            if "?" not in url:
                url += "?"
            if isinstance(val, list):
                url += "".join(["&%s=%s" % (key, v) for v in val])
            else:
                url += "&%s=%s" % (key, val)
        return url


class HTML:
    def __init__(self, uri):
        self.uri = uri
        self.__etree = None
        self.__content = None

    def read(self):
        obj = StorageObject(self.uri)
        if obj.scheme in ["http", "https"]:
            r = requests.get(self.uri)
            return r.content
        with open(self.uri, 'r') as f:
            return f.read()

    @property
    def content(self):
        if not self.__content:
            self.__content = self.read()
        return self.__content

    @property
    def etree(self):
        if not self.__etree:
            self.__etree = etree.parse(BytesIO(self.content), etree.HTMLParser())
        return self.__etree

    @staticmethod
    def __tags_to_list(parent, tag):
        elements = parent.findall(".//%s" % tag)
        if not elements:
            return None
        results = []
        for element in elements:
            text = element.text if element.text else ""
            results.append(text + ''.join(etree.tostring(e).decode() for e in element))
        return results

    @staticmethod
    def __append_data(to_list, parent, tag):
        data = HTML.__tags_to_list(parent, tag)
        if data:
            to_list.append(data)

    def get_tables(self):
        """Gets the data of of HTML tables in the web page as a list of dictionary.
        
        Returns:
            list: A list of dictionary, each contain data from a table in the web page.
            Each dictionary has two keys: "headers" and "data".
            Both "headers" and "data" are 2D lists.
        """
        html = self.etree
        html_tables = html.findall('.//table')
        data_tables = []
        
        for html_table in html_tables:
            table = {
                "headers": [],
                "data": []
            }
            rows = html_table.findall(".//tr")
            for row in rows:
                self.__append_data(table["headers"], row, "th")
                self.__append_data(table["data"], row, "td")
            data_tables.append(table)
        return data_tables


def download(url, file_path):
    """Downloads a file from a URL response.

    Args:
        url (str): The URL of the file to be downloaded.
        file_path (str): The path to store the file.

    Returns: None

    """
    url_response = request.urlopen(url)
    with open(file_path, 'wb') as out_file:
        with contextlib.closing(url_response) as fp:
            logger.debug("Downloading data from %s" % url)
            block_size = 1 << 16
            while True:
                block = fp.read(block_size)
                if not block:
                    break
                out_file.write(block)
