import requests

class WebAPI:

    def __init__(self, base_url="", **kwargs):
        self.kwargs = kwargs

        base_url = base_url.lower()
        if base_url.startswith("http://") or base_url.startswith("https://"):
            self.base_url = base_url
        else:
            raise ValueError("Base URL should start with http:// or https://")

    def get(self, url, **kwargs):
        response = requests.get(url)
        return response

    def build_url(self, url, **kwargs):
        url = url.lower()
        if not (url.startswith("http://") or url.startswith("https://")):
            url = "%s%s" % (self.base_url, url)
        url = self.append_query_string(url, **self.kwargs)
        return self.append_query_string(url, **kwargs)
    
    @staticmethod
    def append_query_string(url, **kwargs):
        if kwargs and "?" not in url:
            url += "?"
        for key, val in kwargs.items():
            if "?" not in url:
                url += "?"
            url += "&%s=%s" % (key, val)
        return url
