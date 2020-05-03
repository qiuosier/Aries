import requests
import re
import logging
from .web import WebAPI
logger = logging.getLogger(__name__)


class DockerAPI(WebAPI):
    def __init__(self, hostname):
        super().__init__("https://%s" % hostname)

    def check_version(self, endpoint="/v2/"):
        """Checks the API version by sending GET request to /v2/ endpoint.

        Returns: The API version number. e.g. 2.0

        """
        response = self.get(endpoint)
        version = response.headers.get("Docker-Distribution-Api-Version", "").split("/")[-1]
        return version

    @staticmethod
    def get_auth_url(authenticate_header):
        """Gets the authentication URL from the www-authenticate header value.

        Args:
            authenticate_header:

        Returns:

        """
        authenticate_header = str(authenticate_header)
        # Only Bearer token authentication is supported.
        if not authenticate_header.startswith("Bearer"):
            raise NotImplementedError("Authentication method not supported: %s" % authenticate_header)
        value = authenticate_header[len("Bearer"):].strip()
        # Convert the values to a dictionary
        auth_dict = dict()
        for v in value.split(","):
            if "=" not in v:
                continue
            v = v.strip()
            arr = v.split('=', 1)
            key = arr[0]
            val = arr[1].strip('"')
            values = auth_dict.get(key, [])
            values.append(val)
            auth_dict[key] = values
        # logger.debug(auth_dict)
        realm = auth_dict.get("realm")
        if not realm:
            raise ValueError("realm not found in www-authenticate header.")
        endpoint = realm[0]
        params = dict()
        for key in ["scope", "service"]:
            if key in auth_dict:
                params[key] = auth_dict[key]
        return WebAPI.append_query_string(endpoint, **params)

    def request(self, method, url, **kwargs):
        """
        This method uses the same arguments as the python requests package
        https://github.com/psf/requests/blob/master/requests/api.py

        Args:
            method: Request method, e.g. GET, OPTIONS, HEAD, POST, PUT, PATCH, or DELETE
            url: URL endpoint for the request, which can be relative URL.
            **kwargs: See https://github.com/psf/requests/blob/master/requests/api.py

        Returns: Request response

        """
        response = super().request(method, url, **kwargs)
        # Try to get authentication token automatically for public repository
        # TODO: Authentication for private repository
        if response.status_code == 401:
            auth_url = self.get_auth_url(response.headers.get("www-authenticate"))
            logger.debug("Requesting token at: %s" % auth_url)
            auth_res = requests.get(auth_url).json()
            token = auth_res.get("token")
            # Adds the token to headers so that it will be used in the subsequent requests
            self.add_header(Authorization="Bearer %s" % token)
            response = super().request(method, url, **kwargs)
        return response


class DockerImage:
    """
    Attributes:
        name: The docker image name, which may contain the hostname if it is not on DockerHub,
            and optionally the tag or digest.
            This is the same as the NAME[:TAG|@DIGEST] to be used in the docker pull command.
            e.g. ubuntu, tensorflow/tensorflow:2.2.0rc4-jupyter, mcr.microsoft.com/windows
            See Also: https://docs.docker.com/engine/reference/commandline/pull/
        hostname: The hostname of the Docker registry.
        path: The path of the docker image under the registry, without tag or digest.
        tag: The docker image tag, if included in the name.

    See Also:
        https://docs.docker.com/registry/spec/api/
        https://github.com/docker/distribution/blob/master/reference/reference.go
        https://docs.docker.com/engine/reference/commandline/pull/
        https://ops.tips/blog/inspecting-docker-image-without-pull/
    """

    DOCKER_IO_REGISTRY = "registry-1.docker.io"

    def __init__(self, name):
        self.name = name
        self.hostname, self.path, self.tag, self._digest = self.parse_name(self.name)
        self.api = DockerAPI(self.hostname)
        self._manifest = None

    @staticmethod
    def parse_name(name):
        """Parses docker image name, which can have the format as accepted by the docker pull command.

        Args:
            name (str): Docker image name, including tag or digest, i.e. NAME[:TAG|@DIGEST]

        Returns:
            A 4-tuple of hostname, path, tag and digest.
            hostname defaults to DOCKER_IO_REGISTRY
        """
        hostname, path = DockerImage.match_host(name)
        if not hostname:
            hostname = DockerImage.DOCKER_IO_REGISTRY

        # Try to match the digest followed by the image name.
        path, digest = DockerImage.match_digest(path)

        # There should be no tag if digest is followed by the image name.
        if digest:
            tag = None
        else:
            path, tag = DockerImage.match_tag(path)

        # Official images are identified with library/IMAGE_NAME
        if "/" not in path and hostname == DockerImage.DOCKER_IO_REGISTRY:
            path = "library/" + path

        return hostname, path, tag, digest

    @staticmethod
    def match_host(name):
        """Parses the docker image name, extracts the hostname and path (which may also include the tag/digest).
        """
        name_pattern = r"(?:((?:localhost)|(?:[^/]*\.[^/]*)|(?:[^/]*\:[^/]*))/)?(.+)"
        match = re.match(name_pattern, name)
        if not match:
            raise ValueError("Invalid image name: %s" % name)
        groups = match.groups()
        hostname = groups[0]
        path_with_tag = groups[1]
        return hostname, path_with_tag

    @staticmethod
    def match_tag(name):
        tag_pattern = r"(.+)\:([\w][\w.-]{0,127})"
        match = re.match(tag_pattern, name)
        if match:
            path = match.groups()[0]
            tag = match.groups()[1]
        else:
            path = name
            tag = "latest"
        return path, tag

    @staticmethod
    def match_digest(name):
        digest_pattern = r"(.+)\@([^/]*)"
        match = re.match(digest_pattern, name)
        if match:
            path = match.groups()[0]
            digest = match.groups()[1]
        else:
            path = name
            digest = None
        return path, digest

    @property
    def digest(self):
        """The digest of the docker image
        """
        if not self._digest:
            self._digest = self.get_manifest().headers.get("Docker-Content-Digest").strip()
        return self._digest

    def get_manifest(self):
        """Gets the manifest HTTP response.
        """
        if not self._manifest:
            reference = "latest"
            if self.tag:
                reference = self.tag
            elif self.digest:
                reference = self.digest
            url = "/v2/%s/manifests/%s" % (self.path, reference)
            response = self.api.request(
                "GET",
                url,
                headers={"Accept": "application/vnd.docker.distribution.manifest.v2+json"}
            )
            if response.status_code != 200:
                raise ValueError(
                    "Request to %s failed with status code %s" % (url, response.status_code)
                )
            self._manifest = response
        return self._manifest

    def inspect(self):
        config_digest = self.get_manifest().json().get("config", {}).get("digest")
        url = "/v2/%s/blobs/%s" % (self.path, config_digest)
        return self.api.request("GET", url).json()

    def is_accessible(self):
        """Checks if the image is accessible.

        Returns: True if the image is accessible.
            This method returns False if the existence cannot be determined.

        """
        try:
            self.get_manifest()
            return True
        except ValueError:
            return False

    def get_size(self):
        """Gets the size of the docker image by adding the sizes of all layers.

        Returns: The size of the docker image in bytes.
            This method returns None if the size cannot be determined.
        """
        try:
            # layers is defined in Image Manifest Version 2, Schema 2
            # See also: https://docs.docker.com/registry/spec/manifest-v2-2/
            layers = self.get_manifest().json().get("layers")
            if not layers:
                return self.get_size_via_fs_layers()
            total_size = 0
            for layer in layers:
                total_size += layer.get("size", 0)
        except Exception as ex:
            logger.error("Cannot determine image size for %s: %s" % (self.name, str(ex)))
            import traceback
            traceback.print_exc()
            return None
        return total_size

    def get_size_via_fs_layers(self):
        """Gets the size of the docker image by adding the sizes of all fsLayers
        This is designed for Image Manifest Version 2, Schema 1

        Returns: The size of the docker image in bytes.
            This method returns None if the size cannot be determined.

        See Also: https://docs.docker.com/registry/spec/manifest-v2-1/

        """
        layers = self.get_manifest().json().get("fsLayers")
        if not layers:
            logger.error("Cannot determine image size for %s" % self.name)
            return None
        size = 0
        for layer in layers:
            digest = layer.get("blobSum")
            if not digest:
                continue
            res = self.api.request("HEAD", "/v2/%s/blobs/%s" % (self.path, digest))
            layer_size = res.headers.get("Content-Length")
            if layer_size and str(layer_size).isdigit():
                size += int(layer_size)
        return size
