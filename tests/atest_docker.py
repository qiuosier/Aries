"""Contains tests for the web module.
"""
import datetime
import logging
import os
import sys
logger = logging.getLogger(__name__)
try:
    from ..test import AriesTest
    from ..docker import DockerImage, DockerAPI
except:
    aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
    if aries_parent not in sys.path:
        sys.path.append(aries_parent)
    from Aries.test import AriesTest
    from Aries.docker import DockerImage, DockerAPI


class TestDocker(AriesTest):
    def test_docker_api(self):
        registries = [
            "registry-1.docker.io",
            "gcr.io",
            "quay.io"
        ]
        for registry in registries:
            api = DockerAPI(registry)
            self.assertEqual("2.0", api.check_v2_support())

    def test_docker_image(self):
        image = DockerImage("ubuntu")
        self.assertEqual(image.hostname, "registry-1.docker.io")
        self.assertEqual(image.path, "library/ubuntu")
        self.assertEqual(image.tag, "latest")

        image = DockerImage("ubuntu:18.04")
        self.assertEqual(image.hostname, "registry-1.docker.io")
        self.assertEqual(image.path, "library/ubuntu")
        self.assertEqual(image.tag, "18.04")

        image = DockerImage("us.gcr.io/davelab-gcloud/cancer:0.1-alpha")
        self.assertEqual(image.hostname, "us.gcr.io")
        self.assertEqual(image.path, "davelab-gcloud/cancer")
        self.assertEqual(image.tag, "0.1-alpha")
        self.assertEqual(image.get_size(), 460399799)
