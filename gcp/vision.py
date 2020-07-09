"""Interacting with Google Cloud Vision API
"""
import os
import logging
from google.cloud import vision
from ..storage import StoragePrefix, StorageFile
from . import utils
logger = logging.getLogger(__name__)


class PDFAnalyzer:
    # How many pages should be grouped into each json output file.
    BATCH_SIZE = 100

    # Supported mime_types are: 'application/pdf' and 'image/tiff'
    MIME_TYPE = 'application/pdf'

    _credentials = None

    @classmethod
    def get_credentials(cls):
        if not cls._credentials:
            file_path = os.environ.get("GOOGLE_VISION_API_CREDENTIALS")
            if not file_path:
                raise AttributeError(
                    "GOOGLE_VISION_API_CREDENTIALS environment variable must be configured for Vision API."
                )
            if not os.path.exists(file_path):
                raise FileNotFoundError("File not found: %s" % file_path)
            logger.debug("Loading credentials from %s ..." % file_path)
            cls._credentials = utils.load_credentials(file_path)
        return cls._credentials

    def __init__(self, input_uri, output_uri=None):
        self.input_uri = input_uri
        if output_uri:
            self.output_uri = output_uri
        else:
            self.output_uri = input_uri + ".json"
        self.operation = None

    def detect_text(self):
        client = vision.ImageAnnotatorClient(credentials=self.get_credentials())

        feature = vision.types.Feature(
            type=vision.enums.Feature.Type.DOCUMENT_TEXT_DETECTION
        )

        gcs_source = vision.types.GcsSource(uri=self.input_uri)
        input_config = vision.types.InputConfig(
            gcs_source=gcs_source, mime_type=self.MIME_TYPE
        )

        gcs_destination = vision.types.GcsDestination(uri=self.output_uri)
        output_config = vision.types.OutputConfig(
            gcs_destination=gcs_destination, batch_size=self.BATCH_SIZE)

        async_request = vision.types.AsyncAnnotateFileRequest(
            features=[feature], input_config=input_config,
            output_config=output_config)

        self.operation = client.async_batch_annotate_files(
            requests=[async_request])
        return self

    def wait(self):
        self.operation.result(timeout=420)
        return self

    def get_results(self):
        results = None
        for f in StoragePrefix(self.output_uri).files:
            result = StorageFile.load_json(f.uri, encoding='utf-8')
            if results is None:
                results = result
            else:
                responses = results.get("responses", [])
                responses.extend(result.get("responses", []))
                results["responses"] = responses
        return results
