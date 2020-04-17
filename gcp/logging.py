from google.cloud.logging.handlers import CloudLoggingHandler


class StackdriverHandler(CloudLoggingHandler):
    """Logging handler for Google Cloud Platform Stackdriver Logging.
    This handler is inherited from google.cloud.logging.handlers.CloudLoggingHandler
    Stackdriver has a limit for the log message to be 256KB
    This handler truncates the message if it is over 200KB

    See Also:
    https://github.com/googleapis/google-cloud-python/blob/master/logging/google/cloud/logging/handlers/handlers.py
    
    Args:
        CloudLoggingHandler ([type]): [description]
    """
    def emit(self, record):
        limit = 200 * 1024
        # Convert the log message to string
        record.msg = str(record.msg)
        if len(record.msg) > limit:
            record.msg = record.msg[:limit]
        return super().emit(record)
