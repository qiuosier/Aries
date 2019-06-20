"""Contains classes for handling outputs.
"""
import io
import logging
import sys
import threading
import traceback
import uuid


class StreamHandler(logging.StreamHandler):
    """Stream Handler with customized formats to output module name and line number.

    """
    log_format = '%(asctime)s | %(levelname)-8s | %(lineno)4d@%(module)-15s | %(message)s'
    time_format = '%Y-%m-%d %H:%M:%S'

    def __init__(self, stream=sys.stdout):
        """Initialize the handler to send logging to standard output.

        Args:
            stream: The stream to which the outputs are sent.

        """
        super().__init__(stream)
        self.setFormatter(
            logging.Formatter(self.log_format, self.time_format)
        )

    @staticmethod
    def enable_logging(logger_name="", level=logging.DEBUG):
        """Sets logging level to debug and add a stream_handler to format the logging outputs.

        This method is intended to be used in a script or a test.
        For example, StreamHandler.enable_logging() will add stream handler to root logger.

        Args:
            logger_name (str): logger name. Default to root logger.
            level: logging level.

        Returns: the logger.

        """
        handler = StreamHandler()
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.addHandler(handler)
        logger.debug("Debug Logging Enabled.")
        return logger


class ThreadLogHandler(logging.NullHandler):
    """Captures the logs of a particular thread.

    Attributes:
        thread_id: The ID of the thread of which the logs are being captured.
        logs (list): A list of formatted log messages.

    Examples:
        log_handler = ThreadLogHandler(threading.current_thread().ident)
        logger = logging.getLogger(__name__)
        logger.addHandler(log_handler)

    See Also:
        https://docs.python.org/3.5/library/logging.html#handler-objects
        https://docs.python.org/3.5/library/logging.html#logrecord-attributes

    """
    # This log_formatter is used to format the log messages.
    log_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(lineno)4d@%(module)-12s | %(message)s',
        '%Y-%m-%d %H:%M:%S'
    )

    def __init__(self, thread_id):
        """Initialize the log handler for a particular thread.

        Args:
            thread_id: The ID of the thread.
        """
        super(ThreadLogHandler, self).__init__()
        self.setFormatter(self.log_formatter)
        self.thread_id = thread_id
        self.logs = []

    def handle(self, record):
        """Determine whether to emit base on the thread ID.
        """
        if record.thread == self.thread_id:
            self.emit(record)

    def emit(self, record):
        """Formats and saves the log message.
        """
        message = self.format(record)
        self.logs.append(message)


class OutputWriter(io.StringIO):
    """Represents a writer for a list of file-like objects (listeners).

    An instance of this class must be initialized with a list of file-like objects (listeners).
    Any output sending to the instance will result in writing into all listeners.

    Although this class accepts any file-like object as listener, only StringIO() is tested.
    This class does not handle the opening and closing of a file.

    """
    def __init__(self, listeners):
        """Initialize a writer with a list of file-like objects (listeners).

        Args:
            listeners (list): A list of file-like objects.
        """
        self.listeners = listeners
        super(OutputWriter, self).__init__()

    def write(self, *args, **kwargs):
        """Writes the output to the listeners.
        """
        for listener in self.listeners:
            listener.write(*args, **kwargs)


class CaptureOutput:
    """Represents an object capturing the standard outputs and standard errors.

    In Python 3.5 and up, redirecting stdout and stderr can be done by using:
        from contextlib import redirect_stdout, redirect_stderr

    Attributes:
        std_out (str): Captured standard outputs.
        std_err (str): Captured standard errors.
        log_out (str): Captured log messages.
        exc_out (str): Captured exception outputs.
        returns: This is not used directly. It can be used to store the return value of a function/method.
        log_handler (ThreadLogHandler): The log handler object for capturing the log messages.

    Class Attributes:
        sys_out: The system stdout before using any instance of this class.
            When the last instance of this class exits, the sys.stdout will be reset to CaptureOutput.sys_out.
        sys_err: The system stderr before using any instance of this class.
            When the last instance of this class exits, the sys.stderr will be reset to CaptureOutput.sys_err.
        out_listeners: A dictionary of file-like objects expecting outputs from stdout.
        err_listeners: A dictionary of file-like objects expecting outputs from stderr.

    Examples:
        with CaptureOutput() as out:
            do_something()

        standard_output = out.std_out
        standard_error = out.std_err
        log_messages = out.log_out

    Multi-Threading:
        When using this class, stdout/stderr from all threads in the same process will be captured.
        To capture the stdout/stderr of a particular thread, run the thread in an independent process.
        Only the logs of the current thread will be captured.

    Warnings:
        Using this class will set the level of root logger to DEBUG.
        sys.stdout and sys.stderr should not be modified when using this class.

    """

    sys_out = None
    sys_err = None

    out_listeners = {}
    err_listeners = {}

    def __init__(self, suppress_exception=False):
        """Initializes log handler and attributes to store the outputs.
        """
        self.uuid = uuid.uuid4()
        self.suppress_exception = suppress_exception

        self.log_handler = ThreadLogHandler(threading.current_thread().ident)
        self.log_handler.setLevel(logging.DEBUG)

        self.std_out = ""
        self.std_err = ""

        self.log_out = ""
        self.exc_out = ""
        self.returns = None

    @staticmethod
    def config_sys_outputs():
        """Configures sys.stdout and sys.stderr.
            If there are listeners, sys.stdout/sys.stderr will be redirect to an instance of OutputWriter,
            which will write the outputs to all listeners.
            If there is no listener, sys.stdout/sys.stderr will be restored to the values saved before.

        """
        if CaptureOutput.out_listeners:
            out_listener_list = [l for l in CaptureOutput.out_listeners.values()]
            out_listener_list.append(CaptureOutput.sys_out)
            sys.stdout = OutputWriter(out_listener_list)
        else:
            sys.stdout = CaptureOutput.sys_out
            CaptureOutput.sys_out = None

        if CaptureOutput.err_listeners:
            err_listener_list = [l for l in CaptureOutput.err_listeners.values()]
            err_listener_list.append(CaptureOutput.sys_err)
            sys.stderr = OutputWriter(err_listener_list)
        else:
            sys.stderr = CaptureOutput.sys_err
            CaptureOutput.sys_err = None

    def __enter__(self):
        """Configures sys.stdout and sys.stderr, and attaches the log handler to root logger.

        Returns: A CaptureOutput object (self).

        """
        # Save the sys.stdout and sys.stderr before the first instance of this class start capturing outputs.
        if CaptureOutput.sys_out is None:
            CaptureOutput.sys_out = sys.stdout
        if CaptureOutput.sys_err is None:
            CaptureOutput.sys_err = sys.stderr

        # Update listeners and re-config output writer.
        CaptureOutput.out_listeners[self.uuid] = io.StringIO()
        CaptureOutput.err_listeners[self.uuid] = io.StringIO()
        self.config_sys_outputs()

        # Modify root logger level and add log handler.
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(self.log_handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Saves the outputs, configures sys.stdout and sys.stderr, and removes log handler.

        Returns: True if self.suppress_exception is True, otherwise False.

        """
        # Capture exceptions, if any
        if exc_type:
            self.exc_out = traceback.format_exc()

        # Removes log handler
        root_logger = logging.getLogger()
        root_logger.removeHandler(self.log_handler)
        self.log_out = "\n".join(self.log_handler.logs)

        # Update listeners and re-config output writer.
        self.std_out = CaptureOutput.out_listeners.pop(self.uuid).getvalue()
        self.std_err = CaptureOutput.err_listeners.pop(self.uuid).getvalue()
        self.config_sys_outputs()

        # Exception will be suppressed if returning True
        if self.suppress_exception:
            return True
        return False
