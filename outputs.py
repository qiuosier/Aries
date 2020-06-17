"""provides classes and methods for logging and capturing outputs from the code, 
like a function or a method. 
"""
import io
import json
import logging
import pprint
import sys
import threading
import traceback
import uuid
import copy
from multiprocessing import Manager
from .strings import stringify
logger = logging.getLogger(__name__)


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

    # Must use lock when iterating or adding items to listeners
    listener_lock = threading.Lock()
    __out_listeners = dict()
    __err_listeners = dict()

    def __init__(self, suppress_exception=False, log_level=logging.DEBUG, filters=None):
        """Initializes log handler and attributes to store the outputs.
        """
        self.uuid = uuid.uuid4()
        self.suppress_exception = suppress_exception

        self.log_handler = ThreadLogHandler(threading.current_thread().ident)
        self.log_handler.setLevel(log_level)
        if filters:
            if not isinstance(filters, list):
                filters = [filters]
            for log_filter in filters:
                self.log_handler.addFilter(log_filter)

        self.std_out = ""
        self.std_err = ""

        self.log_out = ""
        self.logs = []
        self.exc_out = ""
        self.returns = None

    @staticmethod
    def get_listeners(uid):
        with CaptureOutput.listener_lock:
            if uid in CaptureOutput.__out_listeners.keys():
                out_listener = CaptureOutput.__out_listeners[uid]
                err_listener = CaptureOutput.__out_listeners[uid]
                return out_listener, err_listener
            return None, None

    @staticmethod
    def __config_sys_outputs():
        """Configures sys.stdout and sys.stderr.
            If there are listeners, sys.stdout/sys.stderr will be redirect to an instance of OutputWriter,
            which will write the outputs to all listeners.
            If there is no listener, sys.stdout/sys.stderr will be restored to the values saved before.

        Remarks:
            listener_lock is required to run this method to avoid listener dictionaries being modified by other threads.
            This method is being executed in __enter__() and __exit__() with listener_lock.

        """
        if CaptureOutput.__out_listeners:
            out_listener_list = [l for l in CaptureOutput.__out_listeners.values()]
            out_listener_list.append(CaptureOutput.sys_out)
            sys.stdout = OutputWriter(out_listener_list)
        else:
            sys.stdout = CaptureOutput.sys_out
            CaptureOutput.sys_out = None

        if CaptureOutput.__err_listeners:
            err_listener_list = [l for l in CaptureOutput.__err_listeners.values()]
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
        with self.listener_lock:
            CaptureOutput.__out_listeners[self.uuid] = io.StringIO()
            CaptureOutput.__err_listeners[self.uuid] = io.StringIO()
            self.__config_sys_outputs()

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
        self.logs = self.log_handler.logs

        # Update listeners and re-config output writer.
        with self.listener_lock:
            self.std_out = CaptureOutput.__out_listeners.pop(self.uuid).getvalue()
            self.std_err = CaptureOutput.__err_listeners.pop(self.uuid).getvalue()
            self.__config_sys_outputs()

        # Exception will be suppressed if returning True
        if self.suppress_exception:
            return True
        return False


class MessageFormatter(logging.Formatter):
    """Logging Formatter for pretty printing dictionary and list log message.
    By default, this formatter will show time, line number and module in addition to the log message.
    Bytes messages will also be decoded to string.

    See Also: https://docs.python.org/3/library/logging.html#formatter-objects
    """
    # Default message and date formats
    message_format = '%(asctime)s | %(levelname)-8s | %(lineno)4d@%(module)-15s | %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    def __init__(self, fmt=None, datefmt=None, style="%", encoding='utf-8'):
        """Initializes a new instance of formatter
        
        Args:
            fmt (str, optional): Log message format. 
                "MessageFormatter.message_format" attribute will be used if fmt is None.
            datefmt (str, optional): Date format. Defaults to '%Y-%m-%d %H:%M:%S'.
            style (str, optional): formatting styles. Defaults to "%".
                See: https://docs.python.org/3/howto/logging-cookbook.html#formatting-styles
            encoding (str, optional): The encoding for decoding bytes messages. Defaults to 'utf-8'.
        """
        self.encoding = encoding
        if fmt is None:
            fmt = self.message_format
        if datefmt is None:
            datefmt = self.date_format
        super().__init__(fmt, datefmt, style)

    def format(self, record):
        """Formats the log record.

        See https://docs.python.org/3/library/logging.html#logging.Formatter.format
        """
        message = record.msg
        # Decode if the message is bytes
        if self.encoding and isinstance(message, bytes):
            message = message.decode(self.encoding)
        # Try to print dict or list objects as json format with indent.
        # Use pprint if json.dumps() does not work.
        try:
            # if isinstance(message, str):
            #     message = ast.literal_eval(message)
            if isinstance(message, dict) or isinstance(message, list):
                message = json.dumps(message, sort_keys=True, indent=4)
        except Exception:
            message = pprint.pformat(message)
        # Start the message in a new line if the message contains multiple lines.
        # This will print the dict and list starting from a new line.
        if isinstance(message, str) and "\n" in message:
            record.msg = "\n" + message
        else:
            record.msg = message

        message = super(MessageFormatter, self).format(record)
        return message


class StreamHandler(logging.StreamHandler):
    """Stream Handler with customized formats to output module name and line number.
    Logs are sent to sys.stdout
    """

    def __init__(self, stream=sys.stdout, formatter=None):
        """Initialize the handler to send logging to standard output.

        Args:
            stream: The stream to which the outputs are sent.

        """
        super().__init__(stream)
        if isinstance(formatter, str):
            self.setFormatter(MessageFormatter(formatter))
        elif formatter:
            self.setFormatter(formatter)
        else:
            self.setFormatter(MessageFormatter())


class ThreadLogHandler(logging.Handler):
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
        https://github.com/python/cpython/blob/master/Lib/logging/__init__.py

    """
    def __init__(self, thread_id, formatter=None):
        """Initialize the log handler for a particular thread.

        Args:
            thread_id: The ID of the thread.
        """
        super(ThreadLogHandler, self).__init__()
        if formatter is None:
            formatter = MessageFormatter()
        elif isinstance(formatter, str):
            formatter = MessageFormatter(formatter)
        self.setFormatter(formatter)
        self.thread_id = thread_id
        self.logs = []

    def handle(self, record):
        """Determine whether to emit base on the thread ID.
        """
        if record.thread == self.thread_id:
            return super().handle(record)
        return False

    def emit(self, record):
        """Formats and saves the log message.
        """
        try:
            message = self.format(record)
            self.logs.append(message)
        except Exception:
            self.handleError(record)


class PackageLogFilter(logging.Filter):
    """Logging filter to keep logs generated from packages within a certain location
    
    """
    def __init__(self, package_root=None, packages=None):
        """Initialize the filter with a folder path containing the packages.
        This filter keeps only the logs from the packages within the folder.
        
        Args:
            package_root (str): Full path of a folder

        """
        if package_root:
            self.packages = self.get_packages(package_root)
        else:
            self.packages = []
        if isinstance(packages, list):
            self.packages.extend(packages)
        elif packages:
            self.packages.append(packages)

        logger.debug("Filtering logs except packages: %s" % self.packages)
        super().__init__()

    def filter(self, record):
        # Do not filter logs that are not in a package.
        if "." not in record.name:
            return True
        logger_name = record.name.split(".", 1)[0]
        if logger_name in self.packages:
            return True
        return False

    @staticmethod
    def get_packages(folder_path):
        from .storage import StorageFolder
        project_packages = []
        project_folder = StorageFolder(folder_path)
        sub_folders = project_folder.folders
        for sub_folder in sub_folders:
            if "__init__.py" in sub_folder.file_names:
                project_packages.append(sub_folder.name)
        return project_packages


class LoggingConfig:
    """A helper class for configuring python logging.
    """
    def __init__(self, name="", level=logging.DEBUG, formatter=None, filters=None):
        """Initializes a logging config
        
        Args:
            name (str, optional): [description]. Defaults to "".
            level ([type], optional): [description]. Defaults to logging.DEBUG.
            formatter ([type], optional): [description]. Defaults to None.
        """
        self.name = name
        self.level = level
        self.existing_level = None
        self.stream_handler = StreamHandler(formatter=formatter)
        self.log_filters = filters
        if not self.log_filters:
            self.log_filters = []

    def enable(self):
        """Adds a stream_handler to format the logging outputs.
        """
        named_logger = logging.getLogger(self.name)
        self.existing_level = named_logger.getEffectiveLevel()
        named_logger.setLevel(self.level)
        # Add log filters
        for log_filter in self.log_filters:
            self.stream_handler.addFilter(log_filter)
        named_logger.addHandler(self.stream_handler)
        
        return self

    def disable(self):
        """Removes the stream_handler added by enable()
        """
        named_logger = logging.getLogger(self.name)
        named_logger.removeHandler(self.stream_handler)
        for log_filter in self.log_filters:
            named_logger.removeFilter(log_filter)
        named_logger.setLevel(self.existing_level)

    def __enter__(self):
        """Adds a stream_handler to format the logging outputs.
        """
        return self.enable()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Removes the stream_handler added by enable()
        """
        self.disable()
        return False

    @staticmethod
    def decorate(func, name="", level=logging.DEBUG, formatter=None):
        def wrapper(*args, **kwargs):
            with LoggingConfig(name, level, formatter):
                values = func(*args, **kwargs)
            return values
        return wrapper


class LoggingConfigDict:
    default_config_dict = {
        # version must be 1 at this time.
        # See https://docs.python.org/3/library/logging.config.html#configuration-dictionary-schema
        'version': 1,
        'disable_existing_loggers': False,
        'filters': {},
        'handlers': {},
        'loggers': {},
    }

    STREAM_HANDLER = {
        'level': 'DEBUG',
        'class': 'logging.StreamHandler',
        'formatter': 'Aries'
    }

    def __init__(self):
        self.config_dict = copy.deepcopy(self.default_config_dict)
        super().__init__()

    @staticmethod
    def __add_names(config, key, names):
        """Adds names to a logging config value
        The logging config value must be a list, e.g. handler names

        Args:
            config (dict): Logging config dict of a logger/handler, etc.
            key (str): The key in logging config containing a list of values.
            names (list): A list of strings to be added to the list of values.

        Example:
            config can be a logger config like:
            config = {
                'handlers': ['handler1'],
                'level': 'DEBUG',
                'propagate': True,
            }
            Calling __add_names(config, 'handlers', 'handler2') will add 'handler2'
            to the values of 'handlers' in the config. The method will return the following:
            {
                'handlers': ['handler1', 'handler2'],
                'level': 'DEBUG',
                'propagate': True,
            }
        """
        entries = config.get(key, [])
        for name in names:
            if name not in entries:
                entries.append(name)
        config[key] = entries
        return config

    def __add_entry(self, entry_key, entry_name, **kwargs):
        existing = self.config_dict.get(entry_key, dict())
        entry = {entry_name: kwargs}
        existing.update(entry)
        self.config_dict[entry_key] = existing
        return self

    def __update_entries(self, config_key, entry_key, names):
        entries = self.config_dict.get(config_key, {})
        self.config_dict[config_key] = {
            k: self.__add_names(v, entry_key, names) for k, v in entries.items()
        }
        return self

    def add_logger(self, logger_name, level="DEBUG", propagate=True, **kwargs):
        kwargs.update({
            "level": level,
            "propagate": propagate
        })
        self.__add_entry("loggers", logger_name, **kwargs)
        return self

    def add_handler(self, handler_name, handler_class, level="DEBUG", **kwargs):
        """Adds handler to all existing loggers
        
        Args:
            handler_name (str): Handler name
            handler_class (class): Handler class
            level:
        """
        kwargs.update({
            "class": handler_class,
            "level": level
        })
        self.__add_entry("handlers", handler_name, **kwargs)
        # Add handlers to existing loggers
        self.__update_entries("loggers", "handlers", [handler_name])
        return self

    def add_filters(self, filter_dict):
        """Adds filters to all existing handlers.
        If you do not want the filters to be added to some handlers,
        add the handlers after adding the filters.

        Args:
            filter_dict ([type]): [description]
        """
        # Add filters to existing filters
        existing_filters = self.config_dict.get("filters", dict())
        existing_filters.update(filter_dict)
        self.__update_entries("handlers", "filters", filter_dict.keys())
        return self

    def get_config(self):
        return self.config_dict


class Traceback:
    """An extension of the built-in traceback
    """
    @staticmethod
    def local_variables():
        """Returns the local variables where the most recent exception occurs.
        """
        exc_type, exc_value, tb = sys.exc_info()
        if tb is None:
            return dict()
        prev = tb
        curr = tb.tb_next
        while curr is not None:
            prev = curr
            curr = curr.tb_next
            # logger.debug(prev.tb_frame.f_locals)
        return prev.tb_frame.f_locals

    @staticmethod
    def format_exception(limit=None, chain=True):
        """Returns the traceback of the most recent exception, and the local variables

        Returns: A string with traceback and local variables.
        """
        trace = traceback.format_exc(limit, chain)
        var = Traceback.local_variables()
        var_dump = json.dumps(stringify(var), indent=4) if isinstance(var, dict) else str(var)
        trace += "\nLocal Variables:\n" + var_dump
        return trace

    @staticmethod
    def print_exception(limit=None, chain=True):
        """Prints the traceback of the most recent exception, and the local variables
        """
        print(Traceback.format_exception(limit, chain))


class Print:
    """Contain static methods for printing colored messages."""

    BLUE = '\033[94m'  # Blue
    GREEN = '\033[92m'  # Green
    YELLOW = '\033[93m'  # Yellow
    RED = '\033[91m'  # Red

    HEADER = '\033[95m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    ENDC = '\033[0m'

    @staticmethod
    def print(msg, color=None):
        """Prints a message with color.

        Args:
            color: One of the class attribute of ColoredPrint, e.g. ColoredPrint.BLUE.
            msg (str): message.

        """
        if color:
            print(color + Print.format(msg) + Print.ENDC)
        else:
            print(Print.format(msg))

    @staticmethod
    def format(msg):
        # Decode if the message is bytes
        if isinstance(msg, bytes):
            msg = msg.decode()
        # Try to print dict or list objects as json format with indent.
        # Use pprint if json.dumps() does not work.
        try:
            # if isinstance(message, str):
            #     message = ast.literal_eval(message)
            if issubclass(type(msg), dict) or issubclass(type(msg), list):
                msg = json.dumps(msg, sort_keys=True, indent=4)
        except ValueError:
            msg = pprint.pformat(msg)
        # Start the message in a new line if the message contains multiple lines.
        # This will print the dict and list starting from a new line.
        if isinstance(msg, str) and "\n" in msg:
            msg = "\n" + msg
        else:
            msg = msg
        return msg

    @staticmethod
    def green(msg):
        """Prints a message in green."""
        print(Print.GREEN + str(msg) + Print.ENDC)

    @staticmethod
    def red(msg):
        """Prints a message in red."""
        print(Print.RED + str(msg) + Print.ENDC)

    @staticmethod
    def blue(msg):
        """Prints a message in blue."""
        print(Print.BLUE + str(msg) + Print.ENDC)

    @staticmethod
    def yellow(msg):
        """Prints a message in yellow."""
        print(Print.YELLOW + str(msg) + Print.ENDC)
