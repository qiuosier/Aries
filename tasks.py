import io
import math
import logging
import pstats
import subprocess
import sys
import threading
import time
import traceback
from multiprocessing import Process, Pipe
from cProfile import Profile
logger = logging.getLogger(__name__)


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
            thread_id: The ID of the thread
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

    See Also:
        The __run() and run() methods in FunctionTask class uses this class and the multiprocessing package
        to capture the outputs of a particular thread.

    Warnings:
        Using this class will set the level of root logger to DEBUG.

    """
    def __init__(self, suppress_exception=False):
        """Initializes log handler and attributes to store the outputs.
        """
        self.log_handler = ThreadLogHandler(threading.current_thread().ident)
        self.log_handler.setLevel(logging.DEBUG)
        self.suppress_exception = suppress_exception

        self.std_out = ""
        self.std_err = ""
        self.log_out = ""
        self.exc_out = ""
        self.returns = None

    def __enter__(self):
        """Redirects stdout/stderr, and attaches the log handler to root logger.

        Returns: A CaptureOutput object (self).

        """
        self.saved_stdout = sys.stdout
        self.saved_stderr = sys.stderr
        sys.stdout = self.string_io_out = io.StringIO()
        sys.stderr = self.string_io_err = io.StringIO()
        # Modify root logger level and add log handler.
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(self.log_handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Saves the outputs, resets stdout/stderr, and removes log handler.
        """
        # Reset stdout and stderr
        self.std_out = self.string_io_out.getvalue()
        self.std_err = self.string_io_err.getvalue()
        del self.string_io_out
        del self.string_io_err
        sys.stdout = self.saved_stdout
        sys.stderr = self.saved_stderr
        # Removes log handler
        root_logger = logging.getLogger()
        root_logger.removeHandler(self.log_handler)
        self.log_out = "\n".join(self.log_handler.logs)
        # Capture exceptions, if any
        if exc_type:
            self.exc_out = traceback.format_exc()
        if self.suppress_exception:
            return True
        return False


class Task:
    """A base class for representing a task like running a function or a command.

    Attributes:
        thread: The thread running the task, if the the task is running asynchronous.
            The thread value is set by run_async().
        The following attributes are designed to capture the output of running the task.
        std_out (str): Captured standard outputs.
        std_err (str): Captured standard errors.
        log_out (str): Captured log messages.
        exc_out (str): Captured exception outputs.
        returns: Return value of the task.
        pid (int): The PID of the process running the task.

    This class should not be initialized directly.
    The subclass should implement the run() method.
    The run() method should handle the capturing of outputs.
    """
    def __init__(self):
        self.pid = None
        self.thread = None
        self.returns = None
        self.std_out = ""
        self.std_err = ""
        self.log_out = ""
        self.exc_out = ""

    def print_outputs(self):
        """Prints the PID, return value, stdout, stderr and logs.
        """
        print("=" * 80)
        print("PID: %s" % self.pid)
        print("RETURNS: %s" % self.returns)
        print("STD OUT:")
        for line in self.std_out.split("\n"):
            print(line)
        print("STD ERR:")
        for line in self.std_err.split("\n"):
            print(line)
        print("LOGS:")
        for line in self.log_out.split("\n"):
            print(line)

    def run(self):
        """Runs the task and capture the outputs.
        This method should be implemented by a subclass.
        """
        raise NotImplementedError(
            "A run() method should be implemented to run the task and capture the outputs."
        )

    def run_async(self):
        """Runs the task asynchronous by calling the run() method in a daemon thread.

        Returns: The daemon thread running the task.
        """
        thread = threading.Thread(
            target=self.run,
        )
        thread.daemon = True
        thread.start()
        self.thread = thread
        return thread

    def join(self):
        """Block the calling thread until the daemon thread running the task terminates.
        """
        if self.thread and self.thread.isAlive():
            return self.thread.join()
        else:
            return None


class FunctionTask(Task):
    """Represents a task of running a function.

    The return value of the function to be executed should be serializable.

    The function will be in a separated process, so that the stdout/stderr will be captured independently.
    The logging will be captured by identifying the thread ID of the thread running the function.
    The captured outputs are sent back using the "Pipe" of python multiprocess package.
    Data sending through "Pipe" must be serializable.

    Attributes:
        thread: The thread running the function, if the the function is running asynchronous.
            The thread value is set by run_async().
        The following attributes are designed to capture the output of running the function.
        std_out (str): Captured standard outputs.
        std_err (str): Captured standard errors.
        log_out (str): Captured log messages.
        exc_out (str): Captured exception outputs.
        returns: Return value of the task.
        pid (int): The PID of the process running the task.
        func: The function to be executed.
        args: The arguments for executing the function.
        kwargs: The keyword arguments for executing the function.

    """
    # Stores a list of attribute names to be captured from the process running the function
    __output_attributes = [
        "std_out",
        "std_err",
        "log_out",
        "exc_out",
        "returns",
    ]

    def __init__(self, func, *args, **kwargs):
        """Initializes a task to run a function.

        Args:
            func: The function to be executed.
            *args: A list of arguments for the function to be executed.
            **kwargs: A dictionary of keyword arguments for the function to be executed.
        """
        super(FunctionTask, self).__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __unpack_outputs(self, out):
        for k in self.__output_attributes:
            setattr(self, k, out.get(k))

    def __pack_outputs(self, out):
        return {
            k: getattr(out, k) for k in self.__output_attributes
        }

    def __run(self, pipe):
        with CaptureOutput(suppress_exception=True) as out:
            # TODO: Returns may not be serializable.
            out.returns = str(self.func(*self.args, **self.kwargs))
        try:
            logger.debug("Sending captured outputs...")
            pipe.send(self.__pack_outputs(out))
        except Exception as ex:
            print(ex)
            pipe.send({
                "exc_out": traceback.format_exc()
            })

    def run(self):
        """Runs the function in a separated process and captures the outputs.
        """
        receiver, pipe = Pipe()
        p = Process(target=self.__run, args=(pipe,))
        p.start()
        self.pid = p.pid
        out = receiver.recv()
        p.join()
        self.__unpack_outputs(out)
        if self.exc_out:
            print(self.exc_out)
        self.exit_run()

    def exit_run(self):
        pass

    def run_profiler(self):
        """Runs the function with profiler.
        """
        profile = Profile()
        profile.runcall(self.func, *self.args, **self.kwargs)
        stats = pstats.Stats(profile)
        stats.strip_dirs()
        # Display profiling results
        stats.sort_stats('cumulative', 'time').print_stats(0.1)

    def run_and_retry(self, max_retry=10, exceptions=Exception):
        """Runs the function and retry a few times if certain exceptions occurs.
        The time interval between the ith and (i+1)th retry is e**i, i.e. interval increases exponentially.

        Args:
            max_retry (int): The number of times to re-try.
            exceptions (Exception or tuple): An exception class or A tuple of exception classes.

        Returns: The return value of the function.

        """
        error = None
        for i in range(max_retry):
            try:
                results = self.func(*self.args, **self.kwargs)
            except exceptions as ex:
                error = ex
                time.sleep(math.exp(i))
            else:
                return results
        # The following will be executed only if for loop finishes without break/return
        else:
            raise error


class ShellCommand(Task):
    """Represents a task of running a function.
    """
    def __init__(self, cmd):
        super(ShellCommand, self).__init__()
        self.cmd = cmd
        self.process = None

    def run(self):
        self.process = subprocess.Popen(
            self.cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True
        )
        self.pid = self.process.pid
        out, err = self.process.communicate()
        self.std_out = out.decode()
        self.std_err = err.decode()
        self.returns = self.process.returncode
