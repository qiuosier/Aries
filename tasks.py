"""Contains classes for running functions/commands asynchronously.
"""
import logging
import pstats
import subprocess
import threading
import time
import traceback
from cProfile import Profile
# try:
from .outputs import CaptureOutput
# except SystemError:
#     import sys
#     from os.path import dirname
#     aries_parent = dirname(dirname(__file__))
#     if aries_parent not in sys.path:
#         sys.path.append(aries_parent)
#     from Aries.outputs import CaptureOutput

logger = logging.getLogger(__name__)


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
        self.exception = None
        self.std_out = ""
        self.std_err = ""
        self.log_out = ""
        self.exc_out = ""

    @property
    def log_list(self):
        """Log messages as a list.
        """
        return self.log_out.strip("\n").split("\n")

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
        return self.thread

    def join(self):
        """Blocks the calling thread until the daemon thread running the task terminates.
        """
        if self.thread and self.thread.isAlive():
            return self.thread.join()
        else:
            return None


class FunctionTask(Task):
    """Represents a task of running a function.

    The return value of the function to be executed should be serializable.
    The logging will be captured by identifying the thread ID of the thread running the function.

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

    Remarks:
        std_out and std_err will contain the outputs from all threads running in the same process.

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

        self.log_filters = []

        self.out = None

    def add_log_filter(self, log_filter):
        self.log_filters.append(log_filter)
        return self

    def __unpack_outputs(self, out):
        """Sets a list of attributes (self.__output_attributes) by copying values from a dictionary.

        Args:
            out (dict): The dictionary containing the values for attributes.
                The keys in the dictionary must be the same as the attribute names.

        """
        for k in self.__output_attributes:
            setattr(self, k, out.get(k))

    def __pack_outputs(self, out):
        """Saves a list of attributes (self.__output_attributes) to a dictionary.

        Args:
            out: An object with all attributes listed in self.__output_attributes.

        """
        return {
            k: getattr(out, k) for k in self.__output_attributes
        }

    def __run(self):
        try:
            with CaptureOutput(filters=self.log_filters) as out:
                self.out = out
                # Returns may not be serializable.
                out.returns = self.func(*self.args, **self.kwargs)
        except Exception as ex:
            # Catch and save the exception
            # run() determines whether to raise the exception
            #   base on "surpress_exception" argument.
            self.exception = ex
        else:
            # Reset self.exception if the run is successful.
            # This is for run_and_retry()
            self.exception = None
        try:
            # name = self.func.__name__ if hasattr(self.func, "__name__") else str(self.func)
            # logger.debug("Finished running %s()..." % name)
            return self.__pack_outputs(out)
        except Exception as ex:
            print(ex)
            return {
                "exc_out": traceback.format_exc()
            }

    def exit_run(self):
        """Additional processing before exiting the task.

        This method is intended to be implemented by a subclass.
        """
        pass

    def run(self, suppress_exception=True):
        """Runs the function and captures the outputs.
        """
        # receiver, pipe = Pipe()
        # p = Process(target=self.__run, args=(pipe,))
        # p.start()
        #
        # self.pid = p.pid
        # print("%s PROCESS STARTED" % p.pid)
        # out = receiver.recv()
        # print("%s MESSAGE RECEIVED" % p.pid)
        # p.terminate()
        self.__unpack_outputs(self.__run())
        if self.exc_out:
            print(self.exc_out)
        self.exit_run()
        if not suppress_exception and self.exception is not None:
            raise self.exception
        return self.returns

    def run_profiler(self):
        """Runs the function with profiler.
        """
        profile = Profile()
        profile.runcall(self.func, *self.args, **self.kwargs)
        stats = pstats.Stats(profile)
        stats.strip_dirs()
        # Display profiling results
        stats.sort_stats('cumulative', 'time').print_stats(0.1)

    def run_and_retry(self, max_retry=10, exceptions=Exception, 
                      base_interval=2, retry_pattern='exponential', capture_output='True'):
        """Runs the function and retry a few times if certain exceptions occurs.
        The time interval between the ith and (i+1)th retry is base_interval**i, 
            i.e. interval increases exponentially.

        Args:
            max_retry (int): The number of times to re-try.
            exceptions: An exception class or A tuple of exception classes.
            base_interval (int): The interval before the first retry in seconds.
            retry_pattern (str): The pattern of the retry interval. 
                "exponential": The time between two retries will increase exponentially.
                    i.e., the interval will be "base_interval ** i" after the ith try.
                "linear": The time between two retries will increase linear.
                    i.e., the interval will be "base_interval * i" after the ith try.
            capture_output: Indicate if the outputs and logs of the function should be captured.
                Outputs and logs will be captured to std_out, std_err and log_out attributes.
                Setting capture_output to False will improve the performance.

        Returns: The return value of the function.

        """
        error = None
        for i in range(max_retry):
            try:
                if capture_output:
                    results = self.run(suppress_exception=False)
                else:
                    results = self.func(*self.args, **self.kwargs)
            except exceptions as ex:
                error = ex
                traceback.print_exc()
                if retry_pattern == "exponential":
                    time.sleep(base_interval ** (i + 1))
                else:
                    time.sleep(base_interval * (i + 1))
            else:
                return results
        # The following will be executed only if for loop finishes without break/return
        else:
            raise error


class ShellCommand(Task):
    """Represents a task of running a shell command.

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
    
    This class can be used to run a shell command and capture the outputs.
    For example, the command "ls -a ~" displays all files in the user's home directory.
    The following code runs this command:

        cmd = "ls -a ~"
        task = ShellCommand(cmd)
        task.run()
        print(task.std_out)
    
    The outputs are stored in "task.std_out" as a string.

    Run the command asynchronously, if the command takes a long time to complete:

        cmd = "YOUR_AWESOME_COMMAND"
        task = ShellCommand(cmd)
        task.run_async()
        # Feel free to do something else here
        # ...
        # Get the outputs
        task.join()
        print(task.std_out)
    
    """
    def __init__(self, cmd):
        super(ShellCommand, self).__init__()
        self.cmd = cmd
        self.process = None

    def run(self):
        """Runs the command with Popen()
        """
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
        return self
