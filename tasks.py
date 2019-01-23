import math
import logging
import pstats
import subprocess
import threading
import time
import traceback
from cProfile import Profile

from outputs import CaptureOutput

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

        self.out = None

    def __unpack_outputs(self, out):
        for k in self.__output_attributes:
            setattr(self, k, out.get(k))

    def __pack_outputs(self, out):
        return {
            k: getattr(out, k) for k in self.__output_attributes
        }

    def __run(self):
        with CaptureOutput(suppress_exception=True) as out:
            # TODO: Returns may not be serializable.
            out.returns = str(self.func(*self.args, **self.kwargs))
        try:
            logger.debug("Sending captured outputs...")
            return self.__pack_outputs(out)
            # pipe.send(self.__pack_outputs(out))
        except Exception as ex:
            print(ex)
            return {
                "exc_out": traceback.format_exc()
            }

    def run(self):
        """Runs the function in a separated process and captures the outputs.
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
            if error:
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
