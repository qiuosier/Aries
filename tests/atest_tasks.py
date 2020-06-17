"""Contains tests for the tasks module.
"""
import time
import logging

import os
import sys
aries_parent = os.path.join(os.path.dirname(__file__), "..", "..")
if aries_parent not in sys.path:
    sys.path.append(aries_parent)
from Aries.test import AriesTest
from Aries import tasks

logger = logging.getLogger(__name__)


class TestShellCommand(AriesTest):
    def test_run_shell_command(self):
        cmd = tasks.ShellCommand("ls -a %s" % os.path.dirname(__file__))
        cmd.run()
        self.assertIn("..\n", cmd.std_out)
        self.assertIn(os.path.basename(__file__), cmd.std_out)


class TestRunRetry(AriesTest):
    tries = 0

    @staticmethod
    def func_to_retry():
        if TestRunRetry.tries < 2:
            TestRunRetry.tries += 1
            print("Test Run Retry: %s" % TestRunRetry.tries)
            raise ValueError("Try again later")
        return TestRunRetry.tries

    def setUp(self):
        TestRunRetry.tries = 0

    def test_run_and_retry_success(self):
        """Tests the run and retry of FunctionTask
        """
        # Running the function for the first time will raise a ValueError
        with self.assertRaises(ValueError):
            self.func_to_retry()
        # Retry the runing the function until there is no error.
        task = tasks.FunctionTask(self.func_to_retry)
        count = task.run_and_retry(3)
        self.assertEqual(count, 2)

    def test_run_and_retry_fail(self):
        with self.assertRaises(ValueError):
            task = tasks.FunctionTask(self.func_to_retry)
            task.run_and_retry(2)


class TestFunctionTask(AriesTest):

    @staticmethod
    def func_with_delay(name, delay):
        """A function with outputs and logs.
        The function will sleep for the seconds specified in "delay" between
            sending two sets of outputs and logs.

        Args:
            name (str): An identifier to be included in the outputs and log messages.
            delay (int): The number of seconds between sending two sets of outputs and logs.

        Returns: The "name" as a string.

        """
        print("%s Function Started" % name)
        TestFunctionTask.output_logs(name, "BEFORE DELAY")
        print("%s Std Error Test" % name, file=sys.stderr)
        for i in range(delay):
            time.sleep(1)
            print("%s waited %s second" % (name, i + 1))
        TestFunctionTask.output_logs(name, "AFTER DELAY")
        print("%s Function Ended" % name)
        return name

    @staticmethod
    def func_with_exception(name, delay):
        """A function with outputs, logs and an exception.
        The function will sleep for the seconds specified in "delay" between
            sending outputs/logs and raising an exception.

        Args:
            name (str): An identifier to be included in the outputs and log messages.
            delay (int): The number of seconds between sending outputs/logs and raising an exception.

        Returns: This function does not return anything.

        """
        print("%s Function Started" % name)
        TestFunctionTask.output_logs(name)
        print("%s Std Error Test" % name, file=sys.stderr)
        for i in range(delay):
            time.sleep(1)
            print("%s waited %s second" % (name, i + 1))
        raise Exception("%s Exception" % name)

    @staticmethod
    def output_logs(prefix, suffix=""):
        """Outputs five levels of logs. Each log message will have the specific prefix and suffix.

        Args:
            prefix (str): Prefix for the log messages.
            suffix (str): Suffix for the log messages.

        Returns: This function does not return anything.

        """
        logger.debug("%s DEBUG LOG %s" % (prefix, suffix))
        logger.info("%s INFO LOG %s" % (prefix, suffix))
        logger.warning("%s WARNING LOG %s" % (prefix, suffix))
        logger.error("%s ERROR LOG %s" % (prefix, suffix))
        logger.critical("%s CRITICAL LOG %s" % (prefix, suffix))

    @staticmethod
    def expected_logs(prefix, suffix=""):
        """Gets the expected log messages output by output_logs() as a list.

        Args:
            prefix (str): Prefix for the log messages.
            suffix (str): Suffix for the log messages.

        Returns: A list of strings.

        """
        return [
                   "%s %s LOG %s" % (prefix, level, suffix)
                   for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
               ]

    def assert_task_output(self, output_string, expected_list):
        """Checks if a list of strings is a subset (sub-list) of  another list of strings.

        Args:
            output_string: A list of string expected to contain strings in another list.
            expected_list: The smaller list of string to be contained in another list of strings.

        This function will raise exception if not all strings in the expected_list are found in the output_string.

        Returns: This function does not return anything.

        """
        output_list = output_string.strip("\n").split("\n")
        for msg in expected_list:
            for out in output_list:
                if msg in out:
                    break
            else:
                self.fail("Output not found: %s" % msg)

    def test_run_tasks_normal(self):
        """Tests running two functions and capture their outputs independently.

        In this test, t2 output logs and messages while t1 is running,
        the outputs from t2 should not go into t1's output.

        Also, t1 and t2 should not capture output any from the main thread.
        """
        print()
        # t1 will be executed first with a longer delay.
        func_name_1 = "test1"
        t1 = tasks.FunctionTask(self.func_with_delay, func_name_1, 2)
        t1.run_async()
        time.sleep(0.5)
        # t2 will be executed with a shorter delay.
        func_name_2 = "test2"
        t2 = tasks.FunctionTask(self.func_with_delay, func_name_2, 1)
        t2.run_async()
        # Output messages on the main thread.
        # These messages should not go into the outputs of t1 or t2
        print("Message to STDERR", file=sys.stderr)
        logger.info("Main Thread Log")
        print("WAITING for t1")
        # Wait for the functions to terminate.
        # t2 will terminate before t1.
        t1.join()
        print("WAITING for t2")
        t2.join()

        # Print out the captured outputs, for debug purpose.
        t1.print_outputs()
        t2.print_outputs()

        # Assert return values
        self.assertEqual(t1.returns, func_name_1, "Incorrect return value.")
        self.assertEqual(t2.returns, func_name_2, "Incorrect return value.")

        # Assert standard outputs
        expected_outputs_t1 = [
            "%s Function Started" % func_name_1,
            "%s Function Ended" % func_name_1,
        ]
        expected_outputs_t2 = [
            "%s Function Started" % func_name_2,
            "%s Function Ended" % func_name_2,
        ]
        self.assert_task_output(t1.std_out, expected_outputs_t1)
        self.assert_task_output(t2.std_out, expected_outputs_t2)

        # Assert standard error
        self.assert_task_output(t1.std_err, ["%s Std Error Test" % func_name_1])
        self.assert_task_output(t2.std_err, ["%s Std Error Test" % func_name_2])

        # Assert logs
        # Expected logs
        expected_msgs_t1 = self.expected_logs(func_name_1, "BEFORE DELAY") + \
            self.expected_logs(func_name_1, "AFTER DELAY")
        expected_msgs_t2 = self.expected_logs(func_name_2, "BEFORE DELAY") + \
            self.expected_logs(func_name_2, "AFTER DELAY")
        # Check if the expected logs are in the actual logs
        self.assert_task_output(t1.log_out, expected_msgs_t1)
        self.assert_task_output(t2.log_out, expected_msgs_t2)
        # Check the number of logs messages
        self.assertEqual(len(expected_msgs_t1), len(t1.log_list))
        self.assertEqual(len(expected_msgs_t2), len(t2.log_list))

    def test_run_tasks_with_exception(self):
        """Tests running two functions with exceptions and capture their outputs independently.
        """
        print()
        # t1 will be executed first with a longer delay.
        func_name_1 = "test1"
        t1 = tasks.FunctionTask(self.func_with_exception, func_name_1, 2)
        t1.run_async()
        # t2 will be executed with a shorter delay.
        func_name_2 = "test2"
        t2 = tasks.FunctionTask(self.func_with_exception, func_name_2, 1)
        t2.run_async()
        # Print a message from the main thread.
        # This message should not go into the outputs of t1 or t2
        print("Message to STDERR", file=sys.stderr)
        logger.info("Main Thread Log")
        print("WAITING for t1")
        # Wait for the functions to terminate.
        # t2 will terminate before t1.
        t1.join()
        print("WAITING for t2")
        t2.join()

        # Print out the captured outputs.
        t1.print_outputs()
        t2.print_outputs()

        # Assert return values
        self.assertIsNone(t1.returns, "Return value should be None.")
        self.assertIsNone(t2.returns, "Return value should be None.")

        # Assert standard outputs
        expected_outputs_t1 = [
            "%s Function Started" % func_name_1,
        ]
        expected_outputs_t2 = [
            "%s Function Started" % func_name_2,
        ]
        self.assert_task_output(t1.std_out, expected_outputs_t1)
        self.assert_task_output(t2.std_out, expected_outputs_t2)

        # Assert standard error
        self.assert_task_output(t1.std_err, ["%s Std Error Test" % func_name_1])
        self.assert_task_output(t2.std_err, ["%s Std Error Test" % func_name_2])

        # Assert logs
        expected_msgs_t1 = self.expected_logs(func_name_1)
        expected_msgs_t2 = self.expected_logs(func_name_2)
        self.assert_task_output(t1.log_out, expected_msgs_t1)
        self.assert_task_output(t2.log_out, expected_msgs_t2)

        # Assert Exception
        self.assertIn("test1 Exception", t1.exc_out)
        self.assertIn("test2 Exception", t2.exc_out)
