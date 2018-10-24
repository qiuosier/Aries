import unittest
import os
import sys
import time
import logging
sys.path.append(os.path.abspath(os.path.pardir))
tasks = __import__("tasks")
logger = logging.getLogger(__name__)

#
# class TestShellCommand(unittest.TestCase):
#     def test_run_shell_command(self):
#         with tasks.ShellCommand("ls -a") as cmd:
#             cmd.run()
#             self.assertIn("..\n", cmd.std_out)
#
#     def test_run_shell_command_async(self):
#         with tasks.ShellCommand("ls -a") as cmd:
#             t = cmd.run_async()
#             t.join()
#             self.assertIn("..\n", cmd.std_out)


class TestFunctionTask(unittest.TestCase):

    @staticmethod
    def func_with_delay(name, delay):
        print("%s Function Started" % name)
        TestFunctionTask.add_logs(name, "BEFORE DELAY")
        print("%s Std Error Test" % name, file=sys.stderr)
        time.sleep(delay)
        TestFunctionTask.add_logs(name, "AFTER DELAY")
        print("%s Function Ended" % name)
        return name

    @staticmethod
    def func_with_exception(name, delay):
        print("%s Function Started" % name)
        TestFunctionTask.add_logs(name)
        print("%s Std Error Test" % name, file=sys.stderr)
        time.sleep(delay)
        raise Exception("%s Exception" % name)

    @staticmethod
    def add_logs(prefix, suffix=""):
        logger.debug("%s DEBUG LOG %s" % (prefix, suffix))
        logger.info("%s INFO LOG %s" % (prefix, suffix))
        logger.warning("%s WARNING LOG %s" % (prefix, suffix))
        logger.error("%s ERROR LOG %s" % (prefix, suffix))
        logger.critical("%s CRITICAL LOG %s" % (prefix, suffix))

    @staticmethod
    def expected_logs(prefix, suffix=""):
        return [
                   "%s %s LOG %s" % (prefix, level, suffix)
                   for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
               ]

    def assert_task_output(self, output_string, expected_log_messages):
        actual_logs = output_string.strip("\n").split("\n")
        self.assertEqual(
            len(actual_logs),
            len(expected_log_messages),
            "Number of messages mismatch. Messages: %s" % actual_logs
        )
        for i in range(len(expected_log_messages)):
            self.assertIn(expected_log_messages[i], actual_logs[i], "Incorrect message. %s")

    def test_run_tasks_normal(self):
        """Tests running two functions and capture their outputs independently.

        In this test, t2 output logs and messages while t1 is running,
        the outputs from t2 should not go into t1's output.

        Also, t1 and t2 should not capture any from the main thread.
        """
        # t1 will be executed first with a longer delay.
        func_name_1 = "test1"
        with tasks.FunctionTask(self.func_with_delay, func_name_1, 2) as t1:
            t1.run_async()
        # t2 will be executed with a shorter delay.
        func_name_2 = "test2"
        with tasks.FunctionTask(self.func_with_delay, func_name_2, 1) as t2:
            t2.run_async()
        # Print a message from the main thread.
        # This message should not go into the outputs of t1 or t2
        print("WAITING")
        print("Message to STDERR", file=sys.stderr)
        logger.info("Main Thread Log")

        # Wait for the functions to terminate.
        # t2 will terminate before t1.
        t1.join()
        t2.join()

        # Print out the captured outputs.
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
        expected_msgs_t1 = self.expected_logs(func_name_1, "BEFORE DELAY") + \
            self.expected_logs(func_name_1, "AFTER DELAY")
        expected_msgs_t2 = self.expected_logs(func_name_2, "BEFORE DELAY") + \
            self.expected_logs(func_name_2, "AFTER DELAY")
        self.assert_task_output(t1.log_out, expected_msgs_t1)
        self.assert_task_output(t2.log_out, expected_msgs_t2)

    def test_run_tasks_with_exception(self):
        """Tests running two functions with exceptions and capture their outputs independently.
        """
        # t1 will be executed first with a longer delay.
        func_name_1 = "test1"
        with tasks.FunctionTask(self.func_with_exception, func_name_1, 2) as t1:
            t1.run_async()
        # t2 will be executed with a shorter delay.
        func_name_2 = "test2"
        with tasks.FunctionTask(self.func_with_exception, func_name_2, 1) as t2:
            t2.run_async()
        # Print a message from the main thread.
        # This message should not go into the outputs of t1 or t2
        print("WAITING")
        print("Message to STDERR", file=sys.stderr)
        logger.info("Main Thread Log")

        # Wait for the functions to terminate.
        # t2 will terminate before t1.
        t1.join()
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
