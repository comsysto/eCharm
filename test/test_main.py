from unittest import TestCase

from main import CommandLineArguments


class TestCommandLineArguments(TestCase):
    def test_parse_valid_args(self):
        arguments = CommandLineArguments(["main.py", "--tasks=import,merge", "--countries=DE,GB", "--online=false"])
        self.assertEqual(["import", "merge"], arguments.tasks)
        self.assertEqual(["DE", "GB"], arguments.countries)
        self.assertFalse(arguments.online)

    def test_parse_invalid_task_arg(self):
        self.assertRaises(RuntimeError,
                          CommandLineArguments, ["main.py", "--tasks=xxx", "--countries=DE,GB", "--online=true"])
