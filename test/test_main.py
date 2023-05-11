from unittest import TestCase

from main import CommandLineArguments


class TestCommandLineArguments(TestCase):
    def test_parse_commandline_args(self):
        arguments = CommandLineArguments(["main.py", "--tasks=import,merge", "--countries=de,uk", "--online=true"])
        self.assertEqual(["import", "merge"], arguments.tasks)
        self.assertEqual(["de", "uk"], arguments.countries)
        self.assertTrue(arguments.online)