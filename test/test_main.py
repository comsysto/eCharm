from unittest import TestCase

from main import parse_args


class TestCommandLineArguments(TestCase):
    def test_parse_valid_args(self):
        arguments = parse_args('import merge --countries de GB'.split())
        self.assertEqual(["import", "merge"], arguments.tasks)
        self.assertEqual(["DE", "GB"], arguments.countries)
        self.assertFalse(arguments.offline)
        self.assertFalse(arguments.delete_data)

    def test_parse_offline_arg(self):
        arguments = parse_args('import --offline'.split())
        self.assertTrue(arguments.offline)

    def test_parse_delete_data_arg(self):
        arguments = parse_args('import --delete_data'.split())
        self.assertTrue(arguments.delete_data)

    def test_parse_no_task_arg(self):
        with self.assertRaises(SystemExit):
            parse_args([])

    def test_parse_invalid_task_arg(self):
        with self.assertRaises(SystemExit):
            parse_args('invalid_task --countries de'.split())
