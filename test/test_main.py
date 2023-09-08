from unittest import TestCase

from main import parse_args


class TestCommandLineArguments(TestCase):
    def test_parse_valid_args(self):
        arguments = parse_args('import merge --countries de GB --online'.split())
        self.assertEqual(["import", "merge"], arguments.tasks)
        self.assertEqual(["DE", "GB"], arguments.countries)
        self.assertTrue(arguments.online)
        self.assertFalse(arguments.delete_data)

    def test_parse_no_task_arg(self):
        with self.assertRaises(SystemExit):
            parse_args([])

    def test_parse_invalid_task_arg(self):
        with self.assertRaises(SystemExit):
            parse_args('invalid_task --countries de'.split())
