import unittest

import pandas as pd

from charging_stations_pipelines.pipelines.shared import check_coordinates, lst_flatten, parse_date, \
    str_strip_whitespace, str_to_float, try_remove_dupes


class TestShared(unittest.TestCase):

    def test_check_coordinates(self):
        self.assertEqual(check_coordinates("52.52"), 52.52)
        self.assertEqual(check_coordinates("-52.52"), -52.52)
        self.assertEqual(check_coordinates(3.14), 3.14)
        self.assertEqual(check_coordinates(3), 3.0)
        with self.assertRaises(ValueError):
            check_coordinates("N/A")

    def test_parse_date(self):
        self.assertEqual(parse_date("2023-03-29T17:45:00Z").isoformat(), "2023-03-29T17:45:00+00:00")
        self.assertIsNone(parse_date(None))

    def test_str_strip_whitespace(self):
        self.assertEqual(str_strip_whitespace("    test    "), "test")
        self.assertEqual(str_strip_whitespace(pd.Series(["    test1    ", "   test2   "])), ["test1", "test2"])
        self.assertEqual(str_strip_whitespace(["    test1    ", "   test2   "]), ["test1", "test2"])

    def test_str_to_float(self):
        self.assertEqual(str_to_float("5.5"), 5.5)
        self.assertEqual(str_to_float(None), None)

    def test_lst_flatten(self):
        self.assertEqual(lst_flatten([[1, 2, 3], [4, 5, 6]]), [1, 2, 3, 4, 5, 6])
        self.assertEqual(lst_flatten([[1, 2, 3], None]), [1, 2, 3])

    def test_try_remove_dupes(self):
        self.assertEqual(try_remove_dupes([1, 1, 2, 3]), [1, 2, 3])
        self.assertEqual(try_remove_dupes(None), [])
