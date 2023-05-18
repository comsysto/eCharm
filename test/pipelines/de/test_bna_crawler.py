import os
from pathlib import Path
from unittest import TestCase

from charging_stations_pipelines.pipelines.de.bna_crawler import get_bna_data


class Test(TestCase):
    def test_get_bna_data(self):
        path_to_temp_file = Path(__file__).parent.parent.parent.parent.joinpath("data/bna_temp.xlsx")
        get_bna_data(str(path_to_temp_file))
        actual_file_size = os.path.getsize(path_to_temp_file)
        os.remove(path_to_temp_file)
        self.assertTrue(actual_file_size > 6000000) # 6MB

