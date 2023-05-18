import os
from pathlib import Path
from unittest import TestCase

from charging_stations_pipelines.pipelines.de.bna_crawler import get_bna_data


class Test(TestCase):
    def test_get_bna_data(self):
        data_dir = Path(__file__).parent.parent.parent.parent.joinpath("data/bna_temp.xlsx")
        get_bna_data(str(data_dir))
        self.assertTrue(os.path.getsize(data_dir)>6000000) # 6MB

