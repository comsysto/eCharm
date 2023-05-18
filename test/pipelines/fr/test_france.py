import os
from pathlib import Path
from unittest import TestCase

from charging_stations_pipelines.pipelines.fr.france import FraPipeline


class TestFraPipeline(TestCase):
    def test_download_france_gov_file(self):
        temp_target = Path(__file__).parent.parent.parent.parent.joinpath("data/fra_temp.csv")
        FraPipeline.download_france_gov_file(temp_target)
        actual_file_size = os.path.getsize(temp_target)
        os.remove(temp_target)
        self.assertTrue(actual_file_size > 40000000)  # 40MB
