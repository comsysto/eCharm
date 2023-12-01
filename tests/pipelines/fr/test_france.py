import os
from pathlib import Path

from charging_stations_pipelines.pipelines.fr.france import FraPipeline
from tests.shared import skip_if_github


@skip_if_github
def test_download_france_gov_file():
    temp_target = Path(__file__).parent.parent.parent.parent.joinpath("data/fra_temp.csv")
    FraPipeline.download_france_gov_file(temp_target)
    actual_file_size = os.path.getsize(temp_target)
    os.remove(temp_target)
    assert actual_file_size == 20000000  # 20MB
