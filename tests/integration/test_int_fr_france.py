"""Integration tests for the crawler of the France pipeline."""

import tempfile
from pathlib import Path

import pytest

from charging_stations_pipelines.pipelines.fr.france import FraPipeline
from test.shared import skip_if_github


@pytest.mark.integration_test
@pytest.mark.skipif(skip_if_github(), reason="Skip the test when running on Github")
def test_download_france_gov_file():
    """Test the download function."""
    with tempfile.NamedTemporaryFile() as temp_file:
        temp_file_path: Path = Path(temp_file.name)
        FraPipeline.download_france_gov_file(temp_file_path)
        assert temp_file_path.stat().st_size>= 47_498_370  # ~ 50 MB
