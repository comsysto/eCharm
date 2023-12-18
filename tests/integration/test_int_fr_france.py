"""Integration tests for the crawler of the France pipeline."""

import os
import tempfile

import pytest

from charging_stations_pipelines.pipelines.fr.france import FraPipeline
from test.shared import skip_if_github


@pytest.mark.integration_test
@pytest.mark.skipif(skip_if_github(), reason="Skip the test when running on Github")
def test_download_france_gov_file():
    """Test the download function."""
    with tempfile.NamedTemporaryFile() as temp_file:
        FraPipeline.download_france_gov_file(temp_file.name)
        assert os.path.getsize(temp_file.name) >= 47_498_370  # ~ 50 MB
