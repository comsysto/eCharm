"""Integration tests for the crawler of the BNA pipeline."""

import os
import tempfile
from pathlib import Path

import pytest

from charging_stations_pipelines.pipelines.de.bna_crawler import get_bna_data
from test.shared import skip_if_github


@pytest.mark.integration_test
@pytest.mark.skipif(skip_if_github(), reason="Skip the test when running on Github")
def test_get_bna_data():
    """Test the get_bna_data function."""
    with tempfile.NamedTemporaryFile() as temp_file:
        get_bna_data(Path(temp_file.name))
        assert os.path.getsize(temp_file.name) > 6 * 1_000_000, "File size is less than 6MB"
