"""Module for data processing pipelines."""

import configparser
from typing import Optional, Union

import pandas as pd
from sqlalchemy.orm import Session

from charging_stations_pipelines.shared import JSON


class Pipeline:
    """Base class for data processing pipelines."""

    def __init__(self, config: configparser, session: Session, online=False):
        self.config = config
        self.session = session
        self.online = online

        self.data: Optional[Union[pd.DataFrame, JSON]] = None

    def _retrieve_data(self):
        """Retrieves the data from the data source."""
        raise NotImplementedError

    def run(self):
        """Executes the pipeline and ingests the data."""
        raise NotImplementedError
