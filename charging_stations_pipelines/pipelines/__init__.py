"""Pipeline's generic code."""

import configparser
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Final, Optional, Union

import pandas as pd
from sqlalchemy.orm import Session

from charging_stations_pipelines import JSON


class Pipeline(ABC):
    """Abstract base class for data processing pipelines.
        :param config: A `configparser` object containing configurations for the pipeline.
        :param session: A `Session` object representing the session used for database operations.
        :param online: A boolean indicating whether the pipeline should retrieve data online. Default is False.

        :ivar data: A `pandas.DataFrame` or `JSON` object containing the data retrieved from the data source.
        :ivar data_path: A string representing the directory or file where data files will be stored.
    """

    def __init__(self, config: configparser, session: Optional[Session], online=False):
        self.config: Final[configparser] = config
        self.session: Final[Session] = session
        self.online: Final[bool] = online

        self.data: Optional[Union[pd.DataFrame, JSON]] = None
        self.data_path: Optional[Path] = None

    @abstractmethod
    def retrieve_data(self, **kwargs) -> None:
        """Retrieves the data from the data source."""
        raise NotImplementedError

    @abstractmethod
    def run(self, **kwargs) -> None:
        """Executes the pipeline and ingests the data."""
        raise NotImplementedError
