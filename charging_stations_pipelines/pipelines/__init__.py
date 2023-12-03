"""Module for data processing pipelines."""

import configparser

import pandas as pd
from sqlalchemy.orm import Session


class Pipeline:
    """Base class for data processing pipelines."""

    def __init__(self, config: configparser, session: Session, online=False):
        self.config = config
        self.session = session
        self.online = online

        self.data: pd.DataFrame

    def _retrieve_data(self):
        raise NotImplementedError

    def run(self):
        """Executes the pipeline."""
        raise NotImplementedError
