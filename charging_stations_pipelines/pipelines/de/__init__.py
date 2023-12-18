"""This module defines the pipeline for processing data from the BNA (Bundesnetzagentur) data source."""

from typing import Final

DATA_SOURCE_KEY: Final[str] = "BNA"
"""The data source key for the BNA (Bundesnetzagentur) data source."""


class BnaCrawlerException(Exception):
    """Base class for exceptions in BnaCrawler."""
    pass


class FetchWebsiteException(BnaCrawlerException):
    """Raised when there is an error fetching the website."""
    pass


class ExtractURLException(BnaCrawlerException):
    """Raised when there is an error extracting the URL."""
    pass


class DownloadFileException(BnaCrawlerException):
    """Raised when there is an error downloading a file."""
    pass
